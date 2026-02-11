# Implementation Phases

Test-driven development approach with incremental delivery.

**Philosophy:** Phases 1-3 build a complete single-worker system that is production-ready for small-to-medium scale. Phase 4 adds distributed processing for massive scale.

---

## Phase 1: Core Infrastructure

**Goal**: Single worker, full refresh only, CLI interface

**Metadata Schema:**
```sql
-- Just 4 tables!
CREATE TABLE dynamic_tables (...);
CREATE TABLE source_snapshots (...);
CREATE TABLE dependencies (...);
CREATE TABLE refresh_history (...);
-- No work queues, no claims, no coordination
```

**Tests**:
- Parse `CREATE DYNAMIC TABLE` DDL
- Store/retrieve table metadata
- Single worker polls and processes work
- **Initial load does full refresh without CDC (bootstrap)**
- **Bootstrap respects dependency order (A→B→C processed in sequence)**
- Dependency chains processed in same iteration (A→B→C all refreshed in one loop)
- Full refresh updates materialized table
- DuckLake snapshots created correctly
- Subsequent refreshes use recorded snapshots

**Implementation**:
- PostgreSQL metadata schema (4 core tables - see [Metadata Schema](07-metadata-schema.md#core-tables-phases-1-3))
- DDL parser using `sqlglot`
- Simple polling loop: query `dynamic_tables`, check `target_lag`, refresh if needed
- Full refresh execution (TRUNCATE + INSERT)
- CLI tool: `create`, `list`, `describe`, `drop`
- DuckDB + DuckLake connection setup
- Basic logging

**Worker Logic:**
```python
def single_worker_main():
    while True:
        # Get all tables needing refresh in dependency order
        tables = find_tables_needing_refresh_topological()  # Respects dependencies
        
        # Process all stale tables in one iteration (respecting order)
        # IMPORTANT: Topological order matters for both bootstrap AND incremental
        for table in tables:
            refresh_table_full(table)
        
        time.sleep(60)

def refresh_table_full(table):
    # Check if this is initial load
    snapshots = get_source_snapshots(table.name)
    
    if not snapshots:
        # BOOTSTRAP: No prior snapshots, initial load
        # Just run query and insert - no CDC needed
        # BUT still processed in topological order by outer loop
        execute(f"INSERT INTO {table.name} {table.query_sql}")
        record_current_snapshots(table.name)  # Save for next time
    else:
        # Regular full refresh (Phase 1 always does full)
        execute(f"TRUNCATE {table.name}")
        execute(f"INSERT INTO {table.name} {table.query_sql}")
        update_snapshots(table.name)
```

**Note:** Even in Phase 1, process entire dependency chains per iteration - if table B depends on A, and both need refresh, do A then B in same loop, not separate iterations.

**Bootstrap example:**
If you have: `source → A → B → C` (all new dynamic tables)
- First loop: Processes A, B, C in that order (topological)
- Each does bootstrap (INSERT without CDC)
- After this iteration, all tables initialized correctly

**Deliverable**: Working prototype with manual full refresh

**Scope**: Single process, synchronous execution, no concurrency. Can handle 10-100 dynamic tables on one machine.

---

## Phase 2: Incremental Refresh

**Goal**: Affected keys strategy using CDC for efficient updates

**Tests**:
- Extract GROUP BY columns from query
- **Bootstrap: First refresh skips CDC, does full load**
- Get changes when FK updates (customer 5→7)
- Recompute only affected customers
- Handle INSERT/UPDATE/DELETE correctly
- Cardinality threshold switches to full refresh (>30%)
- N-way join changes detected (test with 3 tables)

**Implementation**:
- Query analyzer (extract GROUP BY via `sqlglot`)
- **Bootstrap detection (no source_snapshots = initial load)**
- DuckLake CDC integration (`table_changes()`)
- Key extraction from preimage/postimage
- Affected keys refresh logic (using TEMP tables)
- Cardinality-based strategy selection (runtime count, no fact/dimension classification)
- N-way join handling (any number of upstream tables)
- Source snapshot tracking for consistency

**Deliverable**: Efficient incremental refresh for aggregations

**Scope**: Still single worker, but 10-100x faster refreshes for large tables

---

## Phase 3: Production Essentials

**Goal**: Reliability, observability, production deployment

**Tests**:
- Failed refresh retries with exponential backoff
- Snapshot isolation for dependent tables
- Deduplication avoids unchanged writes (opt-in)
- Dependency chain A→B→C all refresh in single iteration (not 3 loops)
- Topological sort respects all dependencies
- Metrics exported to Prometheus
- Graceful shutdown cleans up in-progress work

**Implementation**:
- Error handling and retry logic
- Snapshot isolation (FOR SYSTEM_TIME AS OF SNAPSHOT)
- Dependency graph and topological ordering (critical for same-iteration chains)
- Deduplication support (opt-in via `DEDUPLICATION = true`)
- Prometheus metrics exporter
- Transaction consistency guarantees
- Basic monitoring dashboards (Grafana)
- Alerting rules (refresh failures, staleness)
- Docker containerization
- Documentation and runbook

**Dependency Chain Processing:**
```python
def find_tables_needing_refresh_topological():
    # Get all tables needing refresh
    stale_tables = query("""
        SELECT name FROM dynamic_tables 
        WHERE last_refresh_time < NOW() - target_lag
    """)
    
    # Sort by dependency order (topological)
    return topological_sort(stale_tables, dependency_graph)
    # Returns: [source_tables, intermediate_tables, leaf_tables]
    # Process all in ONE iteration
```

**Why this matters:** If A→B→C chain is stale, refresh all three in one loop. Don't wait for next iteration between each level.

**Deliverable**: Production-ready single-worker system

**Scope**: One worker process, robust and observable. Suitable for production use at small-medium scale (<100 dynamic tables, <10TB data).

---

## Phase 4: Distributed Architecture

**Goal**: Horizontal scaling across multiple workers and machines for massive scale

### Phase 4.1: Multi-Worker Coordination

**Tests**:
- Two workers don't claim same table
- Stale claim expires and becomes available
- Worker heartbeat keeps claim alive
- Failed worker's claims released after timeout
- Work distributed evenly across workers
- Metrics show worker utilization

**Implementation**:
- Work claim table with optimistic locking (`INSERT ON CONFLICT`)
- Heartbeat mechanism (UPDATE claim every 30s)
- Claim expiry logic (release after 5min no heartbeat)
- Worker ID assignment (hostname + UUID)
- Kubernetes deployment manifests
- ConfigMap for configuration
- HPA based on custom metrics (pending_refresh_count)
- Worker pool management

**Deliverable**: Multiple workers process different tables concurrently

**Scope**: N workers, each processes one table at a time. Scales to 1000+ dynamic tables.

### Phase 4.2: Parallel Single-Table Refresh

**Goal**: Distribute one large refresh across multiple workers for massive cardinality

**Tests**:
- Coordinator splits 50M affected keys into 4 subtasks
- Workers claim and process subtasks independently
- Results merged atomically into target table
- Coordinator failure releases all subtasks
- Subtask failure retried without reprocessing others
- Only parallelizes when >10M affected keys

**Implementation**:
- `refresh_subtasks` work queue table
- Coordinator mode:
  - Analyzes affected key count
  - Decides to parallelize (>10M keys)
  - Splits work into subtasks (hash-based partitioning)
  - Waits for all subtasks to complete
  - Merges results (UNION temp tables)
  - Commits atomically
  - Cleans up temp tables
- Worker subtask mode:
  - Claims subtask from queue
  - Processes key range independently
  - Writes result to temp table
  - Marks subtask complete
- Hash-based key partitioning
- Modulo-based partitioning (customer_id % 4 = partition)
- Result merging logic (collect all temp tables, DELETE old, INSERT new)
- Temp table management and cleanup
- Adaptive parallelization threshold (configurable)

**Deliverable**: Scale to massive cardinality (100M+ affected keys)

**Scope**: One large refresh can use N workers. Handles extreme dimension changes (e.g., country name affecting 50M customers).

### Phase 4.3: Advanced Scaling Features

**Implementation**:
- Large cardinality strategies:
  - Persistent temp tables for >10M keys
  - Batched processing for >100M keys
  - Automatic fallback to full refresh at >50% affected
- Out-of-core processing configuration:
  - DuckDB memory limits per worker
  - Spill directory configuration
  - Monitoring of spill events
- Resource limits:
  - Memory limits per worker (Kubernetes)
  - CPU limits and requests
  - Disk space monitoring
- Priority-based scheduling:
  - High-priority tables processed first
  - SLA-based prioritization
- Load balancing improvements:
  - Estimate refresh duration
  - Distribute work by expected load
  - Rebalance on worker failure

**Deliverable**: Production-scale distributed system

**Scope**: Fully distributed, handles 100TB+ data lakes, 10,000+ dynamic tables, petabyte-scale refreshes.

---

---

## Future Enhancements

**Alternative Interfaces:**
- DuckDB extension for native DDL (works with DBeaver, DataGrip, duckdb CLI)
- REST API for programmatic access

**Advanced Features:**
- Advanced query optimization
- Partition-aware refresh
- Cross-database dependencies
- Streaming refresh

---

## Technology Stack

```
Language:      Python 3.11+
Metadata DB:   PostgreSQL 15+
Data Lake:     DuckDB 0.10+ with DuckLake extension
SQL Parsing:   sqlglot
Testing:       pytest, testcontainers
Orchestration: Kubernetes
Metrics:       Prometheus
```

---

## Development Workflow

Each phase follows this pattern:

1. **Write failing tests** for feature
2. **Implement** minimum code to pass tests
3. **Refactor** for clarity and performance
4. **Document** behavior and decisions
5. **Review** and iterate

Focus on one feature at a time, ensure it works before moving on.
