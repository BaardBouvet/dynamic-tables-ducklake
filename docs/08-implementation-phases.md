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
- **Validate DDL before creation**: Syntax, source tables exist, unsupported features
- **Circular dependency detection**: Reject cycles at CREATE time
- Store/retrieve table metadata
- Single worker polls and processes work
- **Initial load does full refresh without CDC (bootstrap)**
- **Bootstrap snapshot timing**: Capture snapshots BEFORE query execution
- **Bootstrap respects dependency order (A→B→C processed in sequence)**
- Dependency chains processed in same iteration (A→B→C all refreshed in one loop)
- Full refresh updates materialized table
- DuckLake snapshots created correctly
- Subsequent refreshes use recorded snapshots

**Implementation**:
- PostgreSQL metadata schema (4 core tables - see [Metadata Schema](07-metadata-schema.md#core-tables-phases-1-3))
- DDL parser using `sqlglot`
- **DDL validation**: Verify syntax, sources exist, extract GROUP BY
- **Circular dependency prevention**: Topological sort validation before CREATE
- **Configuration management**: CLI args, environment variables, validation
- Simple polling loop: query `dynamic_tables`, check `target_lag`, refresh if needed
- Full refresh execution (TRUNCATE + INSERT)
- CLI tool: `create`, `list`, `describe`, `drop`, **`validate`**
- DuckDB + DuckLake connection setup
- **Startup validation**: Database connectivity, config sanity checks
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
        # CRITICAL: Capture snapshots BEFORE running query
        source_tables = get_source_tables(table.name)
        snapshot_map = {src: get_current_snapshot(src) for src in source_tables}
        
        # Just run query and insert - no CDC needed
        # BUT still processed in topological order by outer loop
        execute(f"INSERT INTO {table.name} {table.query_sql}")
        
        # Record the snapshots we actually used
        for src, snap in snapshot_map.items():
            record_source_snapshot(table.name, src, snap)
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
- **Query rewriting for snapshot isolation** (inject `FOR SYSTEM_TIME AS OF SNAPSHOT` clauses)
- **AST manipulation** using sqlglot for safe query modification
- **Bootstrap detection (no source_snapshots = initial load)**
- DuckLake CDC integration (`table_changes()`)
- Key extraction from preimage/postimage
- Affected keys refresh logic (using TEMP tables)
- Cardinality-based strategy selection (runtime count, no fact/dimension classification)
- **Cardinality threshold tuning** per table
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
- Error handling and retry logic with exponential backoff
- **Schema change detection** and graceful failure
- **Partial chain failure handling** (if B fails in A→B→C, skip C)
- Snapshot isolation (FOR SYSTEM_TIME AS OF SNAPSHOT)
- Dependency graph and topological ordering (critical for same-iteration chains)
- **Downstream lag implementation** (refresh when ANY parent refreshes)
- Deduplication support (opt-in via `DEDUPLICATION = true`)
- **Monitoring metrics**: refresh_duration, cardinality_ratio, dedup_ratio, lag_seconds, failures
- Prometheus metrics exporter
- Transaction consistency guarantees
- Basic monitoring dashboards (Grafana)
- Alerting rules (refresh failures, staleness, lag violations)
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

### Monitoring Metrics Specification (Phase 3)

**Core Metrics:**

```python
# Refresh performance
dynamic_table_refresh_duration_seconds{table, strategy}
  # Histogram: How long refreshes take
  # Labels: table name, strategy (FULL, AFFECTED_KEYS)

dynamic_table_refresh_total{table, status}
  # Counter: Total refreshes attempted
  # Labels: table name, status (SUCCESS, FAILED, SKIPPED)

# Staleness tracking
dynamic_table_lag_seconds{table}
  # Gauge: How stale is table (time since last successful refresh)
  # Alert when: lag > target_lag * 2

dynamic_table_target_lag_seconds{table}
  # Gauge: Configured target lag for comparison

# Incremental refresh metrics
dynamic_table_affected_keys_count{table}
  # Histogram: Number of affected keys per refresh
  
dynamic_table_cardinality_ratio{table}
  # Histogram: Ratio of affected/total rows (strategy selection)

# Deduplication metrics  
dynamic_table_dedup_ratio{table}
  # Histogram: Ratio of rows skipped due to no change
  # Only recorded when deduplication enabled

dynamic_table_rows_changed{table}
  # Histogram: Actual rows written after deduplication

# Error tracking
dynamic_table_errors_total{table, error_type}
  # Counter: Errors by type (schema_change, query_error, timeout, etc.)

# Worker health
dynamic_table_worker_health
  # Gauge: 1 if healthy, 0 if unhealthy
  
dynamic_table_poll_duration_seconds
  # Histogram: Time to poll metadata and find work
```

**Prometheus Alerting Rules:**

```yaml
groups:
  - name: dynamic_tables
    interval: 60s
    rules:
      # Lag violations
      - alert: DynamicTableLagging
        expr: |
          dynamic_table_lag_seconds{} > 
          dynamic_table_target_lag_seconds{} * 2
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Dynamic table {{ $labels.table }} lagging behind target"
          description: "Lag is {{ $value }}s, target is {{ $labels.target_lag }}s"
      
      # Refresh failures
      - alert: DynamicTableRefreshFailing
        expr: |
          increase(dynamic_table_refresh_total{status="FAILED"}[15m]) > 3
        labels:
          severity: critical
        annotations:
          summary: "Dynamic table {{ $labels.table }} failing to refresh"
          description: "{{ $value }} refresh failures in last 15 minutes"
      
      # High dedup opportunity
      - alert: HighDedupOpportunity
        expr: |
          avg_over_time(dynamic_table_dedup_ratio{}[7d]) > 0.5
          and on(table) dynamic_table_deduplication_enabled{} == 0
        labels:
          severity: info
        annotations:
          summary: "Consider enabling deduplication for {{ $labels.table }}"
          description: "Average dedup ratio: {{ $value }}"
      
      # Low dedup efficiency
      - alert: LowDedupEfficiency
        expr: |
          avg_over_time(dynamic_table_dedup_ratio{}[7d]) < 0.15
          and on(table) dynamic_table_deduplication_enabled{} == 1
        labels:
          severity: info
        annotations:
          summary: "Consider disabling deduplication for {{ $labels.table }}"
          description: "Average dedup ratio: {{ $value }} (low benefit)"
      
      # Worker down
      - alert: DynamicTableWorkerDown
        expr: dynamic_table_worker_health == 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Dynamic table worker is unhealthy"
```

**Grafana Dashboard Panels:**

```json
{
  "panels": [
    {
      "title": "Refresh Duration by Table",
      "type": "graph",
      "targets": [
        {
          "expr": "histogram_quantile(0.95, dynamic_table_refresh_duration_seconds_bucket{})"
        }
      ]
    },
    {
      "title": "Table Lag vs Target",
      "type": "graph",
      "targets": [
        {
          "expr": "dynamic_table_lag_seconds{}",
          "legendFormat": "{{ table }} actual"
        },
        {
          "expr": "dynamic_table_target_lag_seconds{}",
          "legendFormat": "{{ table }} target"
        }
      ]
    },
    {
      "title": "Strategy Distribution",
      "type": "pie",
      "targets": [
        {
          "expr": "sum by(strategy) (increase(dynamic_table_refresh_total{}[1h]))"
        }
      ]
    },
    {
      "title": "Deduplication Efficiency",
      "type": "graph",
      "targets": [
        {
          "expr": "dynamic_table_dedup_ratio{}"
        }
      ]
    }
  ]
}
```

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
