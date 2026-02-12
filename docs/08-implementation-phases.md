# Implementation Phases

Test-driven development with incremental delivery. Phases 1-3 build production-ready single-worker system, Phase 4 adds distributed processing.

## Phase 1: Core Infrastructure

**Goal:** Single worker, full refresh only, CLI interface

**Metadata:** Just 4 tables (see [Metadata Schema](07-metadata-schema.md))

**Key Tests:**
- Parse/validate DDL before creation
- Circular dependency detection
- Bootstrap without CDC (initial load)
- Bootstrap captures snapshots BEFORE query execution
- Dependency chains processed in topological order (A→B→C in one iteration)
- Full refresh updates table correctly

**Implementation:**
- DDL parser (`sqlglot`)
- Polling loop: find stale tables → refresh in dependency order
- Full refresh: `TRUNCATE + INSERT`
- CLI: `create`, `list`, `describe`, `drop`, `validate`
- Bootstrap: capture snapshots before first query, skip CDC

**Deliverable:** Working prototype with manual full refresh

**Scope:** Single process, 10-100 dynamic tables

## Phase 2: Incremental Refresh

**Goal:** CDC-based affected keys strategy

**Key Tests:**
- Bootstrap skips CDC, subsequent refreshes use it
- Extract affected keys from CDC changes
- Recompute only affected keys
- Cardinality threshold (>30% → full refresh)
- N-way joins handled correctly

**Implementation:**
- Query analyzer (extract GROUP BY)
- Query rewriting for snapshot isolation (`FOR SYSTEM_TIME AS OF SNAPSHOT`)
- DuckLake CDC integration (`table_changes()`)
- Affected keys refresh via TEMP tables
- Cardinality-based strategy selection
- Source snapshot tracking

**Deliverable:** Efficient incremental refresh

**Scope:** Still single worker, 10-100x faster for large tables

## Phase 3: Production Essentials

**Goal:** Reliability, observability, deployment

**Key Tests:**
- Retry with exponential backoff
- Snapshot isolation for dependent tables
- Deduplication skips unchanged rows
- Dependency chains refresh in single iteration
- Metrics exported
- Graceful shutdown

**Implementation:**
- Error handling + retry logic
- Schema change detection
- Snapshot isolation
- Topological dependency ordering
- Deduplication (opt-in via `DEDUPLICATION = true`)
- Prometheus metrics
- Docker containerization

**Metrics:**
- `refresh_duration`, `lag_seconds`, `cardinality_ratio`, `dedup_ratio`
- `refresh_total{status}`, `errors_total{error_type}`
- `worker_health`

**Alerts:**
- Lag violations (lag > target * 2)
- Refresh failures (>3 in 15min)
- Worker down

**Deliverable:** Production-ready single-worker system

**Scope:** One worker, <100 tables, <10TB data

## Phase 4: Distributed Architecture

### Phase 4.1: Multi-Worker Coordination

**Goal:** Horizontal scaling across multiple workers

**Key Tests:**
- No duplicate claims
- Claim expiry + heartbeat
- Work distributed evenly

**Implementation:**
- Work claim table with optimistic locking
- Heartbeat mechanism (30s updates)
- Claim expiry (5min timeout)
- Kubernetes deployment + HPA

**Deliverable:** N workers process different tables

**Scope:** 1000+ tables

### Phase 4.2: Parallel Single-Table Refresh

**Goal:** Distribute one large refresh across workers

**Key Tests:**
- Coordinator splits 50M keys into subtasks
- Workers process independently
- Results merged atomically
- Only parallelizes when >10M keys

**Implementation:**
- `refresh_subtasks` table
- Coordinator: analyze → split → wait → merge → cleanup
- Workers: claim subtask → process partition → write temp table
- Hash/modulo partitioning
- Result merging (DELETE + INSERT from temps)

**Deliverable:** Scale to 100M+ affected keys

**Scope:** One refresh uses N workers (see [Parallel Single-Table Refresh](14-parallel-single-table-refresh.md))

### Phase 4.3: Advanced Scaling

**Implementation:**
- Persistent temp tables for >10M keys
- Batched processing for >100M keys
- Memory/CPU limits (Kubernetes)
- Priority-based scheduling
- Load balancing

**Deliverable:** Production-scale distributed system

**Scope:** 100TB+ data, 10,000+ tables

## Future Enhancements

- DuckDB extension for native DDL
- REST API
- Partition-aware refresh
- Cross-database dependencies
- Streaming refresh

## Technology Stack

```
Python 3.11+, PostgreSQL 15+, DuckDB 0.10+ (DuckLake)
sqlglot, pytest, Kubernetes, Prometheus
```

