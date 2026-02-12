# Metadata Schema

PostgreSQL database schema for dynamic table metadata. Core tables (Phases 1-3) support single-worker, Phase 4 extensions enable distributed architecture.

## Core Tables (Phases 1-3)

### dynamic_tables

```sql
CREATE TABLE dynamic_tables (
    name VARCHAR PRIMARY KEY,
    schema_name VARCHAR DEFAULT 'dynamic',
    query_sql TEXT NOT NULL,
    target_lag INTERVAL NOT NULL,
    group_by_columns TEXT[],
    refresh_strategy VARCHAR DEFAULT 'AFFECTED_KEYS',
    deduplicate BOOLEAN DEFAULT FALSE,
    cardinality_threshold FLOAT DEFAULT 0.3,
    status VARCHAR DEFAULT 'ACTIVE',  -- ACTIVE, PAUSED, FAILED
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_tables_status ON dynamic_tables(status);
```

### source_snapshots

Tracks last processed CDC snapshot for each source table.

```sql
CREATE TABLE source_snapshots (
    dynamic_table VARCHAR,
    source_table VARCHAR,
    last_snapshot BIGINT NOT NULL,
    last_processed_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (dynamic_table, source_table),
    FOREIGN KEY (dynamic_table) REFERENCES dynamic_tables(name) ON DELETE CASCADE
);
```

**Bootstrap:** No rows for a table means initial load needed (skip CDC).

### dependencies

```sql
CREATE TABLE dependencies (
    downstream VARCHAR,
    upstream VARCHAR,
    PRIMARY KEY (downstream, upstream),
    FOREIGN KEY (downstream) REFERENCES dynamic_tables(name) ON DELETE CASCADE
);

CREATE INDEX idx_dependencies_downstream ON dependencies(downstream);
CREATE INDEX idx_dependencies_upstream ON dependencies(upstream);
```

### refresh_history

```sql
CREATE TABLE refresh_history (
    id BIGSERIAL PRIMARY KEY,
    dynamic_table VARCHAR NOT NULL,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    status VARCHAR NOT NULL,  -- SUCCESS, FAILED
    strategy_used VARCHAR,  -- FULL, AFFECTED_KEYS
    rows_affected BIGINT,
    duration_ms BIGINT,
    error_message TEXT,
    source_snapshots JSONB,
    FOREIGN KEY (dynamic_table) REFERENCES dynamic_tables(name) ON DELETE CASCADE
);

CREATE INDEX idx_history_table ON refresh_history(dynamic_table);
CREATE INDEX idx_history_started ON refresh_history(started_at);
```

## Phase 4.1: Multi-Worker Tables

### pending_refreshes

Work queue for workers to claim.

```sql
CREATE TABLE pending_refreshes (
    id SERIAL PRIMARY KEY,
    dynamic_table VARCHAR UNIQUE NOT NULL,
    next_refresh_due TIMESTAMP NOT NULL,
    priority INT DEFAULT 0,
    enqueued_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (dynamic_table) REFERENCES dynamic_tables(name) ON DELETE CASCADE
);

CREATE INDEX idx_pending_due ON pending_refreshes(next_refresh_due);
CREATE INDEX idx_pending_priority ON pending_refreshes(priority DESC, next_refresh_due);
```

### refresh_claims

Active work claims with heartbeat for liveness detection.

```sql
CREATE TABLE refresh_claims (
    dynamic_table VARCHAR PRIMARY KEY,
    worker_id VARCHAR NOT NULL,
    claimed_at TIMESTAMP NOT NULL,
    heartbeat_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    mode VARCHAR DEFAULT 'single',  -- 'single', 'coordinator' (Phase 4.2)
    subtasks_total INT,
    subtasks_completed INT DEFAULT 0,
    FOREIGN KEY (dynamic_table) REFERENCES dynamic_tables(name) ON DELETE CASCADE
);

CREATE INDEX idx_claims_heartbeat ON refresh_claims(heartbeat_at);
CREATE INDEX idx_claims_expires ON refresh_claims(expires_at);
```

**Claim pattern:** `INSERT ... ON CONFLICT DO NOTHING` for optimistic locking.

## Phase 4.2: Parallel Refresh Tables

### refresh_subtasks

Subtask queue for distributing one large refresh across workers.

```sql
CREATE TABLE refresh_subtasks (
    id SERIAL PRIMARY KEY,
    parent_refresh_id INT NOT NULL REFERENCES pending_refreshes(id) ON DELETE CASCADE,
    dynamic_table VARCHAR NOT NULL,
    subtask_type VARCHAR NOT NULL,  -- 'hash_range', 'modulo', 'partition'
    subtask_data JSONB NOT NULL,
    status VARCHAR DEFAULT 'pending',
    result_location VARCHAR,  -- Temp table path
    claimed_by VARCHAR,
    claimed_at TIMESTAMP,
    heartbeat_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    retry_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_subtasks_pending ON refresh_subtasks(status, created_at) WHERE status = 'pending';
CREATE INDEX idx_subtasks_parent ON refresh_subtasks(parent_refresh_id);
CREATE INDEX idx_subtasks_heartbeat ON refresh_subtasks(heartbeat_at) WHERE status = 'claimed';
```

### Phase 4.2 Columns

```sql
ALTER TABLE dynamic_tables 
ADD COLUMN allow_parallel_refresh BOOLEAN DEFAULT TRUE,
ADD COLUMN parallel_threshold INT DEFAULT 10000000,  -- Min keys to parallelize
ADD COLUMN max_parallelism INT DEFAULT 16;           -- Max workers for one table
```

## Key Queries

**Find stale tables (Phases 1-3):**
```sql
SELECT name FROM dynamic_tables dt
LEFT JOIN (SELECT dynamic_table, MAX(completed_at) as last_refresh
           FROM refresh_history WHERE status = 'SUCCESS' GROUP BY dynamic_table) h
ON dt.name = h.dynamic_table
WHERE dt.status = 'ACTIVE' AND (h.last_refresh IS NULL OR h.last_refresh + dt.target_lag < NOW());
```

**Claim work (Phase 4.1):**
```sql
SELECT p.dynamic_table FROM pending_refreshes p
LEFT JOIN refresh_claims c ON p.dynamic_table = c.dynamic_table
WHERE c.dynamic_table IS NULL OR c.heartbeat_at < NOW() - INTERVAL '5 minutes'
ORDER BY p.priority DESC, p.next_refresh_due LIMIT 1 FOR UPDATE SKIP LOCKED;
```

**Cleanup stale claims:**
```sql
DELETE FROM refresh_claims WHERE heartbeat_at < NOW() - INTERVAL '5 minutes';
```

## Summary by Phase

| Phase | Tables Needed |
|-------|---------------|
| 1-3 (Single worker) | `dynamic_tables`, `source_snapshots`, `dependencies`, `refresh_history` (4 total) |
| 4.1 (Multi-worker) | + `pending_refreshes`, `refresh_claims` (6 total) |
| 4.2 (Parallel refresh) | + `refresh_subtasks` + 3 columns in `dynamic_tables` (7 total) |
