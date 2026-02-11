# Metadata Schema

PostgreSQL database schema for dynamic table metadata.

**Note:** Schema is divided into core (Phases 1-3) and distributed extensions (Phase 4).

---

## Core Tables (Phases 1-3)

These tables are sufficient for a single-worker system.

### dynamic_tables

Table definitions and configuration.

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
    status VARCHAR DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
```

**Columns:**
- `deduplicate`: When true, compare old vs new aggregates and skip writes if identical
- `cardinality_threshold`: If affected/total ratio > this, use full refresh instead of incremental
- `status`: ACTIVE, PAUSED, FAILED

### source_snapshots

Tracks which snapshot each dynamic table was built from for each source.

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

**Bootstrap handling:** 
- No rows for a dynamic_table = initial load needed
- First refresh does full load from current source state (no CDC)
- After successful refresh, insert snapshot IDs for all source tables
- Subsequent refreshes use CDC from these snapshots

### dependencies

Dependency graph between tables.

```sql
CREATE TABLE dependencies (
    downstream VARCHAR,
    upstream VARCHAR,
    PRIMARY KEY (downstream, upstream),
    FOREIGN KEY (downstream) REFERENCES dynamic_tables(name) ON DELETE CASCADE
);
```

### refresh_history

Audit log of all refresh operations.

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
    source_snapshots JSONB,  -- {table_name: snapshot_id}
    FOREIGN KEY (dynamic_table) REFERENCES dynamic_tables(name) ON DELETE CASCADE
);

CREATE INDEX idx_history_table ON refresh_history(dynamic_table);
CREATE INDEX idx_history_started ON refresh_history(started_at);
```

**Single Worker Usage:**

```python
# Simple polling loop (Phase 1-3)
def single_worker_loop():
    while True:
        # Find tables that need refresh
        tables_to_refresh = pg_conn.execute("""
            SELECT dt.name, dt.target_lag, ss.last_snapshot
            FROM dynamic_tables dt
            LEFT JOIN source_snapshots ss ON dt.name = ss.dynamic_table
            WHERE dt.status = 'ACTIVE'
              AND (
                  ss.last_snapshot IS NULL  -- Never refreshed
                  OR dt.updated_at + dt.target_lag < NOW()  -- Lag exceeded
              )
            ORDER BY dt.created_at
        """).fetchall()
        
        for table in tables_to_refresh:
            try:
                refresh_table(table)
            except Exception as e:
                log_error(table, e)
        
        time.sleep(poll_interval)
```

---

## Phase 4 Extensions: Multi-Worker Coordination

Additional tables needed for distributed architecture.

### pending_refreshes (Phase 4.1)

Work queue for multi-worker system.

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

**Why needed:** Multiple workers compete for work, need central queue.

### refresh_claims (Phase 4.1)

Active work claims by workers with heartbeat.

```sql
CREATE TABLE refresh_claims (
    dynamic_table VARCHAR PRIMARY KEY,
    worker_id VARCHAR NOT NULL,
    claimed_at TIMESTAMP NOT NULL,
    heartbeat_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    mode VARCHAR DEFAULT 'single',  -- 'single', 'coordinator' (Phase 4.2)
    subtasks_total INT,  -- For coordinator mode (Phase 4.2)
    subtasks_completed INT DEFAULT 0,  -- For coordinator mode (Phase 4.2)
    FOREIGN KEY (dynamic_table) REFERENCES dynamic_tables(name) ON DELETE CASCADE
);

CREATE INDEX idx_claims_heartbeat ON refresh_claims(heartbeat_at);
CREATE INDEX idx_claims_expires ON refresh_claims(expires_at);
```

**Why needed:** Prevent two workers from processing same table; detect dead workers.

### refresh_subtasks (Phase 4.2)

Subtask work queue for parallel single-table refresh.

```sql
CREATE TABLE refresh_subtasks (
    id SERIAL PRIMARY KEY,
    parent_refresh_id INT NOT NULL REFERENCES pending_refreshes(id) ON DELETE CASCADE,
    dynamic_table VARCHAR NOT NULL,
    subtask_type VARCHAR NOT NULL,  -- 'hash_range', 'modulo', 'partition'
    subtask_data JSONB NOT NULL,
    status VARCHAR DEFAULT 'pending',
    result_location VARCHAR,
    claimed_by VARCHAR,
    claimed_at TIMESTAMP,
    heartbeat_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    retry_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_subtasks_pending ON refresh_subtasks(status, created_at)
    WHERE status = 'pending';
CREATE INDEX idx_subtasks_parent ON refresh_subtasks(parent_refresh_id);
CREATE INDEX idx_subtasks_heartbeat ON refresh_subtasks(heartbeat_at)
    WHERE status = 'claimed';
```

**Why needed:** Distribute one large refresh across multiple workers.

### Phase 4 Configuration Additions

Add to `dynamic_tables` for Phase 4.2:

```sql
ALTER TABLE dynamic_tables 
ADD COLUMN allow_parallel_refresh BOOLEAN DEFAULT TRUE,
ADD COLUMN parallel_threshold INT DEFAULT 10000000,
ADD COLUMN max_parallelism INT DEFAULT 16;
```

**Why needed:** Control parallel refresh behavior per table.

---

## Migration Path

**Phase 1-3 → Phase 4 Migration:**

```sql
-- Add Phase 4.1 tables
CREATE TABLE pending_refreshes (...);
CREATE TABLE refresh_claims (...);

-- Add Phase 4.2 tables  
CREATE TABLE refresh_subtasks (...);

-- Add Phase 4.2 columns
ALTER TABLE dynamic_tables 
ADD COLUMN allow_parallel_refresh BOOLEAN DEFAULT TRUE,
ADD COLUMN parallel_threshold INT DEFAULT 10000000,
ADD COLUMN max_parallelism INT DEFAULT 16;

-- Populate pending_refreshes from existing tables
INSERT INTO pending_refreshes (dynamic_table, next_refresh_due)
SELECT name, NOW()
FROM dynamic_tables
WHERE status = 'ACTIVE';
```

---

## Indexes (Core Schema)

```sql
-- For dependency resolution
CREATE INDEX idx_dependencies_downstream ON dependencies(downstream);
CREATE INDEX idx_dependencies_upstream ON dependencies(upstream);

-- For status filtering
CREATE INDEX idx_tables_status ON dynamic_tables(status);

-- For history queries
CREATE INDEX idx_history_table ON refresh_history(dynamic_table);
CREATE INDEX idx_history_started ON refresh_history(started_at);
```

## Example Queries (Phases 1-3)

### Find tables needing refresh (single worker)

```sql
-- Simple: Check which tables haven't been refreshed within their lag
SELECT dt.name, dt.target_lag
FROM dynamic_tables dt
LEFT JOIN (
    SELECT dynamic_table, MAX(completed_at) as last_refresh
    FROM refresh_history
    WHERE status = 'SUCCESS'
    GROUP BY dynamic_table
) h ON dt.name = h.dynamic_table
WHERE dt.status = 'ACTIVE'
  AND (
      h.last_refresh IS NULL  -- Never refreshed
      OR h.last_refresh + dt.target_lag < NOW()  -- Lag exceeded
  )
ORDER BY dt.created_at;
```

### Record refresh start

```sql
INSERT INTO refresh_history 
(dynamic_table, started_at, status, source_snapshots)
VALUES ('customer_metrics', NOW(), 'RUNNING', '{"orders": 42}');
```

### Update refresh completion

```sql
UPDATE refresh_history
SET completed_at = NOW(),
    status = 'SUCCESS',
    strategy_used = 'AFFECTED_KEYS',
    rows_affected = 1500,
    duration_ms = 2340
WHERE id = 123;
```

### Get dependency order

```sql
-- Topological sort: tables with no dependencies first
WITH RECURSIVE ordered AS (
    -- Base: tables with no dependencies
    SELECT dt.name, 0 as depth
    FROM dynamic_tables dt
    LEFT JOIN dependencies d ON dt.name = d.downstream
    WHERE d.downstream IS NULL
    
    UNION ALL
    
    -- Recursive: tables depending on already ordered tables
    SELECT d.downstream, o.depth + 1
    FROM dependencies d
    JOIN ordered o ON d.upstream = o.name
)
SELECT name, depth
FROM ordered
ORDER BY depth, name;
```

### Check for circular dependencies

```sql
-- Find cycles in dependency graph
WITH RECURSIVE cycle_check AS (
    SELECT downstream, upstream, ARRAY[downstream, upstream] as path
    FROM dependencies
    
    UNION ALL
    
    SELECT cc.downstream, d.upstream, cc.path || d.upstream
    FROM cycle_check cc
    JOIN dependencies d ON cc.upstream = d.downstream
    WHERE NOT (d.upstream = ANY(cc.path))  -- Avoid infinite recursion
)
SELECT DISTINCT path
FROM cycle_check
WHERE downstream = upstream;  -- Found cycle
```

---

## Phase 4 Example Queries

### Find pending work (multi-worker)

```sql
-- Phase 4.1: Claim table-level work
SELECT p.dynamic_table, p.next_refresh_due
FROM pending_refreshes p
LEFT JOIN refresh_claims c ON p.dynamic_table = c.dynamic_table
WHERE c.dynamic_table IS NULL  -- Not claimed
   OR c.heartbeat_at < NOW() - INTERVAL '5 minutes'  -- Stale claim
ORDER BY p.priority DESC, p.next_refresh_due
LIMIT 1
FOR UPDATE SKIP LOCKED;  -- Prevent race conditions
```

### Claim work (Phase 4.1)

```sql
-- Optimistic locking via INSERT ON CONFLICT
INSERT INTO refresh_claims 
(dynamic_table, worker_id, claimed_at, heartbeat_at, expires_at)
VALUES ('customer_metrics', 'worker-123', NOW(), NOW(), NOW() + INTERVAL '5 minutes')
ON CONFLICT (dynamic_table) DO NOTHING
RETURNING dynamic_table;
```

### Update heartbeat (Phase 4.1)

```sql
UPDATE refresh_claims
SET heartbeat_at = NOW(),
    expires_at = NOW() + INTERVAL '5 minutes'
WHERE dynamic_table = 'customer_metrics' 
  AND worker_id = 'worker-123';
```

### Cleanup expired claims (Phase 4.1)

```sql
DELETE FROM refresh_claims
WHERE heartbeat_at < NOW() - INTERVAL '5 minutes';
```

### Find subtask work (Phase 4.2)

```sql
SELECT id, dynamic_table, subtask_type, subtask_data
FROM refresh_subtasks
WHERE status = 'pending'
ORDER BY created_at
LIMIT 1
FOR UPDATE SKIP LOCKED;
```

### Coordinator: Monitor subtask progress (Phase 4.2)

```sql
SELECT 
    COUNT(*) FILTER (WHERE status = 'completed') as completed,
    COUNT(*) FILTER (WHERE status = 'failed') as failed,
    COUNT(*) FILTER (WHERE status = 'pending') as pending,
    COUNT(*) FILTER (WHERE status = 'claimed') as in_progress
FROM refresh_subtasks
WHERE parent_refresh_id = 123;
```

---

## Summary: What You Need When

### Phases 1-3 (Single Worker)
**4 tables total:**
- `dynamic_tables` - Table definitions
- `source_snapshots` - CDC tracking
- `dependencies` - Dependency graph
- `refresh_history` - Audit log

**No need for:**
- ❌ Work queues
- ❌ Claims/heartbeats
- ❌ Subtasks
- ❌ Worker coordination

**Simple polling pattern:**
```python
while True:
    tables = get_tables_needing_refresh()
    for table in tables:
        refresh(table)
    sleep(poll_interval)
```

### Phase 4.1 (Multi-Worker)
**Add 2 tables:**
- `pending_refreshes` - Work queue
- `refresh_claims` - Active claims with heartbeat

**Enables:**
- ✅ Multiple workers processing different tables
- ✅ Fault tolerance (claim expiry)
- ✅ Horizontal scaling

### Phase 4.2 (Parallel Refresh)
**Add 1 table + 3 columns:**
- `refresh_subtasks` - Subtask queue
- Columns in `dynamic_tables`: `allow_parallel_refresh`, `parallel_threshold`, `max_parallelism`

**Enables:**
- ✅ One table distributed across multiple workers
- ✅ Massive cardinality handling (100M+ keys)
- ✅ Coordinator/worker pattern

