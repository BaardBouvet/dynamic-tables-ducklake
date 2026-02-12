# Parallel Single-Table Refresh

**Phase 4.2 Feature**

Distribute one large table refresh across multiple workers when affected key count exceeds threshold (10M+ keys). Coordinator splits work, workers process in parallel, results merged atomically.

## When to Parallelize

| Affected Keys | Workers | Strategy |
|--------------|---------|----------|
| <10M | 1 | Single worker (coordination overhead not worth it) |
| 10M-50M | 2-4 | Parallel |
| 50M-100M | 4-8 | Parallel |
| >100M | 8-16 or FULL | Parallel or full refresh |

**Decision logic:** Target ~5M keys per worker, max 16 workers (diminishing returns), min 2 workers to parallelize.

## Architecture

**Coordinator:**
- Claims table refresh from `pending_refreshes`
- Creates subtasks in `refresh_subtasks`
- Waits for completion
- Merges results atomically
- Cleans up temp tables

**Subtask Worker:**
- Claims subtask from `refresh_subtasks`
- Processes key range independently
- Writes to temp table
- Marks complete (coordinator commits final result)

### Schema

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
    retry_count INT DEFAULT 0
);

ALTER TABLE refresh_claims 
ADD COLUMN mode VARCHAR DEFAULT 'single',  -- 'single', 'coordinator', 'subtask'
ADD COLUMN subtasks_total INT,
ADD COLUMN subtasks_completed INT DEFAULT 0;
```

## Partitioning Strategies

| Strategy | How It Works | Use Case |
|----------|-------------|----------|
| **hash_range** | `hash(key) % N = partition_id` | Default - good distribution for unknown key patterns |
| **modulo** | `key % N = partition_id` | Integer keys with even distribution |
| **partition** | Use existing source table partitions | Hive-partitioned sources (date/region/etc) |

Each worker processes only rows matching its partition filter + affected keys list.

## Workflow

**1. Coordinator decides to parallelize:**
```python
num_workers = min(affected_keys // 5_000_000, 16)
for i in range(num_workers):
    create_subtask(type='hash_range', partition=i, total=num_workers)
```

**2. Workers claim and execute subtasks:**
```python
filter = f"hash({key_col}) % {total} = {partition} AND {key_col} IN (affected_keys)"
CREATE TABLE temp_{subtask_id} AS SELECT ... FROM ... WHERE {filter} GROUP BY ...
```

**3. Coordinator merges results:**
```python
DELETE FROM target WHERE key IN (affected_keys)
for temp_table in subtask_results:
    INSERT INTO target SELECT * FROM temp_table
COMMIT
```

**4. Cleanup:** Drop temp tables, delete subtask records.
## Worker Main Loop

Workers try to claim table-level refreshes first (priority 1), then subtasks (priority 2):

```python
while True:
    table_refresh = claim_pending_refresh()
    if table_refresh:
        execute_table_refresh(table_refresh)
        continue
    
    subtask = claim_pending_subtask()
    if subtask:
        execute_subtask(subtask)
        continue
    
    time.sleep(poll_interval)
```

## Failure Handling

**Coordinator failure:** Subtasks cleaned up via `ON DELETE CASCADE` when claim expires.

**Subtask worker failure:** Stale claims reset to `pending` after 5min timeout, max 3 retries.

## Performance

**Overhead:** ~15-30s (coordination, temp tables, merge)  
**Breakeven:** Only worth it if single-worker would take >2min

| Workers | Speedup |
|---------|---------|
| 2 | 1.6-1.8x |
| 4 | 2.5-3.5x |
| 8 | 4-6x |
| 16 | 6-10x (diminishing returns) |

## Configuration

```python
PARALLEL_MIN_AFFECTED_KEYS = 10_000_000
PARALLEL_MAX_WORKERS = 16
PARALLEL_TIMEOUT_SECONDS = 3600
```

## Best Practices

- Only parallelize when affected keys >10M (overhead not worth it otherwise)
- Limit to 16 workers max (diminishing returns)
- Use hash partitioning by default (good distribution)
- Set aggressive timeouts (prevent coordinator waiting forever)
- Cap retries at 3 (failed subtasks shouldn't loop)
- Clean up temp tables immediately (accumulate quickly)

## Limitations

- Single key column only (composite keys not yet supported)
- Coordinator SPOF (mitigated by cleanup)
- Each worker needs full affected_keys list in memory
- Results transferred via DuckLake storage (S3 costs)

## Future Enhancements

- Composite key support
- Dynamic worker allocation
- Streaming merge (don't wait for all subtasks)
- Cost-based optimization
- Partition pruning

