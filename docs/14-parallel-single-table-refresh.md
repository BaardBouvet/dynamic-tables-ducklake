# Parallel Single-Table Refresh

**Phase 4.2 Feature**

## Overview

Distribute one large table refresh across multiple workers when affected key count exceeds threshold.

**Problem:** A single dimension change (e.g., country name) affects 100M customers. One worker takes too long or runs out of memory.

**Solution:** Coordinator splits work into subtasks, workers process in parallel, results merged atomically.

## When to Parallelize

**Automatic decision based on cardinality:**

```python
def should_parallelize(affected_keys_count, available_workers):
    """Decide if parallelization is worth the coordination overhead"""
    
    # Only for massive cardinality
    if affected_keys_count < 10_000_000:
        return False  # Single worker faster (no coordination overhead)
    
    # Only if enough workers available
    if available_workers < 2:
        return False  # Need at least 2 workers to parallelize
    
    # Don't over-parallelize
    ideal_workers = min(
        affected_keys_count // 5_000_000,  # ~5M keys per worker
        available_workers,
        16  # Max 16-way parallelism (diminishing returns)
    )
    
    return ideal_workers >= 2
```

**Typical thresholds:**
- <10M keys: Single worker
- 10M-50M keys: 2-4 workers
- 50M-100M keys: 4-8 workers
- >100M keys: 8-16 workers or full refresh

## Architecture

### Role Separation

**Coordinator:**
- Claims table-level refresh from `pending_refreshes`
- Analyzes cardinality and decides to parallelize
- Creates subtasks in `refresh_subtasks` table
- Waits for all subtasks to complete
- Merges results
- Commits final result
- Cleans up

**Subtask Worker:**
- Claims subtask from `refresh_subtasks` table
- Processes key range independently
- Writes result to temp table
- Marks subtask complete
- Does NOT commit (coordinator does that)

### Schema

```sql
-- NEW: Subtask work queue
CREATE TABLE refresh_subtasks (
    id SERIAL PRIMARY KEY,
    parent_refresh_id INT NOT NULL REFERENCES pending_refreshes(id) ON DELETE CASCADE,
    dynamic_table VARCHAR NOT NULL,
    subtask_type VARCHAR NOT NULL,  -- 'hash_range', 'modulo', 'partition'
    subtask_data JSONB NOT NULL,    -- Strategy-specific parameters
    status VARCHAR DEFAULT 'pending',  -- pending, claimed, completed, failed
    result_location VARCHAR,        -- Path to temp table with results
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

-- UPDATED: Track coordinator mode
ALTER TABLE refresh_claims 
ADD COLUMN mode VARCHAR DEFAULT 'single',  -- 'single', 'coordinator', 'subtask'
ADD COLUMN parent_refresh_id INT REFERENCES pending_refreshes(id),
ADD COLUMN subtasks_total INT,
ADD COLUMN subtasks_completed INT DEFAULT 0;
```

## Workflow

### 1. Coordinator Initialization

```python
def execute_table_refresh(refresh):
    """Main entry point - decides single vs parallel"""
    
    # Analyze affected keys
    affected_keys_count = count_affected_keys(refresh)
    available_workers = count_idle_workers()
    
    if should_parallelize(affected_keys_count, available_workers):
        logger.info(f"{refresh.table}: {affected_keys_count:,} keys affected - parallelizing")
        coordinate_parallel_refresh(refresh, affected_keys_count)
    else:
        logger.info(f"{refresh.table}: {affected_keys_count:,} keys affected - single worker")
        standard_refresh(refresh)

def coordinate_parallel_refresh(refresh, affected_keys_count):
    """Coordinator: orchestrate parallel refresh"""
    
    # 1. Update claim to coordinator mode
    pg_conn.execute("""
        UPDATE refresh_claims
        SET mode = 'coordinator'
        WHERE dynamic_table = %s AND worker_id = %s
    """, [refresh.table, worker_id])
    
    # 2. Create subtasks
    num_workers = decide_worker_count(affected_keys_count)
    subtasks = create_subtasks(refresh, num_workers)
    
    subtask_ids = []
    for subtask in subtasks:
        result = pg_conn.execute("""
            INSERT INTO refresh_subtasks 
            (parent_refresh_id, dynamic_table, subtask_type, subtask_data)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """, [refresh.id, refresh.table, subtask.type, json.dumps(subtask.data)])
        subtask_ids.append(result.fetchone()[0])
    
    logger.info(f"Created {len(subtask_ids)} subtasks")
    
    # 3. Update claim with subtask count
    pg_conn.execute("""
        UPDATE refresh_claims
        SET subtasks_total = %s
        WHERE dynamic_table = %s AND worker_id = %s
    """, [len(subtask_ids), refresh.table, worker_id])
    
    # 4. Wait for completion
    deadline = time.time() + 3600  # 1 hour timeout
    while time.time() < deadline:
        # Check progress
        result = pg_conn.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE status = 'completed') as completed,
                COUNT(*) FILTER (WHERE status = 'failed') as failed,
                COUNT(*) FILTER (WHERE status = 'pending') as pending,
                COUNT(*) FILTER (WHERE status = 'claimed') as in_progress
            FROM refresh_subtasks
            WHERE parent_refresh_id = %s
        """, [refresh.id]).fetchone()
        
        completed, failed, pending, in_progress = result
        
        # Update progress
        pg_conn.execute("""
            UPDATE refresh_claims
            SET subtasks_completed = %s
            WHERE dynamic_table = %s
        """, [completed, refresh.table])
        
        if failed > 0:
            raise RefreshError(f"{failed} subtasks failed")
        
        if completed == len(subtask_ids):
            logger.info(f"All {len(subtask_ids)} subtasks completed")
            break
        
        logger.debug(f"Progress: {completed}/{len(subtask_ids)} completed, {in_progress} in progress, {pending} pending")
        time.sleep(5)  # Poll every 5 seconds
    else:
        raise TimeoutError(f"Subtasks did not complete within timeout")
    
    # 5. Merge results
    merge_subtask_results(refresh, subtask_ids)
    
    # 6. Cleanup
    cleanup_subtasks(refresh.id)
```

### 2. Subtask Creation Strategies

**Strategy 1: Hash-Based Range Partitioning**

```python
def create_subtasks_hash_range(refresh, num_workers):
    """Partition keys by hash ranges"""
    
    key_column = refresh.group_by_columns[0]  # Assume single key for now
    
    subtasks = []
    for i in range(num_workers):
        subtasks.append(Subtask(
            type='hash_range',
            data={
                'key_column': key_column,
                'partition': i,
                'total_partitions': num_workers,
                'method': 'hash'  # hash(key) % total_partitions = partition
            }
        ))
    
    return subtasks

# Subtask execution:
# SELECT ... WHERE hash(customer_id) % 4 = 2  -- Worker processes partition 2
```

**Strategy 2: Modulo Partitioning**

```python
def create_subtasks_modulo(refresh, num_workers):
    """Partition keys by modulo"""
    
    key_column = refresh.group_by_columns[0]
    
    subtasks = []
    for i in range(num_workers):
        subtasks.append(Subtask(
            type='modulo',
            data={
                'key_column': key_column,
                'partition': i,
                'total_partitions': num_workers
            }
        ))
    
    return subtasks

# Subtask execution:
# SELECT ... WHERE customer_id % 4 = 2
```

**Strategy 3: Source Partition-Based**

```python
def create_subtasks_from_source_partitions(refresh):
    """Use existing source table partitions"""
    
    # Query DuckLake partition metadata
    partitions = duck_conn.execute(f"""
        SELECT DISTINCT hive_partition
        FROM duckdb_tables()
        WHERE table_name = '{refresh.source_table}'
    """).fetchall()
    
    subtasks = []
    for partition in partitions:
        subtasks.append(Subtask(
            type='partition',
            data={
                'partition_spec': partition[0]  # e.g., 'year=2024/month=01'
            }
        ))
    
    return subtasks

# Subtask execution:
# SELECT ... FROM orders WHERE year=2024 AND month=1 AND customer_id IN (affected_keys)
```

### 3. Subtask Execution

```python
def execute_subtask(subtask):
    """Worker processes one subtask"""
    
    # Claim subtask
    claimed = pg_conn.execute("""
        UPDATE refresh_subtasks
        SET status = 'claimed',
            claimed_by = %s,
            claimed_at = NOW(),
            heartbeat_at = NOW()
        WHERE id = %s AND status = 'pending'
        RETURNING id
    """, [worker_id, subtask.id]).fetchone()
    
    if not claimed:
        return  # Someone else claimed it
    
    try:
        # Parse subtask data
        subtask_data = json.loads(subtask.subtask_data)
        
        # Build filtering predicate
        if subtask.subtask_type == 'hash_range':
            filter_predicate = f"""
                hash({subtask_data['key_column']}) % {subtask_data['total_partitions']} = {subtask_data['partition']}
            """
        elif subtask.subtask_type == 'modulo':
            filter_predicate = f"""
                {subtask_data['key_column']} % {subtask_data['total_partitions']} = {subtask_data['partition']}
            """
        elif subtask.subtask_type == 'partition':
            filter_predicate = f"""
                {subtask_data['partition_spec']}  -- e.g., year=2024 AND month=1
            """
        
        # Create temp table for results
        temp_table_name = f"subtask_result_{subtask.id}_{uuid4().hex[:8]}"
        
        # Execute query with partition filter
        duck_conn.execute(f"""
            CREATE TABLE lake.temp.{temp_table_name} AS
            SELECT {refresh.select_clause}
            FROM {refresh.from_clause}
            WHERE {filter_predicate}
              AND {refresh.key_column} IN (
                  SELECT {refresh.key_column} FROM affected_keys  -- Master list
              )
            GROUP BY {refresh.group_by_clause}
        """)
        
        # Mark complete
        pg_conn.execute("""
            UPDATE refresh_subtasks
            SET status = 'completed',
                result_location = %s,
                completed_at = NOW()
            WHERE id = %s
        """, [f"lake.temp.{temp_table_name}", subtask.id])
        
        logger.info(f"Subtask {subtask.id} completed: {temp_table_name}")
        
    except Exception as e:
        # Mark failed
        pg_conn.execute("""
            UPDATE refresh_subtasks
            SET status = 'failed',
                error_message = %s,
                retry_count = retry_count + 1
            WHERE id = %s
        """, [str(e), subtask.id])
        
        logger.error(f"Subtask {subtask.id} failed: {e}")
        raise
```

### 4. Result Merging

```python
def merge_subtask_results(refresh, subtask_ids):
    """Coordinator merges all subtask results into final table"""
    
    # Get all result locations
    results = pg_conn.execute("""
        SELECT result_location
        FROM refresh_subtasks
        WHERE parent_refresh_id = %s AND status = 'completed'
        ORDER by id
    """, [refresh.id]).fetchall()
    
    temp_tables = [row[0] for row in results]
    
    logger.info(f"Merging {len(temp_tables)} subtask results")
    
    # Atomic transaction
    duck_conn.execute("BEGIN TRANSACTION")
    
    try:
        # Step 1: Delete old aggregates for all affected keys
        # (Affected keys list already exists from coordinator's initial analysis)
        duck_conn.execute(f"""
            DELETE FROM {refresh.table}
            WHERE {refresh.key_column} IN (
                SELECT {refresh.key_column} FROM affected_keys
            )
        """)
        
        # Step 2: Insert all subtask results
        for temp_table in temp_tables:
            duck_conn.execute(f"""
                INSERT INTO {refresh.table}
                SELECT * FROM {temp_table}
            """)
        
        duck_conn.execute("COMMIT")
        
        logger.info(f"Merged results committed to {refresh.table}")
        
    except Exception as e:
        duck_conn.execute("ROLLBACK")
        logger.error(f"Merge failed: {e}")
        raise
```

### 5. Cleanup

```python
def cleanup_subtasks(refresh_id):
    """Clean up temp tables and subtask records"""
    
    # Get all temp table names
    temp_tables = pg_conn.execute("""
        SELECT result_location
        FROM refresh_subtasks
        WHERE parent_refresh_id = %s AND result_location IS NOT NULL
    """, [refresh_id]).fetchall()
    
    # Drop temp tables
    for temp_table in temp_tables:
        try:
            duck_conn.execute(f"DROP TABLE IF EXISTS {temp_table[0]}")
        except Exception as e:
            logger.warning(f"Failed to drop {temp_table[0]}: {e}")
    
    # Delete subtask records
    pg_conn.execute("""
        DELETE FROM refresh_subtasks
        WHERE parent_refresh_id = %s
    """, [refresh_id])
    
    logger.info(f"Cleaned up subtasks for refresh {refresh_id}")
```

## Worker Main Loop Enhancement

```python
def worker_main_loop():
    """Enhanced main loop supporting both table-level and subtask work"""
    
    while True:
        try:
            # Priority 1: Try to claim table-level refresh
            table_refresh = claim_pending_refresh(pg_conn, worker_id)
            
            if table_refresh:
                logger.info(f"Claimed table refresh: {table_refresh.table}")
                execute_table_refresh(table_refresh)
                continue
            
            # Priority 2: Try to claim subtask
            subtask = claim_pending_subtask(pg_conn, worker_id)
            
            if subtask:
                logger.info(f"Claimed subtask: {subtask.id} for {subtask.table}")
                execute_subtask(subtask)
                continue
            
            # No work available
            time.sleep(poll_interval)
            
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
            break
        except Exception as e:
            logger.error(f"Worker error: {e}", exc_info=True)
            time.sleep(error_backoff)

def claim_pending_subtask(pg_conn, worker_id):
    """Try to claim a subtask"""
    
    result = pg_conn.execute("""
        SELECT id, dynamic_table, subtask_type, subtask_data
        FROM refresh_subtasks
        WHERE status = 'pending'
        ORDER BY created_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    """).fetchone()
    
    if not result:
        return None
    
    subtask_id, table, subtask_type, subtask_data = result
    
    # Claim it
    pg_conn.execute("""
        UPDATE refresh_subtasks
        SET status = 'claimed',
            claimed_by = %s,
            claimed_at = NOW(),
            heartbeat_at = NOW()
        WHERE id = %s
    """, [worker_id, subtask_id])
    
    pg_conn.commit()
    
    return Subtask(
        id=subtask_id,
        table=table,
        subtask_type=subtask_type,
        subtask_data=subtask_data
    )
```

## Failure Handling

### Coordinator Failure

**Problem:** Coordinator crashes after creating subtasks but before merging.

**Solution:**
```python
def cleanup_orphaned_subtasks():
    """Background job: clean up subtasks from failed coordinators"""
    
    # Find subtasks whose parent refresh claim has expired
    pg_conn.execute("""
        DELETE FROM refresh_subtasks
        WHERE parent_refresh_id IN (
            SELECT pr.id
            FROM pending_refreshes pr
            LEFT JOIN refresh_claims rc ON pr.dynamic_table = rc.dynamic_table
            WHERE rc.dynamic_table IS NULL  -- No active claim
               OR rc.heartbeat_at < NOW() - INTERVAL '5 minutes'  -- Stale claim
        )
    """)
    
    # Cleanup is automatic via ON DELETE CASCADE when claim expires
```

### Subtask Worker Failure

**Problem:** Worker crashes while processing subtask.

**Solution:** Subtask claim expiry (same as table-level claims)
```python
def expire_stale_subtask_claims():
    """Mark subtasks claimed by dead workers as pending"""
    
    pg_conn.execute("""
        UPDATE refresh_subtasks
        SET status = 'pending',
            claimed_by = NULL,
            retry_count = retry_count + 1
        WHERE status = 'claimed'
          AND heartbeat_at < NOW() - INTERVAL '5 minutes'
          AND retry_count < 3  -- Max 3 retries
    """)
    
    # Mark as failed after 3 retries
    pg_conn.execute("""
        UPDATE refresh_subtasks
        SET status = 'failed',
            error_message = 'Max retries exceeded'
        WHERE status = 'claimed'
          AND heartbeat_at < NOW() - INTERVAL '5 minutes'
          AND retry_count >= 3
    """)
```

## Performance Characteristics

**Overhead:**
- Coordination: ~5-10 seconds (create subtasks, wait loop)
- Temp table creation: ~1 second per worker
- Merge: ~1-5 seconds (INSERT from multiple tables)

**Total overhead:** ~15-30 seconds

**Breakeven:** Only worth it if single-worker would take >2 minutes

**Speedup:**
- 2 workers: 1.6-1.8x (coordination overhead)
- 4 workers: 2.5-3.5x
- 8 workers: 4-6x
- 16 workers: 6-10x (diminishing returns)

## Configuration

```python
# Per-table configuration
class DynamicTableConfig:
    allow_parallel_refresh: bool = True  # Enable/disable
    parallel_threshold: int = 10_000_000  # Min affected keys to parallelize
    max_parallelism: int = 16  # Max workers to use
    
# Global configuration
PARALLEL_REFRESH_ENABLED = True
PARALLEL_MIN_AFFECTED_KEYS = 10_000_000
PARALLEL_MAX_WORKERS = 16
PARALLEL_TIMEOUT_SECONDS = 3600
```

## Monitoring

```python
# Prometheus metrics
parallel_refreshes_total = Counter('parallel_refreshes_total', 'Number of parallel refreshes')
subtasks_created = Counter('subtasks_created', 'Number of subtasks created')
subtasks_completed = Counter('subtasks_completed', 'Number of subtasks completed')
subtasks_failed = Counter('subtasks_failed', 'Number of subtasks failed')
parallel_refresh_duration = Histogram('parallel_refresh_seconds', 'Duration of parallel refresh')
subtask_duration = Histogram('subtask_seconds', 'Duration of subtask execution')
coordinator_wait_time = Histogram('coordinator_wait_seconds', 'Time coordinator spends waiting')
```

## Testing

```python
def test_parallel_refresh_with_50m_keys():
    """Test parallel refresh with massive cardinality"""
    
    # Create source data (50M rows, 25M customers)
    create_large_source_table(rows=50_000_000, customers=25_000_000)
    
    # Trigger change affecting 10M customers
    trigger_dimension_change(affected_customers=10_000_000)
    
    # Start 4 workers + 1 coordinator
    workers = [start_worker() for _ in range(4)]
    coordinator = start_worker()  # One will become coordinator
    
    # Trigger refresh
    trigger_refresh('customer_metrics')
    
    # Wait for completion
    wait_for_refresh_complete(timeout=600)
    
    # Verify
    assert refresh_completed_successfully()
    assert subtask_count() == 4
    assert all_subtasks_completed()
    assert results_correct()
    
    # Performance
    duration = get_refresh_duration()
    assert duration < 300  # Should complete in <5 minutes with parallelism

def test_coordinator_failure_cleanup():
    """Test subtasks cleaned up when coordinator fails"""
    
    # Create subtasks
    coordinator = start_coordinator()
    wait_for_subtasks_created()
    
    # Kill coordinator
    coordinator.kill()
    
    # Wait for claim to expire
    time.sleep(310)  # 5min + buffer
    
    # Verify subtasks cleaned up
    assert subtask_count() == 0
    assert refresh_available_for_retry()
```

## Best Practices

1. **Only parallelize large cardinality:** <10M keys not worth overhead
2. **Limit parallelism:** >16 workers has diminishing returns
3. **Monitor coordinator wait time:** Indicates worker saturation
4. **Use hash partitioning by default:** Best distribution for unknown key distribution
5. **Cleanup aggressively:** Temp tables accumulate quickly
6. **Set timeouts:** Prevent coordinator from waiting forever
7. **Cap retries:** Failed subtasks shouldn't retry indefinitely

## Limitations

- **Composite keys:** Currently assumes single key column (can be extended)
- **Coordinator is SPOF:** If coordinator fails, subtasks orphaned (mitigated by cleanup)
- **Memory overhead:** Each worker holds affected_keys list (can use persistent temp)
- **Network transfer:** Results transferred via DuckLake storage (S3 costs)

## Future Enhancements

- Composite key support (hash on multiple columns)
- Dynamic worker allocation (request more workers if falling behind)
- Streaming merge (don't wait for all subtasks, merge as they complete)
- Cost-based optimization (estimate cost of parallel vs single)
- Partition pruning (skip partitions with no affected keys)
