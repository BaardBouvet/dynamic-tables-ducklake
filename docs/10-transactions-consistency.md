# Transaction Handling and Consistency

## Overview

All refresh operations are transactional to ensure consumers see consistent data.

**DuckLake provides full ACID support** with snapshot isolation - every `BEGIN-COMMIT` block creates one atomic snapshot. Built-in retry mechanism handles concurrent transaction conflicts automatically.

## The Problem: Partial Reads

**Without transactions:**
```sql
-- Step 1: Delete old aggregates
DELETE FROM customer_metrics WHERE customer_id IN (5, 7);
-- Consumer query here sees MISSING data!

-- Step 2: Insert new aggregates
INSERT INTO customer_metrics SELECT ...;
-- Consumer query here sees CORRECT data
```

**If a consumer queries between DELETE and INSERT, they see:**
- Missing aggregates for customer 5 and 7
- Incorrect totals (other customers present, but these missing)
- Temporary inconsistent state

## The Solution: Transactional Refresh

**All refresh operations wrapped in transactions:**

```sql
BEGIN TRANSACTION;

DELETE FROM customer_metrics WHERE customer_id IN (5, 7);
INSERT INTO customer_metrics 
SELECT customer_id, COUNT(*), SUM(amount)
FROM orders
WHERE customer_id IN (5, 7)
GROUP BY customer_id;

COMMIT;
```

**Guarantees:**
- Consumers see either old data (before COMMIT) OR new data (after COMMIT)
- Never see partial/missing data during refresh
- ACID properties maintained
- **Each transaction creates exactly one DuckLake snapshot** (atomic version increment)

## Implementation

### DuckDB Transaction Isolation

DuckDB supports ACID transactions with MVCC (Multi-Version Concurrency Control):

```python
def refresh_affected_keys(conn, dynamic_table, affected_keys):
    conn.execute("BEGIN TRANSACTION")
    
    try:
        # Delete old aggregates
        conn.execute(f"""
            DELETE FROM {dynamic_table.name}
            WHERE {build_key_predicate(affected_keys)}
        """)
        
        # Insert new aggregates
        conn.execute(f"""
            INSERT INTO {dynamic_table.name}
            {build_refresh_query(dynamic_table, affected_keys)}
        """)
        
        conn.execute("COMMIT")
        
    except Exception as e:
        conn.execute("ROLLBACK")
        raise RefreshError(f"Failed to refresh {dynamic_table.name}") from e
```

### Full Refresh

```python
def full_refresh(conn, dynamic_table):
    conn.execute("BEGIN TRANSACTION")
    
    try:
        # Option 1: TRUNCATE + INSERT
        conn.execute(f"TRUNCATE TABLE {dynamic_table.name}")
        conn.execute(f"INSERT INTO {dynamic_table.name} {dynamic_table.query}")
        
        conn.execute("COMMIT")
        
    except Exception as e:
        conn.execute("ROLLBACK")
        raise RefreshError(f"Failed to refresh {dynamic_table.name}") from e
```

**Alternative: CREATE + SWAP (zero-downtime):**

```python
def full_refresh_with_swap(conn, dynamic_table):
    temp_table = f"{dynamic_table.name}_temp_{uuid4().hex[:8]}"
    
    # Build in temp table (no transaction needed yet)
    conn.execute(f"CREATE TABLE {temp_table} AS {dynamic_table.query}")
    
    # Atomic swap
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(f"DROP TABLE {dynamic_table.name}")
        conn.execute(f"ALTER TABLE {temp_table} RENAME TO {dynamic_table.name}")
        conn.execute("COMMIT")
    except Exception as e:
        conn.execute("ROLLBACK")
        conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
        raise
```

## Isolation Levels

**DuckLake provides snapshot isolation for all transactions:**

- Each transaction sees a consistent snapshot of the database
- Readers never block writers, writers never block readers
- Each `BEGIN-COMMIT` block creates one DuckLake snapshot
- MVCC (Multi-Version Concurrency Control) allows concurrent access

**For dynamic tables:**
```python
# DuckLake provides snapshot isolation by default
conn.execute("BEGIN TRANSACTION")
# All reads within this transaction see the same consistent snapshot
conn.execute("DELETE FROM table WHERE ...")
conn.execute("INSERT INTO table ...")
conn.execute("COMMIT")  # Creates new snapshot atomically
```

## Long-Running Refreshes

**Challenge:** Large refresh transactions can block readers

**Solutions:**

### 1. Batch Commits (for very large updates)

```python
def refresh_large_table(conn, dynamic_table, affected_keys):
    batch_size = 10000
    key_batches = chunk(affected_keys, batch_size)
    
    for batch in key_batches:
        conn.execute("BEGIN TRANSACTION")
        
        # Delete batch
        conn.execute(f"DELETE ... WHERE key IN ({batch})")
        
        # Insert batch
        conn.execute(f"INSERT ... WHERE key IN ({batch})")
        
        conn.execute("COMMIT")
        # Brief moment for readers to query between batches
```

**Trade-off:** Partial updates visible between batches (less ideal but handles huge updates)

### 2. Shadow Table Pattern

```python
def refresh_via_shadow_table(conn, dynamic_table, affected_keys):
    # Create shadow table with old + new data
    conn.execute(f"""
        CREATE TEMP TABLE shadow AS
        SELECT * FROM {dynamic_table.name}
        WHERE {build_key_predicate(affected_keys, inverted=True)}  -- Keep unchanged
    """)
    
    # Add refreshed data
    conn.execute(f"""
        INSERT INTO shadow
        {build_refresh_query(dynamic_table, affected_keys)}
    """)
    
    # Atomic swap
    conn.execute("BEGIN TRANSACTION")
    conn.execute(f"DROP TABLE {dynamic_table.name}")
    conn.execute(f"ALTER TABLE shadow RENAME TO {dynamic_table.name}")
    conn.execute("COMMIT")
```

**Benefit:** Original table readable during recompute, atomic swap at end

### 3. Partitioned Refresh

For partitioned tables, refresh one partition at a time:

```python
def refresh_partitioned_table(conn, dynamic_table, affected_partitions):
    for partition in affected_partitions:
        conn.execute("BEGIN TRANSACTION")
        
        # Refresh single partition
        conn.execute(f"DELETE ... WHERE partition_key = {partition}")
        conn.execute(f"INSERT ... WHERE partition_key = {partition}")
        
        conn.execute("COMMIT")
        # Other partitions remain queryable
```

## Error Handling

### Automatic Retry on Conflicts

**DuckLake has built-in retry mechanism for transaction conflicts:**

```python
# Configure retry behavior (optional - defaults are reasonable)
conn.execute("SET ducklake_max_retry_count = 10")      # Default: 10
conn.execute("SET ducklake_retry_wait_ms = 100")       # Default: 100ms
conn.execute("SET ducklake_retry_backoff = 1.5")       # Default: 1.5x exponential

# DuckLake handles retries automatically
def refresh_with_retry(conn, dynamic_table, affected_keys):
    try:
        conn.execute("BEGIN TRANSACTION")
        # ... refresh logic ...
        conn.execute("COMMIT")
        # DuckLake automatically retries on conflict up to max_retry_count
        
    except Exception as e:
        conn.execute("ROLLBACK")
        raise RefreshError(f"Refresh failed after retries") from e
```

**Custom retry for application-level errors:**

```python
def refresh_with_custom_retry(conn, dynamic_table, affected_keys, max_retries=3):
    """For retrying non-transaction errors (network, etc.)"""
    for attempt in range(max_retries):
        try:
            conn.execute("BEGIN TRANSACTION")
            # ... refresh logic ...
            conn.execute("COMMIT")
            return  # Success
            
        except NetworkError:
            conn.execute("ROLLBACK")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise
```

### Partial Failure Recovery

```python
def refresh_with_savepoint(conn, dynamic_table, key_batches):
    conn.execute("BEGIN TRANSACTION")
    
    try:
        for i, batch in enumerate(key_batches):
            conn.execute(f"SAVEPOINT batch_{i}")
            
            try:
                # Refresh batch
                refresh_batch(conn, dynamic_table, batch)
                conn.execute(f"RELEASE SAVEPOINT batch_{i}")
                
            except Exception as batch_error:
                # Rollback just this batch, try to continue
                conn.execute(f"ROLLBACK TO SAVEPOINT batch_{i}")
                log_error(f"Batch {i} failed: {batch_error}")
                # Decide: continue or abort entire refresh
        
        conn.execute("COMMIT")
        
    except Exception as e:
        conn.execute("ROLLBACK")
        raise
```

## Transaction Monitoring

**Metrics to track:**

```python
class RefreshMetrics:
    transaction_duration_seconds: float
    rows_deleted: int
    rows_inserted: int
    transaction_conflicts: int
    rollbacks: int
```

**Long transaction alerting:**

```python
# Warn if transaction open for >5 minutes
if transaction_duration > 300:
    log_warning(f"Long-running refresh transaction: {dynamic_table.name}")
    alert("Consider using shadow table pattern for large refreshes")
```

## Best Practices

1. **Always use transactions** for DELETE+INSERT pattern
2. **Keep transactions short** when possible (batch for large updates)
3. **Use shadow tables** for very large full refreshes (zero downtime)
4. **Monitor transaction duration** and alert on long-running refreshes
5. **Partition large tables** to enable incremental partition refresh
6. **Use savepoints** for partial failure recovery in complex refreshes

## Test Cases

```python
def test_transactional_consistency():
    # Given: Dynamic table with data
    create_and_populate_dynamic_table()
    
    # When: Refresh starts but doesn't finish
    start_refresh_async()
    time.sleep(0.1)  # Refresh in progress
    
    # Then: Consumers see old data (not partial)
    result = query_dynamic_table()
    assert result == original_data  # Not partial/missing
    
    # When: Refresh completes
    wait_for_refresh()
    
    # Then: Consumers see new data
    result = query_dynamic_table()
    assert result == expected_new_data

def test_rollback_on_error():
    # When: Refresh fails midway
    with pytest.raises(RefreshError):
        refresh_with_simulated_error()
    
    # Then: Original data still intact
    assert query_dynamic_table() == original_data

def test_concurrent_reads_during_refresh():
    # When: Refresh in transaction
    with concurrent_refresh():
        # Readers should not block
        results = [query_dynamic_table() for _ in range(10)]
    
    # All reads succeed (no blocking)
    assert len(results) == 10
```

## DuckLake-Specific Considerations

**DuckLake provides full ACID guarantees:**
- Stores data in Parquet files with metadata tracking
- Each `BEGIN-COMMIT` block = one snapshot (atomic commit)
- Snapshot isolation: readers see consistent point-in-time view
- Built-in retry mechanism for concurrent transaction conflicts

**Transaction support:**
```python
# DuckLake has full ACID transaction support
conn.execute("ATTACH 'ducklake:lake.duckdb' AS lake")
conn.execute("BEGIN TRANSACTION")
conn.execute("DELETE FROM lake.dynamic.customer_metrics WHERE ...")
conn.execute("INSERT INTO lake.dynamic.customer_metrics ...")
conn.execute("COMMIT")
# Creates new snapshot atomically - all or nothing
```

**Snapshot benefits for dynamic tables:**

1. **Consistent reads during refresh:**
   - Readers see snapshot N while refresh writes snapshot N+1
   - No locks, no blocking, perfect read isolation

2. **Time travel for debugging:**
   ```sql
   -- Query dynamic table at specific snapshot
   SELECT * FROM customer_metrics FOR SYSTEM_TIME AS OF SNAPSHOT 42;
   
   -- Query at timestamp
   SELECT * FROM customer_metrics FOR SYSTEM_TIME AS OF TIMESTAMP '2026-02-11 10:30:00';
   ```

3. **Atomic multi-table updates:**
   - One transaction can update multiple dynamic tables
   - All changes commit as single snapshot
   - Dependent tables stay consistent

**Snapshot metadata:**
```python
# Track snapshot IDs in metadata for consistency
conn.execute("SELECT * FROM lake.snapshots()")
# Returns: snapshot_id, timestamp, changeset, author, message

# Get current snapshot
conn.execute("SELECT * FROM lake.current_snapshot()")
# Returns: snapshot_id

# Optional: Add commit message for audit trail
conn.execute("BEGIN TRANSACTION")
conn.execute("INSERT INTO customer_metrics ...")
conn.execute("CALL lake.set_commit_message('dynamic-table-worker', 'Refresh customer_metrics')")
conn.execute("COMMIT")
```

**Configuration for high-concurrency workloads:**
```python
# Increase retry attempts for busy systems
conn.execute("SET ducklake_max_retry_count = 20")
conn.execute("SET ducklake_retry_wait_ms = 200")
conn.execute("SET ducklake_retry_backoff = 2.0")
```
