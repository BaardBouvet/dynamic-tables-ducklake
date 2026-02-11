# Large Cardinality Handling

## Problem

What if affected keys don't fit in memory?

**Scenario:**
- Country name change affects 100M customers
- `affected_keys` would need to hold 100M customer_ids
- Might exceed available RAM

## Solution 1: DuckDB Out-of-Core Processing (Default)

**DuckDB automatically spills to disk when memory pressure detected:**

```python
def configure_duckdb_for_large_datasets(conn):
    """Configure DuckDB to handle datasets larger than RAM"""
    
    # Set memory limit (DuckDB spills beyond this)
    conn.execute("SET memory_limit = '8GB'")
    
    # Set temp directory for spill
    conn.execute("SET temp_directory = '/tmp/duckdb'")
    
    # Enable parallelism
    conn.execute("SET threads TO 8")
```

**How it works:**
- TEMP tables start in memory
- When memory limit reached, DuckDB spills to disk automatically
- Queries continue seamlessly (just slower)
- No code changes needed

**Performance:**
- In-memory: Full speed
- Spilled: Slower but works (limited by disk I/O)
- Still faster than reprocessing entire table

**This is the default behavior - no special handling needed.**

## Solution 2: Avoid Materializing Keys (Subquery Pattern)

**Use affected keys as a subquery instead of temp table:**

```sql
BEGIN TRANSACTION;

-- Define affected_keys as inline query
DELETE FROM customer_metrics
WHERE customer_id IN (
    SELECT DISTINCT customer_id
    FROM table_changes('orders', 42, 43)
);

INSERT INTO customer_metrics
SELECT customer_id, COUNT(*), SUM(amount)
FROM orders
WHERE customer_id IN (
    SELECT DISTINCT customer_id
    FROM table_changes('orders', 42, 43)
)
GROUP BY customer_id;

COMMIT;
```

**Trade-off:**
- ❌ Executes `table_changes()` twice (might be expensive)
- ✅ No materialization of keys
- ✅ DuckDB can optimize the entire plan together

**DuckDB optimizer might deduplicate the subquery automatically.**

## Solution 3: Use Persistent Temp Table in DuckLake

**When memory is constrained, use DuckLake table instead:**

```python
def refresh_with_persistent_temp_keys(conn, dynamic_table, last_snapshot, current_snapshot):
    """Use DuckLake table for affected keys when too large for memory"""
    
    # Create in dedicated temp schema
    conn.execute("CREATE SCHEMA IF NOT EXISTS lake.temp")
    
    # Store affected keys in DuckLake (uses Parquet, efficient)
    conn.execute(f"""
        CREATE OR REPLACE TABLE lake.temp.affected_keys AS
        SELECT DISTINCT customer_id
        FROM lake.table_changes('orders', {last_snapshot}, {current_snapshot})
    """)
    
    # Use for refresh
    conn.execute("BEGIN TRANSACTION")
    
    conn.execute(f"""
        DELETE FROM lake.customer_metrics
        WHERE customer_id IN (SELECT customer_id FROM lake.temp.affected_keys)
    """)
    
    conn.execute(f"""
        INSERT INTO lake.customer_metrics
        SELECT customer_id, COUNT(*), SUM(amount)
        FROM lake.orders
        WHERE customer_id IN (SELECT customer_id FROM lake.temp.affected_keys)
        GROUP BY customer_id
    """)
    
    conn.execute("COMMIT")
    
    # Cleanup - drop temp table
    conn.execute("DROP TABLE lake.temp.affected_keys")
```

**Benefits:**
- Handles arbitrarily large key sets (Parquet storage)
- DuckDB still optimized for Parquet scanning
- Explicit cleanup (or periodic cleanup of temp schema)

**Drawback:**
- Creates DuckLake snapshot for temp table (extra metadata)
- Need manual cleanup

## Solution 4: Batch Processing

**For extreme cases (billions of keys), process in batches:**

```python
def refresh_in_batches(conn, dynamic_table, last_snapshot, current_snapshot, batch_size=1_000_000):
    """Process affected keys in batches"""
    
    # Get total count first
    total_count = conn.execute(f"""
        SELECT COUNT(DISTINCT customer_id)
        FROM table_changes('orders', {last_snapshot}, {current_snapshot})
    """).fetchone()[0]
    
    num_batches = (total_count + batch_size - 1) // batch_size
    logger.info(f"Processing {total_count} keys in {num_batches} batches")
    
    for batch_num in range(num_batches):
        offset = batch_num * batch_size
        
        # Process one batch at a time
        conn.execute("BEGIN TRANSACTION")
        
        try:
            # Get batch of keys
            conn.execute(f"""
                CREATE TEMP TABLE batch_keys AS
                SELECT DISTINCT customer_id
                FROM table_changes('orders', {last_snapshot}, {current_snapshot})
                ORDER BY customer_id
                LIMIT {batch_size} OFFSET {offset}
            """)
            
            # Delete batch
            conn.execute(f"""
                DELETE FROM customer_metrics
                WHERE customer_id IN (SELECT customer_id FROM batch_keys)
            """)
            
            # Insert batch
            conn.execute(f"""
                INSERT INTO customer_metrics
                SELECT customer_id, COUNT(*), SUM(amount)
                FROM orders
                WHERE customer_id IN (SELECT customer_id FROM batch_keys)
                GROUP BY customer_id
            """)
            
            conn.execute("COMMIT")
            conn.execute("DROP TABLE batch_keys")
            
            logger.info(f"Batch {batch_num + 1}/{num_batches} complete")
            
        except Exception as e:
            conn.execute("ROLLBACK")
            conn.execute("DROP TABLE IF EXISTS batch_keys")
            raise
```

**Trade-offs:**
- ✅ Bounded memory usage per batch
- ✅ Progress visibility
- ❌ Multiple transactions (consumers see partial updates between batches)
- ❌ Slower (multiple passes through source data)
- ❌ More complex code

**Only use if:**
- Memory truly constrained (tested with actual data)
- DuckDB spilling is too slow
- Willing to accept partial visibility

## Solution 5: Cardinality-Based Strategy Selection

**Automatically choose strategy based on affected key count:**

```python
def choose_refresh_strategy(conn, table_name, last_snapshot, current_snapshot):
    """Intelligent strategy selection based on cardinality"""
    
    # Count distinct affected keys
    affected_count_query = f"""
        SELECT COUNT(DISTINCT customer_id) as affected_keys
        FROM table_changes('{table_name}', {last_snapshot}, {current_snapshot})
    """
    affected_keys = conn.execute(affected_count_query).fetchone()[0]
    
    # Count total rows
    total_count_query = f"SELECT COUNT(*) FROM {table_name}"
    total_rows = conn.execute(total_count_query).fetchone()[0]
    
    ratio = affected_keys / total_rows if total_rows > 0 else 0
    
    # Decision tree
    if ratio > 0.5:
        logger.info(f"Affected: {ratio:.1%} - using FULL refresh")
        return 'FULL_REFRESH'
    
    elif affected_keys > 10_000_000:  # 10M keys
        logger.info(f"Affected keys: {affected_keys:,} - using PERSISTENT_TEMP")
        return 'PERSISTENT_TEMP'
    
    elif affected_keys > 1_000_000:  # 1M keys
        logger.info(f"Affected keys: {affected_keys:,} - using OUT_OF_CORE (may spill)")
        return 'OUT_OF_CORE'
    
    else:
        logger.info(f"Affected keys: {affected_keys:,} - using IN_MEMORY")
        return 'IN_MEMORY'

def refresh_with_adaptive_strategy(conn, dynamic_table, last_snapshot, current_snapshot):
    """Adaptive refresh based on cardinality"""
    
    strategy = choose_refresh_strategy(conn, 
                                       dynamic_table.source_table,
                                       last_snapshot, 
                                       current_snapshot)
    
    if strategy == 'FULL_REFRESH':
        full_refresh(conn, dynamic_table)
    
    elif strategy == 'PERSISTENT_TEMP':
        refresh_with_persistent_temp_keys(conn, dynamic_table, last_snapshot, current_snapshot)
    
    else:  # IN_MEMORY or OUT_OF_CORE (same code, DuckDB handles spilling)
        refresh_affected_keys_standard(conn, dynamic_table, last_snapshot, current_snapshot)
```

**Thresholds:**
- <1M keys: In-memory TEMP table (fast)
- 1M-10M keys: TEMP table with spilling (acceptable)
- >10M keys: Persistent temp table or full refresh
- >50% of table: Full refresh (faster than incremental)

## Solution 6: CTE with Materialization Hint

**Force DuckDB to materialize or not materialize:**

```sql
-- Force materialization (like TEMP table)
WITH affected_keys AS MATERIALIZED (
    SELECT DISTINCT customer_id
    FROM table_changes('orders', 42, 43)
)
DELETE FROM customer_metrics
WHERE customer_id IN (SELECT customer_id FROM affected_keys);

-- Then reuse... wait, can't reuse across statements

-- Alternative: Use transaction-scoped temp table
BEGIN TRANSACTION;

CREATE TEMP TABLE affected_keys AS
SELECT DISTINCT customer_id
FROM table_changes('orders', 42, 43);

-- Can reuse within transaction
DELETE FROM customer_metrics WHERE customer_id IN (SELECT customer_id FROM affected_keys);
INSERT INTO customer_metrics SELECT ... WHERE customer_id IN (SELECT customer_id FROM affected_keys);

COMMIT;
```

## Recommended Approach

**Start with default (Solution 1):**

```python
def refresh_affected_keys(conn, dynamic_table, last_snapshot, current_snapshot):
    """Default implementation - relies on DuckDB out-of-core"""
    
    # DuckDB handles spilling automatically
    conn.execute(f"""
        CREATE TEMP TABLE affected_keys AS
        SELECT DISTINCT {key_columns}
        FROM table_changes('{source_table}', {last_snapshot}, {current_snapshot})
    """)
    
    conn.execute("BEGIN TRANSACTION")
    
    conn.execute(f"""
        DELETE FROM {target_table}
        WHERE {key_columns} IN (SELECT {key_columns} FROM affected_keys)
    """)
    
    conn.execute(f"""
        INSERT INTO {target_table}
        SELECT ...
        FROM {source_table}
        WHERE {key_columns} IN (SELECT {key_columns} FROM affected_keys)
        GROUP BY {key_columns}
    """)
    
    conn.execute("COMMIT")
    conn.execute("DROP TABLE affected_keys")
```

**Monitor and optimize if needed:**

```python
class RefreshMetrics:
    affected_keys_count: int
    memory_spill_events: int  # Track if spilling happens
    refresh_duration_seconds: float

# Alert if spilling frequently
if metrics.memory_spill_events > 0:
    logger.warning(f"Memory spill detected: {metrics.memory_spill_events} events")
    # Consider: increase memory_limit or use persistent temp
```

**Only implement batching/persistent temp after profiling shows it's necessary.**

## When to Use Each Solution

| Scenario | Affected Keys | Recommended Solution |
|----------|---------------|---------------------|
| Normal operation | <1M | Default (TEMP table) |
| Large update | 1M-10M | Default (DuckDB spills) |
| Very large update | 10M-100M | Persistent temp table |
| Massive update | >100M or >50% of table | Full refresh |
| Memory-constrained env | Any large | Set memory_limit, allow spilling |
| Batch processing OK | >10M | Batched refresh |

## DuckDB Memory Configuration

```python
def configure_memory_for_environment(conn, environment='production'):
    """Configure DuckDB memory based on deployment environment"""
    
    if environment == 'production':
        # 16GB worker pods
        conn.execute("SET memory_limit = '12GB'")  # Leave room for overhead
        conn.execute("SET temp_directory = '/tmp/duckdb-spill'")
        conn.execute("SET threads = 8")
    
    elif environment == 'development':
        # Laptop with 8GB RAM
        conn.execute("SET memory_limit = '4GB'")
        conn.execute("SET temp_directory = './tmp'")
        conn.execute("SET threads = 4")
    
    elif environment == 'memory-constrained':
        # Small worker (4GB RAM)
        conn.execute("SET memory_limit = '2GB'")
        conn.execute("SET temp_directory = '/tmp/duckdb-spill'")
        conn.execute("SET max_temp_directory_size = '100GB'")  # Allow large spill
        conn.execute("SET threads = 2")
```

## Monitoring

```python
def log_memory_usage(conn, operation: str):
    """Log DuckDB memory usage for monitoring"""
    
    # Query DuckDB internal stats
    stats = conn.execute("""
        SELECT 
            current_setting('memory_limit') as memory_limit,
            current_setting('temp_directory') as temp_dir
    """).fetchone()
    
    logger.info(f"{operation}: memory_limit={stats[0]}, temp_dir={stats[1]}")
    
    # Check temp directory size (spill indicator)
    import os
    temp_dir = stats[1]
    if os.path.exists(temp_dir):
        size = sum(os.path.getsize(f) for f in os.listdir(temp_dir))
        if size > 0:
            logger.warning(f"DuckDB spilled to disk: {size / 1e9:.2f} GB")
            metrics.record('duckdb.memory_spill_gb', size / 1e9)
```

## Testing Large Cardinality

```python
def test_large_cardinality_refresh():
    """Test refresh with millions of affected keys"""
    
    # Setup: 100M rows, 50M customers
    conn.execute("""
        CREATE TABLE orders AS
        SELECT 
            row_number() OVER () as order_id,
            (random() * 50000000)::INT as customer_id,
            random() * 1000 as amount
        FROM range(100000000)
    """)
    
    # Simulate: 10M customers affected (20% of unique customers)
    # ... trigger changes ...
    
    # Monitor refresh
    start = time.time()
    refresh_affected_keys(conn, dynamic_table, last_snapshot, current_snapshot)
    duration = time.time() - start
    
    # Verify
    logger.info(f"Refreshed 10M keys in {duration:.1f}s")
    assert duration < 300  # Should complete in <5 minutes
```

## Best Practices

1. **Start simple**: Use default TEMP table approach
2. **Monitor memory**: Track spill events in production
3. **Set limits**: Configure `memory_limit` for environment
4. **Use cardinality checks**: Fall back to full refresh if >50% affected
5. **Persistent temp only if needed**: Profile first, optimize second
6. **Batch as last resort**: Adds complexity, partial visibility issues
7. **Test with realistic data**: 10M, 100M, 1B row scenarios

## Conclusion

**DuckDB's out-of-core processing handles large cardinality by default.**

No code changes needed for most cases. Only optimize if profiling shows:
- Frequent memory spills causing slowness
- Spill directory filling up
- Refresh times exceeding SLA

Then apply adaptive strategy selection based on actual cardinality metrics.
