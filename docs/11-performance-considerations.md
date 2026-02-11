# Performance Considerations

## Core Principle: Push Computation to DuckDB

**Python is for orchestration only. All heavy computation runs in DuckDB.**

The dynamic table system involves processing large datasets, but Python doesn't need to be slow because:
- DuckDB is extremely fast (millions of rows/second)
- All key extraction, filtering, aggregation happens **in SQL**
- Python just executes queries and coordinates workflow

## Affected Keys Extraction: SQL, Not Python Loops

**❌ WRONG: Extract keys in Python**
```python
def extract_affected_keys_slow(changes):
    """DON'T DO THIS - processes rows in Python"""
    affected_keys = set()
    for row in changes:
        if row['change_type'] in ['update_preimage', 'delete']:
            affected_keys.add(row['customer_id'])  # Slow Python loop
        if row['change_type'] in ['update_postimage', 'insert']:
            affected_keys.add(row['customer_id'])  # Slow Python loop
    return affected_keys
```

**✅ CORRECT: Extract keys in SQL**
```python
def extract_affected_keys_fast(conn, table_name, group_by_cols, last_snapshot, current_snapshot):
    """All processing in DuckDB - millions of rows/second"""
    
    # Single SQL query extracts all affected keys
    key_columns = ', '.join(group_by_cols)
    
    # TEMP table goes to memory catalog, not DuckLake (no snapshot overhead)
    sql = f"""
    CREATE TEMP TABLE affected_keys AS
    SELECT DISTINCT {key_columns}
    FROM table_changes('{table_name}', {last_snapshot}, {current_snapshot})
    WHERE 
        change_type IN ('update_preimage', 'delete')
        OR change_type IN ('update_postimage', 'insert')
    """
    
    # DuckDB returns result - Python never touches individual rows
    result = conn.execute(sql).fetchall()
    return result  # Returns tuples of key values
```

**Performance:**
- ❌ Python loop: ~100K rows/second
- ✅ DuckDB query: ~10M rows/second (100x faster)
- ✅ TEMP tables: In-memory (fast), no DuckLake snapshot created

## Complete Refresh Flow: All SQL

```python
def refresh_affected_keys(conn, dynamic_table, last_snapshot, current_snapshot):
    """Efficient refresh - Python just orchestrates SQL queries"""
    
    table_name = dynamic_table.source_table
    key_cols = dynamic_table.group_by_columns  # ['customer_id']
    key_list = ', '.join(key_cols)
    
    # Step 1: Extract affected keys (runs in DuckDB)
    # TEMP table uses memory catalog, not DuckLake (no snapshot overhead)
    affected_keys_sql = f"""
    CREATE TEMP TABLE affected_keys AS
    SELECT DISTINCT {key_list}
    FROM table_changes('{table_name}', {last_snapshot}, {current_snapshot})
    """
    conn.execute(affected_keys_sql)
    
    # Step 2: Build WHERE clause for affected keys
    # For single key: WHERE customer_id IN (SELECT customer_id FROM affected_keys)
    # For composite key: WHERE (customer_id, product_id) IN (SELECT customer_id, product_id FROM affected_keys)
    
    if len(key_cols) == 1:
        where_clause = f"WHERE {key_cols[0]} IN (SELECT {key_cols[0]} FROM affected_keys)"
    else:
        key_tuple = f"({key_list})"
        where_clause = f"WHERE {key_tuple} IN (SELECT {key_list} FROM affected_keys)"
    
    # Step 3: Transactional refresh (all in DuckDB)
    conn.execute("BEGIN TRANSACTION")
    
    try:
        # Delete old aggregates
        conn.execute(f"""
            DELETE FROM {dynamic_table.name}
            {where_clause}
        """)
        
        # Recompute affected keys
        conn.execute(f"""
            INSERT INTO {dynamic_table.name}
            SELECT {dynamic_table.select_clause}
            FROM {dynamic_table.from_clause}
            {where_clause}  -- Filter source data by affected keys
            GROUP BY {key_list}
        """)
        
        conn.execute("COMMIT")
        
    except Exception as e:
        conn.execute("ROLLBACK")
        raise
    
    # Clean up temp table
    conn.execute("DROP TABLE affected_keys")
```

**What Python does:**
- Build SQL strings
- Execute queries
- Handle transactions
- Error handling

**What DuckDB does (the heavy work):**
- Scan CDC data
- Extract distinct keys
- Filter source data
- Compute aggregates
- Transactional writes

## Multi-Table Joins: Also SQL

**Dimension changes - join graph filtering:**

```python
def refresh_with_dimension_changes(conn, dynamic_table, last_snapshot, current_snapshot):
    """Handle orderlines JOIN products - both in SQL"""
    
    # Extract affected product_ids from dimension table
    # TEMP tables are in-memory, no DuckLake storage overhead
    conn.execute(f"""
        CREATE TEMP TABLE affected_products AS
        SELECT DISTINCT product_id
        FROM table_changes('products', {last_snapshot}, {current_snapshot})
    """)
    
    # Extract affected order_ids from fact table
    conn.execute(f"""
        CREATE TEMP TABLE affected_orders AS
        SELECT DISTINCT order_id
        FROM table_changes('orderlines', {last_snapshot}, {current_snapshot})
    """)
    
    # Find all orderlines that join to affected products
    conn.execute(f"""
        CREATE TEMP TABLE affected_orderline_keys AS
        SELECT DISTINCT ol.customer_id, ol.product_id
        FROM orderlines ol
        WHERE 
            ol.product_id IN (SELECT product_id FROM affected_products)
            OR ol.order_id IN (SELECT affected_orders)
    """)
    
    # Transactional refresh using affected keys
    conn.execute("BEGIN TRANSACTION")
    
    conn.execute(f"""
        DELETE FROM customer_product_summary
        WHERE (customer_id, product_id) IN (
            SELECT customer_id, product_id FROM affected_orderline_keys
        )
    """)
    
    conn.execute(f"""
        INSERT INTO customer_product_summary
        SELECT 
            ol.customer_id,
            ol.product_id,
            p.product_name,  -- Current dimension value
            COUNT(*),
            SUM(ol.quantity * ol.price)
        FROM orderlines ol
        JOIN products p ON ol.product_id = p.product_id
        WHERE (ol.customer_id, ol.product_id) IN (
            SELECT customer_id, product_id FROM affected_orderline_keys
        )
        GROUP BY ol.customer_id, ol.product_id, p.product_name
    """)
    
    conn.execute("COMMIT")
    
    # Cleanup
    conn.execute("DROP TABLE affected_products")
    conn.execute("DROP TABLE affected_orders")
    conn.execute("DROP TABLE affected_orderline_keys")
```

**All DuckDB. Python just orchestrates.**

## Cardinality Checks: SQL

```python
def should_use_full_refresh(conn, table_name, last_snapshot, current_snapshot, threshold=0.3):
    """Check if affected rows exceed threshold - query runs in DuckDB"""
    
    result = conn.execute(f"""
        WITH 
        total AS (SELECT COUNT(*) as cnt FROM {table_name}),
        affected AS (
            SELECT COUNT(*) as cnt 
            FROM table_changes('{table_name}', {last_snapshot}, {current_snapshot})
        )
        SELECT 
            affected.cnt as affected_rows,
            total.cnt as total_rows,
            affected.cnt::FLOAT / total.cnt as ratio
        FROM affected, total
    """).fetchone()
    
    affected_rows, total_rows, ratio = result
    
    if ratio > threshold:
        logger.info(f"Cardinality check: {ratio:.2%} affected, using full refresh")
        return True
    else:
        logger.info(f"Cardinality check: {ratio:.2%} affected, using incremental")
        return False
```

## SQL Parsing: Acceptable Python Overhead

**The only "heavy" Python operation:**

```python
def parse_dynamic_table_query(sql: str):
    """Parse SQL to extract GROUP BY columns - acceptable overhead"""
    import sqlglot
    
    # This runs once per dynamic table at creation time
    # Not in the hot path (refresh loop)
    parsed = sqlglot.parse_one(sql)
    
    # Extract GROUP BY
    group_by = []
    for expr in parsed.find_all(sqlglot.exp.Group):
        group_by.extend([col.sql() for col in expr.expressions])
    
    # Extract source tables
    tables = [table.name for table in parsed.find_all(sqlglot.exp.Table)]
    
    return {
        'group_by_columns': group_by,
        'source_tables': tables,
        'original_query': sql
    }
```

**Why this is fine:**
- Runs once at `CREATE DYNAMIC TABLE` time
- Not in the refresh hot path
- Query parsing is fast (<1ms for typical queries)
- Result is cached in metadata database

## Metadata Operations: PostgreSQL is Fast Enough

```python
def claim_pending_refresh(pg_conn, worker_id):
    """PostgreSQL handles work queue - Python just issues SQL"""
    
    # Optimistic locking via SQL
    result = pg_conn.execute("""
        INSERT INTO refresh_claims (dynamic_table_id, worker_id, claimed_at)
        SELECT id, %s, NOW()
        FROM pending_refreshes
        WHERE id NOT IN (SELECT dynamic_table_id FROM refresh_claims)
        ORDER BY priority DESC, created_at ASC
        LIMIT 1
        ON CONFLICT (dynamic_table_id) DO NOTHING
        RETURNING dynamic_table_id
    """, [worker_id]).fetchone()
    
    return result[0] if result else None
```

**PostgreSQL work queue performance:**
- Can handle 1000s of claims/second
- No Python bottleneck - just executing queries

## Where Python IS the Right Tool

1. **Orchestration logic:**
   ```python
   def worker_loop():
       while True:
           claim = claim_pending_refresh(pg_conn, worker_id)
           if claim:
               execute_refresh(claim)
           else:
               time.sleep(poll_interval)
   ```

2. **Error handling and retry:**
   ```python
   try:
       refresh_dynamic_table(table_id)
   except TemporaryError as e:
       reschedule_with_backoff(table_id, attempt_count)
   except PermanentError as e:
       mark_failed(table_id, error=str(e))
       alert_operator(table_id)
   ```

3. **Configuration and deployment:**
   - Environment variable handling
   - Kubernetes API integration
   - Metrics export (Prometheus)
   - Logging and observability

## When to Consider Go/Rust/Java

**Only if:**
- Profiling shows Python orchestration is the bottleneck (unlikely)
- Need <10ms latency for refresh triggering (unlikely)
- Memory usage becomes issue (unlikely with proper streaming)

**Stay with Python if:**
- SQL queries are the bottleneck (they will be)
- Worker count can scale horizontally (it can)
- Development velocity matters (it does)

## Benchmarking Strategy

**Before changing language, measure:**

```python
import time

def profile_refresh(conn, dynamic_table):
    start = time.time()
    
    # Measure SQL execution
    t1 = time.time()
    affected_keys = extract_affected_keys_fast(conn, ...)
    sql_extract_time = time.time() - t1
    
    t2 = time.time()
    refresh_affected_keys(conn, ...)
    sql_refresh_time = time.time() - t2
    
    total_time = time.time() - start
    
    print(f"""
    Total: {total_time:.3f}s
    - SQL extract: {sql_extract_time:.3f}s ({sql_extract_time/total_time:.1%})
    - SQL refresh: {sql_refresh_time:.3f}s ({sql_refresh_time/total_time:.1%})
    - Python overhead: {total_time - sql_extract_time - sql_refresh_time:.3f}s
    """)
```

**Expected result:**
- 95%+ time in SQL execution (DuckDB)
- <5% Python overhead (acceptable)

## Optimization Checklist

Before reconsidering language:

- [ ] All key extraction in SQL (not Python loops)
- [ ] All filtering in SQL (WHERE clauses on temp tables)
- [ ] All aggregations in SQL (GROUP BY in DuckDB)
- [ ] Temp tables used for intermediate results
- [ ] Cardinality checks in SQL
- [ ] JOIN graph analysis in SQL
- [ ] Profile actual refresh operations
- [ ] Check DuckDB is using parallelism (set threads)
- [ ] Verify queries are using indexes/statistics

## DuckDB Performance Tuning

```python
def configure_duckdb_for_performance(conn):
    """Ensure DuckDB uses all available resources"""
    
    # Use all CPU cores
    conn.execute("SET threads TO 8")  # or multiprocessing.cpu_count()
    
    # Increase memory limit for large operations
    conn.execute("SET memory_limit = '8GB'")
    
    # Allow spilling to disk for large datasets (out-of-core processing)
    conn.execute("SET temp_directory = '/tmp/duckdb'")
    
    # Enable parallelism for queries
    conn.execute("SET enable_object_cache = true")
    
    # Optimize for OLAP workloads
    conn.execute("SET default_null_order = 'nulls_last'")
```

**Out-of-core processing**: DuckDB automatically spills to disk when memory limit reached. This means affected_keys tables with millions of rows work seamlessly - just slower when spilled.

See [Large Cardinality Handling](13-large-cardinality-handling.md) for strategies when affected keys don't fit in memory.

## Expected Performance

**With SQL-based approach:**

- Extract affected keys: **1M+ keys/second**
- Filter source data: **10M+ rows/second scanned**
- Compute aggregates: **5M+ rows/second grouped**
- Transactional write: **1M+ rows/second inserted**

**Bottlenecks (in order):**
1. Source table scan (network if remote storage)
2. Aggregate computation (CPU-bound in DuckDB)
3. DuckLake snapshot write (I/O bound)
4. PostgreSQL metadata update (negligible)
5. Python orchestration (negligible)

## Conclusion

**Keep Python, push computation to SQL.**

Python is the right choice IF:
- All heavy computation is in DuckDB (SQL queries)
- Python just orchestrates, doesn't process data
- Profiling confirms Python overhead <5%

This is an **analytical workload**, not a request-response service. DuckDB is purpose-built for this. The language running DuckDB matters very little.

**Switch languages only after profiling proves Python orchestration is the bottleneck (it won't be).**
