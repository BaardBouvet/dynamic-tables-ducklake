# Performance Considerations

**Core principle:** Python for orchestration only. All heavy computation in DuckDB.

## Push Computation to SQL

**❌ WRONG - Python loops:**
```python
affected_keys = set()
for row in changes:
    affected_keys.add(row['customer_id'])  # ~100K rows/sec
```

**✅ CORRECT - SQL:**
```python
conn.execute("""
    CREATE TEMP TABLE affected_keys AS
    SELECT DISTINCT customer_id
    FROM table_changes('orders', 42, 43)
""")  # ~10M rows/sec (100x faster)
```

## All Key Operations in SQL

**Affected keys extraction:**
```sql
CREATE TEMP TABLE affected_keys AS
SELECT DISTINCT {key_cols}
FROM table_changes('{table}', {last_snap}, {current_snap})
```

**Cardinality check:**
```sql
WITH total AS (SELECT COUNT(*) as cnt FROM {table}),
     affected AS (SELECT COUNT(*) FROM table_changes(...))
SELECT affected.cnt::FLOAT / total.cnt as ratio FROM affected, total
```

**Transactional refresh:**
```sql
BEGIN TRANSACTION;
DELETE FROM target WHERE key IN (SELECT key FROM affected_keys);
INSERT INTO target SELECT ... WHERE key IN (SELECT key FROM affected_keys) GROUP BY key;
COMMIT;
```

**Multi-table joins:**
```sql
CREATE TEMP TABLE affected_products AS SELECT DISTINCT product_id FROM table_changes('products', ...);
CREATE TEMP TABLE affected_orderlines AS SELECT DISTINCT order_id FROM table_changes('orderlines', ...);
CREATE TEMP TABLE affected_keys AS 
    SELECT DISTINCT customer_id, product_id FROM orderlines 
    WHERE product_id IN (SELECT product_id FROM affected_products) 
       OR order_id IN (SELECT order_id FROM affected_orderlines);
```

All DuckDB. Python just orchestrates.

## Python's Role

**What Python does:**
- Build SQL strings
- Execute queries
- Handle transactions and errors
- Retry logic
- Metrics export

**What Python doesn't do:**
- Process individual rows
- Extract keys in loops
- Filter data
- Compute aggregates

## TEMP Tables are Fast

TEMP tables use memory catalog (not DuckLake), so no snapshot overhead. DuckDB spills to disk automatically when memory limit exceeded.

## Expected Performance

| Operation | Throughput |
|-----------|-----------|
| Extract affected keys | 1M+ keys/sec |
| Scan source data | 10M+ rows/sec |
| Compute aggregates | 5M+ rows/sec |
| Transactional write | 1M+ rows/sec |

**Bottlenecks (in order):**
1. Source table scan (network/I/O)
2. Aggregate computation (CPU in DuckDB)
3. DuckLake snapshot write (I/O)
4. PostgreSQL metadata (~1ms, negligible)
5. Python orchestration (<1ms, negligible)

## DuckDB Configuration

```python
conn.execute("SET threads = 8")  # Use all cores
conn.execute("SET memory_limit = '8GB'")  # Spills beyond this
conn.execute("SET temp_directory = '/tmp/duckdb'")  # Spill location
```

Out-of-core processing: DuckDB automatically spills when needed (see [Large Cardinality Handling](13-large-cardinality-handling.md)).

## Benchmarking

Before changing language, profile:

```python
start = time.time()
# ... refresh operation ...
total = time.time() - start
```

Expected: 95%+ time in SQL, <5% Python overhead.

## When to Consider Other Languages

**Only if profiling shows Python orchestration is the bottleneck (it won't be).**

This is an analytical workload, not request-response. DuckDB's performance matters far more than orchestration language.

## Optimization Checklist

- [ ] All key extraction in SQL
- [ ] All filtering in SQL (WHERE clauses)
- [ ] All aggregations in SQL (GROUP BY)
- [ ] TEMP tables for intermediate results
- [ ] Cardinality checks in SQL
- [ ] DuckDB using all cores (`threads`)
- [ ] Memory limit configured
- [ ] Profile actual refresh operations

**Keep Python, push computation to SQL.**

