# Large Cardinality Handling

If affected keys don't fit in memory (e.g., country change affecting 100M customers), use cardinality-based strategy selection.

## Default: DuckDB Out-of-Core Processing

**DuckDB automatically spills to disk when memory limit exceeded.** No special handling needed.

```python
conn.execute("SET memory_limit = '8GB'")
conn.execute("SET temp_directory = '/tmp/duckdb'")
```

TEMP tables start in memory, spill to disk when needed. Still faster than full refresh.

## Cardinality-Based Strategy Selection

```python
def choose_refresh_strategy(affected_keys, total_rows):
    ratio = affected_keys / total_rows
    
    if ratio > 0.5:
        return 'FULL_REFRESH'  # More than half affected? Just rebuild
    elif affected_keys > 10_000_000:
        return 'PERSISTENT_TEMP'  # DuckLake table instead of TEMP
    elif affected_keys > 1_000_000:
        return 'OUT_OF_CORE'  # Allow spilling
    else:
        return 'IN_MEMORY'  # Fast path
```

## Strategies

| Affected Keys | Strategy | Approach |
|---------------|----------|----------|
| <1M | IN_MEMORY | Standard TEMP table |
| 1M-10M | OUT_OF_CORE | TEMP table with DuckDB spilling |
| 10M-100M | PERSISTENT_TEMP | DuckLake table (Parquet) instead of TEMP |
| >100M or >50% | FULL_REFRESH | Faster to rebuild than incremental |

## Persistent Temp Table (10M+ keys)

```python
# Create in DuckLake instead of in-memory temp
conn.execute("""
    CREATE OR REPLACE TABLE lake.temp.affected_keys AS
    SELECT DISTINCT customer_id FROM table_changes('orders', 42, 43)
""")

# Use for refresh
conn.execute("BEGIN TRANSACTION")
conn.execute("DELETE FROM lake.customer_metrics WHERE customer_id IN (SELECT customer_id FROM lake.temp.affected_keys)")
conn.execute("INSERT INTO lake.customer_metrics SELECT ... WHERE customer_id IN (SELECT customer_id FROM lake.temp.affected_keys)")
conn.execute("COMMIT")

# Cleanup
conn.execute("DROP TABLE lake.temp.affected_keys")
```

**Benefits:** Handles arbitrarily large key sets via Parquet storage.  
**Drawback:** Extra DuckLake snapshot, manual cleanup needed.

## Memory Configuration

```python
# Production (16GB worker)
conn.execute("SET memory_limit = '12GB'")  # Leave headroom
conn.execute("SET temp_directory = '/tmp/duckdb-spill'")
conn.execute("SET threads = 8")

# Development (8GB laptop)
conn.execute("SET memory_limit = '4GB'")
conn.execute("SET threads = 4")
```

## Monitoring

Track memory spill events:

```python
if os.path.exists(temp_dir) and size > 0:
    logger.warning(f"DuckDB spilled to disk: {size / 1e9:.2f} GB")
    metrics.record('duckdb.memory_spill_gb', size / 1e9)
```

## Best Practices

- Start with default TEMP table approach (DuckDB handles spilling)
- Monitor spill events in production
- Use cardinality checks to fall back to full refresh (>50%)
- Persistent temp only if profiling shows spilling is too slow
- Test with realistic data (10M, 100M rows)

## Conclusion

**DuckDB's out-of-core processing handles most cases automatically.** Only optimize if profiling shows frequent spills causing SLA violations.

