# Affected Keys Refresh Strategy

## Overview

Primary incremental refresh strategy for dynamic tables with GROUP BY aggregations.

## How It Works

1. **Get changes** from DuckLake CDC: `table_changes(table, last_snapshot, current_snapshot)`
2. **Extract affected keys** from both `preimage` and `postimage`
3. **Delete old aggregates** for affected keys
4. **Recompute** only affected keys from source data

## Bootstrap / Initial Load

**First refresh (no prior snapshots):**

When a dynamic table is first created, there are no entries in `source_snapshots`. This indicates an initial load is needed.

```python
def refresh_dynamic_table(table_name):
    snapshots = get_source_snapshots(table_name)
    
    if not snapshots:
        # BOOTSTRAP: Initial load
        # CRITICAL: Capture snapshots BEFORE running query to avoid race condition
        # If we capture after, source data might change between query and snapshot capture
        source_tables = get_source_tables(table_name)
        snapshot_map = {}
        for source_table in source_tables:
            snapshot_map[source_table] = get_current_snapshot(source_table)
        
        # IMPORTANT: Rewrite query to use captured snapshots for consistency
        # See 03-snapshot-isolation.md for rewrite_query_with_snapshots() implementation
        rewritten_query = rewrite_query_with_snapshots(table.query_sql, snapshot_map)
        
        # No CDC needed - just run the query from captured snapshot state
        execute(f"""
            INSERT INTO {table_name}
            {rewritten_query}  -- Query with snapshot clauses injected
        """)
        
        # Record the snapshots we actually used
        for source_table, snapshot_id in snapshot_map.items():
            insert_source_snapshot(table_name, source_table, snapshot_id)
    else:
        # Regular incremental refresh (use CDC from last snapshots)
        refresh_incremental(table_name, snapshots)
```

**Why this matters:**
- No need to process CDC for initial load (nothing to compare against)
- Faster bootstrap - just run query once
- Subsequent refreshes use incremental strategy from that point forward

**Dependency chains during bootstrap:**

If you have a chain of new dynamic tables (`A → B → C`), the orchestrator processes them in **topological order**:

```python
# Orchestrator handles ordering
tables = find_tables_needing_refresh_topological()  # Returns [A, B, C]

for table in tables:  # Processes in dependency order
    refresh_dynamic_table(table)  # Each does bootstrap if needed
```

This ensures B can query A's data, and C can query B's data, even on first run.

## Example: FK Update

**Scenario**: Order #123 changes from customer_id=5 to customer_id=7

**Step 1: Extract affected keys (SQL, not Python loops)**
```sql
-- DuckDB query extracts all affected keys from CDC
CREATE TEMP TABLE affected_keys AS
SELECT DISTINCT customer_id
FROM table_changes('orders', last_snapshot, current_snapshot)
WHERE change_type IN ('update_preimage', 'update_postimage', 'insert', 'delete');

-- Result: {5, 7}
```

**Step 2: Atomic refresh**
```sql
-- All processing in DuckDB - Python just executes these queries
BEGIN TRANSACTION;

DELETE FROM customer_metrics 
WHERE customer_id IN (SELECT customer_id FROM affected_keys);

INSERT INTO customer_metrics
SELECT customer_id, COUNT(*), SUM(amount)
FROM orders
WHERE customer_id IN (SELECT customer_id FROM affected_keys)
GROUP BY customer_id;

COMMIT;
```

**Why this is fast:**
- DuckDB processes millions of rows/second
- Python never touches individual rows
- All computation pushed to SQL queries

## Why This Works

- **Correctness**: Full recompute of affected keys guarantees accuracy
- **Handles FK updates**: Preimage captures old key, postimage captures new key
- **Handles deletes**: Preimage provides keys to update
- **Handles inserts**: Postimage provides keys to update
- **Performance**: Only recompute subset of keys, not entire table

## Fallback: Full Refresh

For queries without GROUP BY or complex queries where keys cannot be determined:
```sql
TRUNCATE dynamic_table
INSERT INTO dynamic_table SELECT ... (full query)
```

## Query Analysis

Use `sqlglot` to extract GROUP BY columns:
```python
def extract_grouping_columns(sql: str) -> list[str]:
    parsed = sqlglot.parse_one(sql)
    # Extract GROUP BY expressions
    # Extract PARTITION BY from window functions
    return group_by_columns
```

## Change Type Handling

| Change Type | Action |
|------------|--------|
| INSERT | Add key from postimage |
| DELETE | Add key from preimage |
| UPDATE_PREIMAGE | Add key from preimage (old value) |
| UPDATE_POSTIMAGE | Add key from postimage (new value) |

## Python Implementation (Orchestration Only)

**Python just executes SQL queries - all heavy work in DuckDB:**

```python
def refresh_affected_keys(
    conn, 
    dynamic_table_name: str,
    query_sql: str,  # Full user query (will be rewritten with snapshots)
    source_tables: dict[str, int],  # {table_name: snapshot_id}
    group_by_cols: list[str],
    last_snapshot: int,  # For CDC detection
    current_snapshot: int  # For CDC detection
):
    """
    Refresh dynamic table using affected keys strategy.
    Python orchestrates - DuckDB does all processing.
    """
    key_list = ', '.join(group_by_cols)
    
    # Step 1: Extract affected keys (runs in DuckDB)
    # TEMP table goes to memory catalog, not DuckLake (no snapshot created)
    # Note: We extract keys from the PRIMARY source table's CDC
    # For multi-table queries, see 02-multi-table-joins.md for handling multiple sources
    primary_source = list(source_tables.keys())[0]  # Simplified - may have multiple sources
    conn.execute(f"""
        CREATE TEMP TABLE affected_keys AS
        SELECT DISTINCT {key_list}
        FROM table_changes('{primary_source}', {last_snapshot}, {current_snapshot})
    """)
    
    # Step 2: Build WHERE clause for key filtering
    if len(group_by_cols) == 1:
        where_clause = f"WHERE {group_by_cols[0]} IN (SELECT {group_by_cols[0]} FROM affected_keys)"
    else:
        # Composite key: WHERE (col1, col2) IN (SELECT col1, col2 FROM ...)
        key_tuple = f"({key_list})"
        where_clause = f"WHERE {key_tuple} IN (SELECT {key_list} FROM affected_keys)"
    
    # Step 3: Rewrite query to use snapshots for consistency
    # This ensures we read all sources at the snapshot they were last refreshed from
    # See 03-snapshot-isolation.md for implementation details
    rewritten_query = rewrite_query_with_snapshots(query_sql, source_tables)
    
    # Step 4: Transactional refresh (all in DuckDB)
    conn.execute("BEGIN TRANSACTION")
    
    try:
        # Delete old aggregates for affected keys
        conn.execute(f"""
            DELETE FROM {dynamic_table_name}
            {where_clause}
        """)
        
        # Recompute affected keys from source using rewritten query
        # Note: where_clause filters to affected keys only
        conn.execute(f"""
            INSERT INTO {dynamic_table_name}
            {add_where_clause_to_query(rewritten_query, where_clause)}
        """)
        
        conn.execute("COMMIT")
        
    except Exception as e:
        conn.execute("ROLLBACK")
        raise RefreshError(f"Failed to refresh {dynamic_table_name}") from e
    
    finally:
        # Cleanup temp table
        conn.execute("DROP TABLE IF EXISTS affected_keys")

# Example usage:
refresh_affected_keys(
    conn=duckdb_conn,
    dynamic_table_name='customer_metrics',
    query_sql='SELECT customer_id, COUNT(*) as order_count, SUM(amount) FROM orders GROUP BY customer_id',
    source_tables={'orders': 100},  # Read orders at snapshot 100
    group_by_cols=['customer_id'],
    last_snapshot=100,  # Last refresh used snapshot 100
    current_snapshot=105  # Detect changes from 100 to 105
)

# Helper functions (see 03-snapshot-isolation.md for detailed implementation):
# - rewrite_query_with_snapshots(query_sql, snapshot_map): 
#     Injects FOR SYSTEM_TIME AS OF SNAPSHOT clauses into SQL AST
# - add_where_clause_to_query(query_sql, where_clause):
#     Adds/combines WHERE clause for affected keys filtering
```

**Performance:**
- Extract 1M+ affected keys/second (DuckDB)
- Filter and aggregate millions of rows/second (DuckDB)
- Python overhead: <1% (just executing queries)

**Optimization: Deduplication**

For dimension-heavy tables, enable deduplication to avoid writing unchanged values:
- Compare old vs new aggregates before writing
- Skip DELETE+INSERT if values identical
- Particularly useful when dimension attributes change but aggregates don't
- Opt-in via `DEDUPLICATION = true` table property

See [Deduplication Strategy](12-deduplication-strategy.md) for details.

**Large Cardinality Handling**

What if millions of keys are affected?
- DuckDB automatically spills to disk (out-of-core processing)
- Adaptive strategy: full refresh if >50% affected
- Persistent temp tables for extreme cases

See [Large Cardinality Handling](13-large-cardinality-handling.md) for details.

## Tuning Cardinality Threshold

**Default (30%)** works for most cases, but consider tuning based on your workload:

### When to Increase Threshold (40-60%)

Use a higher threshold if:
- **Full refresh is very expensive**: Billions of rows, complex joins, slow queries
- **Incremental has good performance**: Indexes on GROUP BY keys, efficient key extraction
- **Affected keys fit in memory**: No spilling overhead for key sets

**Example scenario:**
```sql
-- 10B row table, full scan takes 5 minutes
-- Incremental with 50% affected keys: 2.5 minutes (still faster)
CREATE DYNAMIC TABLE huge_aggregates
CARDINALITY_THRESHOLD = 0.6  -- Stay incremental longer
AS SELECT ...;
```

### When to Decrease Threshold (10-20%)

Use a lower threshold if:
- **Full refresh is optimized**: Columnar compression, simple aggregation
- **Incremental is complex**: Multi-table joins to extract keys, expensive CDC processing
- **Large key sets cause spilling**: Memory pressure from affected_keys temp tables

**Example scenario:**
```sql
-- Optimized columnar layout, full refresh takes 30 seconds
-- Incremental with 15% affected keys: 25 seconds (marginal benefit)
CREATE DYNAMIC TABLE optimized_table
CARDINALITY_THRESHOLD = 0.15  -- Switch to full early
AS SELECT ...;
```

### How to Measure and Tune

1. **Enable timing metrics** (Phase 3):
   ```python
   metrics.record('refresh.duration', duration_ms)
   metrics.record('refresh.strategy', strategy)  # FULL or AFFECTED_KEYS
   metrics.record('refresh.cardinality_ratio', ratio)
   ```

2. **Analyze performance**:
   ```sql
   -- Find cases near threshold where strategy switches
   SELECT dynamic_table, 
          AVG(duration_ms) FILTER (WHERE strategy = 'FULL') as avg_full,
          AVG(duration_ms) FILTER (WHERE strategy = 'AFFECTED_KEYS') as avg_incremental
   FROM refresh_history
   WHERE cardinality_ratio BETWEEN 0.25 AND 0.35  -- Near default threshold
   GROUP BY dynamic_table;
   ```

3. **Adjust threshold** based on crossover point:
   ```bash
   # If incremental is faster even at 50% cardinality
   dynamic-tables alter product_sales --cardinality-threshold=0.5
   ```

**Default is conservative** - favors correctness and predictable performance over maximum optimization.

See [Performance Considerations](11-performance-considerations.md) for detailed analysis.
