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
        # No CDC needed - just run the query from current state
        execute(f"""
            INSERT INTO {table_name}
            {table.query_sql}  -- Full query, no filtering
        """)
        
        # Record current snapshots for next time
        for source_table in get_source_tables(table_name):
            current_snapshot = get_current_snapshot(source_table)
            insert_source_snapshot(table_name, source_table, current_snapshot)
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
    source_table: str,
    group_by_cols: list[str],
    select_clause: str,
    from_clause: str,
    last_snapshot: int,
    current_snapshot: int
):
    """
    Refresh dynamic table using affected keys strategy.
    Python orchestrates - DuckDB does all processing.
    """
    key_list = ', '.join(group_by_cols)
    
    # Step 1: Extract affected keys (runs in DuckDB)
    # TEMP table goes to memory catalog, not DuckLake (no snapshot created)
    conn.execute(f"""
        CREATE TEMP TABLE affected_keys AS
        SELECT DISTINCT {key_list}
        FROM table_changes('{source_table}', {last_snapshot}, {current_snapshot})
    """)
    
    # Step 2: Build WHERE clause for key filtering
    if len(group_by_cols) == 1:
        where_clause = f"WHERE {group_by_cols[0]} IN (SELECT {group_by_cols[0]} FROM affected_keys)"
    else:
        # Composite key: WHERE (col1, col2) IN (SELECT col1, col2 FROM ...)
        key_tuple = f"({key_list})"
        where_clause = f"WHERE {key_tuple} IN (SELECT {key_list} FROM affected_keys)"
    
    # Step 3: Transactional refresh (all in DuckDB)
    conn.execute("BEGIN TRANSACTION")
    
    try:
        # Delete old aggregates for affected keys
        conn.execute(f"""
            DELETE FROM {dynamic_table_name}
            {where_clause}
        """)
        
        # Recompute affected keys from source
        conn.execute(f"""
            INSERT INTO {dynamic_table_name}
            SELECT {select_clause}
            FROM {from_clause}
            {where_clause}
            GROUP BY {key_list}
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
    source_table='orders',
    group_by_cols=['customer_id'],
    select_clause='customer_id, COUNT(*) as order_count, SUM(amount) as total_amount',
    from_clause='orders',
    last_snapshot=42,
    current_snapshot=43
)
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

See [Performance Considerations](11-performance-considerations.md) for detailed analysis.
