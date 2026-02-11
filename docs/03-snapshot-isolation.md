# Snapshot Isolation for Consistency

## The Problem

```
orders (snapshot 105) ──┐
                        ├──> customer_metrics (built at snapshot 100)
                        └──> high_value_customers (depends on both)
```

If `high_value_customers` reads `orders@105` but `customer_metrics` was built from `orders@100`, results are inconsistent.

## The Solution

Read all sources at the snapshot they were last refreshed from:

```sql
-- customer_metrics was built from orders@100
-- When high_value_customers refreshes:
SELECT * 
FROM orders FOR SYSTEM_TIME AS OF SNAPSHOT 100  -- Same snapshot!
JOIN customer_metrics  -- Already at snapshot 100
WHERE ...
```

## Implementation

### Metadata Tracking

Track which snapshot each dynamic table read from each source:

```sql
-- source_snapshots table
INSERT INTO source_snapshots VALUES
  ('high_value_customers', 'orders', 100),
  ('high_value_customers', 'customer_metrics', 42);
```

### Refresh Algorithm

1. **Determine snapshot for each source**:
   - For base tables: Use snapshot from `source_snapshots`
   - For dynamic tables: Use their materialized snapshot

2. **Execute refresh with snapshot isolation**:
   ```sql
   -- Rewrite user query to use specific snapshots
   SELECT ...
   FROM orders FOR SYSTEM_TIME AS OF SNAPSHOT 100
   JOIN customers FOR SYSTEM_TIME AS OF SNAPSHOT 98
   WHERE key IN (affected_keys)
   ```

3. **After successful refresh**:
   - Can optionally advance snapshots to latest
   - Or keep conservative snapshots for consistency

### Dependency Graph Impact

Refresh order matters:
1. Parent tables refresh first (advance their snapshots)
2. Child tables refresh second (using parent's new snapshot)
3. Ensures cascading consistency

## DuckLake Support

DuckLake provides time-travel queries:
```sql
SELECT * FROM table FOR SYSTEM_TIME AS OF SNAPSHOT 100
SELECT * FROM table FOR SYSTEM_TIME AS OF TIMESTAMP '2026-02-10 10:00:00'
```

This is critical for implementing snapshot isolation.

## Query Rewriting Implementation

### Challenge: Injecting Snapshot Clauses

User writes a query referencing tables by name:
```sql
SELECT customer_id, COUNT(*) 
FROM orders 
JOIN customers ON orders.customer_id = customers.id
GROUP BY customer_id
```

System must rewrite to:
```sql
SELECT customer_id, COUNT(*)
FROM orders FOR SYSTEM_TIME AS OF SNAPSHOT 100
JOIN customers FOR SYSTEM_TIME AS OF SNAPSHOT 98
GROUP BY customer_id
```

### Using sqlglot for AST Manipulation

**Strategy:** Parse SQL into AST, modify table references, regenerate SQL.

```python
import sqlglot
from sqlglot import exp

def rewrite_query_with_snapshots(
    query_sql: str,
    snapshot_map: dict[str, int]  # {table_name: snapshot_id}
) -> str:
    """
    Rewrite query to add FOR SYSTEM_TIME AS OF SNAPSHOT clauses.
    
    Args:
        query_sql: Original user query
        snapshot_map: Mapping of table names to snapshot IDs
        
    Returns:
        Rewritten query with snapshot clauses
    """
    # Parse SQL into AST
    parsed = sqlglot.parse_one(query_sql, dialect='duckdb')
    
    # Find all table references in the query
    for table_node in parsed.find_all(exp.Table):
        table_name = table_node.name
        
        # Check if we have a snapshot for this table
        if table_name in snapshot_map:
            snapshot_id = snapshot_map[table_name]
            
            # Create FOR SYSTEM_TIME clause
            # Note: DuckLake syntax may vary - adjust accordingly
            time_travel = exp.ForSystemTime(
                this=exp.Literal.number(snapshot_id),
                kind='SNAPSHOT'
            )
            
            # Attach to table node
            table_node.set('for_system_time', time_travel)
    
    # Regenerate SQL from modified AST
    rewritten_sql = parsed.sql(dialect='duckdb')
    return rewritten_sql

# Example usage:
original_query = """
    SELECT customer_id, COUNT(*) as order_count
    FROM orders
    JOIN customers ON orders.customer_id = customers.id
    WHERE orders.status = 'completed'
    GROUP BY customer_id
"""

snapshots = {
    'orders': 100,
    'customers': 98
}

rewritten = rewrite_query_with_snapshots(original_query, snapshots)
# Result:
# SELECT customer_id, COUNT(*) as order_count
# FROM orders FOR SYSTEM_TIME AS OF SNAPSHOT 100
# JOIN customers FOR SYSTEM_TIME AS OF SNAPSHOT 98 
#   ON orders.customer_id = customers.id
# WHERE orders.status = 'completed'
# GROUP BY customer_id
```

### Handling Complex Queries

**Subqueries:**
```python
# sqlglot automatically traverses subqueries
for subquery in parsed.find_all(exp.Subquery):
    # Table references in subqueries also get rewritten
    pass
```

**CTEs (WITH clauses):**
```python
# CTEs reference base tables - those get rewritten
WITH top_customers AS (
    SELECT customer_id FROM orders  -- Gets snapshot clause
)
SELECT * FROM top_customers  -- CTE itself doesn't need clause
```

**Views/Dynamic Tables:**
```python
# Dynamic tables are already materialized at a snapshot
# Don't need FOR SYSTEM_TIME - they are frozen
if table_name in dynamic_table_names:
    # Skip - already at correct snapshot
    continue
```

### Full Implementation

```python
def prepare_refresh_query(
    dynamic_table: DynamicTable,
    affected_keys: list,
    pg_conn,  # PostgreSQL metadata
    duck_conn  # DuckDB connection
) -> str:
    """
    Prepare refresh query with snapshot isolation.
    """
    # 1. Get source snapshots from metadata
    snapshots = pg_conn.execute("""
        SELECT source_table, last_snapshot
        FROM source_snapshots
        WHERE dynamic_table = %s
    """, (dynamic_table.name,)).fetchall()
    
    snapshot_map = {row['source_table']: row['last_snapshot'] 
                   for row in snapshots}
    
    # 2. Rewrite base query with snapshots
    rewritten_query = rewrite_query_with_snapshots(
        dynamic_table.query_sql,
        snapshot_map
    )
    
    # 3. Add WHERE clause for affected keys filtering
    if affected_keys:
        rewritten_query = add_affected_keys_filter(
            rewritten_query,
            dynamic_table.group_by_columns,
            affected_keys
        )
    
    return rewritten_query

def add_affected_keys_filter(
    query: str,
    group_by_cols: list[str],
    affected_keys: list
) -> str:
    """Add WHERE clause filtering to affected keys."""
    parsed = sqlglot.parse_one(query, dialect='duckdb')
    
    # Build IN clause
    if len(group_by_cols) == 1:
        # Single key: WHERE customer_id IN (1, 2, 3)
        key_col = group_by_cols[0]
        in_clause = exp.In(
            this=exp.Column(this=key_col),
            expressions=[exp.Literal.number(k) for k in affected_keys]
        )
    else:
        # Composite key: WHERE (col1, col2) IN ((1,'a'), (2,'b'))
        in_clause = create_composite_key_in_clause(group_by_cols, affected_keys)
    
    # Add to WHERE clause (or AND with existing WHERE)
    if parsed.find(exp.Where):
        existing_where = parsed.find(exp.Where)
        existing_where.set(
            'this',
            exp.And(this=existing_where.this, expression=in_clause)
        )
    else:
        parsed.set('where', exp.Where(this=in_clause))
    
    return parsed.sql(dialect='duckdb')
```

### Edge Cases

**1. Table aliases:**
```sql
-- User query with aliases
SELECT * FROM orders o JOIN customers c ...

-- Rewrite must preserve aliases
SELECT * FROM orders FOR SYSTEM_TIME AS OF SNAPSHOT 100 AS o
  JOIN customers FOR SYSTEM_TIME AS OF SNAPSHOT 98 AS c ...
```

**2. Schema-qualified names:**
```sql
-- User query
SELECT * FROM lake.prod.orders

-- Rewrite
SELECT * FROM lake.prod.orders FOR SYSTEM_TIME AS OF SNAPSHOT 100

-- Must handle: catalog.schema.table
table_full_name = f"{catalog}.{schema}.{table}" if catalog else f"{schema}.{table}"
```

**3. Self-joins:**
```sql
-- Same table referenced twice
SELECT * FROM orders o1 JOIN orders o2 ...

-- Both get same snapshot
SELECT * FROM orders FOR SYSTEM_TIME AS OF SNAPSHOT 100 AS o1
  JOIN orders FOR SYSTEM_TIME AS OF SNAPSHOT 100 AS o2 ...
```

### Testing Query Rewriting

```python
def test_simple_query_rewrite():
    query = "SELECT * FROM orders"
    snapshots = {'orders': 100}
    
    result = rewrite_query_with_snapshots(query, snapshots)
    assert "FOR SYSTEM_TIME AS OF SNAPSHOT 100" in result

def test_join_query_rewrite():
    query = """
        SELECT * FROM orders o 
        JOIN customers c ON o.customer_id = c.id
    """
    snapshots = {'orders': 100, 'customers': 98}
    
    result = rewrite_query_with_snapshots(query, snapshots)
    assert "orders" in result and "100" in result
    assert "customers" in result and "98" in result

def test_subquery_rewrite():
    query = """
        SELECT * FROM (
            SELECT customer_id FROM orders
        ) subq
    """
    snapshots = {'orders': 100}
    
    result = rewrite_query_with_snapshots(query, snapshots)
    # Subquery's orders reference should have snapshot clause
    assert result.count("FOR SYSTEM_TIME") == 1

def test_cte_rewrite():
    query = """
        WITH top_orders AS (
            SELECT * FROM orders WHERE amount > 1000
        )
        SELECT * FROM top_orders
    """
    snapshots = {'orders': 100}
    
    result = rewrite_query_with_snapshots(query, snapshots)
    # Only base table gets clause, not CTE reference
    assert result.count("FOR SYSTEM_TIME") == 1 

def test_dynamic_table_not_rewritten():
    query = """
        SELECT * FROM customer_metrics
        WHERE customer_id IN (SELECT id FROM customers)
    """
    snapshots = {'customers': 98}
    dynamic_tables = {'customer_metrics'}
    
    result = rewrite_query_with_snapshots(
        query, snapshots, skip_tables=dynamic_tables
    )
    # customer_metrics doesn't get snapshot clause (already materialized)
    # customers does
    assert "customers" in result and "98" in result
    assert "customer_metrics FOR SYSTEM_TIME" not in result
```

### Alternative: Explicit Snapshot Parameters

If query rewriting is too complex, use explicit snapshot parameters:

```python
def refresh_with_explicit_snapshots(conn, dynamic_table, snapshots):
    """Alternative: Pass snapshots as parameters to query."""
    
    # User query references @snapshot variables
    # CREATE DYNAMIC TABLE metrics AS
    #   SELECT * FROM orders FOR SYSTEM_TIME AS OF SNAPSHOT @orders_snapshot
    
    # Bind snapshot values at execution
    params = {
        f'{table}_snapshot': snapshot_id
        for table, snapshot_id in snapshots.items()
    }
    
    conn.execute(dynamic_table.query_sql, params)
```

**Trade-off:**
- ✅ Simpler - no AST manipulation
- ❌ Requires users to write queries with snapshot placeholders
- ❌ Less transparent

**Recommendation:** Use AST rewriting for transparency - users write normal SQL.

## Test Case

```python
def test_snapshot_consistency():
    # metrics built at orders@100
    refresh("metrics")  # reads orders@100
    
    # orders advances to 105
    insert_order(...)
    
    # report must still read orders@100, not 105
    refresh("report")
    assert report_read_snapshot("orders") == 100
```
