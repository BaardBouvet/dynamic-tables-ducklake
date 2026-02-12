# Snapshot Isolation for Consistency

## The Problem

If `high_value_customers` depends on both `orders` and `customer_metrics`, and `customer_metrics` was built from `orders@100` but `high_value_customers` reads `orders@105`, results are inconsistent.

## The Solution

Read all sources at the snapshots they were last refreshed from:

```sql
-- customer_metrics was built from orders@100
-- high_value_customers refresh:
SELECT * 
FROM orders FOR SYSTEM_TIME AS OF SNAPSHOT 100  -- Same snapshot!
JOIN customer_metrics  -- Already at snapshot 100
WHERE ...
```

## Metadata Tracking

Track which snapshot each dynamic table read from each source:

```sql
-- source_snapshots table
INSERT INTO source_snapshots VALUES
  ('customer_metrics', 'orders', 100),
  ('high_value_customers', 'customer_metrics', 42);
```

## Refresh Algorithm

1. **Get snapshots from metadata:**
   ```python
   snapshots = {
       'orders': 100,        # From source_snapshots
       'customers': 98
   }
   ```

2. **Rewrite query with snapshot clauses:**
   ```sql
   -- Original user query:
   SELECT customer_id, COUNT(*) 
   FROM orders JOIN customers ON orders.customer_id = customers.id
   GROUP BY customer_id
   
   -- Rewritten with snapshots:
   SELECT customer_id, COUNT(*)
   FROM orders FOR SYSTEM_TIME AS OF SNAPSHOT 100
   JOIN customers FOR SYSTEM_TIME AS OF SNAPSHOT 98 ON orders.customer_id = customers.id
   GROUP BY customer_id
   ```

3. **Execute rewritten query**

4. **Update source_snapshots after successful refresh**

## Query Rewriting with sqlglot

Use AST manipulation to inject snapshot clauses:

```python
import sqlglot
from sqlglot import exp

def rewrite_query_with_snapshots(query_sql: str, snapshot_map: dict) -> str:
    """Inject FOR SYSTEM_TIME AS OF SNAPSHOT clauses."""
    parsed = sqlglot.parse_one(query_sql, dialect='duckdb')
    
    # Find all table references
    for table_node in parsed.find_all(exp.Table):
        table_name = table_node.name
        if table_name in snapshot_map:
            snapshot_id = snapshot_map[table_name]
            time_travel = exp.ForSystemTime(
                this=exp.Literal.number(snapshot_id),
                kind='SNAPSHOT'
            )
            table_node.set('for_system_time', time_travel)
    
    return parsed.sql(dialect='duckdb')

# Example:
rewritten = rewrite_query_with_snapshots(
    "SELECT * FROM orders JOIN customers ON ...",
    {'orders': 100, 'customers': 98}
)
# → "SELECT * FROM orders FOR SYSTEM_TIME AS OF SNAPSHOT 100 JOIN customers FOR SYSTEM_TIME AS OF SNAPSHOT 98 ON ..."
```

## Edge Cases

**Subqueries:** sqlglot automatically traverses and rewrites.

**CTEs:** Base table references get rewritten, CTE names don't.

**Aliases:** Preserved during rewriting.
```sql
FROM orders FOR SYSTEM_TIME AS OF SNAPSHOT 100 AS o
```

**Self-joins:** Same table gets same snapshot.
```sql
FROM orders FOR SYSTEM_TIME AS OF SNAPSHOT 100 AS o1
JOIN orders FOR SYSTEM_TIME AS OF SNAPSHOT 100 AS o2
```

**Dynamic tables:** Skip rewriting (already materialized at correct snapshot).

## Dependency Order

Refresh order ensures consistency:
1. Parents refresh first (advance snapshots)
2. Children refresh second (use parent's new snapshot)
3. Cascading consistency maintained

## DuckLake Support

DuckLake provides time-travel:
```sql
SELECT * FROM table FOR SYSTEM_TIME AS OF SNAPSHOT 100
SELECT * FROM table FOR SYSTEM_TIME AS OF TIMESTAMP '2026-02-10 10:00:00'
```

## Test Case

```python
def test_snapshot_consistency():
    refresh("metrics")  # reads orders@100
    insert_order(...)   # orders→105
    refresh("report")   # must read orders@100 (from metrics' snapshots)
    assert report_read_snapshot("orders") == 100
```

