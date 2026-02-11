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
