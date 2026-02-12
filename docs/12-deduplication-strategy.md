# Deduplication Strategy

## Problem

Dimension changes often don't affect aggregates. Without deduplication, we write unchanged values, creating unnecessary snapshots and triggering downstream refreshes.

**Example:**
```sql
-- Product description updated, but aggregate only uses product_id
-- Values are identical before and after, but standard refresh rewrites them
SELECT product_id, SUM(quantity) FROM orders GROUP BY product_id;
```

## Solution

Compare old vs new aggregates before writing. Skip writes when values unchanged.

```sql
BEGIN TRANSACTION;

-- Compute new values (in memory, no snapshot)
CREATE TEMP TABLE new_agg AS
SELECT customer_id, COUNT(*) as cnt, SUM(amount) as total
FROM orders
WHERE customer_id IN (SELECT customer_id FROM affected_keys)
GROUP BY customer_id;

-- Find actual changes (handles NULLs correctly)
CREATE TEMP TABLE changed AS
SELECT n.*
FROM new_agg n
LEFT JOIN customer_metrics m ON n.customer_id = m.customer_id
WHERE m.customer_id IS NULL  -- New key
   OR NOT (n.cnt IS NOT DISTINCT FROM m.cnt 
       AND n.total IS NOT DISTINCT FROM m.total);  -- Value differs

-- Write only changed rows
DELETE FROM customer_metrics WHERE customer_id IN (SELECT customer_id FROM changed);
INSERT INTO customer_metrics SELECT * FROM changed;

COMMIT;
```

**Benefits:**
- Fewer DuckLake snapshots (less storage)
- Downstream tables not triggered unnecessarily
- Fast (comparison in DuckDB memory)

**Cost:**
- Extra JOIN to compare values
- Slightly more complex SQL

## Usage

```sql
CREATE DYNAMIC TABLE product_sales
DEDUPLICATION = true  -- Opt-in
AS
SELECT product_id, SUM(quantity) FROM orders GROUP BY product_id;
```

**Default:** `false` (simpler, no overhead)

## When to Enable

**Good candidates (high dedup ratio):**
- Joins to dimension tables (product names, customer segments)
- Aggregates don't include dimension attributes
- Wide sources, narrow aggregates
- Expensive downstream dependencies

**Bad candidates (low dedup ratio):**
- Fact table inserts/deletes (always change aggregates)
- Narrow aggregates (overhead > benefit)
- Simple GROUP BY on primary key

## Metrics

Track deduplication effectiveness:

```python
dedup_ratio = 1 - (changed_keys / affected_keys)
metrics.record('refresh.dedup_ratio', dedup_ratio)
```

**Example output:**
```
product_sales: 5/100 keys changed (95% deduplicated) ← Enable dedup!
customer_metrics: 87/90 keys changed (3% deduplicated) ← Disable dedup
```

**Recommendation:**
- Disable if consistently <10% dedup ratio
- Keep enabled if >50% dedup ratio
- Monitor and adjust per table

## Implementation

```python
def refresh_affected_keys(conn, table, affected_keys):
    if table.deduplication:
        # Compare before write
        new_values = compute_new_aggregates(conn, affected_keys)
        changed = find_changed_values(conn, new_values)
        write_only_changed(conn, changed)
    else:
        # Standard: always write
        delete_affected(conn, affected_keys)
        recompute_and_insert(conn, affected_keys)
```

See [examples/product_sales.sql](../examples/product_sales.sql) for complete example with 80-95% dedup ratio.
