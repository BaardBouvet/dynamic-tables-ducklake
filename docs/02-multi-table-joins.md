# Incremental Refresh Strategy: Denormalization & Joins

## Primary Use Case

**Denormalization:** Join normalized tables into wide, analytics-ready tables. System automatically performs incremental refresh when source data changes.

## Canonical Example

```sql
CREATE DYNAMIC TABLE order_details
TARGET_LAG = '5 minutes'
AS
SELECT 
    o.order_id, o.order_date, o.amount,
    c.customer_name, c.customer_segment, c.customer_region
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id;
```

**System handles:**
- New/updated orders → refresh those rows
- Customer changes → update all that customer's orders  
- Automatic cardinality-based strategy selection (incremental vs full)

## How It Works

### Fact Table Changes (Simple)

**Change:** New order #999 added

**Algorithm:** Filter to affected order_id, join with customer, and insert
```sql
INSERT INTO order_details
SELECT o.*, c.customer_name, c.customer_segment, c.customer_region
FROM orders o JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_id = 999;
```

**Performance:** Processes 1 order + 1 customer, not millions.

### Dimension Table Changes (The Challenge)

**Change:** Customer #42 name changes: "Alice Corp" → "Alice Corporation"

**Challenge:** Must update ALL orders for this customer.

**Algorithm:**
1. Find affected orders: `SELECT order_id FROM orders WHERE customer_id = 42` (returns 500)
2. Delete old rows: `DELETE FROM order_details WHERE order_id IN (...)`  
3. Recompute: Re-join those 500 orders with updated customer data
4. Insert refreshed rows

**Performance:** Only processes 500 of 10M orders (0.005%)

**Key insight:** By filtering to affected order_ids, only recompute rows that actually changed.

## Cardinality-Based Strategy Selection

Incremental refresh can be slower than full when many rows are affected.

```python
affected_ratio = affected_rows / total_rows

if affected_ratio > 0.3:  # >30%
    return "FULL_REFRESH"
else:
    return "INCREMENTAL"
```

**Decision matrix:**

| Change Example | Affected | Strategy | Why |
|----------------|----------|----------|-----|
| Single customer name | 500 / 10M (0.005%) | Incremental | Tiny fraction |
| Region renamed | 2M / 10M (20%) | Incremental | Still worth it |
| Segment renamed | 8M / 10M (80%) | Full | Cheaper to rebuild |

**Why full is faster for high cardinality:**
- Incremental: DELETE 8M + INSERT 8M = 16M operations + overhead
- Full: Single INSERT of 10M rows = better optimization, sequential writes

**Tunable** via `CARDINALITY_THRESHOLD` property (default: 0.3)

## Multi-Table Joins (N-way)

**3-way join example:**
```sql
SELECT p.product_name, c.customer_segment, COUNT(*)
FROM orders o
JOIN orderlines ol ON o.order_id = ol.order_id
JOIN products p ON ol.product_id = p.product_id
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY p.product_name, c.customer_segment;
```

**When multiple sources change:**
- Count affected rows from ALL changes combined
- Use total cardinality for strategy decision
- Filter includes all changed entities: `WHERE p.product_id = 5 OR c.customer_id = 10`

No limit on number of joined tables - algorithm scales to N-way joins.

## Aggregations with Joins

When using GROUP BY, see [09-aggregation-strategy.md](09-aggregation-strategy.md) for "affected keys" strategy that recomputes only changed aggregates.

## Implementation

### Query Analysis

```python
def analyze_multi_table_query(sql):
    parsed = sqlglot.parse_one(sql
)
    return {
        'source_tables': extract_tables(parsed),
        'join_graph': extract_join_conditions(parsed),
        'group_by_columns': extract_group_by_columns(parsed)
    }
```

### Refresh Algorithm

```python
def refresh_join_query(dynamic_table, changes_by_table):
    join_filters = []
    total_affected = 0
    
    # For each changed table, count affected rows
    for table, changes in changes_by_table.items():
        join = find_join(dynamic_table.join_graph, table)
        for change in changes:
            entity_id = change['postimage'][join.key]
            affected_count = count_joined_rows(entity_id)
            total_affected += affected_count
            join_filters.append((join, entity_id))
    
    # Strategy decision
    ratio = total_affected / estimate_total_rows(dynamic_table)
    if ratio > 0.3:
        return full_refresh(dynamic_table)
    else:
        # Delete affected, recompute with filters
        filter_clause = build_filter_from_changes(join_filters)
        execute(f"DELETE FROM {table} WHERE {filter_clause}")
        execute(f"INSERT INTO {table} SELECT ... WHERE {filter_clause}")
```

## Metadata Tracking

```sql
CREATE TABLE dynamic_table_joins (
    dynamic_table VARCHAR,
    left_table VARCHAR,
    right_table VARCHAR,
    left_key VARCHAR,
    right_key VARCHAR,
    join_type VARCHAR,  -- INNER, LEFT, RIGHT
    PRIMARY KEY (dynamic_table, left_table, right_table)
);
```

Populated during `CREATE DYNAMIC TABLE` via SQL parsing.

## Configuration

```sql
CREATE DYNAMIC TABLE order_details
CARDINALITY_THRESHOLD = 0.5  -- Tolerate 50% affected before full refresh
REFRESH_STRATEGY = 'AUTO'    -- Or 'FULL' / 'INCREMENTAL' to override
AS SELECT ...;
```

See [13-large-cardinality-handling.md](13-large-cardinality-handling.md) for handling extreme cardinalities (>10M affected keys).
