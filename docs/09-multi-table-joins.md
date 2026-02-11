# Multi-Table Joins and Denormalization

## Overview

The dynamic table system handles denormalization through SQL queries - you write the join, the system figures out what to refresh when source data changes.

## Core Concept

**You write the join:**
```sql
CREATE DYNAMIC TABLE sales_by_product
AS
SELECT p.product_name, SUM(ol.quantity) as total_quantity
FROM orderlines ol
JOIN products p ON ol.product_id = p.product_id
GROUP BY p.product_name;
```

**System automatically handles:**
- Product name changes in `products` table
- New orderlines
- Updates to orderlines
- Deletes from orderlines or products

## How It Works

### Change Detection Across Tables

When **any** source table changes, the system must determine affected keys.

**Example: Product name change**
```
products CDC:
  preimage:  {product_id: 5, name: "Widget"}
  postimage: {product_id: 5, name: "Super Widget"}
```

**System determines:**
1. Which fact table rows reference this dimension entity: `product_id = 5`
2. What GROUP BY keys are affected: `{"Widget", "Super Widget"}`
3. Whether to do incremental or full refresh based on cardinality

### Cardinality-Based Strategy Selection

**Key insight:** Sometimes incremental is slower than full refresh.

```python
def select_refresh_strategy(change, dynamic_table):
    # Get affected row count
    affected_count = count_affected_rows(change, dynamic_table)
    total_count = get_total_row_count(dynamic_table.fact_table)
    
    cardinality_ratio = affected_count / total_count
    
    if cardinality_ratio > 0.3:  # >30% of rows affected
        return "FULL_REFRESH"
    else:
        return "AFFECTED_KEYS"
```

**Examples:**

| Change | Affected Rows | Strategy | Reason |
|--------|--------------|----------|---------|
| Product #5 name changes | 100 orderlines | Incremental | Low cardinality |
| Country name changes | 5M orderlines | Full refresh | High cardinality |
| Customer segment change | 10K orders | Incremental | Medium cardinality |

## Scenario 1: Dimension Table Change (Low Cardinality)

**Dynamic Table:**
```sql
CREATE DYNAMIC TABLE sales_by_product
AS
SELECT p.product_name, SUM(ol.quantity) as total_quantity
FROM orderlines ol
JOIN products p ON ol.product_id = p.product_id
GROUP BY p.product_name;
```

**Change:** Product #5 name: "Widget" → "Super Widget" (affects 100 orderlines)

**Refresh Algorithm:**

1. **Detect change via CDC:**
   ```
   products.table_changes():
     - preimage:  {product_id: 5, name: "Widget"}
     - postimage: {product_id: 5, name: "Super Widget"}
   ```

2. **Check cardinality:**
   ```sql
   SELECT COUNT(*) FROM orderlines WHERE product_id = 5;
   -- Returns: 100 (out of 1M total) = 0.01% → Use incremental
   ```

3. **Extract affected GROUP BY keys:**
   - From preimage: "Widget"
   - From postimage: "Super Widget"

4. **Refresh affected keys:**
   ```sql
   -- Transactional refresh (atomic view to consumers)
   BEGIN TRANSACTION;
   
   -- Delete old aggregates
   DELETE FROM sales_by_product 
   WHERE product_name IN ('Widget', 'Super Widget');
   
   -- Recompute by filtering on join key
   INSERT INTO sales_by_product
   SELECT p.product_name, SUM(ol.quantity)
   FROM orderlines ol
   JOIN products p ON ol.product_id = p.product_id
   WHERE p.product_id = 5  -- Filter by changed dimension entity
   GROUP BY p.product_name;
   
   COMMIT;
   -- Consumers only see data before or after, never partial state
   ```

## Scenario 2: Dimension Table Change (High Cardinality)

**Change:** Country name: "USA" → "United States" (affects 5M orderlines)

**Refresh Algorithm:**

1. **Detect change via CDC:**
   ```
   countries.table_changes():
     - preimage:  {country_code: 'US', name: 'USA'}
     - postimage: {country_code: 'US', name: 'United States'}
   ```

2. **Check cardinality:**
   ```sql
   SELECT COUNT(*) FROM orders o
   JOIN customers c ON o.customer_id = c.id
   WHERE c.country_code = 'US';
   -- Returns: 5M (out of 10M total) = 50% → Use full refresh
   ```

3. **Execute full refresh:**
   ```sql
   TRUNCATE sales_by_country;
   INSERT INTO sales_by_country
   SELECT c.country_name, SUM(o.amount)
   FROM orders o
   JOIN customers c ON o.customer_id = c.id
   GROUP BY c.country_name;
   ```

**Why full refresh is faster:**
- Incremental: Delete 50% of aggregates + recompute 50% = ~75% of work
- Full: Recompute everything = 100% of work, but single pass, better optimized

## Scenario 3: Fact Table Change

**Change:** New orderline added or orderline quantity updated

**This is simpler** - standard affected keys strategy works:

```sql
orderlines.table_changes():
  - postimage: {orderline_id: 999, product_id: 5, quantity: 10}
```

Extract affected keys:
- Look up product_id=5 to get product_name
- Recompute aggregate for that product_name



Extract affected keys:
- Look up product_id=5 to get product_name
- Recompute aggregate for that product_name

## Implementation: Multi-Table Query Analysis

**Metadata to track:**

```python
class DynamicTableMetadata:
    name: str
    query_sql: str
    group_by_columns: List[str]
    
    # Multi-table specific
    source_tables: List[str]  # All upstream tables
    join_graph: List[Join]    # Join relationships
    
class Join:
    left_table: str
    right_table: str
    left_key: str   # e.g., 'product_id'
    right_key: str  # e.g., 'product_id'
```

**Query analyzer:**

```python
def analyze_multi_table_query(sql):
    parsed = sqlglot.parse_one(sql)
    
    # Extract components
    tables = extract_tables(parsed)  # {alias: table_name}
    joins = extract_join_conditions(parsed)
    group_by = extract_group_by_columns(parsed, tables)
    
    # No need to classify fact vs dimension - determine at runtime!
    return {
        'group_by_columns': group_by,
        'source_tables': list(tables.values()),
        'join_graph': joins
    }
```

**Why no fact/dimension classification?**

We determine the optimal strategy at runtime based on **actual cardinality**, not semantic table roles:
- A "dimension" table change affecting 80% of rows → full refresh
- A "fact" table change affecting 1% of rows → incremental refresh
- More flexible: same table can be treated differently in different queries

## Refresh Algorithm for Joins

```python
def refresh_with_multi_table_changes(dynamic_table, changes_by_table):
    affected_keys = set()
    total_affected_rows = 0
    join_filters = []
    
    # For EACH upstream table change, count affected rows
    for table_name, changes in changes_by_table.items():
        for change in changes:
            # Check if change affects GROUP BY columns directly
            direct_keys = extract_keys_from_changes([change], dynamic_table.group_by_columns)
            if direct_keys:
                # Changed row contributes GROUP BY keys directly
                affected_keys.update(direct_keys)
                total_affected_rows += 1
            
            # Check if change affects joined rows
            join = find_join_involving_table(dynamic_table.join_graph, table_name)
            if join:
                # Get changed entity ID
                entity_id = (change['preimage'] or change['postimage'])[join.right_key]
                
                # Count how many rows in OTHER tables reference this
                affected_count = count_rows_joined_to_entity(
                    dynamic_table.query,
                    join,
                    entity_id
                )
                total_affected_rows += affected_count
                
                # Track join filter for incremental refresh
                join_filters.append((join, entity_id))
                
                # Extract GROUP BY keys from this change
                keys = extract_group_by_keys_via_join(
                    dynamic_table.query, 
                    change, 
                    dynamic_table.group_by_columns
                )
                affected_keys.update(keys)
    
    # Decide strategy based on TOTAL cardinality (not table role)
    total_rows = estimate_total_result_rows(dynamic_table)
    cardinality_ratio = total_affected_rows / total_rows if total_rows > 0 else 0
    
    if cardinality_ratio > CARDINALITY_THRESHOLD:
        return full_refresh(dynamic_table)
    else:
        return incremental_refresh(dynamic_table, affected_keys, join_filters)

def incremental_refresh(dynamic_table, affected_keys, join_filters):
    # Build WHERE clause from ALL changed entities (regardless of table role)
    filter_clauses = []
    for join, entity_id in join_filters:
        filter_clauses.append(f"{join.left_table}.{join.left_key} = {entity_id}")
    
    filter_clause = " OR ".join(filter_clauses) if filter_clauses else "1=1"
    
    # Delete old aggregates
    execute(f"""
        DELETE FROM {dynamic_table.name}
        WHERE {build_group_by_predicate(affected_keys)}
    """)
    
    # Recompute with filters - only process rows touched by ANY upstream change
    execute(f"""
        INSERT INTO {dynamic_table.name}
        SELECT ... FROM {dynamic_table.query}
        WHERE {filter_clause}
    """)
```

**Key insight:** We don't care if a table is semantically a "fact" or "dimension" - we only care about the **cardinality of affected rows** for this specific refresh.

## Cardinality Threshold Configuration

**Per-table configuration:**

```sql
CREATE TABLE dynamic_tables (
    ...,
    cardinality_threshold FLOAT DEFAULT 0.3,  -- 30% threshold
    prefer_full_refresh BOOLEAN DEFAULT FALSE  -- Override
);
```

**Heuristics:**

- **<10% affected**: Always incremental
- **10-30% affected**: Incremental (marginal benefit)
- **>30% affected**: Full refresh (simpler, faster)
- **User override**: Can force strategy

**Large cardinality challenges:**

What if affected keys are too numerous to fit in memory?
- DuckDB automatically spills to disk (out-of-core processing)
- Adaptive strategies select based on actual counts
- Persistent temp tables for extreme cases (>10M keys)

See [Large Cardinality Handling](13-large-cardinality-handling.md) for detailed strategies.

## Scenario 4: Multiple Dimensions

## Scenario 4: Multiple Upstream Tables (3-way join)

**Dynamic Table:**
```sql
CREATE DYNAMIC TABLE order_summary
AS
SELECT 
  p.product_name,
  c.customer_segment,
  COUNT(DISTINCT o.order_id) as order_count
FROM orders o
JOIN orderlines ol ON o.order_id = ol.order_id
JOIN products p ON ol.product_id = p.product_id
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY p.product_name, c.customer_segment;
```

**Changes:**
- Product #5 name changes (affects 100 orderlines)
- Customer #10 segment changes (affects 500 orders)

**Runtime Strategy:**
```python
# Count affected rows from EACH change
product_change_affected = 100  # Count at runtime
customer_change_affected = 500  # Count at runtime
total_result_rows = 50_000

# Total affected (union of both changes)
combined_cardinality = (100 + 500) / 50_000 = 1.2%

if combined_cardinality < 30%:
    # Incremental with BOTH filters
    WHERE p.product_id = 5 OR c.customer_id = 10
else:
    # Full refresh
```

**Note:** Algorithm naturally handles N-way joins - no limit on number of upstream tables.

## Test Cases

```python
def test_dimension_change_low_cardinality():
    # Given: Sales by product query
    create_dynamic_table("""
        SELECT p.product_name, SUM(ol.quantity)
        FROM orderlines ol
        JOIN products p ON ol.product_id = p.product_id
        GROUP BY p.product_name
    """)
    
    # When: Product name changes (affects 100 of 1M orderlines)
    update("UPDATE products SET name = 'Super Widget' WHERE product_id = 5")
    trigger_refresh()
    
    # Then: Incremental refresh used, both names updated
    assert refresh_strategy_used() == "AFFECTED_KEYS"
    assert not exists("Widget")
    assert get_quantity("Super Widget") == expected

def test_dimension_change_high_cardinality():
    # Given: Sales by country query
    create_dynamic_table("""
        SELECT c.country_name, SUM(o.amount)
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        GROUP BY c.country_name
    """)
    
    # When: Country name changes (affects 5M of 10M orders)
    update("UPDATE countries SET name = 'United States' WHERE code = 'US'")
    trigger_refresh()
    
    # Then: Full refresh used (>30% cardinality)
    assert refresh_strategy_used() == "FULL"
    assert get_amount("United States") == expected

def test_multiple_upstream_changes():
    # Given: 3-way join (orders + products + customers)
    create_dynamic_table("""
        SELECT p.product_name, c.customer_segment, COUNT(DISTINCT o.order_id)
        FROM orders o
        JOIN orderlines ol ON o.order_id = ol.order_id
        JOIN products p ON ol.product_id = p.product_id
        JOIN customers c ON o.customer_id = c.customer_id
        GROUP BY p.product_name, c.customer_segment
    """)
    
    # When: Changes to BOTH product and customer tables
    update("UPDATE products SET name = 'Super Widget' WHERE product_id = 5")
    update("UPDATE customers SET segment = 'Premium' WHERE customer_id = 10")
    trigger_refresh()
    
    # Then: Strategy based on combined cardinality from runtime counts
    # Incremental if union < 30%, else full
    assert verify_correct_aggregates()
```

## Metadata Tracking

**Store join graph in metadata:**

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

**Populate during CREATE DYNAMIC TABLE:**

```python
# Parse query
metadata = analyze_multi_table_query(sql)

# Store joins
for join in metadata.join_graph:
    insert_join(dynamic_table_name, join)
```

## Implementation Phases

**Phase 2 (MVP):**
- Single table aggregations only (no joins)
- Document limitation: "Multi-table joins coming in Phase 3"

**Phase 3 (Multi-table support):**
- Query analyzer detects joins (N-way joins supported)
- Track join graph in metadata
- Runtime cardinality checks for all upstream changes
- Cardinality-based strategy selection (incremental vs full)
- Test with 3-table joins to validate N-way logic

**Phase 4 (Optimization):**
- Smart cardinality estimation (sampling instead of full COUNT)
- Cost-based optimizer (query plan analysis)
- Parallel multi-table refresh coordination

## Configuration

**Cardinality thresholds:**

```python
# Global defaults
CARDINALITY_THRESHOLD_INCREMENTAL = 0.10  # <10%: Always incremental
CARDINALITY_THRESHOLD_FULL = 0.30         # >30%: Always full

# Per-table override
CREATE DYNAMIC TABLE sales_by_product
CARDINALITY_THRESHOLD = 0.50  -- Tolerate higher cardinality for incremental
AS SELECT ...
```

**Strategy hints:**

```sql
-- Force strategy regardless of cardinality
CREATE DYNAMIC TABLE sales_by_country
REFRESH_STRATEGY = 'FULL'  -- Even for small changes
AS SELECT ...
```
