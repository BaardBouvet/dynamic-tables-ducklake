# SQL Interface

## DDL Syntax

Snowflake-compatible syntax:

```sql
CREATE DYNAMIC TABLE lake.dynamic.customer_metrics
TARGET_LAG = '5 minutes'
AS
SELECT customer_id, COUNT(*) as order_count, SUM(amount) as total
FROM lake.orders
GROUP BY customer_id;
```

**With optional deduplication:**

```sql
CREATE DYNAMIC TABLE lake.dynamic.product_sales
TARGET_LAG = '5 minutes'
DEDUPLICATION = true  -- Skip writes when values unchanged
AS
SELECT product_id, SUM(quantity) as total_qty
FROM lake.orderlines
JOIN lake.products USING (product_id)
GROUP BY product_id;
```

### Options

**TARGET_LAG**: Refresh frequency
- Time interval: `'5 minutes'`, `'1 hour'`, `'1 day'`
- Downstream: `'downstream'` (refresh when ANY parent refreshes, in same iteration)

**Downstream lag semantics:**
- With **single parent**: Child refreshes whenever parent refreshes
- With **multiple parents**: Child refreshes when ANY parent refreshes (OR logic)
- Child refresh happens in **same iteration** as parent (not next iteration)
- Example: If parent1 refreshes at 10:00, child also refreshes at 10:00
- Ensures dependent tables are always up-to-date with their sources

**DEDUPLICATION** (optional): Compare before writing
- `true`: Compare old vs new aggregates, skip write if identical (default: `false`)
- Useful for dimension-heavy tables where changes don't affect aggregates
- See [Deduplication Strategy](12-deduplication-strategy.md)

**REFRESH_STRATEGY** (optional): Override auto-detection
- `'AFFECTED_KEYS'` (default for GROUP BY)
- `'FULL'` (recompute everything)

**CARDINALITY_THRESHOLD** (optional): Full refresh threshold (default: `0.3`)
- If `affected_keys / total_rows > threshold`, use full refresh
- Example: `0.5` means switch to full when >50% affected

**Parallel Refresh Options (Phase 4.2):**

**ALLOW_PARALLEL_REFRESH** (optional): Enable distributed refresh (default: `true`)
- `false`: Always use single worker even for large cardinality

**PARALLEL_THRESHOLD** (optional): Min affected keys to parallelize (default: `10000000`)
- Only distribute work if affected keys exceed this threshold

**MAX_PARALLELISM** (optional): Max workers for one refresh (default: `16`)
- Limits how many workers can process one table simultaneously

### Example: High-Cardinality Table

```sql
CREATE DYNAMIC TABLE lake.dynamic.user_events_summary
TARGET_LAG = '10 minutes'
CARDINALITY_THRESHOLD = 0.5  -- Higher threshold before full refresh
PARALLEL_THRESHOLD = 5000000  -- Parallelize at 5M affected keys
MAX_PARALLELISM = 8  -- Use up to 8 workers
AS
SELECT user_id, COUNT(*), SUM(value)
FROM lake.events
GROUP BY user_id;
```

## Submission Methods

### Phase 1-3: CLI Tool (Core Interface)

```bash
# Create
dynamic-tables create -f query.sql

# Create with deduplication
dynamic-tables create --deduplicate -f query.sql

# List
dynamic-tables list

# Describe
dynamic-tables describe customer_metrics

# Alter table to enable deduplication
dynamic-tables alter customer_metrics --deduplicate=true

# Drop
dynamic-tables drop customer_metrics

# Force refresh
dynamic-tables refresh customer_metrics

# Validate query without creating table
dynamic-tables validate -f query.sql
```

### Validation Mode

The `validate` command checks a query without creating the dynamic table:

```bash
# Validate from file
dynamic-tables validate -f customer_metrics.sql

# Validate from stdin
echo "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id" | \
  dynamic-tables validate

# JSON output for programmatic use
dynamic-tables validate -f query.sql --format json
```

**What validation checks:**

✅ **SQL syntax**: Query parses successfully  
✅ **Source tables exist**: All referenced tables are accessible  
✅ **GROUP BY extraction**: Can identify grouping columns  
✅ **Strategy detection**: Determines refresh strategy (AFFECTED_KEYS vs FULL)  
✅ **Unsupported features**: Catches known limitations  

**Example validation output:**

```
✓ Query syntax valid
✓ Source tables: orders, customers (both exist)
✓ Refresh strategy: AFFECTED_KEYS
✓ GROUP BY columns: customer_id
✓ Dependencies: orders, customers

Ready to create dynamic table.
```

**Failed validation example:**

```
✗ Source table not found: nonexistent_table
✗ Unsupported feature: Window function without PARTITION BY
  Line 3: ROW_NUMBER() OVER ()

Cannot create dynamic table.
```

**JSON output format:**

```json
{
  "valid": true,
  "syntax_ok": true,
  "source_tables": ["orders", "customers"],
  "all_sources_exist": true,
  "refresh_strategy": "AFFECTED_KEYS",
  "group_by_columns": ["customer_id"],
  "dependencies": ["orders", "customers"],
  "warnings": [],
  "errors": []
}
```

**When validation fails:**

```json
{
  "valid": false,
  "errors": [
    "Source table 'nonexistent' not found",
    "Unsupported: Window function without PARTITION BY"
  ],
  "warnings": [
    "Large join detected - consider CARDINALITY_THRESHOLD tuning"
  ]
}
```

**Use cases:**

- **Pre-deployment checks**: Validate in CI/CD before deployment
- **Development**: Test queries before committing to CREATE
- **Documentation**: Generate metadata about what a query does
- **Troubleshooting**: Understand why CREATE might fail
```

### Future: DuckDB Extension (Recommended)

```sql
LOAD dynamic_tables;

CREATE DYNAMIC TABLE lake.dynamic.customer_metrics
TARGET_LAG = '5 minutes'
AS SELECT ...;

-- Use in any DuckDB client
SHOW DYNAMIC TABLES;
DROP DYNAMIC TABLE customer_metrics;
```

**Benefits:**
- Native SQL experience in DBeaver, DataGrip, duckdb CLI
- No separate CLI tool needed
- Consistent with DuckDB ecosystem

**Implementation:** Requires C++ extension development

### Future: REST API

```bash
# Create
curl -X POST /api/v1/dynamic-tables \
  -H "Content-Type: application/sql" \
  --data-binary @query.sql

# List
curl /api/v1/dynamic-tables

# Get
curl /api/v1/dynamic-tables/customer_metrics

# Delete
curl -X DELETE /api/v1/dynamic-tables/customer_metrics
```

**Use case:** Programmatic integration when DuckDB extension not desired

## Examples

### Simple Aggregation

```sql
CREATE DYNAMIC TABLE sales_by_region
TARGET_LAG = '10 minutes'
AS
SELECT region, SUM(amount) as total_sales
FROM orders
GROUP BY region;
```

### With Joins

```sql
CREATE DYNAMIC TABLE enriched_orders
TARGET_LAG = '5 minutes'
AS
SELECT o.*, c.name, c.segment
FROM orders o
JOIN customers c ON o.customer_id = c.id;
```

### With Deduplication (Dimension-Heavy)

```sql
-- Product names change frequently, but aggregates by product_id don't
CREATE DYNAMIC TABLE product_sales
TARGET_LAG = '5 minutes'
DEDUPLICATION = true
AS
SELECT 
    ol.product_id,
    SUM(ol.quantity) as total_quantity,
    SUM(ol.quantity * ol.price) as total_revenue
FROM orderlines ol
JOIN products p ON ol.product_id = p.product_id
GROUP BY ol.product_id;
```

### Dependent Table

```sql
CREATE DYNAMIC TABLE high_value_customers
TARGET_LAG = 'downstream'
AS
SELECT *
FROM customer_metrics
WHERE total > 10000;
```

### Force Full Refresh

```sql
CREATE DYNAMIC TABLE complex_report
TARGET_LAG = '1 day'
REFRESH_STRATEGY = 'FULL'
AS
SELECT * FROM orders WHERE complex_conditions(...);
```
