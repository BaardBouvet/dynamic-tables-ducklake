# Dynamic Tables SQL Examples

This directory contains example SQL files demonstrating various dynamic table configurations.

## Usage

All examples contain complete `CREATE DYNAMIC TABLE` statements. Run them with:

```bash
dynamic-tables create -f <example-file.sql>
```

## Examples

### [customer_metrics.sql](customer_metrics.sql)
Basic aggregation with deduplication enabled.
- **Pattern:** Simple GROUP BY aggregation
- **Features:** `DEDUPLICATION = true`
- **Use case:** Customer summary metrics that update frequently

### [product_sales.sql](product_sales.sql)
Join with dimension table where dimension changes don't affect aggregates.
- **Pattern:** JOIN + GROUP BY
- **Features:** `DEDUPLICATION = true`
- **Use case:** Product dimension (name, description) changes frequently, but aggregate uses product_id only
- **Benefit:** 80-95% deduplication ratio typical

### [high_value_customers.sql](high_value_customers.sql)
Downstream dependent table with `'downstream'` lag.
- **Pattern:** Query depends on another dynamic table
- **Features:** `TARGET_LAG = 'downstream'`
- **Use case:** Filter/transform results from parent dynamic table
- **Behavior:** Refreshes automatically whenever parent refreshes

### [high_cardinality_table.sql](high_cardinality_table.sql)
Tuned for large datasets with custom thresholds.
- **Pattern:** High-cardinality GROUP BY (millions of keys)
- **Features:**
  - `CARDINALITY_THRESHOLD = 0.5` - Switch to full refresh at 50% affected
  - `PARALLEL_THRESHOLD = 5000000` - Parallelize at 5M affected keys
  - `MAX_PARALLELISM = 8` - Use up to 8 workers
- **Use case:** User-level or event-level aggregations at scale

### [chained_tables_example.sql](chained_tables_example.sql)
Three-table dependency chain demonstrating complex relationships.
- **Pattern:** Multi-level dependencies (A→B→C, A→C)
- **Tables:**
  - `customer_totals` - Base aggregation from orders
  - `premium_customers` - Filter on customer_totals
  - `premium_customer_insights` - Joins both customer_totals and orders
- **Features:** `TARGET_LAG = 'downstream'` propagation
- **Use case:** Layered analytics with shared base tables

## Property Reference

| Property | Type | Default | Purpose |
|----------|------|---------|---------|
| `TARGET_LAG` | string | required | Refresh frequency (`'5 minutes'`, `'1 hour'`, `'downstream'`) |
| `DEDUPLICATION` | boolean | `false` | Skip writes when values unchanged |
| `REFRESH_STRATEGY` | string | auto | Force `'FULL'` or `'AFFECTED_KEYS'` |
| `CARDINALITY_THRESHOLD` | float | `0.3` | Switch to full refresh when >30% keys affected |
| `PARALLEL_THRESHOLD` | int | `10000000` | Min keys to enable parallel refresh (Phase 4) |
| `MAX_PARALLELISM` | int | `16` | Max workers for one table (Phase 4) |

## Testing Patterns

```bash
# Validate before creating
dynamic-tables validate -f customer_metrics.sql

# Create table
dynamic-tables create -f customer_metrics.sql

# Check creation
dynamic-tables describe customer_metrics

# Force immediate refresh
dynamic-tables refresh customer_metrics

# Modify properties
dynamic-tables alter customer_metrics \
  --set "TARGET_LAG='10 minutes'" \
  --set "DEDUPLICATION=false"

# Drop table
dynamic-tables drop customer_metrics
```

## Development Workflow

1. **Write SQL file** with complete CREATE DYNAMIC TABLE statement
2. **Validate** syntax and properties: `dynamic-tables validate -f ...`
3. **Create** table: `dynamic-tables create -f ...`
4. **Monitor** refresh behavior and metrics
5. **Tune** properties based on observed deduplication ratios and cardinality
6. **Iterate** with ALTER commands to optimize configuration

## Migration Path

These SQL files work with:
- ✅ CLI tool (current implementation)
- ✅ Future DuckDB extension (same syntax)
- ✅ REST API (POST SQL body)

No rewriting needed when switching interfaces!
