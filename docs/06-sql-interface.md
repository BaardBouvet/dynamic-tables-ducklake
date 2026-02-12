# SQL Interface

## DDL Syntax

```sql
CREATE DYNAMIC TABLE lake.dynamic.customer_metrics
TARGET_LAG = '5 minutes'
DEDUPLICATION = true
AS
SELECT customer_id, COUNT(*) as order_count, SUM(amount) as total
FROM lake.orders
GROUP BY customer_id;
```

## Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `TARGET_LAG` | string | required | `'5 minutes'`, `'1 hour'`, or `'downstream'` |
| `DEDUPLICATION` | boolean | `false` | Compare before write, skip if unchanged |
| `REFRESH_STRATEGY` | string | auto | `'AUTO'`, `'AFFECTED_KEYS'`, `'FULL'` |
| `CARDINALITY_THRESHOLD` | float | `0.3` | Switch to full refresh when >30% affected |
| `PARALLEL_THRESHOLD` | int | `10000000` | Min affected keys for parallelization (Phase 4) |
| `MAX_PARALLELISM` | int | `16` | Max workers per table (Phase 4) |

**TARGET_LAG = 'downstream'** semantics:
- Single parent: refresh when parent refreshes
- Multiple parents: refresh when ANY parent refreshes (OR logic)
- Happens in same iteration, not next

## CLI Commands

```bash
# Create
dynamic-tables create -f query.sql

# List/inspect
dynamic-tables list
dynamic-tables describe customer_metrics

# Manage
dynamic-tables alter customer_metrics --set "TARGET_LAG='10 minutes'"
dynamic-tables refresh customer_metrics  # Force refresh
dynamic-tables drop customer_metrics

# Validate (without creating)
dynamic-tables validate -f query.sql
dynamic-tables validate -f query.sql --format json
```

## Validation

Checks performed:
- DDL and query syntax
- Source tables exist
- GROUP BY extraction (for affected keys strategy)
- Property values in valid ranges
- Detects unsupported features

**Output:**
```
✓ DDL syntax valid
✓ Source tables: orders, customers (both exist)
✓ Refresh strategy: AFFECTED_KEYS
✓ GROUP BY columns: customer_id  
✓ Properties: TARGET_LAG='5 minutes', DEDUPLICATION=true
```

**Validation failures:**
```
✗ Source table 'nonexistent' not found
✗ Unsupported: Window function without PARTITION BY
✗ Invalid TARGET_LAG value: 'invalid_value'
```

**JSON format** available for CI/CD integration.

## Examples

### Denormalization (Primary Use Case)

```sql
CREATE DYNAMIC TABLE order_details
TARGET_LAG = '5 minutes'
AS
SELECT o.*, c.customer_name, c.customer_segment
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id;
```

### Aggregation

```sql
CREATE DYNAMIC TABLE sales_by_region
TARGET_LAG = '10 minutes'
AS
SELECT region, SUM(amount) as total_sales
FROM orders
GROUP BY region;
```

### Dependent Table

```sql
CREATE DYNAMIC TABLE high_value_customers
TARGET_LAG = 'downstream'
AS
SELECT * FROM customer_metrics WHERE total > 10000;
```

### High Cardinality

```sql
CREATE DYNAMIC TABLE user_events_summary
TARGET_LAG = '10 minutes'
CARDINALITY_THRESHOLD = 0.5
PARALLEL_THRESHOLD = 5000000
MAX_PARALLELISM = 8
AS
SELECT user_id, COUNT(*), SUM(value)
FROM events
GROUP BY user_id;
```

## Implementation

### Parsing DDL

Use `sqlglot` to parse CREATE DYNAMIC TABLE:

```python
def parse_dynamic_table_ddl(sql: str) -> dict:
    stmt = sqlglot.parse_one(sql, dialect='duckdb')
    
    return {
        'table_name': stmt.this.sql(),
        'query': stmt.expression.sql(),
        'properties': extract_properties(stmt)
    }
```

### CLI create Command

```python
def create_command(args):
    sql = open(args.file).read()
    parsed = parse_dynamic_table_ddl(sql)
    validate_dynamic_table(parsed['query'], parsed['properties'])
    
    # Insert into metadata
    conn.execute("""
        INSERT INTO dynamic_tables (name, query, target_lag, ...)
        VALUES (?, ?, ?, ...)
    """, (parsed['table_name'], ...))
```

### ALTER Implementation

```bash
dynamic-tables alter customer_metrics \
  --set "DEDUPLICATION=true" \
  --set "TARGET_LAG='10 minutes'"
```

Updates metadata without rewriting query.

## Design Rationale

**SQL-first approach:**
- Properties in DDL, not CLI flags → scales to any number of properties
- SQL files are complete, version-controllable
- Same SQL works across CLI, DuckDB extension, REST API
- Matches industry patterns (Snowflake, dbt)

## Future Interfaces

**DuckDB Extension** (recommended):
```sql
LOAD dynamic_tables;
CREATE DYNAMIC TABLE ... AS SELECT ...;
SHOW DYNAMIC TABLES;
```

**REST API:**
```bash
curl -X POST /api/v1/dynamic-tables --data-binary @query.sql
```
