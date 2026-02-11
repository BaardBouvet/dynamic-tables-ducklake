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

## Design Rationale: SQL-First

**All table properties are embedded in SQL DDL, not passed as CLI flags.**

Why? As we add properties (`DEDUPLICATION`, `CARDINALITY_THRESHOLD`, `PARALLEL_THRESHOLD`, etc.), CLI flags don't scale. The SQL approach:
- ✅ Scales to any number of properties without CLI changes
- ✅ SQL files are complete, self-documenting, version-controllable
- ✅ Same SQL works across interfaces (CLI → DuckDB extension → REST API)
- ✅ Matches industry patterns (Snowflake, dbt, Terraform)

## Submission Methods

### Phase 1-3: CLI Tool (Core Interface)

The CLI accepts full `CREATE DYNAMIC TABLE` statements.

```bash
# Create from file containing CREATE DYNAMIC TABLE statement
dynamic-tables create -f customer_metrics.sql

# Create from stdin
cat customer_metrics.sql | dynamic-tables create

# List
dynamic-tables list

# Describe
dynamic-tables describe customer_metrics

# Alter table properties
dynamic-tables alter customer_metrics \
  --set "DEDUPLICATION=true" \
  --set "TARGET_LAG='10 minutes'"

# Drop
dynamic-tables drop customer_metrics

# Force refresh
dynamic-tables refresh customer_metrics

# Validate without creating table
dynamic-tables validate -f customer_metrics.sql
```

**Example SQL file (customer_metrics.sql):**
```sql
CREATE DYNAMIC TABLE lake.dynamic.customer_metrics
TARGET_LAG = '5 minutes'
DEDUPLICATION = true
AS
SELECT customer_id, COUNT(*) as order_count, SUM(amount) as total
FROM lake.orders
GROUP BY customer_id;
```

**Why this design:**
- ✅ No CLI flags needed for table properties (scalable as we add more properties)
- ✅ SQL file is complete, self-documenting
- ✅ Can copy/paste directly into DuckDB extension later (future compatibility)
- ✅ Validation can analyze full CREATE statement
- ❌ More verbose than just SELECT query (but worth it for consistency)

### Validation Mode

The `validate` command checks the CREATE DYNAMIC TABLE statement without actually creating the table:

```bash
# Validate from file
dynamic-tables validate -f customer_metrics.sql

# Validate from stdin
cat customer_metrics.sql | dynamic-tables validate

# JSON output for programmatic use
dynamic-tables validate -f customer_metrics.sql --format json
```

**What validation checks:**

✅ **DDL syntax**: CREATE DYNAMIC TABLE statement parses successfully  
✅ **Query syntax**: AS SELECT portion is valid SQL  
✅ **Source tables exist**: All referenced tables are accessible  
✅ **GROUP BY extraction**: Can identify grouping columns (if using AFFECTED_KEYS)  
✅ **Strategy detection**: Determines refresh strategy (AFFECTED_KEYS vs FULL)  
✅ **Property validation**: TARGET_LAG format, thresholds in valid ranges  
✅ **Unsupported features**: Catches known limitations  

**Example validation output:**

```
✓ DDL syntax valid
✓ Query syntax valid
✓ Source tables: orders, customers (both exist)
✓ Refresh strategy: AFFECTED_KEYS
✓ GROUP BY columns: customer_id
✓ Dependencies: orders, customers
✓ Properties: TARGET_LAG='5 minutes', DEDUPLICATION=true

Ready to create dynamic table 'customer_metrics'.
```

**Failed validation example:**

```
✗ Source table not found: nonexistent_table
✗ Unsupported feature: Window function without PARTITION BY
  Line 5: ROW_NUMBER() OVER ()
✗ Invalid property: TARGET_LAG='invalid_value'

Cannot create dynamic table.
```

**JSON output format:**

```json
{
  "valid": true,
  "table_name": "customer_metrics",
  "syntax_ok": true,
  "source_tables": ["orders", "customers"],
  "all_sources_exist": true,
  "refresh_strategy": "AFFECTED_KEYS",
  "group_by_columns": ["customer_id"],
  "dependencies": ["orders", "customers"],
  "properties": {
    "target_lag": "5 minutes",
    "deduplication": true,
    "cardinality_threshold": 0.3
  },
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
    "Unsupported: Window function without PARTITION BY",
    "Invalid TARGET_LAG value: 'invalid_value'"
  ],
  "warnings": [
    "Large join detected - consider CARDINALITY_THRESHOLD tuning"
  ]
}
```

**Use cases:**

- **Pre-deployment checks**: Validate in CI/CD before deployment
- **Development**: Test complete DDL before committing to CREATE
- **Documentation**: Generate metadata about table definition
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
## Implementation: Parsing CREATE DYNAMIC TABLE

The CLI parses the full SQL statement to extract table metadata and query.

### Using sqlglot

```python
import sqlglot
from sqlglot import exp
from typing import Dict, Any

def parse_dynamic_table_ddl(sql: str) -> Dict[str, Any]:
    """
    Parse CREATE DYNAMIC TABLE statement.
    
    Returns:
        {
            'table_name': str,
            'query': str,  # The AS SELECT ... portion
            'properties': {
                'target_lag': str,
                'deduplication': bool,
                'refresh_strategy': str,
                'cardinality_threshold': float,
                # ... other properties
            }
        }
    """
    # Parse SQL
    statements = sqlglot.parse(sql, dialect='duckdb')
    if not statements:
        raise ValueError("No SQL statement found")
    
    stmt = statements[0]
    
    # Check for CREATE DYNAMIC TABLE pattern
    if not isinstance(stmt, exp.Create):
        raise ValueError("Expected CREATE statement")
    
    # Extract table name
    table_name = stmt.this.sql()
    
    # Extract properties (between CREATE ... and AS)
    properties = {}
    for prop in stmt.args.get('properties', []):
        key = prop.name.lower()
        value = prop.value
        
        if key == 'target_lag':
            properties['target_lag'] = value.this  # String literal
        elif key == 'deduplication':
            properties['deduplication'] = value.this.lower() == 'true'
        elif key == 'refresh_strategy':
            properties['refresh_strategy'] = value.this.upper()
        elif key == 'cardinality_threshold':
            properties['cardinality_threshold'] = float(value.this)
        elif key == 'parallel_threshold':
            properties['parallel_threshold'] = int(value.this)
        elif key == 'max_parallelism':
            properties['max_parallelism'] = int(value.this)
    
    # Extract query (the AS SELECT portion)
    query = stmt.expression.sql(dialect='duckdb')
    
    return {
        'table_name': table_name,
        'query': query,
        'properties': properties
    }
```

### Alternative: Simple Regex Parser

For Phase 1, a simple regex-based parser may be sufficient:

```python
import re
from typing import Dict, Any

def parse_dynamic_table_simple(sql: str) -> Dict[str, Any]:
    """Simple regex-based parser for CREATE DYNAMIC TABLE."""
    
    # Extract table name
    table_match = re.search(
        r'CREATE\s+DYNAMIC\s+TABLE\s+([^\s]+)',
        sql,
        re.IGNORECASE
    )
    if not table_match:
        raise ValueError("Invalid CREATE DYNAMIC TABLE syntax")
    table_name = table_match.group(1)
    
    # Extract AS query
    as_match = re.search(r'\bAS\b\s+(SELECT\b.+)', sql, re.IGNORECASE | re.DOTALL)
    if not as_match:
        raise ValueError("Missing AS SELECT clause")
    query = as_match.group(1).strip()
    
    # Extract properties
    properties = {}
    
    # TARGET_LAG
    lag_match = re.search(r"TARGET_LAG\s*=\s*'([^']+)'", sql, re.IGNORECASE)
    if lag_match:
        properties['target_lag'] = lag_match.group(1)
    
    # DEDUPLICATION
    dedup_match = re.search(r'DEDUPLICATION\s*=\s*(true|false)', sql, re.IGNORECASE)
    if dedup_match:
        properties['deduplication'] = dedup_match.group(1).lower() == 'true'
    
    # REFRESH_STRATEGY
    strategy_match = re.search(r"REFRESH_STRATEGY\s*=\s*'([^']+)'", sql, re.IGNORECASE)
    if strategy_match:
        properties['refresh_strategy'] = strategy_match.group(1).upper()
    
    # CARDINALITY_THRESHOLD
    threshold_match = re.search(r'CARDINALITY_THRESHOLD\s*=\s*([0-9.]+)', sql, re.IGNORECASE)
    if threshold_match:
        properties['cardinality_threshold'] = float(threshold_match.group(1))
    
    return {
        'table_name': table_name,
        'query': query,
        'properties': properties
    }
```

### CLI create Command Implementation

```python
def create_command(args):
    """Handle 'dynamic-tables create -f file.sql'"""
    
    # Read SQL file
    with open(args.file, 'r') as f:
        sql = f.read()
    
    # Parse DDL
    parsed = parse_dynamic_table_ddl(sql)
    
    # Validate
    validate_dynamic_table(
        query=parsed['query'],
        properties=parsed['properties']
    )
    
    # Insert into metadata
    conn = connect_metadata_db()
    conn.execute("""
        INSERT INTO dynamic_tables (
            name, query, target_lag, deduplication, 
            refresh_strategy, cardinality_threshold,
            created_at, last_refresh
        ) VALUES (?, ?, ?, ?, ?, ?, NOW(), NULL)
    """, (
        parsed['table_name'],
        parsed['query'],
        parsed['properties'].get('target_lag', '5 minutes'),
        parsed['properties'].get('deduplication', False),
        parsed['properties'].get('refresh_strategy'),
        parsed['properties'].get('cardinality_threshold', 0.3)
    ))
    
    print(f"✓ Created dynamic table: {parsed['table_name']}")
```

### ALTER Command Implementation

For modifying properties without rewriting query:

```bash
# Change multiple properties
dynamic-tables alter customer_metrics \
  --set "DEDUPLICATION=true" \
  --set "TARGET_LAG='10 minutes'" \
  --set "CARDINALITY_THRESHOLD=0.5"
```

```python
def alter_command(args):
    """Handle 'dynamic-tables alter <table> --set <prop=value>'"""
    
    conn = connect_metadata_db()
    
    for setting in args.set:
        # Parse setting (e.g., "DEDUPLICATION=true")
        key, value = setting.split('=', 1)
        key = key.strip().lower()
        value = value.strip()
        
        # Remove quotes if present
        if value.startswith("'") and value.endswith("'"):
            value = value[1:-1]
        
        # Build UPDATE based on property type
        if key == 'deduplication':
            conn.execute(
                "UPDATE dynamic_tables SET deduplication = ? WHERE name = ?",
                (value.lower() == 'true', args.table)
            )
        elif key == 'target_lag':
            conn.execute(
                "UPDATE dynamic_tables SET target_lag = ? WHERE name = ?",
                (value, args.table)
            )
        # ... handle other properties
        
        print(f"✓ Updated {key} = {value}")
```

## Benefits of This Approach

**✅ Scalability:**
- Add new table properties without changing CLI interface
- Just update parser and metadata schema

**✅ SQL-First:**
- SQL files are complete, self-documenting
- Can version control full table definitions
- Easy migration to DuckDB extension later

**✅ Validation:**
- Can validate complete CREATE statement before submitting
- Catch property errors early

**✅ Developer Experience:**
- Familiar SQL syntax (matches Snowflake)
- Copy/paste between different interfaces (CLI, extension, API)

**✅ Migration Path:**
- Same SQL works in CLI (Phase 1-3) and DuckDB extension (Phase 4+)
- Users can switch interfaces without rewriting DDL