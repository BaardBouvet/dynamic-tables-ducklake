# Dynamic Tables DuckLake - Feature Gap Analysis & TODO

**Date:** February 11, 2026  
**Analysis:** Comparison against Snowflake Dynamic Tables feature set

This document tracks features that users would expect in a first release based on industry standards (Snowflake Dynamic Tables).

---

## ✅ Already Covered in Current Plan

The following Snowflake features are already well-covered:
- ✅ Incremental refresh with affected keys strategy
- ✅ Full refresh fallback
- ✅ TARGET_LAG (time-based and DOWNSTREAM)
- ✅ Dependency tracking and topological ordering
- ✅ Snapshot isolation for consistency
- ✅ Bootstrap/initialization handling
- ✅ Circular dependency detection
- ✅ Automatic refresh scheduling
- ✅ Deduplication (opt-in optimization)
- ✅ Multi-table joins with change propagation
- ✅ High cardinality handling with adaptive strategies
- ✅ Monitoring metrics (Prometheus/Grafana)

---

## 🔴 CRITICAL GAPS (Must Have for First Release)

### 1. SUSPEND/RESUME Commands ⭐⭐⭐
**Status:** Missing  
**Priority:** CRITICAL  
**Phase:** Should be in Phase 3

**What's Missing:**
```sql
ALTER DYNAMIC TABLE <name> SUSPEND;
ALTER DYNAMIC TABLE <name> RESUME;
```

**Why Critical:**
- Users need to pause refreshes for maintenance windows
- Cost control: stop compute consumption when not needed
- Debugging: freeze state to investigate issues
- Standard pattern in all modern data platforms

**Implementation Notes:**
- Add `scheduling_state` column to `dynamic_tables` table (RUNNING/SUSPENDED)
- Update worker poll logic to skip SUSPENDED tables
- Add suspend/resume commands to CLI
- Resume should cascade downstream (unless manually suspended)
- Suspend should cascade downstream automatically

**Related Docs to Update:**
- `06-sql-interface.md` - Add ALTER DYNAMIC TABLE syntax
- `07-metadata-schema.md` - Add scheduling_state column
- `08-implementation-phases.md` - Add to Phase 3

---

### 2. Manual REFRESH Trigger ⭐⭐⭐
**Status:** Missing  
**Priority:** CRITICAL  
**Phase:** Should be in Phase 3

**What's Missing:**
```sql
ALTER DYNAMIC TABLE <name> REFRESH;
```

**Why Critical:**
- Essential for testing: verify refresh logic works
- Manual control: force update outside schedule
- Recovery: re-run failed refreshes
- Development workflow: immediate feedback

**Implementation Notes:**
- Add `manual_refresh` flag to work queue
- Priority over scheduled refreshes
- Update refresh_history with `refresh_trigger` = 'MANUAL' vs 'SCHEDULED'
- Should refresh upstream dependencies at consistent snapshots

**Related Docs to Update:**
- `06-sql-interface.md` - Add ALTER DYNAMIC TABLE REFRESH syntax
- `08-implementation-phases.md` - Add to Phase 3

---

### 3. Enhanced SHOW/List Output ⭐⭐⭐
**Status:** Incomplete  
**Priority:** CRITICAL  
**Phase:** Should be in Phase 3

**What's Missing:**
Current `list` command should return much richer metadata:
- `scheduling_state` - RUNNING/SUSPENDED/FAILED
- `last_refresh_time` - When last refresh completed
- `last_refresh_status` - SUCCESS/FAILED/RUNNING
- `current_lag` - Actual staleness
- `target_lag` - Configured target
- `refresh_mode` - INCREMENTAL/FULL/AUTO
- `refresh_mode_reason` - Why mode was chosen
- `data_timestamp` - Freshness of materialized data
- `last_error` - Most recent error message (if any)
- `rows` - Row count
- `bytes` - Approximate size

**Why Critical:**
- Users can't monitor health without visibility
- Troubleshooting requires state inspection
- Operational dashboards need status
- Standard expectation from Snowflake users

**Implementation Notes:**
- Enhance `dynamic-tables list` output format
- Add `--format json` for programmatic access
- Consider `dynamic-tables status <name>` for detailed view
- Join data from dynamic_tables + refresh_history

**Related Docs to Update:**
- `06-sql-interface.md` - Document enhanced output
- `07-metadata-schema.md` - Ensure all needed columns exist
- `08-implementation-phases.md` - Add to Phase 3

---

### 4. Refresh History Query Interface ⭐⭐⭐
**Status:** Exists but not documented  
**Priority:** CRITICAL  
**Phase:** Should be in Phase 3

**What's Missing:**
- Documented way for users to query `refresh_history` table
- CLI command: `dynamic-tables history <name>`
- Error details and troubleshooting information

**Example Output:**
```
Refresh History for customer_metrics:
┌─────────────────────┬──────────┬──────────┬─────────┬────────────┬─────────────┐
│ refresh_timestamp   │ status   │ duration │ trigger │ rows_affected│ error      │
├─────────────────────┼──────────┼──────────┼─────────┼────────────┼─────────────┤
│ 2026-02-11 10:30:00 │ SUCCESS  │ 2.3s     │ SCHEDULED│ 1,234      │ NULL       │
│ 2026-02-11 10:25:00 │ SUCCESS  │ 2.1s     │ SCHEDULED│ 892        │ NULL       │
│ 2026-02-11 10:20:00 │ FAILED   │ 0.5s     │ SCHEDULED│ 0          │ OOM error  │
└─────────────────────┴──────────┴──────────┴─────────┴────────────┴─────────────┘
```

**Why Critical:**
- Can't troubleshoot failures without error visibility
- Performance analysis requires duration tracking
- Audit trail for compliance
- Standard feature users expect

**Implementation Notes:**
- Add `dynamic-tables history <name> [--limit N]` command
- Show last N refreshes (default 10)
- Include error messages, stack traces (stored in metadata)
- Add `refresh_trigger` column to refresh_history (MANUAL/SCHEDULED)

**Related Docs to Update:**
- `06-sql-interface.md` - Document history command
- `07-metadata-schema.md` - Add refresh_trigger, error_details columns
- `08-implementation-phases.md` - Add to Phase 3

---

## 🟡 HIGH PRIORITY (Should Have for First Release)

### 5. INITIALIZE Control ⭐⭐
**Status:** Not explicitly specified  
**Priority:** HIGH  
**Phase:** Should be in Phase 1/2

**What's Missing:**
```sql
CREATE DYNAMIC TABLE ... 
INITIALIZE = ON_CREATE | ON_SCHEDULE
AS SELECT ...
```

**Options:**
- `ON_CREATE` (default): Synchronous initialization, immediate feedback, creation fails if init fails
- `ON_SCHEDULE`: Async initialization, table created empty, first refresh scheduled

**Why High Priority:**
- Large tables: users don't want to wait for sync init
- Testing: ON_CREATE gives immediate error feedback
- Production: ON_SCHEDULE avoids blocking deployments
- Snowflake standard pattern

**Implementation Notes:**
- Add `INITIALIZE` property to DDL parser
- Default to ON_CREATE for backward compatibility
- ON_SCHEDULE: Skip initial query execution, set state to PENDING_INIT
- Add initialization status to metadata

**Related Docs to Update:**
- `06-sql-interface.md` - Add INITIALIZE to CREATE syntax
- `08-implementation-phases.md` - Add to Phase 1 or 2

---

### 6. REFRESH_MODE Explicit Control ⭐⭐
**Status:** Partially covered, not in SQL syntax  
**Priority:** HIGH  
**Phase:** Should be in Phase 2

**What's Missing:**
```sql
CREATE DYNAMIC TABLE ...
REFRESH_MODE = AUTO | INCREMENTAL | FULL
AS SELECT ...
```

**Modes:**
- `AUTO` (default): System chooses best mode, can change
- `INCREMENTAL`: Enforce incremental, fail if not supported
- `FULL`: Always full refresh

**Why High Priority:**
- Validation: Users want to ensure incremental works
- Performance control: Force full when beneficial
- Debugging: Test specific modes
- Forward compatibility: Lock in known-good mode

**Implementation Notes:**
- Add to DDL syntax (currently only mentioned in ALTER)
- Store in metadata
- Add `refresh_mode_reason` to capture why AUTO made its choice
- Show in `list` output

**Related Docs to Update:**
- `06-sql-interface.md` - Add REFRESH_MODE to CREATE/ALTER syntax
- `07-metadata-schema.md` - Add refresh_mode_reason column
- `08-implementation-phases.md` - Ensure in Phase 2

---

### 7. CREATE OR REPLACE ⭐⭐
**Status:** Missing  
**Priority:** HIGH  
**Phase:** Should be in Phase 3

**What's Missing:**
```sql
CREATE OR REPLACE DYNAMIC TABLE ...
```

**Why High Priority:**
- Standard DDL pattern users expect
- CI/CD: Idempotent deployments
- Development workflow: Easy iteration
- Matches CREATE TABLE pattern

**Implementation Notes:**
- If table exists: DROP + CREATE in transaction
- Preserve grants/permissions if possible
- Document that it triggers reinitialization
- Alternative: Keep current `create --replace` CLI flag

**Related Docs to Update:**
- `06-sql-interface.md` - Add OR REPLACE syntax
- `08-implementation-phases.md` - Add to Phase 3

---

### 8. Structured Error Reporting ⭐⭐
**Status:** Basic error handling mentioned  
**Priority:** HIGH  
**Phase:** Phase 3

**What's Missing:**
- Error codes and categories
- Detailed error context (which source table, which key)
- Retry eligibility classification
- User-friendly error messages

**Error Categories:**
```
- DEPENDENCY_ERROR: Upstream table missing/failed
- SCHEMA_ERROR: Column type mismatch
- RESOURCE_ERROR: OOM, timeout
- QUERY_ERROR: Invalid SQL
- PERMISSION_ERROR: Access denied
- CONFLICT_ERROR: DuckLake transaction conflict
```

**Implementation Notes:**
- Define error taxonomy
- Store error_code, error_message, error_details in refresh_history
- Surface in CLI output and logs
- Add to monitoring metrics (error count by type)

**Related Docs to Update:**
- `05-testing-strategy.md` - Test error scenarios
- `07-metadata-schema.md` - Error columns
- New doc: Error handling guide

---

## 🟢 MEDIUM PRIORITY (Nice to Have for First Release)

### 9. Table Comments ⭐
**Status:** Not mentioned  
**Priority:** MEDIUM

```sql
CREATE DYNAMIC TABLE ...
COMMENT = 'Description of this table'
AS SELECT ...
```

**Implementation:** Add `comment` column to dynamic_tables, show in list/describe

---

### 10. RENAME Operation ⭐
**Status:** Not mentioned  
**Priority:** MEDIUM

```sql
ALTER DYNAMIC TABLE <old_name> RENAME TO <new_name>;
```

**Implementation:** Update metadata, handle dependency updates, cascade to dependents

---

### 11. Enhanced Validation Mode ⭐
**Status:** Basic validation exists  
**Priority:** MEDIUM

**Expand validation to check:**
- Source tables have change tracking enabled
- Required permissions on sources
- Memory requirements estimation
- Query complexity analysis

---

### 12. Observability Integration ⭐
**Status:** Prometheus metrics planned  
**Priority:** MEDIUM

**Add:**
- Grafana dashboard templates
- Alert rule templates (lag violations, failures)
- Log aggregation patterns
- Tracing for distributed refresh (Phase 4)

---

### 13. Recursive CTE Support ⭐
**Status:** Not supported  
**Priority:** MEDIUM  
**Phase:** Should be in Phase 2

**What's Missing:**
Support for `WITH RECURSIVE` common table expressions:

```sql
CREATE DYNAMIC TABLE org_hierarchy
TARGET_LAG = '1 hour'
REFRESH_MODE = FULL  -- Recursive queries require full refresh
AS
WITH RECURSIVE hierarchy AS (
  SELECT employee_id, manager_id, name, 1 as level
  FROM employees
  WHERE manager_id IS NULL
  
  UNION ALL
  
  SELECT e.employee_id, e.manager_id, e.name, h.level + 1
  FROM employees e
  JOIN hierarchy h ON e.manager_id = h.employee_id
)
SELECT * FROM hierarchy;
```

**Why Medium Priority:**
- Common SQL pattern for hierarchical data (org charts, bill of materials, graphs)
- Users expect standard SQL features to work
- Many analytics use cases need recursion (path finding, graph traversal)

**Current Behavior:**
- Validation would fail if it tries to extract GROUP BY from recursive CTE
- Falls back to full refresh (if forced)
- No explicit testing or documentation

**Implementation Notes:**
- Detect `WITH RECURSIVE` during SQL parsing (sqlglot)
- Force `REFRESH_STRATEGY = 'FULL'` automatically
- Cannot use affected keys strategy (no clear grouping keys)
- Change propagation: Single row change can cascade through entire recursion
- Add validation to detect and warn about recursive queries
- Test CDC + recursive CTE interactions

**Validation Behavior:**
```
✓ DDL syntax valid
✓ Query syntax valid (recursive CTE detected)
! Refresh strategy: FULL (recursive queries cannot use AFFECTED_KEYS)
! Warning: Recursive CTEs refresh entire table on any source change
✓ Source tables: employees (exists)
```

**Related Docs to Update:**
- `02-affected-keys-strategy.md` - Document recursive CTE limitation
- `06-sql-interface.md` - Add recursive CTE example and limitations
- `05-testing-strategy.md` - Add test cases for recursive queries
- `08-implementation-phases.md` - Add to Phase 2 validation

**Alternative Approach (Future):**
- For bounded recursion depth, could potentially use affected keys at top level
- Advanced: Detect which hierarchy levels are affected and limit recomputation
- Defer to Phase 4+ enhancement

---

### 14. UNION Query Support ⭐⭐
**Status:** Not explicitly addressed  
**Priority:** MEDIUM  
**Phase:** Should be in Phase 2

**What's Missing:**
Support for queries using `UNION` or `UNION ALL`:

```sql
-- Pattern 1: UNION with top-level GROUP BY
CREATE DYNAMIC TABLE category_totals
TARGET_LAG = '10 minutes'
AS
SELECT category, SUM(amount) as total
FROM (
  SELECT 'sales' as category, amount FROM sales_table
  UNION ALL
  SELECT 'refunds' as category, amount FROM refunds_table
)
GROUP BY category;

-- Pattern 2: UNION of pre-aggregated results  
CREATE DYNAMIC TABLE regional_sales
TARGET_LAG = '1 hour'
AS
SELECT region, SUM(total) as grand_total
FROM (
  SELECT region, SUM(amount) as total FROM orders_na GROUP BY region
  UNION ALL
  SELECT region, SUM(amount) as total FROM orders_eu GROUP BY region
)
GROUP BY region;

-- Pattern 3: Simple UNION without GROUP BY (vertical partitioning)
CREATE DYNAMIC TABLE all_orders
TARGET_LAG = '5 minutes'
REFRESH_MODE = FULL  -- No GROUP BY, requires full refresh
AS
SELECT * FROM orders_2024
UNION ALL
SELECT * FROM orders_2025;
```

**Why Medium Priority:**
- Common pattern for combining data from multiple sources
- Used for category/type-based aggregations
- Vertical partitioning scenarios (time-based, regional)
- Expected SQL feature users may attempt

**Current Behavior:**
- Unclear how affected keys extraction handles UNION branches
- When `sales_table` changes, system must know it only affects `category = 'sales'`
- Nested GROUP BY (inside UNION branches) not addressed in docs
- May silently fall back to full refresh or fail validation

**Implementation Challenges:**
1. **Branch tracking**: Determining which UNION branch contains changed source table
2. **Key extraction**: Extracting affected keys when GROUP BY follows UNION
3. **Nested aggregation**: Handling GROUP BY inside UNION branches + outer GROUP BY
4. **Multiple source mapping**: One branch may reference multiple tables with different changes

**Implementation Notes:**
- Detect `UNION`/`UNION ALL` during SQL parsing (sqlglot)
- Build mapping: source_table → UNION branch → affected keys
- For Pattern 1 (GROUP BY after UNION): Extract keys from appropriate branch
- For Pattern 2 (nested GROUP BY): May require full refresh or advanced branch-specific recomputation
- For Pattern 3 (no GROUP BY): Force `REFRESH_STRATEGY = 'FULL'`
- Add validation to detect and categorize UNION patterns

**Validation Behavior:**
```
✓ DDL syntax valid
✓ Query syntax valid (UNION detected)
! Refresh strategy: FULL (UNION with nested GROUP BY not supported for incremental)
! Warning: UNION queries may require full refresh for correctness
✓ Source tables: sales_table, refunds_table (all exist)
```

**Test Cases Needed:**
- UNION ALL + top-level GROUP BY with single source change
- UNION of pre-aggregated results with nested GROUP BY
- UNION without GROUP BY (verify forces full refresh)
- Mixed: UNION with JOINs inside branches
- Multiple UNIONs (3+ branches)

**Related Docs to Update:**
- `02-affected-keys-strategy.md` - Document UNION limitations and patterns
- `06-sql-interface.md` - Add UNION examples and refresh strategy implications
- `05-testing-strategy.md` - Add test cases for UNION queries
- `08-implementation-phases.md` - Add to Phase 2 validation

**Alternative Approach (Simple):**
- Phase 2: Detect UNION, force `REFRESH_MODE = FULL`
- Phase 4+: Advanced - Branch-specific incremental refresh if feasible
- Recommendation: Start with full refresh for UNION queries, add optimization later if needed

---

### 15. Other Problematic Query Patterns ⭐⭐
**Status:** Not explicitly documented  
**Priority:** MEDIUM  
**Phase:** Should be in Phase 2 (validation)

**What's Missing:**
Detection and handling of various SQL patterns that don't support incremental refresh:

**15.1 DISTINCT without GROUP BY**
```sql
-- No grouping keys to extract - requires full refresh
CREATE DYNAMIC TABLE unique_customers AS
SELECT DISTINCT customer_id, name, email
FROM customers;
```
- Cannot determine affected keys without explicit GROUP BY
- Force `REFRESH_MODE = FULL`

**15.2 Non-deterministic Functions**
```sql
-- Results change even without source data changes
CREATE DYNAMIC TABLE daily_snapshot AS
SELECT *, CURRENT_DATE as snapshot_date, RANDOM() as sample_id
FROM orders;
```
- Functions like `RANDOM()`, `NOW()`, `UUID()`, `CURRENT_DATE` produce different results each execution
- Even with no CDC changes, query produces different results
- Should this trigger refreshes? Probably requires full refresh on schedule, not CDC

**15.3 Set Operations (INTERSECT, EXCEPT)**
```sql
CREATE DYNAMIC TABLE active_dormant_customers AS
SELECT customer_id FROM all_customers
EXCEPT
SELECT customer_id FROM inactive_customers;
```
- Similar to UNION - can't easily determine which branch changed
- Force `REFRESH_MODE = FULL`

**15.4 LIMIT/OFFSET without ORDER BY**
```sql
-- Non-deterministic results
CREATE DYNAMIC TABLE sample_orders AS
SELECT * FROM orders LIMIT 1000;
```
- Results vary between executions even without data changes
- With ORDER BY, still problematic: affected row might leave/enter the top N
- Likely inappropriate for dynamic tables, but should detect and warn

**15.5 Time-based Filters (Moving Windows)**
```sql
-- Results change as time advances, even without source changes
CREATE DYNAMIC TABLE recent_orders AS
SELECT * FROM orders
WHERE order_date >= CURRENT_DATE - INTERVAL '7 days';
```
- Query is "moving window" - results change daily even if orders table is static
- Should refresh on schedule, not just CDC events
- Needs special handling: time-based refresh + CDC-based refresh

**15.6 Correlated Subqueries**
```sql
CREATE DYNAMIC TABLE customer_max_order AS
SELECT customer_id, (
  SELECT MAX(amount) 
  FROM orders o 
  WHERE o.customer_id = c.customer_id
) as max_order
FROM customers c;
```
- Complex dependency: changes in orders affect which customers need recomputation
- May work with current approach if properly parsed
- Needs testing to verify affected keys extraction

**15.7 Window Functions without PARTITION BY**
```sql
-- Already mentioned as unsupported in validation
CREATE DYNAMIC TABLE ranked_orders AS
SELECT *, ROW_NUMBER() OVER (ORDER BY amount DESC) as rank
FROM orders;
```
- No partition key = entire result depends on all rows
- Any change affects all rows → force `REFRESH_MODE = FULL`
- Already detected in validation (good!)

**15.8 QUALIFY Clause (DuckDB-specific)**
```sql
CREATE DYNAMIC TABLE top_customer_per_region AS
SELECT region, customer_id, revenue,
       ROW_NUMBER() OVER (PARTITION BY region ORDER BY revenue DESC) as rn
FROM customer_revenue
QUALIFY rn = 1;  -- DuckDB's filtering for window functions
```
- Similar to window function handling
- If PARTITION BY exists, use partition keys for affected keys
- Needs explicit support in sqlglot parsing

**15.9 PIVOT/UNPIVOT**
```sql
CREATE DYNAMIC TABLE sales_pivot AS
PIVOT customers 
ON region 
USING SUM(sales);
```
- Complex transformation, difficult to track affected keys
- Likely force `REFRESH_MODE = FULL`
- Less common, may be Phase 3+

**15.10 Queries Without Grouping Keys (Pure Filters)**
```sql
-- No aggregation, just filtering
CREATE DYNAMIC TABLE high_value_orders AS
SELECT * FROM orders
WHERE amount > 1000;
```
- No GROUP BY, no aggregation
- Affected rows = all changed orders, can track via CDC
- Actually simpler than GROUP BY case!
- WHERE filter can be AND-ed with affected keys from CDC

**Implementation Priority:**
1. **High**: DISTINCT without GROUP BY, Set operations, Non-deterministic functions, Time-based filters
2. **Medium**: Correlated subqueries (may already work), LIMIT without ORDER BY  
3. **Low**: PIVOT/UNPIVOT (uncommon), QUALIFY (if DuckDB-specific features used)

**Validation Approach (Phase 2):**
```python
def detect_query_pattern(query_sql):
    parsed = sqlglot.parse_one(query_sql)
    
    issues = []
    
    # Check for non-deterministic functions
    if has_function(parsed, ['RANDOM', 'NOW', 'CURRENT_DATE', 'UUID']):
        issues.append("Non-deterministic function detected - results vary without data changes")
    
    # Check for DISTINCT without GROUP BY
    if parsed.find(exp.Distinct) and not parsed.find(exp.Group):
        issues.append("DISTINCT without GROUP BY - requires full refresh")
    
    # Check for set operations
    if parsed.find_all([exp.Intersect, exp.Except]):
        issues.append("Set operations (INTERSECT/EXCEPT) require full refresh")
    
    # Check for LIMIT without ORDER BY
    if parsed.find(exp.Limit) and not parsed.find(exp.Order):
        issues.append("LIMIT without ORDER BY produces non-deterministic results")
    
    # Check for time-based filters
    if has_time_comparison(parsed):
        issues.append("Time-based filter detected - may need schedule-based refresh")
    
    return issues
```

**Related Docs to Update:**
- `02-affected-keys-strategy.md` - Document all unsupported/problematic patterns
- `06-sql-interface.md` - Add examples of what NOT to do
- `05-testing-strategy.md` - Add negative test cases
- `08-implementation-phases.md` - Add pattern detection to Phase 2

---

## ⏸️ DEFERRED (Not Critical for First Release)

These Snowflake features can wait for later releases:

- ❌ **CLONE** - Clone existing dynamic table
- ❌ **SWAP** - Atomic swap of two tables
- ❌ **TRANSIENT** property - Reduced durability for lower cost
- ❌ **Clustering keys** - Physical optimization
- ❌ **Search optimization** - Advanced indexing
- ❌ **Immutability constraints** - Complex partial refresh feature
- ❌ **Data governance policies** - Row access, masking, aggregation policies
- ❌ **Iceberg tables** - Specific integration
- ❌ **Backup/restore** - Disaster recovery
- ❌ **Per-table warehouse** - Resource isolation (may not apply to DuckDB architecture)
- ❌ **Time Travel** - Historical queries (depends on DuckLake support)
- ❌ **COPY GRANTS** - Permission management
- ❌ **Tag support** - Metadata organization
- ❌ **LOG_LEVEL per table** - Fine-grained logging

---

## 📋 Implementation Checklist

### Immediate (Before First Release)

- [ ] **Phase 1 additions:**
  - [ ] Add INITIALIZE = ON_CREATE | ON_SCHEDULE to DDL
  - [ ] Add scheduling_state column to metadata
  - [ ] Document CREATE OR REPLACE (or keep as CLI flag)

- [ ] **Phase 2 additions:**
  - [ ] Add REFRESH_MODE to DDL syntax
  - [ ] Add refresh_mode_reason tracking
  - [ ] Implement mode auto-selection explanation
  - [ ] Add recursive CTE detection in validation
  - [ ] Force FULL refresh for WITH RECURSIVE queries
  - [ ] Test CDC + recursive CTE interactions
  - [ ] Add UNION/UNION ALL detection in validation
  - [ ] Determine UNION query refresh strategy (likely FULL for Phase 2)
  - [ ] Test UNION queries with GROUP BY variations
  - [ ] Add detection for problematic query patterns:
    - [ ] DISTINCT without GROUP BY
    - [ ] Non-deterministic functions (RANDOM, NOW, etc.)
    - [ ] Set operations (INTERSECT, EXCEPT)
    - [ ] LIMIT without ORDER BY
    - [ ] Time-based filters (moving windows)
    - [ ] Window functions without PARTITION BY (already done)
  - [ ] Document refresh strategy for each pattern (FULL vs incremental)

- [ ] **Phase 3 additions:**
  - [ ] Implement SUSPEND/RESUME commands
  - [ ] Implement manual REFRESH trigger
  - [ ] Enhance list/status output (scheduling_state, lag, last_refresh, etc.)
  - [ ] Add `history` command to query refresh_history
  - [ ] Add refresh_trigger column (MANUAL/SCHEDULED)
  - [ ] Add error_code, error_message, error_details to refresh_history
  - [ ] Implement structured error reporting
  - [ ] Add table comments support
  - [ ] Add RENAME operation
  - [ ] Enhanced validation mode

### Documentation Updates Required

- [ ] Update `06-sql-interface.md` with:
  - [ ] SUSPEND/RESUME syntax
  - [ ] REFRESH syntax
  - [ ] INITIALIZE property
  - [ ] REFRESH_MODE property
  - [ ] Enhanced list/status output format
  - [ ] history command
  - [ ] COMMENT property
  - [ ] RENAME syntax
  - [ ] Recursive CTE examples and limitations
  - [ ] UNION query examples and refresh strategy implications
  - [ ] Problematic query patterns and workarounds (DISTINCT, non-deterministic functions, etc.)

- [ ] Update `07-metadata-schema.md` with:
  - [ ] scheduling_state column
  - [ ] refresh_mode_reason column
  - [ ] refresh_trigger column
  - [ ] error_code, error_message, error_details columns
  - [ ] comment column

- [ ] Update `08-implementation-phases.md` with:
  - [ ] Redistribute features across phases based on priority
  - [ ] Add critical gaps to appropriate phases
  - [ ] Add recursive CTE validation to Phase 2

- [ ] Update `02-multi-table-joins.md` with:
  - [ ] Document recursive CTE limitation (requires FULL refresh)
  - [ ] Document UNION query patterns and limitations
  - [ ] Document all problematic query patterns (comprehensive reference)

- [ ] Update `09-aggregation-strategy.md` with:
  - [ ] Document when simple aggregations are appropriate vs joins

- [ ] Update `05-testing-strategy.md` with:
  - [ ] Add test cases for recursive CTE queries
  - [ ] Add test cases for UNION queries (various patterns)
  - [ ] Add negative test cases for unsupported patterns (DISTINCT, non-deterministic, etc.)

- [ ] Create new documentation:
  - [ ] Error handling and troubleshooting guide
  - [ ] Operational runbook (with SUSPEND/RESUME workflows)
  - [ ] Monitoring and alerting guide

---

## 🎯 Success Criteria

The first release should enable users to:

1. ✅ **Create** dynamic tables with minimal friction
2. ✅ **Monitor** health and status effectively
3. ✅ **Control** refresh timing (schedule + manual + pause)
4. ✅ **Troubleshoot** failures with detailed error information
5. ✅ **Optimize** performance with refresh mode control
6. ✅ **Operate** in production with confidence (suspend/resume)

---

## 📞 Open Questions

1. **Resource Control:** Should we support per-table DuckDB settings (memory_limit, threads)?
   - Snowflake has per-table warehouses
   - DuckDB single-process model may not need this
   - Could be Phase 4+ enhancement

2. **SQL vs CLI:** Should DDL operations (SUSPEND/RESUME/REFRESH) be:
   - SQL-first with CLI wrapper? (more Snowflake-like)
   - CLI-only? (simpler for Phase 1-3)
   - **Recommendation:** CLI for Phase 1-3, consider DuckDB extension for SQL in future

3. **Change Tracking:** Should we auto-enable change tracking on source tables?
   - Snowflake requires manual enable
   - We could auto-enable for better UX
   - **Recommendation:** Auto-enable with warning, document requirement

4. **Initialization Timeout:** Should ON_CREATE have a timeout?
   - Large tables might take hours to initialize
   - **Recommendation:** Add configurable timeout (default: 1 hour)

---

## 📚 References

- Snowflake Dynamic Tables Documentation: https://docs.snowflake.com/en/user-guide/dynamic-tables-about
- Snowflake CREATE DYNAMIC TABLE: https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table
- Snowflake ALTER DYNAMIC TABLE: https://docs.snowflake.com/en/sql-reference/sql/alter-dynamic-table
- Current Project Documentation: `docs/` directory
