# Research Analysis: Related Work Comparison

This document summarizes the analysis of related work in incremental view maintenance and dynamic tables, comparing our DuckLake-based approach with existing systems.

## Papers Analyzed

1. **OpenIVM** (April 2024) - `related-work/openivm-arxiv-2404.16486v1.md`
   - SQL-to-SQL compiler for Incremental View Maintenance
   - Based on DBSP (differential dataflow) theory
   - DuckDB extension + cross-system IVM (PostgreSQL → DuckDB)

2. **Snowflake CHANGES/STREAM** (June 2023) - `3589776.txt/pdf`
   - "What's the Difference? Incremental Processing with Change Queries in Snowflake"
   - Primitives for querying table changes over time
   - Query differentiation framework

3. **Snowflake Dynamic Tables** (April 2025) - `2504.10438v1.txt`
   - "Streaming Democratized: Ease Across the Latency Spectrum"
   - Delayed View Semantics (DVS) formalization
   - Production system with >1M active tables

## Key Research Questions

### Is SQL query rewriting for incremental computation a generic problem worth spinning off?

**Answer: Yes, but existing solutions exist**
- OpenIVM provides SQL-to-SQL compilation based on DBSP
- Snowflake implements query differentiation framework
- Both are general-purpose but have different trade-offs vs our approach

### Should we continue with our affected keys approach?

**Answer: Yes - it's a valid point in the design space**

## Comparison Matrix

| Aspect | OpenIVM | Snowflake DTs | Our System |
|--------|---------|---------------|------------|
| **Algorithm** | DBSP delta tables | Query differentiation | Affected keys extraction |
| **Consistency** | Delta propagation | DVS + snapshot isolation | Snapshot isolation via `AT (VERSION)` |
| **CDC Source** | `table_changes()` or custom | Hidden change-tracking columns | DuckLake `table_changes()` |
| **JOIN Support** | In development | 3-way join derivative | Snapshot-isolated recomputation |
| **Optimization** | Eager materialization | Plan shapes (ADDED_ONLY/MINIMIZE) | Cardinality threshold (30%) |
| **Target** | Academic/research | Cloud multi-tenant | DuckLake single-tenant |

## Our Competitive Advantages

### 1. **Pragmatic Cardinality-Based Optimization**
Snowflake acknowledges (Section 6.4):
> "group-by derivative can be substantially more expensive than full refresh if most keys are modified"

Our 30% threshold implements what they call "future work" - cost-based optimization for incremental vs full refresh.

### 2. **DuckLake-Native Architecture**
- Built on native time-travel (`AT (VERSION)`)
- Leverages DuckLake's CDC as separate layer (cleaner than hidden columns)
- No need to build MVCC from scratch

### 3. **Simpler Conceptual Model**
- Affected keys: extract changed GROUP BY keys → delete → recompute
- Easier to understand and debug than DBSP delta tables or query differentiation
- Still mathematically correct for GROUP BY aggregations

### 4. **Production-Ready for Specific Use Case**
While less general than query differentiation, our approach is optimized for:
- Denormalization pipelines
- Aggregation workloads
- 10-100 tables, <10TB data
- Single-worker deployment

## Where Others Are Ahead

### OpenIVM
- More general (works with multiple SQL dialects)
- Cross-system support (PostgreSQL → DuckDB)
- But: JOIN support still "in development" as of April 2024

### Snowflake Dynamic Tables
- **Target lag concept**: User-friendly scheduling (`TARGET_LAG = '5 minutes'`)
- **Broader SQL coverage**: Window functions, outer joins, lateral flatten
- **Sophisticated scheduling**: Canonical refresh periods, dependency graph orchestration
- **Formal transaction isolation theory**: Delayed View Semantics with derivations
- **Production scale**: >1M tables, thousands of customers

### What We Can Adopt

1. **Target Lag API**
   ```python
   CREATE DYNAMIC TABLE orders_summary
   TARGET_LAG = '5 minutes'  # vs manual refresh
   ```

2. **DOWNSTREAM Propagation**
   - Only refresh when downstream tables need it
   - Reduces unnecessary computation

3. **NO_DATA Refresh Action**
   - Detect when sources unchanged (compare snapshot IDs)
   - Update data timestamp without compute

4. **Append-Only Fast Path**
   - Detect when `table_changes()` only has inserts
   - Skip DELETE step entirely

5. **Error Handling**
   - Skip refreshes when falling behind (don't pile up)
   - Auto-suspend after repeated errors

## Key Insights

### All Three Systems Face Similar Challenges

1. **When incremental is slower than full refresh**
   - Snowflake: "group-by derivative can be expensive if most keys modified"
   - Our solution: 30% cardinality threshold
   - OpenIVM: No optimization mentioned yet

2. **Star schema dimension updates**
   - Snowflake admits: "updating dimension table forces full recompute of facts"
   - This is inherent to IVM, not solvable by algorithm choice

3. **Cost-based optimization is hard**
   - Snowflake: "future work" as of April 2025
   - OpenIVM: Not addressed
   - Ours: Simple but effective heuristic (cardinality ratio)

### Performance Reality Check

From Snowflake's production data:
- **90.8%** of queries use ADDED_ONLY plan (append-only, cheap)
- Only **7.1%** use MINIMIZE plan (expensive delta minimization)
- **50%** complete <500ms, **96%** <1 minute

This validates that **most real-world workloads are append-heavy**, which our affected keys approach handles efficiently.

## Strategic Recommendations

### Continue Current Approach
1. Our affected keys + snapshot isolation is a valid, competitive strategy
2. Optimized for DuckLake's native capabilities
3. Simpler than query differentiation for our target use case

### Consider Adding (Priority Order)

**High Priority:**
1. **Target lag scheduling** - User-friendly vs cron
2. **Append-only detection** - Fast path for insert-only workloads
3. **NO_DATA refresh** - Optimize when sources unchanged

**Medium Priority:**
4. **DOWNSTREAM targets** - Lazy evaluation for dependency chains
5. **Better error handling** - Skip when behind, auto-suspend on repeated errors

**Low Priority:**
6. **Incremental JOIN derivative** - Only if cardinality check shows benefit
7. **Cost-based optimization** - Refine the 30% threshold with statistics

### Don't Pursue

1. **General query differentiation** - Too complex, OpenIVM/Snowflake already exist
2. **Cross-database support** - Stay DuckLake-native for now
3. **DBSP implementation** - Academic interest, not practical advantage

## Conclusion

Our system is **not redundant** with existing work. We occupy a distinct point in the design space:

- **OpenIVM**: Most general, DBSP theory, early stage (JOINs in dev)
- **Snowflake**: Broadest SQL coverage, cloud-scale, sophisticated scheduling
- **Ours**: DuckLake-optimized, pragmatic, production-ready for specific niche

The fact that Snowflake (with unlimited resources) still lists cost-based optimization and star schema handling as "future work" validates that these are hard problems. Our simple cardinality heuristic addresses a real gap.

**Ship it.** Iterate based on user feedback, not competitor features.

## References

- [OpenIVM Paper](https://arxiv.org/html/2404.16486v1) - April 2024
- [Snowflake CHANGES/STREAM](https://dl.acm.org/doi/10.1145/3589776) - June 2023  
- [Snowflake Dynamic Tables](https://dl.acm.org/doi/10.1145/3722212.3724455) - April 2025
- DBSP: "Automatic Incremental View Maintenance" - March 2023
- Materialize, Feldera, RisingWave - Commercial IVM systems

## Open Questions for Future Research

1. Can we formalize when affected keys outperforms query differentiation?
2. What's the optimal cardinality threshold (vs hardcoded 30%)?
3. How to handle star schema dimension updates efficiently?
4. Can we detect monotonic queries for append-only optimization?
5. What's the right abstraction for exposing CDC to users (STREAM-like)?

---
*Last Updated: February 15, 2026*
*Papers Reviewed: OpenIVM, Snowflake CHANGES/STREAM, Snowflake Dynamic Tables*
