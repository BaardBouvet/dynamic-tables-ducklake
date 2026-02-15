# Research Papers & Analysis

This directory contains academic and industry papers related to incremental view maintenance, dynamic tables, and streaming SQL processing, along with comparative analysis.

## Contents

### Papers

- **2404.16486v1.pdf / 2404.16486v1.txt** - "OpenIVM: a SQL-to-SQL Compiler for Incremental Computations" (April 2024)
  - SQL-to-SQL compiler for Incremental View Maintenance
  - Based on DBSP (Database Stream Processor) principles
  - DuckDB extension module implementation
  - Cross-system IVM (PostgreSQL → DuckDB for HTAP)
  - Supports projections, filters, GROUP BY, SUM/COUNT (MIN/MAX/JOIN in development)

- **3589776.pdf / 3589776.txt** - "What's the Difference? Incremental Processing with Change Queries in Snowflake" (June 2023)
  - Snowflake's CHANGES queries and STREAM objects
  - Query differentiation framework
  - Implementation details of CDC primitives in SQL
  - Hidden change-tracking columns for row-level metadata

- **2504.10438v1.pdf / 2504.10438v1.txt** - "Streaming Democratized: Ease Across the Latency Spectrum with Delayed View Semantics and Snowflake Dynamic Tables" (April 2025)
  - Snowflake's Dynamic Tables production system
  - Delayed View Semantics (DVS) formalization
  - Transaction isolation extensions for IVM
  - >1M active tables across thousands of customers
  - Target lag scheduling and automatic orchestration

### Analysis

- **ANALYSIS.md** - Comprehensive comparison of our approach vs related work
  - OpenIVM (DBSP-based SQL-to-SQL compiler)
  - Snowflake CHANGES/STREAM primitives
  - Snowflake Dynamic Tables
  - Strategic recommendations for our system

## Key Takeaways

Our **affected keys + snapshot isolation** approach is a valid, competitive strategy in the IVM design space:

1. **Simpler** than DBSP delta tables or query differentiation
2. **Pragmatic** cardinality-based optimization (30% threshold)
3. **DuckLake-native** leveraging built-in time-travel and CDC
4. **Production-ready** for denormalization and aggregation workloads

We occupy a different niche than existing solutions - optimized for DuckLake, focused on GROUP BY aggregations, with runtime-adaptive strategy selection.

## Related Work Not Yet Analyzed

- DBToaster (2012) - Higher-order IVM (cited by OpenIVM)
- Materialize - Differential Dataflow based streaming DB
- Noria (2018) - Partial IVM for request-serving
- DBSP (2023) - General framework for incrementalization (basis for OpenIVM and Feldera)
- Delta Live Tables (Databricks) - Nonstandard SQL semantics
- RisingWave, Feldera, TimePlus - Modern IVM startups

## Future Reading

Consider analyzing if needed:
- Differential Dataflow papers (McSherry et al.)
- DBSP formalization (Budiu et al. 2023)
- Classic IVM papers (Gupta & Mumick 1999)
