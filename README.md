# Dynamic Tables for DuckLake

Snowflake-style dynamic tables on DuckLake with automatic incremental refresh and configurable lag.

## Quick Overview

**Features:**
- Automatic incremental refresh when sources change
- Configurable lag (`5 minutes`, `1 hour`, `downstream`)
- Bootstrap-aware (initial loads skip CDC, process DAG in order)
- Snapshot isolation for consistency
- Handles complex scenarios (FK updates, N-way joins, denormalization)
- Production-ready for small-to-medium scale (<100 tables, <10TB data)

**Target Scale:**
- Single-worker deployment handles 10-100 dynamic tables
- DuckDB single-node processes up to ~10TB efficiently
- For massive scale (1000+ tables, 100TB+), see Phase 4 future enhancements

## Architecture

**Two-database design:**
- **PostgreSQL**: Metadata (4 core tables for Phases 1-3)
- **DuckLake**: Source data (CDC-enabled), materialized dynamic tables

**Single-worker orchestrator** (Python):
- Polls for stale tables in topological (dependency) order
- Uses DuckLake CDC to detect changes since last refresh
- Applies affected-keys strategy for efficient incremental updates
- Processes entire dependency chains in one iteration

## Key Technical Decisions

1. **Language**: Python (DuckDB integration, SQL parsing via sqlglot)
2. **Refresh Strategy**: Affected keys (extract from GROUP BY, handle via CDC preimage/postimage)
3. **Coordination**: Database-based claims (no leader election) - Phase 4 only
4. **Scaling**: Start simple (single worker, 4 tables), add complexity only when needed (Phase 4)
5. **Consistency**: Snapshot isolation using DuckLake time-travel

## Implementation Approach

- **TDD**: Test-first development for all components
- **Phases**: 
  - **Phase 1**: Core infrastructure (CLI, metadata, full refresh, polling loop)
  - **Phase 2**: Incremental refresh (CDC integration, affected keys, N-way joins, cardinality-based strategy)
  - **Phase 3**: Production essentials (error handling, REST API, monitoring, deduplication, Docker)
  - **Phase 4**: Future distributed architecture (see below)
- **Philosophy**: Build complete single-worker system first (Phases 1-3), add distribution only if needed (Phase 4)

## Documentation

**Core System Design (Phases 1-3 - Single Worker):**

1. [Overview](docs/01-overview.md) - Architecture and components
2. [Multi-Table Joins & Denormalization](docs/02-multi-table-joins.md) - Primary use case: incremental refresh for joins
3. [Snapshot Isolation](docs/03-snapshot-isolation.md) - Consistency guarantees and query rewriting
5. [Testing Strategy](docs/05-testing-strategy.md) - TDD approach and comprehensive test cases
6. [SQL Interface](docs/06-sql-interface.md) - DDL syntax, validation, and submission methods
7. [Metadata Schema](docs/07-metadata-schema.md) - PostgreSQL tables (4 core tables for single worker)
8. [Implementation Phases](docs/08-implementation-phases.md) - Development roadmap and detailed requirements
9. [Aggregation Strategy](docs/09-aggregation-strategy.md) - Special case: single-table GROUP BY aggregations
10. [Transactions & Consistency](docs/10-transactions-consistency.md) - Transactional refresh guarantees
11. [Performance Considerations](docs/11-performance-considerations.md) - Why SQL-first approach is fast
12. [Deduplication Strategy](docs/12-deduplication-strategy.md) - Avoid unnecessary writes with cost analysis
13. [Large Cardinality Handling](docs/13-large-cardinality-handling.md) - Out-of-core processing for huge affected key sets
15. [Worker Configuration](docs/15-worker-configuration.md) - Database connections, CLI args, validation, deployment options

**Future Enhancements (Phase 4 - Distributed/Multi-Worker):**

4. [Multi-Worker Architecture](docs/04-multi-worker-architecture.md) - Kubernetes scaling and coordination (if needed for >500 tables)
14. [Parallel Single-Table Refresh](docs/14-parallel-single-table-refresh.md) - Distribute one refresh across workers (for extreme scale)

## Quick Start

```bash
# Phase 1: Create dynamic table via CLI
# (SQL file contains full CREATE DYNAMIC TABLE statement)
dynamic-tables create -f customer_metrics.sql

# Phase 2: Incremental refresh runs automatically
# (detects changes via CDC, recomputes only affected keys)

# Phase 3: Production deployment
docker run -d dynamic-tables-worker \
  --pg-url postgresql://... \
  --duckdb-path /data/lake.db
```

## SQL Example

**Primary use case: Denormalization (flattening normalized tables)**

```sql
CREATE DYNAMIC TABLE lake.dynamic.order_details
TARGET_LAG = '5 minutes'
AS
SELECT 
    o.order_id,
    o.order_date,
    o.amount,
    c.customer_name,
    c.customer_segment,
    c.customer_region
FROM lake.orders o
JOIN lake.customers c ON o.customer_id = c.customer_id;
GROUP BY customer_id;
```

**What happens automatically:**
1. Detect changes in `orders` via DuckLake CDC
2. Extract affected customer_ids from preimage/postimage
3. Recompute only changed aggregates
4. Maintain snapshot consistency with dependent tables

## Technology Stack

- **Language**: Python 3.11+
- **Metadata**: PostgreSQL 15+
- **Data Lake**: DuckDB 0.10+ with DuckLake extension
- **SQL Parsing**: sqlglot
- **Testing**: pytest, testcontainers
- **Deployment**: Docker (single container)
- **Monitoring**: Prometheus metrics exporter

## Future Enhancements (Phase 4+)

For massive scale scenarios (1000+ tables, 100TB+ data), or alternative interfaces:

**DuckDB Extension:**
- Native `CREATE DYNAMIC TABLE` support in DuckDB
- Works with any DuckDB client (DBeaver, DataGrip, duckdb CLI)
- No separate CLI needed - pure SQL experience
- Requires C++ extension development

**REST API:**
- Programmatic table management
- Integration with other tools
- Useful if DuckDB extension not desired

**Multi-Worker Coordination (Phase 4.1):**
- Kubernetes deployment with HPA autoscaling
- Work queue and claim-based coordination (additional metadata tables)
- Multiple workers process different tables concurrently
- See [Multi-Worker Architecture](docs/04-multi-worker-architecture.md)

**Parallel Single-Table Refresh (Phase 4.2):**
- Distribute one large refresh across multiple workers
- Hash-based key partitioning for >10M affected keys
- See [Parallel Single-Table Refresh](docs/14-parallel-single-table-refresh.md)

**Advanced Scaling (Phase 4.3):**
- Priority-based scheduling
- Load balancing and resource limits
- Out-of-core processing tuning

**Note:** Most use cases are well-served by the Phase 3 single-worker system. Consider Phase 4+ only when you've exhausted single-node capacity or want alternative interfaces.
