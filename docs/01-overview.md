# Dynamic Tables for DuckLake - Overview

## Goal

Implement Snowflake-style dynamic tables with configurable lag on DuckLake:
- **Automatic incremental refresh** when source data changes
- **Configurable lag** (e.g., `5 minutes`, `1 hour`, `downstream`)
- **Dependency-aware** refresh scheduling
- **Snapshot isolation** for consistency

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Dynamic Table Workers (Kubernetes Pods)                     │
│  - Poll metadata for pending work                            │
│  - Claim work via database coordination                      │
│  - Execute AFFECTED_KEYS refresh strategy                    │
│  - Maintain snapshot consistency                             │
└───────────────┬─────────────────────────┬───────────────────┘
                │                         │
                ▼                         ▼
    ┌───────────────────────┐  ┌──────────────────────────────┐
    │ Metadata Store        │  │ DuckLake Database            │
    │ (PostgreSQL)          │  │ (DuckDB + DuckLake)          │
    ├───────────────────────┤  ├──────────────────────────────┤
    │ - Table definitions   │  │ - Source tables (CDC)        │
    │ - Snapshot tracking   │  │ - Dynamic tables             │
    │ - Work queue          │  │ - Change feed                │
    │ - Dependencies        │  │ - Snapshot isolation         │
    └───────────────────────┘  └──────────────────────────────┘
```

## Key Components

1. **Metadata Store (PostgreSQL)**: Multi-worker coordination, table definitions, work queue
2. **DuckLake**: Source data with CDC, materialized dynamic tables
3. **Workers (Python)**: Kubernetes pods that claim and process refresh work
4. **Scheduler**: Background process that enqueues pending refreshes

## Core Strategies

**Refresh Strategy**: Affected Keys Recompute
- Extract affected keys from CDC preimage/postimage
- Recompute only changed aggregates
- Handles FK updates, deletes, inserts correctly

**Snapshot Isolation**: 
- Track snapshots per source table
- Read sources at consistent snapshots
- Ensures dependent tables see coherent data

**Work Coordination**:
- Database-based work claims (no leader election)
- Heartbeat + expiry for fault tolerance
- Kubernetes HPA for autoscaling

## Technology Stack

- **Language**: Python (DuckDB integration, SQL parsing)
- **Metadata**: PostgreSQL (multi-worker writes)
- **Data**: DuckDB + DuckLake extension
- **Orchestration**: Kubernetes
- **Testing**: pytest, testcontainers

## Implementation Approach

- **TDD**: Test-first development for all components
- **Core delivery**: CLI tool + background worker (Phases 1-3)
- **Future interfaces**: DuckDB extension (recommended) or REST API
- **Start simple**: Python prototype, proven approach

## Related Documents

- [Affected Keys Strategy](02-affected-keys-strategy.md)
- [Snapshot Isolation](03-snapshot-isolation.md)
- [Multi-Worker Architecture](04-multi-worker-architecture.md)
- [Testing Strategy](05-testing-strategy.md)
- [SQL Interface](06-sql-interface.md)
- [Metadata Schema](07-metadata-schema.md)
- [Implementation Phases](08-implementation-phases.md)
- [Multi-Table Joins](09-multi-table-joins.md)
