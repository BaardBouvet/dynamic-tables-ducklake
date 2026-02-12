# Multi-Worker Architecture (Future Enhancement)

**Phase 4 - Not Currently Implemented**

The single-worker architecture (Phases 1-3) handles most use cases. Multi-worker coordination is a future enhancement for high-scale deployments.

## Potential Design (Future)

**Phase 4.1: Multiple tables across workers**
- Database-coordinated work claims (no leader election)
- Kubernetes HPA autoscaling based on pending work
- Additional metadata tables: `pending_refreshes`, `refresh_claims`

**Phase 4.2: Single table parallelization**
- Partition large tables across workers
- Parallel key range processing
- See [Parallel Single-Table Refresh](14-parallel-single-table-refresh.md) for details

## Current Recommendation

Use single-worker deployment (Phase 1-3):
- Simpler architecture and operations
- Sufficient for most workloads
- Full feature support (joins, deduplication, cardinality handling)
- See [Worker Configuration](15-worker-configuration.md) for deployment

Consider multi-worker only if:
- Single worker cannot keep up with refresh cadence
- High table count (hundreds) requiring parallel processing
- Willing to accept additional operational complexity
