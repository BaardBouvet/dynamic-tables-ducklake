# Multi-Worker Architecture

**Phase 4.1 Feature**

## Design Goals

- Multiple workers process different tables in parallel (Phase 4.1)
- Parallel processing of single table across workers (Phase 4.2) - see [Parallel Single-Table Refresh](14-parallel-single-table-refresh.md)
- No leader election needed
- Database handles coordination
- Kubernetes HPA for autoscaling
- Fault tolerant (claim expiry)

## Work Claim Pattern

### Tables

```sql
-- Pending work
CREATE TABLE pending_refreshes (
    dynamic_table VARCHAR PRIMARY KEY,
    next_refresh_due TIMESTAMP,
    priority INT DEFAULT 0
);

-- Active claims
CREATE TABLE refresh_claims (
    dynamic_table VARCHAR PRIMARY KEY,
    worker_id VARCHAR NOT NULL,
    claimed_at TIMESTAMP NOT NULL,
    heartbeat_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP NOT NULL
);
```

### Claim Flow

1. **Worker polls** for available work:
   ```sql
   SELECT dynamic_table FROM pending_refreshes
   WHERE dynamic_table NOT IN (SELECT dynamic_table FROM refresh_claims)
   LIMIT 1
   ```
let's stick with duckdb and no distributed workload, maybe this means we could simplify this 
2. **Worker attempts claim**:
   ```sql
   INSERT INTO refresh_claims 
   VALUES ('table_a', 'worker-123', NOW(), NOW(), NOW() + INTERVAL '5 minutes')
   ON CONFLICT DO NOTHING
   ```
   - If INSERT succeeds → claim acquired
   - If INSERT fails → another worker claimed it

3. **Worker processes refresh**:
   - Updates `heartbeat_at` every 30 seconds
   - Prevents claim from expiring

4. **Worker releases claim**:
   ```sql
   DELETE FROM refresh_claims WHERE dynamic_table = 'table_a'
   DELETE FROM pending_refreshes WHERE dynamic_table = 'table_a'
   ```

### Stale Claim Recovery

Background job removes expired claims:
```sql
DELETE FROM refresh_claims 
WHERE heartbeat_at < NOW() - INTERVAL '2 minutes'
```

Work becomes available again for other workers.

## Kubernetes Autoscaling

### HPA Configuration

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: dynamic-table-workers
spec:
  scaleTargetRef:
    name: dynamic-table-workers
  minReplicas: 2
  maxReplicas: 20
  metrics:
  - type: External
    external:
      metric:
        name: pending_refresh_count
      target:
        type: AverageValue
        averageValue: "5"
```

### Metrics Exposed

Workers expose metrics via Prometheus:
- `pending_refresh_count`: Rows in `pending_refreshes`
- `refresh_duration_p95`: P95 latency
- `refresh_backlog_seconds`: Age of oldest pending work

### Scaling Behavior

- **Scale up**: When pending work accumulates
- **Scale down**: When work queue empty
- **Scale to zero**: Possible when no work (cost savings)

## Work Scheduler

Separate lightweight process (CronJob or singleton):
```python
while True:
    # Find tables needing refresh
    tables = db.execute("""
        SELECT name FROM dynamic_tables
        WHERE NOW() >= next_refresh_due
          AND status = 'ACTIVE'
    """)
    
    # Enqueue work
    for table in tables:
        db.execute("""
            INSERT INTO pending_refreshes (dynamic_table, next_refresh_due)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """, (table.name, table.next_refresh_due))
    
    sleep(60)
```

## Benefits

- **No coordination overhead**: Database ACID handles conflicts
- **Fault tolerant**: Crashed workers → claims expire → work continues
- **Horizontally scalable**: Add workers as needed
- **Cost efficient**: Scale to zero when idle
