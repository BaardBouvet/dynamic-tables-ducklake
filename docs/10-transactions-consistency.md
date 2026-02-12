# Transaction Handling and Consistency

All refresh operations are transactional to ensure consumers never see partial/missing data. DuckLake provides full ACID support with snapshot isolation.

## The Problem

**Without transactions, consumers see inconsistent state:**
```sql
DELETE FROM customer_metrics WHERE customer_id IN (5, 7);
-- Consumer query here: missing aggregates!
INSERT INTO customer_metrics SELECT ...;
-- Consumer query here: correct data
```

## The Solution

**Wrap all refreshes in transactions:**
```sql
BEGIN TRANSACTION;
DELETE FROM customer_metrics WHERE customer_id IN (5, 7);
INSERT INTO customer_metrics SELECT ... WHERE customer_id IN (5, 7) GROUP BY customer_id;
COMMIT;  -- Atomically creates new DuckLake snapshot
```

**Guarantees:** Consumers see either old data (before COMMIT) or new data (after COMMIT), never partial updates.

## Implementation

```python
def refresh_affected_keys(conn, dynamic_table, affected_keys):
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(f"DELETE FROM {table} WHERE {key_predicate}")
        conn.execute(f"INSERT INTO {table} {refresh_query}")
        conn.execute("COMMIT")
    except Exception as e:
        conn.execute("ROLLBACK")
        raise
```

**Full refresh option 1 (TRUNCATE+INSERT):**
```sql
BEGIN TRANSACTION;
TRUNCATE TABLE customer_metrics;
INSERT INTO customer_metrics SELECT ...;
COMMIT;
```

**Full refresh option 2 (CREATE+SWAP, zero downtime):**
```python
# Build in temp table (no transaction needed)
conn.execute("CREATE TABLE customer_metrics_temp AS SELECT ...")

# Atomic swap
conn.execute("BEGIN TRANSACTION")
conn.execute("DROP TABLE customer_metrics")
conn.execute("ALTER TABLE customer_metrics_temp RENAME TO customer_metrics")
conn.execute("COMMIT")
```

## Isolation  

DuckLake provides snapshot isolation via MVCC:
- Each transaction sees consistent snapshot
- Readers never block writers, writers never block readers
- Each `BEGIN-COMMIT` creates one atomic DuckLake snapshot

## Built-in Retry

DuckLake automatically retries transaction conflicts:
```python
conn.execute("SET ducklake_max_retry_count = 10")      # Default
conn.execute("SET ducklake_retry_wait_ms = 100")       # Default: 100ms
conn.execute("SET ducklake_retry_backoff = 1.5")       # Default: 1.5x exponential
```

No manual retry needed for most cases.

## Long-Running Refreshes

For large refreshes that would block too long, use shadow table pattern:

```python
# Build shadow table (old data + new data)
conn.execute("CREATE TEMP TABLE shadow AS SELECT * FROM customer_metrics WHERE key NOT IN (affected_keys)")
conn.execute("INSERT INTO shadow SELECT ... WHERE key IN (affected_keys)")

# Atomic swap
conn.execute("BEGIN TRANSACTION")
conn.execute("DROP TABLE customer_metrics")
conn.execute("ALTER TABLE shadow RENAME TO customer_metrics")
conn.execute("COMMIT")
```

Original table remains readable during recompute, atomic swap at end.

## Time Travel

DuckLake snapshots enable debugging:
```sql
-- Query at specific snapshot
SELECT * FROM customer_metrics FOR SYSTEM_TIME AS OF SNAPSHOT 42;

-- Query at timestamp
SELECT * FROM customer_metrics FOR SYSTEM_TIME AS OF TIMESTAMP '2026-02-11 10:30:00';
```

## Best Practices

- Always use transactions for DELETE+INSERT pattern
- Keep transactions short (<5 minutes)
- Use shadow tables for large full refreshes (zero downtime)
- Monitor transaction duration
- Partition large tables to enable per-partition refresh

## Test Cases

```python
def test_transactional_consistency():
    start_refresh_async()
    time.sleep(0.1)  # Refresh in progress
    result = query_dynamic_table()
    assert result == original_data  # Not partial!
    
    wait_for_refresh()
    assert query_dynamic_table() == expected_new_data

def test_rollback_on_error():
    with pytest.raises(RefreshError):
        refresh_with_simulated_error()
    assert query_dynamic_table() == original_data  # Intact
```

