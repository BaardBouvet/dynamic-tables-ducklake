# Testing Strategy

## Test-Driven Development

Write tests first, then implement features.

## Test Infrastructure

```python
# Pytest fixtures
@pytest.fixture
def postgres_db():
    """PostgreSQL testcontainer for metadata"""
    with PostgresContainer() as postgres:
        yield postgres.get_connection()

@pytest.fixture
def duckdb():
    """In-memory DuckDB with DuckLake"""
    conn = duckdb.connect(':memory:')
    conn.execute("INSTALL ducklake")
    conn.execute("LOAD ducklake")
    yield conn
```

## Test Layers

### Unit Tests
- SQL parser (extract GROUP BY)
- Strategy selector (AFFECTED_KEYS vs FULL)
- Key extraction from change feed
- Work claim logic

### Integration Tests
- Metadata CRUD operations
- DuckLake CDC queries
- Refresh execution
- Dependency resolution

### End-to-End Tests
- Full refresh cycle
- Multi-worker scenarios
- FK update handling
- Cascading refresh

## Critical Test Cases

### FK Update (Most Important)

```python
def test_fk_update_refreshes_both_keys():
    # Given
    create_dynamic_table("""
        SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id
    """)
    
    # When: Order moves from customer 5 to 7
    update("UPDATE orders SET customer_id = 7 WHERE id = 123")
    trigger_refresh()
    
    # Then
    assert get_count(customer_id=5) == original_5 - 1
    assert get_count(customer_id=7) == original_7 + 1
```

### Concurrent Workers

```python
def test_two_workers_dont_claim_same_table():
    schedule_refresh("table_a")
    
    assert worker1.try_claim("table_a") != worker2.try_claim("table_a")
```

### Stale Claim Recovery

```python
def test_stale_claim_expires():
    worker1.claim("table_a")
    worker1.crash()
    time.sleep(CLAIM_TIMEOUT)
    
    assert worker2.try_claim("table_a") == True
```

### Snapshot Isolation

```python
def test_snapshot_consistency():
    refresh("metrics")  # reads orders@100
    insert_order(...)   # orders now at 105
    refresh("report")   # must read orders@100, not 105
    
    assert report_read_snapshot("orders") == 100
```

### Dependency Cascade

```python
def test_child_refreshes_after_parent():
    create_dynamic_table("parent", "SELECT ... FROM orders")
    create_dynamic_table("child", "SELECT ... FROM parent")
    
    insert_order(...)
    trigger_refresh()
    
    assert parent_refreshed_first()
    assert child_contains_new_data()
```

## Test Data Fixtures

```python
@pytest.fixture
def sample_orders():
    return [
        {"order_id": 1, "customer_id": 5, "amount": 100},
        {"order_id": 2, "customer_id": 7, "amount": 200},
    ]

@pytest.fixture
def sample_dynamic_table():
    return {
        "name": "customer_metrics",
        "query": "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id",
        "target_lag": "5 minutes"
    }
```

## Coverage Goals

- **Unit tests**: 90%+ coverage
- **Integration tests**: All critical paths
- **E2E tests**: Key user scenarios
