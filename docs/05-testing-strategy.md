# Testing Strategy

Test-driven development: write tests first, then implement.

## Test Infrastructure

```python
@pytest.fixture
def postgres_db():
    with PostgresContainer() as postgres:
        yield postgres.get_connection()

@pytest.fixture
def duckdb():
    conn = duckdb.connect(':memory:')
    conn.execute("INSTALL ducklake; LOAD ducklake")
    yield conn
```

## Test Layers

**Unit Tests:** SQL parser, strategy selector, key extraction, work claims  
**Integration Tests:** Metadata CRUD, DuckLake CDC, refresh execution, dependencies  
**End-to-End Tests:** Full refresh cycle, multi-worker, FK updates, cascades

## Critical Test Cases

### 1. FK Update (Most Important)
```python
def test_fk_update_refreshes_both_keys():
    create_dynamic_table("SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id")
    
    update("UPDATE orders SET customer_id = 7 WHERE customer_id = 5")  # Move order
    trigger_refresh()
    
    assert get_count(customer_id=5) == original_5 - 1
    assert get_count(customer_id=7) == original_7 + 1
```

### 2. Concurrent Workers
```python
def test_two_workers_dont_claim_same_table():
    assert worker1.try_claim("table_a") != worker2.try_claim("table_a")

def test_stale_claim_expires():
    worker1.claim("table_a")
    worker1.crash()
    time.sleep(CLAIM_TIMEOUT)
    assert worker2.try_claim("table_a") == True
```

### 3. Snapshot Isolation
```python
def test_snapshot_consistency():
    refresh("metrics")  # reads orders@100
    insert_order(...)   # orders→105
    refresh("report")   # must read orders@100 (from metrics' snapshots)
    assert report_read_snapshot("orders") == 100
```

### 4. Dependency Cascade
```python
def test_child_refreshes_after_parent():
    create_dynamic_table("parent", "SELECT ... FROM orders")
    create_dynamic_table("child", "SELECT ... FROM parent")
    
    insert_order(...)
    trigger_refresh()
    
    assert parent_refreshed_first()
    assert child_contains_new_data()
```

### 5. Bootstrap
```python
def test_bootstrap_dependencies():
    create_dynamic_table("A", "SELECT * FROM orders")
    create_dynamic_table("B", "SELECT * FROM A")
    create_dynamic_table("C", "SELECT * FROM B")
    
    trigger_refresh()  # All new, processed in topological order
    
    assert all_initialized(["A", "B", "C"])
    assert all_have_snapshots(["A", "B", "C"])
```

### 6. Circular Dependencies
```python
def test_circular_dependency_rejected():
    create_dynamic_table("A", "SELECT * FROM B")
    create_dynamic_table("B", "SELECT * FROM orders")
    
    with pytest.raises(CircularDependencyError):
        create_dynamic_table("C", "SELECT * FROM A")
        alter_dynamic_table("A", "SELECT * FROM C")  # Cycle!
```

### 7. Error Handling
```python
def test_retry_with_backoff():
    mock_failure(count=2)
    trigger_refresh()
    assert refresh_attempts == 3
    assert retry_intervals == [1, 2]  # Exponential

def test_partial_chain_failure():
    create_tables("A", "B", "C")  # A→B→C
    mock_refresh_failure("B")
    trigger_refresh()
    
    assert get_status("A") == "SUCCESS"
    assert get_status("B") == "FAILED"
    assert get_status("C") == "SKIPPED"  # Can't proceed with stale B
```

### 8. Schema Changes
```python
def test_source_column_removed():
    create_dynamic_table("metrics", "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id")
    alter_table("orders", "DROP COLUMN customer_id")
    trigger_refresh()
    
    assert get_status("metrics") == "FAILED"
    assert "column not found" in get_last_error("metrics")
```

### 9. Query Validation
```python
def test_validate_unsupported_features():
    query = "SELECT *, ROW_NUMBER() OVER () FROM orders"
    result = validate_query(query)
    assert not result.valid
    assert "unsupported" in result.errors[0]

def test_validate_extracts_group_by():
    query = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id"
    result = validate_query(query)
    assert result.group_by_columns == ["customer_id"]
    assert result.strategy == "AFFECTED_KEYS"
```

### 10. Downstream Lag
```python
def test_downstream_lag_refreshes_with_parent():
    create_dynamic_table("parent", "SELECT * FROM orders", lag="5 min")
    create_dynamic_table("child", "SELECT * FROM parent", lag="downstream")
    
    trigger_refresh_for("parent")
    assert child_also_refreshed()
```

## Coverage Goals

- Unit tests: 90%+
- Integration tests: All critical paths
- E2E tests: Key user scenarios

