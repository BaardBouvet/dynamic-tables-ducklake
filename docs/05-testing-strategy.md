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

### Retry and Error Handling

```python
def test_retry_with_exponential_backoff():
    """Verify failed refresh retries with backoff."""
    # Given: Dynamic table
    create_dynamic_table("metrics", "SELECT...")
    
    # When: First refresh fails (transient error)
    mock_duckdb_connection.fail_next_n_queries(2)
    trigger_refresh()
    
    # Then: Should retry with backoff
    assert refresh_attempts == 3  # Initial + 2 retries
    assert retry_intervals == [1, 2]  # Exponential backoff: 1s, 2s

def test_partial_dependency_chain_failure():
    """If B fails in A→B→C chain, what happens to C?"""
    create_tables("A", "B", "C")  # A→B→C dependency
    
    # When: B refresh fails
    mock_refresh_failure("B")
    trigger_refresh()
    
    # Then: A succeeds, B fails, C skipped (can't proceed with stale B)
    assert get_status("A") == "SUCCESS"
    assert get_status("B") == "FAILED"
    assert get_status("C") == "SKIPPED"  # Or PENDING for next iteration

def test_max_retries_exceeded():
    """Persistent failures mark table as FAILED."""
    create_dynamic_table("metrics", "SELECT...")
    
    # When: All retries exhausted
    mock_duckdb_connection.fail_all_queries()
    trigger_refresh()
    
    # Then: Table marked FAILED
    assert get_table_status("metrics") == "FAILED"
    assert get_last_error("metrics").contains("Max retries exceeded")
```

### Circular Dependency Detection

```python
def test_circular_dependency_rejected():
    """CREATE fails if it creates a cycle."""
    # Given: A depends on B
    create_dynamic_table("A", "SELECT * FROM B")
    create_dynamic_table("B", "SELECT * FROM orders")
    
    # When: Try to make C depend on A and make A depend on C
    with pytest.raises(CircularDependencyError):
        create_dynamic_table("C", "SELECT * FROM A")
        alter_dynamic_table("A", "SELECT * FROM C")  # Would create cycle
    
def test_self_reference_rejected():
    """Table cannot reference itself."""
    with pytest.raises(CircularDependencyError):
        create_dynamic_table("A", "SELECT * FROM A")

def test_complex_cycle_detected():
    """Detect cycles in complex graphs."""
    # A → B → C → D → B (cycle)
    create_dynamic_table("A", "SELECT * FROM orders")
    create_dynamic_table("B", "SELECT * FROM A")
    create_dynamic_table("C", "SELECT * FROM B")
    
    # This would create a cycle
    with pytest.raises(CircularDependencyError):
        create_dynamic_table("D", "SELECT * FROM C")
        alter_dynamic_table("B", "SELECT * FROM D")
```

### Bootstrap and Initialization

```python
def test_concurrent_bootstrap_dependencies():
    """Bootstrap A→B→C where all are new."""
    # Given: Empty system
    create_dynamic_table("A", "SELECT * FROM orders")
    create_dynamic_table("B", "SELECT * FROM A")
    create_dynamic_table("C", "SELECT * FROM B")
    
    # When: First refresh iteration
    trigger_refresh()
    
    # Then: All initialized in topological order
    assert get_data("A") is not None
    assert get_data("B") is not None
    assert get_data("C") is not None
    
    # Snapshots recorded for all
    assert get_source_snapshots("A") == {"orders": some_snapshot}
    assert get_source_snapshots("B") == {"A": some_snapshot}
    assert get_source_snapshots("C") == {"B": some_snapshot}

def test_bootstrap_snapshot_timing():
    """Verify snapshots captured before query execution."""
    # Given: Orders table at snapshot 100
    orders_snapshot = get_current_snapshot("orders")  # 100
    
    # When: Create and bootstrap dynamic table
    create_dynamic_table("metrics", "SELECT * FROM orders")
    
    # Insert data after table created but before refresh
    insert_order(customer=999)  # advances to snapshot 101
    
    # Trigger refresh
    trigger_refresh()
    
    # Then: Should use snapshot from before bootstrap, not current
    # This test verifies we capture snapshot before running query
    recorded_snapshot = get_source_snapshot("metrics", "orders")
    assert recorded_snapshot >= orders_snapshot
```

### Schema Change Handling

```python
def test_source_column_removed_fails_gracefully():
    """Gracefully handle column removal from source."""
    # Given: Dynamic table using orders.customer_id
    create_dynamic_table("metrics", 
        "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id")
    trigger_refresh()  # Works fine
    
    # When: Source schema changes (column removed)
    alter_table("orders", "DROP COLUMN customer_id")
    trigger_refresh()
    
    # Then: Refresh fails with clear error
    assert get_table_status("metrics") == "FAILED"
    assert "column not found" in get_last_error("metrics").lower()

def test_source_column_type_changed():
    """Handle column type changes."""
    create_dynamic_table("metrics", "SELECT customer_id::INT FROM orders")
    
    # Change customer_id from INT to VARCHAR
    alter_table("orders", "ALTER COLUMN customer_id TYPE VARCHAR")
    
    # Refresh should fail or handle conversion
    trigger_refresh()
    # Behavior depends on type compatibility

def test_source_table_dropped():
    """Handle source table deletion."""
    create_dynamic_table("metrics", "SELECT * FROM orders")
    drop_table("orders")
    
    trigger_refresh()
    
    assert get_table_status("metrics") == "FAILED"
    assert "table not found" in get_last_error("metrics").lower()
```

### Snapshot Isolation (Enhanced)

```python
def test_snapshot_isolation_multi_source():
    """Verify consistent snapshots across 3+ source tables."""
    # Create base tables at different snapshots
    insert_data("orders", ...)     # snapshot 100
    insert_data("customers", ...)  # snapshot 50
    insert_data("products", ...)   # snapshot 75
    
    # Create dynamic table from all three
    create_dynamic_table("summary",
        """SELECT o.*, c.name, p.description
           FROM orders o
           JOIN customers c ON o.customer_id = c.id
           JOIN products p ON o.product_id = p.id""")
    
    trigger_refresh()  # Records snapshots: orders@100, customers@50, products@75
    
    # Advance all tables
    insert_data("orders", ...)     # → snapshot 120
    insert_data("customers", ...)  # → snapshot 70
    insert_data("products", ...)   # → snapshot 90
    
    # Create dependent table
    create_dynamic_table("rollup", "SELECT * FROM summary WHERE ...")
    trigger_refresh()
    
    # Verify: rollup reads orders@100, customers@50, products@75
    # (same snapshots summary was built from)
    snapshots = get_recorded_snapshots("rollup")
    assert snapshots["orders"] == 100
    assert snapshots["customers"] == 50
    assert snapshots["products"] == 75

def test_snapshot_consistency_with_query_rewrite():
    """Test query rewriting adds snapshot clauses correctly."""
    # Given: Dynamic table with multiple sources
    query = """
        SELECT o.customer_id, c.name, COUNT(*)
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        GROUP BY o.customer_id, c.name
    """
    create_dynamic_table("metrics", query)
    trigger_refresh()  # Establishes snapshots
    
    # When: Refresh with affected keys
    insert_order(customer=5)
    rewritten_query = prepare_refresh_query("metrics", affected_keys=[5])
    
    # Then: Verify snapshot clauses present
    assert "FOR SYSTEM_TIME AS OF SNAPSHOT" in rewritten_query
    assert rewritten_query.count("FOR SYSTEM_TIME") == 2  # Both tables
```

### Downstream Lag Semantics

```python
def test_downstream_lag_single_parent():
    """TARGET_LAG = 'downstream' refreshes after parent."""
    create_dynamic_table("parent", "SELECT * FROM orders", lag="5 minutes")
    create_dynamic_table("child", "SELECT * FROM parent", lag="downstream")
    
    # When: Parent refreshes
    trigger_refresh_for("parent")
    
    # Then: Child should also refresh in same iteration
    assert get_last_refresh_time("child") == get_last_refresh_time("parent")

def test_downstream_lag_multiple_parents():
    """Downstream with multiple parents: refresh when ANY parent refreshes."""
    create_dynamic_table("parent1", "SELECT * FROM orders", lag="5 min")
    create_dynamic_table("parent2", "SELECT * FROM products", lag="10 min")
    create_dynamic_table("child", 
        "SELECT * FROM parent1 JOIN parent2", lag="downstream")
    
    # When: Only parent1 refreshes (parent2 hasn't hit lag)
    trigger_refresh_for("parent1")
    
    # Then: Child should refresh (at least one parent refreshed)
    assert was_refreshed_in_iteration("child") == True
```

### Query Validation

```python
def test_validate_detects_unsupported_features():
    """Validation catches unsupported query patterns."""
    query = "SELECT *, ROW_NUMBER() OVER () FROM orders"  # Window without PARTITION BY
    
    result = validate_dynamic_table_query(query)
    
    assert result.valid == False
    assert "unsupported" in result.errors[0].lower()
    assert "window function" in result.errors[0].lower()

def test_validate_checks_source_tables_exist():
    """Validation verifies source tables exist."""
    query = "SELECT * FROM nonexistent_table"
    
    result = validate_dynamic_table_query(query)
    
    assert result.valid == False
    assert "table not found" in result.errors[0].lower()

def test_validate_group_by_extraction():
    """Validation extracts GROUP BY columns successfully."""
    query = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id"
    
    result = validate_dynamic_table_query(query)
    
    assert result.valid == True
    assert result.group_by_columns == ["customer_id"]
    assert result.refresh_strategy == "AFFECTED_KEYS"
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
