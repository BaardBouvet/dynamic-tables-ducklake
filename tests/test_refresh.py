"""Test dynamic table refresh logic."""

from typing import Any, Iterator
import pytest
from dynamic_tables.refresh import DynamicTableRefresher
from dynamic_tables.parser import DynamicTableDefinition


class TestDynamicTableRefresh:
    """Test full refresh functionality."""

    @pytest.fixture
    def refresher(self, metadata_store: Any, duckdb_conn: Any) -> DynamicTableRefresher:
        """Create a refresher instance."""
        return DynamicTableRefresher(metadata_store, duckdb_conn)

    @pytest.fixture
    def sample_source_data(self, duckdb_conn: Any) -> Iterator[None]:
        """Create sample source tables with data."""
        # Drop if exists first
        # Drop any existing sales table (DDL - no transaction)
        try:
            duckdb_conn.execute("DROP TABLE IF EXISTS sales")
        except Exception:
            pass

        # Create table (DDL - no transaction)
        duckdb_conn.execute("""
            CREATE TABLE sales (
                product_id INTEGER,
                amount DECIMAL(10,2),
                sale_date DATE
            );
        """)

        # Insert data (DML - in transaction)
        duckdb_conn.execute("BEGIN TRANSACTION")
        duckdb_conn.execute("""
            INSERT INTO sales VALUES
                (1, 100.00, '2024-01-01'),
                (1, 150.00, '2024-01-02'),
                (2, 200.00, '2024-01-01'),
                (2, 250.00, '2024-01-02');
        """)
        duckdb_conn.execute("COMMIT")

        yield

        # Cleanup (DDL - no transaction)
        try:
            duckdb_conn.execute("DROP TABLE IF EXISTS sales")
        except Exception:
            pass

    def test_create_dynamic_table(self, refresher: Any, sample_source_data: Any) -> None:
        """Test creating a dynamic table."""
        definition = DynamicTableDefinition.create(
            name="sales_summary",
            schema_name="main",
            query_sql="SELECT product_id, SUM(amount) as total_sales FROM sales GROUP BY product_id",
        )
        refresher.create_dynamic_table(definition)

        # Verify it's in metadata
        tables = refresher.list_tables()
        assert len(tables) == 1
        assert tables[0]["name"] == "sales_summary"

    def test_create_duplicate_table(self, refresher: Any, sample_source_data: Any) -> None:
        """Test that creating duplicate table raises error."""
        definition = DynamicTableDefinition.create(
            name="sales_summary",
            schema_name="main",
            query_sql="SELECT product_id, SUM(amount) as total_sales FROM sales GROUP BY product_id",
        )
        refresher.create_dynamic_table(definition)

        # Try to create again
        with pytest.raises(ValueError, match="already exists"):
            refresher.create_dynamic_table(definition)

    def test_create_circular_dependency(self, refresher: Any, sample_source_data: Any) -> None:
        """Test that circular dependencies are detected."""
        # Create table A depending on B
        definition_a = DynamicTableDefinition.create(
            name="table_a", schema_name="main", query_sql="SELECT * FROM table_b"
        )
        refresher.create_dynamic_table(definition_a)

        # Try to create table B depending on A (cycle!)
        definition_b = DynamicTableDefinition.create(
            name="table_b", schema_name="main", query_sql="SELECT * FROM table_a"
        )
        with pytest.raises(ValueError, match="Circular dependency"):
            refresher.create_dynamic_table(definition_b)

    def test_refresh_single_table(
        self, refresher: Any, duckdb_conn: Any, sample_source_data: Any
    ) -> None:
        """Test full refresh of a single table."""
        # Create dynamic table
        definition = DynamicTableDefinition.create(
            name="sales_summary",
            schema_name="main",
            query_sql="SELECT product_id, SUM(amount) as total_sales, COUNT(*) as sale_count FROM sales GROUP BY product_id",
        )
        refresher.create_dynamic_table(definition)

        # Perform refresh
        result = refresher.refresh_tables(["sales_summary"])[0]

        assert result["status"] == "SUCCESS"
        assert result["rows_affected"] == 2  # 2 products
        assert result["duration_ms"] > 0

        # Verify data
        rows = duckdb_conn.execute("""
            SELECT product_id, total_sales, sale_count
            FROM sales_summary
            ORDER BY product_id
        """).fetchall()

        assert len(rows) == 2
        assert rows[0] == (1, 250.00, 2)  # Product 1: 100 + 150
        assert rows[1] == (2, 450.00, 2)  # Product 2: 200 + 250

    def test_refresh_updates_existing_data(
        self, refresher: Any, duckdb_conn: Any, sample_source_data: Any
    ) -> None:
        """Test that refresh replaces existing data."""
        definition = DynamicTableDefinition.create(
            name="sales_summary",
            schema_name="main",
            query_sql="SELECT product_id, SUM(amount) as total_sales FROM sales GROUP BY product_id",
        )
        refresher.create_dynamic_table(definition)

        # First refresh
        refresher.refresh_tables(["sales_summary"])

        # Add more sales (with transaction)
        duckdb_conn.execute("BEGIN TRANSACTION")
        duckdb_conn.execute("""
            INSERT INTO sales VALUES (1, 75.00, '2024-01-03')
        """)
        duckdb_conn.execute("COMMIT")

        # Refresh again
        result = refresher.refresh_tables(["sales_summary"])[0]

        assert result["status"] == "SUCCESS"

        # Verify updated data
        rows = duckdb_conn.execute("""
            SELECT product_id, total_sales
            FROM sales_summary
            WHERE product_id = 1
        """).fetchall()

        assert len(rows) == 1
        assert rows[0][1] == 325.00  # 100 + 150 + 75

    def test_refresh_nonexistent_table(self, refresher: Any) -> None:
        """Test refreshing a table that doesn't exist."""
        with pytest.raises(ValueError, match="does not exist"):
            refresher.refresh_tables(["nonexistent"])

    def test_refresh_tables_in_dependency_order(
        self, refresher: Any, duckdb_conn: Any, sample_source_data: Any
    ) -> None:
        """Test refreshing multiple tables in dependency order."""
        # Create first dynamic table
        refresher.create_dynamic_table(
            DynamicTableDefinition.create(
                name="sales_by_product",
                schema_name="main",
                query_sql="SELECT product_id, SUM(amount) as total FROM sales GROUP BY product_id",
            )
        )

        # Create second dynamic table depending on first
        refresher.create_dynamic_table(
            DynamicTableDefinition.create(
                name="top_products",
                schema_name="main",
                query_sql="SELECT product_id, total FROM sales_by_product WHERE total > 200",
            )
        )

        # Refresh all tables
        results = refresher.refresh_tables()

        assert len(results) == 2

        # Verify order: sales_by_product should be refreshed before top_products
        assert results[0]["table"] == "sales_by_product"
        assert results[1]["table"] == "top_products"

        # Verify both succeeded
        assert results[0]["status"] == "SUCCESS"
        assert results[1]["status"] == "SUCCESS"

        # Verify final data in dependent table
        rows = duckdb_conn.execute("""
            SELECT product_id, total
            FROM top_products
            ORDER BY product_id
        """).fetchall()

        assert len(rows) == 2
        assert rows[0][0] == 1  # Product 1 total: 250
        assert rows[1][0] == 2  # Product 2 total: 450

    def test_drop_table(self, refresher: Any, duckdb_conn: Any, sample_source_data: Any) -> None:
        """Test dropping a dynamic table."""
        refresher.create_dynamic_table(
            DynamicTableDefinition.create(
                name="sales_summary",
                schema_name="main",
                query_sql="SELECT product_id, SUM(amount) as total FROM sales GROUP BY product_id",
            )
        )
        refresher.refresh_tables(["sales_summary"])

        # Drop the table
        refresher.drop_dynamic_table("sales_summary")

        # Verify it's gone from metadata
        tables = refresher.list_tables()
        assert len(tables) == 0

        # Verify table doesn't exist in DuckDB
        table_exists = duckdb_conn.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name = 'sales_summary'
        """).fetchone()[0]

        assert table_exists == 0

    def test_cannot_drop_table_with_dependents(
        self, refresher: Any, sample_source_data: Any
    ) -> None:
        """Test that dropping a table with dependents fails."""
        # Create parent table
        refresher.create_dynamic_table(
            DynamicTableDefinition.create(
                name="parent_table", schema_name="main", query_sql="SELECT * FROM sales"
            )
        )

        # Create child table
        refresher.create_dynamic_table(
            DynamicTableDefinition.create(
                name="child_table", schema_name="main", query_sql="SELECT * FROM parent_table"
            )
        )

        # Try to drop parent
        with pytest.raises(ValueError, match="depend on it"):
            refresher.drop_dynamic_table("parent_table")

    def test_refresh_history_recorded(self, refresher: Any, sample_source_data: Any) -> None:
        """Test that refresh history is properly recorded."""
        refresher.create_dynamic_table(
            DynamicTableDefinition.create(
                name="sales_summary",
                schema_name="main",
                query_sql="SELECT product_id, SUM(amount) as total FROM sales GROUP BY product_id",
            )
        )
        refresher.refresh_tables(["sales_summary"])

        # Check history
        cursor = refresher.metadata.conn.cursor()
        cursor.execute("""
            SELECT dynamic_table, status, strategy_used, rows_affected
            FROM refresh_history
            WHERE dynamic_table = 'sales_summary'
        """)

        row = cursor.fetchone()
        assert row is not None
        assert row[0] == "sales_summary"
        assert row[1] == "SUCCESS"
        assert row[2] == "FULL"
        assert row[3] == 2

    def test_snapshots_captured_during_refresh(
        self, refresher: Any, duckdb_conn: Any, sample_source_data: Any
    ) -> None:
        """Test that source snapshots are captured before query execution."""
        # Get current snapshot of sales table before creating dynamic table
        initial_snapshot = duckdb_conn.execute("""
            SELECT snapshot_id 
            FROM ducklake.snapshots() 
            ORDER BY snapshot_id DESC 
            LIMIT 1
        """).fetchone()[0]

        refresher.create_dynamic_table(
            DynamicTableDefinition.create(
                name="sales_summary",
                schema_name="main",
                query_sql="SELECT product_id, SUM(amount) as total FROM sales GROUP BY product_id",
            )
        )
        refresher.refresh_tables(["sales_summary"])

        # Verify snapshots were captured in source_snapshots table
        cursor = refresher.metadata.conn.cursor()
        cursor.execute("""
            SELECT source_table, last_snapshot
            FROM source_snapshots
            WHERE dynamic_table = 'sales_summary'
        """)

        snapshots = cursor.fetchall()
        assert len(snapshots) == 1
        assert snapshots[0][0] == "sales"  # source_table
        assert snapshots[0][1] >= initial_snapshot  # last_snapshot should be >= initial

        # Verify snapshots were recorded in refresh_history
        cursor.execute("""
            SELECT source_snapshots
            FROM refresh_history
            WHERE dynamic_table = 'sales_summary'
            ORDER BY started_at DESC
            LIMIT 1
        """)

        history_row = cursor.fetchone()
        assert history_row is not None
        source_snapshots_json = history_row[0]
        assert source_snapshots_json is not None
        assert "sales" in source_snapshots_json
        assert source_snapshots_json["sales"] >= initial_snapshot

    def test_snapshots_tracked_for_dependent_tables(
        self, refresher: Any, duckdb_conn: Any, sample_source_data: Any
    ) -> None:
        """Test that snapshots are tracked for dynamic tables that depend on other dynamic tables."""
        # Create first-level dynamic table
        refresher.create_dynamic_table(
            DynamicTableDefinition.create(
                name="sales_summary",
                schema_name="main",
                query_sql="SELECT product_id, SUM(amount) as total FROM sales GROUP BY product_id",
            )
        )
        refresher.refresh_tables(["sales_summary"])

        # Create second-level dynamic table that depends on sales_summary
        refresher.create_dynamic_table(
            DynamicTableDefinition.create(
                name="high_value_products",
                schema_name="main",
                query_sql="SELECT product_id FROM sales_summary WHERE total > 200",
            )
        )
        refresher.refresh_tables(["high_value_products"])

        # Verify that high_value_products has snapshots for both sales_summary and sales
        cursor = refresher.metadata.conn.cursor()
        cursor.execute("""
            SELECT source_table, last_snapshot
            FROM source_snapshots
            WHERE dynamic_table = 'high_value_products'
            ORDER BY source_table
        """)

        snapshots = cursor.fetchall()
        # Should have snapshots for both sales (transitive) and sales_summary (direct)
        assert len(snapshots) == 2
        assert snapshots[0][0] == "sales"
        assert snapshots[1][0] == "sales_summary"

    def test_snapshot_isolation_in_dependency_chain(self, refresher: Any, duckdb_conn: Any) -> None:
        """Test that snapshot isolation ensures consistency in dependency chains.

        Scenario: C depends on both A and B, where B also depends on A.
        When C refreshes, it must read A at the same snapshot that B used,
        otherwise the data from A and B could be inconsistent.
        """
        # Create base table A
        duckdb_conn.execute("""
            CREATE TABLE orders (
                order_id INTEGER,
                amount DECIMAL(10,2)
            )
        """)
        duckdb_conn.execute("INSERT INTO orders VALUES (1, 100), (2, 200)")

        # Create dynamic table B that depends on A
        refresher.create_dynamic_table(
            DynamicTableDefinition.create(
                name="order_summary",
                schema_name="main",
                query_sql="SELECT COUNT(*) as order_count, SUM(amount) as total_amount FROM orders",
            )
        )
        refresher.refresh_tables(["order_summary"])

        # Verify B's results
        result_b = duckdb_conn.execute(
            "SELECT order_count, total_amount FROM order_summary"
        ).fetchone()
        assert result_b[0] == 2  # 2 orders
        assert result_b[1] == 300  # 100 + 200

        # Now create dynamic table C that depends on both A and B
        refresher.create_dynamic_table(
            DynamicTableDefinition.create(
                name="order_validation",
                schema_name="main",
                query_sql="""
                SELECT 
                    os.order_count,
                    os.total_amount,
                    COUNT(*) as actual_count,
                    SUM(o.amount) as actual_amount
                FROM order_summary os
                CROSS JOIN orders o
                GROUP BY os.order_count, os.total_amount
            """,
            )
        )

        # Insert more data into A BEFORE refreshing C
        # This creates the scenario where A has progressed but B hasn't been refreshed yet
        duckdb_conn.execute("INSERT INTO orders VALUES (3, 300)")

        # Refresh C - it should use the SAME snapshot of A that B used,
        # not the current state of A
        refresher.refresh_tables(["order_validation"])

        # Verify C read A at the snapshot that B used (2 orders, 300 total)
        # NOT the current state (3 orders, 600 total)
        result_c = duckdb_conn.execute("""
            SELECT order_count, total_amount, actual_count, actual_amount 
            FROM order_validation
        """).fetchone()

        # If snapshot isolation works correctly:
        # - order_count and total_amount come from B (2, 300)
        # - actual_count and actual_amount come from A at the SAME snapshot B used (2, 300)
        # So they should match
        assert result_c[0] == result_c[2], (
            "order_count should match actual_count (snapshot isolation)"
        )
        assert result_c[1] == result_c[3], (
            "total_amount should match actual_amount (snapshot isolation)"
        )

        # Cleanup
        duckdb_conn.execute("DROP TABLE IF EXISTS orders")

    def test_incremental_refresh_affected_keys(self, refresher: Any, duckdb_conn: Any) -> None:
        """Test incremental refresh using affected keys strategy (Phase 2).

        Simplest possible example:
        1. Create source table with orders
        2. Create dynamic table aggregating by customer_id
        3. Do initial refresh (bootstrap - full refresh)
        4. Update an order to change its customer_id
        5. Do incremental refresh
        6. Verify only affected customers were recomputed
        """
        # Create source table with orders
        duckdb_conn.execute("DROP TABLE IF EXISTS orders")
        duckdb_conn.execute("""
            CREATE TABLE orders (
                order_id INTEGER,
                customer_id INTEGER,
                amount DECIMAL(10,2)
            )
        """)

        # Insert initial data
        duckdb_conn.execute("BEGIN TRANSACTION")
        duckdb_conn.execute("""
            INSERT INTO orders VALUES
                (1, 100, 50.00),
                (2, 100, 75.00),
                (3, 200, 100.00),
                (4, 300, 25.00)
        """)
        duckdb_conn.execute("COMMIT")

        # Create dynamic table that aggregates by customer_id
        refresher.create_dynamic_table(
            DynamicTableDefinition.create(
                name="customer_metrics",
                schema_name="main",
                query_sql="""
                    SELECT 
                        customer_id,
                        COUNT(*) as order_count,
                        SUM(amount) as total_amount
                    FROM orders
                    GROUP BY customer_id
                """,
            )
        )

        # Bootstrap: Initial full refresh
        refresher.refresh_tables(["customer_metrics"])

        # Verify initial state
        results = duckdb_conn.execute("""
            SELECT customer_id, order_count, total_amount 
            FROM customer_metrics 
            ORDER BY customer_id
        """).fetchall()

        assert len(results) == 3
        assert results[0] == (100, 2, 125.00)  # 2 orders, $125 total
        assert results[1] == (200, 1, 100.00)  # 1 order, $100 total
        assert results[2] == (300, 1, 25.00)  # 1 order, $25 total

        # Now UPDATE order #2: change customer from 100 to 400
        # This affects TWO customers: 100 (loses an order) and 400 (gains an order)
        duckdb_conn.execute("BEGIN TRANSACTION")
        duckdb_conn.execute("""
            UPDATE orders 
            SET customer_id = 400 
            WHERE order_id = 2
        """)
        duckdb_conn.execute("COMMIT")

        # Incremental refresh - should only recompute customers 100 and 400
        # (not customers 200 or 300)
        refresher.refresh_tables(["customer_metrics"])

        # Verify updated state
        results = duckdb_conn.execute("""
            SELECT customer_id, order_count, total_amount 
            FROM customer_metrics 
            ORDER BY customer_id
        """).fetchall()

        assert len(results) == 4
        assert results[0] == (100, 1, 50.00)  # Lost order #2, now just order #1
        assert results[1] == (200, 1, 100.00)  # Unchanged
        assert results[2] == (300, 1, 25.00)  # Unchanged
        assert results[3] == (400, 1, 75.00)  # Gained order #2

        # Verify that refresh strategy was INCREMENTAL (not FULL)
        cursor = refresher.metadata.conn.cursor()
        cursor.execute("""
            SELECT strategy_used, affected_keys_count
            FROM refresh_history 
            WHERE dynamic_table = 'customer_metrics'
            ORDER BY started_at DESC
            LIMIT 1
        """)
        result = cursor.fetchone()
        last_strategy = result[0]
        affected_keys_count = result[1]

        assert last_strategy == "INCREMENTAL", "Second refresh should use INCREMENTAL strategy"
        assert affected_keys_count == 2, (
            "Should have exactly 2 affected keys: customer 100 (lost order) and 400 (gained order)"
        )

        # Cleanup
        duckdb_conn.execute("DROP TABLE IF EXISTS orders")
