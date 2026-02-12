"""Test dynamic table refresh logic."""

from typing import Any, Iterator
import pytest
from dynamic_tables.refresh import DynamicTableRefresher
from dynamic_tables.parser import DDLParser


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
        ddl = """
        CREATE DYNAMIC TABLE sales_summary
        TARGET_LAG = '5 minutes'
        AS
        SELECT product_id, SUM(amount) as total_sales
        FROM sales
        GROUP BY product_id
        """

        definition = DDLParser.parse(ddl)
        refresher.create_dynamic_table(definition)

        # Verify it's in metadata
        tables = refresher.list_tables()
        assert len(tables) == 1
        assert tables[0]["name"] == "sales_summary"
        assert tables[0]["status"] == "ACTIVE"

    def test_create_duplicate_table(self, refresher: Any, sample_source_data: Any) -> None:
        """Test that creating duplicate table raises error."""
        ddl = """
        CREATE DYNAMIC TABLE sales_summary
        TARGET_LAG = '5 minutes'
        AS
        SELECT product_id, SUM(amount) as total_sales
        FROM sales
        GROUP BY product_id
        """

        definition = DDLParser.parse(ddl)
        refresher.create_dynamic_table(definition)

        # Try to create again
        with pytest.raises(ValueError, match="already exists"):
            refresher.create_dynamic_table(definition)

    def test_create_circular_dependency(self, refresher: Any, sample_source_data: Any) -> None:
        """Test that circular dependencies are detected."""
        # Create table A depending on B
        ddl_a = """
        CREATE DYNAMIC TABLE table_a
        TARGET_LAG = '5 minutes'
        AS
        SELECT * FROM table_b
        """

        definition_a = DDLParser.parse(ddl_a)
        refresher.create_dynamic_table(definition_a)

        # Try to create table B depending on A (cycle!)
        ddl_b = """
        CREATE DYNAMIC TABLE table_b
        TARGET_LAG = '5 minutes'
        AS
        SELECT * FROM table_a
        """

        definition_b = DDLParser.parse(ddl_b)
        with pytest.raises(ValueError, match="Circular dependency"):
            refresher.create_dynamic_table(definition_b)

    def test_refresh_table(self, refresher: Any, duckdb_conn: Any, sample_source_data: Any) -> None:
        """Test full refresh of a table."""
        # Create dynamic table
        ddl = """
        CREATE DYNAMIC TABLE sales_summary
        TARGET_LAG = '5 minutes'
        AS
        SELECT product_id, SUM(amount) as total_sales, COUNT(*) as sale_count
        FROM sales
        GROUP BY product_id
        """

        definition = DDLParser.parse(ddl)
        refresher.create_dynamic_table(definition)

        # Perform refresh
        result = refresher.refresh_table("sales_summary")

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
        ddl = """
        CREATE DYNAMIC TABLE sales_summary
        TARGET_LAG = '5 minutes'
        AS
        SELECT product_id, SUM(amount) as total_sales
        FROM sales
        GROUP BY product_id
        """

        definition = DDLParser.parse(ddl)
        refresher.create_dynamic_table(definition)

        # First refresh
        refresher.refresh_table("sales_summary")

        # Add more sales (with transaction)
        duckdb_conn.execute("BEGIN TRANSACTION")
        duckdb_conn.execute("""
            INSERT INTO sales VALUES (1, 75.00, '2024-01-03')
        """)
        duckdb_conn.execute("COMMIT")

        # Refresh again
        result = refresher.refresh_table("sales_summary")

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
            refresher.refresh_table("nonexistent")

    def test_refresh_all_in_dependency_order(
        self, refresher: Any, duckdb_conn: Any, sample_source_data: Any
    ) -> None:
        """Test refreshing multiple tables in dependency order."""
        # Create first dynamic table
        ddl1 = """
        CREATE DYNAMIC TABLE sales_by_product
        TARGET_LAG = '5 minutes'
        AS
        SELECT product_id, SUM(amount) as total
        FROM sales
        GROUP BY product_id
        """

        refresher.create_dynamic_table(DDLParser.parse(ddl1))

        # Create second dynamic table depending on first
        ddl2 = """
        CREATE DYNAMIC TABLE top_products
        TARGET_LAG = '10 minutes'
        AS
        SELECT product_id, total
        FROM sales_by_product
        WHERE total > 200
        """

        refresher.create_dynamic_table(DDLParser.parse(ddl2))

        # Refresh all
        results = refresher.refresh_all()

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
        ddl = """
        CREATE DYNAMIC TABLE sales_summary
        TARGET_LAG = '5 minutes'
        AS
        SELECT product_id, SUM(amount) as total
        FROM sales
        GROUP BY product_id
        """

        refresher.create_dynamic_table(DDLParser.parse(ddl))
        refresher.refresh_table("sales_summary")

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
        ddl1 = """
        CREATE DYNAMIC TABLE parent_table
        TARGET_LAG = '5 minutes'
        AS
        SELECT * FROM sales
        """

        refresher.create_dynamic_table(DDLParser.parse(ddl1))

        # Create child table
        ddl2 = """
        CREATE DYNAMIC TABLE child_table
        TARGET_LAG = '5 minutes'
        AS
        SELECT * FROM parent_table
        """

        refresher.create_dynamic_table(DDLParser.parse(ddl2))

        # Try to drop parent
        with pytest.raises(ValueError, match="depend on it"):
            refresher.drop_dynamic_table("parent_table")

    def test_refresh_history_recorded(self, refresher: Any, sample_source_data: Any) -> None:
        """Test that refresh history is properly recorded."""
        ddl = """
        CREATE DYNAMIC TABLE sales_summary
        TARGET_LAG = '5 minutes'
        AS
        SELECT product_id, SUM(amount) as total
        FROM sales
        GROUP BY product_id
        """

        refresher.create_dynamic_table(DDLParser.parse(ddl))
        refresher.refresh_table("sales_summary")

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
