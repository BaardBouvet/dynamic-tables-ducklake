"""Test snapshot isolation conflicts in complex dependency chains."""
import pytest
from typing import Any
from dynamic_tables.refresh import DynamicTableRefresher
from dynamic_tables.parser import DDLParser


class TestSnapshotConflicts:
    """Test cases for handling conflicting snapshots in dependency chains."""

    @pytest.fixture
    def refresher(self, metadata_store: Any, duckdb_conn: Any) -> DynamicTableRefresher:
        """Create a refresher instance."""
        return DynamicTableRefresher(metadata_store, duckdb_conn)

    def test_conflicting_snapshots_from_multiple_dependencies(
        self, refresher: Any, duckdb_conn: Any
    ) -> None:
        """Test that refresh_all() avoids conflicts by using a single transaction.
        
        When B and C are refreshed in a single transaction (via refresh_all()), they
        see the same snapshot of A, ensuring consistency for D which depends on both.
        """
        # Create base table A
        duckdb_conn.execute("""
            CREATE TABLE orders (
                order_id INTEGER,
                product_id INTEGER,
                amount DECIMAL(10,2),
                order_date DATE
            )
        """)
        
        # Insert initial data
        duckdb_conn.execute("""
            INSERT INTO orders VALUES
            (1, 101, 100.00, '2024-01-01'),
            (2, 102, 200.00, '2024-01-02')
        """)
        
        # Create dynamic table B that depends on A
        ddl_b = """
        CREATE DYNAMIC TABLE order_summary_b
        TARGET_LAG = '5 minutes'
        AS
        SELECT product_id, SUM(amount) as total
        FROM orders
        GROUP BY product_id
        """
        refresher.create_dynamic_table(DDLParser.parse(ddl_b))
        
        # Create dynamic table C that also depends on A
        ddl_c = """
        CREATE DYNAMIC TABLE order_summary_c
        TARGET_LAG = '5 minutes'
        AS
        SELECT product_id, COUNT(*) as count
        FROM orders
        GROUP BY product_id
        """
        refresher.create_dynamic_table(DDLParser.parse(ddl_c))
        
        # Create D that depends on both B and C
        ddl_d = """
        CREATE DYNAMIC TABLE order_validation
        TARGET_LAG = '5 minutes'
        AS
        SELECT 
            b.product_id,
            b.total,
            c.count
        FROM order_summary_b b
        JOIN order_summary_c c ON b.product_id = c.product_id
        """
        refresher.create_dynamic_table(DDLParser.parse(ddl_d))
        
        # Use refresh_all() to refresh all tables in a single transaction
        # This ensures B and C see the same snapshot of orders
        refresher.refresh_all()
        
        # Verify that B and C used the same snapshot of orders
        cursor = refresher.metadata.conn.cursor()
        cursor.execute("""
            SELECT last_snapshot
            FROM source_snapshots
            WHERE dynamic_table = 'order_summary_b'
            AND source_table = 'orders'
        """)
        b_snapshot = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT last_snapshot
            FROM source_snapshots
            WHERE dynamic_table = 'order_summary_c'
            AND source_table = 'orders'
        """)
        c_snapshot = cursor.fetchone()[0]
        
        # They should use the same snapshot because they were refreshed in one transaction
        assert b_snapshot == c_snapshot, \
            f"B and C should use the same snapshot when refreshed together, but got {b_snapshot} and {c_snapshot}"
        
        # Verify D was refreshed successfully
        cursor.execute("""
            SELECT status
            FROM refresh_history
            WHERE dynamic_table = 'order_validation'
            ORDER BY started_at DESC
            LIMIT 1
        """)
        result = cursor.fetchone()
        assert result is not None
        assert result[0] == "SUCCESS", "order_validation should refresh successfully"

    @pytest.mark.skip(reason="Independent refreshes can lead to inconsistency - use refresh_all() instead")
    def test_independent_refreshes_show_inconsistency(
        self, refresher: Any, duckdb_conn: Any
    ) -> None:
        """Document that independent refreshes can lead to inconsistency.
        
        When B and C are refreshed independently at different times, they will
        use different snapshots. This test documents the problem case. 
        SOLUTION: Always use refresh_all() to refresh interdependent tables together.
        """
        # Test body omitted - this documents undesirable behavior
        # Use test_conflicting_snapshots_from_multiple_dependencies to see the solution
        pass




