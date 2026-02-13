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
        """Test that refreshing D auto-refreshes B and C together when they have conflicting snapshots.
        
        When D depends on B and C, and B and C used different snapshots of A, the system
        should detect this and automatically refresh B and C together in a transaction
        before refreshing D, ensuring consistency.
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
        refresher.refresh_tables(["order_summary_b"])
        
        # Add more data to A (creates new snapshot)
        duckdb_conn.execute("""
            INSERT INTO orders VALUES
            (3, 103, 300.00, '2024-01-03')
        """)
        
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
        refresher.refresh_tables(["order_summary_c"])
        
        # Verify B and C used different snapshots of orders
        cursor = refresher.metadata.conn.cursor()
        cursor.execute("""
            SELECT last_snapshot
            FROM source_snapshots
            WHERE dynamic_table = 'order_summary_b'
            AND source_table = 'orders'
        """)
        b_snapshot_before = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT last_snapshot
            FROM source_snapshots
            WHERE dynamic_table = 'order_summary_c'
            AND source_table = 'orders'
        """)
        c_snapshot_before = cursor.fetchone()[0]
        
        assert b_snapshot_before != c_snapshot_before, \
            "B and C should have different snapshots initially"
        
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
        
        # When we refresh D, it should detect the conflict and automatically
        # refresh B and C together first
        refresher.refresh_tables(["order_validation"])
        
        # Verify that B and C now use the same snapshot of orders
        cursor.execute("""
            SELECT last_snapshot
            FROM source_snapshots
            WHERE dynamic_table = 'order_summary_b'
            AND source_table = 'orders'
        """)
        b_snapshot_after = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT last_snapshot
            FROM source_snapshots
            WHERE dynamic_table = 'order_summary_c'
            AND source_table = 'orders'
        """)
        c_snapshot_after = cursor.fetchone()[0]
        
        # They should now use the same snapshot (both were auto-refreshed)
        assert b_snapshot_after == c_snapshot_after, \
            f"After refreshing D, B and C should use the same snapshot, but got {b_snapshot_after} and {c_snapshot_after}"
        
        # B and C should have been refreshed (different from before)
        assert b_snapshot_after != b_snapshot_before, "B should have been refreshed"
        assert c_snapshot_after != c_snapshot_before, "C should have been refreshed"
        
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




