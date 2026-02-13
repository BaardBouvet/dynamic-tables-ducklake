"""Benchmark suite for Phase 2: Incremental Refresh with CDC-based Affected Keys.

Tests performance of key operations:
- Affected keys extraction from CDC
- Cardinality calculations for strategy selection
- Incremental DELETE operations
- Incremental INSERT with aggregations
- Full refresh baseline comparisons
- Multi-table join refresh scenarios

Run with: pytest tests/test_benchmarks_phase2.py --benchmark-only
Run quick: pytest tests/test_benchmarks_phase2.py -k quick --benchmark-only
"""

import pytest

from dynamic_tables.metadata import MetadataStore
from dynamic_tables.profiling import BenchmarkReport, measure_operation
from tests.conftest_benchmark import DATA_PROFILES, DataProfile, SyntheticDataGenerator


class TestAffectedKeysExtraction:
    """Benchmark affected keys extraction from CDC (Phase 2 core operation)."""

    def test_extract_affected_keys(
        self,
        benchmark,
        benchmark_duckdb_conn,
        data_generator: SyntheticDataGenerator,
        benchmark_profile: DataProfile,
    ):
        """Extract affected keys from table_changes() - all dataset sizes."""
        # Setup: Create table and initial snapshot
        profile = benchmark_profile
        table_name = "sales_fact"

        snapshot_v0 = data_generator.create_fact_table(table_name, profile, 0)
        snapshot_v1 = data_generator.modify_fact_table(table_name, profile, 1)

        # Benchmark: Extract distinct affected keys using CDC
        def extract_keys():
            benchmark_duckdb_conn.execute(f"""
                CREATE OR REPLACE TEMP TABLE affected_keys AS
                SELECT DISTINCT customer_id
                FROM table_changes('{table_name}', {snapshot_v0}, {snapshot_v1})
            """)

            # Get row count to verify
            count = benchmark_duckdb_conn.execute("SELECT COUNT(*) FROM affected_keys").fetchone()[
                0
            ]

            return count

        result = benchmark(extract_keys)

        # Verify we extracted some keys
        assert result > 0, "Should extract at least some affected keys"

    def test_extract_affected_keys_with_joins(
        self,
        benchmark,
        benchmark_duckdb_conn,
        data_generator: SyntheticDataGenerator,
    ):
        """Extract affected keys from multi-table CDC scenario."""
        profile = DATA_PROFILES["small"]

        # Setup fact and dimension tables
        fact_snap_v0 = data_generator.create_fact_table("orders", profile, 0)
        dim_snap_v0 = data_generator.create_dimension_table("customers", 1000, 0)

        # Modify both tables
        fact_snap_v1 = data_generator.modify_fact_table("orders", profile, 1)
        dim_snap_v1 = data_generator.modify_dimension_table("customers", 0.05, 1)

        # Benchmark: Extract keys from UNION of both sources
        def extract_keys_union():
            benchmark_duckdb_conn.execute(
                """
                CREATE OR REPLACE TEMP TABLE affected_keys AS
                SELECT DISTINCT customer_id FROM (
                    SELECT customer_id FROM table_changes('orders', ?, ?)
                    UNION ALL
                    SELECT id as customer_id FROM table_changes('customers', ?, ?)
                )
            """,
                [fact_snap_v0, fact_snap_v1, dim_snap_v0, dim_snap_v1],
            )

            count = benchmark_duckdb_conn.execute("SELECT COUNT(*) FROM affected_keys").fetchone()[
                0
            ]

            return count

        result = benchmark(extract_keys_union)
        assert result > 0


class TestCardinalityCalculation:
    """Benchmark cardinality calculation for strategy selection (30% threshold)."""

    def test_calculate_cardinality_ratio(
        self,
        benchmark,
        benchmark_duckdb_conn,
        data_generator: SyntheticDataGenerator,
        benchmark_profile: DataProfile,
    ):
        """Calculate affected rows ratio to decide incremental vs full refresh."""
        profile = benchmark_profile
        table_name = "sales_agg"

        # Create aggregated target table
        data_generator.create_fact_table("sales_fact", profile, 0)
        benchmark_duckdb_conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT 
                customer_id,
                COUNT(*) as order_count,
                SUM(amount) as total_amount
            FROM sales_fact
            GROUP BY customer_id
        """)

        # Create affected keys temp table
        affected_count = int(profile.rows * 0.1 / profile.group_by_cardinality)
        benchmark_duckdb_conn.execute(f"""
            CREATE TEMP TABLE affected_keys AS
            SELECT DISTINCT customer_id
            FROM sales_fact
            LIMIT {max(affected_count, 10)}
        """)

        # Benchmark: Calculate cardinality ratio
        def calculate_ratio():
            result = benchmark_duckdb_conn.execute(f"""
                SELECT 
                    (SELECT COUNT(*) FROM affected_keys)::FLOAT / 
                    (SELECT COUNT(*) FROM {table_name})::FLOAT as ratio
            """).fetchone()

            return result[0] if result else 0.0

        ratio = benchmark(calculate_ratio)

        # Should complete very fast (<1ms goal)
        assert 0.0 <= ratio <= 1.0


class TestIncrementalDelete:
    """Benchmark DELETE phase of incremental refresh."""

    def test_delete_affected_keys(
        self,
        benchmark,
        benchmark_duckdb_conn,
        data_generator: SyntheticDataGenerator,
        benchmark_profile: DataProfile,
    ):
        """DELETE rows matching affected keys from target table."""
        profile = benchmark_profile
        target_table = "customer_summary"

        # Setup: Create aggregated table
        data_generator.create_fact_table("orders", profile, 0)
        benchmark_duckdb_conn.execute(f"""
            CREATE TABLE {target_table} AS
            SELECT 
                customer_id,
                COUNT(*) as order_count,
                SUM(amount) as total_sales
            FROM orders
            GROUP BY customer_id
        """)

        # Create affected keys (10% of customers)
        affected_count = max(int(profile.group_by_cardinality * 0.1), 10)
        benchmark_duckdb_conn.execute(f"""
            CREATE TEMP TABLE affected_keys AS
            SELECT DISTINCT customer_id
            FROM {target_table}
            LIMIT {affected_count}
        """)

        # Benchmark: DELETE operation
        def delete_affected():
            benchmark_duckdb_conn.execute(f"""
                DELETE FROM {target_table}
                WHERE customer_id IN (SELECT customer_id FROM affected_keys)
            """).fetchone()

            # Return number of rows deleted
            return affected_count

        benchmark(delete_affected)

    @pytest.mark.parametrize("cardinality_pct", [0.01, 0.1, 0.3])
    def test_delete_varying_cardinality(
        self,
        benchmark,
        benchmark_duckdb_conn,
        data_generator: SyntheticDataGenerator,
        cardinality_pct: float,
    ):
        """DELETE performance across different cardinality ratios."""
        profile = DATA_PROFILES["small"]
        target_table = "summary_table"

        # Setup
        data_generator.create_fact_table("source", profile, 0)
        benchmark_duckdb_conn.execute(f"""
            CREATE TABLE {target_table} AS
            SELECT 
                customer_id,
                COUNT(*) as cnt
            FROM source
            GROUP BY customer_id
        """)

        # Vary affected key count
        affected_count = max(int(profile.group_by_cardinality * cardinality_pct), 10)
        benchmark_duckdb_conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE affected_keys AS
            SELECT DISTINCT customer_id
            FROM {target_table}
            LIMIT {affected_count}
        """)

        def delete_op():
            benchmark_duckdb_conn.execute(f"""
                DELETE FROM {target_table}
                WHERE customer_id IN (SELECT customer_id FROM affected_keys)
            """)

        benchmark(delete_op)


class TestIncrementalInsert:
    """Benchmark INSERT phase with GROUP BY aggregations."""

    def test_insert_with_group_by(
        self,
        benchmark,
        benchmark_duckdb_conn,
        data_generator: SyntheticDataGenerator,
        benchmark_profile: DataProfile,
    ):
        """INSERT aggregated rows for affected keys only."""
        profile = benchmark_profile
        source_table = "orders"
        target_table = "customer_metrics"

        # Setup
        data_generator.create_fact_table(source_table, profile, 0)
        benchmark_duckdb_conn.execute(f"""
            CREATE TABLE {target_table} (
                customer_id INTEGER,
                order_count BIGINT,
                total_amount DECIMAL(20,2),
                avg_quantity DOUBLE
            )
        """)

        # Create affected keys
        affected_count = max(int(profile.group_by_cardinality * 0.1), 10)
        benchmark_duckdb_conn.execute(f"""
            CREATE TEMP TABLE affected_keys AS
            SELECT DISTINCT customer_id
            FROM {source_table}
            LIMIT {affected_count}
        """)

        # Benchmark: INSERT with aggregation
        def insert_aggregated():
            benchmark_duckdb_conn.execute(f"""
                INSERT INTO {target_table}
                SELECT 
                    customer_id,
                    COUNT(*) as order_count,
                    SUM(amount) as total_amount,
                    AVG(quantity) as avg_quantity
                FROM {source_table}
                WHERE customer_id IN (SELECT customer_id FROM affected_keys)
                GROUP BY customer_id
            """)

        benchmark(insert_aggregated)

    def test_insert_with_2way_join(
        self,
        benchmark,
        benchmark_duckdb_conn,
        data_generator: SyntheticDataGenerator,
    ):
        """INSERT with 2-way JOIN (fact + dimension)."""
        profile = DATA_PROFILES["small"]

        # Setup
        data_generator.create_fact_table("orders", profile, 0)
        data_generator.create_dimension_table("customers", 10000, 0)

        benchmark_duckdb_conn.execute("""
            CREATE TABLE order_summary (
                customer_id INTEGER,
                customer_name VARCHAR,
                order_count BIGINT,
                total_sales DECIMAL(20,2)
            )
        """)

        affected_count = max(int(profile.group_by_cardinality * 0.1), 10)
        benchmark_duckdb_conn.execute(f"""
            CREATE TEMP TABLE affected_keys AS
            SELECT DISTINCT customer_id
            FROM orders
            LIMIT {affected_count}
        """)

        # Benchmark: INSERT with JOIN
        def insert_with_join():
            benchmark_duckdb_conn.execute("""
                INSERT INTO order_summary
                SELECT 
                    o.customer_id,
                    c.name as customer_name,
                    COUNT(*) as order_count,
                    SUM(o.amount) as total_sales
                FROM orders o
                JOIN customers c ON o.customer_id = c.id
                WHERE o.customer_id IN (SELECT customer_id FROM affected_keys)
                GROUP BY o.customer_id, c.name
            """)

        benchmark(insert_with_join)


class TestFullRefreshBaseline:
    """Baseline: Full refresh for comparison with incremental."""

    def test_full_refresh(
        self,
        benchmark,
        benchmark_duckdb_conn,
        data_generator: SyntheticDataGenerator,
        benchmark_profile: DataProfile,
    ):
        """Full refresh: DELETE all + INSERT all (baseline for comparison)."""
        profile = benchmark_profile
        source = "orders"
        target = "customer_totals"

        # Setup
        data_generator.create_fact_table(source, profile, 0)
        benchmark_duckdb_conn.execute(f"""
            CREATE TABLE {target} (
                customer_id INTEGER,
                total_amount DECIMAL(20,2)
            )
        """)

        # Initial load
        benchmark_duckdb_conn.execute(f"""
            INSERT INTO {target}
            SELECT customer_id, SUM(amount)
            FROM {source}
            GROUP BY customer_id
        """)

        # Benchmark: Full refresh
        def full_refresh():
            # Method 1: DELETE + INSERT
            benchmark_duckdb_conn.execute(f"DELETE FROM {target}")
            benchmark_duckdb_conn.execute(f"""
                INSERT INTO {target}
                SELECT customer_id, SUM(amount)
                FROM {source}
                GROUP BY customer_id
            """)

        benchmark(full_refresh)


class TestEndToEndRefresh:
    """End-to-end benchmarks simulating complete refresh workflows."""

    def test_incremental_refresh_workflow(
        self,
        benchmark,
        benchmark_duckdb_conn,
        metadata_store: MetadataStore,
        data_generator: SyntheticDataGenerator,
    ):
        """Complete incremental refresh: CDC → DELETE → INSERT."""
        profile = DATA_PROFILES["small"]
        report = BenchmarkReport(
            scenario=f"incremental_refresh_{profile.name}",
            total_duration_seconds=0.0,
            strategy="incremental",
        )

        # Setup
        source = "sales"
        target = "sales_summary"

        snap_v0 = data_generator.create_fact_table(source, profile, 0)

        benchmark_duckdb_conn.execute(f"""
            CREATE TABLE {target} AS
            SELECT 
                customer_id,
                COUNT(*) as order_count,
                SUM(amount) as revenue
            FROM {source}
            GROUP BY customer_id
        """)

        # Simulate change
        snap_v1 = data_generator.modify_fact_table(source, profile, 1)

        # Benchmark: Complete workflow
        def incremental_refresh():
            # Step 1: Extract affected keys
            with measure_operation("cdc_extract", profile.affected_rows) as op1:
                benchmark_duckdb_conn.execute(f"""
                    CREATE OR REPLACE TEMP TABLE affected_keys AS
                    SELECT DISTINCT customer_id
                    FROM table_changes('{source}', {snap_v0}, {snap_v1})
                """)
            report.add_operation(op1.get_metrics())

            # Step 2: Calculate cardinality
            with measure_operation("cardinality_check") as op2:
                ratio_result = benchmark_duckdb_conn.execute(f"""
                    SELECT 
                        (SELECT COUNT(*) FROM affected_keys)::FLOAT /
                        (SELECT COUNT(*) FROM {target})::FLOAT
                """).fetchone()
                ratio = ratio_result[0] if ratio_result else 0.0
                op2.metadata["ratio"] = ratio
            report.add_operation(op2.get_metrics())

            # Step 3: DELETE affected
            with measure_operation("delete_affected") as op3:
                benchmark_duckdb_conn.execute(f"""
                    DELETE FROM {target}
                    WHERE customer_id IN (SELECT customer_id FROM affected_keys)
                """)
            report.add_operation(op3.get_metrics())

            # Step 4: INSERT recomputed
            with measure_operation("insert_recomputed") as op4:
                benchmark_duckdb_conn.execute(f"""
                    INSERT INTO {target}
                    SELECT 
                        customer_id,
                        COUNT(*) as order_count,
                        SUM(amount) as revenue
                    FROM {source}
                    WHERE customer_id IN (SELECT customer_id FROM affected_keys)
                    GROUP BY customer_id
                """)
            report.add_operation(op4.get_metrics())

        benchmark(incremental_refresh)

        # Assert reasonable performance expectations
        assert report.operations[0].duration_seconds < 10.0, "CDC should be reasonably fast"


# Mark all benchmarks as benchmarks only (skip in normal test runs)
pytestmark = pytest.mark.benchmark
