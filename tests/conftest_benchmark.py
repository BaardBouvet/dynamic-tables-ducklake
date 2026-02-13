"""Benchmark-specific fixtures and data generators."""

from dataclasses import dataclass
from typing import Any, Iterator

import duckdb
import pytest

from dynamic_tables.profiling import configure_duckdb_for_benchmarks


@dataclass
class DataProfile:
    """Data profile for benchmark scenarios."""

    name: str
    rows: int
    affected_pct: float  # Percentage of rows affected (0.0 to 1.0)
    num_dimensions: int = 1  # Number of dimension tables for joins
    group_by_cardinality: int = 1000  # Unique values in GROUP BY columns

    @property
    def affected_rows(self) -> int:
        """Calculate number of affected rows."""
        return int(self.rows * self.affected_pct)


# Predefined data profiles for common benchmark scenarios
DATA_PROFILES = {
    "tiny": DataProfile("tiny", rows=1_000, affected_pct=0.1),
    "small": DataProfile("small", rows=100_000, affected_pct=0.1),
    "medium": DataProfile("medium", rows=1_000_000, affected_pct=0.1),
    "large": DataProfile("large", rows=10_000_000, affected_pct=0.1),
    "xlarge": DataProfile("xlarge", rows=100_000_000, affected_pct=0.1),
}


@pytest.fixture(scope="session")
def benchmark_duckdb_config() -> dict[str, Any]:
    """DuckDB configuration optimized for benchmarks."""
    return {
        "threads": 4,
        "memory_limit": "8GB",
        "temp_directory": "/tmp/duckdb_bench",
    }


@pytest.fixture
def benchmark_duckdb_conn(
    minio_container: Any,
    postgres_container: Any,
    benchmark_duckdb_config: dict[str, Any],
) -> Iterator[duckdb.DuckDBPyConnection]:
    """DuckDB connection optimized for benchmark performance.

    Reuses the standard DuckDB setup but with performance tuning.
    """
    conn = duckdb.connect(":memory:")

    # Ensure bucket exists
    from minio import Minio

    config = minio_container.get_config()
    minio_cli = Minio(
        config["endpoint"],
        access_key=minio_container.access_key,
        secret_key=minio_container.secret_key,
        secure=False,
    )
    bucket = "test-bucket"
    if not minio_cli.bucket_exists(bucket):
        minio_cli.make_bucket(bucket)

    # Install and load DuckLake extension
    try:
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        conn.execute("INSTALL ducklake;")
        conn.execute("LOAD ducklake;")

        # Configure S3 settings for MinIO
        conn.execute(f"""
            SET s3_endpoint='{config["endpoint"]}';
            SET s3_access_key_id='{minio_container.access_key}';
            SET s3_secret_access_key='{minio_container.secret_key}';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
        """)

        # Attach DuckLake
        data_path = f"s3://{bucket}/ducklake-bench/"
        from urllib.parse import urlparse

        pg_url_full = postgres_container.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql://"
        )
        parsed = urlparse(pg_url_full)
        pg_connstr = (
            f"host={parsed.hostname} port={parsed.port} "
            f"dbname={parsed.path.lstrip('/')} "
            f"user={parsed.username} password={parsed.password}"
        )

        conn.execute(f"""
            ATTACH 'ducklake:postgres:{pg_connstr}' AS ducklake (
                DATA_PATH '{data_path}',
                METADATA_SCHEMA 'ducklake_bench'
            );
        """)

        conn.execute("USE ducklake;")

        # Apply benchmark optimizations
        configure_duckdb_for_benchmarks(
            conn,
            threads=benchmark_duckdb_config["threads"],
            memory_limit=benchmark_duckdb_config["memory_limit"],
        )

    except Exception as e:
        conn.close()
        raise RuntimeError(f"Failed to setup benchmark DuckDB: {e}") from e

    yield conn

    # Cleanup
    try:
        tables = conn.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'main' 
            AND table_type = 'BASE TABLE'
        """).fetchall()

        for (table_name,) in tables:
            conn.execute(f"DROP TABLE IF EXISTS main.{table_name}")
    except Exception:
        pass  # Ignore cleanup errors

    conn.close()


class SyntheticDataGenerator:
    """Generate synthetic benchmark data with configurable characteristics."""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def create_fact_table(
        self,
        table_name: str,
        profile: DataProfile,
        snapshot_version: int = 0,
    ) -> int:
        """Create a fact table with synthetic data.

        Returns the snapshot ID after creating the table.
        """
        # Create table with typical fact table structure
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} (
                id BIGINT,
                customer_id INTEGER,
                product_id INTEGER,
                region_id INTEGER,
                order_date DATE,
                amount DECIMAL(10,2),
                quantity INTEGER,
                version INTEGER DEFAULT {snapshot_version}
            )
        """)

        # Generate data with controlled GROUP BY cardinality
        # Use modulo to control how many distinct customer_id values exist
        self.conn.execute(f"""
            INSERT INTO {table_name}
            SELECT
                row_number() OVER () as id,
                (random() * {profile.group_by_cardinality})::INTEGER as customer_id,
                (random() * 1000)::INTEGER as product_id,
                (random() * 50)::INTEGER as region_id,
                DATE '2024-01-01' + (random() * 365)::INTEGER as order_date,
                (random() * 1000 + 10)::DECIMAL(10,2) as amount,
                (random() * 10 + 1)::INTEGER as quantity,
                {snapshot_version} as version
            FROM range({profile.rows})
        """)

        # Get latest snapshot ID (DuckLake creates snapshots automatically on write)
        snapshot_result = self.conn.execute("""
            SELECT snapshot_id 
            FROM ducklake.snapshots() 
            ORDER BY snapshot_id DESC 
            LIMIT 1
        """).fetchone()
        
        return snapshot_result[0] if snapshot_result else 0

    def modify_fact_table(
        self,
        table_name: str,
        profile: DataProfile,
        new_snapshot_version: int,
    ) -> int:
        """Modify a percentage of rows in the fact table.

        Simulates CDC changes by updating affected_pct % of rows.
        Returns the new snapshot ID.
        """
        # Update a percentage of rows
        num_affected = profile.affected_rows

        self.conn.execute(f"""
            UPDATE {table_name}
            SET 
                amount = amount * (1 + random() * 0.2 - 0.1),
                quantity = quantity + (random() * 4 - 2)::INTEGER,
                version = {new_snapshot_version}
            WHERE id <= {num_affected}
        """)

        # Get latest snapshot ID (DuckLake creates snapshots automatically on write)
        snapshot_result = self.conn.execute("""
            SELECT snapshot_id 
            FROM ducklake.snapshots() 
            ORDER BY snapshot_id DESC 
            LIMIT 1
        """).fetchone()
        
        return snapshot_result[0] if snapshot_result else 0

    def create_dimension_table(
        self,
        table_name: str,
        num_rows: int,
        snapshot_version: int = 0,
    ) -> int:
        """Create a dimension table for join scenarios.

        Returns the snapshot ID.
        """
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} (
                id INTEGER,
                name VARCHAR,
                category VARCHAR,
                status VARCHAR,
                version INTEGER DEFAULT {snapshot_version}
            )
        """)

        self.conn.execute(f"""
            INSERT INTO {table_name}
            SELECT
                i as id,
                'Item_' || i as name,
                'Category_' || (i % 10) as category,
                CASE WHEN i % 5 = 0 THEN 'active' ELSE 'inactive' END as status,
                {snapshot_version} as version
            FROM range({num_rows}) t(i)
        """)

        # Get latest snapshot ID (DuckLake creates snapshots automatically on write)
        snapshot_result = self.conn.execute("""
            SELECT snapshot_id 
            FROM ducklake.snapshots() 
            ORDER BY snapshot_id DESC 
            LIMIT 1
        """).fetchone()
        
        return snapshot_result[0] if snapshot_result else 0

    def modify_dimension_table(
        self,
        table_name: str,
        pct_affected: float,
        new_snapshot_version: int,
    ) -> int:
        """Modify a percentage of dimension rows.

        Returns the new snapshot ID.
        """
        self.conn.execute(f"""
            UPDATE {table_name}
            SET 
                status = CASE WHEN status = 'active' THEN 'inactive' ELSE 'active' END,
                version = {new_snapshot_version}
            WHERE random() < {pct_affected}
        """)

        # Get latest snapshot ID (DuckLake creates snapshots automatically on write)
        snapshot_result = self.conn.execute("""
            SELECT snapshot_id 
            FROM ducklake.snapshots() 
            ORDER BY snapshot_id DESC 
            LIMIT 1
        """).fetchone()
        
        return snapshot_result[0] if snapshot_result else 0


@pytest.fixture
def data_generator(
    benchmark_duckdb_conn: duckdb.DuckDBPyConnection,
) -> SyntheticDataGenerator:
    """Fixture providing synthetic data generation."""
    return SyntheticDataGenerator(benchmark_duckdb_conn)


@pytest.fixture(
    params=[
        pytest.param("tiny", marks=pytest.mark.quick),
        pytest.param("small", marks=pytest.mark.quick),
        pytest.param("medium", marks=[pytest.mark.full]),
        pytest.param("large", marks=[pytest.mark.full]),
        pytest.param("xlarge", marks=[pytest.mark.full]),
    ]
)
def benchmark_profile(request) -> DataProfile:
    """Benchmark profiles with quick/full marks for filtering.
    
    Use -m quick for tiny/small datasets (~20s)
    Use -m full for medium/large/xlarge datasets (~30min)
    """
    return DATA_PROFILES[request.param]


@pytest.fixture(params=[0.001, 0.01, 0.1, 0.3, 0.5])
def cardinality_ratio(request) -> float:
    """Different cardinality ratios for affected keys (0.1% to 50%)."""
    return request.param
