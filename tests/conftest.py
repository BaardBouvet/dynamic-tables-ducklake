"""Test fixtures using testcontainers."""

from typing import Any, Iterator, Tuple
import pytest
from testcontainers.postgres import PostgresContainer
from testcontainers.minio import MinioContainer
import duckdb
from minio import Minio

from dynamic_tables.metadata import MetadataStore

# Load benchmark fixtures
pytest_plugins = ["tests.conftest_benchmark"]


@pytest.fixture(scope="session")
def postgres_container() -> Iterator[Any]:
    """PostgreSQL container for metadata store."""
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def minio_container() -> Iterator[Any]:
    """MinIO container for object storage."""
    with MinioContainer() as minio:
        yield minio


@pytest.fixture
def metadata_store(postgres_container: Any) -> Iterator[MetadataStore]:
    """Initialize metadata store with test database."""
    # Get connection URL and replace sqlalchemy driver with postgresql
    connection_string = postgres_container.get_connection_url().replace(
        "postgresql+psycopg2://", "postgresql://"
    )
    store = MetadataStore(connection_string)
    store.connect()
    yield store

    # Clean up tables between tests
    cursor = store.conn.cursor()
    cursor.execute("TRUNCATE TABLE refresh_history CASCADE")
    cursor.execute("TRUNCATE TABLE dependencies CASCADE")
    cursor.execute("TRUNCATE TABLE source_snapshots CASCADE")
    cursor.execute("TRUNCATE TABLE dynamic_tables CASCADE")
    store.conn.commit()

    store.close()


@pytest.fixture
def minio_client(minio_container: Any) -> Iterator[Tuple[Minio, str]]:
    """MinIO client for object storage."""
    client = Minio(
        minio_container.get_config()["endpoint"],
        access_key=minio_container.access_key,
        secret_key=minio_container.secret_key,
        secure=False,
    )

    # Create test bucket
    bucket_name = "test-bucket"
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    yield client, bucket_name


@pytest.fixture
def duckdb_conn(minio_container: Any, postgres_container: Any) -> Iterator[Any]:
    """DuckDB connection with DuckLake extension and MinIO backend."""
    conn = duckdb.connect(":memory:")

    # Ensure bucket exists
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

    # Install and load DuckLake extension (required)
    try:
        # Load httpfs for S3 support
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

        # Attach DuckLake with PostgreSQL as catalog and S3 for data
        data_path = f"s3://{bucket}/ducklake-data/"

        # Parse PostgreSQL connection URL to libpq format
        pg_url_full = postgres_container.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql://"
        )
        # Extract components: postgresql://user:password@host:port/dbname
        from urllib.parse import urlparse

        parsed = urlparse(pg_url_full)
        pg_connstr = f"host={parsed.hostname} port={parsed.port} dbname={parsed.path.lstrip('/')} user={parsed.username} password={parsed.password}"

        # Format: ducklake:postgres:connection_params
        conn.execute(f"""
            ATTACH 'ducklake:postgres:{pg_connstr}' AS ducklake (
                DATA_PATH '{data_path}',
                METADATA_SCHEMA 'ducklake'
            );
        """)

        # Switch to the DuckLake database
        conn.execute("USE ducklake;")

    except Exception as e:
        conn.close()
        raise RuntimeError(f"DuckLake extension is required but failed to load: {e}") from e

    yield conn

    # Cleanup - drop all tables after each test
    tables = conn.execute("""
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'main' 
        AND table_type = 'BASE TABLE'
    """).fetchall()

    # Drop each table
    for (table_name,) in tables:
        conn.execute(f"DROP TABLE IF EXISTS main.{table_name}")

    conn.close()
