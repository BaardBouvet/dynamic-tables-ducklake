"""Test fixtures using testcontainers."""

import pytest
from testcontainers.postgres import PostgresContainer
from testcontainers.minio import MinioContainer
import duckdb
from minio import Minio

from dynamic_tables.metadata import MetadataStore


@pytest.fixture(scope="session")
def postgres_container():
    """PostgreSQL container for metadata store."""
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def minio_container():
    """MinIO container for object storage."""
    with MinioContainer() as minio:
        yield minio


@pytest.fixture
def metadata_store(postgres_container):
    """Initialize metadata store with test database."""
    # Get connection URL and replace sqlalchemy driver with postgresql
    connection_string = postgres_container.get_connection_url().replace(
        "postgresql+psycopg2://", "postgresql://"
    )
    store = MetadataStore(connection_string)
    store.connect()
    yield store
    store.close()


@pytest.fixture
def minio_client(minio_container):
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
def duckdb_conn(minio_container, postgres_container):
    """DuckDB connection with DuckLake extension and MinIO backend."""
    conn = duckdb.connect(":memory:")
    
    # Install and load DuckLake extension (required)
    try:
        # Load httpfs for S3 support
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        
        conn.execute("INSTALL ducklake;")
        conn.execute("LOAD ducklake;")
        
        # Configure S3 settings for MinIO
        config = minio_container.get_config()
        conn.execute(f"""
            SET s3_endpoint='{config['endpoint']}';
            SET s3_access_key_id='{minio_container.access_key}';
            SET s3_secret_access_key='{minio_container.secret_key}';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
        """)
        
        # Attach DuckLake with PostgreSQL as catalog and S3 for data
        bucket = "test-bucket"
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
    
    # Cleanup
    try:
        conn.execute("DETACH ducklake;")
    except:
        pass
    conn.close()
