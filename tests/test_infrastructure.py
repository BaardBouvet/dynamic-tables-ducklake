"""Test metadata store initialization."""

from typing import Any
import pytest


def test_metadata_store_init(metadata_store: Any) -> None:
    """Test that metadata store initializes with correct schema."""
    cursor = metadata_store.conn.cursor()

    # Check that all tables exist
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        ORDER BY table_name
    """)

    tables = [row[0] for row in cursor.fetchall()]

    assert "dynamic_tables" in tables
    assert "source_snapshots" in tables
    assert "dependencies" in tables
    assert "refresh_history" in tables


def test_can_connect_to_duckdb(duckdb_conn: Any) -> None:
    """Test that DuckDB connection works and DuckLake is loaded."""
    result = duckdb_conn.execute("SELECT 42 as answer").fetchone()
    assert result[0] == 42

    # Verify DuckLake extension is loaded
    extensions = duckdb_conn.execute("""
        SELECT extension_name, loaded 
        FROM duckdb_extensions() 
        WHERE extension_name = 'ducklake'
    """).fetchall()

    assert len(extensions) == 1
    assert extensions[0][1] is True  # loaded = true

    # Verify we're connected to DuckLake database
    current_db = duckdb_conn.execute("SELECT current_database()").fetchone()[0]
    assert current_db == "ducklake"


def test_minio_connection(minio_client: Any) -> None:
    """Test that MinIO is accessible."""
    client, bucket = minio_client

    # Verify bucket exists
    assert client.bucket_exists(bucket)


def test_duckdb_s3_integration(duckdb_conn: Any, minio_client: Any) -> None:
    """Test that DuckLake can create tables with data stored in S3/MinIO."""
    client, bucket = minio_client

    # Create a table in DuckLake (data will be stored in S3)
    duckdb_conn.execute("""
        CREATE TABLE test_data (
            id INTEGER,
            name VARCHAR
        );
    """)

    # Insert data
    duckdb_conn.execute("""
        INSERT INTO test_data 
        SELECT i as id, 'value_' || i as name 
        FROM range(10) t(i)
    """)

    # Verify data is readable
    result = duckdb_conn.execute("SELECT COUNT(*) FROM test_data").fetchone()
    assert result[0] == 10

    # Verify data files exist in MinIO under the ducklake-data path
    objects = list(client.list_objects(bucket, prefix="ducklake-data/", recursive=True))
    assert len(objects) > 0, "Expected DuckLake to write data files to S3"


@pytest.mark.skip(
    reason="DuckLake snapshot time-travel needs investigation - may require specific configuration"
)
def test_ducklake_snapshots(duckdb_conn: Any) -> None:
    """Test that DuckLake snapshot functionality works."""
    # Create a table
    duckdb_conn.execute("""
        CREATE TABLE orders (
            id INTEGER,
            amount DECIMAL(10,2)
        );
    """)

    # Get initial snapshot after table creation
    first_snapshot = duckdb_conn.execute("""
        SELECT snapshot_id 
        FROM ducklake.snapshots() 
        ORDER BY snapshot_id DESC 
        LIMIT 1
    """).fetchone()[0]

    # Insert initial data
    duckdb_conn.execute("INSERT INTO orders VALUES (1, 100.00), (2, 200.00);")

    # Get snapshot after first insert
    second_snapshot = duckdb_conn.execute("""
        SELECT snapshot_id 
        FROM ducklake.snapshots() 
        ORDER BY snapshot_id DESC 
        LIMIT 1
    """).fetchone()[0]

    assert second_snapshot > first_snapshot

    # Insert more data
    duckdb_conn.execute("INSERT INTO orders VALUES (3, 300.00);")

    # Get the new snapshot after second insert
    third_snapshot = duckdb_conn.execute("""
        SELECT snapshot_id 
        FROM ducklake.snapshots() 
        ORDER BY snapshot_id DESC 
        LIMIT 1
    """).fetchone()[0]

    assert third_snapshot > second_snapshot

    # Verify we can read at the second snapshot (2 rows)
    count_at_second = duckdb_conn.execute(f"""
        SELECT COUNT(*) 
        FROM orders AT (VERSION => {second_snapshot})
    """).fetchone()[0]

    assert count_at_second == 2

    # Current table should have all data
    count_current = duckdb_conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    assert count_current == 3
