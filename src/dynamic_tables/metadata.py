"""Metadata schema management for PostgreSQL."""

from typing import Optional
import psycopg2
from psycopg2.extensions import connection as Connection


METADATA_SCHEMA = """
-- Core Tables (Phases 1-3)

CREATE TABLE IF NOT EXISTS dynamic_tables (
    name VARCHAR PRIMARY KEY,
    schema_name VARCHAR NOT NULL,
    query_sql TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS source_snapshots (
    dynamic_table VARCHAR,
    source_table VARCHAR,
    last_snapshot BIGINT NOT NULL,
    PRIMARY KEY (dynamic_table, source_table),
    FOREIGN KEY (dynamic_table) REFERENCES dynamic_tables(name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS dependencies (
    downstream VARCHAR,
    upstream VARCHAR,
    PRIMARY KEY (downstream, upstream),
    FOREIGN KEY (downstream) REFERENCES dynamic_tables(name) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_dependencies_downstream ON dependencies(downstream);
CREATE INDEX IF NOT EXISTS idx_dependencies_upstream ON dependencies(upstream);

CREATE TABLE IF NOT EXISTS refresh_history (
    id BIGSERIAL PRIMARY KEY,
    dynamic_table VARCHAR NOT NULL,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    status VARCHAR NOT NULL,  -- SUCCESS, FAILED
    strategy_used VARCHAR,  -- FULL, AFFECTED_KEYS
    rows_affected BIGINT,
    affected_keys_count BIGINT,
    duration_ms BIGINT,
    error_message TEXT,
    source_snapshots JSONB,
    FOREIGN KEY (dynamic_table) REFERENCES dynamic_tables(name) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_history_table ON refresh_history(dynamic_table);
CREATE INDEX IF NOT EXISTS idx_history_started ON refresh_history(started_at);
"""


class MetadataStore:
    """PostgreSQL metadata store for dynamic tables."""

    def __init__(self, connection_string: str):
        """Initialize metadata store.

        Args:
            connection_string: PostgreSQL connection string
        """
        self.connection_string = connection_string
        self._conn: Optional[Connection] = None

    def connect(self) -> None:
        """Connect to PostgreSQL and initialize schema."""
        self._conn = psycopg2.connect(self.connection_string)
        self._init_schema()

    def _init_schema(self) -> None:
        """Initialize metadata schema."""
        if not self._conn:
            raise RuntimeError("Not connected to database")

        with self._conn.cursor() as cur:
            cur.execute(METADATA_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        """Close the connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    @property
    def conn(self) -> Connection:
        """Get the database connection."""
        if not self._conn:
            raise RuntimeError("Not connected to database")
        return self._conn
