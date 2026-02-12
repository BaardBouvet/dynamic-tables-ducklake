"""Dynamic table refresh logic."""

from typing import List, Dict, Any
from datetime import datetime
import time

from dynamic_tables.metadata import MetadataStore
from dynamic_tables.parser import DynamicTableDefinition, DependencyGraph


class DynamicTableRefresher:
    """Handles full refresh of dynamic tables."""

    def __init__(self, metadata_store: MetadataStore, duckdb_conn: Any) -> None:
        """Initialize refresher.

        Args:
            metadata_store: PostgreSQL metadata store
            duckdb_conn: DuckDB connection with DuckLake
        """
        self.metadata = metadata_store
        self.duckdb = duckdb_conn

    def create_dynamic_table(self, definition: DynamicTableDefinition) -> None:
        """Create a new dynamic table definition.

        Args:
            definition: Parsed table definition

        Raises:
            ValueError: If table already exists or would create circular dependency
        """
        cursor = self.metadata.conn.cursor()

        # Check if table already exists
        cursor.execute("SELECT name FROM dynamic_tables WHERE name = %s", (definition.name,))
        if cursor.fetchone():
            raise ValueError(f"Dynamic table '{definition.name}' already exists")

        # Build dependency graph to check for cycles
        graph = self._load_dependency_graph()

        # Add new table to graph (this will raise if cycle detected)
        graph.add_table(definition.name, definition.source_tables)

        # Insert table definition
        cursor.execute(
            """
            INSERT INTO dynamic_tables (
                name, schema_name, query_sql, target_lag,
                group_by_columns, refresh_strategy, deduplicate,
                cardinality_threshold, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
            (
                definition.name,
                definition.schema_name,
                definition.query_sql,
                definition.target_lag,
                definition.group_by_columns,
                definition.refresh_strategy,
                definition.deduplicate,
                definition.cardinality_threshold,
                "ACTIVE",
            ),
        )

        # Insert dependencies
        for source in definition.source_tables:
            cursor.execute(
                """
                INSERT INTO dependencies (downstream, upstream)
                VALUES (%s, %s)
            """,
                (definition.name, source),
            )

        self.metadata.conn.commit()

    def drop_dynamic_table(self, table_name: str) -> None:
        """Drop a dynamic table.

        Args:
            table_name: Name of table to drop
        """
        cursor = self.metadata.conn.cursor()

        # Check if other tables depend on this one
        cursor.execute(
            """
            SELECT downstream FROM dependencies WHERE upstream = %s
        """,
            (table_name,),
        )

        dependents = cursor.fetchall()
        if dependents:
            dependent_names = [row[0] for row in dependents]
            raise ValueError(f"Cannot drop '{table_name}': tables {dependent_names} depend on it")

        # Delete from metadata (CASCADE will handle dependencies and history)
        cursor.execute("DELETE FROM dynamic_tables WHERE name = %s", (table_name,))

        # Drop the actual table in DuckDB (DDL - outside transaction)
        try:
            self.duckdb.execute(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            # Table may not exist yet, that's okay
            pass

        self.metadata.conn.commit()

    def refresh_table(self, table_name: str) -> Dict[str, Any]:
        """Perform full refresh of a dynamic table.

        Args:
            table_name: Name of table to refresh

        Returns:
            Refresh metrics (rows_affected, duration_ms, etc.)

        Raises:
            ValueError: If table doesn't exist
        """
        cursor = self.metadata.conn.cursor()

        # Get table definition
        cursor.execute(
            """
            SELECT query_sql, schema_name
            FROM dynamic_tables
            WHERE name = %s
        """,
            (table_name,),
        )

        row = cursor.fetchone()
        if not row:
            raise ValueError(f"Dynamic table '{table_name}' does not exist")

        query_sql, schema_name = row

        # Record start time
        started_at = datetime.utcnow()
        start_time = time.time()

        # Record refresh in history
        cursor.execute(
            """
            INSERT INTO refresh_history (
                dynamic_table, started_at, status, strategy_used
            ) VALUES (%s, %s, 'RUNNING', 'FULL')
            RETURNING id
        """,
            (table_name, started_at),
        )

        result = cursor.fetchone()
        if result is None:
            raise RuntimeError("Failed to create refresh history record")
        history_id = result[0]
        self.metadata.conn.commit()

        try:
            # Full refresh: TRUNCATE + INSERT
            # First, create or truncate the table
            full_table_name = f"{schema_name}.{table_name}" if schema_name != "main" else table_name

            # Check if table exists
            table_exists = (
                self.duckdb.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = '{table_name}'
                AND table_schema = '{schema_name}'
            """).fetchone()[0]
                > 0
            )

            if table_exists:
                # Delete existing data (with transaction)
                self.duckdb.execute("BEGIN TRANSACTION")
                self.duckdb.execute(f"DELETE FROM {full_table_name}")
                self.duckdb.execute("COMMIT")
            else:
                # Create table from query (DDL - outside transaction)
                self.duckdb.execute(f"""
                    CREATE TABLE {full_table_name} AS 
                    SELECT * FROM (
                        {query_sql}
                    ) LIMIT 0
                """)

            # Insert data (DML - in transaction)
            self.duckdb.execute("BEGIN TRANSACTION")
            self.duckdb.execute(f"""
                INSERT INTO {full_table_name}
                {query_sql}
            """)
            self.duckdb.execute("COMMIT")

            # Get row count
            rows_affected = self.duckdb.execute(
                f"SELECT COUNT(*) FROM {full_table_name}"
            ).fetchone()[0]

            # Calculate duration
            duration_ms = int((time.time() - start_time) * 1000)

            # Update history record
            cursor.execute(
                """
                UPDATE refresh_history
                SET completed_at = %s,
                    status = 'SUCCESS',
                    rows_affected = %s,
                    duration_ms = %s
                WHERE id = %s
            """,
                (datetime.utcnow(), rows_affected, duration_ms, history_id),
            )

            self.metadata.conn.commit()

            return {"status": "SUCCESS", "rows_affected": rows_affected, "duration_ms": duration_ms}

        except Exception as e:
            # Rollback transaction if it was started
            try:
                self.duckdb.execute("ROLLBACK")
            except Exception:
                pass

            # Update history with error
            cursor.execute(
                """
                UPDATE refresh_history
                SET completed_at = %s,
                    status = 'FAILED',
                    error_message = %s
                WHERE id = %s
            """,
                (datetime.utcnow(), str(e), history_id),
            )

            self.metadata.conn.commit()
            raise

    def refresh_all(self) -> List[Dict[str, Any]]:
        """Refresh all dynamic tables in dependency order.

        Returns:
            List of refresh results for each table
        """
        graph = self._load_dependency_graph()
        sorted_tables = graph.topological_sort()

        results = []
        for table_name in sorted_tables:
            result = self.refresh_table(table_name)
            result["table"] = table_name
            results.append(result)

        return results

    def list_tables(self) -> List[Dict[str, Any]]:
        """List all dynamic tables.

        Returns:
            List of table information
        """
        cursor = self.metadata.conn.cursor()
        cursor.execute("""
            SELECT name, schema_name, target_lag, status, created_at
            FROM dynamic_tables
            ORDER BY name
        """)

        tables = []
        for row in cursor.fetchall():
            tables.append(
                {
                    "name": row[0],
                    "schema": row[1],
                    "target_lag": str(row[2]),
                    "status": row[3],
                    "created_at": row[4],
                }
            )

        return tables

    def _load_dependency_graph(self) -> DependencyGraph:
        """Load current dependency graph from metadata.

        Returns:
            Populated dependency graph
        """
        cursor = self.metadata.conn.cursor()

        # Get all tables with their dependencies
        cursor.execute("""
            SELECT dt.name, COALESCE(array_agg(d.upstream), ARRAY[]::text[])
            FROM dynamic_tables dt
            LEFT JOIN dependencies d ON dt.name = d.downstream
            GROUP BY dt.name
        """)

        graph = DependencyGraph()
        for row in cursor.fetchall():
            table_name = row[0]
            dependencies = [dep for dep in row[1] if dep]  # Filter out nulls
            graph.add_table(table_name, dependencies)

        return graph
