"""Dynamic table refresh logic."""

from typing import List, Dict, Any
from datetime import datetime, UTC
import time
import json
import sqlglot
from sqlglot import exp

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

    def _rewrite_query_with_snapshots(self, query_sql: str, snapshot_map: Dict[str, int]) -> str:
        """Rewrite query to use FOR SYSTEM_TIME AS OF SNAPSHOT clauses.
        
        Args:
            query_sql: Original SQL query
            snapshot_map: Dict mapping table names to snapshot IDs
            
        Returns:
            Rewritten SQL query with snapshot clauses
            
        Raises:
            RuntimeError: If query rewriting fails
        """
        if not snapshot_map:
            return query_sql
            
        try:
            import re
            
            # Parse the SQL query
            parsed = sqlglot.parse_one(query_sql, dialect='duckdb')
            
            # Find all table references and inject snapshot clauses
            for table_node in parsed.find_all(exp.Table):
                table_name = table_node.name
                
                if table_name in snapshot_map:
                    snapshot_id = snapshot_map[table_name]
                    
                    # Create HistoricalData node for AT (VERSION => snapshot_id)
                    # sqlglot natively supports this via HistoricalData expression
                    historical = exp.HistoricalData(
                        this='AT',
                        kind='VERSION',
                        expression=exp.Literal.number(snapshot_id)
                    )
                    
                    # Attach the AT clause to the table node
                    table_node.set('when', historical)
            
            # Convert back to SQL
            result_sql = parsed.sql(dialect='duckdb')
            
            # Post-process: DuckDB requires alias BEFORE AT clause, but sqlglot generates AT before alias
            # Reorder: "table AT (VERSION => N) AS alias" -> "table AS alias AT (VERSION => N)"
            # Note: sqlglot always generates explicit AS, even for implicit aliases in input
            result_sql = re.sub(
                r'(\w+)\s+AT\s+\((VERSION\s+=>\s+\d+)\)\s+AS\s+(\w+)',
                r'\1 AS \3 AT (\2)',
                result_sql
            )
            
            return result_sql
            
        except Exception as e:
            # If parsing fails, raise error - we cannot proceed without snapshot isolation
            raise RuntimeError(f"Failed to rewrite query with snapshot isolation: {e}") from e

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

        # For snapshot isolation, we need to determine which snapshots to use for each source table
        # Strategy:
        # 1. For each direct dependency that's a dynamic table, inherit the snapshots it used
        # 2. For remaining dependencies (base tables or dynamic tables themselves), use current snapshot
        # 3. This ensures consistency: if C depends on B and A, and B depends on A,
        #    then C reads A at the same snapshot that B used
        
        cursor.execute(
            """
            SELECT DISTINCT upstream
            FROM dependencies
            WHERE downstream = %s
        """,
            (table_name,),
        )
        
        direct_dependencies = [row[0] for row in cursor.fetchall()]
        snapshots_to_use = {}
        
        # First pass: inherit snapshots from dynamic table dependencies
        for dep in direct_dependencies:
            # Check if this dependency is a dynamic table
            cursor.execute(
                "SELECT name FROM dynamic_tables WHERE name = %s",
                (dep,)
            )
            is_dynamic_table = cursor.fetchone() is not None
            
            if is_dynamic_table:
                # Use the snapshots that this dynamic table used
                cursor.execute(
                    """
                    SELECT source_table, last_snapshot
                    FROM source_snapshots
                    WHERE dynamic_table = %s
                """,
                    (dep,)
                )
                dep_snapshots = {row[0]: row[1] for row in cursor.fetchall()}
                # Merge these snapshots - dynamic table snapshots take priority
                snapshots_to_use.update(dep_snapshots)
        
        # Second pass: capture current snapshot for any missing dependencies
        # (including the direct dependencies themselves)
        result = self.duckdb.execute("""
            SELECT snapshot_id 
            FROM ducklake.snapshots() 
            ORDER BY snapshot_id DESC 
            LIMIT 1
        """).fetchone()
        
        if result:
            current_snapshot = result[0]
            for dep in direct_dependencies:
                if dep not in snapshots_to_use:
                    snapshots_to_use[dep] = current_snapshot
        
        # Rewrite query with snapshot isolation
        query_with_snapshots = self._rewrite_query_with_snapshots(query_sql, snapshots_to_use)

        # Record start time
        started_at = datetime.now(UTC)
        start_time = time.time()

        # Record refresh in history with the snapshots we actually used
        cursor.execute(
            """
            INSERT INTO refresh_history (
                dynamic_table, started_at, status, strategy_used, source_snapshots
            ) VALUES (%s, %s, 'RUNNING', 'FULL', %s)
            RETURNING id
        """,
            (table_name, started_at, json.dumps(snapshots_to_use)),
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
                # Use original query for schema inference, not snapshot query
                self.duckdb.execute(f"""
                    CREATE TABLE {full_table_name} AS 
                    SELECT * FROM (
                        {query_sql}
                    ) LIMIT 0
                """)

            # Insert data (DML - in transaction)
            # Use snapshot-isolated query for actual data
            self.duckdb.execute("BEGIN TRANSACTION")
            self.duckdb.execute(f"""
                INSERT INTO {full_table_name}
                {query_with_snapshots}
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
                (datetime.now(UTC), rows_affected, duration_ms, history_id),
            )

            # Update source_snapshots table with the NEW snapshots we just captured
            for source_table, snapshot_id in snapshots_to_use.items():
                cursor.execute(
                    """
                    INSERT INTO source_snapshots (dynamic_table, source_table, last_snapshot, last_processed_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (dynamic_table, source_table)
                    DO UPDATE SET 
                        last_snapshot = EXCLUDED.last_snapshot,
                        last_processed_at = EXCLUDED.last_processed_at
                """,
                    (table_name, source_table, snapshot_id, datetime.now(UTC)),
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
                (datetime.now(UTC), str(e), history_id),
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
