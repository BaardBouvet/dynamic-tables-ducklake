"""Dynamic table definitions and dependency management."""

from dataclasses import dataclass
from typing import List, Set, Dict
import sqlglot
from sqlglot import exp


def extract_source_tables(query: str) -> List[str]:
    """Extract source table names from query.

    Args:
        query: SQL query

    Returns:
        List of table names (schema.table format if schema is specified)

    Raises:
        ValueError: If query cannot be parsed
    """
    try:
        parsed = sqlglot.parse_one(query, read="duckdb")
        tables = set()

        for table in parsed.find_all(exp.Table):
            table_name = table.name
            # Include schema if present (db property in sqlglot), otherwise just table name
            if table.db:
                tables.add(f"{table.db}.{table_name}")
            else:
                tables.add(table_name)

        return sorted(tables)
    except Exception as e:
        raise ValueError(f"Failed to parse query: {e}")


@dataclass
class DynamicTableDefinition:
    """Dynamic table definition."""

    name: str
    schema_name: str
    query_sql: str
    source_tables: List[str]

    @classmethod
    def create(cls, name: str, schema_name: str, query_sql: str) -> "DynamicTableDefinition":
        """Create a dynamic table definition with auto-extracted source tables.

        Args:
            name: Table name
            schema_name: Schema name
            query_sql: SQL query

        Returns:
            DynamicTableDefinition instance
        """
        source_tables = extract_source_tables(query_sql)
        return cls(
            name=name,
            schema_name=schema_name,
            query_sql=query_sql,
            source_tables=source_tables,
        )


class DependencyGraph:
    """Manage dynamic table dependencies and detect cycles."""

    def __init__(self) -> None:
        """Initialize dependency graph."""
        self.graph: Dict[str, Set[str]] = {}

    def add_table(self, table: str, depends_on: List[str]) -> None:
        """Add a table and its dependencies.

        Args:
            table: Table name
            depends_on: List of tables this table depends on

        Raises:
            ValueError: If adding this table would create a cycle
        """
        # Create a temporary graph with the new dependency
        temp_graph = dict(self.graph)
        temp_graph[table] = set(depends_on)

        # Check for cycles
        if self._has_cycle(temp_graph):
            raise ValueError(f"Circular dependency detected involving table '{table}'")

        # No cycle, add it
        self.graph[table] = set(depends_on)

    def remove_table(self, table: str) -> None:
        """Remove a table from the graph.

        Args:
            table: Table name
        """
        self.graph.pop(table, None)

    def topological_sort(self) -> List[str]:
        """Return tables in dependency order.

        Returns:
            List of table names in topological order (dependencies first)

        Raises:
            ValueError: If graph has cycles
        """
        if self._has_cycle(self.graph):
            raise ValueError("Cannot sort: graph contains cycles")

        # Build reverse graph - count how many tables depend on each table
        in_degree = {node: 0 for node in self.graph}

        # Count incoming edges (how many tables does this table depend on)
        # Only count dependencies that are also dynamic tables (in the graph)
        for node in self.graph:
            in_degree[node] = len([dep for dep in self.graph[node] if dep in self.graph])

        # Start with nodes that have no dependencies
        queue = [node for node in self.graph if in_degree[node] == 0]
        result = []

        while queue:
            node = queue.pop(0)
            result.append(node)

            # Find all tables that depend on this node
            for other in self.graph:
                if node in self.graph[other]:
                    in_degree[other] -= 1
                    if in_degree[other] == 0:
                        queue.append(other)

        return result

    def _has_cycle(self, graph: Dict[str, Set[str]]) -> bool:
        """Check if graph has a cycle using DFS.

        Args:
            graph: Adjacency list representation

        Returns:
            True if cycle exists
        """
        visited = set()
        rec_stack = set()

        def dfs(node: str) -> bool:
            visited.add(node)
            rec_stack.add(node)

            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    if neighbor in graph and dfs(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True

            rec_stack.remove(node)
            return False

        for node in graph:
            if node not in visited:
                if dfs(node):
                    return True

        return False
