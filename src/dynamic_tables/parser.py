"""DDL parser for dynamic table definitions."""

from dataclasses import dataclass
from typing import List, Set, Dict
import re
import sqlglot
from sqlglot import exp


@dataclass
class DynamicTableDefinition:
    """Parsed dynamic table definition."""
    
    name: str
    schema_name: str
    query_sql: str
    target_lag: str
    source_tables: List[str]
    group_by_columns: List[str]
    refresh_strategy: str = "AFFECTED_KEYS"
    deduplicate: bool = False
    cardinality_threshold: float = 0.3


class DDLParser:
    """Parser for CREATE DYNAMIC TABLE statements."""
    
    # Simple regex pattern for CREATE DYNAMIC TABLE
    # Format: CREATE DYNAMIC TABLE [schema.]name
    #         TARGET_LAG = 'interval'
    #         [REFRESH_STRATEGY = 'strategy']
    #         [DEDUPLICATE = true|false]
    #         [CARDINALITY_THRESHOLD = 0.3]
    #         AS query
    
    @staticmethod
    def parse(ddl: str) -> DynamicTableDefinition:
        """Parse CREATE DYNAMIC TABLE DDL.
        
        Args:
            ddl: DDL statement
            
        Returns:
            Parsed table definition
            
        Raises:
            ValueError: If DDL is invalid
        """
        # Normalize whitespace
        ddl = " ".join(ddl.split())
        
        # Extract table name
        # Match: CREATE DYNAMIC TABLE [IF NOT EXISTS] [schema.]table
        name_match = re.search(
            r"CREATE\s+DYNAMIC\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:(\w+)\.(\w+)|(\w+))",
            ddl,
            re.IGNORECASE
        )
        if not name_match:
            raise ValueError("Invalid CREATE DYNAMIC TABLE syntax: missing table name")
        
        if name_match.group(1) and name_match.group(2):
            schema_name = name_match.group(1)
            table_name = name_match.group(2)
        else:
            schema_name = "dynamic"
            table_name = name_match.group(3)
        
        # Extract TARGET_LAG
        lag_match = re.search(r"TARGET_LAG\s*=\s*'([^']+)'", ddl, re.IGNORECASE)
        if not lag_match:
            raise ValueError("TARGET_LAG is required")
        target_lag = lag_match.group(1)
        
        # Extract optional parameters
        strategy_match = re.search(r"REFRESH_STRATEGY\s*=\s*'(\w+)'", ddl, re.IGNORECASE)
        refresh_strategy = strategy_match.group(1) if strategy_match else "AFFECTED_KEYS"
        
        dedup_match = re.search(r"DEDUPLICATE\s*=\s*(true|false)", ddl, re.IGNORECASE)
        deduplicate = dedup_match.group(1).lower() == "true" if dedup_match else False
        
        threshold_match = re.search(r"CARDINALITY_THRESHOLD\s*=\s*([\d.]+)", ddl, re.IGNORECASE)
        cardinality_threshold = float(threshold_match.group(1)) if threshold_match else 0.3
        
        # Extract query after AS
        as_match = re.search(r"\bAS\s+(.+)$", ddl, re.IGNORECASE | re.DOTALL)
        if not as_match:
            raise ValueError("Missing AS clause with query")
        query_sql = as_match.group(1).strip()
        
        # Parse query to extract source tables and GROUP BY columns
        source_tables = DDLParser._extract_source_tables(query_sql)
        group_by_columns = DDLParser._extract_group_by_columns(query_sql)
        
        return DynamicTableDefinition(
            name=table_name,
            schema_name=schema_name,
            query_sql=query_sql,
            target_lag=target_lag,
            source_tables=source_tables,
            group_by_columns=group_by_columns,
            refresh_strategy=refresh_strategy,
            deduplicate=deduplicate,
            cardinality_threshold=cardinality_threshold,
        )
    
    @staticmethod
    def _extract_source_tables(query: str) -> List[str]:
        """Extract source table names from query.
        
        Args:
            query: SQL query
            
        Returns:
            List of table names (schema.table format if schema is specified)
        """
        try:
            parsed = sqlglot.parse_one(query, read="duckdb")
            tables = set()
            
            for table in parsed.find_all(exp.Table):
                table_name = table.name
                # Include schema if present, otherwise just table name
                if table.catalog:
                    tables.add(f"{table.catalog}.{table_name}")
                else:
                    tables.add(table_name)
            
            return sorted(tables)
        except Exception as e:
            raise ValueError(f"Failed to parse query: {e}")
    
    @staticmethod
    def _extract_group_by_columns(query: str) -> List[str]:
        """Extract GROUP BY columns from query.
        
        Args:
            query: SQL query
            
        Returns:
            List of column names
        """
        try:
            parsed = sqlglot.parse_one(query, read="duckdb")
            group_by = parsed.find(exp.Group)
            
            if not group_by:
                return []
            
            columns = []
            for expr in group_by.expressions:
                if isinstance(expr, exp.Column):
                    columns.append(expr.name)
                else:
                    # For expressions, use the SQL representation
                    columns.append(expr.sql())
            
            return columns
        except Exception as e:
            raise ValueError(f"Failed to parse query: {e}")


class DependencyGraph:
    """Manage dynamic table dependencies and detect cycles."""
    
    def __init__(self):
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
        for node in self.graph:
            in_degree[node] = len(self.graph[node])
        
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
