"""Test dependency management and source table extraction."""

import pytest
from dynamic_tables.parser import DependencyGraph, extract_source_tables


class TestExtractSourceTables:
    """Test source table extraction from SQL queries."""

    def test_extract_single_table(self) -> None:
        """Test extracting a single source table."""
        query = "SELECT * FROM sales"
        tables = extract_source_tables(query)

        assert tables == ["sales"]

    def test_extract_multiple_tables_join(self) -> None:
        """Test extracting multiple tables from a join."""
        query = """
        SELECT o.id, c.name, SUM(oi.amount) as total
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        JOIN order_items oi ON o.id = oi.order_id
        GROUP BY o.id, c.name
        """
        tables = extract_source_tables(query)

        assert sorted(tables) == ["customers", "order_items", "orders"]

    def test_extract_with_schema(self) -> None:
        """Test extracting tables with schema qualification."""
        query = "SELECT * FROM analytics.events JOIN main.users ON events.user_id = users.id"
        tables = extract_source_tables(query)

        assert "analytics.events" in tables
        assert "main.users" in tables

    def test_extract_from_subquery(self) -> None:
        """Test extracting tables from query with subquery."""
        query = "SELECT * FROM (SELECT * FROM sales) s JOIN customers c ON s.customer_id = c.id"
        tables = extract_source_tables(query)

        assert "sales" in tables
        assert "customers" in tables


class TestDependencyGraph:
    """Test dependency graph and cycle detection."""

    def test_add_simple_dependency(self) -> None:
        """Test adding simple dependencies."""
        graph = DependencyGraph()

        graph.add_table("a", [])
        graph.add_table("b", ["a"])
        graph.add_table("c", ["b"])

        assert len(graph.graph) == 3

    def test_detect_direct_cycle(self) -> None:
        """Test detecting direct cycle (A -> B -> A)."""
        graph = DependencyGraph()

        graph.add_table("a", ["b"])

        with pytest.raises(ValueError, match="Circular dependency"):
            graph.add_table("b", ["a"])

    def test_detect_indirect_cycle(self) -> None:
        """Test detecting indirect cycle (A -> B -> C -> A)."""
        graph = DependencyGraph()

        graph.add_table("a", ["b"])
        graph.add_table("b", ["c"])

        with pytest.raises(ValueError, match="Circular dependency"):
            graph.add_table("c", ["a"])

    def test_detect_self_cycle(self) -> None:
        """Test detecting self-reference cycle."""
        graph = DependencyGraph()

        with pytest.raises(ValueError, match="Circular dependency"):
            graph.add_table("a", ["a"])

    def test_topological_sort_simple(self) -> None:
        """Test topological sorting."""
        graph = DependencyGraph()

        graph.add_table("c", ["a", "b"])
        graph.add_table("b", ["a"])
        graph.add_table("a", [])

        order = graph.topological_sort()

        # 'a' should come before 'b', and both before 'c'
        assert order.index("a") < order.index("b")
        assert order.index("a") < order.index("c")
        assert order.index("b") < order.index("c")

    def test_topological_sort_complex(self) -> None:
        """Test topological sorting with complex dependencies."""
        graph = DependencyGraph()

        # Diamond pattern: d depends on b,c; b,c depend on a
        graph.add_table("a", [])
        graph.add_table("b", ["a"])
        graph.add_table("c", ["a"])
        graph.add_table("d", ["b", "c"])

        order = graph.topological_sort()

        assert order.index("a") < order.index("b")
        assert order.index("a") < order.index("c")
        assert order.index("b") < order.index("d")
        assert order.index("c") < order.index("d")

    def test_remove_table(self) -> None:
        """Test removing a table from the graph."""
        graph = DependencyGraph()

        graph.add_table("a", [])
        graph.add_table("b", ["a"])

        graph.remove_table("b")

        assert "b" not in graph.graph
        assert "a" in graph.graph
