"""Test DDL parser and dependency management."""

import pytest
from dynamic_tables.parser import DDLParser, DependencyGraph, DynamicTableDefinition


class TestDDLParser:
    """Test DDL parsing."""
    
    def test_parse_simple_table(self):
        """Test parsing a simple dynamic table."""
        ddl = """
        CREATE DYNAMIC TABLE sales_summary
        TARGET_LAG = '5 minutes'
        AS
        SELECT product_id, SUM(amount) as total_sales
        FROM sales
        GROUP BY product_id
        """
        
        definition = DDLParser.parse(ddl)
        
        assert definition.name == "sales_summary"
        assert definition.schema_name == "dynamic"
        assert definition.target_lag == "5 minutes"
        assert "sales" in definition.source_tables
        assert "product_id" in definition.group_by_columns
        assert definition.refresh_strategy == "AFFECTED_KEYS"
        assert definition.deduplicate is False
    
    def test_parse_with_schema(self):
        """Test parsing table with explicit schema."""
        ddl = """
        CREATE DYNAMIC TABLE analytics.daily_metrics
        TARGET_LAG = '1 hour'
        AS
        SELECT date, COUNT(*) as count FROM events GROUP BY date
        """
        
        definition = DDLParser.parse(ddl)
        
        assert definition.name == "daily_metrics"
        assert definition.schema_name == "analytics"
    
    def test_parse_with_options(self):
        """Test parsing with all options."""
        ddl = """
        CREATE DYNAMIC TABLE user_stats
        TARGET_LAG = '10 minutes'
        REFRESH_STRATEGY = 'FULL'
        DEDUPLICATE = true
        CARDINALITY_THRESHOLD = 0.5
        AS
        SELECT user_id, COUNT(*) FROM actions GROUP BY user_id
        """
        
        definition = DDLParser.parse(ddl)
        
        assert definition.refresh_strategy == "FULL"
        assert definition.deduplicate is True
        assert definition.cardinality_threshold == 0.5
    
    def test_parse_multiple_sources(self):
        """Test extracting multiple source tables."""
        ddl = """
        CREATE DYNAMIC TABLE order_summary
        TARGET_LAG = '5 minutes'
        AS
        SELECT o.id, c.name, SUM(oi.amount) as total
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        JOIN order_items oi ON o.id = oi.order_id
        GROUP BY o.id, c.name
        """
        
        definition = DDLParser.parse(ddl)
        
        assert len(definition.source_tables) == 3
        assert "orders" in definition.source_tables
        assert "customers" in definition.source_tables
        assert "order_items" in definition.source_tables
    
    def test_parse_missing_target_lag(self):
        """Test that missing TARGET_LAG raises error."""
        ddl = """
        CREATE DYNAMIC TABLE bad_table
        AS
        SELECT * FROM sales
        """
        
        with pytest.raises(ValueError, match="TARGET_LAG is required"):
            DDLParser.parse(ddl)
    
    def test_parse_missing_query(self):
        """Test that missing AS clause raises error."""
        ddl = """
        CREATE DYNAMIC TABLE bad_table
        TARGET_LAG = '5 minutes'
        """
        
        with pytest.raises(ValueError, match="Missing AS clause"):
            DDLParser.parse(ddl)
    
    def test_extract_group_by_expressions(self):
        """Test extracting complex GROUP BY expressions."""
        ddl = """
        CREATE DYNAMIC TABLE daily_sales
        TARGET_LAG = '1 hour'
        AS
        SELECT DATE_TRUNC('day', created_at) as day, SUM(amount)
        FROM sales
        GROUP BY DATE_TRUNC('day', created_at)
        """
        
        definition = DDLParser.parse(ddl)
        
        # Should capture the expression
        assert len(definition.group_by_columns) > 0


class TestDependencyGraph:
    """Test dependency graph and cycle detection."""
    
    def test_add_simple_dependency(self):
        """Test adding simple dependencies."""
        graph = DependencyGraph()
        
        graph.add_table("a", [])
        graph.add_table("b", ["a"])
        graph.add_table("c", ["b"])
        
        assert len(graph.graph) == 3
    
    def test_detect_direct_cycle(self):
        """Test detecting direct cycle (A -> B -> A)."""
        graph = DependencyGraph()
        
        graph.add_table("a", ["b"])
        
        with pytest.raises(ValueError, match="Circular dependency"):
            graph.add_table("b", ["a"])
    
    def test_detect_indirect_cycle(self):
        """Test detecting indirect cycle (A -> B -> C -> A)."""
        graph = DependencyGraph()
        
        graph.add_table("a", ["b"])
        graph.add_table("b", ["c"])
        
        with pytest.raises(ValueError, match="Circular dependency"):
            graph.add_table("c", ["a"])
    
    def test_detect_self_cycle(self):
        """Test detecting self-reference cycle."""
        graph = DependencyGraph()
        
        with pytest.raises(ValueError, match="Circular dependency"):
            graph.add_table("a", ["a"])
    
    def test_topological_sort_simple(self):
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
    
    def test_topological_sort_complex(self):
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
    
    def test_remove_table(self):
        """Test removing a table from the graph."""
        graph = DependencyGraph()
        
        graph.add_table("a", [])
        graph.add_table("b", ["a"])
        
        graph.remove_table("b")
        
        assert "b" not in graph.graph
        assert "a" in graph.graph
