"""Dynamic Tables for DuckLake - Self-refreshing materialized views."""

from dynamic_tables.parser import DynamicTableDefinition, DependencyGraph, extract_source_tables
from dynamic_tables.metadata import MetadataStore
from dynamic_tables.refresh import DynamicTableRefresher

__version__ = "0.1.0"

__all__ = [
    "DynamicTableDefinition",
    "DependencyGraph",
    "extract_source_tables",
    "MetadataStore",
    "DynamicTableRefresher",
]
