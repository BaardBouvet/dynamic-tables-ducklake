"""Profiling utilities for benchmarking and performance analysis."""

import json
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import duckdb


@dataclass
class OperationMetrics:
    """Metrics for a single operation."""

    operation: str
    duration_seconds: float
    rows_processed: int = 0
    memory_mb: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def rows_per_second(self) -> float:
        """Calculate throughput in rows/second."""
        if self.duration_seconds > 0 and self.rows_processed > 0:
            return self.rows_processed / self.duration_seconds
        return 0.0


@dataclass
class BenchmarkReport:
    """Aggregated benchmark report for a complete test scenario."""

    scenario: str
    total_duration_seconds: float
    operations: list[OperationMetrics] = field(default_factory=list)
    strategy: str = ""  # incremental or full
    decision_metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def total_rows_processed(self) -> int:
        """Total rows across all operations."""
        return sum(op.rows_processed for op in self.operations)

    @property
    def avg_throughput(self) -> float:
        """Average rows/second across benchmark."""
        if self.total_duration_seconds > 0 and self.total_rows_processed > 0:
            return self.total_rows_processed / self.total_duration_seconds
        return 0.0

    @property
    def peak_memory_mb(self) -> float:
        """Peak memory usage across all operations."""
        return max((op.memory_mb for op in self.operations), default=0.0)

    def add_operation(self, metrics: OperationMetrics) -> None:
        """Add operation metrics to the report."""
        self.operations.append(metrics)

    def to_dict(self) -> dict[str, Any]:
        """Convert report to dictionary for serialization."""
        return {
            "scenario": self.scenario,
            "total_duration_seconds": self.total_duration_seconds,
            "total_rows_processed": self.total_rows_processed,
            "avg_throughput": self.avg_throughput,
            "peak_memory_mb": self.peak_memory_mb,
            "strategy": self.strategy,
            "decision_metadata": self.decision_metadata,
            "operations": [asdict(op) for op in self.operations],
        }

    def save_json(self, filepath: Path) -> None:
        """Save report to JSON file."""
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def load_json(cls, filepath: Path) -> "BenchmarkReport":
        """Load report from JSON file."""
        with open(filepath, "r") as f:
            data = json.load(f)
        operations = [OperationMetrics(**op) for op in data.pop("operations", [])]
        # Remove computed properties from data before creating instance
        data.pop("total_rows_processed", None)
        data.pop("avg_throughput", None)
        data.pop("peak_memory_mb", None)
        report = cls(**data)
        report.operations = operations
        return report


class OperationTimer:
    """Context manager for timing operations with detailed metrics."""

    def __init__(self, operation_name: str, rows_processed: int = 0):
        self.operation_name = operation_name
        self.rows_processed = rows_processed
        self.start_time = 0.0
        self.duration = 0.0
        self.metadata: dict[str, Any] = {}

    def __enter__(self) -> "OperationTimer":
        """Start timing."""
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Stop timing."""
        self.duration = time.perf_counter() - self.start_time

    def get_metrics(self) -> OperationMetrics:
        """Get metrics for this operation."""
        return OperationMetrics(
            operation=self.operation_name,
            duration_seconds=self.duration,
            rows_processed=self.rows_processed,
            metadata=self.metadata,
        )


@contextmanager
def measure_operation(operation_name: str, rows_processed: int = 0) -> OperationTimer:
    """Context manager to measure an operation and yield a timer.

    Usage:
        with measure_operation("my_op", rows=1000) as timer:
            do_work()
            timer.metadata["custom"] = value
        metrics = timer.get_metrics()
    """
    timer = OperationTimer(operation_name, rows_processed)
    with timer:
        yield timer


def explain_analyze(conn: duckdb.DuckDBPyConnection, query: str) -> dict[str, Any]:
    """Execute EXPLAIN ANALYZE and return parsed results.

    Returns timing and execution plan information from DuckDB.
    """
    explain_query = f"EXPLAIN ANALYZE {query}"
    result = conn.execute(explain_query).fetchall()

    # Parse the explain output
    explain_lines = [row[1] for row in result if len(row) > 1]
    explain_text = "\n".join(explain_lines)

    # Extract timing if available (DuckDB format varies)
    timing_info = {}
    for line in explain_lines:
        if "Time:" in line or "time:" in line:
            timing_info["explain_line"] = line.strip()

    return {
        "query": query,
        "explain_output": explain_text,
        "timing": timing_info,
    }


def get_memory_usage_mb(conn: duckdb.DuckDBPyConnection) -> float:
    """Get current memory usage from DuckDB in MB."""
    try:
        _ = conn.execute("SELECT current_setting('memory_limit') as limit").fetchone()
        # This gets the memory limit, not actual usage
        # DuckDB doesn't expose actual memory usage via SQL easily
        # For now, return 0 and rely on external profiling
        return 0.0
    except Exception:
        return 0.0


def configure_duckdb_for_benchmarks(
    conn: duckdb.DuckDBPyConnection,
    threads: int = 4,
    memory_limit: str = "4GB",
) -> None:
    """Configure DuckDB connection for optimal benchmark performance.

    Args:
        conn: DuckDB connection to configure
        threads: Number of threads for parallel execution
        memory_limit: Memory limit (e.g., "4GB", "8GB")
    """
    conn.execute(f"SET threads = {threads}")
    conn.execute(f"SET memory_limit = '{memory_limit}'")
    conn.execute("SET temp_directory = '/tmp/duckdb_temp'")
    conn.execute("SET preserve_insertion_order = false")  # Performance optimization
    conn.execute("SET enable_progress_bar = false")  # Cleaner benchmark output


class BenchmarkSession:
    """Session for collecting multiple benchmark reports."""

    def __init__(self, session_name: str):
        self.session_name = session_name
        self.reports: list[BenchmarkReport] = []
        self.start_time = time.perf_counter()

    def add_report(self, report: BenchmarkReport) -> None:
        """Add a benchmark report to the session."""
        self.reports.append(report)

    def save_session(self, output_dir: Path) -> None:
        """Save all reports in the session."""
        session_dir = output_dir / self.session_name
        session_dir.mkdir(parents=True, exist_ok=True)

        for i, report in enumerate(self.reports):
            filename = f"{report.scenario.replace(' ', '_')}_{i:03d}.json"
            report.save_json(session_dir / filename)

        # Save session summary
        summary = {
            "session_name": self.session_name,
            "total_duration": time.perf_counter() - self.start_time,
            "total_reports": len(self.reports),
            "scenarios": [r.scenario for r in self.reports],
        }
        with open(session_dir / "session_summary.json", "w") as f:
            json.dump(summary, f, indent=2)
