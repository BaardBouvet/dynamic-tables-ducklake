#!/usr/bin/env python3
"""Benchmark runner and comparator for Phase 2 performance validation.

Usage:
    python tests/run_benchmarks.py --quick          # Run quick benchmarks only
    python tests/run_benchmarks.py --full           # Run all benchmarks
    python tests/run_benchmarks.py --compare BASELINE  # Compare against baseline
    python tests/run_benchmarks.py --profile        # Run with memory profiling
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


def run_pytest_benchmark(
    extra_args: list[str] = None,
    save_baseline: str = "",
) -> int:
    """Run pytest with benchmark configuration.

    Args:
        extra_args: Additional pytest arguments (including -m for marks)
        save_baseline: Name to save baseline as

    Returns:
        Exit code from pytest
    """
    cmd = [
        "uv",
        "run",
        "pytest",
        "tests/test_benchmarks_phase2.py",
        "--benchmark-only",
        "-v",
    ]

    if save_baseline:
        cmd.append(f"--benchmark-save={save_baseline}")

    if extra_args:
        cmd.extend(extra_args)

    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    return result.returncode


def compare_benchmarks(baseline: str, current: str = "0001") -> dict[str, Any]:
    """Compare current benchmark results against a baseline.

    Args:
        baseline: Baseline name to compare against
        current: Current benchmark name (default: latest)

    Returns:
        Comparison results dictionary
    """
    cmd = [
        "uv",
        "run",
        "pytest-benchmark",
        "compare",
        baseline,
        current,
        "--histogram",
    ]

    print(f"Comparing: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)

    return {
        "exit_code": result.returncode,
        "output": result.stdout,
    }


def generate_markdown_report(baseline: str = "main") -> str:
    """Generate markdown report for PR comments.

    Args:
        baseline: Baseline to compare against

    Returns:
        Markdown formatted report
    """
    benchmarks_dir = Path(".benchmarks")

    if not benchmarks_dir.exists():
        return "⚠️ No benchmark results found."

    # Find latest benchmark file
    bench_files = sorted(benchmarks_dir.glob("**/*.json"))
    if not bench_files:
        return "⚠️ No benchmark JSON files found."

    latest = bench_files[-1]

    try:
        with open(latest) as f:
            data = json.load(f)
    except Exception as e:
        return f"❌ Error reading benchmark data: {e}"

    # Extract benchmark results
    benchmarks = data.get("benchmarks", [])

    if not benchmarks:
        return "⚠️ No benchmark results in file."

    # Build markdown table
    report = ["## 📊 Benchmark Results\n"]
    report.append("| Test | Median (s) | Ops/sec | Stddev |")
    report.append("|------|-----------|---------|--------|")

    for bench in benchmarks:
        name = bench.get("name", "unknown")
        stats = bench.get("stats", {})
        median = stats.get("median", 0.0)
        ops_sec = 1.0 / median if median > 0 else 0.0
        stddev = stats.get("stddev", 0.0)

        report.append(f"| `{name}` | {median:.4f} | {ops_sec:.2f} | {stddev:.4f} |")

    report.append(f"\n**Total benchmarks:** {len(benchmarks)}")
    report.append(f"**Baseline:** `{baseline}`")

    return "\n".join(report)


def main():
    """Main entry point for benchmark runner."""
    parser = argparse.ArgumentParser(
        description="Run and compare benchmarks for Phase 2 optimizations"
    )

    parser.add_argument(
        "--quick",
        action="store_true",
        help="Run quick benchmarks only (tiny/small datasets)",
    )

    parser.add_argument(
        "--full",
        action="store_true",
        help="Run full benchmark suite including large datasets",
    )

    parser.add_argument(
        "--compare",
        metavar="BASELINE",
        help="Compare against specified baseline",
    )

    parser.add_argument(
        "--profile",
        action="store_true",
        help="Run with memory profiling enabled",
    )

    parser.add_argument(
        "--save",
        metavar="NAME",
        help="Save benchmark results with specified name",
    )

    parser.add_argument(
        "--markdown",
        action="store_true",
        help="Generate markdown report for CI/PR comments",
    )

    parser.add_argument(
        "--baseline-name",
        default="main",
        help="Baseline name for markdown report (default: main)",
    )

    args = parser.parse_args()

    # Determine test filter
    extra_args = []

    if args.quick:
        extra_args.extend(["-m", "quick"])
        print("🚀 Running quick benchmarks (tiny/small datasets only)...")
    elif args.full:
        print("🏃 Running full benchmark suite (all dataset sizes)...")
    else:
        # Default to quick if nothing specified
        extra_args.extend(["-m", "quick"])
        print("🚀 Running quick benchmarks by default. Use --full for complete suite.")

    if args.profile:
        extra_args.append("--benchmark-cprofile=tottime")
        print("📊 Memory profiling enabled")

    # Run benchmarks
    save_name = args.save or ""
    
    # If markdown report requested but no explicit save name, use temp name
    if args.markdown and not save_name:
        save_name = "latest-run"
    
    exit_code = run_pytest_benchmark(extra_args, save_name)

    if exit_code != 0:
        print(f"❌ Benchmarks failed with exit code {exit_code}")
        return exit_code

    print("✅ Benchmarks completed successfully")

    # Compare if requested
    if args.compare:
        print(f"\n📈 Comparing against baseline: {args.compare}")
        compare_benchmarks(args.compare)

    # Generate markdown report if requested
    if args.markdown:
        print("\n📝 Generating markdown report...")
        report = generate_markdown_report(args.baseline_name)
        print(report)

        # Save to file for CI to use
        report_file = Path("benchmark_report.md")
        report_file.write_text(report)
        print(f"\n💾 Report saved to {report_file}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
