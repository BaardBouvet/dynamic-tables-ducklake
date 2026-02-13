# Benchmarking Guide

This guide covers the benchmarking infrastructure for validating Phase 2 optimizations (CDC-based incremental refresh with affected keys strategy).

## Quick Start

```bash
# Install benchmark dependencies
just install

# Run quick benchmarks (~2 minutes)
just benchmark-quick

# Run full benchmark suite (~30 minutes)
just benchmark

# Compare against baseline
just benchmark-compare main

# Save new baseline after verified improvements
just benchmark-save phase2-initial
```

## Overview

The benchmark suite validates Phase 2 performance goals:

- **10-100x speedup** for incremental refresh vs full refresh at low cardinality (<30% affected)
- **Sub-second refresh** for <100K affected keys
- **Linear scaling** up to 10M affected keys with out-of-core processing
- **Correct strategy selection** at 30% cardinality threshold
- **<5% Python overhead** (95%+ time in SQL execution)

## Benchmark Categories

### 1. Affected Keys Extraction

Tests `table_changes()` CDC query performance across different:
- Dataset sizes: 100K to 100M rows
- Affected cardinality: 0.1% to 50%
- Multi-table scenarios (UNION of multiple CDC sources)

**Target**: 1M+ keys/second extraction rate

### 2. Cardinality Calculation

Tests strategy decision timing:
- Count affected keys vs total rows
- Calculate cardinality ratio
- Decide incremental (<30%) vs full refresh (≥30%)

**Target**: <100ms decision time

### 3. Incremental DELETE

Tests DELETE performance by affected cardinality:
- Small (0.1%), medium (1%), large (10%), threshold (30%) scenarios
- Various table sizes

**Target**: 1M+ rows/second deletion rate

### 4. Incremental INSERT

Tests INSERT with aggregations:
- Simple GROUP BY
- 2-way JOINs (fact + dimension)
- N-way JOINs (3-5 tables)
- Snapshot isolation queries

**Target**: 1M+ rows/second insertion rate

### 5. Full Refresh Baseline

Baseline for comparison:
- DELETE all + INSERT all
- Measures worst-case performance

**Used to validate**: 10-100x speedup claims

### 6. End-to-End Workflows

Complete refresh scenarios:
- CDC → Cardinality Check → DELETE → INSERT
- Multi-table dependency chains
- Parallel refresh simulations

**Target**: Total workflow <5% Python overhead

## Data Profiles

Predefined synthetic data profiles for reproducible benchmarks:

| Profile | Rows     | Affected % | GROUP BY Cardinality | Run Time |
|---------|----------|------------|---------------------|----------|
| tiny    | 1K       | 10%        | 100                 | <10s     |
| small   | 100K     | 10%        | 1K                  | ~30s     |
| medium  | 1M       | 10%        | 10K                 | ~5min    |
| large   | 10M      | 10%        | 100K                | ~15min   |
| xlarge  | 100M     | 10%        | 1M                  | ~60min   |

Quick benchmarks use `tiny` and `small`. Full suite uses all profiles.

## Running Benchmarks

### Quick Validation (Development)

```bash
# Fast iteration during development
just benchmark-quick

# Run specific test
just benchmark-one "test_extract_affected_keys_quick"

# With memory profiling
just benchmark-profile
```

### Comprehensive Testing (Pre-Merge)

```bash
# Full suite with all dataset sizes
just benchmark

# Save as baseline before merging
just benchmark-save feature-xyz
```

### Comparing Performance

```bash
# Compare against main branch baseline
just benchmark-compare main

# Compare against specific baseline
just benchmark-compare phase2-initial

# Generate markdown report for PR
just benchmark-report
```

## Interpreting Results

### Pytest-Benchmark Output

```
-------------------------- benchmark: 5 tests --------------------------
Name                              Min       Max      Mean    StdDev   Median
------------------------------------------------------------------------
test_extract_affected_keys     0.023s    0.025s    0.024s   0.001s    0.024s
test_calculate_cardinality     0.0001s   0.0002s   0.0001s  0.00002s  0.0001s
test_delete_affected           0.015s    0.018s    0.016s   0.001s    0.016s
test_insert_aggregated         0.032s    0.036s    0.034s   0.002s    0.034s
test_full_refresh_baseline     0.245s    0.252s    0.248s   0.003s    0.248s
------------------------------------------------------------------------
```

**Key Metrics:**
- **Median**: Most stable measurement (use for comparisons)
- **StdDev**: Low = consistent performance, High = investigate variance
- **Min/Max**: Identify outliers or warm-up effects

### Profiling Data

With `--profile`, get operation-level breakdowns:

```json
{
  "scenario": "incremental_refresh_small",
  "total_duration_seconds": 0.082,
  "avg_throughput": 1234567.89,
  "peak_memory_mb": 145.2,
  "strategy": "incremental",
  "operations": [
    {
      "operation": "cdc_extract",
      "duration_seconds": 0.024,
      "rows_processed": 10000,
      "metadata": {}
    },
    {
      "operation": "delete_affected",
      "duration_seconds": 0.016,
      "rows_processed": 1000
    },
    {
      "operation": "insert_recomputed",
      "duration_seconds": 0.034,
      "rows_processed": 1000
    }
  ]
}
```

**Look for:**
- Operations taking >50% of total time (bottlenecks)
- Throughput (rows/sec) below targets
- Memory usage approaching limits (triggers spilling)

### Performance Targets

Compare your results against Phase 2 goals:

| Operation              | Target Throughput | Target Latency |
|------------------------|------------------|----------------|
| CDC Extraction         | >1M keys/sec     | -              |
| Cardinality Check      | -                | <100ms         |
| Incremental DELETE     | >1M rows/sec     | -              |
| Incremental INSERT     | >1M rows/sec     | -              |
| Full Refresh Baseline  | >500K rows/sec   | -              |
| **Speedup Ratio**      | **10-100x** at <30% cardinality | - |

## Regression Detection

### Automatic Comparison

Benchmarks auto-save to `.benchmarks/` directory:

```
.benchmarks/
├── main/               # Main branch baseline
├── phase2-initial/     # Feature branch baseline
└── Linux-CPython-3.11-64bit/
    ├── 0001_*.json    # Latest run
    ├── 0002_*.json
    └── ...
```

### Manual Comparison

```bash
# Compare latest vs main
pytest-benchmark compare main 0001

# Generate comparison histogram
pytest-benchmark compare main 0001 --histogram
```

### CI Integration

> **Note**: GitHub Actions workflow for automated benchmark comparisons on PRs will be added in Phase 3 along with other production CI/CD infrastructure.

For now, run benchmarks manually before merging:

```bash
# Before making changes
just benchmark-save my-baseline

# After changes
just benchmark-compare my-baseline
```

## Adding New Benchmarks

### 1. Create Benchmark Test

```python
# tests/test_benchmarks_phase2.py

def test_my_new_operation(
    benchmark,
    benchmark_duckdb_conn,
    data_generator,
    quick_profile,
):
    """Benchmark description."""
    # Setup
    profile = quick_profile
    # ... create test data ...
    
    # Benchmark
    def operation():
        # ... code to benchmark ...
        pass
    
    result = benchmark(operation)
    
    # Assertions
    assert result is not None
```

### 2. Add to Quick or Full Suite

```python
# For quick benchmarks (development)
def test_operation_quick(quick_profile):
    ...

# For comprehensive benchmarks (pre-merge)
def test_operation_full(full_profile):
    ...

# For specific scenarios
@pytest.mark.parametrize("cardinality", [0.01, 0.1, 0.3, 0.5])
def test_operation_cardinality(cardinality):
    ...
```

### 3. Add Profiling Instrumentation

```python
from dynamic_tables.profiling import measure_operation, BenchmarkReport

report = BenchmarkReport("my_scenario", total_duration_seconds=0.0)

with measure_operation("step1", rows_processed=1000) as timer:
    # ... do work ...
    timer.metadata["custom_metric"] = value

report.add_operation(timer.get_metrics())

# Save detailed report
report.save_json(Path("reports/my_scenario.json"))
```

## Updating Baselines

### When to Update

Update baselines when:
- ✅ Intentional performance improvements verified
- ✅ Refactoring with validated no-regression
- ✅ Infrastructure upgrades (new DuckDB version, etc.)

Do NOT update if:
- ❌ Unexplained performance changes
- ❌ Regressions you plan to fix later
- ❌ Noisy/inconsistent results

### How to Update

```bash
# 1. Verify improvements are real (run multiple times)
just benchmark-quick
just benchmark-quick
just benchmark-quick

# 2. Run full suite
just benchmark

# 3. Update baseline
just benchmark-update-baseline

# 4. Commit to git
git add .benchmarks/
git commit -m "chore: update benchmark baselines after CDC optimization"
```

## Troubleshooting

### Inconsistent Results

**Symptom**: High StdDev, varying Min/Max

**Solutions**:
- Close other applications (reduce CPU contention)
- Disable CPU frequency scaling: `cpupower frequency-set --governor performance`
- Increase warmup rounds in `pyproject.toml`
- Run benchmarks multiple times and average

### Out of Memory

**Symptom**: Benchmark crashes or OOM errors

**Solutions**:
- Reduce dataset size (use `small` instead of `large`)
- Increase `memory_limit` in `benchmark_duckdb_config`
- Verify spill-to-disk is working (check `/tmp/duckdb_bench`)
- Reduce parallel threads

### Slow Benchmarks

**Symptom**: Benchmarks take too long

**Solutions**:
- Use `just benchmark-quick` for iteration
- Run specific test: `just benchmark-one test_name`
- Reduce parameterization (fewer cardinality ratios)
- Use smaller profiles

### Baseline Not Found

**Symptom**: `pytest-benchmark compare` fails

**Solutions**:
- Create baseline: `just benchmark-save main`
- Check `.benchmarks/` directory exists
- Ensure baseline name matches exactly

## Best Practices

1. **Run benchmarks before starting optimization work** - Establish baseline
2. **Use quick benchmarks during development** - Fast feedback loop
3. **Run full suite before merging** - Comprehensive validation
4. **Compare against baseline** - Detect regressions early
5. **Save baselines with meaningful names** - `phase2-before-optimization`, `phase2-after-caching`, etc.
6. **Commit baselines to git** - Track performance over time
7. **Profile when investigating bottlenecks** - Use `--profile` to identify hot spots
8. **Document performance improvements in PRs** - Include benchmark comparison

## Example Workflow

### Optimizing an Operation

```bash
# 1. Establish baseline
git checkout main
just benchmark-save main
git checkout feature/my-optimization

# 2. Make changes
# ... edit code ...

# 3. Verify improvement
just benchmark-quick

# 4. Compare
just benchmark-compare main
# Expected: Your operation is faster!

# 5. Full validation
just benchmark

# 6. Update baseline for this feature
just benchmark-save my-optimization

# 7. Create PR with comparison report
just benchmark-report
# Include benchmark_report.md in PR description
```

## CI/CD Integration

> **Note**: Automated CI/CD workflows will be added in Phase 3 along with other production infrastructure (Docker, monitoring, error handling).

For Phase 2, run benchmarks manually before committing changes:

```bash
# Before starting work
just benchmark-save before-changes

# After implementing changes
just benchmark-compare before-changes

# Review the comparison to ensure no regressions
```

## Performance Expectations

Based on Phase 2 design goals:

### Incremental Refresh (10% affected)
- **100K row table**: <0.1s
- **1M row table**: <1s
- **10M row table**: <10s
- **100M row table**: <2min

### Full Refresh (Baseline)
- **100K row table**: ~0.5s
- **1M row table**: ~5s
- **10M row table**: ~60s
- **100M row table**: ~20min

### Expected Speedup
- 0.1% affected: **100x** faster
- 1% affected: **50x** faster
- 10% affected: **10x** faster
- 30% affected: **3x** faster
- 50% affected: Use full refresh (comparable or slower)

## Further Reading

- [Phase 2 Implementation Plan](08-implementation-phases.md#phase-2)
- [Performance Considerations](11-performance-considerations.md)
- [Large Cardinality Handling](13-large-cardinality-handling.md)
- [pytest-benchmark docs](https://pytest-benchmark.readthedocs.io/)
