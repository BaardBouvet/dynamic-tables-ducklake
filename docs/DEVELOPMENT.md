# Development Setup

## Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) - Fast Python package installer
- Docker - Required for running tests with testcontainers
- DuckLake extension - Must be available in your DuckDB installation

## DuckLake Extension

This project requires the DuckLake extension for DuckDB. DuckLake provides:
- CDC (Change Data Capture) via `table_changes()`
- Snapshot isolation with `FOR SYSTEM_TIME AS OF SNAPSHOT`
- Object storage backend integration

To install DuckLake:
```sql
INSTALL ducklake;
LOAD ducklake;
```

See [DuckLake documentation](https://ducklake.select/docs/) for details.

## Installing Docker

On Ubuntu/Debian:
```bash
sudo apt install docker.io
sudo systemctl start docker
sudo usermod -aG docker $USER  # Add your user to docker group
# Log out and back in for the group change to take effect
```

Or using snap:
```bash
sudo snap install docker
```

Verify Docker is running:
```bash
docker ps
```

## Setup

1. Create virtual environment and install dependencies:
```bash
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"
```

2. Run tests (requires Docker):
```bash
pytest -v
```

## Project Structure

```
src/dynamic_tables/
  - metadata.py          # PostgreSQL metadata store
  - cli.py              # CLI commands (coming soon)
  - parser.py           # DDL parser (coming soon)
  - refresh.py          # Refresh logic (coming soon)

tests/
  - conftest.py         # Test fixtures with testcontainers
  - test_infrastructure.py   # Infrastructure tests
```

## Testing with Testcontainers

The test suite uses testcontainers to spin up:
- PostgreSQL (metadata store)
- MinIO (object storage backend)
- DuckDB with S3-compatible storage

These containers are automatically managed by pytest fixtures and cleaned up after tests.
