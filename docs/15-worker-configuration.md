# Worker Configuration

Single-worker deployment configuration for PostgreSQL metadata and DuckDB/DuckLake data storage.

## CLI Parameters

```bash
python -m dynamic_tables.worker \
  --pg-url postgresql://localhost:5432/metadata \
  --duckdb-path /data/lake.db \
  --poll-interval 60 \
  --log-level INFO \
  --duckdb-settings "threads=8;memory_limit='12GB'"
```

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `--pg-url` | Yes | - | PostgreSQL connection string |
| `--duckdb-path` | Yes | - | DuckDB file path or `:memory:` |
| `--poll-interval` | No | `60` | Seconds between polling cycles |
| `--log-level` | No | `INFO` | `DEBUG`\|`INFO`\|`WARNING`\|`ERROR` |
| `--duckdb-settings` | No | - | Semicolon-separated `key=value` pairs |

**DuckDB path formats:**
- Local: `/data/lake.db`, `./lake.duckdb`
- Remote: `ducklake:s3://bucket/lake.duckdb`
- Memory: `:memory:` (testing only)

**Common DuckDB settings:**
```bash
--duckdb-settings "threads=8;memory_limit='12GB';temp_directory='/tmp/duckdb'"
```

**DuckLake options:**
```bash
--duckdb-settings "ducklake_inline_threshold=1024;ducklake_snapshot_retention_days=30"
```

See [DuckDB configuration](https://duckdb.org/docs/configuration/overview) and [DuckLake docs](https://ducklake.select/docs/).

## Environment Variables

All CLI parameters have environment variable equivalents. CLI arguments override environment variables.

```bash
export DYNAMIC_TABLES_PG_URL="postgresql://localhost:5432/metadata"
export DYNAMIC_TABLES_DUCKDB_PATH="/data/lake.db"
export DYNAMIC_TABLES_POLL_INTERVAL="60"
export DYNAMIC_TABLES_LOG_LEVEL="INFO"
export DYNAMIC_TABLES_DUCKDB_SETTINGS="threads=8;memory_limit='12GB'"

python -m dynamic_tables.worker
```

## Docker

```bash
docker run -d \
  -e DYNAMIC_TABLES_PG_URL=postgresql://user:pass@postgres:5432/metadata \
  -e DYNAMIC_TABLES_DUCKDB_PATH=/data/lake.db \
  -e DYNAMIC_TABLES_DUCKDB_SETTINGS="threads=8;memory_limit='12GB'" \
  -v /path/to/data:/data \
  dynamic-tables-worker:latest
```

**Docker Compose:**
```yaml
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: metadata
    volumes:
      - postgres-data:/var/lib/postgresql/data
  
  worker:
    image: dynamic-tables-worker:latest
    depends_on: [postgres]
    environment:
      DYNAMIC_TABLES_PG_URL: postgresql://postgres:5432/metadata
      DYNAMIC_TABLES_DUCKDB_PATH: /data/lake.db
    volumes:
      - lake-data:/data

volumes:
  postgres-data:
  lake-data:
```

## Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dynamic-tables-worker
spec:
  replicas: 1
  template:
    spec:
      containers:
      - name: worker
        image: dynamic-tables-worker:latest
        env:
        - name: DYNAMIC_TABLES_PG_URL
          valueFrom:
            secretKeyRef:
              name: dt-secrets
              key: pg-url
        - name: DYNAMIC_TABLES_DUCKDB_PATH
          value: /data/lake.db
        resources:
          limits:
            memory: "16Gi"
            cpu: "8"
        volumeMounts:
        - name: lake-storage
          mountPath: /data
      volumes:
      - name: lake-storage
        persistentVolumeClaim:
          claimName: lake-pvc
```

## Common Configurations

| Environment | Settings |
|-------------|----------|
| **Development** | `threads=4;memory_limit='4GB'` |
| **Production 16GB** | `threads=8;memory_limit='12GB';temp_directory='/tmp/duckdb'` |
| **S3 storage** | `s3_region='us-west-2'` (use `ducklake:s3://...` path) |
| **Memory-constrained** | `threads=2;memory_limit='2GB';temp_directory='/mnt/spill'` |

## Security

- Use environment variables or Kubernetes Secrets for credentials
- Enable PostgreSQL SSL: `postgresql://...?sslmode=require`
- For cloud storage, prefer IAM roles over explicit credentials

## Troubleshooting

**Connection errors:**
- Verify PostgreSQL: `pg_isready -h host -p 5432`
- Check DuckDB path is writable
- Ensure DuckLake extension installed

**Out of memory:**
- Increase `memory_limit` setting
- Ensure adequate disk space for `temp_directory`
- See [Large Cardinality Handling](13-large-cardinality-handling.md)

**Worker not refreshing:**
- Check logs: `--log-level DEBUG`
- Verify tables: `SELECT * FROM dynamic_tables`
- Confirm source tables have CDC enabled

