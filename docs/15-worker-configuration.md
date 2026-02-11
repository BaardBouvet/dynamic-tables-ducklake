# Worker Configuration

## Overview

The single-worker deployment requires two database connections:
- **PostgreSQL**: Metadata storage
- **DuckDB/DuckLake**: Data storage with CDC

## Required Parameters

```bash
python -m dynamic_tables.worker \
  --pg-url postgresql://localhost:5432/metadata \
  --duckdb-path /data/lake.db
```

**`--pg-url`** (required)
- PostgreSQL connection string
- Format: `postgresql://[user[:password]@][host][:port][/dbname]`
- Example: `postgresql://user:pass@postgres:5432/metadata`

**`--duckdb-path`** (required)
- Path to DuckDB database file
- Local: `/data/lake.db`, `./lake.duckdb`
- Remote: `ducklake:s3://bucket/lake.duckdb`
- Memory: `:memory:` (testing only)

## Optional Parameters

**`--poll-interval`** (default: `60`)
- Seconds between polling cycles
- Example: `--poll-interval 30`

**`--log-level`** (default: `INFO`)
- Logging verbosity: `DEBUG`, `INFO`, `WARNING`, `ERROR`

**`--duckdb-settings`**
- Pass-through DuckDB configuration options
- Format: semicolon-separated `key=value` pairs
- Example: `--duckdb-settings "threads=8;memory_limit='12GB'"`

**Common DuckDB settings:**
```bash
--duckdb-settings "threads=8;memory_limit='12GB';temp_directory='/tmp/duckdb'"
```

**DuckLake features:**
```bash
# Enable data inlining for small values
--duckdb-settings "ducklake_inline_threshold=1024"

# Configure compression
--duckdb-settings "ducklake_compression='zstd';ducklake_compression_level=3"

# Snapshot retention
--duckdb-settings "ducklake_snapshot_retention_days=30"
```

See [DuckDB configuration](https://duckdb.org/docs/configuration/overview) and [DuckLake docs](https://ducklake.select/docs/) for all available options.

## Environment Variables

```bash
export DYNAMIC_TABLES_PG_URL="postgresql://localhost:5432/metadata"
export DYNAMIC_TABLES_DUCKDB_PATH="/data/lake.db"
export DYNAMIC_TABLES_POLL_INTERVAL="60"
export DYNAMIC_TABLES_LOG_LEVEL="INFO"
export DYNAMIC_TABLES_DUCKDB_SETTINGS="threads=8;memory_limit='12GB'"

python -m dynamic_tables.worker
```

CLI arguments override environment variables.

## Docker Deployment

```bash
docker run -d \
  -e DYNAMIC_TABLES_PG_URL=postgresql://user:pass@postgres:5432/metadata \
  -e DYNAMIC_TABLES_DUCKDB_PATH=/data/lake.db \
  -e DYNAMIC_TABLES_DUCKDB_SETTINGS="threads=8;memory_limit='12GB'" \
  -v /path/to/data:/data \
  dynamic-tables-worker:latest
```

### Docker Compose

```yaml
version: '3.8'
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: metadata
      POSTGRES_USER: dt_user
      POSTGRES_PASSWORD: dt_password
    volumes:
      - postgres-data:/var/lib/postgresql/data
  
  worker:
    image: dynamic-tables-worker:latest
    depends_on:
      - postgres
    environment:
      DYNAMIC_TABLES_PG_URL: postgresql://dt_user:dt_password@postgres:5432/metadata
      DYNAMIC_TABLES_DUCKDB_PATH: /data/lake.db
      DYNAMIC_TABLES_DUCKDB_SETTINGS: "threads=8;memory_limit='12GB'"
    volumes:
      - lake-data:/data

volumes:
  postgres-data:
  lake-data:
```

## Kubernetes Deployment

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: dynamic-tables-secrets
type: Opaque
stringData:
  pg-url: postgresql://user:pass@postgres:5432/metadata
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: dynamic-tables-config
data:
  duckdb-settings: "threads=8;memory_limit='12GB';temp_directory='/tmp/duckdb'"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dynamic-tables-worker
spec:
  replicas: 1
  selector:
    matchLabels:
      app: dynamic-tables-worker
  template:
    metadata:
      labels:
        app: dynamic-tables-worker
    spec:
      containers:
      - name: worker
        image: dynamic-tables-worker:latest
        env:
        - name: DYNAMIC_TABLES_PG_URL
          valueFrom:
            secretKeyRef:
              name: dynamic-tables-secrets
              key: pg-url
        - name: DYNAMIC_TABLES_DUCKDB_PATH
          value: /data/lake.db
        - name: DYNAMIC_TABLES_DUCKDB_SETTINGS
          valueFrom:
            configMapKeyRef:
              name: dynamic-tables-config
              key: duckdb-settings
        resources:
          requests:
            memory: "8Gi"
            cpu: "4"
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

## Implementation

The worker applies settings on startup:

```python
def initialize_duckdb(args):
    import duckdb
    
    conn = duckdb.connect(args.duckdb_path)
    conn.execute("INSTALL ducklake")
    conn.execute("LOAD ducklake")
    
    # Apply pass-through settings
    if args.duckdb_settings:
        for setting in args.duckdb_settings.split(';'):
            if '=' in setting:
                key, value = setting.split('=', 1)
                conn.execute(f"SET {key.strip()} = {value.strip()}")
    
    return conn
```

## Common Configurations

**Development (laptop):**
```bash
--duckdb-settings "threads=4;memory_limit='4GB'"
```

**Production (16GB RAM):**
```bash
--duckdb-settings "threads=8;memory_limit='12GB';temp_directory='/tmp/duckdb'"
```

**Remote storage (S3):**
```bash
--duckdb-path ducklake:s3://bucket/lake.duckdb \
--duckdb-settings "s3_region='us-west-2'"
# AWS credentials via AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env vars
```

**Memory-constrained (4GB RAM):**
```bash
--duckdb-settings "threads=2;memory_limit='2GB';temp_directory='/mnt/spill';max_temp_directory_size='100GB'"
```

## Security

- Use environment variables for credentials in production
- Use Kubernetes Secrets for sensitive data
- Enable PostgreSQL SSL: `postgresql://...?sslmode=require`
- For S3/cloud storage, prefer IAM roles over explicit credentials

## Troubleshooting

**Connection errors:**
- Verify PostgreSQL is running: `pg_isready -h host -p 5432`
- Check DuckDB file path exists and is writable
- Ensure DuckLake extension is installed

**Out of memory:**
- Increase `memory_limit` in `--duckdb-settings`
- Ensure adequate disk space for spilling (`temp_directory`)
- See [Large Cardinality Handling](13-large-cardinality-handling.md)

**Worker not refreshing:**
- Check logs with `--log-level DEBUG`
- Verify tables exist: `SELECT * FROM dynamic_tables`
- Check source tables have CDC enabled in DuckLake

