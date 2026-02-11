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

## Configuration Validation

The worker validates configuration on startup to fail fast with clear errors:

```python
def validate_configuration(args):
    """Validate configuration before starting worker."""
    errors = []
    
    # Check PostgreSQL connection
    try:
        pg_conn = psycopg2.connect(args.pg_url)
        pg_conn.close()
    except Exception as e:
        errors.append(f"PostgreSQL connection failed: {e}")
    
    # Check DuckDB path
    if args.duckdb_path != ':memory:':
        duckdb_dir = os.path.dirname(args.duckdb_path)
        if not os.path.exists(duckdb_dir):
            errors.append(f"DuckDB directory does not exist: {duckdb_dir}")
        if not os.access(duckdb_dir, os.W_OK):
            errors.append(f"DuckDB directory not writable: {duckdb_dir}")
    
    # Check DuckDB can connect
    try:
        duck_conn = duckdb.connect(args.duckdb_path)
        duck_conn.execute("INSTALL ducklake")
        duck_conn.execute("LOAD ducklake")
        duck_conn.close()
    except Exception as e:
        errors.append(f"DuckDB initialization failed: {e}")
    
    # Validate poll interval
    if args.poll_interval < 1:
        errors.append(f"Poll interval must be >= 1 second, got {args.poll_interval}")
    
    # Parse DuckDB settings
    if args.duckdb_settings:
        for setting in args.duckdb_settings.split(';'):
            if '=' not in setting:
                errors.append(f"Invalid DuckDB setting format: '{setting}' (expected key=value)")
    
    # Report errors
    if errors:
        logger.error("Configuration validation failed:")
        for error in errors:
            logger.error(f"  - {error}")
        sys.exit(1)
    else:
        logger.info("Configuration validated successfully")

def main():
    args = parse_args()
    
    # Validate before doing anything else
    validate_configuration(args)
    
    # ... continue with worker initialization
```

**Benefits:**
- **Fail fast**: Catch configuration errors at startup, not during first refresh
- **Clear errors**: Specific messages about what's wrong
- **Prevents silent failures**: Don't start worker that can't work

**Example validation output:**

```
2026-02-11 10:00:00 INFO Configuration validated successfully
2026-02-11 10:00:00 INFO PostgreSQL: postgresql://postgres:5432/metadata
2026-02-11 10:00:00 INFO DuckDB: /data/lake.db
2026-02-11 10:00:00 INFO DuckLake extension: loaded
2026-02-11 10:00:00 INFO Poll interval: 60s
2026-02-11 10:00:00 INFO Worker starting...
```

**Example validation failure:**

```
2026-02-11 10:00:00 ERROR Configuration validation failed:
2026-02-11 10:00:00 ERROR   - PostgreSQL connection failed: could not connect to server: Connection refused
2026-02-11 10:00:00 ERROR   - DuckDB directory not writable: /readonly/data
2026-02-11 10:00:00 ERROR   - Invalid DuckDB setting format: 'threads8' (expected key=value)
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

