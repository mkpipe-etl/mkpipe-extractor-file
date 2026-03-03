# mkpipe-extractor-file

Unified file-based extractor for mkpipe. Reads data from local or cloud storage in multiple formats.

## Installation

```bash
pip install mkpipe-extractor-file
```

## Supported Storage

| Storage | Scheme | Description |
|---------|--------|-------------|
| Local | (no prefix) | Local filesystem paths |
| S3 | `s3a://` | Amazon S3 / S3-compatible |
| GCS | `gs://` | Google Cloud Storage |
| ADLS | `abfss://` | Azure Data Lake Storage |
| HDFS | `hdfs://` | Hadoop Distributed File System |

## Supported Formats

`parquet`, `csv`, `json`, `orc`, `avro`, `iceberg`, `delta`

## Connection Configuration

Connection is configured in the `connections` section of `mkpipe_project.yaml`:

```yaml
connections:
  source:
    variant: file
    extra:
      storage: local        # local | s3 | gcs | adls | hdfs
      format: parquet        # parquet | csv | json | orc | avro | iceberg | delta
      path: /data/warehouse  # base path (files resolved as <path>/<table_name>)
```

### Connection Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `variant` | str | **required** | Must be `file` |
| `extra.storage` | str | `local` | Storage backend: `local`, `s3`, `gcs`, `adls`, `hdfs` |
| `extra.format` | str | `parquet` | File format to read |
| `extra.path` | str | `""` | Base path. Table name is appended: `<path>/<table_name>` |
| `extra.catalog_name` | str | `default` | Catalog name (only for `iceberg` format) |
| `bucket_name` | str | - | S3 bucket name (used when `path` is not set) |
| `s3_prefix` | str | - | S3 key prefix inside the bucket |
| `aws_access_key` | str | - | AWS access key for S3 |
| `aws_secret_key` | str | - | AWS secret key for S3 |
| `region` | str | - | AWS region for S3 |
| `credentials_file` | str | - | Service account JSON key file path for GCS |

### Path Resolution

The extractor resolves file paths in this order:
1. If `extra.path` is set: `<path>/<table_name>`
2. If `storage=s3` and `bucket_name` is set: `s3a://<bucket_name>/<s3_prefix>/<table_name>`
3. Otherwise: `<table_name>` as-is

## YAML Examples

### Local Filesystem

```yaml
connections:
  source:
    variant: file
    extra:
      storage: local
      format: parquet
      path: /data/warehouse

pipelines:
  - name: local_pipeline
    source: source
    target: my_target
    tables:
      - name: users
        target_name: stg_users
```

### Amazon S3

```yaml
connections:
  source:
    variant: file
    extra:
      storage: s3
      format: parquet
    bucket_name: my-data-bucket
    s3_prefix: raw/2024
    aws_access_key: ${AWS_ACCESS_KEY_ID}
    aws_secret_key: ${AWS_SECRET_ACCESS_KEY}
    region: eu-central-1
```

### Google Cloud Storage

```yaml
connections:
  source:
    variant: file
    extra:
      storage: gcs
      format: json
      path: gs://my-bucket/raw
    credentials_file: /secrets/gcp-sa.json
```

### Apache Iceberg

```yaml
connections:
  source:
    variant: file
    extra:
      format: iceberg
      catalog_name: my_catalog

pipelines:
  - name: iceberg_pipeline
    source: source
    target: my_target
    tables:
      - name: db.schema.my_table   # full iceberg table reference
```

### Delta Lake

```yaml
connections:
  source:
    variant: file
    extra:
      storage: s3
      format: delta
      path: s3a://my-bucket/delta-tables
```

## Replication Methods

### Full Load (default)

Reads the entire dataset each time. Write mode is `overwrite`.

```yaml
tables:
  - name: products
    target_name: stg_products
```

### Incremental

Reads only new/changed rows based on `iterate_column`. Requires a backend to track the last extracted point.

```yaml
tables:
  - name: events
    target_name: stg_events
    replication_method: incremental
    iterate_column: updated_at
```

- First run: reads all data (`overwrite`)
- Subsequent runs: filters `iterate_column >= last_point` (`append`)

## CSV Options

When `format: csv`, the extractor automatically applies:
- `header: true` (first row is header)
- `inferSchema: true` (auto-detect column types)

## Notes

- Environment variables can be used with `${VAR_NAME}` syntax in YAML
- For S3-compatible storage (MinIO, etc.), set the endpoint via Spark config in `settings.spark.extra_config`
- Iceberg format requires a properly configured Spark catalog
- Delta format requires `delta-spark` package in your Spark environment
