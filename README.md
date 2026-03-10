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
| `extra.catalog` | str | `null` | Catalog type: `glue`, `nessie`, `rest`, `hadoop` (Iceberg) · `unity`, `hms` (Delta) |
| `extra.catalog_name` | str | `default` | Spark catalog identifier (used in `catalog_name.db.table`) |
| `extra.catalog_uri` | str | `null` | Catalog REST/Nessie endpoint URI |
| `extra.catalog_warehouse` | str | `null` | Warehouse path (S3/GCS/HDFS URI) |
| `bucket_name` | str | - | S3 bucket name (used when `path` is not set) |
| `s3_prefix` | str | - | S3 key prefix inside the bucket |
| `aws_access_key` | str | - | AWS access key for S3 |
| `aws_secret_key` | str | - | AWS secret key for S3 |
| `region` | str | - | AWS region for S3 / Glue |
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

### Apache Iceberg — Glue Catalog

```yaml
connections:
  source:
    variant: file
    extra:
      format: iceberg
      catalog: glue
      catalog_name: my_glue       # Spark catalog identifier
      catalog_warehouse: s3a://my-bucket/warehouse
    aws_access_key: ${AWS_ACCESS_KEY_ID}
    aws_secret_key: ${AWS_SECRET_ACCESS_KEY}
    region: eu-central-1

pipelines:
  - name: glue_pipeline
    source: source
    destination: pg_target
    tables:
      - name: my_db.my_table      # <glue_database>.<table>
        target_name: stg_my_table
```

### Apache Iceberg — Nessie Catalog

```yaml
connections:
  source:
    variant: file
    extra:
      format: iceberg
      catalog: nessie
      catalog_name: nessie
      catalog_uri: http://nessie-server:19120/api/v1
      catalog_warehouse: s3a://my-bucket/warehouse
      nessie_ref: main              # branch/tag (default: main)
      nessie_auth_type: BEARER      # NONE | BEARER
      nessie_token: ${NESSIE_TOKEN}
    aws_access_key: ${AWS_ACCESS_KEY_ID}
    aws_secret_key: ${AWS_SECRET_ACCESS_KEY}
    region: eu-central-1

pipelines:
  - name: nessie_pipeline
    source: source
    destination: pg_target
    tables:
      - name: my_db.my_table
        target_name: stg_my_table
```

### Apache Iceberg — REST Catalog (Polaris / Unity / custom)

```yaml
connections:
  source:
    variant: file
    extra:
      format: iceberg
      catalog: rest
      catalog_name: polaris
      catalog_uri: https://polaris.example.com/api/catalog
      catalog_warehouse: my_warehouse   # warehouse name or S3 path
      rest_credential: "client_id:client_secret"  # OAuth2 client credentials
      rest_scope: PRINCIPAL_ROLE:my_role
    aws_access_key: ${AWS_ACCESS_KEY_ID}
    aws_secret_key: ${AWS_SECRET_ACCESS_KEY}

pipelines:
  - name: polaris_pipeline
    source: source
    destination: pg_target
    tables:
      - name: my_namespace.my_table
        target_name: stg_my_table
```

### Apache Iceberg — Hadoop Catalog (local / HDFS)

```yaml
connections:
  source:
    variant: file
    extra:
      format: iceberg
      catalog: hadoop
      catalog_name: local
      catalog_warehouse: /data/iceberg-warehouse  # local or hdfs:// path

pipelines:
  - name: hadoop_catalog_pipeline
    source: source
    destination: pg_target
    tables:
      - name: my_db.my_table
        target_name: stg_my_table
```

### Delta Lake — path-based (no catalog)

```yaml
connections:
  source:
    variant: file
    extra:
      storage: s3
      format: delta
      path: s3a://my-bucket/delta-tables
```

### Delta Lake — Hive Metastore (HMS)

```yaml
connections:
  source:
    variant: file
    extra:
      format: delta
      catalog: hms
      catalog_name: spark_catalog
      catalog_uri: thrift://hive-metastore:9083

pipelines:
  - name: hms_delta_pipeline
    source: source
    destination: pg_target
    tables:
      - name: my_db.my_table     # <hive_database>.<table>
        target_name: stg_my_table
```

### Delta Lake — Unity Catalog

```yaml
connections:
  source:
    variant: file
    extra:
      format: delta
      catalog: unity
      catalog_name: my_unity_catalog
      catalog_uri: https://my-workspace.azuredatabricks.net
      unity_token: ${DATABRICKS_TOKEN}

pipelines:
  - name: unity_pipeline
    source: source
    destination: pg_target
    tables:
      - name: my_schema.my_table   # <schema>.<table> within Unity Catalog
        target_name: stg_my_table
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
