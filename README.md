# mkpipe-extractor-file

Unified file-based extractor for mkpipe. Reads data from local or cloud storage in multiple formats including Parquet, CSV, JSON, ORC, Avro, Apache Iceberg, and Delta Lake.

## Installation

```bash
pip install mkpipe-extractor-file
```

## Supported Storage

| Storage | Value | Scheme | Description |
|---------|-------|--------|-------------|
| Local filesystem | `local` | (no prefix) | Local or network-mounted paths |
| Amazon S3 | `s3` | `s3a://` | AWS S3 or S3-compatible (MinIO, etc.) |
| Google Cloud Storage | `gcs` | `gs://` | GCP Cloud Storage |
| Azure Data Lake | `adls` | `abfss://` | Azure Data Lake Storage Gen2 |
| HDFS | `hdfs` | `hdfs://` | Hadoop Distributed File System |

## Supported Formats

| Format | Value | Notes |
|--------|-------|-------|
| Apache Parquet | `parquet` | Default. Columnar, highly recommended for large datasets |
| CSV | `csv` | Header auto-detected, schema inferred |
| JSON | `json` | One JSON object per line (newline-delimited) |
| ORC | `orc` | Columnar, common in Hive ecosystems |
| Avro | `avro` | Row-based, good for streaming pipelines |
| Apache Iceberg | `iceberg` | Table format with catalog support (Glue, Nessie, REST, Hadoop) |
| Delta Lake | `delta` | Table format with catalog support (HMS, Unity Catalog) |

## Connection Configuration

```yaml
connections:
  source:
    variant: file
    extra:
      storage: local        # local | s3 | gcs | adls | hdfs
      format: parquet       # parquet | csv | json | orc | avro | iceberg | delta
      path: /data/warehouse # base path — table name appended: <path>/<table_name>
```

### Connection Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `variant` | str | **required** | Must be `file` |
| `extra.storage` | str | `local` | Storage backend: `local` \| `s3` \| `gcs` \| `adls` \| `hdfs` |
| `extra.format` | str | `parquet` | File format: `parquet` \| `csv` \| `json` \| `orc` \| `avro` \| `iceberg` \| `delta` |
| `extra.path` | str | `""` | Base path. Table name appended: `<path>/<table_name>` |
| `extra.catalog` | str | `null` | Catalog type — **Iceberg**: `glue` \| `nessie` \| `rest` \| `hadoop` · **Delta**: `hms` \| `unity` |
| `extra.catalog_name` | str | `default` | Spark catalog identifier — table referenced as `<catalog_name>.<catalog_database>.<table>` |
| `extra.catalog_database` | str | `default` | Database/namespace within the catalog (e.g. Glue database name). Table path on S3: `<warehouse>/<catalog_database>.db/<table>/` |
| `extra.catalog_uri` | str | `null` | Catalog endpoint URI (Nessie: `http://...`, REST: `https://...`, HMS: `thrift://...`) |
| `extra.catalog_warehouse` | str | `null` | Warehouse root path (S3/GCS/HDFS URI or local path) |
| `extra.nessie_ref` | str | `main` | Nessie branch or tag to read from |
| `extra.nessie_auth_type` | str | `NONE` | Nessie auth type: `NONE` \| `BEARER` |
| `extra.nessie_token` | str | `null` | Nessie bearer token |
| `extra.rest_credential` | str | `null` | REST catalog OAuth2 credential `client_id:client_secret` |
| `extra.rest_scope` | str | `null` | REST catalog OAuth2 scope (e.g. `PRINCIPAL_ROLE:my_role`) |
| `extra.unity_token` | str | `null` | Databricks personal access token for Unity Catalog |
| `bucket_name` | str | `null` | S3 bucket name (used when `path` is not set) |
| `s3_prefix` | str | `null` | S3 key prefix inside the bucket |
| `aws_access_key` | str | `null` | AWS access key ID |
| `aws_secret_key` | str | `null` | AWS secret access key |
| `region` | str | `null` | AWS region (S3 and Glue) |
| `credentials_file` | str | `null` | GCS service account JSON key file path |

### Path Resolution

For non-catalog formats (`parquet`, `csv`, `json`, `orc`, `avro`, `delta` without catalog), the extractor resolves paths in this order:

1. `extra.path` is set → `<path>/<table_name>`
2. `storage=s3` and `bucket_name` is set → `s3a://<bucket_name>/<s3_prefix>/<table_name>`
3. Otherwise → `<table_name>` as-is

For catalog-based formats (`iceberg` with `catalog`), the path is ignored — the table is referenced as `<catalog_name>.<catalog_database>.<table_name>`. For `delta` with catalog, it is `<catalog_name>.<table_name>`.

## Table Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | str | **required** | Source table/path name. For catalogs: `<db>.<table>` |
| `target_name` | str | **required** | Name of the output dataset in the destination |
| `replication_method` | str | `full` | `full` or `incremental` |
| `iterate_column` | str | `null` | Column used for incremental filtering (`>=` comparison) |
| `iterate_column_type` | str | `null` | Type of `iterate_column`: `datetime` \| `int` |
| `dedup_columns` | list | `null` | Columns used to generate `mkpipe_id` (xxhash64) for deduplication |
| `tags` | list | `[]` | Tags for selective pipeline execution (`mkpipe run --tags ...`) |

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
    destination: my_target
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
      storage: s3
      format: iceberg
      catalog: glue
      catalog_name: my_glue       # Spark catalog identifier
      catalog_database: my_database   # Glue database name (default: "default")
      catalog_warehouse: s3a://my-bucket/warehouse
    aws_access_key: ${AWS_ACCESS_KEY_ID}
    aws_secret_key: ${AWS_SECRET_ACCESS_KEY}
    region: eu-central-1

settings:
  spark:
    extra_config:
      spark.sql.extensions: "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

pipelines:
  - name: glue_pipeline
    source: source
    destination: pg_target
    tables:
      - name: my_table
        target_name: stg_my_table
```

The table will be read from Glue as `my_glue.my_database.my_table`.

> **Important:** `spark.sql.extensions` must be set in `settings.spark.extra_config` — it is a static Spark config and cannot be modified after SparkSession creation.

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

Reads the entire dataset each run. Write mode passed to loader is `overwrite`.

```yaml
tables:
  - name: products
    target_name: stg_products
    replication_method: full
```

### Incremental

Reads only new/changed rows based on `iterate_column >= last_point`. The last extracted value is persisted in the backend between runs.

```yaml
tables:
  - name: events
    target_name: stg_events
    replication_method: incremental
    iterate_column: updated_at
    iterate_column_type: datetime   # datetime | int
    dedup_columns: [event_id]       # used for mkpipe_id generation
```

| Run | Behavior | Write mode |
|-----|----------|------------|
| First run | No `last_point` yet — reads all data | `overwrite` |
| Subsequent runs | Filters `iterate_column >= last_point` | `append` |

## Format-Specific Behaviour

### CSV
Automatically applies:
- `header: true` — first row treated as column names
- `inferSchema: true` — column types auto-detected

### Iceberg
- Requires `catalog` to be configured (see catalog examples above)
- Table referenced as `<catalog_name>.<catalog_database>.<table>` — `catalog_database` is set in connection config
- Requires `spark.sql.extensions` to be set at SparkSession creation time (see Glue example above)

### Delta
- Without `catalog`: path-based read via `spark.read.format('delta').load(path)`
- With `catalog: hms` or `catalog: unity`: table referenced as `<catalog_name>.<table>`

## Notes

- Environment variables: use `${VAR_NAME}` syntax in YAML
- For S3-compatible storage (MinIO, Ceph, etc.), configure the endpoint in `settings.spark.extra_config`:
  ```yaml
  settings:
    spark:
      extra_config:
        spark.hadoop.fs.s3a.endpoint: http://minio:9000
        spark.hadoop.fs.s3a.path.style.access: "true"
  ```
- **Iceberg requires `spark.sql.extensions`** to be set at SparkSession creation time. Add this to your `settings.spark.extra_config`:
  ```yaml
  settings:
    spark:
      extra_config:
        spark.sql.extensions: "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  ```
  This is a static Spark config and **cannot** be set after the session is created.
- **Delta Lake also requires `spark.sql.extensions`** for catalog-based usage. Add to `settings.spark.extra_config`:
  ```yaml
  settings:
    spark:
      extra_config:
        spark.sql.extensions: "io.delta.sql.DeltaSparkSessionExtension"
  ```
- `iterate_column` supports SQL expressions: e.g. `greatest(cdate, udate)`
