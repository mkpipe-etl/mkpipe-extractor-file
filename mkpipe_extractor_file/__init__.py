from typing import Optional

from mkpipe.exceptions import ConfigError
from mkpipe.spark.base import BaseExtractor
from mkpipe.models import ConnectionConfig, ExtractResult, TableConfig
from mkpipe.utils import get_logger

JAR_PACKAGES = [
    'org.apache.hadoop:hadoop-aws:3.3.4',
    'com.amazonaws:aws-java-sdk-bundle:1.12.262',
    'com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22',
    'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1',
    'io.delta:delta-spark_2.12:3.2.1',
]

logger = get_logger(__name__)

SUPPORTED_FORMATS = ('parquet', 'csv', 'json', 'orc', 'avro', 'iceberg', 'delta')


class FileExtractor(BaseExtractor, variant='file'):
    def __init__(self, connection: ConnectionConfig):
        self.connection = connection
        self.storage = connection.extra.get('storage', 'local')
        self.format = connection.extra.get('format', 'parquet')
        self.base_path = connection.extra.get('path', '')
        self.bucket_name = connection.bucket_name
        self.aws_access_key = connection.aws_access_key
        self.aws_secret_key = connection.aws_secret_key
        self.region = connection.region
        self.credentials_file = connection.credentials_file

        if self.format not in SUPPORTED_FORMATS:
            raise ConfigError(
                f"Unsupported format: '{self.format}'. "
                f"Supported: {SUPPORTED_FORMATS}"
            )

    def _resolve_path(self, table_name: str) -> str:
        if self.base_path:
            return f'{self.base_path.rstrip("/")}/{table_name}'
        if self.storage == 's3' and self.bucket_name:
            prefix = self.connection.s3_prefix or ''
            return f's3a://{self.bucket_name}/{prefix.strip("/")}/{table_name}'
        return table_name

    def _configure_storage(self, spark):
        hadoop = spark.sparkContext._jsc.hadoopConfiguration()
        if self.storage == 's3':
            if self.aws_access_key:
                hadoop.set('fs.s3a.access.key', self.aws_access_key)
                hadoop.set('fs.s3a.secret.key', self.aws_secret_key or '')
            if self.region:
                hadoop.set('fs.s3a.endpoint.region', self.region)
        elif self.storage == 'gcs':
            if self.credentials_file:
                hadoop.set('google.cloud.auth.service.account.json.keyfile', self.credentials_file)

    def extract(self, table: TableConfig, spark, last_point: Optional[str] = None) -> ExtractResult:
        logger.info({
            'table': table.target_name,
            'status': 'extracting',
            'storage': self.storage,
            'format': self.format,
        })

        self._configure_storage(spark)
        path = self._resolve_path(table.name)

        if self.format == 'iceberg':
            catalog_name = self.connection.extra.get('catalog_name', 'default')
            df = spark.read.format('iceberg').load(f'{catalog_name}.{table.name}')
        elif self.format == 'delta':
            df = spark.read.format('delta').load(path)
        else:
            reader = spark.read.format(self.format)
            if self.format == 'csv':
                reader = reader.option('header', 'true').option('inferSchema', 'true')
            df = reader.load(path)

        write_mode = 'overwrite'
        last_point_value = None

        if table.replication_method.value == 'incremental' and table.iterate_column:
            if last_point:
                from pyspark.sql import functions as F
                df = df.filter(F.col(table.iterate_column) >= last_point)
                write_mode = 'append'
            row = df.agg({table.iterate_column: 'max'}).first()
            if row and row[0] is not None:
                last_point_value = str(row[0])

        logger.info({
            'table': table.target_name,
            'status': 'extracted',
            'write_mode': write_mode,
        })

        return ExtractResult(df=df, write_mode=write_mode, last_point_value=last_point_value)
