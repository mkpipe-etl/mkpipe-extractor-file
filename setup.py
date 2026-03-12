from setuptools import setup, find_packages

setup(
    name='mkpipe-extractor-file',
    version='0.2.3',
    license='Apache License 2.0',
    packages=find_packages(),
    install_requires=['mkpipe'],
    include_package_data=True,
    entry_points={
        'mkpipe.extractors': [
            'file = mkpipe_extractor_file:FileExtractor',
        ],
    },
    description='File-based extractor for mkpipe (S3, ADLS, GCS, HDFS, local). Supports parquet, csv, json, orc, avro, iceberg, delta.',
    author='Metin Karakus',
    author_email='metin_karakus@yahoo.com',
    python_requires='>=3.9',
)
