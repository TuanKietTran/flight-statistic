# spark.py - PySpark implementation
from pyspark.sql import SparkSession
from .config import get_config

config = get_config()

def create_spark_session() -> SparkSession:
    spark = SparkSession.builder \
        .appName(config['SPARK_APP_NAME']) \
        .config("spark.hadoop.fs.s3a.endpoint", config['MINIO_ENDPOINT']) \
        .config("spark.hadoop.fs.s3a.access.key", config['MINIO_ACCESS_KEY']) \
        .config("spark.hadoop.fs.s3a.secret.key", config['MINIO_SECRET_KEY']) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .getOrCreate()
    return spark
