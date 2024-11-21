# spark.py - PySpark implementation
from pyspark.sql import SparkSession
from .config import get_config

config = get_config()


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName(config["SPARK_APP_NAME"])
        .config("spark.hadoop.fs.s3a.endpoint", config["MINIO_ENDPOINT"])
        .config("spark.hadoop.fs.s3a.access.key", config["MINIO_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.secret.key", config["MINIO_SECRET_KEY"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        .getOrCreate()
    )
    return spark


def load_data_from_s3(bucket_name: str, file_name: str):
    spark = create_spark_session()
    s3_path = f"s3a://{bucket_name}/{file_name}"
    df_spark = spark.read.parquet(s3_path)
    df_spark.show()  # Display data for verification
    return df_spark


def process_data(df_spark):
    # Example processing: replace 'column_name' and 'desired_value' with actual column name and condition
    df_processed = df_spark.filter(df_spark["column_name"] == "desired_value")
    return df_processed


def save_processed_data(df_spark, bucket_name: str, file_name: str):
    s3_output_path = f"s3a://{bucket_name}/{file_name}"
    df_spark.write.mode("overwrite").parquet(s3_output_path)
