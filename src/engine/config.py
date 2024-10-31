# config.py - configuration settings for the entire project
import os

def get_config():
    return {
        "MINIO_ENDPOINT": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        "MINIO_ACCESS_KEY": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        "MINIO_SECRET_KEY": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        "YUGABYTE_HOST": os.getenv("YUGABYTE_HOST", "localhost"),
        "YUGABYTE_PORT": os.getenv("YUGABYTE_PORT", "5433"),
        "YUGABYTE_USER": os.getenv("YUGABYTE_USER", "yugabyte"),
        "YUGABYTE_PASSWORD": os.getenv("YUGABYTE_PASSWORD", "yugabyte"),
        "SPARK_APP_NAME": os.getenv("SPARK_APP_NAME", "Flight Data Processing"),
    }