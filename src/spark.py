# spark.py - PySpark implementation
from pyspark.sql import SparkSession
from config import get_config

config = get_config()

def create_spark_session():
    spark = SparkSession.builder \
        .appName(config['SPARK_APP_NAME']) \
        .getOrCreate()
    return spark

# Example transformation function

def transform_data(spark, input_path):
    df = spark.read.json(input_path)
    # A simple transformation: filtering columns
    transformed_df = df.select("flightNumber", "departure", "arrival", "status")
    return transformed_df