# data_ingestion.py - Read data from S3 and load into YugabyteDB

import logging
from pyspark.sql.functions import col
from engine.spark import create_spark_session, load_data_from_s3
from engine.yugabyte import (
    create_star_schema,
    create_staging_table,
    drop_staging_table,
    insert_batch_data,
    df2filestream,
    upsert
)
from pyspark.sql import DataFrame

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def read_airports_from_s3(spark, bucket_name, file_name) -> DataFrame:
    logger.info(f"Reading airports data from S3 bucket '{bucket_name}'...")
    airports_df = load_data_from_s3(spark, bucket_name, file_name)
    if airports_df is not None:
        return airports_df
    else:
        logger.warning("No airports data found in S3.")
        return None

def read_airlines_from_s3(spark, bucket_name, file_name) -> DataFrame:
    logger.info(f"Reading airlines data from S3 bucket '{bucket_name}'...")
    airlines_df = load_data_from_s3(spark, bucket_name, file_name)
    if airlines_df is not None:
        return airlines_df
    else:
        logger.warning("No airlines data found in S3.")
        return None

def prepare_and_load_data(airports_df: DataFrame, airlines_df: DataFrame):
    logger.info("Preparing data for loading into YugabyteDB...")
    # Create star schema in YugabyteDB
    create_star_schema()
    # Create staging tables
    create_staging_table()
    # Insert data into staging tables
    logger.info("Inserting data into staging tables...")
    if airports_df is not None:
        # Select relevant columns and rename to match the schema
        airport_dim_data = airports_df.select(
            col("iata").alias("airport_code"),
            col("name").alias("airport_name")
        ).filter(col("airport_code").isNotNull()).distinct()
        insert_batch_data(df2filestream(airport_dim_data), "airport_staging")
        logger.info("Airports data inserted into 'airport_staging'.")
    if airlines_df is not None:
        carrier_dim_data = airlines_df.select(
            col("icao").alias("carrier_code"),
            col("name").alias("carrier_name")
        ).filter(col("carrier_code").isNotNull()).distinct()
        insert_batch_data(df2filestream(carrier_dim_data), "carrier_staging")
        logger.info("Airlines data inserted into 'carrier_staging'.")
    # Upsert data from staging tables into main tables
    upsert()
    # Drop staging tables
    drop_staging_table()
    logger.info("Data loading process completed.")

def main():
    spark = create_spark_session()
    bucket_name = "your-s3-bucket-name"  # Replace with your actual S3 bucket name
    airports_file = "airports.parquet"
    airlines_file = "airlines.parquet"

    airports_df = read_airports_from_s3(spark, bucket_name, airports_file)
    airlines_df = read_airlines_from_s3(spark, bucket_name, airlines_file)

    if airports_df is not None or airlines_df is not None:
        prepare_and_load_data(airports_df, airlines_df)
    else:
        logger.warning("Data fetching from S3 failed. Exiting.")

if __name__ == "__main__":
    logger.info("Script started.")
    main()
    logger.info("Script completed.")
