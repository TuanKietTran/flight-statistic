# data_ingestion.py - Read data from S3 and load into YugabyteDB

import logging
from pyspark.sql.functions import col
from engine.spark import create_spark_session, load_data_from_s3
from engine.yugabyte import (
    create_table,
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

def read_airports_from_s3(bucket_name: str, file_name: str) -> DataFrame:
    logger.info(f"Reading airports data from S3 bucket '{bucket_name}'...")
    df_spark = load_data_from_s3(bucket_name, file_name)
    if df_spark is not None:
        return df_spark
    else:
        logger.warning("No airports data found in S3.")
        return None

def read_airlines_from_s3(bucket_name: str, file_name: str) -> DataFrame:
    logger.info(f"Reading airlines data from S3 bucket '{bucket_name}'...")
    df_spark = load_data_from_s3(bucket_name, file_name)
    if df_spark is not None:
        return df_spark
    else:
        logger.warning("No airlines data found in S3.")
        return None

def prepare_and_load_data(airports_df: DataFrame, airlines_df: DataFrame):
    logger.info("Preparing data for loading into YugabyteDB...")
    # Create tables in YugabyteDB
    create_table()
    # Create staging tables
    create_staging_table()
    # Insert data into staging tables
    logger.info("Inserting data into staging tables...")
    if airports_df is not None:
        # Normalize data types and convert all values to strings
        airports_df = airports_df.select([col(c).cast("string") for c in airports_df.columns])
        # Insert data into airports_staging
        logger.info("Inserting data into 'airports_staging'...")
        insert_batch_data(df2filestream(airports_df), "airports_staging")
        logger.info("Airports data inserted into 'airports_staging'.")
    if airlines_df is not None:
        # Normalize data types and convert all values to strings
        airlines_df = airlines_df.select([col(c).cast("string") for c in airlines_df.columns])
        # Insert data into airlines_staging
        logger.info("Inserting data into 'airlines_staging'...")
        insert_batch_data(df2filestream(airlines_df), "airlines_staging")
        logger.info("Airlines data inserted into 'airlines_staging'.")
    # Upsert data from staging tables into main tables
    upsert()
    # Drop staging tables
    drop_staging_table()
    logger.info("Data loading process completed.")

def main():
    spark = create_spark_session()
    bucket_name = "flight-data-lake"  # Replace with your actual S3 bucket name
    airports_file = "flightrada24_airports.parquet"
    airlines_file = "flightrada24_airlines.parquet"

    airports_df = read_airports_from_s3(bucket_name, airports_file)
    airlines_df = read_airlines_from_s3(bucket_name, airlines_file)

    if airports_df is not None or airlines_df is not None:
        prepare_and_load_data(airports_df, airlines_df)
    else:
        logger.warning("Data fetching from S3 failed. Exiting.")

if __name__ == "__main__":
    logger.info("Script started.")
    main()
    logger.info("Script completed.")
