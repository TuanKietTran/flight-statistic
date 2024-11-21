import pandas as pd
import logging
from engine.s3 import upload_data_frame
from aeroapi_python.AeroAPI import AeroAPI
from engine.spark import (
    create_spark_session,
    load_data_from_s3,
    process_data,
    save_processed_data,
)


# Initialize the AeroAPI instance with your API key
api_key = "VvPdTvkdpXKylGXc0QIrgNGXm2mKa3hd"
aero_api = AeroAPI(api_key)

# Set up logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def fetch_data(api_func):
    # Fetch data from the API and return it as a DataFrame
    data = api_func()
    df = pd.json_normalize(data)
    return df


def fetch_and_upload_data(bucket_name: str):
    logger.info("Starting data fetch and upload process.")
    data = fetch_data(lambda: aero_api.airports.get_airports())

    # Upload the DataFrame to S3
    upload_data_frame(bucket_name, "airports.parquet", data, format="parquet")
    logger.info("Data fetch and upload process completed.")


def main():
    bucket_name = "newbucket"
    file_name = "airports.parquet"

    # Step 1: Fetch data from FlightAware and upload to S3
    fetch_and_upload_data(bucket_name)

    # Step 2: Load the data into Spark from S3
    spark = create_spark_session()
    df_spark = load_data_from_s3(bucket_name, file_name)

    # Step 3: Process the data in Spark
    df_processed = process_data(df_spark)

    # Step 4: Save the processed data back to S3
    save_processed_data(df_processed, bucket_name, "processed_airports.parquet")


if __name__ == "__main__":
    main()
