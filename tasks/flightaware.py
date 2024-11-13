import pandas as pd
import logging
from engine.s3 import upload_data_frame
from aeroapi_python.AeroAPI import AeroAPI
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Initialize the AeroAPI instance with your API key
api_key = 'VvPdTvkdpXKylGXc0QIrgNGXm2mKa3hd'
aero_api = AeroAPI(api_key)

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG to see detailed output
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def fetch_and_upload_data(bucket_name: str):
    # Fetch data from AeroAPI and upload it to S3
    def process_and_upload(api_func, file_prefix, format="parquet"):
        logger.info(f"Fetching data for {file_prefix}...")
        data = api_func()  # Fetch data from the API

        # Log the fetched data to inspect its structure
        logger.debug(f"Fetched data: {data}")

        # Check data format and normalize if necessary
        if isinstance(data, dict):
            data = [data]  # Convert single dictionary to list for uniform processing
            df = pd.json_normalize(data)
        elif isinstance(data, list) and all(isinstance(i, dict) for i in data):
            df = pd.json_normalize(data, sep='_')  # Flatten nested structures if necessary
        else:
            logger.error(f"Unexpected data format: {type(data)}. Unable to convert to DataFrame.")
            return

        # Ensure consistent data types for all columns
        for column in df.columns:
            if df[column].dtype == 'object':
                df[column] = pd.to_numeric(df[column], errors='coerce')
                df[column] = df[column].fillna(-1).astype('int64') if df[column].isna().any() else df[column]

        logger.debug(f"DataFrame columns and types after conversion: {df.dtypes}")

        file_name = f"{file_prefix}.{format}"
        logger.info(f"Uploading {file_name} to bucket '{bucket_name}' in {format} format...")
        upload_data_frame(bucket_name, file_name, df, format=format)
        logger.info(f"{file_name} uploaded to bucket '{bucket_name}' successfully.")

    # Fetch and upload airports data
    logger.info("Starting data fetch and upload process.")
    process_and_upload(lambda: aero_api.airports.get_airports(), "airports", format="parquet")
    logger.info("Data fetch and upload process completed.")

if __name__ == "__main__":
    logger.info("Script started.")
    fetch_and_upload_data("test_bucket")
    logger.info("Script completed.")
