from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import flightradar24
import logging
from engine.spark import create_spark_session, save_processed_data

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Initialize the Flightradar24 API
fr_api = flightradar24.Api()

# Initialize Spark session
spark = create_spark_session()

def fetch_and_upload_data(bucket_name: str):
    # Function to fetch data and directly upload it
    def process_and_upload(api_func, file_prefix, format="parquet"):
        logger.info(f"Fetching data for {file_prefix}...")
        data = api_func()

        if data:
            logger.info(f"Data for {file_prefix} fetched successfully. Processing data...")
            rows_data = data.get('rows', [])

            if rows_data:
                # Normalize column types
                for row in rows_data:
                    for key, value in row.items():
                        row[key] = str(value)

                # Create Spark DataFrame from normalized rows
                schema = StructType([
                    StructField(key, DoubleType() if isinstance(row[key], (int, float)) else StringType(), True)
                    for key in rows_data[0].keys()
                ])
                
                logger.info("Creating Spark DataFrame...")
                df = spark.createDataFrame(rows_data, schema)

                # Upload to S3
                file_name = f"{file_prefix}.{format}"
                logger.info(f"Uploading {file_name} to bucket '{bucket_name}' in {format} format...")
                save_processed_data(df, bucket_name, file_name)
                logger.info(f"{file_name} uploaded to bucket '{bucket_name}' successfully.")
        else:
            logger.warning(f"No data returned for {file_prefix}. Skipping upload.")

    # Fetch and upload airports and airlines data
    logger.info("Starting data fetch and upload process.")
    process_and_upload(fr_api.get_airports, "flightrada24_airports",)
    process_and_upload(fr_api.get_airlines, "flightrada24_airlines")
    logger.info("Data fetch and upload process completed.")

if __name__ == "__main__":
    bucket_name = "flight-data-lake"
    logger.info("Script started.")
    fetch_and_upload_data(bucket_name)
    logger.info("Script completed.")
