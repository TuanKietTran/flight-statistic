# main.py - Example of bringing all pieces together
from pandas import DataFrame, read_csv
from engine.s3 import create_bucket, upload_data_frame
from engine.spark import create_spark_session
from engine.yugabyte import *

FILENAME = 'Airline_Delay_Cause.csv'

with create_spark_session() as spark:
    # MinIO - Create bucket and upload data
    def create_bucket_and_upload_data(bucket_name: str):
        create_bucket(bucket_name)
        upload_data_frame(bucket_name, FILENAME, read_csv(f"./data/{FILENAME}"), "csv")

    def transform_data(input_path: str):
        df = spark.read.csv(input_path, header=True, inferSchema=True)
        # A simple transformation: filtering columns to match new sheet structure
        transformed_df = df.select( "year",
                                    "month",
                                    "carrier",
                                    "carrier_name",
                                    "airport",
                                    "airport_name",
                                    "arr_flights",
                                    "arr_del15",
                                    "carrier_ct",
                                    "weather_ct",
                                    "nas_ct",
                                    "security_ct",
                                    "late_aircraft_ct",
                                    "arr_cancelled",
                                    "arr_diverted",
                                    "arr_delay",
                                    "carrier_delay",
                                    "weather_delay",
                                    "nas_delay",
                                    "security_delay",
                                    "late_aircraft_delay")
        return transformed_df.dropna()

    def save_data(transformed_df: DataFrame):
        create_star_schema()
        
        # Save transformed data to YugabyteDB
        carrier_dim_data, airport_dim_data, time_dim_data, ot_delay_fact_data = extract_tables(transformed_df)
        create_staging_table()
        # Loading to staging table
        insert_batch_data(df2filestream(carrier_dim_data), "carrier_staging")
        insert_batch_data(df2filestream(airport_dim_data), "airport_staging")
        insert_batch_data(df2filestream(time_dim_data), "time_staging")
        insert_batch_data(df2filestream(ot_delay_fact_data), "ot_delay_staging")
        
        # Update and insert to main table
        upsert()
        drop_staging_table()

    bucket_name = "flight-data-lake"
    create_bucket_and_upload_data(bucket_name)
    transformed_df = transform_data(f"s3a://{bucket_name}/{FILENAME}")
    save_data(transformed_df)
