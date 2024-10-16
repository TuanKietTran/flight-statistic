# main.py - Example of bringing all pieces together
from pandas import DataFrame
from engine.s3 import create_bucket, upload_file
from engine.spark import create_spark_session
from engine.yugabyte import create_table, insert_data

with create_spark_session() as spark:
    # MinIO - Create bucket and upload data
    def create_bucket_and_upload_data(bucket_name: str):
        create_bucket(bucket_name)
        create_table()
        upload_file(bucket_name, "sample_flight_data.csv", "./data/sample_flight_data.csv")

    def transform_data(input_path: str):
        df = spark.read.csv(input_path, header=True, inferSchema=True)
        # A simple transformation: filtering columns to match new sheet structure
        transformed_df = df.select("flight", "year", "month", "day", "dep_time", "arr_time", "origin", "dest", "air_time", "distance", "name")
        return transformed_df

    def save_data(transformed_df: DataFrame):
        # Save transformed data to YugabyteDB
        for row in transformed_df.collect():
            insert_data(row['flight'], row['year'], row['month'], row['day'], row['dep_time'], row['arr_time'], row['origin'], row['dest'], row['air_time'], row['distance'], row['name'])

    bucket_name = "flight-data-lake"
    create_bucket_and_upload_data(bucket_name)
    transformed_df = transform_data(f"s3a://{bucket_name}/sample_flight_data.csv")
    save_data(transformed_df)