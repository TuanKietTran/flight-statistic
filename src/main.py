# main.py - Example of bringing all pieces together
from src.lake.s3 import create_bucket, upload_file
from spark import create_spark_session, transform_data
from warehouse.yugabyte import create_table, insert_data

if __name__ == "__main__":
    # MinIO - Create bucket and upload data
    bucket_name = "flight-data-lake"
    create_bucket(bucket_name)
    upload_file(bucket_name, "sample_flight_data.json", "./data/sample_flight_data.json")

    # Spark - Process Data
    spark = create_spark_session()
    transformed_df = transform_data(spark, f"s3a://{bucket_name}/sample_flight_data.json")

    # Save transformed data to YugabyteDB
    create_table()
    for row in transformed_df.collect():
        insert_data(row['flightNumber'], row['departure'], row['arrival'], row['status'])