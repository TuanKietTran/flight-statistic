# Create and manage buckets using boto3 to interact with MinIO (S3-compatible storage)

# s3.py
import boto3
from .config import get_config
from io import BytesIO

config = get_config()

s3_client = boto3.client(
    's3',
    endpoint_url=config['MINIO_ENDPOINT'],
    aws_access_key_id=config['MINIO_ACCESS_KEY'],
    aws_secret_access_key=config['MINIO_SECRET_KEY'],
)

def create_bucket(bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except s3_client.exceptions.NoSuchBucket:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' created successfully.")

def upload_file(bucket_name, file_name, file_path):
    try:
        s3_client.head_object(Bucket=bucket_name, Key=file_name)
        print(f"File '{file_name}' already exists in bucket '{bucket_name}'.")
    except s3_client.exceptions.ClientError:
        s3_client.upload_file(file_path, bucket_name, file_name)
        print(f"File '{file_name}' uploaded to bucket '{bucket_name}' successfully.")

def upload_data_frame(bucket_name, file_name, data_frame, format="csv"):
    # Create the bucket if it does not exist
    create_bucket(bucket_name)

    # Convert DataFrame to bytes in the desired format
    buffer = BytesIO()
    if format == "csv":
        data_frame.to_csv(buffer, index=False)
    elif format == "parquet":
        data_frame.to_parquet(buffer, index=False)
    else:
        raise ValueError("Unsupported format: Choose 'csv' or 'parquet'")

    buffer.seek(0)  # Move to the start of the BytesIO buffer

    # Upload the file to MinIO
    s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())
    print(f"DataFrame uploaded as '{file_name}' to bucket '{bucket_name}' successfully.")
