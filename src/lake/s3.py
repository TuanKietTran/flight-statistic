# Create and manage buckets using boto3 to interact with MinIO (S3-compatible storage)

# s3.py
import boto3
from config import get_config

config = get_config()

s3_client = boto3.client(
    's3',
    endpoint_url=config['MINIO_ENDPOINT'],
    aws_access_key_id=config['MINIO_ACCESS_KEY'],
    aws_secret_access_key=config['MINIO_SECRET_KEY'],
)

def create_bucket(bucket_name):
    s3_client.create_bucket(Bucket=bucket_name)
    print(f"Bucket '{bucket_name}' created successfully.")

def upload_file(bucket_name, file_name, file_path):
    s3_client.upload_file(file_path, bucket_name, file_name)
    print(f"File '{file_name}' uploaded to bucket '{bucket_name}' successfully.")