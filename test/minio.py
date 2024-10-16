import boto3
from botocore.exceptions import NoCredentialsError

def test_minio_connection():
    try:
        s3 = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',  # Update to match your MINIO_ENDPOINT
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin'
        )
        response = s3.list_buckets()
        print("Connected to MinIO successfully. Buckets:", response.get('Buckets', []))
    except NoCredentialsError:
        print("Credentials not available.")

test_minio_connection()
