import os
import boto3
from botocore.exceptions import ClientError
from config import *
import json
from helpers import retry


@retry(max_attempts=3, delay=5)
def initialize_bucket(bucket_name, folders=None):
    """Create a bucket if it doesn't exist and create specified subfolders."""
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1"
    )
    
    # Create bucket
    try:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' created.")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'BucketAlreadyOwnedByYou':
            print(f"Bucket '{bucket_name}' already exists.")
        else:
            raise

    # Create folders inside the bucket
    if folders:
        for folder in folders:
            s3.put_object(Bucket=bucket_name, Key=f"{folder}/")  # trailing slash to indicate folder
            print(f"Folder '{folder}/' created in bucket '{bucket_name}'.")         

@retry(max_attempts=3, delay=5)
def move_files_to_folder(file_paths, dest_folder):
    """Move specific files to a folder (processed or quarantine) in MinIO."""

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1"
    )

    for path in file_paths:
        # Extract filename from full S3 path
        filename = path.split("/")[-1]
        source_key = f"data/{filename}"
        dest_key = f"{dest_folder}/{filename}"

        try:
            copy_source = {"Bucket": BUCKET_NAME, "Key": source_key}
            s3.copy_object(Bucket=BUCKET_NAME, Key=dest_key, CopySource=copy_source)
            s3.delete_object(Bucket=BUCKET_NAME, Key=source_key)
            print(f"Moved {source_key} -> {dest_key}")
        except Exception as e:
            print(f"Failed to move {source_key} -> {dest_key}: {e}")


def read_schema(file_name):
    """
    Read a JSON schema from S3/MinIO and return a Python dict ready for StructType.fromJson().
    Ensures 'metadata' exists for all fields.
    """
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1"
    )
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=f"schema/{file_name}" )
        schema_str = obj['Body'].read().decode('utf-8')
        schema_json = json.loads(schema_str)

        # Ensure 'metadata' exists for all fields
        for field in schema_json.get("fields", []):
            if "metadata" not in field:
                field["metadata"] = {}

        return schema_json

    except s3.exceptions.NoSuchKey:
        print(f"JSON '{file_name}' not found in bucket '{BUCKET_SCHEMA}'. Schema will be inferred.")
        return None


def derive_schema_filename(file_path: str) -> str:
    """
    Example: 
    "/data/farm_data.csv" --> "farm_data_schema.json"
    """
    schema_file = os.path.basename(file_path).split(".")[0]
    return f"{schema_file}.json"