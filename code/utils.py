import os
import boto3
import json
import hashlib
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame
from botocore.exceptions import ClientError
from config import *
from helpers import *
from logger import * 

@retry(max_attempts=3, delay=5)
def initialize_bucket(bucket_name, folders=None):
    """
    Create a bucket in MinIO if it doesn't exist, and create specified subfolders.

    Args:
        bucket_name (str): Name of the bucket to create.
        folders (list[str] | None): Optional list of folder names to create inside the bucket.
    """
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
        logger.info(f"Bucket '{bucket_name}' created.")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'BucketAlreadyOwnedByYou':
            logger.info(f"Bucket '{bucket_name}' already exists.")
        else:
            raise

    # Create folders inside the bucket
    if folders:
        for folder in folders:
            s3.put_object(Bucket=bucket_name, Key=f"{folder}/")  # trailing slash to indicate folder
            logger.info(f"Folder '{folder}/' created in bucket '{bucket_name}'.")         

@retry(max_attempts=3, delay=5)
def move_files_to_folder(file_paths, dest_folder):
    """
    Move specific files to a folder (e.g., processed or quarantine) in MinIO.

    Args:
        file_paths (list[str]): List of file paths to move.
        dest_folder (str): Destination folder inside the bucket.
    """

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
            logger.info(f"Moved {source_key} -> {dest_key}")
        except Exception as e:
            logger.info(f"Failed to move {source_key} -> {dest_key}: {e}")


def read_schema(file_name):
    """
    Read a JSON schema from S3/MinIO and return a dict ready for StructType.fromJson().
    Ensures 'metadata' exists for all fields.

    Args:
        file_name (str): Schema file name to read.

    Returns:
        dict | None: Parsed schema JSON or None if not found.
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
        logger.info(f"JSON '{file_name}' not found in bucket '{BUCKET_SCHEMA}'. Schema will be inferred.")
        return None


def derive_schema_filename(file_path: str) -> str:
    """
    Derive schema filename from input file path.

    Example:
        "/data/farm_data.csv" → "farm_data.json"

    Args:
        file_path (str): Input file path.

    Returns:
        str: Corresponding schema filename.
    """
    schema_file = os.path.basename(file_path).split(".")[0]
    return f"{schema_file}.json"

# ----------------------------
# File Readers
# ----------------------------

def read_batch_files(paths: list, fmt: str, spark: SparkSession, schema: StructType = None) -> DataFrame:
    """
    Read batch files (CSV or JSON) into Spark DataFrame.
    
    Args:
        paths (list): List of file paths.
        fmt (str): File format ("csv" or "json").
        spark (SparkSession): Spark session.
        schema (StructType, optional): Predefined schema.

    Returns:
        DataFrame: Parsed data.
    """
    logger.info(f"Reading {fmt.upper()} files: {paths} with schema: {'provided' if schema else 'inferred'}")
    if fmt.lower() == "csv":
        reader = spark.read.option("header", "true")
        if schema:
            reader = reader.schema(schema)
        else:
            reader = reader.option("inferSchema", "true")
        return reader.csv(paths)
    elif fmt.lower() == "json":
        reader = spark.read
        if schema:
            reader = reader.schema(schema)
        else:
            reader = reader.option("inferSchema", "true")
        return reader.json(paths)
    else:
        raise ValueError(f"Unsupported format: {fmt}")

# ----------------------------
# Audit Logging
# ----------------------------

@retry(max_attempts=3, delay=5)
def write_audit(spark, audit_dir, file_name, fmt, total, good, bad, status, message):
    """
    Write audit log entry (JSON) for ingestion process.
    
    Args:
        spark (SparkSession): Spark session.
        audit_dir (str): Directory to store audits.
        file_name (str): Source file.
        fmt (str): File format.
        total (int): Total rows processed.
        good (int): Rows passed validation.
        bad (int): Rows failed validation.
        status (str): Process status.
        message (str): Additional info or error message.
    """
    logger.info(f"Writing audit for file: {file_name} | Status: {status}")
    now = datetime.utcnow()
    ts_str = now.strftime("%Y-%m-%d %H:%M:%S")
    audit_date = now.strftime("%Y-%m-%d")
    data = [(ts_str, file_name, fmt, total, good, bad, status, message)]
    columns = ["ts","file_name","format","total_rows","good_rows","bad_rows","status","message"]
    df = spark.createDataFrame(data, schema=columns) \
              .withColumn("audit_date", lit(audit_date))
    audit_path = f"{audit_dir}/audit_date={audit_date}"
    df.coalesce(1).write.mode("append").json(audit_path)
    logger.info(f"Audit written to {audit_path}")

# ----------------------------
# JDBC Writer
# ----------------------------

@retry(max_attempts=3, delay=5)
def write_jdbc(df: DataFrame, table: str):
    df.write.jdbc(DB_URL, table, "append", properties=JDBC_PROPERTIES)

def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()