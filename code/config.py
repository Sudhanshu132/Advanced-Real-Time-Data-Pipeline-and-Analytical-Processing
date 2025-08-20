import os

# MinIO Config

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME      = os.getenv("BUCKET_NAME")
BUCKET_DATA      = f"s3a://{BUCKET_NAME}/data"
BUCKET_PROCESSED = f"s3a://{BUCKET_NAME}/processed"
BUCKET_QURANTINE = f"s3a://{BUCKET_NAME}/qurantine"
BUCKET_AUDIT = f"s3a://{BUCKET_NAME}/audit"
BUCKET_SCHEMA = f"s3a://{BUCKET_NAME}/schema"
CHECKPOINT_PATH  = os.getenv("CHECKPOINT_PATH", "/tmp/spark-checkpoints")
TRIGGER_INTERVAL_SEC = os.getenv("TRIGGER_INTERVAL_SEC")


# PostgreSQL Config

DB_URL      = os.getenv("DB_URL")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

JDBC_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver",
    "rewriteBatchedStatements": "true",
    "batchsize": "5000"
}