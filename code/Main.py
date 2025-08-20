from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, lit, current_timestamp
from pyspark.sql.types import StructType
from config import * 
from utils import *
from helpers import *

# ----------------- Spark Session -----------------
spark = SparkSession.builder \
    .appName("Advanced-Real-Time-DataPipeline") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.committer.name", "magic") \
    .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true") \
    .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ----------------- Initialize Buckets -----------------
initialize_bucket(BUCKET_NAME, folders=["data","processed","audit","qurantine","schema"])

# ----------------- Streaming Read -----------------
logger.info("Starting streaming read from input directory")

streaming = spark.readStream.format("text") \
    .option("wholetext", "true") \
    .option("pathGlobFilter", "*.{csv,json}") \
    .load(BUCKET_DATA) \
    .withColumn("file_path", input_file_name()) \
    .dropDuplicates(["file_path"])

# ----------------- Batch Processing -----------------
def process_batch(batch_df, batch_id):
    """
    Process each incoming file independently, with per-file schema check, safe reads,
    and safe move to processed/quarantine folders.
    """
    logger.info(f"Processing batch {batch_id} with {batch_df.count()} files")
    
    # Collect distinct file paths in this batch
    file_paths = [r.file_path for r in batch_df.select("file_path").distinct().collect()]

    for file_path in file_paths:
        fmt = file_path.split(".")[-1].lower()
        try:
            logger.info(f"Processing file: {file_path}")

            # --- Load schema for this file ---
            schema_file_name = derive_schema_filename(file_path)
            schema_json = read_schema(schema_file_name)
            schema = StructType.fromJson(schema_json) if schema_json else None

            # --- Read the file safely ---
            raw = read_batch_files([file_path], fmt, spark, schema=schema)

            # Trigger action to ensure Spark reads the file
            row_count = raw.count()
            if row_count == 0:
                logger.warning(f"No data in file: {file_path}")
                move_files_to_folder([file_path], "qurantine")
                continue

            # --- Data Processing ---
            trimmed = trim_all_strings(raw)
            non_all_null = drop_all_null_rows(trimmed)

            good, bad = validate_and_quarantine(
                non_all_null,
                file_path,
                BUCKET_QURANTINE,
                key_fields=["sensor_id", "timestamp", "temperature_C"],
                numeric_fields=["temperature_C"],
                ranges={"temperature_C": (-50, 50)},
                heavy_null_threshold=0.5
            )

            good_with_meta = add_metadata(good, file_path)

            # --- Write Good Data to PostgreSQL ---
            if not good_with_meta.rdd.isEmpty():
                table = derive_table_name(file_path, fmt)
                logger.info(f"Writing {good_with_meta.count()} rows to PostgreSQL table: {table}_transformed")
                good_with_meta.write.jdbc(DB_URL, f"{table}_transformed", "append", properties=JDBC_PROPERTIES)

                # --- Aggregated Metrics ---
                apply_aggregations(good_with_meta, file_path, fmt)

            # --- Materialize counts to avoid lazy execution ---
            good_rows = good_with_meta.count() if not good_with_meta.rdd.isEmpty() else 0
            bad_rows = bad.count() if not bad.rdd.isEmpty() else 0

            # --- Move files safely after processing ---
            if good_rows > 0:
                move_files_to_folder([file_path], "processed")
            if bad_rows > 0:
                move_files_to_folder([file_path], "qurantine")

            # --- Write Audit Logs ---
            write_audit(
                spark, BUCKET_AUDIT, file_path, fmt,
                total=row_count,
                good=good_rows,
                bad=bad_rows,
                status="SUCCESS",
                message=f"Batch {batch_id}"
            )

            logger.info(f"File {file_path} processed successfully.")

        except Exception as e:
            logger.error(f"Failed to process file: {file_path} | Error: {e}", exc_info=True)
            write_audit(
                spark, BUCKET_AUDIT, file_path, fmt,
                total=0, good=0, bad=0,
                status="FAILURE",
                message=str(e)
            )
            move_files_to_folder([file_path], "qurantine")

# ----------------- Start Streaming -----------------
logger.info("Starting streaming query")

query = streaming.writeStream.trigger(processingTime=f"{TRIGGER_INTERVAL_SEC} seconds") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()
