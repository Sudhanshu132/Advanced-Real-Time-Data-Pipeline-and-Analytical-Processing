import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
from pyspark.sql.types import StructType
from config import * 
from utils import *
from helpers import *
from logger import * 

# ----------------------------
# Spark Session Initialization
# ----------------------------
# Configure Spark session with MinIO (S3-compatible) connector
# Includes committers, path style access, and credentials
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

# Reduce Spark logging noise
spark.sparkContext.setLogLevel("ERROR")

# ----------------------------
# Bucket Initialization
# ----------------------------
# Ensures required folders exist in the MinIO bucket
initialize_bucket(BUCKET_NAME, folders=["data","processed","audit","qurantine","schema"])

# ----------------------------
# Streaming Read (File Arrival Detection)
# ----------------------------
logger.info("Starting streaming read from input directory")

# Watch `data/` folder in MinIO for new files (CSV/JSON)
# "wholetext" ensures full file content is read
# Deduplicate files by path to avoid re-processing
streaming = spark.readStream.format("text") \
    .option("wholetext", "true") \
    .option("pathGlobFilter", "*.{csv,json}") \
    .load(BUCKET_DATA) \
    .withColumn("file_path", input_file_name()) \
    .dropDuplicates(["file_path"])

# ----------------------------
# Batch Processing Logic
# ----------------------------

def process_batch(batch_df, batch_id):
    """
    Process a micro-batch of incoming files.

    Steps:
        1. Group files by format (CSV/JSON)
        2. Load schema dynamically (from schema/ folder if available)
        3. Read data into Spark DataFrame
        4. Apply cleaning (trim strings, drop null-only rows)
        5. Validate records and quarantine bad rows
        6. Add metadata (hash, file path, ingestion timestamp)
        7. Write good data into PostgreSQL
        8. Compute and write aggregated metrics
        9. Write audit logs
        10. Move processed files to "processed" or "qurantine" folders

    Args:
        batch_df (DataFrame): Batch containing discovered file paths.
        batch_id (int): Unique batch ID assigned by Spark.
    """
    logger.info(f"Processing batch {batch_id} with {batch_df.count()} files")
    files_by_fmt = batch_df.select("file_path").distinct().rdd \
        .map(lambda r: r.file_path) \
        .groupBy(lambda f: f.split(".")[-1].lower()) \
        .mapValues(list) \
        .collectAsMap()

    for fmt, paths in files_by_fmt.items():
        file_name_list = ",".join(paths)
        good_file_paths = []
        bad_file_paths = []

        try:
            # ----------------------------
            # Dynamic Schema Load
            # ----------------------------
            schema_file_name = derive_schema_filename(paths[0])
            schema_json = read_schema(schema_file_name)
            schema = StructType.fromJson(schema_json) if schema_json else None

            # Read raw data (CSV/JSON) using schema (if available)
            raw = read_batch_files(paths, fmt, spark, schema=schema)

            # ----------------------------
            # Data Cleaning
            # ----------------------------
            trimmed = trim_all_strings(raw)
            non_all_null = drop_all_null_rows(trimmed)

            # ----------------------------
            # Data Validation
            # ----------------------------
            good, bad = validate_and_quarantine(
                non_all_null,
                file_name_list,
                BUCKET_QURANTINE,
                key_fields=["sensor_id", "timestamp", "temperature_C"],
                numeric_fields=["temperature_C"],
                ranges={"temperature_C": (-50, 50)},
                heavy_null_threshold=0.5
            )

            good_with_meta = add_metadata(good, file_name_list)

            # ----------------------------
            # Write Good Data to PostgreSQL
            # ----------------------------
            if not good_with_meta.rdd.isEmpty():
                table = derive_table_name(paths[0])
                logger.info(f"Writing {good_with_meta.count()} rows to PostgreSQL table: {table}_transformed")
                write_jdbc(good_with_meta, f"{table}_transformed")
                # Also compute aggregated metrics (min/max/avg/stddev)
                apply_aggregations(good_with_meta, f"{table}_agg")

            # ----------------------------
            # File Path Tracking
            # ----------------------------
            if not good.rdd.isEmpty():
                good_file_paths = set(r.file_path for r in good.select("file_path").distinct().collect())
            if not bad.rdd.isEmpty():
                bad_file_paths = set(r.file_path for r in bad.select("file_path").distinct().collect())
                bad_file_paths -= good_file_paths  # Remove overlap

            # ----------------------------
            # Audit Logging
            # ----------------------------
            write_audit(
                spark, BUCKET_AUDIT, file_name_list, fmt,
                total=raw.rdd.countApprox(5000),
                good=good_with_meta.rdd.countApprox(5000),
                bad=bad.rdd.countApprox(5000),
                status="SUCCESS",
                message=f"Batch {batch_id}"
            )

            # ----------------------------
            # Move Processed Files
            # ----------------------------
            if good_file_paths:
                move_files_to_folder(list(good_file_paths), "processed")
            if bad_file_paths:
                move_files_to_folder(list(bad_file_paths), "qurantine")

            logger.info(f"Batch {batch_id} processing completed for files: {file_name_list}")

        except Exception as e:
            # On failure: write audit, move files to quarantine, rethrow error
            logger.error(f"Batch {batch_id} failed for files: {file_name_list} | Error: {e}", exc_info=True)
            write_audit(
                spark, BUCKET_AUDIT, file_name_list, fmt,
                total=0, good=0, bad=0,
                status="FAILURE",
                message=str(e)
            )
            move_files_to_folder(paths, "qurantine")
            raise e

# ----------------------------
# Streaming Job (Forever Loop)
# ----------------------------
while True:
    try:
        # Trigger batch processing every TRIGGER_INTERVAL_SEC
        query = streaming.writeStream.trigger(processingTime=f"{TRIGGER_INTERVAL_SEC} seconds") \
                    .option("checkpointLocation", CHECKPOINT_PATH) \
                    .foreachBatch(process_batch) \
                    .start()
        query.awaitTermination()
    except Exception as e:
        # In case streaming fails, restart after short delay
        logger.error(f"Streaming query failed: {e}, restarting in 10s...")
        time.sleep(10)