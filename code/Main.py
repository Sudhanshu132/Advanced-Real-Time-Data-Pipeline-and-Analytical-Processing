from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
from pyspark.sql.types import StructType
from config import * 
from utils import *
from helpers import *
import time

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
            # --- Dynamic schema load from S3 ---
            schema_file_name = derive_schema_filename(paths[0])
            schema_json = read_schema(schema_file_name)
            schema = StructType.fromJson(schema_json) if schema_json else None

            raw = read_batch_files(paths, fmt, spark, schema=schema)

            # ----------------- Data Processing -----------------
            trimmed = trim_all_strings(raw)
            non_all_null = drop_all_null_rows(trimmed)

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

            # ----------------- Write Good Data -----------------
            if not good_with_meta.rdd.isEmpty():
                table = derive_table_name(paths[0])
                logger.info(f"Writing {good_with_meta.count()} rows to PostgreSQL table: {table}")
                # good_with_meta.write.jdbc(DB_URL, f"{table}_transformed", "append", properties=JDBC_PROPERTIES)
                write_jdbc(good_with_meta, f"{table}_transformed")
                # --- Aggregated Metrics (optional) ---
                apply_aggregations(good_with_meta, f"{table}_agg")

            # ----------------- Collect File Paths -----------------
            if not good.rdd.isEmpty():
                good_file_paths = set(r.file_path for r in good.select("file_path").distinct().collect())
            if not bad.rdd.isEmpty():
                bad_file_paths = set(r.file_path for r in bad.select("file_path").distinct().collect())
                bad_file_paths -= good_file_paths  # Remove overlap

            # ----------------- Write Audit Logs -----------------
            write_audit(
                spark, BUCKET_AUDIT, file_name_list, fmt,
                total=raw.rdd.countApprox(5000),
                good=good_with_meta.rdd.countApprox(5000),
                bad=bad.rdd.countApprox(5000),
                status="SUCCESS",
                message=f"Batch {batch_id}"
            )

            # ----------------- Move Files -----------------
            if good_file_paths:
                move_files_to_folder(list(good_file_paths), "processed")
            if bad_file_paths:
                move_files_to_folder(list(bad_file_paths), "qurantine")

            logger.info(f"Batch {batch_id} processing completed for files: {file_name_list}")

        except Exception as e:
            logger.error(f"Batch {batch_id} failed for files: {file_name_list} | Error: {e}", exc_info=True)
            write_audit(
                spark, BUCKET_AUDIT, file_name_list, fmt,
                total=0, good=0, bad=0,
                status="FAILURE",
                message=str(e)
            )
            move_files_to_folder(paths, "qurantine")
            raise e

# ----------------- Start Streaming -----------------
while True:
    try:
        query = streaming.writeStream.trigger(processingTime=f"{TRIGGER_INTERVAL_SEC} seconds") \
                    .option("checkpointLocation", CHECKPOINT_PATH) \
                    .foreachBatch(process_batch) \
                    .start()
        query.awaitTermination()
    except Exception as e:
        logger.error(f"Streaming query failed: {e}, restarting in 10s...")
        time.sleep(10)