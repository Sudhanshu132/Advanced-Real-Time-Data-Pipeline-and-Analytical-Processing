import hashlib
import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, sha2, concat_ws, coalesce, lit, current_timestamp, when, date_format, isnan
from pyspark.sql.types import StructType
from pyspark.sql.functions import min, max, avg, stddev, col
from pyspark.sql import DataFrame
from config import * 

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)
pg_schema = "public"

def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def trim_all_strings(df: DataFrame) -> DataFrame:
    for f in df.schema.fields:
        if f.dataType.simpleString() == "string":
            df = df.withColumn(f.name, trim(col(f.name)))
    return df


def drop_all_null_rows(df: DataFrame) -> DataFrame:
    return df.na.drop(how="all")


def validate_and_quarantine(df: DataFrame, file_name: str, quarantine_dir: str,
                            key_fields: list = None, numeric_fields: list = None,
                            ranges: dict = None, heavy_null_threshold: float = 0.5):
    """
    Generic validator: validates key fields, numeric fields, and ranges.
    Quarantines invalid rows with error reasons.
    Automatically adds 'file_path' and 'ingestion_ts' columns.
    """
    if df is None or df.rdd.isEmpty():
        return df, df

    df_with_error = df.withColumn("error_reason", lit(None)) \
                      .withColumn("file_path", lit(file_name)) \
                      .withColumn("ingestion_ts", current_timestamp())

    # Key field null check
    if key_fields:
        for field in key_fields:
            if field in df.columns:
                df_with_error = df_with_error.withColumn(
                    "error_reason",
                    when(col("error_reason").isNotNull(), col("error_reason"))
                    .when(col(field).isNull(), lit(f"Missing key: {field}"))
                    .otherwise(col("error_reason"))
                )

    # Numeric fields check
    if numeric_fields:
        for field in numeric_fields:
            if field in df.columns:
                df_with_error = df_with_error.withColumn(
                    "error_reason",
                    when(col("error_reason").isNotNull(), col("error_reason"))
                    .when(col(field).cast("double").isNull() | isnan(col(field)), lit(f"Invalid numeric in {field}"))
                    .otherwise(col("error_reason"))
                )

    # Range checks
    if ranges:
        for field, (min_val, max_val) in ranges.items():
            if field in df.columns:
                df_with_error = df_with_error.withColumn(
                    "error_reason",
                    when(col("error_reason").isNotNull(), col("error_reason"))
                    .when((col(field) < min_val) | (col(field) > max_val),
                          lit(f"{field} out of range [{min_val},{max_val}]"))
                    .otherwise(col("error_reason"))
                )

    # Heavy null-row detection
    null_threshold = int(len(df.columns) * heavy_null_threshold)
    df_with_error = df_with_error.withColumn(
        "error_reason",
        when(col("error_reason").isNotNull(), col("error_reason"))
        .when(sum([col(c).isNull().cast("int") for c in df.columns]) >= null_threshold,
              lit("Too many nulls in row"))
        .otherwise(col("error_reason"))
    )

    # Split good vs bad
    good = df_with_error.filter(col("error_reason").isNull()).drop("error_reason")
    bad = df_with_error.filter(col("error_reason").isNotNull())

    # Write bad rows
    write_quarantine(bad, quarantine_dir, file_name)

    return good, bad

logger = logging.getLogger(__name__)

def apply_aggregations(df: DataFrame, file_name: str, fmt: str, data_source: str = "minio_bucket") -> DataFrame:
    """
    Aggregates numeric columns per sensor_id (or default if missing) and adds metadata.
    Handles missing numeric columns gracefully.
    """
    if df is None or df.rdd.isEmpty():
        logger.warning(f"No data to aggregate for {file_name}")
        return None

    # Ensure grouping column exists
    group_col = "sensor_id"
    if group_col not in df.columns:
        logger.warning(f"'{group_col}' column missing in {file_name}, adding default value 'unknown'")
        df = df.withColumn(group_col, lit("unknown"))

    # Identify numeric columns dynamically
    numeric_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() in ("double","int","float","bigint")]

    if not numeric_cols:
        logger.warning(f"No numeric columns found for aggregation in {file_name}")
        return None

    # Build aggregation expressions
    agg_exprs = []
    for c in numeric_cols:
        agg_exprs += [
            min(col(c)).alias(f"min_{c}"),
            max(col(c)).alias(f"max_{c}"),
            avg(col(c)).alias(f"avg_{c}"),
            stddev(col(c)).alias(f"stddev_{c}")
        ]

    # Perform aggregation
    grouped = df.groupBy(group_col).agg(*agg_exprs)

    # Add metadata
    aggregated_df = grouped.withColumn("data_source", lit(data_source)) \
                           .withColumn("file_name", lit(file_name)) \
                           .withColumn("ingestion_ts", current_timestamp())

    # Write to PostgreSQL if JDBC properties and DB_URL are defined
    table = f"{pg_schema}.{fmt}_sensor_agg"
    try:
        aggregated_df.write.jdbc(DB_URL, table, mode="append", properties=JDBC_PROPERTIES)
        logger.info(f"Aggregated metrics stored in table {table} for {file_name}")
    except Exception as e:
        logger.error(f"Failed to write aggregated metrics for {file_name} | Error: {e}", exc_info=True)

    return aggregated_df


def add_metadata(df: DataFrame, file_name: str) -> DataFrame:
    """
    Adds metadata columns: 'file_path', 'ingestion_ts', and row hash
    """
    concat_cols = concat_ws("||", *[coalesce(col(c).cast("string"), lit("NULL")) for c in df.columns])
    return df.withColumn("file_path", lit(file_name))\
             .withColumn("ingestion_ts", current_timestamp())\
             .withColumn("row_hash", sha2(concat_cols, 256))


def derive_table_name(file_name: str, fmt: str) -> str:
    base = os.path.basename(file_name).split(".")[0]
    return f"{pg_schema}.{fmt}_{base}".replace("-", "_").replace(" ", "_")


def read_batch_files(paths: list, fmt: str, spark: SparkSession, schema: StructType = None) -> DataFrame:
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


def write_audit(spark, audit_dir, file_name, fmt, total, good, bad, status, message):
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


def write_quarantine(bad: DataFrame, quarantine_dir: str, file_name: str):
    if not bad.rdd.isEmpty():
        logger.info(f"Writing {bad.count()} rows to quarantine for file: {file_name}")
        bad = bad.withColumn("quarantine_date", date_format(current_timestamp(), "yyyy-MM-dd"))
        bad.write.mode("append")\
           .partitionBy("quarantine_date")\
           .json(os.path.join(quarantine_dir, md5_hex(file_name))) # It should indicate filename
