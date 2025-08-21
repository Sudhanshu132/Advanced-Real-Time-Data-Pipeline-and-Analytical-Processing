import os
import time
from functools import wraps
from pyspark.sql.functions import col, trim, sha2, concat_ws, coalesce, lit, current_timestamp, when, date_format, isnan
from pyspark.sql.functions import min, max, avg, stddev, col
from pyspark.sql import DataFrame
from config import *
from logger import * 

# ----------------------------
# Retry Decorator
# ----------------------------

def retry(max_attempts=3, delay=5, backoff=2):
    """
    Retry decorator for fault-tolerant operations.
    
    Args:
        max_attempts (int): Maximum number of retry attempts.
        delay (int): Initial delay between retries (in seconds).
        backoff (int): Exponential backoff multiplier.

    Usage:
        @retry(max_attempts=3, delay=5)
        def function(...):
            ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            current_delay = delay
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    logger.error(f"Error in {func.__name__}: {e} | Attempt {attempts}/{max_attempts}")
                    if attempts == max_attempts:
                        raise
                    time.sleep(current_delay)
                    current_delay *= backoff
        return wrapper
    return decorator

def trim_all_strings(df: DataFrame) -> DataFrame:
    for f in df.schema.fields:
        if f.dataType.simpleString() == "string":
            df = df.withColumn(f.name, trim(col(f.name)))
    return df


def drop_all_null_rows(df: DataFrame) -> DataFrame:
    return df.na.drop(how="all")

# ----------------------------
# Data Validation & Quarantine
# ----------------------------

def validate_and_quarantine(df: DataFrame, file_name: str, quarantine_dir: str,
                            key_fields: list = None, numeric_fields: list = None,
                            ranges: dict = None, heavy_null_threshold: float = 0.5):
    """
    Generic data validator that checks:
      - Key fields are not null
      - Numeric fields contain valid numbers
      - Field values fall within specified ranges
      - Rows with excessive nulls are flagged

    Bad rows are quarantined with error reasons.

    Args:
        df (DataFrame): Input dataframe.
        file_name (str): Source file name.
        quarantine_dir (str): Path to quarantine invalid rows.
        key_fields (list): List of mandatory key columns.
        numeric_fields (list): Columns expected to be numeric.
        ranges (dict): Range checks in format {col: (min, max)}.
        heavy_null_threshold (float): Proportion of nulls in row considered invalid.

    Returns:
        (DataFrame, DataFrame): Tuple of (good_rows, bad_rows).
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

# ----------------------------
# Aggregations
# ----------------------------

def apply_aggregations(df: DataFrame, table: str, data_source: str = "minio_bucket") -> DataFrame:
    """
    Apply basic aggregations (min, max, avg, stddev) on numeric columns.
    Grouped by 'sensor_id' (or default value if missing).
    Writes results into Postgres.

    Args:
        df (DataFrame): Input data.
        table (str): Target table name.
        data_source (str): Source identifier.

    Returns:
        DataFrame: Aggregated result with metadata.
    """
    if df is None or df.rdd.isEmpty():
        logger.warning(f"No data to aggregate for {table}")
        return None

    # Ensure grouping column exists
    group_col = "sensor_id"
    if group_col not in df.columns:
        logger.warning(f"'{group_col}' column missing in {table}, adding default value 'unknown'")
        df = df.withColumn(group_col, lit("unknown"))

    # Identify numeric columns dynamically
    numeric_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() in ("double","int","float","bigint")]

    if not numeric_cols:
        logger.warning(f"No numeric columns found for aggregation in {table}")
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
                           .withColumn("file_name", lit(table)) \
                           .withColumn("ingestion_ts", current_timestamp())
    
    # Persist aggregated metrics
    try:
        aggregated_df.write.jdbc(DB_URL, table, mode="append", properties=JDBC_PROPERTIES)
        logger.info(f"Aggregated metrics stored in table {table} for {table}")
    except Exception as e:
        logger.error(f"Failed to write aggregated metrics for {table} | Error: {e}", exc_info=True)

    return aggregated_df
# ----------------------------
# Metadata Helpers
# ----------------------------
def add_metadata(df: DataFrame, file_name: str) -> DataFrame:
    """
    Adds metadata columns: 'file_path', 'ingestion_ts', and row hash
    """
    concat_cols = concat_ws("||", *[coalesce(col(c).cast("string"), lit("NULL")) for c in df.columns])
    return df.withColumn("file_path", lit(file_name))\
             .withColumn("ingestion_ts", current_timestamp())\
             .withColumn("row_hash", sha2(concat_cols, 256))


def derive_table_name(file_name: str) -> str:
    """Generate a Postgres table name from a file name (safe for schema)."""
    base = os.path.basename(file_name).split(".")[0]
    return f"{DB_SCHEMA}.{base}".replace("-", "_").replace(" ", "_")



# ----------------------------
# Quarantine
# ----------------------------

@retry(max_attempts=3, delay=5)
def write_quarantine(bad: DataFrame, quarantine_dir: str, file_name: str):
    """
    Persist invalid (quarantined) rows as JSON, partitioned by date.
    
    Args:
        bad (DataFrame): Invalid rows.
        quarantine_dir (str): Directory for quarantined data.
        file_name (str): Source file name.
    """
    if not bad.rdd.isEmpty():
        logger.info(f"Writing {bad.count()} rows to quarantine for file: {file_name}")
        bad = bad.withColumn("quarantine_date", date_format(current_timestamp(), "yyyy-MM-dd"))
        bad.write.mode("append")\
           .partitionBy("quarantine_date")\
           .json(os.path.join(quarantine_dir,derive_table_name(file_name)))
