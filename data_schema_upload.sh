#!/bin/bash

# -----------------------------
# Upload CSV files to MinIO data folder
# -----------------------------

CURRENT_DIR="$(pwd)"
MINIO_CSV_TARGET="myminio/my-bucket/data/"
MINIO_JSON_TARGET="myminio/my-bucket/schema/"

# Path to MinIO client
MC_CMD="$CURRENT_DIR/utils/mc"

# -----------------------------
# Upload JSON files to MinIO schema folder
# -----------------------------
echo "Uploading JSON schema files to MinIO..."
for f in "$CURRENT_DIR/schema/"*.json; do
    echo "Uploading $f to $MINIO_JSON_TARGET"
    $MC_CMD cp "$f" "$MINIO_JSON_TARGET"
done

# -----------------------------
# Upload CSV files to MinIO data folder
# -----------------------------
echo "Uploading CSV files to MinIO..."
for f in "$CURRENT_DIR/dataset/"*.csv; do
    echo "Uploading $f to $MINIO_CSV_TARGET"
    $MC_CMD cp "$f" "$MINIO_CSV_TARGET"
done

echo "Upload completed!"
