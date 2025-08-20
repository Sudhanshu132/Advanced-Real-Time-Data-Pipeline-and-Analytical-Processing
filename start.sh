#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status

# -----------------------------
# Step 1: Build Docker image
# -----------------------------
echo "Building Docker image..."
docker build -t sdp-spark:1.0.0 .
if [ $? -ne 0 ]; then
    echo "Docker build failed!"
    exit 1
fi

# -----------------------------
# Step 2: Start containers
# -----------------------------
echo "Starting Docker containers..."
docker-compose up -d
if [ $? -ne 0 ]; then
    echo "docker-compose failed!"
    exit 1
fi

# -----------------------------
# Step 2a: Execute schema SQL file
# -----------------------------
echo "Executing schema SQL file..."
docker cp schema/smart_farming_crop_yield_2024_transformed.sql postgres:/smart_farming_crop_yield_2024_transformed.sql
docker exec -i postgres psql -U admin -d farmingdb -f /smart_farming_crop_yield_2024_transformed.sql
if [ $? -ne 0 ]; then
    echo "Executing SQL file failed!"
    exit 1
fi

# -----------------------------
# Step 3: Run Spark job
# -----------------------------
echo "Running Spark job inside Spark container..."
docker exec spark spark-submit /home/spark/main.py
if [ $? -ne 0 ]; then
    echo "Spark job failed!"
    exit 1
fi

echo "Pipeline executed successfully!"
