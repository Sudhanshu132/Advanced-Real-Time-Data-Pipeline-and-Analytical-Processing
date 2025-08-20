@echo off
SETLOCAL

REM -----------------------------
REM Step 1: Build Docker image
REM -----------------------------
echo Building Docker image...
docker build -t sdp-spark:1.0.0 .
IF %ERRORLEVEL% NEQ 0 (
    echo Docker build failed!
    EXIT /B 1
)

REM -----------------------------
REM Step 2: Start containers
REM -----------------------------
echo Starting Docker containers...
docker-compose up -d
IF %ERRORLEVEL% NEQ 0 (
    echo Docker-compose failed!
    EXIT /B 1
)

REM -----------------------------
REM Step 2a: Execute schema SQL file
REM -----------------------------
echo Executing schema SQL file...
docker cp schema\smart_farming_crop_yield_2024_transformed.sql postgres:/smart_farming_crop_yield_2024_transformed.sql
docker exec -i postgres psql -U admin -d farmingdb -f /smart_farming_crop_yield_2024_transformed.sql
IF %ERRORLEVEL% NEQ 0 (
    echo Executing SQL file failed!
    EXIT /B 1
)

REM -----------------------------
REM Step 3: Run Spark job
REM -----------------------------
echo Running Spark job inside Spark container...
docker exec spark spark-submit /home/spark/main.py
IF %ERRORLEVEL% NEQ 0 (
    echo Spark job failed!
    EXIT /B 1
)
