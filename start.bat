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
REM Step 3: Run Spark job
REM -----------------------------
echo Running Spark job inside Spark container...
docker exec spark spark-submit /home/spark/Main.py