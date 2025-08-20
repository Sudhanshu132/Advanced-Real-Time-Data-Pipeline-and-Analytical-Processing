@echo off
SETLOCAL

REM -----------------------------
REM Upload CSV files to MinIO data folder
REM -----------------------------

set CURRENT_DIR=%~dp0
set MINIO_CSV_TARGET=myminio/my-bucket/data/

echo Uploading CSV files to MinIO...
for %%f in ("%CURRENT_DIR%dataset\*.csv") do (
    echo Uploading %%f to %MINIO_CSV_TARGET%
    "%CURRENT_DIR%utils\mc.exe" cp "%%f" %MINIO_CSV_TARGET%
)

REM -----------------------------
REM Upload JSON files to MinIO schema folder
REM -----------------------------
set MINIO_JSON_TARGET=myminio/my-bucket/schema/

echo Uploading JSON schema files to MinIO...
for %%f in ("%CURRENT_DIR%schema\*.json") do (
    echo Uploading %%f to %MINIO_JSON_TARGET%
    "%CURRENT_DIR%utils\mc.exe" cp "%%f" %MINIO_JSON_TARGET%
)

pause
ENDLOCAL
