@echo off

where docker-compose >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo docker-compose not found. Please install Docker Desktop / Compose.
    exit /b 1
)

echo Stopping Docker containers...
docker-compose down