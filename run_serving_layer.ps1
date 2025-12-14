# Script to run Serving Layer API Server

Clear-Host

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  SERVING LAYER API SERVER" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if virtual environment is activated
if (-not $env:VIRTUAL_ENV) {
    Write-Host "Virtual environment not activated. Activating .venv..." -ForegroundColor Yellow

    if (Test-Path ".venv\Scripts\Activate.ps1") {
        . .venv\Scripts\Activate.ps1
    }
    else {
        Write-Host "Virtual environment not found. Please create one first:" -ForegroundColor Red
        Write-Host "python -m venv .venv" -ForegroundColor Yellow
        exit 1
    }
}

# Set environment variables
$env:USE_MOCK_DATA = "true"
$env:MINIO_ENDPOINT = "http://localhost:9002"

Write-Host "Environment:" -ForegroundColor Green
Write-Host "  USE_MOCK_DATA = $env:USE_MOCK_DATA" -ForegroundColor Gray
Write-Host "  MINIO_ENDPOINT = $env:MINIO_ENDPOINT" -ForegroundColor Gray
Write-Host ""

# Check dependencies
Write-Host "Checking dependencies..." -ForegroundColor Yellow

try {
    python -c "import fastapi, uvicorn" 2>$null

    if ($LASTEXITCODE -ne 0) {
        Write-Host "Installing dependencies..." -ForegroundColor Yellow
        pip install -r serving_layer/requirements.txt
    }
    else {
        Write-Host "Dependencies OK" -ForegroundColor Green
    }
}
catch {
    Write-Host "Failed to check/install dependencies" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  BTL BIG DATA - SERVING LAYER" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Starting FastAPI server..." -ForegroundColor Green

# RUN SERVER (CHỈ 1 CÁCH DUY NHẤT)
python -m uvicorn serving_layer.serving_layer:app --host 0.0.0.0 --port 8000 --reload
