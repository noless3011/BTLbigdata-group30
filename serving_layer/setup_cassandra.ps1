# Cassandra Quick Setup Script (PowerShell)
# This script sets up Cassandra for the serving layer

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Cassandra Serving Layer Setup" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if running in project root
if (-Not (Test-Path "serving_layer/cassandra_schema.cql")) {
    Write-Host "❌ Error: Please run this script from the project root directory" -ForegroundColor Red
    Write-Host "   cd BTLbigdata-group30" -ForegroundColor Yellow
    exit 1
}

# Check if Docker is running
try {
    docker ps | Out-Null
} catch {
    Write-Host "❌ Error: Docker is not running. Please start Docker first." -ForegroundColor Red
    exit 1
}

# Step 1: Start Cassandra
Write-Host "[1/5] Starting Cassandra container..." -ForegroundColor Yellow
Push-Location config
docker-compose up -d cassandra
Pop-Location

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Failed to start Cassandra" -ForegroundColor Red
    exit 1
}

# Step 2: Wait for Cassandra to be ready
Write-Host "[2/5] Waiting for Cassandra to be ready (this may take 60-90 seconds)..." -ForegroundColor Yellow
$maxRetries = 30
$retryCount = 0

while ($retryCount -lt $maxRetries) {
    try {
        $result = docker exec cassandra cqlsh -e "SELECT now() FROM system.local" 2>$null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✓ Cassandra is ready!" -ForegroundColor Green
            break
        }
    } catch {
        # Continue waiting
    }

    $retryCount++
    if ($retryCount -eq $maxRetries) {
        Write-Host "❌ Error: Cassandra failed to start in time" -ForegroundColor Red
        Write-Host "   Check logs: docker logs cassandra" -ForegroundColor Yellow
        exit 1
    }

    Write-Host "   Attempt $retryCount/$maxRetries - waiting..." -ForegroundColor Gray
    Start-Sleep -Seconds 3
}

# Step 3: Initialize schema
Write-Host "[3/5] Initializing Cassandra schema..." -ForegroundColor Yellow
python serving_layer/init_cassandra_schema.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Error: Schema initialization failed" -ForegroundColor Red
    exit 1
}

# Step 4: Check if MinIO has data
Write-Host "[4/5] Checking for data in MinIO..." -ForegroundColor Yellow
$minioRunning = docker ps | Select-String "minio"

if ($minioRunning) {
    Write-Host "✓ MinIO is running" -ForegroundColor Green

    # Wait a bit for MinIO to be ready
    Start-Sleep -Seconds 2

    # Check if we have batch views
    try {
        $hasData = docker exec minio mc ls local/bucket-0/batch_views/ 2>$null
        if ($hasData) {
            Write-Host "✓ Found data in MinIO" -ForegroundColor Green
            Write-Host "[5/5] Running initial data sync..." -ForegroundColor Yellow
            python serving_layer/cassandra_sync.py --once

            if ($LASTEXITCODE -eq 0) {
                Write-Host "✓ Data sync completed successfully" -ForegroundColor Green
            } else {
                Write-Host "⚠ Warning: Data sync had some errors, but schema is ready" -ForegroundColor Yellow
            }
        } else {
            Write-Host "⚠ Warning: No data found in MinIO batch_views/" -ForegroundColor Yellow
            Write-Host "   You should run batch jobs first, then sync:" -ForegroundColor Gray
            Write-Host "   python batch_layer/run_batch_jobs.py s3a://bucket-0/master_dataset s3a://bucket-0/batch_views" -ForegroundColor White
            Write-Host "   python serving_layer/cassandra_sync.py --once" -ForegroundColor White
        }
    } catch {
        Write-Host "⚠ Warning: Could not check MinIO data" -ForegroundColor Yellow
        Write-Host "[5/5] Skipping data sync" -ForegroundColor Yellow
    }
} else {
    Write-Host "⚠ Warning: MinIO is not running" -ForegroundColor Yellow
    Write-Host "   Start it with: cd config; docker-compose up -d minio" -ForegroundColor Gray
    Write-Host "[5/5] Skipping data sync (no MinIO)" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "✓ Cassandra Setup Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host ""
Write-Host "1. Start the Cassandra-based API:" -ForegroundColor Yellow
Write-Host "   uvicorn serving_layer.serving_layer_cassandra:app --host 0.0.0.0 --port 8000" -ForegroundColor White
Write-Host ""
Write-Host "2. Start the dashboard:" -ForegroundColor Yellow
Write-Host "   streamlit run serving_layer/dashboard.py" -ForegroundColor White
Write-Host ""
Write-Host "3. (Optional) Start continuous sync service:" -ForegroundColor Yellow
Write-Host "   python serving_layer/cassandra_sync.py" -ForegroundColor White
Write-Host ""
Write-Host "Access points:" -ForegroundColor Cyan
Write-Host "  - API: http://localhost:8000" -ForegroundColor White
Write-Host "  - API Docs: http://localhost:8000/docs" -ForegroundColor White
Write-Host "  - Dashboard: http://localhost:8501" -ForegroundColor White
Write-Host ""
Write-Host "Useful commands:" -ForegroundColor Cyan
Write-Host "  - Connect to Cassandra: docker exec -it cassandra cqlsh" -ForegroundColor White
Write-Host "  - Check status: docker exec cassandra nodetool status" -ForegroundColor White
Write-Host "  - View logs: docker logs cassandra" -ForegroundColor White
Write-Host "  - Manual sync: python serving_layer/cassandra_sync.py --once" -ForegroundColor White
Write-Host ""
