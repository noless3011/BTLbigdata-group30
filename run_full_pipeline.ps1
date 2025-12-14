# Script chạy toàn bộ pipeline Big Data
# BTLbigdata-group30 - Lambda Architecture

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  BTL BIG DATA - FULL PIPELINE RUNNER" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Kiểm tra Docker
Write-Host "[1/5] Kiểm tra Docker..." -ForegroundColor Yellow
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "ERROR: Docker chưa được cài đặt hoặc không có trong PATH" -ForegroundColor Red
    exit 1
}
Write-Host "✓ Docker đã sẵn sàng" -ForegroundColor Green
Write-Host ""

# Kiểm tra Python
Write-Host "[2/5] Kiểm tra Python và dependencies..." -ForegroundColor Yellow
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Host "ERROR: Python chưa được cài đặt" -ForegroundColor Red
    exit 1
}

# Kiểm tra dependencies
Write-Host "  Đang kiểm tra dependencies..." -ForegroundColor Gray
$required = @("kafka-python", "pyspark", "faker")
$missing = @()
foreach ($pkg in $required) {
    $installed = python -m pip show $pkg 2>$null
    if (-not $installed) {
        $missing += $pkg
    }
}

if ($missing.Count -gt 0) {
    Write-Host "  Cài đặt dependencies thiếu..." -ForegroundColor Yellow
    python -m pip install -r config/requirements.txt
}
Write-Host "✓ Python và dependencies đã sẵn sàng" -ForegroundColor Green
Write-Host ""

# Bước 1: Khởi động infrastructure
Write-Host "[3/5] Khởi động Docker infrastructure..." -ForegroundColor Yellow
Set-Location config
Write-Host "  Đang khởi động containers (Kafka, MinIO, Spark, HDFS, MongoDB)..." -ForegroundColor Gray

# Kiểm tra xem containers đã chạy chưa
$running = docker ps --format "{{.Names}}"
if ($running -match "kafka|minio|zookeeper") {
    Write-Host "  Một số containers đã chạy. Đang restart..." -ForegroundColor Gray
    docker-compose down
}

docker-compose up -d
Write-Host "  Đợi services khởi động (30 giây)..." -ForegroundColor Gray
Start-Sleep -Seconds 30

# Kiểm tra services
Write-Host "  Kiểm tra services..." -ForegroundColor Gray
$kafka_ok = docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "  Đợi thêm 20 giây để Kafka sẵn sàng..." -ForegroundColor Gray
    Start-Sleep -Seconds 20
}

# Tạo Kafka topics nếu chưa có
Write-Host "  Tạo Kafka topics..." -ForegroundColor Gray
$topics = @("auth_topic", "assessment_topic", "video_topic", "course_topic", "profile_topic", "notification_topic")
foreach ($topic in $topics) {
    docker exec kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic $topic --partitions 3 --replication-factor 1 2>$null | Out-Null
}

# Tạo MinIO bucket
Write-Host "  Tạo MinIO bucket..." -ForegroundColor Gray
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin 2>$null | Out-Null
docker exec minio mc mb local/bucket-0 --ignore-existing 2>$null | Out-Null

Set-Location ..
Write-Host "✓ Infrastructure đã sẵn sàng" -ForegroundColor Green
Write-Host "  - Kafka: localhost:9092" -ForegroundColor Gray
Write-Host "  - MinIO: http://localhost:9000 (Console: http://localhost:9001)" -ForegroundColor Gray
Write-Host "  - Spark Master: http://localhost:8080" -ForegroundColor Gray
Write-Host ""

# Bước 2: Chạy Producer
Write-Host "[4/5] Chạy Producer để tạo events..." -ForegroundColor Yellow
Write-Host "  Đang tạo events và gửi vào Kafka (sẽ chạy 60 giây)..." -ForegroundColor Gray
Write-Host "  (Nhấn Ctrl+C để dừng sớm)" -ForegroundColor Gray
Write-Host ""

$producerJob = Start-Job -ScriptBlock {
    Set-Location $using:PWD
    python ingestion_layer/producer.py
}

# Đợi 60 giây để producer tạo đủ data
Start-Sleep -Seconds 60
Stop-Job $producerJob
Remove-Job $producerJob

Write-Host "✓ Producer đã tạo events" -ForegroundColor Green
Write-Host ""

# Bước 3: Chạy Ingestion (Kafka → MinIO)
Write-Host "[5/5] Chạy Ingestion Layer (Kafka → MinIO)..." -ForegroundColor Yellow
Write-Host "  Đang đọc từ Kafka và ghi vào MinIO..." -ForegroundColor Gray
Write-Host "  (Sẽ chạy 2 phút để ingest data, sau đó tự động dừng)" -ForegroundColor Gray
Write-Host ""

# Chạy ingestion trong background với timeout
$ingestionScript = @"
import time
import subprocess
import sys

# Chạy ingestion script
proc = subprocess.Popen([sys.executable, 'ingestion_layer/minio_ingest.py'], 
                        stdout=subprocess.PIPE, 
                        stderr=subprocess.PIPE)

# Đợi 120 giây (2 phút)
time.sleep(120)

# Dừng process
proc.terminate()
proc.wait()
"@

$ingestionScript | Out-File -FilePath "temp_ingestion.py" -Encoding utf8
python temp_ingestion.py
Remove-Item temp_ingestion.py

Write-Host "✓ Ingestion đã hoàn thành" -ForegroundColor Green
Write-Host ""

# Bước 4: Chạy Batch Processing
Write-Host "[BONUS] Chạy Batch Processing Jobs..." -ForegroundColor Yellow
Write-Host "  Đang chạy 5 batch jobs để tạo 37 batch views..." -ForegroundColor Gray
Write-Host "  (Có thể mất vài phút)" -ForegroundColor Gray
Write-Host ""

# Set MinIO endpoint cho local execution
$env:MINIO_ENDPOINT = "http://localhost:9002"

# Set Java options để fix lỗi getSubject trên Windows với Java 17+
$javaOpts = "--add-opens=java.base/javax.security.auth=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED"
# Set JAVA_TOOL_OPTIONS for all Java processes (applies to SparkSubmit)
$env:JAVA_TOOL_OPTIONS = $javaOpts
# Also set PYSPARK_SUBMIT_ARGS
$env:PYSPARK_SUBMIT_ARGS = "--driver-java-options '$javaOpts' --conf spark.driver.extraJavaOptions='$javaOpts' --conf spark.executor.extraJavaOptions='$javaOpts' pyspark-shell"

# Chạy batch jobs
python batch_layer/run_batch_jobs.py s3a://bucket-0/master_dataset s3a://bucket-0/batch_views

Write-Host "✓ Batch processing đã hoàn thành" -ForegroundColor Green
Write-Host ""

# Tổng kết
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  PIPELINE ĐÃ CHẠY THÀNH CÔNG!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Kiểm tra kết quả:" -ForegroundColor Yellow
Write-Host "  1. MinIO Console: http://localhost:9001" -ForegroundColor White
Write-Host "     - Username: minioadmin" -ForegroundColor Gray
Write-Host "     - Password: minioadmin" -ForegroundColor Gray
Write-Host "     - Bucket: bucket-0" -ForegroundColor Gray
Write-Host ""
Write-Host "  2. Spark UI: http://localhost:8080" -ForegroundColor White
Write-Host ""
Write-Host "  3. Kafka Topics:" -ForegroundColor White
Write-Host "     docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list" -ForegroundColor Gray
Write-Host ""
Write-Host "Để dừng infrastructure:" -ForegroundColor Yellow
Write-Host "  cd config" -ForegroundColor Gray
Write-Host "  docker-compose down" -ForegroundColor Gray
Write-Host ""

# Bước 6: Chạy Serving Layer API
Write-Host "[6/6] Chạy Serving Layer API..." -ForegroundColor Yellow
Write-Host "  FastAPI sẽ chạy tại http://localhost:8000" -ForegroundColor Gray
Write-Host ""

python -m uvicorn serving_layer.serving_layer:app --host 0.0.0.0 --port 8000