# Script setup Spark environment để dùng Spark từ C:/spark/spark3.5

$SPARK_HOME = "C:\spark\spark3.5"

# Kiểm tra Spark có tồn tại không
if (-not (Test-Path $SPARK_HOME)) {
    Write-Host "ERROR: Spark không tìm thấy tại $SPARK_HOME" -ForegroundColor Red
    exit 1
}

Write-Host "Setting SPARK_HOME = $SPARK_HOME" -ForegroundColor Green

# Set environment variables
$env:SPARK_HOME = $SPARK_HOME
$env:PYTHONPATH = "$SPARK_HOME\python;$SPARK_HOME\python\lib\py4j-0.10.9.7-src.zip;$env:PYTHONPATH"

# Thêm vào PATH
$env:PATH = "$SPARK_HOME\bin;$SPARK_HOME\python;$env:PATH"

# Kiểm tra Java
$javaVersion = java -version 2>&1 | Select-String "version"
Write-Host "Java version: $javaVersion" -ForegroundColor Yellow

# Kiểm tra Spark
if (Test-Path "$SPARK_HOME\bin\spark-submit.cmd") {
    Write-Host "✓ Spark found at $SPARK_HOME" -ForegroundColor Green
} else {
    Write-Host "⚠ Spark bin không tìm thấy" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Để dùng Spark này, chạy:" -ForegroundColor Cyan
Write-Host "  . .\setup_spark_env.ps1" -ForegroundColor White
Write-Host "  python batch_layer/run_batch_jobs.py ..." -ForegroundColor White

