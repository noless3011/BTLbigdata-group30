# Script test batch jobs với Java 17 đã được set đúng

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  TEST BATCH JOBS WITH JAVA 17" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Set JAVA_HOME
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
Write-Host "JAVA_HOME = $env:JAVA_HOME" -ForegroundColor Green

# Set MinIO endpoint
$env:MINIO_ENDPOINT = "http://localhost:9002"
Write-Host "MINIO_ENDPOINT = $env:MINIO_ENDPOINT" -ForegroundColor Green

# Create dummy HADOOP_HOME to avoid winutils.exe error (we use MinIO/S3, not HDFS)
$hadoopHome = Join-Path $env:TEMP "hadoop_home"
$hadoopBin = Join-Path $hadoopHome "bin"
New-Item -ItemType Directory -Force -Path $hadoopHome | Out-Null
New-Item -ItemType Directory -Force -Path $hadoopBin | Out-Null
$env:HADOOP_HOME = $hadoopHome
Write-Host "HADOOP_HOME = $env:HADOOP_HOME" -ForegroundColor Green

# Try to download winutils.exe
$winutilsPath = Join-Path $hadoopBin "winutils.exe"
if (-not (Test-Path $winutilsPath) -or (Get-Item $winutilsPath -ErrorAction SilentlyContinue).Length -eq 0) {
    Write-Host "Downloading winutils.exe..." -ForegroundColor Yellow
    try {
        Invoke-WebRequest -Uri "https://github.com/steveloughran/winutils/raw/master/hadoop-3.3.0/bin/winutils.exe" -OutFile $winutilsPath -ErrorAction Stop
        $fileSize = (Get-Item $winutilsPath).Length
        if ($fileSize -gt 0) {
            Write-Host "✓ Downloaded winutils.exe ($fileSize bytes)" -ForegroundColor Green
        } else {
            Write-Host "⚠ Downloaded file is empty" -ForegroundColor Yellow
        }
    } catch {
        Write-Host "⚠ Could not download winutils.exe: $($_.Exception.Message)" -ForegroundColor Yellow
        Write-Host "  Please download manually from:" -ForegroundColor Gray
        Write-Host "  https://github.com/steveloughran/winutils/tree/master/hadoop-3.3.0/bin" -ForegroundColor Gray
        Write-Host "  Save to: $winutilsPath" -ForegroundColor Gray
        # Create empty file as fallback
        New-Item -ItemType File -Path $winutilsPath -Force | Out-Null
    }
} else {
    $fileSize = (Get-Item $winutilsPath).Length
    if ($fileSize -gt 0) {
        Write-Host "✓ winutils.exe found ($fileSize bytes)" -ForegroundColor Green
    } else {
        Write-Host "⚠ winutils.exe exists but is empty" -ForegroundColor Yellow
    }
}

# Set Java options
$javaOpts = "--add-opens=java.base/javax.security.auth=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED"
$env:JAVA_TOOL_OPTIONS = $javaOpts
$env:PYSPARK_SUBMIT_ARGS = "--driver-java-options '$javaOpts' --conf spark.driver.extraJavaOptions='$javaOpts' --conf spark.executor.extraJavaOptions='$javaOpts' pyspark-shell"

Write-Host ""
Write-Host "Verifying Java version..." -ForegroundColor Yellow
java -version

Write-Host ""
Write-Host "Running batch job: auth" -ForegroundColor Yellow
Write-Host ""

# Chạy batch job
python batch_layer/run_batch_jobs.py auth s3a://bucket-0/master_dataset s3a://bucket-0/batch_views

