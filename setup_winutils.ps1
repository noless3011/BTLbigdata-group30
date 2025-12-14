# Script setup winutils.exe cho Hadoop trên Windows

$hadoopBin = Join-Path $env:TEMP "hadoop_home\bin"
if (-not (Test-Path $hadoopBin)) {
    New-Item -ItemType Directory -Force -Path $hadoopBin | Out-Null
}

$winutilsPath = Join-Path $hadoopBin "winutils.exe"

if (Test-Path $winutilsPath) {
    Write-Host "winutils.exe already exists at: $winutilsPath" -ForegroundColor Green
    exit 0
}

Write-Host "Downloading winutils.exe for Hadoop 3.3..." -ForegroundColor Yellow

$url = "https://github.com/steveloughran/winutils/raw/master/hadoop-3.3.0/bin/winutils.exe"

try {
    Invoke-WebRequest -Uri $url -OutFile $winutilsPath -ErrorAction Stop
    Write-Host "Successfully downloaded winutils.exe" -ForegroundColor Green
    Write-Host "Location: $winutilsPath" -ForegroundColor Gray
} catch {
    Write-Host "Failed to download automatically: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please download manually:" -ForegroundColor Yellow
    Write-Host "  1. Go to: https://github.com/steveloughran/winutils/tree/master/hadoop-3.3.0/bin" -ForegroundColor Cyan
    Write-Host "  2. Click on winutils.exe" -ForegroundColor Cyan
    Write-Host "  3. Click 'Download' button" -ForegroundColor Cyan
    Write-Host "  4. Save to: $winutilsPath" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Or create empty file (may cause issues):" -ForegroundColor Yellow
    Write-Host "  New-Item -ItemType File -Path '$winutilsPath' -Force" -ForegroundColor Gray
}

