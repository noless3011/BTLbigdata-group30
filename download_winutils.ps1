# Script download winutils.exe cho Hadoop 3.3

$hadoopBin = Join-Path $env:TEMP "hadoop_home\bin"
if (-not (Test-Path $hadoopBin)) {
    New-Item -ItemType Directory -Force -Path $hadoopBin | Out-Null
}

$winutilsPath = Join-Path $hadoopBin "winutils.exe"

Write-Host "Downloading winutils.exe for Hadoop 3.3..." -ForegroundColor Yellow

# Try multiple URLs
$urls = @(
    "https://github.com/steveloughran/winutils/raw/master/hadoop-3.3.0/bin/winutils.exe",
    "https://raw.githubusercontent.com/steveloughran/winutils/master/hadoop-3.3.0/bin/winutils.exe"
)

$downloaded = $false
foreach ($url in $urls) {
    try {
        Write-Host "  Trying: $url" -ForegroundColor Gray
        Invoke-WebRequest -Uri $url -OutFile $winutilsPath -ErrorAction Stop
        Write-Host "✓ Downloaded winutils.exe to: $winutilsPath" -ForegroundColor Green
        $downloaded = $true
        break
    } catch {
        Write-Host "  Failed: $($_.Exception.Message)" -ForegroundColor Gray
    }
}

if (-not $downloaded) {
    Write-Host ""
    Write-Host "⚠ Could not download winutils.exe automatically" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please download manually:" -ForegroundColor Yellow
    Write-Host "  1. Go to: https://github.com/steveloughran/winutils/tree/master/hadoop-3.3.0/bin" -ForegroundColor Cyan
    Write-Host "  2. Download winutils.exe" -ForegroundColor Cyan
    Write-Host "  3. Save to: $winutilsPath" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Creating empty file as fallback..." -ForegroundColor Yellow
    echo "" | Out-File -FilePath $winutilsPath -Encoding ASCII
}

