# Script fix JAVA_HOME để trỏ đúng Java 17

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  FIX JAVA_HOME - Set to Java 17" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Tìm Java 17
$java17Path = "C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"

if (-not (Test-Path $java17Path)) {
    Write-Host "ERROR: Java 17 không tìm thấy tại $java17Path" -ForegroundColor Red
    Write-Host "Đang tìm Java 17..." -ForegroundColor Yellow
    
    # Tìm trong các thư mục khác
    $possiblePaths = @(
        "C:\Program Files\Eclipse Adoptium\jdk-17*",
        "C:\Program Files\Java\jdk-17*",
        "C:\Program Files (x86)\Java\jdk-17*"
    )
    
    $found = $false
    foreach ($pattern in $possiblePaths) {
        $paths = Get-ChildItem -Path (Split-Path $pattern) -Filter (Split-Path -Leaf $pattern) -Directory -ErrorAction SilentlyContinue
        if ($paths) {
            $java17Path = $paths[0].FullName
            Write-Host "Tìm thấy Java 17 tại: $java17Path" -ForegroundColor Green
            $found = $true
            break
        }
    }
    
    if (-not $found) {
        Write-Host "ERROR: Không tìm thấy Java 17. Vui lòng cài đặt Java 17 LTS." -ForegroundColor Red
        Write-Host "Download: https://adoptium.net/temurin/releases/?version=17" -ForegroundColor Yellow
        exit 1
    }
}

# Verify Java 17
Write-Host "Kiểm tra Java version..." -ForegroundColor Yellow
$javaExe = Join-Path $java17Path "bin\java.exe"
if (Test-Path $javaExe) {
    $version = & $javaExe -version 2>&1 | Select-String "version"
    Write-Host "Java version: $version" -ForegroundColor Green
} else {
    Write-Host "ERROR: java.exe không tìm thấy tại $javaExe" -ForegroundColor Red
    exit 1
}

# Set JAVA_HOME
Write-Host ""
Write-Host "Setting JAVA_HOME..." -ForegroundColor Yellow
$env:JAVA_HOME = $java17Path
Write-Host "JAVA_HOME = $env:JAVA_HOME" -ForegroundColor Green

# Update PATH để Java 17 được ưu tiên
Write-Host ""
Write-Host "Updating PATH..." -ForegroundColor Yellow
$javaBin = Join-Path $java17Path "bin"
if ($env:PATH -notlike "*$javaBin*") {
    $env:PATH = "$javaBin;$env:PATH"
    Write-Host "Đã thêm $javaBin vào PATH" -ForegroundColor Green
} else {
    Write-Host "PATH đã có $javaBin" -ForegroundColor Gray
}

# Verify
Write-Host ""
Write-Host "Verification..." -ForegroundColor Yellow
$currentJava = Get-Command java | Select-Object -ExpandProperty Source
$currentVersion = java -version 2>&1 | Select-String "version"
Write-Host "Java executable: $currentJava" -ForegroundColor Gray
Write-Host "Java version: $currentVersion" -ForegroundColor Gray

if ($currentVersion -match "17") {
    Write-Host ""
    Write-Host "✓ JAVA_HOME đã được set đúng!" -ForegroundColor Green
    Write-Host ""
    Write-Host "Để set vĩnh viễn (cho tất cả sessions):" -ForegroundColor Yellow
    Write-Host "  [System.Environment]::SetEnvironmentVariable('JAVA_HOME', '$java17Path', 'User')" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Hoặc set trong System Environment Variables:" -ForegroundColor Yellow
    Write-Host "  1. Mở System Properties > Environment Variables" -ForegroundColor Gray
    Write-Host "  2. Set JAVA_HOME = $java17Path" -ForegroundColor Gray
    Write-Host "  3. Thêm %JAVA_HOME%\bin vào PATH" -ForegroundColor Gray
} else {
    Write-Host ""
    Write-Host "⚠ Cảnh báo: Java version không phải 17" -ForegroundColor Yellow
    Write-Host "  Có thể PATH vẫn đang dùng Java khác" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Để test batch jobs:" -ForegroundColor Cyan
Write-Host "  . .\fix_java_home.ps1" -ForegroundColor White
Write-Host '  $env:MINIO_ENDPOINT = "http://localhost:9002"' -ForegroundColor White
Write-Host '  python batch_layer/run_batch_jobs.py auth s3a://bucket-0/master_dataset s3a://bucket-0/batch_views' -ForegroundColor White
Write-Host ""

