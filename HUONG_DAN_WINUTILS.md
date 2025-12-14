# Hướng Dẫn Fix Lỗi winutils.exe trên Windows

## Vấn đề

Khi chạy PySpark trên Windows, bạn gặp lỗi:
```
Could not locate Hadoop executable: ...\winutils.exe
```

## Giải pháp

### Cách 1: Download winutils.exe (Khuyến nghị)

1. **Download winutils.exe:**
   - Truy cập: https://github.com/steveloughran/winutils/tree/master/hadoop-3.3.0/bin
   - Click vào file `winutils.exe`
   - Click nút "Download" (raw file)
   - Lưu file vào: `C:\Users\<YourUser>\AppData\Local\Temp\hadoop_home\bin\winutils.exe`

2. **Hoặc dùng script tự động:**
   ```powershell
   .\setup_winutils.ps1
   ```

### Cách 2: Tạo winutils.exe stub (Workaround)

Nếu không download được, có thể tạo file stub (có thể không hoạt động hoàn hảo):

```powershell
$hadoopBin = Join-Path $env:TEMP "hadoop_home\bin"
New-Item -ItemType Directory -Force -Path $hadoopBin | Out-Null
New-Item -ItemType File -Path (Join-Path $hadoopBin "winutils.exe") -Force | Out-Null
```

### Cách 3: Bỏ qua winutils (Không khuyến nghị)

Code đã tự động tạo HADOOP_HOME dummy, nhưng vẫn cần winutils.exe thật để Spark chạy chmod operations.

## Lưu ý

- winutils.exe cần phải là file executable thật, không phải file rỗng
- File size thường khoảng 100-200 KB
- Nếu download không được, có thể do network hoặc GitHub rate limit
- Thử download thủ công hoặc dùng VPN/proxy

## Kiểm tra

Sau khi có winutils.exe, test lại:
```powershell
.\test_batch_with_java17.ps1
```

