# Hướng Dẫn Chạy Pipeline Big Data

## Tổng Quan

Project này sử dụng **Lambda Architecture** với các layer:
1. **Ingestion Layer**: Kafka → MinIO (Raw Events)
2. **Batch Layer**: Spark Batch → 37 Batch Views
3. **Speed Layer**: Spark Streaming (đang phát triển)
4. **Serving Layer**: Query Interface (đang phát triển)

---

## Cách 1: Chạy Tự Động

### Script tự động chạy toàn bộ pipeline:

```powershell
.\run_full_pipeline.ps1
```

Script này sẽ:
1. ✅ Kiểm tra Docker và Python
2. ✅ Khởi động infrastructure (Kafka, MinIO, Spark, HDFS, MongoDB)
3. ✅ Tạo Kafka topics và MinIO bucket
4. ✅ Chạy Producer để tạo events (60 giây)
5. ✅ Chạy Ingestion để đưa data vào MinIO (2 phút)
6. ✅ Chạy Batch Processing để tạo 37 batch views

**Thời gian**: ~5-10 phút tùy máy

---

## Cách 2: Chạy Từng Bước (Để Debug)

### Bước 1: Khởi động Infrastructure

```powershell
cd config
docker-compose up -d
```

Đợi 30-60 giây để các services khởi động hoàn toàn.

**Kiểm tra services:**
- Kafka: `docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list`
- MinIO: Mở http://localhost:9001 (minioadmin/minioadmin)
- Spark: http://localhost:8080

**Tạo Kafka topics:**
```powershell
$topics = @("auth_topic", "assessment_topic", "video_topic", "course_topic", "profile_topic", "notification_topic")
foreach ($topic in $topics) {
    docker exec kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic $topic --partitions 3 --replication-factor 1
}
```

**Tạo MinIO bucket:**
```powershell
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec minio mc mb local/bucket-0 --ignore-existing
```

### Bước 2: Chạy Producer

```powershell
python ingestion_layer/producer.py
```

Producer sẽ tạo events và gửi vào Kafka. Nhấn `Ctrl+C` để dừng (chạy ít nhất 30-60 giây để có đủ data).

### Bước 3: Chạy Ingestion (Kafka → MinIO)

**Lưu ý**: Ingestion chạy streaming, sẽ chạy liên tục. Có 2 cách:

**Cách A: Chạy và dừng thủ công**
```powershell
python ingestion_layer/minio_ingest.py
# Đợi 2-3 phút để ingest data, sau đó nhấn Ctrl+C
```

**Cách B: Chạy với timeout (tự động dừng sau 2 phút)**
```powershell
# Tạo script tạm
@"
import time
import subprocess
import sys
proc = subprocess.Popen([sys.executable, 'ingestion_layer/minio_ingest.py'])
time.sleep(120)  # 2 phút
proc.terminate()
proc.wait()
"@ | Out-File -FilePath temp_ingest.py -Encoding utf8

python temp_ingest.py
Remove-Item temp_ingest.py
```

**Kiểm tra data trong MinIO:**
- Mở http://localhost:9001
- Vào bucket `bucket-0`
- Xem thư mục `master_dataset/` với các partition theo topic

### Bước 4: Chạy Batch Processing

```powershell
# Chạy tất cả batch jobs
python batch_layer/run_batch_jobs.py s3a://bucket-0/master_dataset s3a://bucket-0/batch_views

# Hoặc chạy từng job riêng
python batch_layer/run_batch_jobs.py auth s3a://bucket-0/master_dataset s3a://bucket-0/batch_views
python batch_layer/run_batch_jobs.py assessment s3a://bucket-0/master_dataset s3a://bucket-0/batch_views
python batch_layer/run_batch_jobs.py video s3a://bucket-0/master_dataset s3a://bucket-0/batch_views
python batch_layer/run_batch_jobs.py course s3a://bucket-0/master_dataset s3a://bucket-0/batch_views
python batch_layer/run_batch_jobs.py profile_notification s3a://bucket-0/master_dataset s3a://bucket-0/batch_views
```

**Kết quả**: 37 batch views sẽ được tạo trong `s3a://bucket-0/batch_views/`

---

## Cách 3: Test Từng Component

Sử dụng script test:

```powershell
# Test tất cả
.\test_step_by_step.ps1

# Test từng bước
.\test_step_by_step.ps1 -Step infra
.\test_step_by_step.ps1 -Step producer
.\test_step_by_step.ps1 -Step ingestion
.\test_step_by_step.ps1 -Step batch
```

---

## Kiểm Tra Kết Quả

### 1. MinIO Console
- URL: http://localhost:9001
- Username: `minioadmin`
- Password: `minioadmin`
- Bucket: `bucket-0`
  - `master_dataset/` - Raw events từ Kafka
  - `batch_views/` - 37 batch views đã xử lý

### 2. Spark UI
- URL: http://localhost:8080
- Xem job progress, stages, tasks

### 3. Kafka Topics
```powershell
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic auth_topic --from-beginning --max-messages 5
```

### 4. Kiểm tra Batch Views
```powershell
# Sử dụng MinIO client (mc) hoặc MinIO Console
docker exec minio mc ls local/bucket-0/batch_views/ --recursive
```

---

## Troubleshooting

### Lỗi: Cannot connect to Kafka
```powershell
# Kiểm tra Kafka đang chạy
docker ps | Select-String kafka

# Restart Kafka
docker restart kafka
```

### Lỗi: MinIO access denied
- Kiểm tra credentials: `minioadmin/minioadmin`
- Kiểm tra endpoint: `http://localhost:9000` (local) hoặc `http://minio:9000` (Docker)

### Lỗi: Spark job fails với OOM
- Tăng memory trong `batch_layer/config.py`:
  ```python
  "executor_memory": "4g",  # Tăng từ 2g
  "driver_memory": "2g"     # Tăng từ 1g
  ```

### Lỗi: No data in MinIO
1. Kiểm tra Producer đã chạy và gửi events
2. Kiểm tra Ingestion đã chạy đủ lâu (ít nhất 2 phút)
3. Kiểm tra Kafka có data:
   ```powershell
   docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic auth_topic --from-beginning --max-messages 1
   ```

### Lỗi: Python dependencies missing
```powershell
pip install -r config/requirements.txt
```

---

## Dừng Infrastructure

```powershell
cd config
docker-compose down
```

Để xóa data (bắt đầu lại từ đầu):
```powershell
cd config
docker-compose down -v
# Xóa thư mục data
Remove-Item -Recurse -Force ..\hdfs_data\*, ..\mongo_data\*, ..\minio_data\* -ErrorAction SilentlyContinue
```

---

## Cấu Trúc Data

```
bucket-0/
├── master_dataset/              # Raw events từ Kafka
│   ├── topic=auth_topic/
│   ├── topic=assessment_topic/
│   ├── topic=video_topic/
│   ├── topic=course_topic/
│   ├── topic=profile_topic/
│   └── topic=notification_topic/
│
├── batch_views/                  # 37 batch views
│   ├── auth_daily_active_users/
│   ├── auth_hourly_login_patterns/
│   ├── assessment_student_submissions/
│   ├── video_total_watch_time/
│   └── ... (33 views khác)
│
└── checkpoints/                  # Spark checkpoints
    ├── ingest/
    └── batch/
```

---

## Next Steps

Sau khi chạy thành công:
1. ✅ Kiểm tra data trong MinIO Console
2. ✅ Xem Spark UI để monitor jobs
3. ✅ Phát triển Speed Layer (real-time streaming)
4. ✅ Phát triển Serving Layer (query interface)

---

## Liên Hệ

Nếu gặp vấn đề, kiểm tra:
- Logs: `docker logs <container_name>`
- README files trong từng layer directory
- Documentation trong `docs/`

