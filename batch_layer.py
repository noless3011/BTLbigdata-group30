from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, date_format, from_json, avg, sum
from pyspark.sql.types import StructType, StructField, StringType

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("BatchLayer_EducationSystem") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .getOrCreate()

# Giả sử Raw Data được lưu trên HDFS theo ngày
# Đường dẫn HDFS: hdfs://namenode:8020/data/master_dataset/
raw_data_path = "hdfs://namenode:9000/data/master_dataset/"

# Schema for the JSON data inside the 'value' column
json_schema = StructType([
    StructField("user_id", StringType()),
    StructField("action", StringType()),
    StructField("timestamp", StringType()),
    StructField("assignment_id", StringType()),
    StructField("event_category", StringType()),
    StructField("video_id", StringType()),
    StructField("duration_watched_seconds", StringType()),  # Will cast to int later
    StructField("score", StringType())                      # Will cast to int later
])

# 1. ĐỌC IMMUTABLE MASTER DATA (Parquet từ HDFS)
print("Đang đọc Master Data từ HDFS...")
try:
    # Read Parquet (Partition discovery is automatic)
    df_raw = spark.read.parquet(raw_data_path)
    
    # Parse JSON content from 'value' column
    df_master = df_raw.withColumn("parsed_data", from_json(col("value"), json_schema)) \
                      .select("parsed_data.*")
except Exception as e:
    print(f"Lỗi đọc file: {e}")
    df_master = spark.createDataFrame([], schema=json_schema)

# 2. XỬ LÝ BATCH (PRECOMPUTE VIEWS)

# Ví dụ 1: Tổng hợp số lần nộp bài (Submit) theo từng Sinh viên
batch_view_assessment = df_master \
    .filter(col("action") == "SUBMIT_ASSIGNMENT") \
    .groupBy("user_id") \
    .agg(count("assignment_id").alias("total_submissions")) \
    .orderBy(desc("total_submissions"))

# Ví dụ 2: Thống kê hoạt động đăng nhập theo ngày
batch_view_auth = df_master \
    .filter(col("action") == "LOGIN") \
    .withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd")) \
    .groupBy("date") \
    .agg(count("user_id").alias("daily_active_users"))

# Ví dụ 3: Phân tích hiệu quả học tập (Join Video Engagement & Assessment Scores)
# Tính tổng thời gian xem video của mỗi sinh viên
video_engagement = df_master \
    .filter((col("event_category") == "VIDEO") & (col("action") == "STOP_VIDEO")) \
    .withColumn("duration", col("duration_watched_seconds").cast("int")) \
    .groupBy("user_id") \
    .agg(sum("duration").alias("total_watch_time_sec"))

# Tính điểm trung bình bài tập của mỗi sinh viên
student_performance = df_master \
    .filter((col("event_category") == "ASSESSMENT") & (col("action") == "GRADE_ASSIGNMENT")) \
    .withColumn("score_val", col("score").cast("int")) \
    .groupBy("user_id") \
    .agg(avg("score_val").alias("avg_score"))

# Join 2 metrics lại để xem tương quan
engagement_performance_join = video_engagement.join(student_performance, "user_id", "inner") \
    .orderBy(desc("avg_score"))

# 3. LƯU BATCH VIEWS (SERVING LAYER - PARQUET)
# Sơ đồ yêu cầu lưu dạng Parquet để Serving layer (Spark Query) có thể đọc nhanh
print("Đang ghi Batch Views xuống HDFS (Parquet)...")

batch_view_assessment.write \
    .mode("overwrite") \
    .parquet("hdfs://namenode:9000/data/batch_views/student_submissions")

batch_view_auth.write \
    .mode("overwrite") \
    .parquet("hdfs://namenode:9000/data/batch_views/daily_logins")

engagement_performance_join.write \
    .mode("overwrite") \
    .parquet("hdfs://namenode:9000/data/batch_views/engagement_performance")

print("Batch Job hoàn tất.")
spark.stop()