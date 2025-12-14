"""
Auth Batch Job - Authentication Analytics
Precomputes authentication-related batch views from raw events in MinIO
"""

import os
# Set Java options BEFORE importing PySpark
# This fixes the "getSubject is not supported" error on Windows with Java 17+
if "PYSPARK_SUBMIT_ARGS" not in os.environ:
    java_opts = "--add-opens=java.base/javax.security.auth=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED"
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--driver-java-options '{java_opts}' --conf spark.driver.extraJavaOptions='{java_opts}' --conf spark.executor.extraJavaOptions='{java_opts}' pyspark-shell"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, date_format, hour, 
    min as spark_min, max as spark_max, avg, 
    window, unix_timestamp, when, sum as spark_sum
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import sys
from datetime import datetime

def create_spark_session():
    """Initialize Spark Session with MinIO configuration"""
    import os
    # Use localhost:9002 for local execution, minio:9000 for Docker/K8s
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9002")
    
    # Get HADOOP_HOME from environment (set in run_batch_jobs.py)
    hadoop_home = os.getenv("HADOOP_HOME", "")
    
    spark_builder = SparkSession.builder \
        .appName("Auth_Batch_Job") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.driver.extraJavaOptions", "--add-opens=java.base/javax.security.auth=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED") \
        .config("spark.executor.extraJavaOptions", "--add-opens=java.base/javax.security.auth=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.hadoop.fs.defaultFS", "file:///")
    
    # Set HADOOP_HOME if available (to avoid winutils.exe error on Windows)
    if hadoop_home:
        spark_builder = spark_builder.config("spark.hadoop.hadoop.home.dir", hadoop_home)
    
    return spark_builder.getOrCreate()

def get_auth_schema():
    """Define schema for AUTH events"""
    return StructType([
        StructField("event_category", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("role", StringType(), True),
        StructField("timestamp", StringType(), False),
        StructField("session_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("registration_source", StringType(), True)
    ])

def compute_daily_active_users(df):
    """Compute daily active users (DAU)"""
    return df.filter(col("event_type") == "LOGIN") \
        .withColumn("date", date_format(col("timestamp_parsed"), "yyyy-MM-dd")) \
        .groupBy("date") \
        .agg(
            countDistinct("user_id").alias("daily_active_users"),
            countDistinct("session_id").alias("total_sessions")
        ) \
        .orderBy("date")

def compute_hourly_login_patterns(df):
    """Compute hourly login patterns for peak usage analysis"""
    return df.filter(col("event_type") == "LOGIN") \
        .withColumn("hour", hour(col("timestamp_parsed"))) \
        .withColumn("date", date_format(col("timestamp_parsed"), "yyyy-MM-dd")) \
        .groupBy("date", "hour") \
        .agg(count("user_id").alias("login_count")) \
        .orderBy("date", "hour")

def compute_user_session_metrics(df):
    """Compute session duration metrics per user"""
    # Get LOGIN and LOGOUT pairs
    logins = df.filter(col("event_type") == "LOGIN") \
        .select(
            col("user_id"),
            col("session_id"),
            col("timestamp_parsed").alias("login_time")
        )
    
    logouts = df.filter(col("event_type") == "LOGOUT") \
        .select(
            col("session_id"),
            col("timestamp_parsed").alias("logout_time")
        )
    
    # Join to calculate session duration
    sessions = logins.join(logouts, "session_id", "inner") \
        .withColumn(
            "session_duration_minutes",
            (unix_timestamp("logout_time") - unix_timestamp("login_time")) / 60
        )
    
    # Aggregate per user
    return sessions.groupBy("user_id") \
        .agg(
            count("session_id").alias("total_sessions"),
            avg("session_duration_minutes").alias("avg_session_duration_minutes"),
            spark_min("session_duration_minutes").alias("min_session_duration_minutes"),
            spark_max("session_duration_minutes").alias("max_session_duration_minutes"),
            spark_sum("session_duration_minutes").alias("total_time_online_minutes")
        ) \
        .orderBy(col("total_sessions").desc())

def compute_user_activity_summary(df):
    """Compute overall user activity summary"""
    return df.groupBy("user_id", "role") \
        .agg(
            count(when(col("event_type") == "LOGIN", 1)).alias("total_logins"),
            count(when(col("event_type") == "LOGOUT", 1)).alias("total_logouts"),
            spark_min("timestamp_parsed").alias("first_activity"),
            spark_max("timestamp_parsed").alias("last_activity"),
            countDistinct("session_id").alias("unique_sessions")
        ) \
        .orderBy(col("total_logins").desc())

def compute_registration_analytics(df):
    """Compute registration analytics"""
    signups = df.filter(col("event_type") == "SIGNUP")
    
    return signups.withColumn("date", date_format(col("timestamp_parsed"), "yyyy-MM-dd")) \
        .groupBy("date", "registration_source", "role") \
        .agg(count("user_id").alias("new_signups")) \
        .orderBy("date")

def main(input_path, output_path):
    """Main batch job execution"""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"[AUTH BATCH] Reading auth events from: {input_path}")
    
    # Read raw auth events from MinIO (partitioned by topic=auth_topic)
    df_raw = spark.read.parquet(f"{input_path}/topic=auth_topic")
    
    # Parse timestamp
    df = df_raw.withColumn("timestamp_parsed", col("timestamp").cast(TimestampType()))
    
    print(f"[AUTH BATCH] Total auth events: {df.count()}")
    
    # Compute batch views
    print("[AUTH BATCH] Computing daily active users...")
    dau = compute_daily_active_users(df)
    dau.write.mode("overwrite").parquet(f"{output_path}/auth_daily_active_users")
    
    print("[AUTH BATCH] Computing hourly login patterns...")
    hourly_patterns = compute_hourly_login_patterns(df)
    hourly_patterns.write.mode("overwrite").parquet(f"{output_path}/auth_hourly_login_patterns")
    
    print("[AUTH BATCH] Computing user session metrics...")
    session_metrics = compute_user_session_metrics(df)
    session_metrics.write.mode("overwrite").parquet(f"{output_path}/auth_user_session_metrics")
    
    print("[AUTH BATCH] Computing user activity summary...")
    activity_summary = compute_user_activity_summary(df)
    activity_summary.write.mode("overwrite").parquet(f"{output_path}/auth_user_activity_summary")
    
    print("[AUTH BATCH] Computing registration analytics...")
    registration_analytics = compute_registration_analytics(df)
    registration_analytics.write.mode("overwrite").parquet(f"{output_path}/auth_registration_analytics")
    
    print("[AUTH BATCH] Auth batch job completed successfully!")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: auth_batch_job.py <input_path> <output_path>")
        print("Example: auth_batch_job.py s3a://bucket-0/master_dataset s3a://bucket-0/batch_views")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    main(input_path, output_path)
