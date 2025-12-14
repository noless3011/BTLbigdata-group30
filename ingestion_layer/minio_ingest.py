import os
# Set Java options BEFORE importing PySpark
# This fixes the "getSubject is not supported" error on Windows with Java 17+
if "PYSPARK_SUBMIT_ARGS" not in os.environ:
    java_opts = "--add-opens=java.base/javax.security.auth=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED"
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--driver-java-options '{java_opts}' --conf spark.driver.extraJavaOptions='{java_opts}' --conf spark.executor.extraJavaOptions='{java_opts}' pyspark-shell"

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructType

# Initialize Spark with MinIO configuration
import os
# Use localhost:9002 for local execution, minio:9000 for Docker/K8s
minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9002")

spark = SparkSession.builder \
    .appName("KafkaToMinIO_Ingestion") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.driver.extraJavaOptions", "--add-opens=java.base/javax.security.auth=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED") \
    .config("spark.executor.extraJavaOptions", "--add-opens=java.base/javax.security.auth=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED") \
    .getOrCreate()

# Read from Kafka - Subscribe to all 6 event topics
# Use "localhost:9092" for local execution, "kafka:29092" for Docker/K8s execution
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "auth_topic,assessment_topic,video_topic,course_topic,profile_topic,notification_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Write Raw Data to MinIO (S3-compatible) using Checkpointing
# Store raw JSON events as-is, no transformation at ingestion layer
# Partitioned by topic for efficient querying in batch layer
query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "timestamp") \
    .writeStream \
    .format("parquet") \
    .option("path", "s3a://bucket-0/master_dataset/") \
    .option("checkpointLocation", "s3a://bucket-0/checkpoints/ingest/") \
    .partitionBy("topic") \
    .trigger(processingTime="1 minute") \
    .start()

print("="*60)
print("INGESTION LAYER - Kafka to MinIO")
print("="*60)
print("Topics: auth, assessment, video, course, profile, notification")
print("Destination: s3a://bucket-0/master_dataset/")
print("Checkpoint: s3a://bucket-0/checkpoints/ingest/")
print("Partitioning: By topic")
print("Trigger: Every 1 minute")
print("="*60)
print("Streaming started. Press Ctrl+C to stop.")
query.awaitTermination()

