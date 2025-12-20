from pyspark.sql import SparkSession
import os

# =========================
# Environment configuration
# =========================
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio.minio.svc.cluster.local:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# =========================
# Spark session
# =========================
spark = (
    SparkSession.builder
    .appName("KafkaToMinIO_Ingestion")
    # MinIO / S3A configuration
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .getOrCreate()
)

# =========================
# Logging
# =========================
print("=" * 60)
print("KAFKA → MINIO INGESTION LAYER")
print("=" * 60)
print(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"MinIO endpoint: {MINIO_ENDPOINT}")
print("=" * 60)

# =========================
# Kafka source
# =========================
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option(
        "subscribe",
        "auth_topic,assessment_topic,video_topic,course_topic,profile_topic,notification_topic",
    )
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", 100)
    .load()
)

# =========================
# Write raw events to MinIO
# =========================
query = (
    df.selectExpr(
        "CAST(key AS STRING) AS key",
        "CAST(value AS STRING) AS value",
        "topic",
        "timestamp",
    )
    .writeStream
    .format("parquet")
    .option("path", "s3a://bucket-0/master_dataset/")
    .option("checkpointLocation", "s3a://bucket-0/checkpoints/ingest/")
    .partitionBy("topic")
    .trigger(processingTime="1 minute")
    .start()
)

print("=" * 60)
print("INGESTION LAYER STARTED")
print("Destination : s3a://bucket-0/master_dataset/")
print("Checkpoint  : s3a://bucket-0/checkpoints/ingest/")
print("Partition   : topic")
print("Trigger     : 1 minute")
print("=" * 60)

query.awaitTermination()
