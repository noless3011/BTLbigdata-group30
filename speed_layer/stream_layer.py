import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    approx_count_distinct,
    col,
    count,
    current_timestamp,
    from_json,
    window,
)
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Configuration from Environment Variables
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
CASSANDRA_HOST = "cassandra.cassandra.svc.cluster.local"
CASSANDRA_PORT = "9042"
CASSANDRA_KEYSPACE = "university_analytics"

# Initialize Spark Session
jar_list = "/app/jars/hadoop-aws-3.3.4.jar,/app/jars/aws-java-sdk-bundle-1.12.262.jar"
classpath = "/app/jars/hadoop-aws-3.3.4.jar:/app/jars/aws-java-sdk-bundle-1.12.262.jar"

spark = (
    SparkSession.builder.appName("SpeedLayer_RealTimeProcessing")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0",
    )
    # Cassandra Configuration
    .config("spark.cassandra.connection.host", CASSANDRA_HOST)
    .config("spark.cassandra.connection.port", CASSANDRA_PORT)
    .config("spark.jars", jar_list)
    # Streaming Optimization: Disable Adaptive Query Execution for Streams to stop warnings
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.driver.extraClassPath", classpath)
    .config("spark.executor.extraClassPath", classpath)
    .config("spark.sql.shuffle.partitions", "20")
    .config("spark.default.parallelism", "20")
    # S3/MinIO Configuration
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Define Schema
common_schema = StructType(
    [
        # Common
        StructField("event_category", StringType()),
        StructField("event_type", StringType()),
        StructField("user_id", StringType()),
        StructField("role", StringType()),
        StructField("timestamp", StringType()),
        # Course
        StructField("course_id", StringType()),
        # Video
        StructField("video_id", StringType()),
        StructField("watch_duration_seconds", IntegerType()),
        # ... (Other fields can remain if needed, trimmed for brevity based on usage)
    ]
)

# 1. READ FROM KAFKA
topics = "auth_topic,assessment_topic,video_topic,course_topic,profile_topic,notification_topic"

df_kafka = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", topics)
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "latest")
    .load()
)

# Parse JSON and Handle Timestamp
df_parsed = (
    df_kafka.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), common_schema).alias("data"))
    .select("data.*")
    .withColumn("timestamp", col("timestamp").cast("timestamp"))
    .withColumn("processing_time", current_timestamp())
)

# Define Watermark
df_watermarked = df_parsed.withWatermark("timestamp", "1 minute")

# 2. DEFINE AGGREGATIONS

# Aggregation 1: Active Users
rt_active_users = (
    df_watermarked.filter(col("event_type") == "LOGIN")
    .groupBy(window(col("timestamp"), "1 minutes", "30 seconds"))
    .agg(approx_count_distinct("user_id").alias("active_users"))
    .select(
        col("window.start").alias("start"),
        col("window.end").alias("end"),
        "active_users",
    )
)

# Aggregation 2: Course Popularity
rt_course_popularity = (
    df_watermarked.filter(col("course_id").isNotNull())
    .groupBy(window(col("timestamp"), "1 minutes", "1 minutes"), col("course_id"))
    .agg(count("*").alias("interactions"))
    .select(
        col("window.start").alias("start"),
        col("window.end").alias("end"),
        "course_id",
        "interactions",
    )
)

# Aggregation 3: Video Engagement
rt_video_engagement = (
    df_watermarked.filter(
        (col("event_category") == "VIDEO") & (col("event_type") == "VIDEO_WATCHED")
    )
    .groupBy(window(col("timestamp"), "1 minutes", "1 minutes"), col("video_id"))
    .agg(count("*").alias("views"))
    .select(
        col("window.start").alias("start"),
        col("window.end").alias("end"),
        "video_id",
        "views",
    )
)

# 3. DUAL WRITE FUNCTION (MINIO + CASSANDRA)


def write_to_dual_storage(batch_df, epoch_id, s3_folder, cassandra_table):
    """
    Writes a micro-batch to both MinIO (Parquet) and Cassandra.
    """
    # Cache the dataframe to prevent re-computing the aggregation for the second write
    batch_df.persist()

    try:
        print(
            f"[{epoch_id}] Writing to MinIO: {s3_folder} & Cassandra: {cassandra_table}"
        )

        # 1. Write to MinIO
        s3_path = f"s3a://bucket-0/speed_views/{s3_folder}"
        batch_df.write.mode("append").parquet(s3_path)

        # 2. Write to Cassandra
        # Ensure column names in batch_df match Cassandra table columns exactly
        batch_df.write.format("org.apache.spark.sql.cassandra").options(
            table=cassandra_table, keyspace=CASSANDRA_KEYSPACE
        ).mode("append").save()

    except Exception as e:
        print(f"Error in batch {epoch_id}: {str(e)}")
    finally:
        # Important: Unpersist to free up memory
        batch_df.unpersist()


# 4. START STREAMS

# Stream 1: Active Users
query_dau = (
    rt_active_users.writeStream.foreachBatch(
        lambda df, epoch: write_to_dual_storage(
            df, epoch, "active_users", "active_users"
        )
    )
    .option("checkpointLocation", "s3a://bucket-0/checkpoints/active_users")
    .trigger(processingTime="30 seconds")
    .start()
)

# Stream 2: Course Popularity
query_course = (
    rt_course_popularity.writeStream.foreachBatch(
        lambda df, epoch: write_to_dual_storage(
            df, epoch, "course_popularity", "course_popularity"
        )
    )
    .option("checkpointLocation", "s3a://bucket-0/checkpoints/course_popularity")
    .trigger(processingTime="30 seconds")
    .start()
)

# Stream 3: Video Engagement
query_video = (
    rt_video_engagement.writeStream.foreachBatch(
        lambda df, epoch: write_to_dual_storage(
            df, epoch, "video_engagement", "video_engagement"
        )
    )
    .option("checkpointLocation", "s3a://bucket-0/checkpoints/video_engagement")
    .trigger(processingTime="30 seconds")
    .start()
)

print("Speed Layer Streams Started with Dual Write (MinIO + Cassandra)...")
print(f"Active Queries: {len(spark.streams.active)}")
spark.streams.awaitAnyTermination()
