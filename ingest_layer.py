from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructType

# Initialize Spark
spark = SparkSession.builder \
    .appName("KafkaToHDFS_Ingestion") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .getOrCreate()

# Read from Kafka
# Use "localhost:9092" for local execution, "kafka:29092" for Docker execution
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "course_topic,auth_topic,assessment_topic,video_topic,profile_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Write Raw Data to HDFS (Append mode) using Checkpointing
# We cast key and value to STRING to store them as raw text/json in Parquet
query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "timestamp") \
    .writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/data/master_dataset/") \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/ingest/") \
    .partitionBy("topic") \
    .trigger(processingTime="1 minute") \
    .start()

print("Ingestion Job Started...")
query.awaitTermination()
