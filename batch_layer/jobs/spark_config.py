
from pyspark.sql import SparkSession
import os
import glob

def create_spark_session(app_name="Batch_Job"):
    """Initialize Spark Session with MinIO configuration and S3 JARs"""
    
    # Debug: Check if JARs exist (to verify Docker build)
    print(f"DEBUG: Checking /app/jars content:")
    jars = glob.glob("/app/jars/*.jar")
    print(jars)
    
    # JAR paths
    jar_list = "/app/jars/hadoop-aws-3.3.4.jar,/app/jars/aws-java-sdk-bundle-1.12.262.jar"
    classpath = "/app/jars/hadoop-aws-3.3.4.jar:/app/jars/aws-java-sdk-bundle-1.12.262.jar"

    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT", "http://minio:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ACCESS_KEY", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_SECRET_KEY", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.jars", jar_list) \
        .config("spark.driver.extraClassPath", classpath) \
        .config("spark.executor.extraClassPath", classpath) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
