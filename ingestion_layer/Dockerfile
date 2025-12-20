# Dockerfile for Ingestion Layer
# FROM apache/spark-py:latest
from apache/spark:3.5.4-python3


USER root

RUN curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.4.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.4/spark-sql-kafka-0-10_2.12-3.5.4.jar && \
    curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.4.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.4/spark-token-provider-kafka-0-10_2.12-3.5.4.jar && \
    curl -L -o /opt/spark/jars/kafka-clients-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    curl -L -o /opt/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.367.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar

# Install required Python packages
RUN pip install --no-cache-dir \
    pyspark==3.5.4 \
    py4j


# Copy ingestion script
COPY ingestion_layer/minio_ingest.py /opt/spark/work-dir/

# Set working directory
WORKDIR /opt/spark/work-dir

# Run ingestion script
CMD ["python3", "minio_ingest.py"]


# Ingestion Layer Spark Image
# FROM apache/spark:3.5.4-python3

# USER root

# # ---- Kafka + MinIO (S3A) dependencies ----
# RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
#       https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
#     curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.571.jar \
#       https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.571/aws-java-sdk-bundle-1.12.571.jar

# # ---- Copy ingestion script ----
# COPY ingestion_layer/minio_ingest.py /opt/spark/work-dir/

# WORKDIR /opt/spark/work-dir

# CMD ["/opt/spark/bin/spark-submit", "minio_ingest.py"]
