# Dockerfile for Speed Layer
FROM apache/spark-py:v3.5.0

USER root

# Install required Python packages if needed (none strictly needed for core pyspark, but good practice)
# RUN pip install --no-cache-dir ...

# Copy application script
COPY stream_layer.py /opt/spark/work-dir/

# Set working directory
WORKDIR /opt/spark/work-dir

# Set default env vars (can be overridden by K8s)
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV MINIO_ENDPOINT=http://minio:9000

# Run the streaming job
CMD ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", "--master", "local[*]", "stream_layer.py"]
