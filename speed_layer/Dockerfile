FROM python:3.9-slim-bookworm

# Install Java and curl (Required for PySpark and JAR downloads)
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless procps curl && \
    rm -rf /var/lib/apt/lists/*
    
# Fix JAVA_HOME by symlinking the installed version (works for both amd64 and arm64)
RUN ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-home
ENV JAVA_HOME=/usr/lib/jvm/java-home

# Install Python dependencies
COPY speed_layer/requirements.txt .
# If no requirements.txt exists in local dir, we manually install essentials
RUN pip install pyspark==3.5.0 kafka-python

# Copy application code
COPY speed_layer/ /app/speed_layer/

# Download S3A JARs and commons-pool2 (Required for Spark Kafka)
RUN mkdir -p /app/jars && \
    curl -o /app/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o /app/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -o /app/jars/commons-pool2-2.11.1.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Set Spark extra classpath
ENV SPARK_EXTRA_CLASSPATH=/app/jars/hadoop-aws-3.3.4.jar:/app/jars/aws-java-sdk-bundle-1.12.262.jar:/app/jars/commons-pool2-2.11.1.jar

# Set Spark extra classpath
WORKDIR /app

# Set default command (can be overridden by K8s)
CMD ["python", "speed_layer/stream_layer.py"]
