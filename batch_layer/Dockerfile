FROM python:3.9-slim-bookworm

# Install Java (Required for PySpark) and curl (for downloading JARs)
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless procps curl && \
    rm -rf /var/lib/apt/lists/*

# Fix JAVA_HOME by symlinking the installed version (works for both amd64 and arm64)
RUN ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-home
ENV JAVA_HOME=/usr/lib/jvm/java-home

WORKDIR /app

# Install Python dependencies
# Install Python dependencies
RUN pip install pyspark==3.5.0

# We use --packages in SparkSession builder to let Spark handle dependencies automatically
# This ensures we get the correct transitive dependencies (like java-driver-core for Cassandra)

# Copy application code
COPY batch_layer/ /app/batch_layer/

# Default command
CMD ["python", "batch_layer/run_batch_jobs.py"]
