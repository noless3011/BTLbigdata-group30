FROM python:3.9-slim

# Install Java (Required for PySpark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

# Install Python dependencies
RUN pip install pyspark==3.5.0

# Copy application code
COPY batch_layer/ /app/batch_layer/

# Default command
CMD ["python", "batch_layer/run_batch_jobs.py"]
