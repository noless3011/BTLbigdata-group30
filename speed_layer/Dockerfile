FROM python:3.9-slim-bookworm

# Install Java (Required for PySpark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*
    
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
# If no requirements.txt exists in local dir, we manually install essentials
RUN pip install pyspark==3.5.0 kafka-python

# Copy application code
COPY speed_layer/ /app/speed_layer/

# Set default command (can be overridden by K8s)
CMD ["python", "speed_layer/stream_layer.py"]
