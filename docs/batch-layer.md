# Batch Layer

## Overview
The batch layer processes historical data from MinIO using Apache Spark, creating precomputed views stored in Cassandra.

## Components

### Batch Processing Job (`batch_layer.py`)
- **Purpose**: Process large volumes of historical educational data
- **Technology**: PySpark on Kubernetes
- **Input**: Parquet files from MinIO (S3-compatible storage)
- **Output**: Aggregated views in Cassandra tables

## Data Processing

### Input Sources (MinIO)
- `s3a://spark-data/auth/`
- `s3a://spark-data/assessments/`
- `s3a://spark-data/videos/`
- `s3a://spark-data/courses/`
- `s3a://spark-data/profiles/`
- `s3a://spark-data/notifications/`

### Output Tables (Cassandra)
- User activity summaries
- Course engagement metrics
- Assessment performance analytics
- Video watch statistics

## Configuration

```yaml
# spark/ingestion-job.yaml
spec:
  driver:
    cores: 1
    memory: "1g"
  executor:
    cores: 1
    instances: 2
    memory: "1g"
```

## Deployment

```bash
# Apply Spark job
kubectl apply -f spark/ingestion-job.yaml

# Monitor job
kubectl get sparkapplications -n spark
kubectl logs -f <spark-driver-pod> -n spark

# Check job status
kubectl describe sparkapplication batch-processing -n spark
```

## MinIO Integration

```bash
# MinIO credentials (configured in job spec)
AWS_ACCESS_KEY_ID: minioadmin
AWS_SECRET_ACCESS_KEY: minioadmin
S3_ENDPOINT: http://minio.minio.svc:9000
```

## Key Features
- Distributed data processing with Spark
- Fault-tolerant batch computations
- Scheduled periodic execution
- Scalable worker pools
- S3-compatible storage integration

## Typical Workflow
1. Data accumulates in MinIO buckets
2. Spark job reads Parquet files
3. Aggregations and transformations applied
4. Results written to Cassandra batch views
5. Serving layer queries precomputed views