# Lambda Architecture Documentation

## Overview
This directory contains comprehensive documentation for the educational platform analytics system built using Lambda Architecture on Kubernetes.

## Architecture Summary

```
┌─────────────────────────────────────────────────────────────────┐
│                        LAMBDA ARCHITECTURE                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Ingestion Layer                                                 │
│  ┌──────────┐                                                    │
│  │ Producer │──────▶ Kafka Topics (6)                           │
│  └──────────┘                                                    │
│                          │                                        │
│                          ├─────────────┬─────────────┐           │
│                          ▼             ▼             ▼           │
│                    Speed Layer    Batch Layer   (Archive)        │
│                    ┌──────────┐  ┌──────────┐  ┌──────────┐    │
│                    │  Spark   │  │  Spark   │  │  MinIO   │    │
│                    │Streaming │  │  Batch   │  │ Storage  │    │
│                    └────┬─────┘  └────┬─────┘  └──────────┘    │
│                         │             │                          │
│                         ▼             ▼                          │
│                    ┌─────────────────────────┐                  │
│                    │      Cassandra DB        │                  │
│                    │  ┌──────────────────┐   │                  │
│                    │  │ Realtime Views   │   │                  │
│                    │  │ Batch Views      │   │                  │
│                    │  └──────────────────┘   │                  │
│                    └──────────┬──────────────┘                  │
│                               │                                  │
│                               ▼                                  │
│                    ┌─────────────────────────┐                  │
│                    │   Serving Layer         │                  │
│                    │  ┌────────┬──────────┐  │                  │
│                    │  │FastAPI │Streamlit │  │                  │
│                    │  └────────┴──────────┘  │                  │
│                    └─────────────────────────┘                  │
│                               │                                  │
│                               ▼                                  │
│                         End Users                                │
└─────────────────────────────────────────────────────────────────┘
```

## Documentation Files

### 📚 Core Layers
- **[ingestion-layer.md](ingestion-layer.md)** - Real-time data generation and Kafka producer
- **[batch-layer.md](batch-layer.md)** - Historical data processing with Spark
- **[speed-layer.md](speed-layer.md)** - Real-time stream processing
- **[serving-layer.md](serving-layer.md)** - API and dashboard for data access

### 🏗️ Infrastructure
- **[infrastructure.md](infrastructure.md)** - Kafka, MinIO, Cassandra, Kubernetes setup
- **[quick-start.md](quick-start.md)** - Step-by-step deployment guide

## Quick Reference

### Key Technologies
| Component | Technology | Purpose |
|-----------|------------|---------|
| Orchestration | Kubernetes (Minikube) | Container orchestration |
| Messaging | Apache Kafka (Strimzi) | Event streaming |
| Storage | MinIO (S3) | Object storage |
| Database | Apache Cassandra | Time-series + analytics |
| Batch Processing | Apache Spark | Historical aggregations |
| Stream Processing | Spark Structured Streaming | Real-time processing |
| API | FastAPI | REST endpoints |
| Dashboard | Streamlit | Interactive UI |

### Service Ports
| Service | Internal | NodePort | Access |
|---------|----------|----------|--------|
| Kafka Bootstrap | 9092 | 30092 | Message broker |
| MinIO API | 9000 | 30900 | S3 storage |
| MinIO Console | 9001 | 30901 | Web UI |
| Cassandra CQL | 9042 | - | Database |
| Serving API | 8000 | 30002 | REST API |
| Serving UI | 8501 | 30001 | Dashboard |

### Kafka Topics
1. `auth_topic` - Authentication events
2. `assessment_topic` - Quiz/test submissions
3. `video_topic` - Video playback tracking
4. `course_topic` - Course enrollments
5. `profile_topic` - User profile changes
6. `notification_topic` - System notifications

## Getting Started

### 1️⃣ First Time Setup
```bash
# Start cluster
minikube start --memory=8192 --cpus=4

# Deploy infrastructure (order matters!)
kubectl apply -f kafka/deployment.yaml
kubectl apply -f kafka/topics.yaml
kubectl apply -f minio/deployment.yaml
kubectl apply -f cassandra/deployment.yaml
```

### 2️⃣ Deploy Application
```bash
# Build images in Minikube
eval $(minikube docker-env)
docker build -t producer:latest -f ingestion_layer/Dockerfile .
docker build -t speed-layer:latest -f speed_layer/Dockerfile .
docker build -t serving-layer:latest -f serving_layer/Dockerfile .

# Deploy layers
kubectl apply -f ingestion_layer/deployment.yaml
kubectl apply -f speed_layer/deployment.yaml
kubectl apply -f serving_layer/deployment.yaml
kubectl apply -f spark/ingestion-job.yaml
```

### 3️⃣ Access Services
```bash
# Get Minikube IP
MINIKUBE_IP=$(minikube ip)

# Open dashboard
open http://$MINIKUBE_IP:30001

# View API docs
open http://$MINIKUBE_IP:30002/docs
```

See **[quick-start.md](quick-start.md)** for detailed instructions.

## Data Flow

### Real-Time Path (Speed Layer)
1. Producer generates events → Kafka topics
2. Speed Layer consumes streams → processes in windows
3. Results written to Cassandra realtime views
4. Serving Layer queries latest data

### Batch Path (Batch Layer)
1. Events archived to MinIO (Parquet files)
2. Spark batch jobs read historical data
3. Aggregations computed and stored in Cassandra batch views
4. Serving Layer merges batch + realtime views

## Common Tasks

### View Logs
```bash
kubectl logs -f deployment/producer          # Producer activity
kubectl logs -f deployment/speed-layer       # Stream processing
kubectl logs -f deployment/serving-layer     # API/dashboard
```

### Check Data
```bash
# Kafka messages
kubectl exec -it kafka-cluster-kafka-0 -n kafka -- \
  bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic auth_topic --max-messages 5

# Cassandra tables
kubectl exec -it cassandra-0 -- cqlsh -e \
  "USE education_platform; SELECT * FROM realtime_user_activity LIMIT 5;"
```

### Debug Issues
```bash
kubectl get pods --all-namespaces              # Check pod status
kubectl describe pod <pod-name>                 # Pod details
kubectl logs <pod-name> --previous              # Previous crash logs
```

## Architecture Benefits

✅ **Fault Tolerance** - Multiple data paths ensure availability  
✅ **Scalability** - Independent scaling of batch and speed layers  
✅ **Low Latency** - Speed layer provides real-time insights  
✅ **Accuracy** - Batch layer provides comprehensive historical views  
✅ **Flexibility** - Easy to add new analytics or data sources  

## Performance Characteristics

| Metric | Speed Layer | Batch Layer |
|--------|-------------|-------------|
| Latency | < 5 seconds | Hours |
| Completeness | Last 15-30 min | All history |
| Accuracy | Approximate | Exact |
| Update Frequency | Continuous | Scheduled |

## Further Reading

- **[Infrastructure Details](infrastructure.md)** - Network, storage, monitoring
- **[Layer Documentation](ingestion-layer.md)** - Deep dives into each component
- **[Quick Start](quick-start.md)** - Deployment walkthrough
- [Apache Kafka](https://kafka.apache.org/documentation/)
- [Apache Spark](https://spark.apache.org/docs/latest/)
- [Apache Cassandra](https://cassandra.apache.org/doc/latest/)

## Project Structure
```
BTLbigdata-group30/
├── docs/                    # 📖 This documentation
├── ingestion_layer/         # Producer code & deployment
├── batch_layer/             # Spark batch jobs
├── speed_layer/             # Spark streaming jobs
├── serving_layer/           # API + dashboard
├── kafka/                   # Kafka cluster & topics
├── minio/                   # Object storage
├── cassandra/               # Database deployment
└── spark/                   # Spark operator & jobs
```

## Support & Troubleshooting

For deployment issues, see:
- **[Quick Start Guide](quick-start.md)** - Troubleshooting section
- **[Infrastructure Doc](infrastructure.md)** - Common issues & solutions

For architecture questions:
- Review individual layer documentation
- Check Kafka/Cassandra/Spark official docs
- Examine deployment YAML configurations