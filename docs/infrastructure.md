# Infrastructure

## Overview
The Lambda architecture is deployed on Kubernetes (Minikube) with supporting data services for storage, messaging, and persistence.

## Core Components

### Kubernetes Cluster
- **Platform**: Minikube (local development/testing)
- **Orchestration**: Kubernetes 1.28+
- **Namespaces**: `default`, `kafka`, `minio`, `spark`, `kube-system`

### Message Broker - Kafka (Strimzi)
- **Technology**: Apache Kafka via Strimzi Operator
- **Namespace**: `kafka`
- **Cluster Name**: `kafka-cluster`
- **Deployment**: `kafka/deployment.yaml`, `kafka/topics.yaml`

#### Kafka Ports
| Port | Type | Purpose |
|------|------|---------|
| 9092 | Internal | Plain listener (internal cluster) |
| 9093 | Internal | TLS listener |
| 9094 | External | NodePort listener |
| 30092 | NodePort | External bootstrap access |

#### Kafka Topics (6)
- `auth_topic`
- `assessment_topic`
- `video_topic`
- `course_topic`
- `profile_topic`
- `notification_topic`

### Object Storage - MinIO
- **Technology**: MinIO (S3-compatible)
- **Namespace**: `minio`
- **Deployment**: `minio/deployment.yaml`

#### MinIO Ports
| Port | NodePort | Purpose |
|------|----------|---------|
| 9000 | 30900 | S3 API |
| 9001 | 30901 | Web Console |

#### MinIO Credentials
```
Access Key: minioadmin
Secret Key: minioadmin
```

#### Storage Buckets
- `spark-data/auth/`
- `spark-data/assessments/`
- `spark-data/videos/`
- `spark-data/courses/`
- `spark-data/profiles/`
- `spark-data/notifications/`

### Database - Cassandra
- **Technology**: Apache Cassandra
- **Namespace**: `default`
- **Service**: `cassandra.default.svc`
- **Deployment**: `cassandra/deployment.yaml`

#### Cassandra Configuration
| Parameter | Value |
|-----------|-------|
| CQL Port | 9042 |
| Keyspace | `education_platform` |
| Replication | SimpleStrategy, RF=1 |

#### Table Categories
- **Batch Views**: Historical aggregations
- **Realtime Views**: Streaming updates (last 15-30 min)

### Compute - Apache Spark
- **Technology**: Spark on Kubernetes
- **Namespace**: `spark`
- **Operator**: Spark Operator
- **Deployment**: `spark/ingestion-job.yaml`

#### Spark Resources
```yaml
Driver: 1 core, 1GB memory
Executors: 2 instances, 1 core each, 1GB memory
```

## Network Architecture

### Internal Service Communication
```
Producer → Kafka (9092)
Kafka → Speed Layer → Cassandra (9042)
Batch Layer → MinIO (9000) → Cassandra (9042)
Serving Layer → Cassandra (9042)
```

### External Access Points
| Service | NodePort | Internal Port | Protocol |
|---------|----------|---------------|----------|
| Kafka Bootstrap | 30092 | 9094 | TCP |
| MinIO API | 30900 | 9000 | HTTP |
| MinIO Console | 30901 | 9001 | HTTP |
| Serving UI | 30001 | 8501 | HTTP |
| Serving API | 30002 | 8000 | HTTP |

## Deployment Commands

### Deploy All Infrastructure
```bash
# Kafka (Strimzi)
kubectl apply -f kafka/deployment.yaml
kubectl apply -f kafka/topics.yaml

# MinIO
kubectl apply -f minio/deployment.yaml

# Cassandra
kubectl apply -f cassandra/deployment.yaml

# Spark Operator (if needed)
kubectl apply -f spark/operator.yaml
```

### Verify Deployments
```bash
# Check all pods
kubectl get pods --all-namespaces

# Check services
kubectl get svc --all-namespaces

# Check Kafka cluster
kubectl get kafka -n kafka

# Check Spark applications
kubectl get sparkapplications -n spark
```

## Port Forwarding (Development)

### Manual Port Forwards
```bash
# Kafka
kubectl port-forward -n kafka svc/kafka-cluster-kafka-bootstrap 9092:9092

# MinIO
kubectl port-forward -n minio svc/minio 9000:9000
kubectl port-forward -n minio svc/minio 9001:9001

# Cassandra
kubectl port-forward svc/cassandra 9042:9042

# Serving Layer
kubectl port-forward svc/serving-layer 8501:8501
kubectl port-forward svc/serving-layer 8000:8000
```

### Automated Script
Use `deployment/portforward-all.sh` for background forwarding:
```bash
# Start all port forwards
./deployment/portforward-all.sh

# Stop all port forwards
./deployment/stop-portforwards.sh
```

## Resource Requirements

### Minimum System Resources
- **CPU**: 4 cores
- **Memory**: 8GB RAM
- **Disk**: 20GB free space

### Per-Component Resources
| Component | CPU | Memory | Replicas |
|-----------|-----|--------|----------|
| Kafka | 0.5 | 1Gi | 3 |
| MinIO | 0.5 | 512Mi | 1 |
| Cassandra | 1 | 2Gi | 1 |
| Speed Layer | 1 | 1Gi | 1 |
| Serving Layer | 0.5 | 512Mi | 1 |
| Spark Driver | 1 | 1Gi | 1 |
| Spark Executor | 1 | 1Gi | 2 |

## Monitoring & Troubleshooting

### Check Component Health
```bash
# Kafka
kubectl exec -it kafka-cluster-kafka-0 -n kafka -- bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# Cassandra
kubectl exec -it cassandra-0 -- cqlsh -e "DESCRIBE KEYSPACES;"

# MinIO
curl http://$(minikube ip):30900/minio/health/live

# Serving Layer
curl http://$(minikube ip):30001
```

### Common Issues

#### ImagePullBackOff / ErrImageNeverPull
```bash
# Build images in Minikube Docker
eval $(minikube docker-env)
docker build -t <image>:latest .
```

#### Pod CrashLoopBackOff
```bash
# Check logs
kubectl logs <pod-name> --previous
kubectl describe pod <pod-name>
```

#### Service Not Accessible
```bash
# Verify service endpoints
kubectl get endpoints <service-name>

# Check NodePort range
kubectl get svc -A | grep NodePort
```

## Persistence & Data Retention

### Data Lifecycle
1. **Kafka**: 7-day retention (configurable)
2. **MinIO**: Persistent (manual cleanup)
3. **Cassandra**: TTL-based cleanup on realtime views
4. **Batch Views**: Indefinite retention

### Backup Strategy
```bash
# MinIO backup
mc mirror minio/spark-data /backup/minio

# Cassandra snapshot
kubectl exec cassandra-0 -- nodetool snapshot education_platform
```

## Security Considerations

### Current Setup (Development)
- Default credentials (minioadmin/minioadmin)
- No TLS/SSL enabled
- No authentication on Kafka
- NodePort exposure on all interfaces

### Production Recommendations
- Enable Kafka SASL/SSL
- Configure MinIO with proper IAM policies
- Use Cassandra authentication
- Implement network policies
- Use Ingress with TLS certificates
- Secret management (Kubernetes Secrets/Vault)