# Quick Start Guide

## Prerequisites
- Minikube installed and running
- kubectl configured
- Docker installed
- At least 8GB RAM, 4 CPU cores available

## 1. Start Minikube

```bash
minikube start --memory=8192 --cpus=4
minikube addons enable ingress
```

## 2. Deploy Infrastructure (in order)

### Step 1: Deploy Kafka
```bash
kubectl apply -f kafka/deployment.yaml
kubectl wait --for=condition=ready kafka/kafka-cluster -n kafka --timeout=300s
kubectl apply -f kafka/topics.yaml
```

### Step 2: Deploy MinIO
```bash
kubectl apply -f minio/deployment.yaml
kubectl wait --for=condition=ready pod -l app=minio -n minio --timeout=120s
```

### Step 3: Deploy Cassandra
```bash
kubectl apply -f cassandra/deployment.yaml
kubectl wait --for=condition=ready pod -l app=cassandra --timeout=300s
```

### Step 4: Initialize Cassandra Schema
```bash
kubectl exec -it cassandra-0 -- cqlsh -e "
CREATE KEYSPACE IF NOT EXISTS education_platform 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
"
```

## 3. Deploy Application Layers

### Ingestion Layer (Producer)
```bash
# Build image in Minikube
eval $(minikube docker-env)
docker build -t producer:latest -f ingestion_layer/Dockerfile .

# Deploy
kubectl apply -f ingestion_layer/deployment.yaml
```

### Batch Layer (Spark Jobs)
```bash
kubectl apply -f spark/ingestion-job.yaml
```

### Speed Layer
```bash
# Build image
docker build -t speed-layer:latest -f speed_layer/Dockerfile .

# Deploy
kubectl apply -f speed_layer/deployment.yaml
```

### Serving Layer
```bash
# Build image
docker build -t serving-layer:latest -f serving_layer/Dockerfile .

# Deploy
kubectl apply -f serving_layer/deployment.yaml
```

## 4. Verify Deployment

```bash
# Check all pods are running
kubectl get pods --all-namespaces

# Expected output should show Running status for:
# - kafka-cluster-kafka-0,1,2 (kafka namespace)
# - minio-xxx (minio namespace)
# - cassandra-0 (default namespace)
# - producer-xxx (default namespace)
# - speed-layer-xxx (default namespace)
# - serving-layer-xxx (default namespace)
```

## 5. Access Services

### Get Minikube IP
```bash
MINIKUBE_IP=$(minikube ip)
echo "Minikube IP: $MINIKUBE_IP"
```

### Access URLs
- **Streamlit Dashboard**: `http://$MINIKUBE_IP:30001`
- **API Docs**: `http://$MINIKUBE_IP:30002/docs`
- **MinIO Console**: `http://$MINIKUBE_IP:30901` (minioadmin/minioadmin)

### Or Use Port Forwarding
```bash
# Serving Layer UI
kubectl port-forward svc/serving-layer 8501:8501

# Serving Layer API
kubectl port-forward svc/serving-layer 8000:8000

# MinIO Console
kubectl port-forward -n minio svc/minio 9001:9001
```

Then access:
- Dashboard: http://localhost:8501
- API: http://localhost:8000/docs
- MinIO: http://localhost:9001

## 6. Verify Data Flow

### Check Kafka Messages
```bash
kubectl exec -it kafka-cluster-kafka-0 -n kafka -- bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic auth_topic \
  --from-beginning \
  --max-messages 5
```

### Check Cassandra Data
```bash
kubectl exec -it cassandra-0 -- cqlsh -e "
USE education_platform;
SELECT * FROM batch_user_stats LIMIT 5;
SELECT * FROM realtime_user_activity LIMIT 5;
"
```

### Check Producer Logs
```bash
kubectl logs -f deployment/producer
```

### Check Speed Layer Logs
```bash
kubectl logs -f deployment/speed-layer
```

## 7. Test the Dashboard

1. Open `http://localhost:8501` (or `http://$MINIKUBE_IP:30001`)
2. You should see real-time metrics updating
3. Navigate through different analytics tabs
4. Verify charts are rendering with data

## 8. Test the API

```bash
# Health check
curl http://localhost:8000/health

# Get dashboard summary
curl http://localhost:8000/api/dashboard/summary | jq

# Get user stats (replace user_id)
curl http://localhost:8000/api/users/user_123 | jq
```

## Troubleshooting

### Pods Not Starting
```bash
# Check pod status
kubectl get pods --all-namespaces
kubectl describe pod <pod-name> -n <namespace>
kubectl logs <pod-name> -n <namespace>
```

### ImagePullBackOff Error
```bash
# Rebuild in Minikube context
eval $(minikube docker-env)
docker build -t <image-name>:latest .
kubectl delete pod <pod-name>
```

### Kafka Not Ready
```bash
# Check Kafka cluster status
kubectl get kafka -n kafka
kubectl logs kafka-cluster-kafka-0 -n kafka
```

### No Data in Dashboard
1. Verify producer is running: `kubectl logs deployment/producer`
2. Check Kafka has messages (see step 6)
3. Verify speed layer is processing: `kubectl logs deployment/speed-layer`
4. Check Cassandra has data (see step 6)

## Clean Up

```bash
# Delete all deployments
kubectl delete -f serving_layer/deployment.yaml
kubectl delete -f speed_layer/deployment.yaml
kubectl delete -f ingestion_layer/deployment.yaml
kubectl delete -f spark/ingestion-job.yaml
kubectl delete -f cassandra/deployment.yaml
kubectl delete -f minio/deployment.yaml
kubectl delete -f kafka/topics.yaml
kubectl delete -f kafka/deployment.yaml

# Or delete entire cluster
minikube delete
```

## Next Steps

- Read individual layer documentation in `/docs`
- Customize Kafka topic configurations
- Adjust Spark job resources
- Configure data retention policies
- Set up monitoring and alerts

## Useful Commands

```bash
# Watch all pods
watch kubectl get pods --all-namespaces

# Port forward all services (background)
./deployment/portforward-all.sh

# Stop port forwards
./deployment/stop-portforwards.sh

# Shell into Kafka
kubectl exec -it kafka-cluster-kafka-0 -n kafka -- /bin/bash

# Shell into Cassandra
kubectl exec -it cassandra-0 -- /bin/bash

# View logs for all producer pods
kubectl logs -f -l app=producer
```
