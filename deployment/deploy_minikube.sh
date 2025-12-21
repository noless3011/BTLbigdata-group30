#!/bin/bash
# System Configuration
export TZ=Asia/Ho_Chi_Minh

# Quick Start Script for Minikube Testing on Linux
# Run this script to deploy everything at once

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${CYAN}================================${NC}"
echo -e "${CYAN}MINIKUBE KAFKA INGESTION TEST   ${NC}"
echo -e "${CYAN}================================${NC}"
echo ""

# Step 1: Start Minikube
echo -e "${YELLOW}[1/10] Starting Minikube cluster (with upgraded resources)...${NC}"
minikube start --cpus=6 --memory=12288 --driver=docker
if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Failed to start Minikube${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Minikube started${NC}"
echo ""

# Step 2: Create namespaces
echo -e "${YELLOW}[2/10] Creating namespaces...${NC}"
kubectl create namespace kafka --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace minio --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace cassandra --dry-run=client -o yaml | kubectl apply -f -
echo -e "${GREEN}✅ Namespaces created${NC}"
echo ""

# Step 3: Install Strimzi operator
echo -e "${YELLOW}[3/10] Installing Strimzi Kafka Operator...${NC}"
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
sleep 5
kubectl wait deployment/strimzi-cluster-operator --for=condition=Available --timeout=300s -n kafka
echo -e "${GREEN}✅ Strimzi operator ready${NC}"
echo ""

# Step 4: Create storage
echo -e "${YELLOW}[4/10] Setting up storage...${NC}"
minikube ssh "sudo mkdir -p /mnt/kafka-data/0 /mnt/kafka-data/1 /mnt/kafka-data/2 && sudo chmod -R 777 /mnt/kafka-data"
kubectl apply -f kafka/storage-class.yaml -n kafka
kubectl apply -f kafka/persistent-volumn-minikube.yaml -n kafka
echo -e "${GREEN}✅ Storage configured${NC}"
echo ""

# Step 5: Deploy Kafka
echo -e "${YELLOW}[5/10] Deploying Kafka cluster (this takes 3-5 minutes)...${NC}"
kubectl apply -f kafka/deployment.yaml -n kafka
kubectl wait kafka/kafka-cluster --for=condition=Ready --timeout=1200s -n kafka
echo -e "${GREEN}✅ Kafka cluster ready${NC}"
echo ""

# Step 6: Create topics
echo -e "${YELLOW}[6/10] Creating Kafka topics...${NC}"
kubectl apply -f kafka/topics.yaml -n kafka
sleep 10
echo -e "${GREEN}✅ Topics created${NC}"
echo ""

# Step 7: Deploy MinIO
echo -e "${YELLOW}[7/10] Deploying MinIO...${NC}"
kubectl apply -f minio/deployment.yaml -n minio
kubectl wait deployment/minio --for=condition=Available --timeout=300s -n minio
echo -e "${GREEN}✅ MinIO ready${NC}"
echo ""

# Step 8: Setup MinIO bucket
echo -e "${YELLOW}[8/10] Configuring MinIO bucket...${NC}"
export MINIKUBE_IP=$(minikube ip)
export MINIO_NODE_URL=http://$MINIKUBE_IP:30900

# Configure mc and create bucket
if command -v mc &> /dev/null; then
    mc alias set minikube $MINIO_NODE_URL minioadmin minioadmin
    mc mb minikube/bucket-0 --ignore-existing
    echo -e "${YELLOW}Cleaning up old checkpoints...${NC}"
    mc rm --recursive --force minikube/bucket-0/checkpoints/ingest/ 2>/dev/null || true
else
    echo -e "${YELLOW}⚠️ 'mc' client not found locally. Trying to use docker run...${NC}"
    docker run --network host --entrypoint /bin/sh minio/mc -c "mc alias set minikube $MINIO_NODE_URL minioadmin minioadmin; mc mb minikube/bucket-0; mc rm --recursive --force minikube/bucket-0/checkpoints/ingest/"
fi
echo -e "${GREEN}✅ MinIO bucket prepared (and checkpoints cleared)${NC}"
echo ""

# Step 9: Deploy Cassandra
echo -e "${YELLOW}[9/14] Deploying Cassandra...${NC}"
kubectl apply -f cassandra/deployment.yaml
kubectl wait statefulset/cassandra --for=jsonpath='{.status.readyReplicas}'=1 --timeout=300s -n cassandra
echo -e "${GREEN}✅ Cassandra ready${NC}"
echo ""

# Step 10: Initialize Cassandra Schema
echo -e "${YELLOW}[10/14] Initializing Cassandra schema...${NC}"
echo "   Waiting for Cassandra to be fully ready (30 seconds)..."
sleep 30

# Port forward Cassandra temporarily for schema initialization
kubectl port-forward service/cassandra 9042:9042 -n cassandra &
CASSANDRA_PF_PID=$!
sleep 5

# Initialize schema
export CASSANDRA_HOST=localhost
export CASSANDRA_PORT=9042
./venv/bin/python3 serving_layer/init_cassandra_schema.py

# Kill port-forward
kill $CASSANDRA_PF_PID 2>/dev/null || true
echo -e "${GREEN}✅ Cassandra schema initialized${NC}"
echo ""

# Step 11: Build Docker Images
echo -e "${YELLOW}[11/14] Building Docker images inside Minikube...${NC}"
eval $(minikube docker-env)

echo "   Building Speed Layer..."
docker build -t speed-layer:latest -f speed_layer/Dockerfile .
echo "   Building Serving Layer..."
docker build -t serving-layer:latest -f serving_layer/Dockerfile .
echo "   Building Batch Layer..."
docker build -t batch-layer:latest -f batch_layer/Dockerfile .
echo -e "${GREEN}✅ Images built${NC}"
echo ""

# Step 12: Deploy Speed Layer
echo -e "${YELLOW}[12/14] Deploying Speed Layer...${NC}"
kubectl apply -f speed_layer/deployment.yaml -n default
echo -e "${GREEN}✅ Speed Layer deployed${NC}"
echo ""

# Step 13: Deploy Serving Layer
echo -e "${YELLOW}[13/14] Deploying Serving Layer (with Cassandra backend)...${NC}"
kubectl apply -f serving_layer/deployment.yaml -n default
echo -e "${GREEN}✅ Serving Layer deployed${NC}"
echo ""

# Step 14: Deploy Batch Layer (Job)
echo -e "${YELLOW}[14/14] Deploying Batch Layer Job...${NC}"
kubectl apply -f batch_layer/deployment.yaml -n default
echo -e "${GREEN}✅ Batch Layer Job submitted${NC}"
echo ""

# Step 15: Display status
echo -e "${YELLOW}[15/15] Checking deployment status...${NC}"
echo ""
echo -e "${CYAN}Kafka Pods:${NC}"
kubectl get pods -n kafka -l strimzi.io/cluster=kafka-cluster
echo ""
echo -e "${CYAN}MinIO Pods:${NC}"
kubectl get pods -n minio
echo ""
echo -e "${CYAN}Cassandra Pods:${NC}"
kubectl get pods -n cassandra
echo ""
echo -e "${CYAN}Application Pods (Default Namespace):${NC}"
kubectl get pods -n default
echo ""
echo -e "${CYAN}Kafka Topics:${NC}"
kubectl get kafkatopics -n kafka
echo ""

# Step 14: Instructions
echo -e "${YELLOW}[COMPLETE] System Deployed${NC}"
echo ""
echo -e "${CYAN}================================${NC}"
echo -e "${CYAN}STARTING DATA PIPELINE...${NC}"
echo -e "${CYAN}================================${NC}"
echo ""

# Export Connectivity for local scripts
export MINIKUBE_IP=$(minikube ip)
export KAFKA_BOOTSTRAP_SERVERS=$MINIKUBE_IP:30092
export MINIO_ENDPOINT=http://$MINIKUBE_IP:30900

echo -e "${GREEN}✅ Kafka Bootstrap Server: $KAFKA_BOOTSTRAP_SERVERS${NC}"
echo -e "${GREEN}✅ MinIO API Endpoint: $MINIO_ENDPOINT${NC}"
echo ""

# Start Batch Ingestion
echo -e "${YELLOW}Starting Batch Ingestion Layer...${NC}"
echo "   Reading from Kafka ($KAFKA_BOOTSTRAP_SERVERS) → Writing to MinIO ($MINIO_ENDPOINT)"
# Explicitly pass env vars for safety
KAFKA_BOOTSTRAP_SERVERS=$KAFKA_BOOTSTRAP_SERVERS MINIO_ENDPOINT=$MINIO_ENDPOINT ./venv/bin/python3 ingestion_layer/minio_ingest_k8s.py > /tmp/ingestion.log 2>&1 &
INGESTION_PID=$!
echo $INGESTION_PID > /tmp/ingestion.pid
sleep 5
echo -e "${GREEN}✅ Batch Ingestion started (PID: $INGESTION_PID)${NC}"
echo "   Logs: /tmp/ingestion.log"
echo ""

# Start Producer
echo -e "${YELLOW}Starting Event Producer...${NC}"
echo "   Generating events → Kafka topics"
./venv/bin/python3 ingestion_layer/producer.py > /tmp/producer.log 2>&1 &
PRODUCER_PID=$!
echo $PRODUCER_PID > /tmp/producer.pid
sleep 3
echo -e "${GREEN}✅ Producer started (PID: $PRODUCER_PID)${NC}"
echo "   Logs: /tmp/producer.log"
echo ""

# Start Cassandra Sync (after a delay to let data accumulate)
echo -e "${YELLOW}Scheduling Cassandra Sync (will run in 60 seconds)...${NC}"
(sleep 60 && \
 kubectl port-forward service/cassandra 9042:9042 -n cassandra > /dev/null 2>&1 & \
 CASSANDRA_SYNC_PF_PID=$! && \
 echo $CASSANDRA_SYNC_PF_PID > /tmp/cassandra-pf.pid && \
 sleep 5 && \
 CASSANDRA_HOST=localhost MINIO_ENDPOINT=$MINIO_ENDPOINT ./venv/bin/python3 serving_layer/cassandra_sync.py --once > /tmp/cassandra-sync.log 2>&1 && \
 echo -e "${GREEN}✅ Initial Cassandra sync completed${NC}" && \
 kill $CASSANDRA_SYNC_PF_PID 2>/dev/null) &
SYNC_SCHEDULER_PID=$!
echo -e "${GREEN}✅ Cassandra sync scheduled (PID: $SYNC_SCHEDULER_PID)${NC}"
echo "   Logs: /tmp/cassandra-sync.log"
echo ""

echo -e "${GREEN}================================${NC}"
echo -e "${GREEN}DEPLOYMENT COMPLETE! ✅${NC}"
echo -e "${GREEN}================================${NC}"
echo ""
echo -e "${CYAN}Running Processes:${NC}"
echo "  - MinIO Port-Forward: PID $MINIO_PF_PID"
echo "  - Batch Ingestion: PID $INGESTION_PID"
echo "  - Event Producer: PID $PRODUCER_PID"
echo "  - Cassandra Sync Scheduler: PID $SYNC_SCHEDULER_PID"
echo ""
echo -e "${CYAN}To Stop Background Processes:${NC}"
echo "  kill \$(cat /tmp/minio-pf.pid) \$(cat /tmp/ingestion.pid) \$(cat /tmp/producer.pid) \$(cat /tmp/cassandra-pf.pid 2>/dev/null) 2>/dev/null || true"
echo ""
echo -e "${YELLOW}NEXT STEPS:${NC}"
echo ""
echo "1. Wait 2-3 minutes for data to flow through the pipeline"
echo ""
echo "2. Run verification to check system health:"
echo "   ./deployment/verify_minikube.sh"
echo ""
echo "3. Access services:"
echo "   - MinIO Console: http://localhost:9000 (minioadmin/minioadmin)"
echo "   - Serving API: kubectl port-forward service/serving-layer 8000:8000"
echo "   - Dashboard: kubectl port-forward service/serving-layer 8501:8501"
echo ""
echo "4. Monitor logs:"
echo "   - Ingestion: tail -f /tmp/ingestion.log"
echo "   - Producer: tail -f /tmp/producer.log"
echo "   - Speed Layer: kubectl logs -f -l app=speed-layer"
echo "   - Cassandra Sync: tail -f /tmp/cassandra-sync.log"
echo ""
echo "5. Access Cassandra:"
echo "   - Port-forward: kubectl port-forward service/cassandra 9042:9042 -n cassandra"
echo "   - Connect: kubectl exec -it cassandra-0 -n cassandra -- cqlsh"
echo "   - Status: kubectl exec cassandra-0 -n cassandra -- nodetool status"
echo ""
echo -e "${GREEN}================================${NC}"
