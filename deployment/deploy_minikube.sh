#!/bin/bash

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
# minikube stop --alsologtostderr
echo -e "${YELLOW}[1/10] Starting Minikube cluster...${NC}"
minikube start --cpus=6 --memory=12288 --driver=docker --addons=ingress
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
kubectl apply -f ./kafka/storage-class.yaml -n kafka
kubectl apply -f ./kafka/persistent-volumn-minikube.yaml -n kafka
echo -e "${GREEN}✅ Storage configured${NC}"
echo ""

# Step 5: Deploy Kafka
echo -e "${YELLOW}[5/10] Deploying Kafka cluster (this takes 3-5 minutes)...${NC}"
kubectl apply -f ./kafka/deployment.yaml -n kafka
kubectl wait kafka/kafka-cluster --for=condition=Ready --timeout=1200s -n kafka
echo -e "${GREEN}✅ Kafka cluster ready${NC}"
echo ""

# Step 6: Create topics
echo -e "${YELLOW}[6/10] Creating Kafka topics...${NC}"
kubectl apply -f ./kafka/topics.yaml -n kafka
sleep 10
echo -e "${GREEN}✅ Topics created${NC}"
echo ""

# Step 7: Deploy MinIO
echo -e "${YELLOW}[7/10] Deploying MinIO...${NC}"
kubectl apply -f ./minio/deployment.yaml -n minio
kubectl wait deployment/minio --for=condition=Available --timeout=300s -n minio
echo -e "${GREEN}✅ MinIO ready${NC}"
echo ""

# Step 8: Setup MinIO bucket
echo -e "${YELLOW}[8/10] Configuring MinIO bucket...${NC}"
echo -e "${CYAN}Starting port-forward for MinIO setup...${NC}"

# Start port-forward in background
kubectl port-forward service/minio 9000:9000 -n minio > /dev/null 2>&1 &
PF_PID=$!
sleep 5

# Configure mc and create bucket
# Check if mc is installed, if not, try using a container or skip
if command -v mc &> /dev/null; then
    mc alias set minikube http://localhost:9000 minioadmin minioadmin
    mc mb minikube/bucket-0
else
    echo -e "${YELLOW}⚠️ 'mc' client not found locally. Trying to use docker run...${NC}"
    docker run --network host --entrypoint /bin/sh minio/mc -c "mc alias set minikube http://localhost:9000 minioadmin minioadmin; mc mb minikube/bucket-0"
fi

# Kill port-forward
kill $PF_PID
echo -e "${GREEN}✅ MinIO bucket created${NC}"
echo ""

# Step 9: Build Docker Images
echo -e "${YELLOW}[9/12] Building Docker images inside Minikube...${NC}"
eval $(minikube docker-env)

# echo "   Building Producer..."
# docker build -t producer:latest -f ./producer/Dockerfile .
echo "   Building ingestion Layer..."
docker build -t ingesyion-layer:latest -f ./ingestion_layer/Dockerfile .
echo "   Building Speed Layer..."
docker build -t speed-layer:latest -f ./speed_layer/Dockerfile .
echo "   Building Serving Layer..."
docker build -t serving-layer:latest -f ./serving_layer/Dockerfile .
echo "   Building Batch Layer..."
docker build -t batch-layer:latest -f ./batch_layer/Dockerfile .
echo -e "${GREEN}✅ Images built${NC}"
echo ""

echo -e "${YELLOW}[10/13] Deploying Ingestion Layer...${NC}"
kubectl apply -f ./ingestion_layer/ingestion-job.yaml -n default
echo -e "${GREEN}✅ Ingestion Layer deployed${NC}"
echo ""

echo -e "${YELLOW}[10/12] Deploying ingestion Layer...${NC}"
kubectl apply -f ./ingestion_layer/deployment.yaml -n default
echo -e "${GREEN}✅ Speed Layer deployed${NC}"
echo ""

# Step 10: Deploy Speed Layer
echo -e "${YELLOW}[10/12] Deploying Speed Layer...${NC}"
kubectl apply -f ./speed_layer/deployment.yaml -n default
echo -e "${GREEN}✅ Speed Layer deployed${NC}"
echo ""

# Step 11: Deploy Serving Layer
echo -e "${YELLOW}[11/12] Deploying Serving Layer...${NC}"
kubectl apply -f ./serving_layer/deployment.yaml -n default
echo -e "${GREEN}✅ Serving Layer deployed${NC}"
echo ""

# Step 12: Deploy Batch Layer (Job) - DISABLED (Orchestrated by Airflow now)
# echo -e "${YELLOW}[12/12] Deploying Batch Layer Job...${NC}"
# kubectl apply -f ./batch_layer/deployment.yaml -n default
# echo -e "${GREEN}✅ Batch Layer Job submitted${NC}"
# echo ""

# Step 12: Deploy Cassandra
echo -e "${YELLOW}[12/15] Deploying Cassandra...${NC}"
kubectl apply -f ./cassandra/deployment.yaml -n default
kubectl wait deployment/cassandra --for=condition=Available --timeout=600s -n default
echo -e "${GREEN}✅ Cassandra deployed${NC}"
echo ""

# Step 13: Deploy Trino
echo -e "${YELLOW}[13/15] Deploying Trino...${NC}"
kubectl create configmap trino-catalog --from-file=./trino/catalog/ -n default
kubectl apply -f ./trino/deployment.yaml -n default
echo -e "${GREEN}✅ Trino deployed${NC}"
echo ""

# Step 14: Deploy Airflow
echo -e "${YELLOW}[14/15] Deploying Airflow...${NC}"
# Prepare Airflow DAGs mount
minikube ssh "sudo mkdir -p /mnt/airflow/dags && sudo chmod -R 777 /mnt/airflow/dags"
# Mount local DAGs to Minikube
# We rely on the user manually mounting or copying, but for this script we assume hostPath works if we used 'minikube mount' or similar, 
# but hostPath in Minikube VM points to the VM's filesystem.
# We'll use 'minikube cp' to sync DAGs or assume development mode where user mounts.
# For simplicity, we create the PV hostpath structure inside minikube.

kubectl apply -f ./airflow/kubernetes/rbac.yaml -n default
kubectl apply -f ./airflow/kubernetes/deployment.yaml -n default
kubectl wait deployment/airflow --for=condition=Available --timeout=600s -n default
echo -e "${GREEN}✅ Airflow deployed${NC}"
echo ""

# Step 13: Display status
echo -e "${YELLOW}[13/13] Checking deployment status...${NC}"
echo ""
echo -e "${CYAN}Kafka Pods:${NC}"
kubectl get pods -n kafka -l strimzi.io/cluster=kafka-cluster
echo ""
echo -e "${CYAN}MinIO Pods:${NC}"
kubectl get pods -n minio
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
echo -e "${CYAN}NEXT STEPS:${NC}"
echo -e "${CYAN}================================${NC}"
echo ""
echo -e "${YELLOW}1. Open 2 NEW terminals and run:${NC}"
echo "   Terminal 1: kubectl port-forward service/minio 9000:9000 -n minio"
echo "   Terminal 2: kubectl port-forward service/minio 9001:9001 -n minio"
echo ""
echo -e "${YELLOW}2. Configure Kafka Connection (Linux/Ubuntu):${NC}"
echo "   export KAFKA_BOOTSTRAP_SERVERS=\$(minikube ip):30092"
echo ""
echo -e "${YELLOW}2. Wait 30 seconds for port-forwards to be ready${NC}"
echo ""
echo -e "${YELLOW}3. Run verification:${NC}"
echo "   ./deployment/verify_minikube.sh"
echo ""
echo -e "${GREEN}================================${NC}"
echo -e "${GREEN}DEPLOYMENT COMPLETE! ✅${NC}"
echo -e "${GREEN}================================${NC}"
echo ""
