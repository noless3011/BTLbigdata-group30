#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${CYAN}================================${NC}"
echo -e "${CYAN}=       LMS DATA PIPELINE      =${NC}"
echo -e "${CYAN}================================${NC}"
echo ""

Step 1: Start Minikube
echo -e "${YELLOW}[1/9] Starting Minikube cluster...${NC}"
minikube start --cpus=4 --memory=8192 --driver=docker
if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Failed to start Minikube${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Minikube started${NC}"
echo ""

# Step 2: Deploy Kafka
echo -e "${YELLOW}[2/9] Setting up Kafka cluster...${NC}"

kubectl create namespace kafka

echo "Installing Strimzi Kafka Operator..."
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
sleep 5
echo "Waiting Strimzi operator ready..."
kubectl wait deployment/strimzi-cluster-operator --for=condition=Available --timeout=300s -n kafka
echo -e "${GREEN}✅ Strimzi operator ready${NC}"
echo ""

echo "Setting up storage..."
minikube ssh "sudo mkdir -p /mnt/kafka-data/0 /mnt/kafka-data/1 /mnt/kafka-data/2 && sudo chmod -R 777 /mnt/kafka-data"
kubectl apply -f ./kafka/kafka-storageclass.yaml -n kafka
kubectl apply -f ./kafka/kafka-persistent-volumn.yaml -n kafka
echo -e "${GREEN}✅ Storage configured${NC}"
echo ""

echo "Deploying Kafka cluster..."
# kubectl delete -f ./kafka/kafka-cluster.yaml -n kafka
kubectl apply -f ./kafka/kafka-cluster.yaml -n kafka
kubectl wait kafka/kafka-cluster --for=condition=Ready --timeout=1200s -n kafka
echo -e "${GREEN}✅ Kafka cluster ready${NC}"
echo ""

echo -e "Creating Kafka topics..."
kubectl apply -f ./kafka/kafka-topics.yaml -n kafka
sleep 10
echo -e "${GREEN}✅ Topics created${NC}"
echo ""

# Step 3: Deploy MinIO
echo -e "${YELLOW}[3/9] Setting up MinIO...${NC}"

kubectl create namespace minio

kubectl apply -f ./minio/deployment.yaml -n minio
kubectl wait deployment/minio --for=condition=Available --timeout=300s -n minio
echo -e "${GREEN}✅ MinIO ready${NC}"
echo ""

echo "Configuring MinIO bucket..."
echo "${CYAN}Starting port-forward for MinIO setup...${NC}"

# Start port-forward in background
kubectl port-forward service/minio 9000:9000 -n minio > /dev/null 2>&1 &
PF_PID=$!
sleep 5

if command -v mc &> /dev/null; then
    mc alias set minikube http://localhost:9000 minioadmin minioadmin
    mc mb minikube/bucket-0
else
    echo -e "${YELLOW}⚠️ 'mc' client not found locally...${NC}"
    kill $PF_PID
    exit 1
fi

# Kill port-forward
kill $PF_PID
echo -e "${GREEN}✅ MinIO bucket created${NC}"
echo ""

# Step 4: Build Docker Images
echo -e "${YELLOW}[4/9] Building Docker images inside Minikube...${NC}"
eval $(minikube docker-env)

echo "   Building ingestion Layer..."

docker build -t ingestion-layer:latest -f ./ingestion_layer/Dockerfile .
echo "   Building Speed Layer..."
docker build -t speed-layer:latest -f ./speed_layer/Dockerfile .
echo "   Building Serving Layer..."
docker build -t serving-layer:latest -f ./serving_layer/Dockerfile .
echo "   Building Batch Layer..."
docker build -t batch-layer:latest -f ./batch_layer/Dockerfile .
echo -e "${GREEN}✅ Images built${NC}"
echo ""

kubectl create ns spark

echo -e "${YELLOW}[6/9] Deploying Ingestion Layer...${NC}"
kubectl apply -f ./ingestion_layer/ingestion-job.yaml -n spark
kubectl wait --for=condition=complete job/kafka-ingestion -n spark --timeout=600s
echo -e "${GREEN}✅ Ingestion Layer deployed${NC}"
echo ""

# Step 10: Deploy Speed Layer
echo -e "${YELLOW}[7/9] Deploying Speed Layer...${NC}"
kubectl apply -f ./speed_layer/deployment.yaml -n default
echo -e "${GREEN}✅ Speed Layer deployed${NC}"
echo ""

# Step 11: Deploy Serving Layer
echo -e "${YELLOW}[8/9] Deploying Serving Layer...${NC}"
kubectl apply -f ./serving_layer/deployment.yaml -n default
echo -e "${GREEN}✅ Serving Layer deployed${NC}"
echo ""

# Step 12: Deploy Batch Layer (Job)
echo -e "${YELLOW}[9/9] Deploying Batch Layer Job...${NC}"
kubectl apply -f ./batch_layer/deployment.yaml -n default
echo -e "${GREEN}✅ Batch Layer Job submitted${NC}"
echo ""

Display status
echo -e "${YELLOW}Checking deployment status...${NC}"
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
# echo -e "${YELLOW}[COMPLETE] System Deployed${NC}"
# echo ""
# echo -e "${CYAN}================================${NC}"
# echo -e "${CYAN}NEXT STEPS:${NC}"
# echo -e "${CYAN}================================${NC}"
# echo ""
# echo -e "${YELLOW}1. Open 2 NEW terminals and run:${NC}"
# echo "   Terminal 1: kubectl port-forward service/minio 9000:9000 -n minio"
# echo "   Terminal 2: kubectl port-forward service/minio 9001:9001 -n minio"
# echo ""
# echo -e "${YELLOW}2. Configure Kafka Connection (Linux/Ubuntu):${NC}"
# echo "   export KAFKA_BOOTSTRAP_SERVERS=\$(minikube ip):30092"
# echo ""
# echo -e "${YELLOW}2. Wait 30 seconds for port-forwards to be ready${NC}"
# echo ""
# echo -e "${YELLOW}3. Run verification:${NC}"
# echo "   ./deployment/verify_minikube.sh"
echo ""
echo -e "${GREEN}================================${NC}"
echo -e "${GREEN}DEPLOYMENT COMPLETE! ✅${NC}"
echo -e "${GREEN}================================${NC}"
echo ""
