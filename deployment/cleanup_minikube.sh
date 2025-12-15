#!/bin/bash

# Cleanup Script for Minikube
# Usage: ./cleanup_minikube.sh [--delete-cluster]

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${CYAN}================================${NC}"
echo -e "${CYAN}CLEANUP MINIKUBE RESOURCES      ${NC}"
echo -e "${CYAN}================================${NC}"
echo ""

# 1. Delete Application Layers
echo -e "${YELLOW}Deleting Speed & Serving Layers...${NC}"
kubectl delete deployment speed-layer serving-layer -n default --ignore-not-found=true
kubectl delete service serving-layer -n default --ignore-not-found=true
kubectl delete job batch-layer-job -n default --ignore-not-found=true
echo -e "${GREEN}✅ Application layers deleted${NC}"
echo ""

# 2. Delete MinIO and Kafka
echo -e "${YELLOW}Deleting MinIO and Kafka...${NC}"
kubectl delete namespace minio --ignore-not-found=true
kubectl delete namespace kafka --ignore-not-found=true
echo -e "${GREEN}✅ Infrastructure namespaces deleted${NC}"
echo ""

# 3. Stop Minikube
echo -e "${YELLOW}Stopping Minikube...${NC}"
minikube stop
echo -e "${GREEN}✅ Minikube stopped${NC}"
echo ""

if [[ "$1" == "--delete-cluster" ]]; then
    echo -e "${YELLOW}Deleting Minikube cluster (FULL RESET)...${NC}"
    minikube delete
    echo -e "${GREEN}✅ Minikube cluster deleted${NC}"
else
    echo -e "${CYAN}Minikube cluster preserved. To delete completely, run:${NC}"
    echo -e "  ./cleanup_minikube.sh --delete-cluster"
fi

echo ""
echo -e "${GREEN}Cleanup complete! ✅${NC}"
