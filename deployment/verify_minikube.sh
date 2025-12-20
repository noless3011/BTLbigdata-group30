#!/bin/bash

# Automated Verification Script for Lambda Architecture
# Usage: ./verify_minikube.sh

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${CYAN}===================================${NC}"
echo -e "${CYAN}FULL SYSTEM VERIFICATION STARTING${NC}"
echo -e "${CYAN}===================================${NC}"
echo ""

# 1. Check Pod Status
echo -e "${YELLOW}[1/6] Checking Application Pods...${NC}"
SPEED_POD=$(kubectl get pods -n default -l app=speed-layer -o jsonpath='{.items[0].status.phase}')
SERVING_POD=$(kubectl get pods -n default -l app=serving-layer -o jsonpath='{.items[0].status.phase}')
MINIO_POD=$(kubectl get pods -n minio -l app=minio -o jsonpath='{.items[0].status.phase}')

if [ "$SPEED_POD" == "Running" ] && [ "$SERVING_POD" == "Running" ] && [ "$MINIO_POD" == "Running" ]; then
    echo -e "${GREEN}✅ All Application Pods are Running${NC}"
else
    echo -e "${RED}❌ Some pods are not ready: Speed=$SPEED_POD, Serving=$SERVING_POD, MinIO=$MINIO_POD${NC}"
fi
echo ""

# 2. Setup Port Forwarding for Tests
echo -e "${YELLOW}[2/6] Setting up temporary port-forwarding...${NC}"
kubectl port-forward service/minio 9000:9000 -n minio > /dev/null 2>&1 &
PF_PID=$!
sleep 5
echo -e "${GREEN}✅ Port-forwarding active${NC}"
echo ""

# 3. Produce Data
echo -e "${YELLOW}[3/6] Generating Test Data (Producer)...${NC}"
export KAFKA_BOOTSTRAP_SERVERS=$(minikube ip):30092
# Run producer for 10 seconds (requires modifying producer to support timeout or just running it briefly)
# We will just run it in background and kill it
python3 ./producer.py > producer.log 2>&1 &
PROD_PID=$!
echo "   Producer running (PID $PROD_PID)... waiting 15s..."
sleep 15
kill $PROD_PID
echo -e "${GREEN}✅ Data generation complete${NC}"
echo ""

# 4. Verify MinIO Ingestion (Raw Data)
echo -e "${YELLOW}[4/6] Verifying Raw Data Ingestion (MinIO)...${NC}"
# Use mc or docker mc
if command -v mc &> /dev/null; then
    mc alias set minikube http://localhost:9000 minioadmin minioadmin > /dev/null
    COUNT=$(mc ls --recursive minikube/bucket-0/master_dataset/ | wc -l)
else
    COUNT=$(docker run --rm --network host --entrypoint /bin/sh minio/mc -c "mc alias set minikube http://localhost:9000 minioadmin minioadmin > /dev/null; mc ls --recursive minikube/bucket-0/master_dataset/ | wc -l")
fi

if [ "$COUNT" -gt 0 ]; then
    echo -e "${GREEN}✅ Ingestion Successful: Found $COUNT files in master_dataset/${NC}"
else
    echo -e "${RED}❌ Ingestion Failed: No files found in master_dataset/${NC}"
fi
echo ""

# 5. Verify Serving Layer API
echo -e "${YELLOW}[5/6] Verifying Serving Layer API...${NC}"
# Port forward API
kubectl port-forward service/serving-layer 8000:8000 -n default > /dev/null 2>&1 &
API_PF_PID=$!
sleep 5

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/docs)
if [ "$HTTP_CODE" == "200" ]; then
    echo -e "${GREEN}✅ Serving API is responding (HTTP 200)${NC}"
else
    echo -e "${RED}❌ Serving API failed (HTTP $HTTP_CODE)${NC}"
fi
kill $API_PF_PID
echo ""

# 6. Verify Batch Job
echo -e "${YELLOW}[6/6] Checking Batch Job Status...${NC}"
JOB_STATUS=$(kubectl get job batch-layer-job -n default -o jsonpath='{.status.succeeded}')
if [ "$JOB_STATUS" == "1" ]; then
    echo -e "${GREEN}✅ Batch Job Completed Successfully${NC}"
else
    echo -e "${YELLOW}⚠️ Batch Job not yet complete/succeeded (Status: $JOB_STATUS)${NC}"
fi
echo ""

# Cleanup
kill $PF_PID
echo -e "${CYAN}===================================${NC}"
echo -e "${CYAN}VERIFICATION FINISHED${NC}"
echo -e "${CYAN}===================================${NC}"
