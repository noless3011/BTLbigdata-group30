#!/bin/bash

# Cassandra Verification Script for Minikube
# Checks if Cassandra is deployed and functioning correctly

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${CYAN}================================${NC}"
echo -e "${CYAN}CASSANDRA VERIFICATION${NC}"
echo -e "${CYAN}================================${NC}"
echo ""

# Check if Cassandra pod exists
echo -e "${YELLOW}[1/7] Checking Cassandra pod...${NC}"
CASSANDRA_POD=$(kubectl get pods -n cassandra -l app=cassandra -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [ -z "$CASSANDRA_POD" ]; then
    echo -e "${RED}❌ Cassandra pod not found${NC}"
    echo "Run: kubectl apply -f cassandra/deployment.yaml"
    exit 1
fi
echo -e "${GREEN}✅ Cassandra pod found: $CASSANDRA_POD${NC}"
echo ""

# Check if Cassandra pod is ready
echo -e "${YELLOW}[2/7] Checking pod status...${NC}"
POD_STATUS=$(kubectl get pod $CASSANDRA_POD -n cassandra -o jsonpath='{.status.phase}')
if [ "$POD_STATUS" != "Running" ]; then
    echo -e "${RED}❌ Cassandra pod not running (status: $POD_STATUS)${NC}"
    echo "Wait for pod to be ready or check logs:"
    echo "  kubectl logs $CASSANDRA_POD -n cassandra"
    exit 1
fi
echo -e "${GREEN}✅ Cassandra pod is running${NC}"
echo ""

# Check Cassandra service
echo -e "${YELLOW}[3/7] Checking Cassandra service...${NC}"
SERVICE_EXISTS=$(kubectl get service cassandra -n cassandra 2>/dev/null)
if [ -z "$SERVICE_EXISTS" ]; then
    echo -e "${RED}❌ Cassandra service not found${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Cassandra service exists${NC}"
echo ""

# Check if Cassandra is responsive
echo -e "${YELLOW}[4/7] Testing Cassandra connectivity...${NC}"
CQLSH_TEST=$(kubectl exec $CASSANDRA_POD -n cassandra -- cqlsh -e "SELECT now() FROM system.local" 2>&1)
if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Cassandra not responsive${NC}"
    echo "Error: $CQLSH_TEST"
    echo "Cassandra may still be starting up. Wait a few minutes and try again."
    exit 1
fi
echo -e "${GREEN}✅ Cassandra is responsive${NC}"
echo ""

# Check if keyspace exists
echo -e "${YELLOW}[5/7] Checking university_analytics keyspace...${NC}"
KEYSPACE_CHECK=$(kubectl exec $CASSANDRA_POD -n cassandra -- cqlsh -e "DESCRIBE KEYSPACE university_analytics" 2>&1)
if echo "$KEYSPACE_CHECK" | grep -q "not found"; then
    echo -e "${YELLOW}⚠️  Keyspace not found${NC}"
    echo "Run schema initialization:"
    echo "  python serving_layer/init_cassandra_schema.py"
else
    echo -e "${GREEN}✅ Keyspace exists${NC}"
fi
echo ""

# Check tables
echo -e "${YELLOW}[6/7] Checking tables...${NC}"
TABLE_COUNT=$(kubectl exec $CASSANDRA_POD -n cassandra -- cqlsh -e "USE university_analytics; DESCRIBE TABLES" 2>&1 | grep -v "^$" | wc -l)
if [ "$TABLE_COUNT" -gt 5 ]; then
    echo -e "${GREEN}✅ Found $TABLE_COUNT tables${NC}"
else
    echo -e "${YELLOW}⚠️  Only $TABLE_COUNT tables found (expected 30+)${NC}"
    echo "Schema may not be fully initialized."
fi
echo ""

# Check for data
echo -e "${YELLOW}[7/7] Checking for data...${NC}"
SYSTEM_SUMMARY=$(kubectl exec $CASSANDRA_POD -n cassandra -- cqlsh -e "SELECT * FROM university_analytics.system_summary LIMIT 1" 2>&1)
if echo "$SYSTEM_SUMMARY" | grep -q "main"; then
    echo -e "${GREEN}✅ Data found in system_summary table${NC}"

    # Get some stats
    STUDENT_COUNT=$(kubectl exec $CASSANDRA_POD -n cassandra -- cqlsh -e "SELECT COUNT(*) FROM university_analytics.student_overview" 2>&1 | grep -oP '\d+' | head -1)
    COURSE_COUNT=$(kubectl exec $CASSANDRA_POD -n cassandra -- cqlsh -e "SELECT COUNT(*) FROM university_analytics.course_overview" 2>&1 | grep -oP '\d+' | head -1)

    echo -e "${CYAN}   Students: ${STUDENT_COUNT:-0}${NC}"
    echo -e "${CYAN}   Courses: ${COURSE_COUNT:-0}${NC}"
else
    echo -e "${YELLOW}⚠️  No data found in Cassandra${NC}"
    echo "Run data sync:"
    echo "  python serving_layer/cassandra_sync.py --once"
fi
echo ""

# Cluster status
echo -e "${CYAN}Cassandra Cluster Status:${NC}"
kubectl exec $CASSANDRA_POD -n cassandra -- nodetool status 2>&1 | grep -E "^UN|^DN" || echo "  (Single node cluster)"
echo ""

echo -e "${GREEN}================================${NC}"
echo -e "${GREEN}CASSANDRA VERIFICATION COMPLETE${NC}"
echo -e "${GREEN}================================${NC}"
echo ""
echo -e "${CYAN}Useful Commands:${NC}"
echo "  - Connect to Cassandra: kubectl exec -it $CASSANDRA_POD -n cassandra -- cqlsh"
echo "  - View logs: kubectl logs $CASSANDRA_POD -n cassandra"
echo "  - Port forward: kubectl port-forward service/cassandra 9042:9042 -n cassandra"
echo "  - Check status: kubectl exec $CASSANDRA_POD -n cassandra -- nodetool status"
echo ""
echo -e "${CYAN}Data Management:${NC}"
echo "  - Initialize schema: python serving_layer/init_cassandra_schema.py"
echo "  - Sync data: python serving_layer/cassandra_sync.py --once"
echo "  - View data: kubectl exec $CASSANDRA_POD -n cassandra -- cqlsh -e 'SELECT * FROM university_analytics.system_summary;'"
echo ""
