#!/bin/bash

# Port-forward script for Kubernetes services
# Updated for Split Architecture (MinIO/Cassandra/UI)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="${SCRIPT_DIR}/portforward-logs"
PID_DIR="${SCRIPT_DIR}/portforward-pids"

# Create directories for logs and PIDs
mkdir -p "$LOG_DIR"
mkdir -p "$PID_DIR"

echo "Starting port-forwarding for all services..."
echo "Logs will be saved to: $LOG_DIR"
echo "PID files will be saved to: $PID_DIR"

# Function to start a port-forward in the background
start_portforward() {
    local SERVICE_NAME=$1
    local NAMESPACE=$2
    local LOCAL_PORT=$3
    local REMOTE_PORT=$4
    local RESOURCE_TYPE=${5:-service}

    echo "Starting port-forward: ${SERVICE_NAME} (${NAMESPACE}) ${LOCAL_PORT}:${REMOTE_PORT}"

    nohup kubectl port-forward \
        -n "$NAMESPACE" \
        --address 0.0.0.0 \
        "${RESOURCE_TYPE}/${SERVICE_NAME}" \
        "${LOCAL_PORT}:${REMOTE_PORT}" \
        > "$LOG_DIR/${SERVICE_NAME}-${LOCAL_PORT}.log" 2>&1 &

    echo $! > "$PID_DIR/${SERVICE_NAME}-${LOCAL_PORT}.pid"
    echo "  -> PID: $! (saved to ${SERVICE_NAME}-${LOCAL_PORT}.pid)"
}

# ---------------------------------------------------------
# 1. CLEANUP: Stop any existing port-forwards
# ---------------------------------------------------------
echo ""
echo "Stopping any existing port-forwards..."
if [ -d "$PID_DIR" ]; then
    for pidfile in "$PID_DIR"/*.pid; do
        if [ -f "$pidfile" ]; then
            pid=$(cat "$pidfile")
            if kill -0 "$pid" 2>/dev/null; then
                echo "  Stopping PID $pid"
                kill "$pid" 2>/dev/null || true
            fi
            rm "$pidfile"
        fi
    done
fi

sleep 2

echo ""
echo "Starting new port-forwards..."
echo ""

# ---------------------------------------------------------
# 2. INFRASTRUCTURE (Kafka, MinIO, Cassandra)
# ---------------------------------------------------------

# Kafka
start_portforward "kafka-cluster-kafka-bootstrap" "kafka" 9092 9092 "service"
start_portforward "kafka-cluster-kafka-bootstrap" "kafka" 9093 9093 "service"

# MinIO
start_portforward "minio" "minio" 9000 9000 "service"
start_portforward "minio" "minio" 9001 9001 "service"

# Cassandra
start_portforward "cassandra" "cassandra" 9042 9042 "service"


# ---------------------------------------------------------
# 3. SERVING LAYER (UI & APIs)
# ---------------------------------------------------------

# Serving Layer UI (Streamlit)
start_portforward "serving-ui" "default" 8501 8501 "service"

# Serving Layer API - MinIO Backend
# Mapping local 8001 -> container 8000
start_portforward "serving-api-minio" "default" 8001 8000 "service"

# Serving Layer API - Cassandra Backend
# Mapping local 8002 -> container 8000
start_portforward "serving-api-cassandra" "default" 8002 8000 "service"


# ---------------------------------------------------------
# 4. SUMMARY
# ---------------------------------------------------------
# Wait for processes to stabilize
sleep 3

echo ""
echo "========================================"
echo "Port-forwarding setup complete!"
echo "========================================"
echo ""
echo "Access your services at:"
echo "  - Serving Layer UI:       http://157.245.202.83:8501"
echo "  - API (MinIO Backend):    http://157.245.202.83:8001"
echo "  - API (Cassandra Backend):http://157.245.202.83:8002"
echo ""
echo "Infrastructure:"
echo "  - MinIO Console:          http://157.245.202.83:9001"
echo "  - Cassandra CQL:          157.245.202.83:9042"
echo "  - Kafka Bootstrap:        157.245.202.83:9092"
echo ""
echo "Log files are in: $LOG_DIR"
echo "To stop, run: ./port-stop.sh (or kill PIDs in $PID_DIR)"
echo ""
