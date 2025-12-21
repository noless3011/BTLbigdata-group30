#!/bin/bash

# Port-forward script for Kubernetes services
# This script runs all port-forwards in the background using nohup
# so they persist even after SSH disconnection

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
    local RESOURCE_TYPE=${5:-service}  # Default to 'service', can be 'pod', 'deployment', etc.

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

# Stop any existing port-forwards
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

# 1. Kafka Bootstrap Server (external access)
start_portforward "kafka-cluster-kafka-bootstrap" "kafka" 9092 9092 "service"

# 2. Kafka UI/Management (if you want to use external tools)
start_portforward "kafka-cluster-kafka-bootstrap" "kafka" 9093 9093 "service"

# 3. MinIO API
start_portforward "minio" "minio" 9000 9000 "service"

# 4. MinIO Console (Web UI)
start_portforward "minio" "minio" 9001 9001 "service"

# 5. Serving Layer UI (Streamlit Dashboard)
start_portforward "serving-layer" "default" 8501 8501 "service"

# 6. Serving Layer API
start_portforward "serving-layer" "default" 8000 8000 "service"

# 7. Cassandra CQL Port
# Note: Cassandra is accessed via service in cassandra namespace
start_portforward "cassandra" "cassandra" 9042 9042 "service"

# Wait for all processes to start
sleep 3

echo ""
echo "========================================"
echo "Port-forwarding setup complete!"
echo "========================================"
echo ""
echo "Access your services at:"
echo "  - Kafka Bootstrap:        <your-server-ip>:9092"
echo "  - Kafka TLS:              <your-server-ip>:9093"
echo "  - MinIO API:              http://<your-server-ip>:9000"
echo "  - MinIO Console:          http://<your-server-ip>:9001"
echo "  - Serving Layer UI:       http://<your-server-ip>:8501"
echo "  - Serving Layer API:      http://<your-server-ip>:8000"
echo "  - Cassandra CQL:          <your-server-ip>:9042"
echo ""
echo "Log files are in: $LOG_DIR"
echo "PID files are in: $PID_DIR"
echo ""
echo "To stop all port-forwards, run:"
echo "  ./port-stop.sh"
echo ""
