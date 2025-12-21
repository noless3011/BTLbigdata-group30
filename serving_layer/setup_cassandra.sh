#!/bin/bash

# Cassandra Quick Setup Script
# This script sets up Cassandra for the serving layer

set -e

echo "========================================"
echo "Cassandra Serving Layer Setup"
echo "========================================"
echo ""

# Check if running in project root
if [ ! -f "serving_layer/cassandra_schema.cql" ]; then
    echo "❌ Error: Please run this script from the project root directory"
    echo "   cd BTLbigdata-group30"
    exit 1
fi

# Check if Docker is running
if ! docker ps > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running. Please start Docker first."
    exit 1
fi

# Step 1: Start Cassandra
echo "[1/5] Starting Cassandra container..."
cd config
docker-compose up -d cassandra
cd ..

# Step 2: Wait for Cassandra to be ready
echo "[2/5] Waiting for Cassandra to be ready (this may take 60-90 seconds)..."
MAX_RETRIES=30
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if docker exec cassandra cqlsh -e "SELECT now() FROM system.local" > /dev/null 2>&1; then
        echo "✓ Cassandra is ready!"
        break
    fi

    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
        echo "❌ Error: Cassandra failed to start in time"
        echo "   Check logs: docker logs cassandra"
        exit 1
    fi

    echo "   Attempt $RETRY_COUNT/$MAX_RETRIES - waiting..."
    sleep 3
done

# Step 3: Initialize schema
echo "[3/5] Initializing Cassandra schema..."
python serving_layer/init_cassandra_schema.py

if [ $? -ne 0 ]; then
    echo "❌ Error: Schema initialization failed"
    exit 1
fi

# Step 4: Check if MinIO has data
echo "[4/5] Checking for data in MinIO..."
if docker ps | grep -q minio; then
    echo "✓ MinIO is running"

    # Wait a bit for MinIO to be ready
    sleep 2

    # Check if we have batch views
    HAS_DATA=$(docker exec minio mc ls local/bucket-0/batch_views/ 2>/dev/null | wc -l || echo "0")

    if [ "$HAS_DATA" -gt "0" ]; then
        echo "✓ Found data in MinIO"
        echo "[5/5] Running initial data sync..."
        python serving_layer/cassandra_sync.py --once

        if [ $? -eq 0 ]; then
            echo "✓ Data sync completed successfully"
        else
            echo "⚠ Warning: Data sync had some errors, but schema is ready"
        fi
    else
        echo "⚠ Warning: No data found in MinIO batch_views/"
        echo "   You should run batch jobs first, then sync:"
        echo "   python batch_layer/run_batch_jobs.py s3a://bucket-0/master_dataset s3a://bucket-0/batch_views"
        echo "   python serving_layer/cassandra_sync.py --once"
    fi
else
    echo "⚠ Warning: MinIO is not running"
    echo "   Start it with: cd config && docker-compose up -d minio"
    echo "[5/5] Skipping data sync (no MinIO)"
fi

echo ""
echo "========================================"
echo "✓ Cassandra Setup Complete!"
echo "========================================"
echo ""
echo "Next steps:"
echo ""
echo "1. Start the Cassandra-based API:"
echo "   uvicorn serving_layer.serving_layer_cassandra:app --host 0.0.0.0 --port 8000"
echo ""
echo "2. Start the dashboard:"
echo "   streamlit run serving_layer/dashboard.py"
echo ""
echo "3. (Optional) Start continuous sync service:"
echo "   python serving_layer/cassandra_sync.py"
echo ""
echo "Access points:"
echo "  - API: http://localhost:8000"
echo "  - API Docs: http://localhost:8000/docs"
echo "  - Dashboard: http://localhost:8501"
echo ""
echo "Useful commands:"
echo "  - Connect to Cassandra: docker exec -it cassandra cqlsh"
echo "  - Check status: docker exec cassandra nodetool status"
echo "  - View logs: docker logs cassandra"
echo "  - Manual sync: python serving_layer/cassandra_sync.py --once"
echo ""
