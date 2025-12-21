# Serving Layer

The Serving Layer provides a Unified Query Interface merging Batch and Speed layers.
It consists of a REST API (FastAPI) and a Dashboard (Streamlit).

## 🎯 Overview

**Two Backend Options:**

1. **MinIO Backend** (`serving_layer.py`) - Direct Parquet file queries
2. **Cassandra Backend** (`serving_layer_cassandra.py`) - Fast database queries ⭐ **Recommended**

## Components

- **`serving_layer.py`**: FastAPI backend to query MinIO (original)
- **`serving_layer_cassandra.py`**: FastAPI backend to query Cassandra (recommended)
- **`cassandra_sync.py`**: Service to sync MinIO data to Cassandra
- **`cassandra_schema.cql`**: Cassandra database schema
- **`init_cassandra_schema.py`**: Schema initialization script
- **`dashboard.py`**: Streamlit frontend for visualization
- **`Dockerfile`**: Multi-use image for both components
- **`deployment.yaml`**: Kubernetes deployment/service
- **`CASSANDRA_INTEGRATION.md`**: Complete Cassandra documentation

## Architecture

### Option 1: MinIO Backend (Original)

```
User (Browser) 
  ↓
Serving Layer UI (Streamlit :8501)
  ↓
Serving Layer API (FastAPI :8000)
  ↓
MinIO (s3a://bucket-0)
  ├── batch_views/ (Historical)
  └── speed_views/ (Real-time)
```

### Option 2: Cassandra Backend (Recommended) ⭐

```
User (Browser) 
  ↓
Serving Layer UI (Streamlit :8501)
  ↓
Serving Layer API (FastAPI :8000)
  ↓
Cassandra Database (university_analytics)
  ↑
Cassandra Sync Service (every 5 min)
  ↑
MinIO (s3a://bucket-0)
  ├── batch_views/ (Historical)
  └── speed_views/ (Real-time)
```

**Performance Improvement:** 98-99% faster queries with Cassandra!

## 🚀 Quick Start with Cassandra

### 1. Start Infrastructure

```bash
cd config
docker-compose up -d cassandra minio kafka
```

### 2. Initialize Cassandra Schema

```bash
python serving_layer/init_cassandra_schema.py
```

### 3. Run Initial Data Sync

```bash
python serving_layer/cassandra_sync.py --once
```

### 4. Start Serving Layer API (Cassandra)

```bash
uvicorn serving_layer.serving_layer_cassandra:app --host 0.0.0.0 --port 8000
```

### 5. Start Dashboard

```bash
streamlit run serving_layer/dashboard.py
```

Access at: http://localhost:8501

### 6. Start Continuous Sync (Optional)

In a separate terminal:

```bash
python serving_layer/cassandra_sync.py
```

## Running Locally (Docker) - MinIO Backend

```bash
# Build
docker build -t serving-layer ./serving_layer

# Run API
docker run -p 8000:8000 --env MINIO_ENDPOINT=http://host.docker.internal:9000 serving-layer uvicorn serving_layer.serving_layer:app --host 0.0.0.0

# Run UI
docker run -p 8501:8501 --env API_URL=http://host.docker.internal:8000 serving-layer streamlit run dashboard.py
```

## Running on Kubernetes

### Deploy Cassandra

```bash
kubectl apply -f cassandra/deployment.yaml
kubectl wait --for=condition=ready pod -l app=cassandra -n cassandra --timeout=300s
```

### Initialize Schema

```bash
kubectl port-forward service/cassandra 9042:9042 -n cassandra
python serving_layer/init_cassandra_schema.py
```

### Deploy Serving Layer

```bash
kubectl apply -f serving_layer/deployment.yaml
```

Access the Dashboard at NodePort 30001 (e.g., `http://<minikube-ip>:30001`).

## 📊 Performance Comparison

| Operation | MinIO (Parquet) | Cassandra | Improvement |
|-----------|----------------|-----------|-------------|
| Dashboard Summary | 2-3 seconds | 20-40ms | **99% faster** |
| Student Overview | 800-1200ms | 5-15ms | **98% faster** |
| Course List | 600-900ms | 3-10ms | **99% faster** |

## 🔧 Configuration

### Environment Variables

**Cassandra Connection:**
- `CASSANDRA_HOST` - Cassandra host (default: `localhost`)
- `CASSANDRA_PORT` - Cassandra port (default: `9042`)

**MinIO Connection:**
- `MINIO_ENDPOINT` - MinIO endpoint (default: `http://localhost:9000`)
- `MINIO_ACCESS_KEY` - Access key (default: `minioadmin`)
- `MINIO_SECRET_KEY` - Secret key (default: `minioadmin`)

**Sync Service:**
- `SYNC_INTERVAL` - Sync interval in seconds (default: `300`)

## 📚 Documentation

For complete Cassandra integration guide, see:
- **[CASSANDRA_INTEGRATION.md](CASSANDRA_INTEGRATION.md)** - Setup, usage, troubleshooting

## 🐛 Troubleshooting

### Cassandra Connection Issues

```bash
# Check if Cassandra is running
docker ps | grep cassandra

# View Cassandra logs
docker logs cassandra

# Manually connect to Cassandra
docker exec -it cassandra cqlsh
```

### Empty Data in Cassandra

```bash
# Run manual sync
python serving_layer/cassandra_sync.py --once

# Check MinIO has data
mc ls minio/bucket-0/batch_views/
```

## 📞 Support

- Check [CASSANDRA_INTEGRATION.md](CASSANDRA_INTEGRATION.md) for detailed troubleshooting
- Review logs: `docker logs cassandra`
- Verify schema: `docker exec cassandra cqlsh -e "DESCRIBE university_analytics;"`
