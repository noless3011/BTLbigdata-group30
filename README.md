# BTLbigdata-group30 - University Learning Analytics System

Lambda Architecture implementation for student learning analytics with Kafka, Spark, MinIO, Cassandra, Trino, and Airflow.

---

## 📁 Project Structure (Reorganized)

```
BTLbigdata-group30/
├── airflow/                        # Airflow Orchestration
│   ├── dags/                      # Application DAGs
│   └── kubernetes/                # Airflow Deployment
│
├── cassandra/                      # Cassandra Batch Views
│   ├── deployment.yaml            # Deployment
│   └── schema.cql                 # Database Schema
│
├── trino/                          # Trino Query Engine
│   ├── deployment.yaml            # Deployment
│   └── catalog/                   # Data Catalogs (Cassandra, MinIO)
│
├── batch_layer/                    # Batch Processing Layer ✅
│   ├── jobs/                      # PySpark batch jobs (5 jobs)
│   ├── Dockerfile                 # Batch layer image
│   ├── run_batch_jobs.py          # Job Runner
│   └── jobs/spark_config.py       # Spark & Cassandra Config
│
├── ingestion_layer/                # Kafka → Storage Ingestion
│   └── ...                        # (Unchanged)
│
├── speed_layer/                    # Real-Time Stream Processing ⏳
│   └── ...                        # (Unchanged)
│
├── serving_layer/                  # Unified Query Interface ⏳
│   └── ...                        # (Unchanged)
│
├── deployment/                     # Deployment scripts & guides
│   ├── deploy_minikube.sh        # Auto-deploy EVERYTHING
│   └── ...
│
└── README.md                       # This file
```

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                      DATA SOURCES                                │
│            (Student Learning Events - 6 Categories)              │
└───────────────────────────┬─────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│                   INGESTION LAYER (Kafka)                        │
└────────┬────────────────────────────────────┬───────────────────┘
         ↓                                     ↓
┌────────────────────────┐        ┌───────────────────────────────┐
│   BATCH LAYER ✅       │        │   SPEED LAYER ⏳              │
│   (Historical Data)    │        │   (Real-Time Data)            │
│                        │        │                               │
│ • Airflow Orchestrator │        │ • Spark Streaming             │
│ • PySpark Jobs         │        │ • Windowed Aggregations       │
│ • Cassandra Storage    │        │ • Incremental Updates         │
└────────┬───────────────┘        └───────────┬───────────────────┘
         │                                     │
         └──────────────┬──────────────────────┘
                        ↓
         ┌──────────────────────────────────┐
         │     SERVING LAYER (Trino) ⏳     │
         │  (Unified Query Interface)       │
         │                                  │
         │  Query Batch Views (Cassandra)   │
         │  Query Data Lake (MinIO)         │
         └──────────────────────────────────┘
```

---

## 🚀 Quick Start (Minikube)

### 1. Deploy Infrastructure
Run the unified deployment script to set up Kafka, MinIO, Cassandra, Trino, and Airflow.

```bash
cd deployment
./deploy_minikube.sh
```

**Note**: This requires significant resources (allocates 6 CPUs, 12GB RAM to Minikube).

### 2. Access Services
The script will output port-forwarding commands. Open separate terminals:

```bash
# MinIO Console
kubectl port-forward service/minio 9001:9001 -n minio

# Airflow UI (admin/admin)
kubectl port-forward service/airflow 8080:8080 -n default

# Trino UI
kubectl port-forward service/trino 8080:8080 -n default
```

- **Airflow**: [http://localhost:8080](http://localhost:8080)
- **MinIO**: [http://localhost:9001](http://localhost:9001)

### 3. Initialize Database
Connect to Cassandra to create the schema:

```bash
kubectl exec -it deployment/cassandra -- cqlsh
```
*Tip: Copy contents of `cassandra/schema.cql` and paste into `cqlsh` prompt.*

### 4. Run Data Pipeline

1.  **Generate Events**:
    ```bash
    python ingestion_layer/producer.py
    ```
2.  **Ingest to MinIO**:
    ```bash
    python ingestion_layer/minio_ingest_k8s.py
    ```
3.  **Trigger Batch Jobs (Airflow)**:
    - Go to Airflow UI.
    - Unpause the `batch_layer_pipeline` DAG.
    - Trigger the DAG manually to run all batch jobs.

### 5. Query Results (Trino)

Connect to Trino using CLI or DBeaver (JDBC):

```sql
-- Query Authentication Stats from Cassandra
SELECT * FROM cassandra.lms_analytics.auth_daily_active_users;

-- Query Video Engagement
SELECT * FROM cassandra.lms_analytics.video_popularity ORDER BY total_views_seconds DESC;
```

---

## 🔧 Batch Layer Details

The batch layer has been upgraded to write to **Cassandra** for low-latency serving.

- **Source**: MinIO (Raw Events, Parquet)
- **Processing**: PySpark (Orchestrated by Airflow)
- **Sink**:
    1.  **Cassandra** (Primary Serving Store)
    2.  **MinIO** (Parquet Backup - `batch_views/`)

### Airflow DAG
The `batch_layer_pipeline` DAG runs daily and executes all 5 batch jobs in parallel using `KubernetesPodOperator`.

### Spark-Cassandra Integration
Jobs use `spark-cassandra-connector` to write DataFrames directly to Cassandra tables.

---

## 🎯 Status

| Component | Status | Tech Stack |
|:---|:---|:---|
| **Ingestion** | ✅ Complete | Kafka, MinIO |
| **Batch Processing** | ✅ Complete | Spark, Airflow |
| **Batch Storage** | ✅ Complete | Cassandra, MinIO |
| **Orchestration** | ✅ Complete | Airflow |
| **Serving Query** | ✅ Complete | Trino |
| **Speed Layer** | ⏳ In Progress | Spark Streaming |

---

## 👥 Team - Group 30
