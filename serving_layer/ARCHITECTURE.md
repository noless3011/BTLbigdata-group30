# Serving Layer Architecture - Lambda Architecture

## Overview

The serving layer has been refactored to implement a true **Lambda Architecture**, separating batch processing and real-time stream processing concerns.

## Architecture Components

### 1. Batch Layer (MinIO + Parquet)
**Service:** `serving_layer.py` (Port 8000)  
**Storage:** MinIO (S3-compatible object storage)  
**Data Format:** Parquet files  
**Purpose:** Serve historical, aggregated data processed by batch jobs

#### Endpoints:
- `GET /analytics/batch/summary` - Total students, courses, batch job status
- `GET /analytics/batch/dau?hours=X` - Historical daily active users
- `GET /analytics/batch/student_engagement_distribution` - Student engagement levels
- `GET /analytics/batch/recent_activity` - Cumulative activity stats
- `GET /analytics/courses` - All courses with stats
- `GET /analytics/course/{course_id}` - Course details
- `GET /analytics/course/{course_id}/students` - Students in course
- `GET /analytics/students` - All students with stats
- `GET /analytics/student/{student_id}` - Student details
- `GET /analytics/student/{student_id}/courses` - Student's courses

#### Data Sources (MinIO Paths):
```
batch_views/
├── student_overview/
├── course_overview/
├── student_course_enrollment/
├── student_course_detailed/
└── auth_daily_active_users/
```

### 2. Speed Layer (Cassandra)
**Service:** `serving_layer_cassandra.py` (Port 8001)  
**Storage:** Cassandra database  
**Purpose:** Serve real-time data from streaming jobs

#### Endpoints:
- `GET /analytics/speed/summary` - Current active users, speed layer health
- `GET /analytics/speed/recent_activity?hours=X` - Recent videos/interactions
- `GET /analytics/speed/active_users?hours=X` - Real-time active users time series
- `GET /analytics/speed/active_users/latest` - Most recent active user count
- `GET /analytics/speed/course_popularity?limit=X&hours=X` - Top courses by recent activity
- `GET /analytics/speed/video?limit=X&hours=X` - Video engagement stats

#### Cassandra Tables:
```
university_analytics.active_users
university_analytics.course_popularity
university_analytics.video_engagement
```

### 3. Dashboard (Streamlit)
**Service:** `dashboard.py`  
**Purpose:** Unified UI combining both layers

#### Features:
- **Overview Dashboard:**
  - Real-time metrics from Speed Layer (🟢)
  - Historical metrics from Batch Layer (🔵)
  - Combined visualizations
  
- **Courses View:**
  - Course catalog from Batch Layer
  - Student enrollment data
  
- **Students View:**
  - Student profiles from Batch Layer
  - Course enrollment history

## Data Flow

```
┌─────────────────┐
│  Kafka Topics   │
└────────┬────────┘
         │
    ┌────┴─────┐
    │          │
    ▼          ▼
┌────────┐  ┌──────────────┐
│ Speed  │  │ Batch Layer  │
│ Layer  │  │ (Spark Jobs) │
└───┬────┘  └──────┬───────┘
    │              │
    ▼              ▼
┌──────────┐  ┌──────────┐
│Cassandra │  │  MinIO   │
│(Real-time│  │ (Parquet)│
└────┬─────┘  └─────┬────┘
     │              │
     ▼              ▼
┌─────────────────────────┐
│ Serving Layer APIs      │
│ ┌──────────┬──────────┐ │
│ │Speed API │Batch API │ │
│ │  :8001   │  :8000   │ │
│ └────┬─────┴─────┬────┘ │
└──────┼───────────┼──────┘
       │           │
       └─────┬─────┘
             ▼
     ┌──────────────┐
     │  Dashboard   │
     │  (Streamlit) │
     └──────────────┘
```

## Environment Variables

### Batch Layer (serving_layer.py)
```bash
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
```

### Speed Layer (serving_layer_cassandra.py)
```bash
CASSANDRA_HOST=cassandra
CASSANDRA_PORT=9042
```

### Dashboard (dashboard.py)
```bash
API_URL_BATCH=http://localhost:8000   # Batch Layer API
API_URL_SPEED=http://localhost:8001   # Speed Layer API
```

## Running the Services

### Start Batch Layer API (MinIO)
```bash
cd serving_layer
python serving_layer.py
# Runs on port 8000
```

### Start Speed Layer API (Cassandra)
```bash
cd serving_layer
python serving_layer_cassandra.py
# Runs on port 8001
```

### Start Dashboard
```bash
cd serving_layer
streamlit run dashboard.py
```

## Key Design Decisions

### 1. Separation of Concerns
- **Batch Layer:** Historical data, complex aggregations, full data reprocessing
- **Speed Layer:** Real-time data, windowed aggregations, low latency

### 2. Data Freshness vs Completeness
- **Speed Layer:** Fresh data (< 1 minute old), approximate counts
- **Batch Layer:** Complete data, exact counts, but may be hours old

### 3. Query Patterns
- **Dashboard Overview:** Combines both layers
  - Current users → Speed Layer
  - Total students → Batch Layer
  - Recent trends → Speed Layer
  - Historical analysis → Batch Layer

### 4. Storage Trade-offs
- **MinIO/Parquet:** Cost-effective for large historical datasets, columnar format for analytics
- **Cassandra:** Low-latency reads, time-series optimized, handles high write throughput

## Performance Considerations

### Batch Layer
- Parquet files are cached by S3FileSystem
- Pre-aggregated views reduce query time
- Full scans acceptable for batch queries

### Speed Layer
- Cassandra queries filtered by time windows
- ALLOW FILTERING used where appropriate for time-range queries
- In-memory aggregation for small result sets
- Partition keys optimized for time-series access

## Monitoring

### Health Checks
- `GET /health` on both APIs returns connection status
- Dashboard shows "Speed Layer Status" indicator:
  - 🟢 Healthy: Data < 5 minutes old
  - 🟡 Stale: Data 5-15 minutes old
  - 🔴 Down: No data or > 15 minutes old

### Metrics to Monitor
1. **Speed Layer Lag:** Time since last write to Cassandra
2. **Batch Job Frequency:** Last batch job completion time
3. **API Response Times:** Both serving APIs
4. **Dashboard Refresh Rate:** User-configurable auto-refresh

## Future Enhancements

1. **Serving Layer Unification:**
   - Implement a unified API that automatically routes queries to the appropriate layer
   - Add caching layer (Redis) for frequently accessed data

2. **Advanced Merging:**
   - Smart deduplication of users across batch and speed layers
   - Lambda merge views that combine batch + speed data

3. **Data Validation:**
   - Cross-validation between batch and speed layers
   - Alerting on data discrepancies

4. **Query Optimization:**
   - Add materialized views in Cassandra for common queries
   - Implement query result caching

5. **Scalability:**
   - Add load balancing for multiple serving layer instances
   - Implement read replicas for high availability