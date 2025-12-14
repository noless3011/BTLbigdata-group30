# Serving Layer (Unified Query Interface)

## Overview

The serving layer provides a unified query interface that merges batch views (historical, high-latency) with speed views (recent, low-latency) to answer queries with complete, up-to-date results.

## Status

✅ **IMPLEMENTED** - Basic REST API with view merger logic

## Planned Components

### 1. Query Service (`serving_layer.py`)
- Unified query interface
- Merges batch + speed views
- REST API endpoints

### 2. View Merger
- Combines precomputed batch views
- Adds incremental speed layer updates
- Handles overlapping data

### 3. Query Optimization
- Cache frequently accessed views
- Lazy loading strategies
- Query result caching

## Architecture

```
User Query → Serving Layer → ┌─ Batch Views (MinIO)
                             └─ Speed Views (In-memory)
                                      ↓
                              Merged Result → Response
```

## Query Patterns

### Example: Student Video Engagement

```python
# Batch view: Historical data (up to yesterday)
batch_video = read_parquet("s3a://bucket-0/batch_views/video_total_watch_time")

# Speed view: Today's data (real-time)
speed_video = read_stream("kafka://video_topic")

# Merge: Complete view
total_engagement = batch_video.union(speed_video).groupBy("user_id").sum("watch_time")
```

## API Endpoints (Planned)

- `GET /api/v1/student/{id}/engagement` - Student engagement metrics
- `GET /api/v1/video/{id}/analytics` - Video analytics
- `GET /api/v1/course/{id}/statistics` - Course statistics
- `GET /api/v1/dashboard/overview` - System-wide dashboard

## Technologies

- Apache Spark (query engine)
- Flask/FastAPI (REST API)
- Redis (caching)
- MinIO (batch view storage)

## Implementation

### ✅ Completed

1. ✅ REST API service (FastAPI)
2. ✅ View merger logic (batch + speed views)
3. ✅ Mock data support (for development)
4. ✅ Query endpoints for students, videos, courses, dashboard

### ⏳ Next Steps

1. ⏳ Add caching layer (Redis)
2. ⏳ Integrate with real batch views (when batch layer runs)
3. ⏳ Integrate with speed layer (real-time views)
4. ⏳ Build dashboard UI
5. ⏳ Add authentication/authorization

## Usage

### Start API Server

```bash
# Install dependencies
pip install -r serving_layer/requirements.txt

# Run server
python serving_layer/run_serving_layer.py
```

Or use uvicorn directly:
```bash
uvicorn serving_layer.serving_layer:app --host 0.0.0.0 --port 8000 --reload
```

### API Endpoints

- **Root**: http://localhost:8000/
- **API Docs**: http://localhost:8000/docs (Swagger UI)
- **ReDoc**: http://localhost:8000/redoc
- **Health**: http://localhost:8000/api/v1/health

### Example Queries

```bash
# Student engagement
curl http://localhost:8000/api/v1/student/SV001/engagement

# Assessment - Student submissions
curl http://localhost:8000/api/v1/assessment/student/SV001/submissions

# Assessment - Quiz performance
curl http://localhost:8000/api/v1/assessment/quiz/QUIZ001/performance

# Assessment - Teacher workload
curl http://localhost:8000/api/v1/assessment/teacher/GV001/workload

# Auth - User activity
curl http://localhost:8000/api/v1/auth/user/SV001/activity

# Auth - Active sessions (realtime)
curl http://localhost:8000/api/v1/auth/sessions/active

# Video analytics
curl http://localhost:8000/api/v1/video/VID001/analytics

# Course - Material downloads
curl http://localhost:8000/api/v1/course/COURSE001/materials/downloads

# Course - Interactions
curl http://localhost:8000/api/v1/course/COURSE001/interactions

# Profile activity
curl http://localhost:8000/api/v1/profile/user/SV001/activity

# Teacher dashboard
curl http://localhost:8000/api/v1/teacher/GV001/dashboard

# Dashboard overview
curl http://localhost:8000/api/v1/dashboard/overview
```

Xem chi tiết tại: [API_REFERENCE.md](API_REFERENCE.md)

## Configuration

Edit `serving_layer/config.py` to configure:
- API host/port
- MinIO endpoints
- Kafka settings
- Mock data mode
- Cache settings
