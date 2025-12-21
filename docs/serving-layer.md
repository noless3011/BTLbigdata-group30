# Serving Layer

## Overview
The serving layer merges batch and real-time views from Cassandra, exposing unified data through REST API and interactive Streamlit dashboard.

## Components

### API Server (`serving_layer/api.py`)
- **Purpose**: RESTful API for data access
- **Technology**: FastAPI (Uvicorn)
- **Port**: 8000
- **Endpoints**: User analytics, course metrics, assessment reports

### Dashboard (`serving_layer/app.py`)
- **Purpose**: Interactive data visualization
- **Technology**: Streamlit
- **Port**: 8501
- **Features**: Real-time charts, filters, KPIs

## Architecture

```
Cassandra (Batch + Realtime Views)
          ↓
    Serving Layer
    ├── FastAPI (REST)
    └── Streamlit (UI)
          ↓
    End Users / Applications
```

## API Endpoints

### Core Endpoints
- `GET /health` - Service health check
- `GET /api/users/{user_id}` - User activity data
- `GET /api/courses/{course_id}` - Course engagement metrics
- `GET /api/assessments` - Assessment analytics
- `GET /api/videos/popular` - Trending videos
- `GET /api/dashboard/summary` - KPI summary

## Configuration

```yaml
# Environment Variables
CASSANDRA_HOST: cassandra.default.svc
CASSANDRA_PORT: 9042
CASSANDRA_KEYSPACE: education_platform
API_PORT: 8000
STREAMLIT_PORT: 8501
```

## Deployment

```bash
# Deploy serving layer
kubectl apply -f serving_layer/deployment.yaml

# Access via NodePort
kubectl get svc serving-layer
# Streamlit UI: http://<node-ip>:30001
# FastAPI: http://<node-ip>:30002

# Or port-forward locally
kubectl port-forward svc/serving-layer 8501:8501
kubectl port-forward svc/serving-layer 8000:8000
```

## Dashboard Features

### Visualizations
- **User Activity**: Login trends, active users
- **Course Analytics**: Enrollment, completion rates
- **Assessment Metrics**: Score distributions, pass rates
- **Video Engagement**: Watch time, popular content
- **Real-time KPIs**: Current active users, recent events

### Filters
- Date range selection
- Course/user filtering
- Metric aggregation level

## Data Merging Strategy

```python
# Lambda Architecture Query Pattern
def get_user_stats(user_id):
    batch_view = cassandra.query("batch_user_stats", user_id)
    realtime_view = cassandra.query("realtime_user_stats", user_id)
    return merge(batch_view, realtime_view)
```

## Access Methods

### External Access (Production)
```bash
# Via NodePort
STREAMLIT_UI: http://<minikube-ip>:30001
API: http://<minikube-ip>:8000

# Get Minikube IP
minikube ip
```

### Port Forwarding (Development)
```bash
# Forward both services
kubectl port-forward svc/serving-layer 8501:8501 &
kubectl port-forward svc/serving-layer 8000:8000 &

# Access locally
curl http://localhost:8000/health
open http://localhost:8501
```

## API Usage Examples

```bash
# Health check
curl http://localhost:8000/health

# Get user analytics
curl http://localhost:8000/api/users/user123

# Course engagement
curl http://localhost:8000/api/courses/course456

# Dashboard summary
curl http://localhost:8000/api/dashboard/summary
```

## Key Features
- Unified view of batch + real-time data
- Low-latency query response
- Interactive data exploration
- RESTful API for integration
- Auto-refresh dashboard
- Responsive design

## Monitoring

```bash
# Check service status
kubectl get pods -l app=serving-layer
kubectl logs -f deployment/serving-layer

# Test API availability
curl http://<service-ip>:8000/health
```

## Typical Use Cases
1. Executive dashboards (KPIs, trends)
2. Student/instructor analytics portals
3. Third-party application integration
4. Real-time monitoring systems
5. Operational reporting