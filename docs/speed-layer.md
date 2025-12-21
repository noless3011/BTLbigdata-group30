# Speed Layer

## Overview
The speed layer processes real-time streaming data from Kafka using Spark Structured Streaming, providing low-latency views to complement batch layer results.

## Components

### Stream Processing Jobs (`speed_layer/`)
- **Purpose**: Real-time event processing and aggregation
- **Technology**: Spark Structured Streaming on Kubernetes
- **Input**: Kafka topics (real-time events)
- **Output**: Real-time views in Cassandra tables

## Streaming Jobs

### Active Stream Processors
1. **Authentication Stream** - Processes login/logout events
2. **Assessment Stream** - Real-time quiz/test analytics
3. **Video Stream** - Live video engagement tracking
4. **Course Stream** - Course enrollment updates
5. **Profile Stream** - User profile change events
6. **Notification Stream** - Alert and notification processing

## Configuration

```yaml
# Environment Variables
KAFKA_BOOTSTRAP_SERVERS: kafka-cluster-kafka-bootstrap.kafka.svc:9092
CASSANDRA_HOST: cassandra.default.svc
CASSANDRA_PORT: 9042
CASSANDRA_KEYSPACE: education_platform
```

## Deployment

```bash
# Deploy speed layer
kubectl apply -f speed_layer/deployment.yaml

# Monitor streaming jobs
kubectl logs -f deployment/speed-layer -n default

# Check processing status
kubectl get pods -l app=speed-layer
```

## Kafka Integration

```bash
# Input Topics (same as ingestion layer)
- auth_topic
- assessment_topic
- video_topic
- course_topic
- profile_topic
- notification_topic
```

## Real-Time Views

### Cassandra Tables (Realtime)
- `realtime_user_activity` - Last 15 minutes of activity
- `realtime_course_engagement` - Live course metrics
- `realtime_assessment_scores` - Recent test results
- `realtime_video_views` - Current video watching stats

## Key Features
- Sub-second latency processing
- Windowed aggregations (tumbling/sliding windows)
- Stateful stream processing
- Exactly-once semantics
- Automatic checkpointing and recovery

## Processing Windows

```python
# Typical window configurations
- Sliding Window: 5 minutes, slide 1 minute
- Tumbling Window: 10 minutes
- Session Window: 30 minutes timeout
```

## Monitoring

```bash
# Check stream lag
kubectl exec -it kafka-cluster-kafka-0 -n kafka -- bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group speed-layer-group

# View processed records
kubectl logs deployment/speed-layer | grep "Processed records"
```

## Typical Workflow
1. Events arrive in Kafka topics
2. Spark Streaming consumes in micro-batches
3. Real-time aggregations computed
4. Results merged into Cassandra realtime views
5. Serving layer queries combine batch + realtime data