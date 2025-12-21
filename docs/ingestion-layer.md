# Ingestion Layer

## Overview
The ingestion layer generates and streams real-time educational platform events into Kafka topics.

## Components

### Producer (`ingestion_layer/producer.py`)
- **Purpose**: Generate fake streaming data simulating user activity
- **Technology**: Python with `kafka-python` and `Faker`
- **Output**: Publishes events to 6 Kafka topics

## Kafka Topics

| Topic | Description |
|-------|-------------|
| `auth_topic` | User authentication events (login/logout) |
| `assessment_topic` | Quiz and test submissions |
| `video_topic` | Video playback events |
| `course_topic` | Course enrollment and completion |
| `profile_topic` | User profile updates |
| `notification_topic` | System notifications |

## Configuration

```bash
# Environment Variables
KAFKA_BOOTSTRAP_SERVERS=kafka-cluster-kafka-bootstrap.kafka.svc:9092
```

## Deployment

```bash
# Build image in Minikube
eval $(minikube docker-env)
docker build -t producer:latest -f ingestion_layer/Dockerfile .

# Deploy
kubectl apply -f ingestion_layer/deployment.yaml

# Monitor logs
kubectl logs -f deployment/producer -n default
```

## Verification

```bash
# Check topic messages
kubectl exec -it kafka-cluster-kafka-0 -n kafka -- bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic auth_topic \
  --from-beginning \
  --max-messages 5
```

## Key Features
- Continuous real-time data generation
- Realistic educational platform event simulation
- Automatic retry and error handling
- Configurable event generation rate