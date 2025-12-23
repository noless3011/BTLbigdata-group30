#!/bin/bash

# 1. Create monitoring namespace
kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -

# 2. Deploy Kafka Metrics Config & Update Kafka
kubectl apply -f kafka/kafka-metrics.yaml
kubectl apply -f kafka/deployment.yaml

# 3. Create Kafka Dashboard ConfigMap from JSON
# We generate this on the fly to ensure it uses the latest JSON content
kubectl create configmap kafka-dashboard \
  --from-file=kafka-dashboard.json=monitoring/kafka-dashboard.json \
  -n monitoring \
  --dry-run=client -o yaml | kubectl apply -f -

# 3b. Create MinIO Dashboard ConfigMap from JSON
kubectl create configmap minio-dashboard \
  --from-file=minio-dashboard.json=monitoring/minio-dashboard.json \
  -n monitoring \
  --dry-run=client -o yaml | kubectl apply -f -

# 4. Deploy Prometheus & Grafana (includes all other ConfigMaps)
kubectl apply -f monitoring/deployment.yaml

# 5. Restart Pods to pick up changes
echo "Restarting Kafka pods to apply JMX metrics config..."
# Rolling restart via Strimzi (safe)
kubectl -n kafka rollout restart owners/my-cluster || kubectl delete pod -n kafka -l app.kubernetes.io/name=kafka

echo "Restarting Monitoring pods..."
kubectl -n monitoring rollout restart deployment/prometheus
kubectl -n monitoring rollout restart deployment/grafana

echo "Deployment complete. Waiting for pods to be ready..."
kubectl -n monitoring wait --for=condition=ready pod --all --timeout=300s

echo "=================================================="
echo "Monitoring Stack Deployed Successfully!"
echo "Put forwarding commands:"
echo "Prometheus: kubectl port-forward svc/prometheus 9090:9090 -n monitoring"
echo "Grafana:    kubectl port-forward svc/grafana 3000:3000 -n monitoring"
echo "=================================================="
