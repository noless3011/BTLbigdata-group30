eval $(minikube docker-env)

# docker build -t custom-airflow:latest -f airflow/Dockerfile . 
docker build -t batch-layer:latest -f batch_layer/Dockerfile .

# kubectl apply -f airflow/kubernetes/rbac.yaml
# kubectl apply -f airflow/kubernetes/deployment.yaml
# 2. Deploy Cassandra (Ensure it's ready)
# kubectl apply -f cassandra/deployment.yaml
# 3. Initialize Cassandra Schema (if not done)
# Wait for Cassandra pod to be 'Running' first!
# cat cassandra/schema.cql | kubectl exec -i deployment/cassandra -- cqlsh