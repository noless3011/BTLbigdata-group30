# BTLbigdata-group30
Hệ thống thu thập, lưu trữ, phân tích và xử lý kết quả học tập của sinh viên để dự đoán điểm số

## Progress
- Test Kafka deployment using Strimzi opetator on Minikube 
- Setup and test Postgres--Debzium--Kafka pipeline.
- Setup 3 Ubuntu 24.04 VMs using UTM 
- Instal and setup Kubenetes on VMs for multi-node deployment.
- Test Kafka deployment using Confluent's CFK (failed)
- Add router for the VMs 

## TODO
- Migrate Kafka deployment to K8s on VMs
- Serve the data to the Postgres--Debzium--Kafka pipeline
- Setup a minimum HA cluster (3 master, 3 worker, loadbalancer)