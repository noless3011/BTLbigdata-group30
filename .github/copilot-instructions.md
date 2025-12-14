**Purpose**
- **Goal**: Help AI coding agents become productive quickly by documenting the repo architecture, developer workflows, conventions, and command examples specific to this project.

**Big Picture (what this repo implements)**
- **Architecture**: A Lambda-style demo pipeline (batch + stream) built with PySpark, Kafka and HDFS. Key components:
  - Ingestion (stream -> HDFS): `ingest_layer.py` writes raw Kafka messages as Parquet to `hdfs://namenode:9000/data/master_dataset/`.
  - Speed layer (real-time): `stream_layer.py` reads Kafka, computes minute windows and prints streaming aggregations to console.
  - Batch layer: `batch_layer.py` reads the raw Parquet master dataset and writes precomputed Parquet views under `hdfs://namenode:9000/data/batch_views/`.
  - Data generators & producers: `generate_fake_data.ipynb` and `producer.py` create synthetic records and push events to Kafka topics.
  - Deployment: `docker-compose.yml` brings up Zookeeper, Kafka, HDFS (namenode/datanode) and Spark master/worker for local development.

**Key files & paths**
- `docker-compose.yml` — local dev topology. Kafka advertised listeners are configured for host (`localhost:9092`) and container (`kafka:29092`).
- `hadoop.env` — Hadoop/HDFS env variables used by the compose stack.
- `ingest_layer.py` — Structured Streaming job that writes raw Kafka `key,value,topic,timestamp` into HDFS (Parquet), partitioned by `topic`.
- `stream_layer.py` — Structured Streaming speed layer with windowed aggregations (minute windows, watermarking).
- `batch_layer.py` — Batch Spark job that parses JSON `value` from Parquet raw data and writes aggregated Parquet views for serving.
- `producer.py` & `generate_fake_data.ipynb` — event and dataset generators for testing.
- `hdfs_data/` — local Docker-mounted HDFS data volumes (namenode/datanode persistence).

**Project-specific conventions & patterns**
- Kafka bootstrap mapping: use `localhost:9092` when running producers/clients on the host machine; use `kafka:29092` when code runs inside the Docker network (containers or Spark inside docker-compose).
- Spark + Kafka connector: jobs add `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0` (seen in `ingest_layer.py` and `stream_layer.py` configs).
- Raw ingestion stores the original Kafka `value` as STRING JSON inside Parquet. Upstream jobs parse via `from_json(col('value'), schema)` — follow that pattern when creating new processors.
- Checkpointing: streaming jobs use HDFS checkpoint locations under `hdfs://namenode:9000/checkpoints/`. Always set stable checkpoint paths for fault recovery.
- Serving views: precomputed Parquet under `/data/batch_views/` — this is the canonical interface for any serving/query job.
- CSV/JSON exports (in notebooks) are saved with `utf-8-sig` and JSON `force_ascii=False` for correct Vietnamese text handling.

**How to run locally (developer workflow)**
- Start the local stack (PowerShell):
```powershell
docker compose up -d --build
```
- Confirm services: Zookeeper, Kafka, namenode, datanode, spark-master, spark-worker.
- To run the event producer from the host (uses `localhost:9092`):
```powershell
python -m pip install -r requirements.txt
python producer.py
```
- To run the ingestion streaming job (host Spark or containerized Spark). Using `spark-submit` from host (requires Spark installed) with Kafka package and HDFS configs:
```powershell
# example spark-submit from host (adjust Spark home / bin path as needed)
%spark_home%\bin\spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf spark.hadoop.dfs.client.use.datanode.hostname=true \
  ingest_layer.py
```
- When running inside docker-compose Spark image, ensure the job uses `kafka:29092` as bootstrap server. For interactive debugging, run Python scripts inside the container or build a job image.
- To run the stream (speed) layer:
```powershell
# similar spark-submit pattern
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf spark.hadoop.dfs.client.use.datanode.hostname=true \
  stream_layer.py
```
- To run the batch job (precompute views):
```powershell
spark-submit batch_layer.py
```

**Integration and external dependencies**
- Apache Kafka: topics used by producers: `auth_topic`, `assessment_topic`, `video_topic`, `course_topic`, `profile_topic` (see `producer.py`).
- HDFS: accessible at `hdfs://namenode:9000` from containers. Data locations:
  - Raw ingestion: `/data/master_dataset/`
  - Checkpoints: `/checkpoints/ingest/`
  - Batch views: `/data/batch_views/` (subpaths `student_submissions`, `daily_logins`, `engagement_performance`)
- Spark: jobs rely on the Kafka connector package (specified above).
- NoSQL: not yet integrated — repository currently writes serving data to Parquet; add Mongo/Cassandra if you implement a serving database.

**Missing pieces & immediate next steps (recommended)**
- `serving_layer.py` is empty — implement a serving/query component that reads precomputed Parquet views and exposes them (REST API or writes into a NoSQL store). This is required to complete the Lambda flow.
- Add a manifest for CI or containerized Spark run scripts (Kubernetes manifests are expected by the course; current repo uses docker-compose for local dev). Plan migration steps to K8s for final submission.
- Add explicit `spark-submit` example scripts (wrappers) or Dockerfile for running each job inside a container; document when to use `localhost:9092` vs `kafka:29092`.
- Implement or document exactly-once semantics where required: streaming + sinks and checkpoint strategies.

**Troubleshooting tips (common gotchas)**
- If Spark can't connect to Kafka, check which bootstrap server to use (host vs container) and `KAFKA_ADVERTISED_LISTENERS` in `docker-compose.yml`.
- If HDFS write fails from Spark, ensure `spark.hadoop.dfs.client.use.datanode.hostname=true` (used in scripts) and that HDFS containers are healthy.
- Logs: Spark UI on `http://localhost:8080`, HDFS Namenode web UI on `http://localhost:9870`.

**If you are editing code, prefer small targeted PRs**
- When adding new streaming jobs, include a small README with:
  - expected input topics and message schema
  - checkpoint path used
  - output HDFS path or sink details

**Ask for feedback**
- If any repo areas are unclear (missing schemas, intended serving DB, or expected evaluation queries), mention them in the PR description so reviewers/agents can update the instructions.

---
End of instructions — please tell me which part you want expanded (runbooks, K8s deployment examples, or serving layer templates).
