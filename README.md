# BTLbigdata-group30

Hệ thống thu thập, lưu trữ, phân tích và xử lý kết quả học tập của sinh viên để dự đoán điểm số

## 📋 Tuần 5 - Phân chia công việc

### Ingestion Layer

- **Kafka streaming**: Thịnh, Phú, Tiến
- **Batch ingestion to HDFS**: Lâm, Lộc

**Mục tiêu**: Trong 1 tuần phải xong ingestion layer

---

## 📚 Kafka Learning Resources

### For Streaming Team (Thịnh, Phú, Tiến)

**Start Here**: [`kafka/README.md`](kafka/README.md)

**Learning Path** (1 week):

1. **Day 1-2**: Understand Kafka basics (`kafka/README.md` sections 1-2)
2. **Day 3-4**: Complete tutorials (`kafka/01-basic-producer-consumer/`, `kafka/02-json-messages/`)
3. **Day 5-6**: Implement project examples (`kafka/project-examples/`)
4. **Day 7**: Integration testing & documentation

**Key Files**:

- 📖 `kafka/README.md` - Complete learning guide
- 🎯 `kafka/01-basic-producer-consumer/` - Your first Kafka app
- 📊 `kafka/02-json-messages/` - Working with structured data
- 🚀 `kafka/project-examples/` - Production-ready code for our project

**What You'll Build**:

- Student activity producer (send events to Kafka)
- Spark Structured Streaming consumer (process events in real-time)
- Integration with MongoDB (store processed data)

---

## 🗂️ Project Structure

```
BTLbigdata-group30/
├── kafka/                          # Kafka learning & examples (NEW!)
│   ├── README.md                   # Complete Kafka guide
│   ├── 01-basic-producer-consumer/ # Tutorial 1
│   ├── 02-json-messages/           # Tutorial 2
│   ├── 03-partitions/              # Tutorial 3 (coming soon)
│   ├── 04-consumer-groups/         # Tutorial 4 (coming soon)
│   └── project-examples/           # Production code
│       ├── student_activity_producer.py
│       ├── attendance_producer.py
│       └── spark_streaming_consumer.py
│
├── generate_fake_data/             # Data generation (existing)
├── problem-definition.md           # Project requirements
├── architecture-design.md          # System architecture
├── deployment-guide.md             # Setup instructions
└── docker-compose.yml              # Local development environment
```

---

## 🚀 Quick Start for Kafka Team

---

## 🛠 Installation & Setup Instructions

Follow these steps strictly in order to set up the Lambda Architecture environment.

### 1. Prerequisites

- **Python 3.8 - 3.11** (Avoid 3.12+ for now due to PySpark compatibility).
- **Docker Desktop** (Running).
- **Amazon Corretto JDK 8 or 11** installed (Ensure `JAVA_HOME` is set automatically by the installer).

### 2. Python Environment Setup

Create a virtual environment to keep dependencies isolated.

**Windows:**

```bash
python -m venv venv
.\venv\Scripts\activate
```

**Mac/Linux:**

```bash
python3 -m venv venv
source venv/bin/activate
```

**Install Dependencies:**

```bash
pip install -r requirements.txt
```

### 3. Network Configuration (Crucial)

You must map the Docker container hostnames to your local machine so PySpark can talk to HDFS and Kafka.

**Windows:**
Open Notepad as Administrator and edit: `C:\Windows\System32\drivers\etc\hosts`

**Mac/Linux:**
Open Terminal and edit: `sudo nano /etc/hosts`

**Add this line to the bottom of the file:**

```text
127.0.0.1 namenode datanode kafka
```

### 4. Start Infrastructure (Docker)

Spin up Zookeeper, Kafka, and Hadoop (HDFS).

```bash
docker-compose up -d
```

> ⏳ **Wait 1-2 minutes** after this command for the NameNode and DataNode to fully initialize (Safemode OFF).

---

## 🚀 Execution Guide (Pipeline Order)

Open multiple terminal tabs/windows to run the components of the Lambda Architecture.

#### Terminal 1: Data Source (Simulation)

Start generating fake events to Kafka. Keep this running.

```bash
python producer.py
```

#### Terminal 2: Speed Layer (Real-time)

Process data directly from Kafka streams.

```bash
python stream_layer.py
```

#### Terminal 3: Ingest Layer (Data Lake)

Capture data from Kafka and save it to HDFS (Simulating "Immutable Master Data").

```bash
python ingest_layer.py
```

> _Note: Let this run for 10-20 seconds to capture enough data, then stop it (Ctrl+C). It should automatically upload the data to HDFS._

#### Terminal 4: Batch Layer (Historical Processing)

Once data is on HDFS (from the previous step), run the batch job to create pre-computed views (Parquet files).

```bash
python batch_layer.py
```

#### Terminal 5: Serving Layer (Query)

Query the final unified view (merging Batch Views + Real-time Views).

```bash
python serving_layer.py
```

---

### 🧹 Cleanup

To stop the containers and free up resources:

```bash
docker-compose down
```

---

## 📞 Support

- **Questions about Kafka?** → Check `kafka/README.md` or ask in team chat
- **Stuck on a tutorial?** → Review the code comments (detailed explanations)
- **Need help?** → Contact team leads

---

**Next Milestone**: Ingestion layer complete by end of Week 5! 🎯
