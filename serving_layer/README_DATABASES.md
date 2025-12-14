# Database Integration Guide - Cassandra & MongoDB

## Tổng quan

Serving Layer sử dụng **Cassandra** và **MongoDB** để lưu trữ và xử lý dữ liệu:

- **Cassandra**: Lưu trữ batch views và speed views, hỗ trợ query nhanh
- **MongoDB**: Lưu trữ metadata, query cache, và operational data

## Kiến trúc

```
┌─────────────────────────────────────────────────┐
│           Serving Layer API                     │
└──────────────────┬──────────────────────────────┘
                   │
        ┌──────────┴──────────┐
        │                     │
   ┌────▼────┐          ┌─────▼─────┐
   │Cassandra│          │  MongoDB  │
   │         │          │           │
   │ Batch   │          │ Metadata  │
   │ Views   │          │ Cache     │
   │ Speed   │          │ Config    │
   │ Views   │          │           │
   └─────────┘          └───────────┘
```

## Cassandra

### Mục đích
- Lưu trữ batch views (dữ liệu lịch sử đã được xử lý)
- Lưu trữ speed views (dữ liệu real-time)
- Cache query results
- Hỗ trợ query nhanh với partition key

### Schema

#### Keyspace: `learning_analytics`

```cql
CREATE KEYSPACE learning_analytics
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};
```

#### Tables

1. **batch_views** - Batch views storage
```cql
CREATE TABLE batch_views (
    view_name text,
    partition_key text,
    data text,
    updated_at timestamp,
    PRIMARY KEY (view_name, partition_key)
);
```

2. **speed_views** - Real-time views
```cql
CREATE TABLE speed_views (
    view_name text,
    timestamp timestamp,
    data text,
    PRIMARY KEY (view_name, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
```

3. **merged_views** - Cached merged results
```cql
CREATE TABLE merged_views (
    query_key text,
    data text,
    created_at timestamp,
    expires_at timestamp,
    PRIMARY KEY (query_key)
);
```

4. **query_results** - Query cache
```cql
CREATE TABLE query_results (
    query_key text,
    result text,
    created_at timestamp,
    expires_at timestamp,
    PRIMARY KEY (query_key)
);
```

### Usage

```python
from serving_layer.database_handlers import CassandraHandler
from serving_layer.config import serving_config

handler = CassandraHandler(serving_config)

# Insert batch view
handler.insert_batch_view("auth_daily_active_users", "2025-12-15", json.dumps(data))

# Get batch view
data = handler.get_batch_view("auth_daily_active_users", "2025-12-15")

# Insert speed view
handler.insert_speed_view("auth_realtime_active_users_5min", json.dumps(data))

# Get speed view
realtime_data = handler.get_speed_view("auth_realtime_active_users_5min", limit=100)
```

## MongoDB

### Mục đích
- Lưu trữ metadata của batch views
- Cache query results
- Lưu trữ user profiles
- System configuration

### Collections

1. **batch_views_metadata** - Metadata của batch views
```json
{
  "view_name": "auth_daily_active_users",
  "last_updated": "2025-12-15T10:00:00Z",
  "row_count": 150,
  "size_bytes": 1024000,
  "schema": {...}
}
```

2. **query_cache** - Cached query results
```json
{
  "query_key": "student_SV001_engagement",
  "result": {...},
  "created_at": "2025-12-15T10:00:00Z",
  "expires_at": 1734264000
}
```

3. **user_profiles** - User profile data
4. **system_config** - System configuration

### Usage

```python
from serving_layer.database_handlers import MongoDBHandler
from serving_layer.config import serving_config

handler = MongoDBHandler(serving_config)

# Insert metadata
handler.insert_batch_view_metadata("auth_daily_active_users", {
    "last_updated": datetime.now(),
    "row_count": 150
})

# Get metadata
metadata = handler.get_batch_view_metadata("auth_daily_active_users")

# Cache query
handler.cache_query_result("student_SV001", {"data": ...}, ttl_seconds=300)

# Get cached query
cached = handler.get_cached_query("student_SV001")
```

## Configuration

### Environment Variables

```bash
# MongoDB
MONGODB_HOST=localhost
MONGODB_PORT=27017
MONGODB_USERNAME=root
MONGODB_PASSWORD=example
MONGODB_DATABASE=learning_analytics

# Cassandra
CASSANDRA_HOSTS=localhost
CASSANDRA_PORT=9042
CASSANDRA_KEYSPACE=learning_analytics
```

### Config File

Xem `serving_layer/config.py` để cấu hình chi tiết.

## Setup

### 1. Start Docker Containers

```bash
docker-compose up -d cassandra mongo
```

### 2. Wait for Services

```bash
# Check Cassandra
docker exec cassandra nodetool status

# Check MongoDB
docker exec mongo mongosh --eval "db.adminCommand('ping')"
```

### 3. Install Dependencies

```bash
pip install -r serving_layer/requirements.txt
```

### 4. Test Connection

```python
from serving_layer.database_handlers import MongoDBHandler, CassandraHandler
from serving_layer.config import serving_config

# Test MongoDB
mongo = MongoDBHandler(serving_config)
print(f"MongoDB connected: {mongo.is_connected()}")

# Test Cassandra
cassandra = CassandraHandler(serving_config)
print(f"Cassandra connected: {cassandra.is_connected()}")
```

## Data Flow

### Batch Views

1. Batch Layer tạo batch views → Lưu vào MinIO (Parquet)
2. Serving Layer đọc từ MinIO → Lưu vào Cassandra (để query nhanh)
3. Metadata lưu vào MongoDB

### Speed Views

1. Speed Layer tạo speed views → Lưu trực tiếp vào Cassandra
2. Serving Layer query từ Cassandra (real-time)

### Query Flow

1. Client request → Serving Layer API
2. Check cache (MongoDB hoặc Cassandra)
3. Nếu cache miss:
   - Đọc batch view từ Cassandra
   - Đọc speed view từ Cassandra
   - Merge views
   - Cache result
4. Return merged result

## Performance Tips

1. **Cassandra Partitioning**: Sử dụng partition key hợp lý (date, user_id, etc.)
2. **MongoDB Indexing**: Tạo index cho query_key, view_name
3. **Caching**: Set TTL phù hợp (300s cho query cache)
4. **Batch Operations**: Insert nhiều records cùng lúc

## Troubleshooting

### Cassandra không connect được

```bash
# Check container
docker ps | grep cassandra

# Check logs
docker logs cassandra

# Test connection
docker exec cassandra cqlsh -e "SELECT now() FROM system.local"
```

### MongoDB không connect được

```bash
# Check container
docker ps | grep mongo

# Check logs
docker logs mongo

# Test connection
docker exec mongo mongosh --eval "db.adminCommand('ping')"
```

### Connection timeout

- Kiểm tra ports: 9042 (Cassandra), 27017 (MongoDB)
- Kiểm tra firewall
- Kiểm tra credentials

## Next Steps

1. ✅ Setup Cassandra và MongoDB
2. ⏳ Migrate batch views từ MinIO sang Cassandra
3. ⏳ Implement speed views writing to Cassandra
4. ⏳ Add indexing và optimization
5. ⏳ Monitor performance

