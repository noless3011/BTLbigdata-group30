# Serving Layer Configuration

import os

serving_config = {
    # API Configuration
    "api": {
        "host": "0.0.0.0",
        "port": 8000,
        "reload": True,
        "title": "Learning Analytics API",
        "version": "1.0.0"
    },
    
    # Data Paths
    "paths": {
        "batch_views": "s3a://bucket-0/batch_views",
        "speed_views": "s3a://bucket-0/realtime_views",
        "master_dataset": "s3a://bucket-0/master_dataset"
    },
    
    # MinIO Configuration
    "minio": {
        "endpoint": os.getenv("MINIO_ENDPOINT", "http://localhost:9002"),
        "access_key": "minioadmin",
        "secret_key": "minioadmin",
        "bucket": "bucket-0"
    },
    
    # Kafka Configuration (for speed views)
    "kafka": {
        "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "topics": {
            "auth": "auth_topic",
            "assessment": "assessment_topic",
            "video": "video_topic",
            "course": "course_topic",
            "profile": "profile_topic",
            "notification": "notification_topic"
        }
    },
    
    # MongoDB Configuration (for operational data and metadata)
    "mongodb": {
        "host": os.getenv("MONGODB_HOST", "localhost"),
        "port": int(os.getenv("MONGODB_PORT", "27017")),
        "username": os.getenv("MONGODB_USERNAME", "root"),
        "password": os.getenv("MONGODB_PASSWORD", "example"),
        "database": os.getenv("MONGODB_DATABASE", "learning_analytics"),
        "collections": {
            "batch_views_metadata": "batch_views_metadata",
            "speed_views_cache": "speed_views_cache",
            "user_profiles": "user_profiles",
            "query_cache": "query_cache",
            "system_config": "system_config"
        }
    },
    
    # Cassandra Configuration (for batch views storage and fast queries)
    "cassandra": {
        "hosts": os.getenv("CASSANDRA_HOSTS", "localhost").split(","),
        "port": int(os.getenv("CASSANDRA_PORT", "9042")),
        "keyspace": os.getenv("CASSANDRA_KEYSPACE", "learning_analytics"),
        "replication": {
            "class": "SimpleStrategy",
            "replication_factor": 1
        },
        "consistency_level": "ONE",  # For local development
        "tables": {
            "batch_views": "batch_views",
            "speed_views": "speed_views",
            "merged_views": "merged_views",
            "query_results": "query_results"
        }
    },
    
    # View Merge Configuration
    "merge": {
        "speed_override_batch": True,  # Speed views override batch views for overlapping periods
        "time_window_hours": 24,  # Speed views cover last 24 hours
        "default_merge_key": "date"  # Default key for merging views
    },
    
    # Caching Configuration
    "cache": {
        "enabled": True,
        "ttl_seconds": 300,  # 5 minutes
        "max_size": 1000  # Max cached items
    },
    
    # Mock Data Mode (for development when batch views not available)
    "use_mock_data": os.getenv("USE_MOCK_DATA", "false").lower() == "true",
    
    # Spark Configuration
    "spark": {
        "executor_memory": "2g",
        "executor_cores": 2,
        "num_executors": 2,
        "driver_memory": "1g"
    },
    
    # Logging
    "logging": {
        "level": "INFO",
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    }
}

