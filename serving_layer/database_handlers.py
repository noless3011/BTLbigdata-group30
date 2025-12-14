"""
Database Handlers for Cassandra and MongoDB
Handles connections and operations for both databases
"""

import os
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# MongoDB
try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
    MONGO_AVAILABLE = True
except ImportError:
    MONGO_AVAILABLE = False
    logger.warning("pymongo not available. MongoDB features disabled.")

# Cassandra
try:
    from cassandra.cluster import Cluster, Session
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.query import SimpleStatement, ConsistencyLevel
    from cassandra.policies import DCAwareRoundRobinPolicy
    CASSANDRA_AVAILABLE = True
except ImportError:
    CASSANDRA_AVAILABLE = False
    logger.warning("cassandra-driver not available. Cassandra features disabled.")


class MongoDBHandler:
    """Handler for MongoDB operations"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.client = None
        self.db = None
        self.connected = False
        
        if not MONGO_AVAILABLE:
            logger.warning("MongoDB handler initialized but pymongo not available")
            return
        
        self._connect()
    
    def _connect(self):
        """Establish MongoDB connection"""
        try:
            mongo_config = self.config["mongodb"]
            connection_string = f"mongodb://{mongo_config['username']}:{mongo_config['password']}@{mongo_config['host']}:{mongo_config['port']}/"
            
            self.client = MongoClient(
                connection_string,
                serverSelectionTimeoutMS=5000
            )
            
            # Test connection
            self.client.admin.command('ping')
            
            self.db = self.client[mongo_config["database"]]
            self.connected = True
            logger.info(f"Connected to MongoDB: {mongo_config['host']}:{mongo_config['port']}")
            
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error(f"Failed to connect to MongoDB: {str(e)}")
            self.connected = False
        except Exception as e:
            logger.error(f"MongoDB connection error: {str(e)}")
            self.connected = False
    
    def is_connected(self) -> bool:
        """Check if MongoDB is connected"""
        if not self.connected or self.client is None:
            return False
        
        try:
            self.client.admin.command('ping')
            return True
        except:
            self.connected = False
            return False
    
    def get_collection(self, collection_name: str):
        """Get a MongoDB collection"""
        if not self.is_connected():
            return None
        return self.db[collection_name]
    
    def insert_batch_view_metadata(self, view_name: str, metadata: Dict[str, Any]):
        """Insert batch view metadata"""
        collection = self.get_collection(self.config["mongodb"]["collections"]["batch_views_metadata"])
        if collection is None:
            return False
        
        try:
            metadata["view_name"] = view_name
            metadata["updated_at"] = datetime.now()
            collection.update_one(
                {"view_name": view_name},
                {"$set": metadata},
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error inserting batch view metadata: {str(e)}")
            return False
    
    def get_batch_view_metadata(self, view_name: str) -> Optional[Dict[str, Any]]:
        """Get batch view metadata"""
        collection = self.get_collection(self.config["mongodb"]["collections"]["batch_views_metadata"])
        if collection is None:
            return None
        
        try:
            return collection.find_one({"view_name": view_name})
        except Exception as e:
            logger.error(f"Error getting batch view metadata: {str(e)}")
            return None
    
    def cache_query_result(self, query_key: str, result: Dict[str, Any], ttl_seconds: int = 300):
        """Cache query result in MongoDB"""
        collection = self.get_collection(self.config["mongodb"]["collections"]["query_cache"])
        if collection is None:
            return False
        
        try:
            cache_entry = {
                "query_key": query_key,
                "result": result,
                "created_at": datetime.now(),
                "expires_at": datetime.now().timestamp() + ttl_seconds
            }
            collection.update_one(
                {"query_key": query_key},
                {"$set": cache_entry},
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error caching query result: {str(e)}")
            return False
    
    def get_cached_query(self, query_key: str) -> Optional[Dict[str, Any]]:
        """Get cached query result"""
        collection = self.get_collection(self.config["mongodb"]["collections"]["query_cache"])
        if collection is None:
            return None
        
        try:
            cached = collection.find_one({"query_key": query_key})
            if cached and cached.get("expires_at", 0) > datetime.now().timestamp():
                return cached.get("result")
            elif cached:
                # Expired, remove it
                collection.delete_one({"query_key": query_key})
            return None
        except Exception as e:
            logger.error(f"Error getting cached query: {str(e)}")
            return None
    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            self.connected = False


class CassandraHandler:
    """Handler for Cassandra operations"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.cluster = None
        self.session = None
        self.connected = False
        
        if not CASSANDRA_AVAILABLE:
            logger.warning("Cassandra handler initialized but cassandra-driver not available")
            return
        
        self._connect()
        self._setup_keyspace()
        self._setup_tables()
    
    def _connect(self):
        """Establish Cassandra connection"""
        try:
            cassandra_config = self.config["cassandra"]
            hosts = cassandra_config["hosts"]
            
            self.cluster = Cluster(
                hosts,
                port=cassandra_config["port"],
                protocol_version=4
            )
            
            self.session = self.cluster.connect()
            self.connected = True
            logger.info(f"Connected to Cassandra: {hosts}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {str(e)}")
            self.connected = False
    
    def is_connected(self) -> bool:
        """Check if Cassandra is connected"""
        if not self.connected or self.session is None:
            return False
        
        try:
            self.session.execute("SELECT now() FROM system.local")
            return True
        except:
            self.connected = False
            return False
    
    def _setup_keyspace(self):
        """Create keyspace if not exists"""
        if not self.is_connected():
            return
        
        try:
            cassandra_config = self.config["cassandra"]
            keyspace = cassandra_config["keyspace"]
            replication = cassandra_config["replication"]
            
            create_keyspace_query = f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace}
            WITH REPLICATION = {{
                'class': '{replication["class"]}',
                'replication_factor': {replication.get("replication_factor", 1)}
            }}
            """
            
            self.session.execute(create_keyspace_query)
            self.session.set_keyspace(keyspace)
            logger.info(f"Keyspace '{keyspace}' ready")
            
        except Exception as e:
            logger.error(f"Error setting up keyspace: {str(e)}")
    
    def _setup_tables(self):
        """Create tables if not exist"""
        if not self.is_connected():
            return
        
        try:
            cassandra_config = self.config["cassandra"]
            tables = cassandra_config["tables"]
            
            # Batch views table
            create_batch_views = f"""
            CREATE TABLE IF NOT EXISTS {tables["batch_views"]} (
                view_name text,
                partition_key text,
                data text,
                updated_at timestamp,
                PRIMARY KEY (view_name, partition_key)
            )
            """
            
            # Speed views table (for real-time data)
            create_speed_views = f"""
            CREATE TABLE IF NOT EXISTS {tables["speed_views"]} (
                view_name text,
                timestamp timestamp,
                data text,
                PRIMARY KEY (view_name, timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC)
            """
            
            # Merged views table (cached merged results)
            create_merged_views = f"""
            CREATE TABLE IF NOT EXISTS {tables["merged_views"]} (
                query_key text,
                data text,
                created_at timestamp,
                expires_at timestamp,
                PRIMARY KEY (query_key)
            )
            """
            
            # Query results cache
            create_query_results = f"""
            CREATE TABLE IF NOT EXISTS {tables["query_results"]} (
                query_key text,
                result text,
                created_at timestamp,
                expires_at timestamp,
                PRIMARY KEY (query_key)
            )
            """
            
            self.session.execute(create_batch_views)
            self.session.execute(create_speed_views)
            self.session.execute(create_merged_views)
            self.session.execute(create_query_results)
            
            logger.info("Cassandra tables ready")
            
        except Exception as e:
            logger.error(f"Error setting up tables: {str(e)}")
    
    def insert_batch_view(self, view_name: str, partition_key: str, data: str):
        """Insert batch view data into Cassandra"""
        if not self.is_connected():
            return False
        
        try:
            table = self.config["cassandra"]["tables"]["batch_views"]
            query = f"""
            INSERT INTO {table} (view_name, partition_key, data, updated_at)
            VALUES (?, ?, ?, ?)
            """
            
            self.session.execute(
                query,
                (view_name, partition_key, data, datetime.now())
            )
            return True
        except Exception as e:
            logger.error(f"Error inserting batch view: {str(e)}")
            return False
    
    def get_batch_view(self, view_name: str, partition_key: Optional[str] = None, limit: Optional[int] = None) -> Optional[List[Dict[str, Any]]]:
        """Get batch view data from Cassandra"""
        if not self.is_connected():
            return None
        
        try:
            table = self.config["cassandra"]["tables"]["batch_views"]
            
            if partition_key:
                query = f"SELECT * FROM {table} WHERE view_name = ? AND partition_key = ?"
                if limit:
                    query += f" LIMIT {limit}"
                rows = self.session.execute(query, (view_name, partition_key))
            else:
                query = f"SELECT * FROM {table} WHERE view_name = ?"
                if limit:
                    query += f" LIMIT {limit}"
                rows = self.session.execute(query, (view_name,))
            
            results = []
            for row in rows:
                results.append({
                    "view_name": row.view_name,
                    "partition_key": row.partition_key,
                    "data": row.data,
                    "updated_at": row.updated_at
                })
            
            return results
        except Exception as e:
            logger.error(f"Error getting batch view: {str(e)}")
            return None
    
    def insert_speed_view(self, view_name: str, data: str, timestamp: Optional[datetime] = None):
        """Insert speed view (real-time) data into Cassandra"""
        if not self.is_connected():
            return False
        
        try:
            if timestamp is None:
                timestamp = datetime.now()
            
            table = self.config["cassandra"]["tables"]["speed_views"]
            query = f"""
            INSERT INTO {table} (view_name, timestamp, data)
            VALUES (?, ?, ?)
            """
            
            self.session.execute(query, (view_name, timestamp, data))
            return True
        except Exception as e:
            logger.error(f"Error inserting speed view: {str(e)}")
            return False
    
    def get_speed_view(self, view_name: str, limit: int = 100) -> Optional[List[Dict[str, Any]]]:
        """Get recent speed view data from Cassandra"""
        if not self.is_connected():
            return None
        
        try:
            table = self.config["cassandra"]["tables"]["speed_views"]
            query = f"SELECT * FROM {table} WHERE view_name = ? LIMIT ?"
            
            rows = self.session.execute(query, (view_name, limit))
            
            results = []
            for row in rows:
                results.append({
                    "view_name": row.view_name,
                    "timestamp": row.timestamp,
                    "data": row.data
                })
            
            return results
        except Exception as e:
            logger.error(f"Error getting speed view: {str(e)}")
            return None
    
    def cache_query_result(self, query_key: str, result: str, ttl_seconds: int = 300):
        """Cache query result in Cassandra"""
        if not self.is_connected():
            return False
        
        try:
            table = self.config["cassandra"]["tables"]["query_results"]
            expires_at = datetime.now().timestamp() + ttl_seconds
            
            query = f"""
            INSERT INTO {table} (query_key, result, created_at, expires_at)
            VALUES (?, ?, ?, ?)
            """
            
            self.session.execute(
                query,
                (query_key, result, datetime.now(), datetime.fromtimestamp(expires_at))
            )
            return True
        except Exception as e:
            logger.error(f"Error caching query result: {str(e)}")
            return False
    
    def get_cached_query(self, query_key: str) -> Optional[str]:
        """Get cached query result from Cassandra"""
        if not self.is_connected():
            return None
        
        try:
            table = self.config["cassandra"]["tables"]["query_results"]
            query = f"SELECT * FROM {table} WHERE query_key = ?"
            
            row = self.session.execute(query, (query_key,)).one()
            
            if row and row.expires_at > datetime.now():
                return row.result
            elif row:
                # Expired, delete it
                delete_query = f"DELETE FROM {table} WHERE query_key = ?"
                self.session.execute(delete_query, (query_key,))
            
            return None
        except Exception as e:
            logger.error(f"Error getting cached query: {str(e)}")
            return None
    
    def close(self):
        """Close Cassandra connection"""
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()
        self.connected = False

