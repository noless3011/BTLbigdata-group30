import os
import json
import logging
from pyspark.sql import SparkSession

from pymongo import MongoClient

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("serving_layer")


def get_mongo_client():
    uri = os.environ.get("MONGO_URI")
    if not uri:
        # inside docker-compose use the service name 'mongo'
        uri = "mongodb://root:example@mongo:27017/"
    return MongoClient(uri)


def write_df_to_mongo(df, db, collection_name, client):
    # Convert rows to dicts via toJSON -> parse, avoids toPandas
    log.info(f"Collecting records for collection={collection_name}...")
    json_rows = df.toJSON().collect()
    if not json_rows:
        log.info(f"No records found for {collection_name}, skipping write.")
        return

    docs = [json.loads(r) for r in json_rows]

    coll = client[db][collection_name]
    # Simple replace strategy: remove existing then insert fresh batch
    log.info(f"Replacing {len(docs)} documents into {db}.{collection_name} ...")
    coll.delete_many({})
    coll.insert_many(docs)
    log.info("Write complete.")


def main():
    spark = SparkSession.builder.appName("ServingLayer") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    hdfs_base = os.environ.get("HDFS_BASE", "hdfs://namenode:9000/data/batch_views/")
    views = {
        "student_submissions": os.path.join(hdfs_base, "student_submissions"),
        "daily_logins": os.path.join(hdfs_base, "daily_logins"),
        "engagement_performance": os.path.join(hdfs_base, "engagement_performance"),
    }

    client = get_mongo_client()
    db_name = os.environ.get("SERVING_DB", "serving_db")

    for name, path in views.items():
        try:
            log.info(f"Reading view {name} from {path}")
            df = spark.read.parquet(path)
            write_df_to_mongo(df, db_name, name, client)
        except Exception as e:
            log.warning(f"Could not process view {name} at {path}: {e}")

    spark.stop()


if __name__ == "__main__":
    main()
