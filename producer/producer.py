import argparse
import json
import os
from time import sleep
import logging

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic

load_dotenv()
logging.basicConfig(level=logging.INFO)

# OUTPUT_TOPICS = os.getenv("KAFKA_OUTPUT_TOPICS")
# BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
OUTPUT_TOPICS = "postgres.public.logs"
BOOTSTRAP_SERVERS = "debezium-cluster-kafka-bootstrap:9092"

parser = argparse.ArgumentParser()
parser.add_argument(
    "-m",
    "--mode",
    default="setup",
    choices=["setup", "teardown"],
    help="Determine the execution mode"
)
parser.add_argument(
    "-b",
    "--bootstrap_servers",
    default=BOOTSTRAP_SERVERS,
    help="Location of the bootstrap server"
)

args = parser.parse_args()

def create_topic(admin, topic_name):
    try:
        topic = NewTopic(name=topic_name, num_partitions=12, replication_factor=1)
        admin.create_topics([topic])
        logging.info(f"Topic {topic_name} has been created")
    except Exception:
        logging.warning(f"Topic {topic_name} already exists. Skipping creation")
        pass

def create_streams(servers):
    producer = None
    admin = None
    logging.info("Instantiating Kafka admin and producer...")
    for _ in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=servers,
                value_serializer=str.encode,
                batch_size=16384,
                buffer_memory=33554432, #32MB
                compression_type="gzip",
                linger_ms=50,
                acks=1       
            )
            admin = KafkaAdminClient(bootstrap_servers=servers)
            logging.info("Instantiated Kafka admin and producer")
            break
        except Exception as e:
            logging.error(f"Attemp to instantiate admin and producer with bootstrap servers {servers} failed with error {e}")
            sleep(10)
            pass

    try:
        df = pd.read_csv("./data/standard_log.csv")
        logging.info(f"Loaded {len(df)} records from standard_log.csv")

        def format_record(row):
            record = {
                'user_id' : int(row['userid']),
                'id' : int(row['id']),
                'event_name' : str(row['eventname']),
                'component' : str(row['component']),
                'action' : str(row['action']),
                'target' : str(row['target']),
                'object_table' : None if pd.isna(row['objecttable']) else str(row['objecttable']),
                'object_id' : None if pd.isna(row['objectid']) else int(row['objectid']),
                'crud' : str(row['crud']),
                'edu_level' : int(row['edulevel']),
                'context_id' : int(row['contextid']),
                'context_level' : int(row['contextlevel']),
                'context_instance_id' : int(row['contextinstanceid']),
                'course_id' : int(row['courseid']),
                'anonymous' : '1' if row['anonymous'] else '0',
                'time_created' : int(row['timecreated']),
                'origin' : str(row['origin']),
                'ip' : str(row['ip']),
                'related_user_id' : None if pd.isna(row['relateduserid']) else int(row['relateduserid']),
                'real_user_id' : None if pd.isna(row['realuserid']) else int(row['realuserid'])
            }
            return json.dumps(record)
        
        logging.info("Formatting records...")
        records = df.apply(format_record, axis=1).tolist()

    except Exception as e:
        logging.error(f"Error loading data file: {e}")
        return
    
    create_topic(admin, topic_name=OUTPUT_TOPICS)

    logging.info("Sending records...")
    for i, record in enumerate(records):
        producer.send(OUTPUT_TOPICS, value=record)

        if i % 1000 == 0:
            logging.info(f"Sent {i} records")

        sleep(0.05)
    
    producer.flush()
    logging.info(f"Finished sending {len(records)} records")

def teardown_stream(topic_name, servers=["localhost:9092"]):
    try:
        admin = KafkaAdminClient(bootstrap_servers=servers)
        logging.info(admin.delete_topics([topic_name]))
        logging.info(f"Topic {topic_name} deleted")
    except Exception as e:
        logging.error(str(e))
    
if __name__ == "__main__":
    print("Running...")
    parsed_args = vars(args)
    mode = parsed_args["mode"]
    servers = parsed_args["bootstrap_servers"]

    logging.info("Tearing down all existing topic...")
    try:
        teardown_stream(OUTPUT_TOPICS, [servers])
    except Exception as e:
        logging.warning(f"Topic does not exist. Skipping...")
    
    if mode == "setup":
        create_streams([servers])



        