from postgresql_client import PostgresSQLClient
from dotenv import load_dotenv
from models import Log
import logging
import pandas as pd
import os
from time import sleep


load_dotenv()
logging.basicConfig(level=logging.INFO)

# SAMPLE_DATA_PATH = os.path.join(
#     os.path.dirname(__file__), "data", "standard_log.csv"
# )
SAMPLE_DATA_PATH = "./data/standard_log.csv"

def load_data(limit=None):
    try:
        logging.info(f"Loading data from {SAMPLE_DATA_PATH} (limit={limit})")
        if limit and limit > 0:
            df = pd.read_csv(SAMPLE_DATA_PATH, nrows=limit)
        else:
            # df = pd.read_parquet(SAMPLE_DATA_PATH, engine="fastparquet")
            df = pd.read_csv(SAMPLE_DATA_PATH)
        records = []
        for _, row in df.iterrows():
            record = Log(
                user_id = int(row['userid']),
                id = int(row['id']),
                event_name = str(row['eventname']),
                component = str(row['component']),
                action = str(row['action']),
                target = str(row['target']),
                object_table = None if pd.isna(row['objecttable']) else str(row['objecttable']),
                object_id = None if pd.isna(row['objectid']) else int(row['objectid']),
                crud = str(row['crud']),
                edu_level = int(row['edulevel']),
                context_id = int(row['contextid']),
                context_level = int(row['contextlevel']),
                context_instance_id = int(row['contextinstanceid']),
                course_id = int(row['courseid']),
                anonymous = '1' if row['anonymous'] else '0',
                time_created = int(row['timecreated']),
                origin = str(row['origin']),
                ip = str(row['ip']),
                related_user_id = None if pd.isna(row['relateduserid']) else int(row['relateduserid']),
                real_user_id = None if pd.isna(row['realuserid']) else int(row['realuserid'])
            )
            records.append(record)
        logging.info(f"Succesfully loaded {len(records)} records from {SAMPLE_DATA_PATH}")

        return records
        
    except Exception as e:
        logging.error(f"Error loading data {str(e)}")
        raise



def main():
    logging.info("Inserting data....")
    pc = PostgresSQLClient(
        port=os.getenv("POSTGRES_PORT"),
        database=os.getenv("POSTGRES_DB"),
        host=os.getenv("POSTGRES_HOST"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )
    logging.info("Successfully connected to PostgresSQL database")

    try:
        limit = int(os.getenv("RECORD_LIMIT"))
    except ValueError:
        logging.warning(f"Invalid RECORD_LIMIT, ignoring and loading all rows")

    logging.info(f"Record limit: {limit}")
    records = load_data(limit=limit)
    valid_records = 0
    invalid_records = 0

    session = pc.get_session()

    
    batch_size = 100
    current_batch = []

    def commit_batch(batch):
        nonlocal valid_records, invalid_records
        try:
            session.bulk_save_objects(batch)
            session.commit()
            valid_records += len(batch)
            logging.info(f"Processed {valid_records} valid records")
            sleep(0.5)
        except Exception as e:
            logging.error(f"Failed to insert batch of {len(batch)}: {str(e)}")
            invalid_records += len(batch)
            session.rollback()
        finally:
            batch.clear()

    logging.info("Starting record insertion")
    
    for record in records:
        current_batch.append(record)
        if len(current_batch) >= batch_size:
            commit_batch(current_batch)

    if current_batch:
        commit_batch(current_batch)
        valid_records += len(current_batch)
    
    session.close()
    logging.info("Finished record insertion\nFinal Summary:")
    logging.info(f"Total records processed: {len(records)}")
    logging.info(f"Valid records inserted: {valid_records}")
    logging.warning(f"Invalid records skipped: {invalid_records}")


if __name__ == "__main__":
    main()


