# producer.py
import json
import time
import requests
import psycopg2
from kafka import KafkaProducer
import os
import logging

from requests.auth import HTTPBasicAuth

'''
todo 
* consume username/password as envs
* create row for db if one is not there
* see about fault tolerance? 
* create a version of this producer for the cloud which pulls documents as well... can have it running from the other machine...
'''
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s:%(lineno)d - %(message)s')
logger = logging.getLogger(__name__)

COUCHDB_URL = os.getenv("COUCHDB_URL")
DB_NAME = os.getenv("DB_NAME")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
POSTGRES_URL = os.getenv("POSTGRES_URL")


def get_last_seq_id(database):
    last_seq = None
    try:
        # Use 'with' statement to handle connection and cursor automatically
        with psycopg2.connect(POSTGRES_URL) as conn:
            logger.info(f"connection parameters: {conn.get_dsn_parameters()}")
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT seq_id FROM mydb.db_sequence_id WHERE database = %s;", (database,))
                last_seq = cursor.fetchone()
    except Exception as e:
        logger.error(f"Error occurred getting last seq_id: {e}")
        time.sleep(10)

    return last_seq[0] if last_seq else None


def save_dummy_seq_id(database):
    logger.info(f"Saving dummy seq_id for database: {database}")
    try:
        # Use 'with' statement to handle connection and cursor automatically
        with psycopg2.connect(POSTGRES_URL) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO mydb.db_sequence_id (database, seq_id) VALUES (%s, %s)",
                    (database, None)
                )
                logger.info(f"Saved dummy seq_id for database: {database}")
    except Exception as e:
        logger.error(f"Error occurred saving dummy seq last seq_id: {e}")
        time.sleep(10)


def save_seq_id(database, seq_id):
    logger.info(f"Saving sequence id for database {database}")
    try:
        # Use 'with' statement to handle connection and cursor automatically
        with psycopg2.connect(POSTGRES_URL) as conn:
            with conn.cursor() as cursor:
                update_query = """
                    UPDATE mydb.db_sequence_id
                    SET seq_id = %s
                    WHERE database = %s;
                """
                cursor.execute(update_query, (seq_id, database))
                conn.commit()
                logger.info(f"Saved database {database} seq_id: {seq_id}")
                time.sleep(1)
    except Exception as e:
        logger.error(f"Error occurred saving seq seq_id: {e}")

'''
{
  "version": "0.0.1",
  "current_utc_epoch_ms": "1744050907728",
  "data": {
    "seq": "4837005-g1AAAACReJzLYWBgYMpgTmHgzcvPy09JdcjLz8gvLskBCScyJNX___8_K4M5iUHlhVYuUIw91cQ8OTHJFF09DhPyWIAkQwOQ-g836Fky2CBDC1Pj5JQUdG1ZAJr5LYY",
    "id": "20250407152725-3103329",
    "changes": [
      {
        "rev": "1-b9657f4caf57a555c4bcbc958c962793"
      }
    ]
  }
}
'''
def process_change(change_data, counter, save_frequency=1000):
    message = {"version": "0.0.1",
               "database" : DB_NAME,
               "current_utc_epoch_ms": str(time.time_ns() // 1_000_000),
               "data": change_data}
    producer.send(KAFKA_TOPIC, message)
    last_seq_id = change_data.get("seq")
    if counter % save_frequency == 0:
        logger.info(f"Reached frequency {save_frequency}, saving to db: {change_data}, last_seq_id {last_seq_id}")
        save_seq_id(DB_NAME, last_seq_id)
        logger.info(f"Sent message to Kafka: {message}")
    else:
        logger.debug(f"Sent message to Kafka: {change_data}")
    counter = (counter + 1) % save_frequency
    return last_seq_id, counter


def listen_to_changes(url, last_seq_id):
    """Listens to the CouchDB _changes feed in continuous mode."""
    params = {"feed": "continuous", "include_docs": "false", "heartbeat": "5000"}
    if last_seq_id is not None:
        params["since"] = last_seq_id
    counter = 0
    with requests.get(url, stream=True, auth=HTTPBasicAuth("admin", "password"), verify=False,
                      params=params) as response:
        if response.status_code == 200:
            for line in response.iter_lines():
                if line:
                    try:
                        change_data = json.loads(line.decode('utf-8'))
                        last_seq_id, counter = process_change(change_data, counter)
                    except Exception as e:
                        print(f"Error processing change: {str(e)}")
        else:
            print(f"Failed to connect: {response.status_code} - {response.text}")
            return False


producer = KafkaProducer(
    bootstrap_servers=["kafka1:9092", "kafka2:9093", "kafka3:9094"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

logger.info("Kafka Producer started")
while True:
    last_seq_id = get_last_seq_id(DB_NAME)
    logger.info(f"Starting from seq_id: {last_seq_id}")
    if last_seq_id is None:
        save_dummy_seq_id(DB_NAME)
    url = f"{COUCHDB_URL}/{DB_NAME}/_changes"
    logger.info(f"Using URL {url}")
    try:
        listen_to_changes(url, last_seq_id)
    except Exception as e:
        logger.error(f"Error in producer: {e}")
    time.sleep(5)  # Prevent excessive requests
