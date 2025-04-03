# producer.py
import json
import time
import requests
import psycopg2
from kafka import KafkaProducer
import os
import logging

from requests.auth import HTTPBasicAuth

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s:%(lineno)d')
logger = logging.getLogger(__name__)

COUCHDB_URL = os.getenv("COUCHDB_URL")
DB_NAME = os.getenv("DB_NAME")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
POSTGRES_URL = os.getenv("POSTGRES_URL")

def get_postgres_connection():
    return psycopg2.connect(POSTGRES_URL)

def get_last_seq_id(database):
    conn = get_postgres_connection()
    cursor = conn.cursor()
    cursor.execute('SET search_path TO mydb;')
    # cursor.execute("""
    #     CREATE TABLE IF NOT EXISTS db_sequence_id (
    #         database TEXT PRIMARY KEY,
    #         seq_id TEXT NOT NULL
    #     );
    # """) #todo probably here we want to change it to have the primary key be db_name rather than some serial key
    cursor.execute(f"SELECT seq_id FROM db_sequence_id where database = '{database}';")
    last_seq = cursor.fetchone()
    cursor.close()
    conn.close()
    return last_seq[0] if last_seq else None

def save_seq_id(database, seq_id):
    conn = get_postgres_connection()
    cursor = conn.cursor()
    cursor.execute('SET search_path TO mydb;')
    cursor.execute("INSERT INTO db_sequence_id (database, seq_id) VALUES (%s,%s)", (database,seq_id))
    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"Saved database {database} seq_id: {seq_id}")

def process_change(change_data, counter, save_frequency = 100):
    producer.send(KAFKA_TOPIC, change_data)
    if counter % save_frequency == 0:
        logger.info(f"Reached frequency {save_frequency}, saving to db: {change_data}")
    last_seq_id = change_data.get("seq")
    save_seq_id(DB_NAME, last_seq_id)
    logger.debug(f"Sent message to Kafka: {change_data}")
    counter = (counter + 1)%save_frequency
    return last_seq_id, counter

def listen_to_changes(url, last_seq_id):
    """Listens to the CouchDB _changes feed in continuous mode."""
    params = {"feed": "continuous", "include_docs": "false", "heartbeat" : "5000"}
    if last_seq_id is not None:
        params["since"] = last_seq_id
    counter = 0
    with requests.get(url, stream=True, auth=HTTPBasicAuth("admin", "password"), verify=False, params=params) as response:
        if response.status_code == 200:
            for line in response.iter_lines():
                if line:
                    try:
                        change_data = line.decode('utf-8')
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
    url = f"{COUCHDB_URL}/{DB_NAME}/_changes"
    logger.info(f"Using URL {url}")
    try:
        listen_to_changes(url, last_seq_id)
    except Exception as e:
        logger.error(f"Error in producer: {e}")
    time.sleep(5)  # Prevent excessive requests


#
# while True:
#     try:
#         params = {"feed": "continuous", "include_docs": "false"}
#         if last_seq_id is not None:
#             params["since"] = last_seq_id
#         response = requests.get(COUCHDB_URL, params=params, stream=True)
#         for line in response.iter_lines():
#             if line:
#                 data = json.loads(line)
#                 producer.send(KAFKA_TOPIC, data)
#                 last_seq_id = data.get("seq")
#                 if counter % save_frequency == 0:
#                     logger.info(f"Reached frequency {save_frequency}, saving to db: {data}")
#                     save_seq_id(DB_NAME, data.get("seq"))
#                 logger.debug(f"Sent message to Kafka: {data}")
#     except Exception as e:
#         logger.error(f"Error in producer: {e}")
#     time.sleep(5)  # Prevent excessive requests


