# producer.py
import json
import time
import requests
import psycopg2
from kafka import KafkaProducer
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS db_sequence_id (
            database TEXT PRIMARY KEY,
            seq_id TEXT NOT NULL
        );
    """) #todo probably here we want to change it to have the primary key be db_name rather than some serial key
    cursor.execute(f"SELECT seq_id FROM sequence_id where database = '{database}' ORDER BY id DESC LIMIT 1;")
    last_seq = cursor.fetchone()
    cursor.close()
    conn.close()
    return last_seq[0] if last_seq else "0"

def save_seq_id(database, seq_id):
    conn = get_postgres_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO db_sequence_id (database, seq_id) VALUES (%s)", (database,seq_id))
    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"Saved database {database} seq_id: {seq_id}")

producer = KafkaProducer(
    bootstrap_servers=["kafka1:9092", "kafka2:9093", "kafka3:9094"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

logger.info("Kafka Producer started")

last_seq_id = get_last_seq_id(DB_NAME)
logger.info(f"Starting from seq_id: {last_seq_id}")
url = f"{COUCHDB_URL}/{DB_NAME}/_changes"
logger.info(f"Using URL {url}")
save_frequency = 100
counter = 0
while True:
    try:
        response = requests.get(COUCHDB_URL, params={"feed": "continuous", "since": last_seq_id, "include_docs": "true"}, stream=True)
        for line in response.iter_lines():
            if line:
                data = json.loads(line)
                producer.send(KAFKA_TOPIC, data)
                last_seq_id = data.get("seq")
                if counter % save_frequency == 0:
                    logger.info(f"Reached frequency {save_frequency}, saving to db")
                    save_seq_id(DB_NAME, data.get("seq"))
                logger.debug(f"Sent message to Kafka: {data}")
        time.sleep(5)  # Prevent excessive requests
    except Exception as e:
        logger.error(f"Error in producer: {e}")