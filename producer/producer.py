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
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
POSTGRES_URL = os.getenv("POSTGRES_URL")

def get_postgres_connection():
    return psycopg2.connect(POSTGRES_URL)

def save_seq_id(seq_id):
    conn = get_postgres_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sequence_id (
            id SERIAL PRIMARY KEY,
            seq_id TEXT NOT NULL
        );
    """)# we can probably remove this and put it in the sql file that gets booted with the docker postgres
    cursor.execute("INSERT INTO sequence_id (seq_id) VALUES (%s)", (seq_id,))
    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"Saved seq_id: {seq_id}")

producer = KafkaProducer(
    bootstrap_servers=["kafka1:9092", "kafka2:9093", "kafka3:9094"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

logger.info("Kafka Producer started")

while True:
    try:
        response = requests.get(COUCHDB_URL, params={"feed": "continuous", "since": "now", "include_docs": "true"}, stream=True)
        for line in response.iter_lines():
            if line:
                data = json.loads(line)
                producer.send(KAFKA_TOPIC, data)
                save_seq_id(data.get("seq"))
                logger.info(f"Sent message to Kafka: {data}")
        logger.info(f"Waiting 5 seconds before retrying a new connection")
        time.sleep(5)  # Prevent excessive requests
    except Exception as e:
        logger.error(f"Error in producer: {e}")