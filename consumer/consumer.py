# consumer.py
import json
import psycopg2
from kafka import KafkaConsumer
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
POSTGRES_URL = os.getenv("POSTGRES_URL")

def get_postgres_connection():
    return psycopg2.connect(POSTGRES_URL)

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=["kafka1:9092", "kafka2:9093", "kafka3:9094"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

logger.info("Kafka Consumer started")

for message in consumer:
    try:
        data = message.value
        conn = get_postgres_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_data (
                id SERIAL PRIMARY KEY,
                data JSONB NOT NULL
            );
        """)
        cursor.execute("INSERT INTO processed_data (data) VALUES (%s)", (json.dumps(data),))
        conn.commit()
        cursor.close()
        conn.close()
        consumer.commit()
        logger.info(f"Consumed and stored message: {data}")
    except Exception as e:
        logger.error(f"Error in consumer: {e}")