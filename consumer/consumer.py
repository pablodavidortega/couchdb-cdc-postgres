# consumer.py
import json
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import Json
from kafka import KafkaConsumer
import os
import logging


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s:%(lineno)d - %(message)s')
logger = logging.getLogger(__name__)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
POSTGRES_URL = os.getenv("POSTGRES_URL")


# Helper: Get UTC timestamp from epoch milliseconds
def epoch_ms_to_utc(ms):
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)

# Database insert function
def insert_processed_data(conn, message):
    insert_query = """
        INSERT INTO mydb.processed_data (database, doc_id, change_ts, seq_id, rev, document)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (database, doc_id) DO NOTHING;
    """
    with conn.cursor() as cursor:
        cursor.execute(
            insert_query,
            (
                message["database"], # probably need to add database in the other side as well
                message["doc_id"],
                epoch_ms_to_utc(message["change_ts"]),
                message["seq_id"],
                message["rev"],
                Json(message["document"]), # check if null
            )
        )
        conn.commit()

# consumer = KafkaConsumer(
#     KAFKA_TOPIC,
#     bootstrap_servers=["kafka1:9092", "kafka2:9093", "kafka3:9094"],
#     value_deserializer=lambda v: json.loads(v.decode("utf-8"))
# )

logger.info("Kafka Consumer started")

# Main consumer loop
def consume_messages():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=["kafka1:9092", "kafka2:9093", "kafka3:9094"],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset='earliest',  # Optional: depends on your needs
        enable_auto_commit=False       # Optional
    )

    with consumer, psycopg2.connect(POSTGRES_URL) as conn:
        logger.info("Consumer and DB connection established. Waiting for messages...")
        for message in consumer:
            try:
                data = message.value
                insert_processed_data(conn, data)
                logger.debug(f"Processed message: {data}")
            except Exception as e:
                logger.error(f"Failed to process message: {e}")

if __name__ == "__main__":
    consume_messages()