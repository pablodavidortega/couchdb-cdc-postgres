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
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                insert_query,
                (
                    message["database"],  # Make sure 'database' is included in the message
                    message["doc_id"],
                    epoch_ms_to_utc(message["change_ts"]),
                    message["seq_id"],
                    message["rev"],
                    Json(message["document"]),  # Handles JSONB insertion properly
                )
            )
        conn.commit()
        return True
    except Exception as e:
        # Log or print the error if you want
        print(f"Error inserting processed data: {e}")
        return False


logger.info("Kafka Consumer started")

# Main consumer loop
def consume_messages():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=["kafka1:9092", "kafka2:9093", "kafka3:9094"],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset='earliest',
        enable_auto_commit=False  # Manual commit mode
    )

    with consumer, psycopg2.connect(POSTGRES_URL) as conn:
        logger.info("Consumer and DB connection established. Waiting for messages...")
        for message in consumer:
            try:
                data = message.value
                is_success = insert_processed_data(conn, data)

                if is_success:
                    # Manually commit the offset after successful DB commit
                    consumer.commit()
                    logger.debug(f"Successfully processed and committed message: {data}")
                else:
                    logger.warning(f"Insert failed, message will be reprocessed: {data}")

            except Exception as e:
                logger.error(f"Failed to process message: {e}")

if __name__ == "__main__":
    consume_messages()