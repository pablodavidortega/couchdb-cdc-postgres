# consumer.py
import json
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import Json
from kafka import KafkaConsumer
import os
import logging

'''
todo it could be worth it do add batchign here 
in a scenario where we are using monarch messaging to scoot all of this up to ECRT for example, we would want to use batching to send up a large chunk of messages
batching could be done in batches of 1000
currently we have 365 bytes per message (including seq number which isnt really useful outside of the edge), we would need to add store number/cluster as well for roughly 250-300B
'''
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s:%(lineno)d - %(message)s')
logger = logging.getLogger(__name__)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
POSTGRES_URL = os.getenv("POSTGRES_URL")


# Helper: Get UTC timestamp from epoch milliseconds
def epoch_ms_to_utc(ms):
    return datetime.fromtimestamp(float(ms) / 1000.0, tz=timezone.utc)


# Database insert function
def insert_processed_data(conn, message):
    insert_query = """
        INSERT INTO mydb.processed_data (database, doc_id, change_ts, seq_id, rev, document)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (database, doc_id, rev) DO NOTHING;
    """
    try:
        with conn.cursor() as cursor:
            document = message.get("document", None)
            if len(message["data"]["changes"]) > 1:
                logger.info(f"this guy has more than one changes entry! {message["data"]}")
            cursor.execute(
                insert_query,
                (
                    message["database"],  # Make sure 'database' is included in the message
                    message["data"]["id"],
                    epoch_ms_to_utc(message["current_utc_epoch_ms"]),
                    message["data"]["seq"],
                    message["data"]["changes"][0]["rev"],
                    document,  # Handles JSONB insertion properly
                )
            )
        conn.commit()
        return True
    except Exception as e:
        # Log or print the error if you want
        logger.error(f"Error inserting processed data: {e}")
        return False


# Function to process a single Kafka message
def process_message(conn, message):
    data = message.value
    retries = 3
    success = False

    # Retry logic for message processing
    for _ in range(retries):
        if insert_processed_data(conn, data):
            success = True
            break
        else:
            logger.warning(f"Retrying message: {data}...")
            time.sleep(2)  # Sleep before retrying

    if not success:
        logger.error(f"Failed to process message after {retries} retries: {data}")

    return success


# Main consumer loop
# Function to consume and process Kafka messages
def consume_messages():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=["kafka1:9092", "kafka2:9093", "kafka3:9094"],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset='earliest',
        enable_auto_commit=False,  # Manual commit
        group_id='cdc_group1'
    )

    # Create database connection
    with psycopg2.connect(POSTGRES_URL) as conn:
        logger.info("Consumer and DB connection established. Waiting for messages...")
        try:
            for message in consumer:
                if process_message(conn, message):
                    consumer.commit()
                    logger.debug(f"Successfully processed and committed message: {message.value}")
                else:
                    logger.warning(f"Insert failed for message, will reprocess: {message.value}")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            consumer.close()
            logger.info("Kafka consumer closed.")


if __name__ == "__main__":
    logger.info("Kafka Consumer started")
    consume_messages()
