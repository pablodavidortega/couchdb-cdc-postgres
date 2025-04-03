# consumer.py
import json
import psycopg2
from kafka import KafkaConsumer
import os

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
POSTGRES_URL = os.getenv("POSTGRES_URL")

def get_postgres_connection():
    return psycopg2.connect(POSTGRES_URL)

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=["kafka1:9092", "kafka2:9093", "kafka3:9094"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

for message in consumer:
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
