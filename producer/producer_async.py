# producer_async.py
import os
import asyncio
import json
import logging
import time
import traceback

import aiohttp
import aiokafka
import asyncpg
from aiohttp import ClientSession
from typing import Optional
import uvloop

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s:%(lineno)d - %(message)s')
logger = logging.getLogger(__name__)

COUCHDB_URL = os.getenv("COUCHDB_URL")
DB_NAMES = os.getenv("DB_NAMES", "").split(",")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
POSTGRES_URL = os.getenv("POSTGRES_URL")
COUCHDB_USERNAME = os.getenv("COUCHDB_USERNAME", "admin")
COUCHDB_PASSWORD = os.getenv("COUCHDB_PASSWORD", "password")
SAVE_FREQUENCY = int(os.getenv("SAVE_FREQUENCY", "1000"))

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9093,kafka3:9094").split(",")


async def get_last_seq_id(pool, database: str) -> Optional[str]:
    async with pool.acquire() as conn:
        try:
            result = await conn.fetchrow("SELECT seq_id FROM mydb.db_sequence_id WHERE database = $1", database)
            return result['seq_id'] if result else None
        except Exception as e:
            logger.error(f"Error fetching seq_id: {e}")
            return None


async def save_dummy_seq_id(pool, database: str):
    async with pool.acquire() as conn:
        try:
            await conn.execute("INSERT INTO mydb.db_sequence_id (database, seq_id) VALUES ($1, NULL)", database)
            logger.info(f"Saved dummy seq_id for {database}")
        except Exception as e:
            logger.error(f"Error inserting dummy seq_id: {e}")


async def save_seq_id(pool, database: str, seq_id: str):
    async with pool.acquire() as conn:
        try:
            await conn.execute(
                "UPDATE mydb.db_sequence_id SET seq_id = $1 WHERE database = $2",
                seq_id, database
            )
            logger.info(f"Updated seq_id for {database}")
        except Exception as e:
            logger.error(f"Error updating seq_id: {e}")


async def process_change(producer, pool, database: str, change_data: dict, counter: int):
    message = {
        "version": "0.0.1",
        "database": database,
        "current_utc_epoch_ms": str(time.time_ns() // 1_000_000),
        "data": change_data
    }
    await producer.send_and_wait(KAFKA_TOPIC, json.dumps(message).encode("utf-8"))
    last_seq_id = change_data.get("seq")

    if counter % SAVE_FREQUENCY == 0 and last_seq_id:
        await save_seq_id(pool, database, last_seq_id)
        logger.info(f"Saved seq_id at count {counter} for {database}")

    return last_seq_id, (counter + 1)


async def listen_to_changes(session: ClientSession, producer, pool, database: str):
    last_seq_id = await get_last_seq_id(pool, database)
    if last_seq_id is None:
        await save_dummy_seq_id(pool, database)
        last_seq_id = None

    url = f"{COUCHDB_URL}/{database}/_changes"
    params = {"feed": "continuous", "include_docs": "false", "heartbeat": "5000"}
    if last_seq_id:
        params["since"] = last_seq_id

    auth = aiohttp.BasicAuth(COUCHDB_USERNAME, COUCHDB_PASSWORD)
    counter = 0

    logger.info(f"Listening to changes for {database} starting from {last_seq_id}")

    try:
        async with session.get(url, auth=auth, params=params, timeout=None, ssl=False) as resp:
            async for line in resp.content:
                if line:
                    try:
                        decoded_line = line.decode('utf-8').strip()
                        if not decoded_line:
                            continue  # skip heartbeats
                        change_data = json.loads(decoded_line)
                        last_seq_id, counter = await process_change(producer, pool, database, change_data, counter)
                    except Exception as e:
                        logger.error(f"Error processing change for {database}: {e}")
                        traceback.print_exc()
    except Exception as e:
        logger.error(f"Error connecting to {url}: {e}")


async def main():
    logger.info("Starting producer...")

    # Init PostgreSQL connection pool
    pool = await asyncpg.create_pool(dsn=POSTGRES_URL)

    # Init Kafka Producer
    producer = aiokafka.AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    await producer.start()

    async with aiohttp.ClientSession() as session:
        tasks = [
            listen_to_changes(session, producer, pool, db.strip())
            for db in DB_NAMES if db.strip()
        ]

        await asyncio.gather(*tasks)

    await producer.stop()
    await pool.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown signal received. Exiting...")
