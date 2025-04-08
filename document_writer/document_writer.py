import os
import json
from time import sleep

import requests
import logging
import datetime
# Configure logging
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, log_level, logging.INFO), format='%(asctime)s - %(levelname)s:%(lineno)d - %(message)s')


# Read environment variables
COUCHDB_URL = os.getenv("COUCHDB_URL", "http://localhost:5984")
COUCHDB_USER = os.getenv("COUCHDB_USER", "admin")
COUCHDB_PASSWORD = os.getenv("COUCHDB_PASSWORD", "password")
DB_NAME = os.getenv("DB_NAME")  # Change this if necessary

# CouchDB authentication
AUTH = (COUCHDB_USER, COUCHDB_PASSWORD)
HEADERS = {"Content-Type": "application/json"}

# Ensure database exists
def ensure_database():
    response = requests.put(f"{COUCHDB_URL}/{DB_NAME}", auth=AUTH)
    if response.status_code not in [200, 201, 412]:
        logging.error(f"Error creating database: {response.text}")
    else:
        logging.info(f"Database '{DB_NAME}' is ready.")

# Function to write incrementing documents
def write_documents():
    current_timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    seq_number = 1
    while True:
        doc_id = f"{current_timestamp}-{seq_number}"
        doc = {"_id": str(doc_id), "message": f"Document {doc_id}"}
        try:
            response = requests.put(f"{COUCHDB_URL}/{DB_NAME}/{doc_id}", auth=AUTH, headers=HEADERS, data=json.dumps(doc))

            if response.status_code in [201, 202]:
                logging.debug(f"Successfully written document ID {doc_id}")
            else:
                logging.error(f"Error writing document {doc_id}: {response.text}")
        except Exception as e:
            sleep_amt = 10
            logging.error(f"Exception happened, sleeping {sleep_amt} seconds: {str(e)}")
            sleep(sleep_amt)

        seq_number += 1

if __name__ == "__main__":
    ensure_database()
    write_documents()
