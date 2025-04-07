-- Ensure the schema exists
CREATE SCHEMA IF NOT EXISTS mydb;

-- Create the `db_sequence_id` table inside `mydb` schema
CREATE TABLE IF NOT EXISTS mydb.db_sequence_id (
    database TEXT PRIMARY KEY,
    seq_id TEXT
);

CREATE TABLE IF NOT EXISTS mydb.processed_data (
    database TEXT NOT NULL,
    doc_id TEXT NOT NULL,
    rev TEXT NOT NULL,
    change_ts TIMESTAMP NOT NULL,
    seq_id TEXT NOT NULL,
    document JSONB,
    PRIMARY KEY (database, doc_id, rev)
);
