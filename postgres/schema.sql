-- Ensure the schema exists
CREATE SCHEMA IF NOT EXISTS mydb;

-- Create the `db_sequence_id` table inside `mydb` schema
CREATE TABLE IF NOT EXISTS mydb.db_sequence_id (
    database TEXT PRIMARY KEY,
    seq_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS mydb.processed_data (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL
);
