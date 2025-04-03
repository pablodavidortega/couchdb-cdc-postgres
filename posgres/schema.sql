CREATE TABLE IF NOT EXISTS db_sequence_id (
    database TEXT PRIMARY KEY,
    seq_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_data (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL
);
