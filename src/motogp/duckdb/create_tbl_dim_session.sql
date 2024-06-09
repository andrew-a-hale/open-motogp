CREATE TABLE IF NOT EXISTS dwh.dim_session (
    id VARCHAR PRIMARY KEY,
    name VARCHAR,
    timestamp TIMESTAMP
)