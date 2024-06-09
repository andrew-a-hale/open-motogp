CREATE TABLE IF NOT EXISTS dwh.dim_rider (
    id VARCHAR PRIMARY KEY,
    name VARCHAR,
    country VARCHAR,
    team VARCHAR,
    number INT,
    timestamp TIMESTAMP
)