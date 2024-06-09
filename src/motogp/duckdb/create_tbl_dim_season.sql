CREATE TABLE IF NOT EXISTS dwh.dim_season (
    id VARCHAR PRIMARY KEY,
    year INT,
    timestamp TIMESTAMP
)