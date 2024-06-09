CREATE TABLE IF NOT EXISTS dwh.dim_event (
    id VARCHAR PRIMARY KEY,
    name VARCHAR,
    short_name VARCHAR,
    date_start DATE,
    date_end DATE,
    timestamp TIMESTAMP
)