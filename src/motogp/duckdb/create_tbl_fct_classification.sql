CREATE OR REPLACE SEQUENCE classification_id START 1;

CREATE TABLE IF NOT EXISTS dwh.fct_classification (
    id INT PRIMARY KEY DEFAULT nextval('classification_id'),
    season_id VARCHAR,
    event_id VARCHAR,
    category_id VARCHAR,
    session_id VARCHAR,
    rider_id VARCHAR,
    position INT,
    points INT,
    timestamp TIMESTAMP
);