CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    season_id TEXT,
    event_id TEXT,
    category_id TEXT,
    session_id TEXT,
    status INTEGER DEFAULT 0,
    attempt INTEGER DEFAULT 0,
    added_timestamp INTEGER,
    updated_timestamp INTEGER
)