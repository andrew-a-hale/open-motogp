import duckdb


def setup_db(fresh: bool = False):
    conn = duckdb.connect("motogp.db")

    if fresh:
        conn.execute("DROP TABLE IF EXISTS sessions")
        conn.execute("DROP TABLE IF EXISTS records")

    query = """\
CREATE TABLE IF NOT EXISTS sessions (
    id VARCHAR PRIMARY KEY,
    status VARCHAR,
    attempt INT
)"""

    conn.execute(query)

    query = """\
CREATE TABLE IF NOT EXISTS records (
    season_id VARCHAR,
    season_year INT,
    event_id VARCHAR,
    event_name VARCHAR,
    event_short_name VARCHAR,
    category_id VARCHAR,
    category_name VARCHAR,
    session_id VARCHAR,
    session_name VARCHAR,
    rider_id VARCHAR,
    rider_name VARCHAR,
    rider_position INT,
    rider_points INT,
    timestamp TIMESTAMP
)"""

    conn.execute(query)

    return conn


def get_session_status(session_id: str, conn: duckdb.DuckDBPyConnection) -> str:
    return conn.execute(
        "SELECT status, attempt FROM sessions WHERE id = ?", [session_id]
    ).fetchone()


def update_session_status(session_id: str, conn: duckdb.DuckDBPyConnection) -> str:
    return conn.execute(
        """\
INSERT INTO sessions (id, status)
VALUES (?, 'COMPLETED')
ON CONFLICT
DO UPDATE SET attempt = attempt + 1""",
        [session_id],
    ).fetchone()
