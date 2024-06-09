from dataclasses import dataclass
import datetime
from enum import Enum
import re
from typing import Dict, List
from motogp.database import setup_duckdb, setup_sqlite


class CategoryParseError(Exception):
    pass


@dataclass
class Season:
    id: str
    year: int

    @staticmethod
    def last_season_timestamp():
        conn = setup_duckdb()
        res = conn.execute(
            "SELECT MAX(timestamp) AS ts FROM dwh.dim_season"
        ).fetchone()[0]
        conn.close()

        if res:
            return res
        else:
            return datetime.datetime.min

    @staticmethod
    def from_req(json: Dict):
        return Season(json.get("id"), json.get("year"))  # type: ignore

    @staticmethod
    def from_db(id: str):
        conn = setup_duckdb()
        res = conn.execute(
            "SELECT id, year FROM dwh.dim_season WHERE id = ?", [id]
        ).fetchone()
        conn.close()
        return Season(*res)

    def sync(self):
        conn = setup_duckdb()
        conn.execute(
            """\
INSERT OR IGNORE INTO dwh.dim_season (id, year, timestamp)
VALUES ($id, $year, current_timestamp)""",
            {"id": self.id, "year": self.year},
        )
        conn.commit()
        conn.close()


@dataclass
class Event:
    id: str
    name: str
    short_name: str
    start: datetime.date
    end: datetime.date

    @staticmethod
    def last_event_timestamp():
        conn = setup_duckdb()
        res = conn.execute(
            "SELECT MAX(timestamp) AS ts FROM dwh.dim_event",
        ).fetchone()[0]
        conn.close()

        if res:
            return res
        else:
            return datetime.datetime.min

    @staticmethod
    def from_req(json: Dict):
        return Event(
            json.get("id"),
            json.get("name"),
            json.get("short_name").lower(),
            datetime.date.fromisoformat(json.get("date_start")),
            datetime.date.fromisoformat(json.get("date_end")),
        )

    @staticmethod
    def from_db(id: str):
        conn = setup_duckdb()
        res = conn.execute(
            "SELECT id, name, short_name, date_start, date_end FROM dwh.dim_event WHERE id = ?",
            [id],
        ).fetchone()
        conn.close()
        return Event(*res)

    def sync(self):
        conn = setup_duckdb()
        conn.execute(
            """\
INSERT OR IGNORE INTO dwh.dim_event (id, name, short_name, date_start, date_end, timestamp)
VALUES ($id, $name, $short_name, $date_start, $date_end, current_timestamp)""",
            {
                "id": self.id,
                "name": self.name,
                "short_name": self.short_name,
                "date_start": self.start,
                "date_end": self.end,
            },
        )
        conn.commit()
        conn.close()


@dataclass
class Category:
    id: str
    name: str

    @staticmethod
    def from_req(json: Dict):
        name = json.get("name")
        matches = re.search("[A-Za-z0-9]+", name)  # type: ignore
        if matches:
            return Category(json.get("id"), matches.group(0).lower())  # type: ignore

        raise CategoryParseError(f"Failed to parse Category from {name}")

    @staticmethod
    def from_db(id: str):
        conn = setup_duckdb()
        res = conn.execute(
            "SELECT id, name FROM dwh.dim_category WHERE id = ?", [id]
        ).fetchone()
        conn.close()
        return Category(*res)

    def sync(self):
        conn = setup_duckdb()
        conn.execute(
            """\
INSERT OR IGNORE INTO dwh.dim_category (id, name, timestamp)
VALUES ($id, $name, current_timestamp)""",
            {
                "id": self.id,
                "name": self.name,
            },
        )
        conn.commit()
        conn.close()


@dataclass
class Session:
    id: str
    name: str

    @staticmethod
    def from_req(json: Dict):
        type = json.get("type")
        number = json.get("number")

        if number:
            name = f"{type}{number}"
        else:
            name = type

        return Session(json.get("id"), name.lower())  # type: ignore

    @staticmethod
    def from_db(id: str):
        conn = setup_duckdb()
        res = conn.execute(
            "SELECT id, name FROM dwh.dim_session WHERE id = ?", [id]
        ).fetchone()
        conn.commit()
        conn.close()
        return Session(*res)

    def sync(self):
        conn = setup_duckdb()
        conn.execute(
            """\
INSERT OR IGNORE INTO dwh.dim_session (id, name, timestamp)
VALUES ($id, $name, current_timestamp)""",
            {
                "id": self.id,
                "name": self.name,
            },
        )
        conn.commit()
        conn.close()


@dataclass
class Rider:
    id: str
    name: str
    country: str
    team: str
    number: int

    @staticmethod
    def from_db(id: str):
        conn = setup_duckdb()
        res = conn.execute(
            "SELECT id, name, country, team, number FROM dwh.dim_rider WHERE id = ?",
            [id],
        ).fetchone()
        conn.close()
        return Rider(res[0], res[1], res[2], res[3], res[4])

    def sync(self):
        conn = setup_duckdb()
        conn.execute(
            """\
INSERT INTO dwh.dim_rider (id, name, country, team, number, timestamp)
VALUES ($id, $name, $country, $team, $number, current_timestamp)
ON CONFLICT DO UPDATE
SET name = $name, country = $country, team = $team, number = $number, timestamp = get_current_timestamp()""",
            {
                "id": self.id,
                "name": self.name,
                "country": self.country,
                "team": self.team,
                "number": self.number,
            },
        )
        conn.commit()
        conn.close()


@dataclass
class RiderResult:
    rider: Rider
    position: int
    points: int

    @staticmethod
    def from_req(json: Dict):
        rider = Rider(
            json.get("rider").get("id"),
            json.get("rider").get("full_name"),
            json.get("rider").get("country").get("name"),
            json.get("team").get("name"),
            json.get("rider").get("number"),
        )
        pos = json.get("position")
        pts = json.get("point")
        return RiderResult(rider, pos, pts)


class TaskStatus(Enum):
    NEW = 0
    QUEUED = 1
    COMPLETED = 2
    ERROR = 3


@dataclass
class Task:
    id: str
    season_id: str
    event_id: str
    category_id: str
    session_id: str
    status: TaskStatus = TaskStatus.NEW
    attempt: int = 0

    @staticmethod
    def from_db(id: str):
        conn = setup_sqlite()
        task = conn.execute(
            """\
SELECT id, season_id, event_id, category_id, session_id, status, attempt
FROM tasks
WHERE id = ?""",
            [id],
        ).fetchone()
        conn.close()

        return Task(
            task[0],
            task[1],
            task[2],
            task[3],
            task[4],
            TaskStatus(task[5]),
            task[6],
        )

    def upsert_status(self, status: TaskStatus) -> None:
        conn = setup_sqlite()
        res = conn.execute(
            """\
INSERT INTO tasks (id, season_id, event_id, category_id, session_id, added_timestamp)
VALUES (:id, :season_id, :event_id, :category_id, :session_id, current_timestamp)
ON CONFLICT DO UPDATE
SET status = :status, attempt = attempt + 1, updated_timestamp = current_timestamp
RETURNING status""",
            {
                "id": self.id,
                "season_id": self.season_id,
                "event_id": self.event_id,
                "category_id": self.category_id,
                "session_id": self.session_id,
                "status": status.value,
            },
        ).fetchone()
        conn.commit()
        conn.close()

        self.status = TaskStatus(res[0])


@dataclass
class Classification:
    season_id: str
    event_id: str
    category_id: str
    session_id: str
    results: List[RiderResult]

    @staticmethod
    def from_req(json: Dict, task: Task):
        classification = json.get("classification")
        assert classification is not None
        results = [RiderResult.from_req(item) for item in classification]
        return Classification(
            task.season_id, task.event_id, task.category_id, task.session_id, results
        )

    @staticmethod
    def from_db(session_id: str):
        conn = setup_duckdb()
        res = conn.execute(
            """\
SELECT 
    season_id,
    event_id,
    category_id,
    session_id,
    rider_id,
    position,
    points
FROM dwh.fct_classification
WHERE session_id = ?""",
            [session_id],
        ).fetchall()
        conn.close()

        season_id = res[0][0]
        event_id = res[0][1]
        category_id = res[0][2]

        rider_results = []
        for row in res:
            rider = Rider.from_db(row[4])
            rider_results.append(RiderResult(rider, row[5], row[6]))

        return Classification(
            season_id, event_id, category_id, session_id, rider_results
        )

    def sync(self):
        conn = setup_duckdb()
        rows = []
        for result in self.results:
            result.rider.sync()
            rows.append(
                [
                    self.season_id,
                    self.event_id,
                    self.category_id,
                    self.session_id,
                    result.rider.id,
                    result.position,
                    result.points,
                ]
            )

        conn.executemany(
            """\
INSERT OR REPLACE INTO dwh.fct_classification (season_id, event_id, category_id, session_id, rider_id, position, points, timestamp)
VALUES (?, ?, ?, ?, ?, ?, ?, current_timestamp)""",
            rows,
        )
        conn.commit()
        conn.close()


@dataclass
class TaskQueue:
    tasks: List[Task]
    cursor: int = 0
    size: int = 0

    @staticmethod
    def from_list(tasks):
        return TaskQueue(tasks, 0, len(tasks))

    def sync(self):
        records = []
        for task in self.tasks:
            records.append(
                {
                    "id": task.id,
                    "season_id": task.season_id,
                    "event_id": task.event_id,
                    "category_id": task.category_id,
                    "session_id": task.session_id,
                    "status": task.status.value,
                    "attempt": task.attempt,
                }
            )

        conn = setup_sqlite()
        conn.executemany(
            """\
INSERT INTO tasks (
    id,
    season_id,
    event_id,
    category_id,
    session_id,
    added_timestamp
) VALUES (:id, :season_id, :event_id, :category_id, :session_id, current_timestamp)
ON CONFLICT DO UPDATE
SET status = :status, attempt = :attempt, updated_timestamp = current_timestamp""",
            records,
        )
        conn.commit()
        conn.close()

    @staticmethod
    def from_db():
        query = """\
SELECT
    id,
    season_id,
    event_id,
    category_id,
    session_id,
    status,
    attempt
FROM tasks
WHERE status < 2"""

        conn = setup_sqlite()
        res = conn.execute(query).fetchall()
        conn.close()

        tasks = []
        for row in res:
            tasks.append(
                Task(
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    TaskStatus(row[5]),
                    row[6],
                )
            )

        return TaskQueue.from_list(tasks)
