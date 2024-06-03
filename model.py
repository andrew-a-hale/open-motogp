from dataclasses import dataclass
import datetime
import re
from typing import Iterable, Optional

import duckdb

from database import get_session_status, update_session_status


class CategoryParseError(Exception):
    pass


@dataclass
class Season:
    id: str
    year: int

    @staticmethod
    def from_req(json: dict):
        return Season(json.get("id"), json.get("year"))  # type: ignore


@dataclass
class Event:
    id: str
    name: str
    short_name: str

    @staticmethod
    def from_req(json: dict):
        return Event(json.get("id"), json.get("name"), json.get("short_name").lower())  # type: ignore


@dataclass
class Category:
    id: str
    name: str

    @staticmethod
    def from_req(json: dict):
        name = json.get("name")
        matches = re.search("[A-Za-z0-9]+", name)  # type: ignore
        if matches:
            return Category(json.get("id"), matches.group(0).lower())  # type: ignore

        raise CategoryParseError(f"Failed to parse Category from {name}")


@dataclass
class Session:
    id: str
    name: str

    @staticmethod
    def from_req(json: dict):
        type = json.get("type")
        number = json.get("number")

        if number:
            name = f"{type}{number}"
        else:
            name = type

        return Session(json.get("id"), name.lower())  # type: ignore

    def update_status(self, conn: duckdb.DuckDBPyConnection) -> None:
        update_session_status(self.id, conn)

    def missing(self, conn: duckdb.DuckDBPyConnection) -> bool:
        if not get_session_status(self.id, conn):
            return True

        return False


@dataclass
class RiderResult:
    id: str
    name: str
    position: int
    points: Optional[int]


@dataclass
class Record:
    season: Season
    event: Event
    category: Category
    session: Session
    classification: Iterable[RiderResult]

    def write(self, conn: duckdb.DuckDBPyConnection) -> None:
        records = []
        for rider_result in self.classification:
            records.append(
                [
                    self.season.id,
                    self.season.year,
                    self.event.id,
                    self.event.name,
                    self.event.short_name,
                    self.category.id,
                    self.category.name,
                    self.session.id,
                    self.session.name,
                    rider_result.id,
                    rider_result.name,
                    rider_result.position,
                    rider_result.points,
                    datetime.datetime.now(),
                ]
            )

        conn.executemany(
            """\
INSERT INTO records (
    season_id,
    season_year,
    event_id,
    event_name,
    event_short_name,
    category_id,
    category_name,
    session_id,
    session_name,
    rider_id,
    rider_name,
    rider_position,
    rider_points,
    timestamp
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            records,
        )
