from typing import List, assert_type

import pytest

from motogp.endpoints import (
    get_categories,
    get_classification,
    get_events,
    get_seasons,
    get_sessions,
)
from motogp.model import Category, Event, Season, Session, Task, Classification
from motogp.database import setup_duckdb, setup_sqlite

setup_duckdb(True)
setup_sqlite(True)


# Integration Tests
class TestEndpoints:
    season = get_seasons()[0]
    event = get_events(season.id)[0]
    category = get_categories(event.id)[0]
    session = get_sessions(event.id, category.id)[0]

    def test_get_seasons(cls):
        assert_type(cls.season, List[Season])

    def test_get_events(cls):
        assert_type(cls.event, List[Event])

    def test_get_categories(cls):
        assert_type(cls.category, List[Category])

    def test_get_sessions(cls):
        assert_type(cls.session, List[Session])

    @pytest.mark.asyncio
    async def test_get_classification(cls):
        task = Task(
            "0",
            cls.season.id,
            cls.event.id,
            cls.category.id,
            cls.session.id,
        )
        classification = await get_classification(task)
        assert_type(classification, Classification)
