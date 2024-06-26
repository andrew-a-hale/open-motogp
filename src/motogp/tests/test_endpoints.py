from typing import AsyncGenerator, List, assert_type

import pytest

from motogp.endpoints import (
    get_seasons,
    get_events,
    get_categories,
    get_sessions,
    get_classification,
    async_get_sessions,
    async_get_classification,
)
from motogp.model import (
    Season,
    Event,
    Category,
    Session,
    Classification,
    Task,
)
from motogp.database import setup_duckdb, setup_sqlite

setup_duckdb(True)
setup_sqlite(True)


# Integration Tests
class TestEndpoints:
    # change to fixtures
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
    async def test_async_get_sessions(cls):
        assert_type(
            await async_get_sessions(cls.event.id, cls.category.id),
            AsyncGenerator[any, Session],
        )

    @pytest.mark.asyncio
    async def test_get_classification(cls):
        task = Task(
            "0",
            cls.season.id,
            cls.event.id,
            cls.category.id,
            cls.session.id,
        )
        classification = await async_get_classification(task)
        assert_type(classification, Classification)

    def test_get_classification(cls):
        task = Task(
            "0",
            cls.season.id,
            cls.event.id,
            cls.category.id,
            cls.session.id,
        )
        classification = get_classification(task)
        assert_type(classification, Classification)
