import datetime
from typing import assert_type
import uuid
from motogp.model import (
    Category,
    Classification,
    Event,
    Rider,
    RiderResult,
    Season,
    Session,
    Task,
    TaskStatus,
    TaskQueue,
)
from motogp.database import setup_duckdb, setup_sqlite

setup_duckdb(True)
setup_sqlite(True)


class TestSeason:
    season = Season("0", 2024)

    def test_sync(cls):
        cls.season.sync()
        assert Season.from_db(cls.season.id) == cls.season

    def test_last_season_timestamp(_):
        assert_type(Season.last_season_timestamp(), datetime.datetime)


class TestEvent:
    event = Event("0", "test", "t", datetime.date.today(), datetime.date.today())

    def test_sync(cls):
        cls.event.sync()
        assert Event.from_db(cls.event.id) == cls.event

    def test_last_event_timestamp(_):
        assert_type(Event.last_event_timestamp(), datetime.datetime)


class TestCategory:
    category = Category("0", "motogp")

    def test_sync(cls):
        cls.category.sync()
        assert Category.from_db(cls.category.id) == cls.category


class TestSession:
    session = Session("0", "test")

    def test_sync(cls):
        cls.session.sync()
        new_session = Session.from_db(cls.session.id)
        assert new_session == cls.session


class TestTask:
    season = Season("0", "test")
    event = Event("0", "test", "t", datetime.date.today(), datetime.date.today())
    category = Category("0", "test")
    session = Session("test", "test")
    task_id = str(uuid.uuid4())
    task = Task(task_id, season.id, event.id, category.id, session.id)
    task_2 = Task("error", season.id, event.id, category.id, session.id)
    task_queue = TaskQueue.from_list([task, task_2])

    def test_upsert_status_new(cls):
        cls.task.upsert_status(TaskStatus.NEW)
        assert cls.task.status == TaskStatus.NEW
        assert cls.task.from_db(cls.task_id).status == TaskStatus.NEW

    def test_upsert_status_queued(cls):
        cls.task.upsert_status(TaskStatus.QUEUED)
        assert cls.task.status == TaskStatus.QUEUED
        assert cls.task.from_db(cls.task_id).status == TaskStatus.QUEUED

    def test_upsert_status_completed(cls):
        cls.task.upsert_status(TaskStatus.COMPLETED)
        assert cls.task.status == TaskStatus.COMPLETED
        assert cls.task.from_db(cls.task_id).status == TaskStatus.COMPLETED

    def test_upsert_status_error(cls):
        cls.task.upsert_status(TaskStatus.ERROR)
        assert cls.task.status == TaskStatus.ERROR
        assert cls.task.from_db(cls.task_id).status == TaskStatus.ERROR

    def test_task_queue_sync(cls):
        cls.task_queue.sync()
        tq = TaskQueue.from_db()
        assert tq.size == cls.task_queue.size
        assert tq.tasks == cls.task_queue.tasks


class TestClassification:
    season_id = "0"
    event_id = "0"
    category_id = "0"
    session_id = "0"
    task_id = str(uuid.uuid4())
    task = Task(task_id, season_id, event_id, category_id, session_id)
    rider = Rider("0", "test", "test", "test", 1)
    rider_result = RiderResult(rider, 1, 1)
    classification = Classification(
        season_id, event_id, category_id, session_id, [rider_result]
    )

    def test_sync(cls):
        cls.rider.sync()
        rider = Rider.from_db(cls.rider.id)
        assert cls.rider == rider

        cls.classification.sync()
        results = Classification.from_db(cls.season_id)
        assert cls.classification == results
