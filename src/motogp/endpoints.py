import datetime
from typing import List
import functools

import httpx
import requests
from motogp.model import Classification, Season, Event, Category, Session

# get year, event, category, sessions, and classification
BASE_URL = "https://api.pulselive.motogp.com/motogp/v1/results"
SEASONS_ENDPOINT = BASE_URL + "/seasons"
EVENTS_ENDPOINT_TEMPL = BASE_URL + "/events?seasonUuid={season}&isFinished=true"
CATEGORIES_ENDPOINT_TEMPL = BASE_URL + "/categories?eventUuid={event}"
SESSIONS_ENDPOINT_TEMPL = (
    BASE_URL + "/sessions?eventUuid={event}&categoryUuid={category}"
)
CLASSIFICATION_ENDPOINT_TEMPL = (
    BASE_URL + "/session/{session}/classification?test=false"
)


def get_seasons(incremental: bool = True) -> List[Season]:
    res = requests.get(SEASONS_ENDPOINT)
    res.raise_for_status()
    seasons = res.json()
    assert len(seasons) > 0

    seasons = [Season.from_req(s) for s in seasons]
    seasons.sort(key=lambda x: x.year)
    if incremental:
        ts = Season.last_season_timestamp()
        new_seasons = [x.id for x in seasons if x.year >= ts.date().year]
        seasons = [s for s in seasons if s.id in new_seasons]

    return seasons


def get_events(season_id: str, incremental: bool = True) -> List[Event]:
    res = requests.get(EVENTS_ENDPOINT_TEMPL.format(season=season_id))
    res.raise_for_status()
    events = res.json()
    assert len(events) > 0

    events = [Event.from_req(e) for e in events]
    events.sort(key=lambda x: x.start)
    if incremental:
        ts = Event.last_event_timestamp()
        new_events = [x.id for x in events if x.start >= ts.date()]
        events = [e for e in events if e.id in new_events]

    return events


@functools.cache
def get_categories(event_id: str) -> List[Category]:
    res = requests.get(CATEGORIES_ENDPOINT_TEMPL.format(event=event_id))
    res.raise_for_status()
    categories = res.json()
    assert len(categories) > 0
    return [Category.from_req(c) for c in categories]


def get_sessions(event_id: str, category_id: str) -> List[Session]:
    res = requests.get(
        SESSIONS_ENDPOINT_TEMPL.format(event=event_id, category=category_id)
    )
    res.raise_for_status()
    sessions = res.json()
    assert len(sessions) > 0
    return [Session.from_req(s) for s in sessions]


async def get_classification(task) -> Classification:
    client = httpx.AsyncClient()
    res = await client.get(
        CLASSIFICATION_ENDPOINT_TEMPL.format(session=task.session_id)
    )
    res.raise_for_status()
    res = res.json()
    assert len(res) > 0
    return Classification.from_req(res, task)
