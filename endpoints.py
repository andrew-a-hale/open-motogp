from typing import Iterable
import httpx
from model import Season, Event, Category, Session, RiderResult

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


def get_seasons() -> Iterable[Season]:
    res = httpx.get(SEASONS_ENDPOINT)
    res.raise_for_status()
    seasons = res.json()
    assert len(seasons) > 0
    return [Season.from_req(s) for s in seasons]


def get_events(season_id: str) -> Iterable[Event]:
    res = httpx.get(EVENTS_ENDPOINT_TEMPL.format(season=season_id))
    res.raise_for_status()
    events = res.json()
    assert len(events) > 0
    return [Event.from_req(e) for e in events]


def get_categories(event_id: str) -> Iterable[Category]:
    res = httpx.get(CATEGORIES_ENDPOINT_TEMPL.format(event=event_id))
    res.raise_for_status()
    categories = res.json()
    assert len(categories) > 0
    return [Category.from_req(c) for c in categories]


def get_sessions(event_id: str, category_id: str) -> Iterable[Session]:
    res = httpx.get(
        SESSIONS_ENDPOINT_TEMPL.format(event=event_id, category=category_id)
    )
    res.raise_for_status()
    sessions = res.json()
    assert len(sessions) > 0
    return [Session.from_req(s) for s in sessions]


def parse_classification(result: dict) -> Iterable[RiderResult]:
    classification = result.get("classification")
    assert classification is not None

    results = []
    for item in classification:
        results.append(
            RiderResult(
                item.get("rider").get("id"),
                item.get("rider").get("full_name"),
                item.get("position"),
                item.get("points"),
            )
        )

    return results


async def get_classification(
    client: httpx.AsyncClient, session_id: str
) -> Iterable[RiderResult]:
    res = await client.get(CLASSIFICATION_ENDPOINT_TEMPL.format(session=session_id))
    res.raise_for_status()
    res = res.json()
    assert len(res) > 0
    return parse_classification(res)
