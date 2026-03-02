"""Tests for src/api/routes/discover.py.

Covers:
  - GET /api/discover/trending/{media_type}
  - GET /api/discover/search
  - POST /api/discover/add

All TMDB service calls are mocked with AsyncMock so no real HTTP requests
are made. The override_db / http fixtures follow the exact same pattern as
test_api_routes.py.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.deps import get_db
from src.config import settings as app_settings
from src.main import app
from src.models.media_item import MediaItem, MediaType, QueueState
from src.services.tmdb import TmdbExternalIds, TmdbItem, TmdbSearchResult


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def override_db(session: AsyncSession):
    """Override the FastAPI get_db dependency with the test session."""

    async def _override() -> AsyncSession:
        yield session

    app.dependency_overrides[get_db] = _override
    yield
    app.dependency_overrides.clear()


@pytest.fixture
async def http(override_db, tmdb_key) -> AsyncClient:
    """Async HTTP client backed by the FastAPI test app with DB wired.

    Depends on tmdb_key so the TMDB api_key is always set for the duration
    of the test, preventing false 503 responses in happy-path tests.
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
def tmdb_key(monkeypatch: pytest.MonkeyPatch):
    """Ensure settings.tmdb.api_key is non-empty for the duration of the test.

    The discover routes guard against an empty api_key and return 503 if it
    is not configured.  Most happy-path tests need a non-empty key so they
    reach the actual route logic.  Tests that specifically verify the 503
    behaviour should NOT request this fixture and instead patch the key to ""
    themselves.
    """
    monkeypatch.setattr(app_settings.tmdb, "api_key", "test-api-key")
    yield


# ---------------------------------------------------------------------------
# Helper factories
# ---------------------------------------------------------------------------


def _make_tmdb_item(
    tmdb_id: int = 123,
    title: str = "Test Movie",
    year: int = 2024,
    media_type: str = "movie",
    overview: str = "A test film.",
    poster_path: str | None = "/poster.jpg",
    vote_average: float = 7.5,
) -> TmdbItem:
    """Build a TmdbItem for use in mock return values."""
    return TmdbItem(
        tmdb_id=tmdb_id,
        title=title,
        year=year,
        media_type=media_type,
        overview=overview,
        poster_path=poster_path,
        vote_average=vote_average,
    )


async def _create_media_item(
    session: AsyncSession,
    *,
    title: str = "Test Movie",
    year: int = 2024,
    media_type: MediaType = MediaType.MOVIE,
    state: QueueState = QueueState.WANTED,
    tmdb_id: str | None = None,
    imdb_id: str | None = None,
    source: str | None = None,
) -> MediaItem:
    """Persist a MediaItem and return it."""
    now = datetime.now(timezone.utc)
    item = MediaItem(
        title=title,
        year=year,
        media_type=media_type,
        state=state,
        tmdb_id=tmdb_id,
        imdb_id=imdb_id,
        source=source,
        added_at=now,
        state_changed_at=now,
        retry_count=0,
    )
    session.add(item)
    await session.flush()
    return item


# ---------------------------------------------------------------------------
# GET /api/discover/trending/{media_type}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_trending_movies_returns_items(http: AsyncClient) -> None:
    """GET trending/movie returns enriched DiscoverItems with correct fields."""
    mock_items = [
        _make_tmdb_item(tmdb_id=100, title="Trending Movie", year=2024, media_type="movie"),
        _make_tmdb_item(tmdb_id=101, title="Another Hit", year=2023, media_type="movie"),
    ]

    with patch(
        "src.api.routes.discover.tmdb_client.get_trending",
        new_callable=AsyncMock,
        return_value=mock_items,
    ):
        resp = await http.get("/api/discover/trending/movie")

    assert resp.status_code == 200
    data = resp.json()
    assert data["media_type"] == "movie"
    assert data["time_window"] == "week"
    assert len(data["items"]) == 2

    first = data["items"][0]
    assert first["tmdb_id"] == 100
    assert first["title"] == "Trending Movie"
    assert first["year"] == 2024
    assert first["media_type"] == "movie"
    assert first["queue_status"] == "available"


@pytest.mark.asyncio
async def test_trending_tv_returns_items(http: AsyncClient) -> None:
    """GET trending/tv returns items with media_type='tv'."""
    mock_items = [
        _make_tmdb_item(tmdb_id=200, title="Popular Show", year=2024, media_type="tv"),
    ]

    with patch(
        "src.api.routes.discover.tmdb_client.get_trending",
        new_callable=AsyncMock,
        return_value=mock_items,
    ):
        resp = await http.get("/api/discover/trending/tv")

    assert resp.status_code == 200
    data = resp.json()
    assert data["media_type"] == "tv"
    assert len(data["items"]) == 1
    assert data["items"][0]["media_type"] == "tv"


@pytest.mark.asyncio
async def test_trending_invalid_media_type_400(http: AsyncClient) -> None:
    """GET trending/anime returns 400 with a descriptive error."""
    resp = await http.get("/api/discover/trending/anime")

    assert resp.status_code == 400
    assert "media_type" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_trending_invalid_media_type_collection_400(http: AsyncClient) -> None:
    """GET trending/collection returns 400."""
    resp = await http.get("/api/discover/trending/collection")

    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_trending_unconfigured_503(http: AsyncClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """GET trending/movie returns 503 when TMDB api_key is not configured."""
    mock_tmdb_cfg = MagicMock()
    mock_tmdb_cfg.api_key = ""

    with patch("src.api.routes.discover.settings") as mock_settings:
        mock_settings.tmdb = mock_tmdb_cfg
        resp = await http.get("/api/discover/trending/movie")

    assert resp.status_code == 503
    assert "api key" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_trending_returns_empty_when_no_results(http: AsyncClient) -> None:
    """GET trending/movie returns 200 with empty items list when TMDB returns nothing."""
    with patch(
        "src.api.routes.discover.tmdb_client.get_trending",
        new_callable=AsyncMock,
        return_value=[],
    ):
        resp = await http.get("/api/discover/trending/movie")

    assert resp.status_code == 200
    assert resp.json()["items"] == []


@pytest.mark.asyncio
async def test_trending_poster_url_built_correctly(http: AsyncClient) -> None:
    """GET trending builds a full poster_url using the image_base_url setting."""
    mock_items = [
        _make_tmdb_item(tmdb_id=1, title="Poster Movie", poster_path="/poster123.jpg"),
    ]

    with patch(
        "src.api.routes.discover.tmdb_client.get_trending",
        new_callable=AsyncMock,
        return_value=mock_items,
    ):
        resp = await http.get("/api/discover/trending/movie")

    assert resp.status_code == 200
    item = resp.json()["items"][0]
    assert item["poster_url"] is not None
    assert "/w342/poster123.jpg" in item["poster_url"]


@pytest.mark.asyncio
async def test_trending_null_poster_path_gives_null_url(http: AsyncClient) -> None:
    """GET trending sets poster_url=null when poster_path is None."""
    mock_items = [
        _make_tmdb_item(tmdb_id=1, title="No Poster Movie", poster_path=None),
    ]

    with patch(
        "src.api.routes.discover.tmdb_client.get_trending",
        new_callable=AsyncMock,
        return_value=mock_items,
    ):
        resp = await http.get("/api/discover/trending/movie")

    assert resp.status_code == 200
    assert resp.json()["items"][0]["poster_url"] is None


# ---------------------------------------------------------------------------
# GET /api/discover/search
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_returns_items(http: AsyncClient) -> None:
    """GET /api/discover/search returns enriched items and pagination fields."""
    mock_result = TmdbSearchResult(
        items=[
            _make_tmdb_item(tmdb_id=300, title="Search Result", year=2022),
        ],
        total_results=42,
        page=1,
        total_pages=3,
    )

    with patch(
        "src.api.routes.discover.tmdb_client.search",
        new_callable=AsyncMock,
        return_value=mock_result,
    ):
        resp = await http.get("/api/discover/search", params={"q": "test query"})

    assert resp.status_code == 200
    data = resp.json()
    assert data["query"] == "test query"
    assert data["total_results"] == 42
    assert data["page"] == 1
    assert data["total_pages"] == 3
    assert len(data["items"]) == 1
    assert data["items"][0]["tmdb_id"] == 300
    assert data["items"][0]["title"] == "Search Result"


@pytest.mark.asyncio
async def test_search_passes_query_and_params_to_client(http: AsyncClient) -> None:
    """GET /api/discover/search forwards q, media_type, and page to tmdb_client.search."""
    mock_result = TmdbSearchResult(items=[], total_results=0, page=2, total_pages=1)
    mock_search = AsyncMock(return_value=mock_result)

    with patch("src.api.routes.discover.tmdb_client.search", mock_search):
        await http.get(
            "/api/discover/search",
            params={"q": "inception", "media_type": "movie", "page": "2"},
        )

    mock_search.assert_called_once_with("inception", "movie", 2)


@pytest.mark.asyncio
async def test_search_unconfigured_503(http: AsyncClient) -> None:
    """GET /api/discover/search returns 503 when TMDB api_key is not configured."""
    mock_tmdb_cfg = MagicMock()
    mock_tmdb_cfg.api_key = ""

    with patch("src.api.routes.discover.settings") as mock_settings:
        mock_settings.tmdb = mock_tmdb_cfg
        resp = await http.get("/api/discover/search", params={"q": "test"})

    assert resp.status_code == 503


@pytest.mark.asyncio
async def test_search_empty_results(http: AsyncClient) -> None:
    """GET /api/discover/search returns 200 with empty items when TMDB finds nothing."""
    mock_result = TmdbSearchResult(items=[], total_results=0, page=1, total_pages=0)

    with patch(
        "src.api.routes.discover.tmdb_client.search",
        new_callable=AsyncMock,
        return_value=mock_result,
    ):
        resp = await http.get("/api/discover/search", params={"q": "xyzzynonexistent"})

    assert resp.status_code == 200
    data = resp.json()
    assert data["items"] == []
    assert data["total_results"] == 0


@pytest.mark.asyncio
async def test_search_default_media_type_is_multi(http: AsyncClient) -> None:
    """GET /api/discover/search uses media_type='multi' as default."""
    mock_result = TmdbSearchResult(items=[], total_results=0, page=1, total_pages=1)
    mock_search = AsyncMock(return_value=mock_result)

    with patch("src.api.routes.discover.tmdb_client.search", mock_search):
        await http.get("/api/discover/search", params={"q": "test"})

    args, _ = mock_search.call_args
    assert args[1] == "multi"


# ---------------------------------------------------------------------------
# POST /api/discover/add
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_add_creates_wanted_item(http: AsyncClient, session: AsyncSession) -> None:
    """POST /api/discover/add creates a MediaItem in WANTED state with correct fields."""
    mock_ext_ids = TmdbExternalIds(imdb_id="tt9999999", tvdb_id=None)

    with patch(
        "src.api.routes.discover.tmdb_client.get_external_ids",
        new_callable=AsyncMock,
        return_value=mock_ext_ids,
    ):
        resp = await http.post(
            "/api/discover/add",
            json={
                "tmdb_id": 55555,
                "media_type": "movie",
                "title": "New Discovery",
                "year": 2024,
            },
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "created"
    assert data["imdb_id"] == "tt9999999"
    assert isinstance(data["item_id"], int)
    assert "New Discovery" in data["message"]

    # Verify the item was persisted in the DB with correct attributes
    from sqlalchemy import select

    result = await session.execute(
        select(MediaItem).where(MediaItem.tmdb_id == "55555")
    )
    item = result.scalar_one_or_none()
    assert item is not None
    assert item.title == "New Discovery"
    assert item.year == 2024
    assert item.media_type == MediaType.MOVIE
    assert item.state == QueueState.WANTED
    assert item.imdb_id == "tt9999999"
    assert item.source == "discover"
    assert item.tmdb_id == "55555"


@pytest.mark.asyncio
async def test_add_duplicate_returns_exists(http: AsyncClient, session: AsyncSession) -> None:
    """POST /api/discover/add returns status='exists' when tmdb_id already in DB."""
    existing = await _create_media_item(
        session,
        title="Already In Queue",
        tmdb_id="77777",
        imdb_id="tt1111111",
        state=QueueState.SLEEPING,
    )

    with patch(
        "src.api.routes.discover.tmdb_client.get_external_ids",
        new_callable=AsyncMock,
    ) as mock_ext:
        resp = await http.post(
            "/api/discover/add",
            json={
                "tmdb_id": 77777,
                "media_type": "movie",
                "title": "Already In Queue",
                "year": 2023,
            },
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "exists"
    assert data["item_id"] == existing.id
    assert data["imdb_id"] == "tt1111111"
    # get_external_ids must NOT be called when item already exists
    mock_ext.assert_not_called()


@pytest.mark.asyncio
async def test_add_invalid_media_type_400(http: AsyncClient) -> None:
    """POST /api/discover/add with media_type='anime' returns 400."""
    resp = await http.post(
        "/api/discover/add",
        json={
            "tmdb_id": 12345,
            "media_type": "anime",
            "title": "Some Anime",
            "year": 2024,
        },
    )

    assert resp.status_code == 400
    assert "anime" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_add_tv_maps_to_show(http: AsyncClient, session: AsyncSession) -> None:
    """POST /api/discover/add with media_type='tv' creates a MediaItem with MediaType.SHOW."""
    mock_ext_ids = TmdbExternalIds(imdb_id="tt8888888", tvdb_id=11111)

    with patch(
        "src.api.routes.discover.tmdb_client.get_external_ids",
        new_callable=AsyncMock,
        return_value=mock_ext_ids,
    ):
        resp = await http.post(
            "/api/discover/add",
            json={
                "tmdb_id": 99999,
                "media_type": "tv",
                "title": "Great TV Show",
                "year": 2023,
            },
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "created"

    from sqlalchemy import select

    result = await session.execute(
        select(MediaItem).where(MediaItem.tmdb_id == "99999")
    )
    item = result.scalar_one_or_none()
    assert item is not None
    assert item.media_type == MediaType.SHOW


@pytest.mark.asyncio
async def test_add_without_external_ids_still_creates_item(http: AsyncClient, session: AsyncSession) -> None:
    """POST /api/discover/add creates an item with imdb_id=None when get_external_ids returns None."""
    with patch(
        "src.api.routes.discover.tmdb_client.get_external_ids",
        new_callable=AsyncMock,
        return_value=None,
    ):
        resp = await http.post(
            "/api/discover/add",
            json={
                "tmdb_id": 44444,
                "media_type": "movie",
                "title": "No External IDs",
                "year": 2024,
            },
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "created"
    assert data["imdb_id"] is None

    from sqlalchemy import select

    result = await session.execute(
        select(MediaItem).where(MediaItem.tmdb_id == "44444")
    )
    item = result.scalar_one_or_none()
    assert item is not None
    assert item.imdb_id is None


@pytest.mark.asyncio
async def test_add_without_year_succeeds(http: AsyncClient) -> None:
    """POST /api/discover/add with year=None creates a valid item."""
    with patch(
        "src.api.routes.discover.tmdb_client.get_external_ids",
        new_callable=AsyncMock,
        return_value=TmdbExternalIds(imdb_id=None),
    ):
        resp = await http.post(
            "/api/discover/add",
            json={
                "tmdb_id": 33333,
                "media_type": "movie",
                "title": "No Year Movie",
            },
        )

    assert resp.status_code == 200
    assert resp.json()["status"] == "created"


@pytest.mark.asyncio
async def test_add_sets_source_to_discover(http: AsyncClient, session: AsyncSession) -> None:
    """POST /api/discover/add sets source='discover' on the created item."""
    with patch(
        "src.api.routes.discover.tmdb_client.get_external_ids",
        new_callable=AsyncMock,
        return_value=TmdbExternalIds(imdb_id="tt0000001"),
    ):
        resp = await http.post(
            "/api/discover/add",
            json={
                "tmdb_id": 22222,
                "media_type": "movie",
                "title": "Source Movie",
                "year": 2024,
            },
        )

    assert resp.status_code == 200

    from sqlalchemy import select

    result = await session.execute(
        select(MediaItem).where(MediaItem.tmdb_id == "22222")
    )
    item = result.scalar_one_or_none()
    assert item is not None
    assert item.source == "discover"


@pytest.mark.asyncio
async def test_add_stores_tmdb_id_as_string(http: AsyncClient, session: AsyncSession) -> None:
    """POST /api/discover/add stores tmdb_id as a string in the database."""
    with patch(
        "src.api.routes.discover.tmdb_client.get_external_ids",
        new_callable=AsyncMock,
        return_value=TmdbExternalIds(imdb_id=None),
    ):
        resp = await http.post(
            "/api/discover/add",
            json={
                "tmdb_id": 11111,
                "media_type": "movie",
                "title": "TMDB ID String Test",
                "year": 2024,
            },
        )

    assert resp.status_code == 200

    from sqlalchemy import select

    result = await session.execute(
        select(MediaItem).where(MediaItem.tmdb_id == "11111")
    )
    item = result.scalar_one_or_none()
    assert item is not None
    assert item.tmdb_id == "11111"  # stored as string, not int


# ---------------------------------------------------------------------------
# Queue status badges
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_queue_status_in_queue_badge(http: AsyncClient, session: AsyncSession) -> None:
    """trending returns IN_QUEUE badge for items in active (non-library) states."""
    await _create_media_item(
        session,
        title="Wanted Movie",
        tmdb_id="5001",
        state=QueueState.WANTED,
    )

    mock_items = [
        _make_tmdb_item(tmdb_id=5001, title="Wanted Movie"),
    ]

    with patch(
        "src.api.routes.discover.tmdb_client.get_trending",
        new_callable=AsyncMock,
        return_value=mock_items,
    ):
        resp = await http.get("/api/discover/trending/movie")

    assert resp.status_code == 200
    item = resp.json()["items"][0]
    assert item["queue_status"] == "in_queue"
    assert item["queue_item_id"] is not None


@pytest.mark.asyncio
async def test_queue_status_in_library_badge_complete(http: AsyncClient, session: AsyncSession) -> None:
    """trending returns IN_LIBRARY badge for items in COMPLETE state."""
    await _create_media_item(
        session,
        title="Complete Movie",
        tmdb_id="5002",
        state=QueueState.COMPLETE,
    )

    mock_items = [
        _make_tmdb_item(tmdb_id=5002, title="Complete Movie"),
    ]

    with patch(
        "src.api.routes.discover.tmdb_client.get_trending",
        new_callable=AsyncMock,
        return_value=mock_items,
    ):
        resp = await http.get("/api/discover/trending/movie")

    assert resp.status_code == 200
    item = resp.json()["items"][0]
    assert item["queue_status"] == "in_library"


@pytest.mark.asyncio
async def test_queue_status_in_library_badge_done(http: AsyncClient, session: AsyncSession) -> None:
    """trending returns IN_LIBRARY badge for items in DONE state."""
    await _create_media_item(
        session,
        title="Done Movie",
        tmdb_id="5003",
        state=QueueState.DONE,
    )

    mock_items = [
        _make_tmdb_item(tmdb_id=5003, title="Done Movie"),
    ]

    with patch(
        "src.api.routes.discover.tmdb_client.get_trending",
        new_callable=AsyncMock,
        return_value=mock_items,
    ):
        resp = await http.get("/api/discover/trending/movie")

    assert resp.status_code == 200
    item = resp.json()["items"][0]
    assert item["queue_status"] == "in_library"


@pytest.mark.asyncio
async def test_queue_status_available_when_not_in_db(http: AsyncClient) -> None:
    """trending returns AVAILABLE badge for items not in the database."""
    mock_items = [
        _make_tmdb_item(tmdb_id=9999, title="Unknown Movie"),
    ]

    with patch(
        "src.api.routes.discover.tmdb_client.get_trending",
        new_callable=AsyncMock,
        return_value=mock_items,
    ):
        resp = await http.get("/api/discover/trending/movie")

    assert resp.status_code == 200
    item = resp.json()["items"][0]
    assert item["queue_status"] == "available"
    assert item["queue_item_id"] is None


@pytest.mark.asyncio
async def test_queue_status_mixed_badges(http: AsyncClient, session: AsyncSession) -> None:
    """trending correctly assigns different badges across a mixed list."""
    await _create_media_item(session, title="Complete Movie", tmdb_id="6001", state=QueueState.COMPLETE)
    await _create_media_item(session, title="Sleeping Movie", tmdb_id="6002", state=QueueState.SLEEPING)
    # 6003 is not in DB at all

    mock_items = [
        _make_tmdb_item(tmdb_id=6001, title="Complete Movie"),
        _make_tmdb_item(tmdb_id=6002, title="Sleeping Movie"),
        _make_tmdb_item(tmdb_id=6003, title="Fresh Movie"),
    ]

    with patch(
        "src.api.routes.discover.tmdb_client.get_trending",
        new_callable=AsyncMock,
        return_value=mock_items,
    ):
        resp = await http.get("/api/discover/trending/movie")

    assert resp.status_code == 200
    items_by_tmdb_id = {str(i["tmdb_id"]): i for i in resp.json()["items"]}

    assert items_by_tmdb_id["6001"]["queue_status"] == "in_library"
    assert items_by_tmdb_id["6002"]["queue_status"] == "in_queue"
    assert items_by_tmdb_id["6003"]["queue_status"] == "available"


@pytest.mark.asyncio
async def test_queue_status_in_queue_for_scraping_state(http: AsyncClient, session: AsyncSession) -> None:
    """trending returns IN_QUEUE badge for items in SCRAPING state."""
    await _create_media_item(session, title="Scraping Movie", tmdb_id="7001", state=QueueState.SCRAPING)

    mock_items = [_make_tmdb_item(tmdb_id=7001, title="Scraping Movie")]

    with patch(
        "src.api.routes.discover.tmdb_client.get_trending",
        new_callable=AsyncMock,
        return_value=mock_items,
    ):
        resp = await http.get("/api/discover/trending/movie")

    assert resp.status_code == 200
    assert resp.json()["items"][0]["queue_status"] == "in_queue"


@pytest.mark.asyncio
async def test_queue_status_in_queue_for_dormant_state(http: AsyncClient, session: AsyncSession) -> None:
    """trending returns IN_QUEUE badge for items in DORMANT state."""
    await _create_media_item(session, title="Dormant Movie", tmdb_id="7002", state=QueueState.DORMANT)

    mock_items = [_make_tmdb_item(tmdb_id=7002, title="Dormant Movie")]

    with patch(
        "src.api.routes.discover.tmdb_client.get_trending",
        new_callable=AsyncMock,
        return_value=mock_items,
    ):
        resp = await http.get("/api/discover/trending/movie")

    assert resp.status_code == 200
    assert resp.json()["items"][0]["queue_status"] == "in_queue"


@pytest.mark.asyncio
async def test_search_returns_queue_status_for_known_items(
    http: AsyncClient, session: AsyncSession
) -> None:
    """GET /api/discover/search enriches items with correct queue_status from the DB."""
    await _create_media_item(session, title="Found Movie", tmdb_id="8001", state=QueueState.COMPLETE)

    mock_result = TmdbSearchResult(
        items=[_make_tmdb_item(tmdb_id=8001, title="Found Movie")],
        total_results=1,
        page=1,
        total_pages=1,
    )

    with patch(
        "src.api.routes.discover.tmdb_client.search",
        new_callable=AsyncMock,
        return_value=mock_result,
    ):
        resp = await http.get("/api/discover/search", params={"q": "Found Movie"})

    assert resp.status_code == 200
    assert resp.json()["items"][0]["queue_status"] == "in_library"


# ---------------------------------------------------------------------------
# Response shape contracts
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_trending_response_shape(http: AsyncClient) -> None:
    """GET trending returns the expected top-level response shape."""
    with patch(
        "src.api.routes.discover.tmdb_client.get_trending",
        new_callable=AsyncMock,
        return_value=[],
    ):
        resp = await http.get("/api/discover/trending/movie")

    assert resp.status_code == 200
    data = resp.json()
    assert "items" in data
    assert "media_type" in data
    assert "time_window" in data


@pytest.mark.asyncio
async def test_search_response_shape(http: AsyncClient) -> None:
    """GET search returns the expected top-level response shape."""
    mock_result = TmdbSearchResult(items=[], total_results=0, page=1, total_pages=1)

    with patch(
        "src.api.routes.discover.tmdb_client.search",
        new_callable=AsyncMock,
        return_value=mock_result,
    ):
        resp = await http.get("/api/discover/search", params={"q": "test"})

    assert resp.status_code == 200
    data = resp.json()
    assert "items" in data
    assert "query" in data
    assert "total_results" in data
    assert "page" in data
    assert "total_pages" in data


@pytest.mark.asyncio
async def test_add_response_shape(http: AsyncClient) -> None:
    """POST add returns the expected top-level response shape."""
    with patch(
        "src.api.routes.discover.tmdb_client.get_external_ids",
        new_callable=AsyncMock,
        return_value=TmdbExternalIds(imdb_id="tt0000099"),
    ):
        resp = await http.post(
            "/api/discover/add",
            json={"tmdb_id": 19191, "media_type": "movie", "title": "Shape Test"},
        )

    assert resp.status_code == 200
    data = resp.json()
    assert "status" in data
    assert "item_id" in data
    assert "imdb_id" in data
    assert "message" in data


# ---------------------------------------------------------------------------
# GET /api/discover/top_rated/{media_type}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_top_rated_returns_items(http: AsyncClient) -> None:
    """GET top_rated/movie returns enriched items."""
    mock_items = [
        _make_tmdb_item(tmdb_id=800, title="Top Movie", year=2020, media_type="movie"),
    ]

    with patch(
        "src.api.routes.discover.tmdb_client.get_top_rated",
        new_callable=AsyncMock,
        return_value=mock_items,
    ):
        resp = await http.get("/api/discover/top_rated/movie")

    assert resp.status_code == 200
    data = resp.json()
    assert len(data["items"]) == 1
    assert data["items"][0]["tmdb_id"] == 800
    assert data["media_type"] == "movie"


@pytest.mark.asyncio
async def test_top_rated_invalid_media_type(http: AsyncClient) -> None:
    """GET top_rated/anime returns 400."""
    resp = await http.get("/api/discover/top_rated/anime")
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# GET /api/discover/genres/{media_type}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_genres_returns_list(http: AsyncClient) -> None:
    """GET genres/movie returns a list of genres."""
    mock_genres = [{"id": 28, "name": "Action"}, {"id": 35, "name": "Comedy"}]

    with patch(
        "src.api.routes.discover.tmdb_client.get_genres",
        new_callable=AsyncMock,
        return_value=mock_genres,
    ):
        resp = await http.get("/api/discover/genres/movie")

    assert resp.status_code == 200
    data = resp.json()
    assert len(data["genres"]) == 2
    assert data["genres"][0]["id"] == 28
    assert data["genres"][0]["name"] == "Action"


# ---------------------------------------------------------------------------
# GET /api/discover/by-genre/{media_type}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_by_genre_returns_items(http: AsyncClient) -> None:
    """GET by-genre/movie returns enriched items."""
    mock_result = TmdbSearchResult(
        items=[_make_tmdb_item(tmdb_id=900, title="Genre Movie", year=2023)],
        total_results=100,
        page=1,
        total_pages=5,
    )

    with patch(
        "src.api.routes.discover.tmdb_client.discover",
        new_callable=AsyncMock,
        return_value=mock_result,
    ):
        resp = await http.get("/api/discover/by-genre/movie", params={"genre_id": 28})

    assert resp.status_code == 200
    data = resp.json()
    assert len(data["items"]) == 1
    assert data["items"][0]["tmdb_id"] == 900
    assert data["total_results"] == 100
    assert data["total_pages"] == 5


@pytest.mark.asyncio
async def test_by_genre_invalid_media_type(http: AsyncClient) -> None:
    """GET by-genre/anime returns 400."""
    resp = await http.get("/api/discover/by-genre/anime", params={"genre_id": 28})
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_by_genre_enriches_status(http: AsyncClient, session: AsyncSession) -> None:
    """GET by-genre enriches items with queue status badges."""
    await _create_media_item(
        session,
        title="Complete Genre Movie",
        tmdb_id="950",
        state=QueueState.COMPLETE,
    )

    mock_result = TmdbSearchResult(
        items=[_make_tmdb_item(tmdb_id=950, title="Complete Genre Movie")],
        total_results=1,
        page=1,
        total_pages=1,
    )

    with patch(
        "src.api.routes.discover.tmdb_client.discover",
        new_callable=AsyncMock,
        return_value=mock_result,
    ):
        resp = await http.get("/api/discover/by-genre/movie", params={"genre_id": 28})

    assert resp.status_code == 200
    assert resp.json()["items"][0]["queue_status"] == "in_library"


# ---------------------------------------------------------------------------
# POST /api/discover/add — season pack and movie field defaults
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_add_tv_show_sets_season1_and_is_season_pack(
    http: AsyncClient, session: AsyncSession
) -> None:
    """POST /api/discover/add with media_type='tv' sets season=1, is_season_pack=True, episode=None.

    TV shows are added as season-1 packs so the scrape pipeline targets the
    correct season from the first retry cycle.
    """
    from sqlalchemy import select

    mock_ext_ids = TmdbExternalIds(imdb_id="tt1234500", tvdb_id=98765)

    with patch(
        "src.api.routes.discover.tmdb_client.get_external_ids",
        new_callable=AsyncMock,
        return_value=mock_ext_ids,
    ):
        resp = await http.post(
            "/api/discover/add",
            json={
                "tmdb_id": 40404,
                "media_type": "tv",
                "title": "Season Pack Show",
                "year": 2023,
            },
        )

    assert resp.status_code == 200
    assert resp.json()["status"] == "created"

    result = await session.execute(
        select(MediaItem).where(MediaItem.tmdb_id == "40404")
    )
    item = result.scalar_one_or_none()
    assert item is not None, "MediaItem must be persisted in the database"
    assert item.season == 1, "TV show must default to season=1"
    assert item.is_season_pack is True, "TV show must default to is_season_pack=True"
    assert item.episode is None, "TV show must have episode=None (season pack, not a single episode)"


@pytest.mark.asyncio
async def test_add_movie_keeps_default_season_episode_fields(
    http: AsyncClient, session: AsyncSession
) -> None:
    """POST /api/discover/add with media_type='movie' sets season=None, is_season_pack=False, episode=None.

    Movies carry no season/episode context; those fields must remain at their
    zero-value defaults.
    """
    from sqlalchemy import select

    mock_ext_ids = TmdbExternalIds(imdb_id="tt9876500", tvdb_id=None)

    with patch(
        "src.api.routes.discover.tmdb_client.get_external_ids",
        new_callable=AsyncMock,
        return_value=mock_ext_ids,
    ):
        resp = await http.post(
            "/api/discover/add",
            json={
                "tmdb_id": 50505,
                "media_type": "movie",
                "title": "Plain Movie",
                "year": 2024,
            },
        )

    assert resp.status_code == 200
    assert resp.json()["status"] == "created"

    result = await session.execute(
        select(MediaItem).where(MediaItem.tmdb_id == "50505")
    )
    item = result.scalar_one_or_none()
    assert item is not None, "MediaItem must be persisted in the database"
    assert item.season is None, "Movie must have season=None"
    assert item.is_season_pack is False, "Movie must have is_season_pack=False"
    assert item.episode is None, "Movie must have episode=None"


@pytest.mark.asyncio
async def test_add_tv_show_without_external_ids_still_sets_season_pack_fields(
    http: AsyncClient, session: AsyncSession
) -> None:
    """POST /api/discover/add with media_type='tv' and no TMDB external IDs still sets season/pack fields.

    The season-pack defaults are applied before the TMDB call resolves, so a
    None return from get_external_ids must not prevent them from being set.
    """
    from sqlalchemy import select

    with patch(
        "src.api.routes.discover.tmdb_client.get_external_ids",
        new_callable=AsyncMock,
        return_value=None,  # TMDB returned nothing
    ):
        resp = await http.post(
            "/api/discover/add",
            json={
                "tmdb_id": 60606,
                "media_type": "tv",
                "title": "Unknown Show",
                "year": 2022,
            },
        )

    assert resp.status_code == 200
    assert resp.json()["status"] == "created"
    assert resp.json()["imdb_id"] is None

    result = await session.execute(
        select(MediaItem).where(MediaItem.tmdb_id == "60606")
    )
    item = result.scalar_one_or_none()
    assert item is not None
    assert item.season == 1
    assert item.is_season_pack is True
    assert item.episode is None
