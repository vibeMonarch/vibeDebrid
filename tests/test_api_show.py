"""Tests for src/api/routes/show.py.

Covers:
  - GET  /api/show/{tmdb_id}      — 503 (no key), 404 (not found), 200 (success)
  - POST /api/show/add            — 400 (no seasons + no subscribe), 200 (creates),
                                    skips existing, subscribe creates MonitoredShow
  - PUT  /api/show/{tmdb_id}/subscribe — create, update, unchanged, response shape

All TMDB calls are mocked with AsyncMock. DB dependency is overridden with the
in-memory test session from conftest.py. Pattern follows test_api_discover.py.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.deps import get_db
from src.config import settings as app_settings
from src.main import app
from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.monitored_show import MonitoredShow
from src.services.tmdb import TmdbSeasonInfo, TmdbShowDetail

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

TMDB_ID = 12345
TMDB_ID_STR = "12345"


@pytest.fixture
def override_db(session: AsyncSession):
    """Override the FastAPI get_db dependency with the test session."""

    async def _override() -> AsyncSession:
        yield session

    app.dependency_overrides[get_db] = _override
    yield
    app.dependency_overrides.clear()


@pytest.fixture
def tmdb_key(monkeypatch: pytest.MonkeyPatch):
    """Ensure settings.tmdb.api_key is non-empty so 503 guards don't fire in happy-path tests."""
    monkeypatch.setattr(app_settings.tmdb, "api_key", "test-api-key")
    yield


@pytest.fixture
async def http(override_db, tmdb_key) -> AsyncClient:
    """Async HTTP client backed by the test app with DB and TMDB key wired."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
async def http_no_tmdb_key(override_db) -> AsyncClient:
    """HTTP client with DB but NO TMDB key — for testing 503 responses."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


# ---------------------------------------------------------------------------
# Helper factories
# ---------------------------------------------------------------------------


def _make_tmdb_show(
    *,
    tmdb_id: int = TMDB_ID,
    title: str = "Test Show",
    seasons: list[TmdbSeasonInfo] | None = None,
    imdb_id: str | None = "tt9999001",
) -> TmdbShowDetail:
    """Build a TmdbShowDetail for use as a mock return value."""
    if seasons is None:
        seasons = [
            TmdbSeasonInfo(season_number=1, name="Season 1", episode_count=10, air_date="2020-01-01"),
            TmdbSeasonInfo(season_number=2, name="Season 2", episode_count=8, air_date="2021-01-01"),
        ]
    return TmdbShowDetail(
        tmdb_id=tmdb_id,
        title=title,
        year=2020,
        overview="A test show.",
        poster_path="/poster.jpg",
        backdrop_path="/backdrop.jpg",
        status="Ended",
        vote_average=8.0,
        number_of_seasons=len(seasons),
        seasons=seasons,
        imdb_id=imdb_id,
        genres=[{"id": 18, "name": "Drama"}],
    )


async def _make_show_item(
    session: AsyncSession,
    *,
    tmdb_id: str = TMDB_ID_STR,
    season: int = 1,
    episode: int | None = None,
    state: QueueState = QueueState.WANTED,
    is_season_pack: bool = True,
    title: str = "Test Show",
) -> MediaItem:
    """Persist a show MediaItem and return it."""
    now = datetime.now(UTC)
    item = MediaItem(
        title=title,
        year=2020,
        media_type=MediaType.SHOW,
        tmdb_id=tmdb_id,
        imdb_id="tt9999001",
        state=state,
        source="show_detail",
        added_at=now,
        state_changed_at=now,
        retry_count=0,
        season=season,
        episode=episode,
        is_season_pack=is_season_pack,
    )
    session.add(item)
    await session.flush()
    return item


async def _make_monitored_show(
    session: AsyncSession,
    *,
    tmdb_id: int = TMDB_ID,
    enabled: bool = True,
) -> MonitoredShow:
    """Persist a MonitoredShow and return it."""
    now = datetime.now(UTC)
    show = MonitoredShow(
        tmdb_id=tmdb_id,
        imdb_id="tt9999001",
        title="Test Show",
        year=2020,
        quality_profile="high",
        enabled=enabled,
        created_at=now,
        updated_at=now,
    )
    session.add(show)
    await session.flush()
    return show


# ---------------------------------------------------------------------------
# GET /api/show/{tmdb_id}
# ---------------------------------------------------------------------------


class TestGetShowDetail:
    """Tests for GET /api/show/{tmdb_id}."""

    async def test_returns_503_when_tmdb_api_key_not_configured(
        self, http_no_tmdb_key: AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Returns 503 Service Unavailable when the TMDB api_key is empty."""
        monkeypatch.setattr(app_settings.tmdb, "api_key", "")

        resp = await http_no_tmdb_key.get(f"/api/show/{TMDB_ID}")

        assert resp.status_code == 503
        assert "tmdb" in resp.json()["detail"].lower()

    async def test_returns_404_when_tmdb_returns_none(self, http: AsyncClient) -> None:
        """Returns 404 Not Found when TMDB has no data for the given tmdb_id."""
        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=None,
        ):
            resp = await http.get(f"/api/show/{TMDB_ID}")

        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"].lower()

    async def test_returns_200_with_show_detail_on_success(
        self, http: AsyncClient
    ) -> None:
        """Returns 200 with full ShowDetail JSON on success."""
        mock_show = _make_tmdb_show()

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            resp = await http.get(f"/api/show/{TMDB_ID}")

        assert resp.status_code == 200
        data = resp.json()
        assert data["tmdb_id"] == TMDB_ID
        assert data["title"] == "Test Show"
        assert data["imdb_id"] == "tt9999001"

    async def test_response_includes_seasons(self, http: AsyncClient) -> None:
        """ShowDetail response includes a seasons list with season info."""
        mock_show = _make_tmdb_show()

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            resp = await http.get(f"/api/show/{TMDB_ID}")

        assert resp.status_code == 200
        data = resp.json()
        assert "seasons" in data
        assert len(data["seasons"]) == 2  # two seasons in mock show (no S0)
        s1 = data["seasons"][0]
        assert s1["season_number"] == 1
        assert s1["name"] == "Season 1"
        assert s1["episode_count"] == 10
        assert "status" in s1
        assert "queue_item_ids" in s1

    async def test_response_includes_quality_profiles(self, http: AsyncClient) -> None:
        """ShowDetail response includes quality_profiles from settings."""
        mock_show = _make_tmdb_show()

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            resp = await http.get(f"/api/show/{TMDB_ID}")

        assert resp.status_code == 200
        data = resp.json()
        assert "quality_profiles" in data
        assert isinstance(data["quality_profiles"], list)
        assert len(data["quality_profiles"]) >= 1

    async def test_response_includes_is_subscribed(self, http: AsyncClient) -> None:
        """ShowDetail response includes is_subscribed boolean."""
        mock_show = _make_tmdb_show()

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            resp = await http.get(f"/api/show/{TMDB_ID}")

        assert resp.status_code == 200
        data = resp.json()
        assert "is_subscribed" in data
        assert data["is_subscribed"] is False

    async def test_is_subscribed_true_when_enabled_monitored_show_exists(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """is_subscribed is True in the response when an enabled MonitoredShow exists."""
        await _make_monitored_show(session, tmdb_id=TMDB_ID, enabled=True)
        mock_show = _make_tmdb_show()

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            resp = await http.get(f"/api/show/{TMDB_ID}")

        assert resp.status_code == 200
        assert resp.json()["is_subscribed"] is True

    async def test_season_status_in_library_when_complete_item_exists(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Season shows in_library status when a COMPLETE MediaItem exists for that season."""
        await _make_show_item(session, season=1, state=QueueState.COMPLETE)
        mock_show = _make_tmdb_show()

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            resp = await http.get(f"/api/show/{TMDB_ID}")

        assert resp.status_code == 200
        seasons = resp.json()["seasons"]
        s1 = next(s for s in seasons if s["season_number"] == 1)
        assert s1["status"] == "in_library"

    async def test_season_status_in_queue_when_wanted_item_exists(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Season shows in_queue status when a WANTED MediaItem exists for that season."""
        await _make_show_item(session, season=2, state=QueueState.WANTED)
        mock_show = _make_tmdb_show()

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            resp = await http.get(f"/api/show/{TMDB_ID}")

        assert resp.status_code == 200
        seasons = resp.json()["seasons"]
        s2 = next(s for s in seasons if s["season_number"] == 2)
        assert s2["status"] == "in_queue"

    async def test_season_status_available_when_no_items(
        self, http: AsyncClient
    ) -> None:
        """Season shows available status when no MediaItems exist for that season."""
        mock_show = _make_tmdb_show(
            seasons=[TmdbSeasonInfo(season_number=1, name="S1", episode_count=10, air_date="2020-01-01")]
        )

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            resp = await http.get(f"/api/show/{TMDB_ID}")

        assert resp.status_code == 200
        assert resp.json()["seasons"][0]["status"] == "available"

    async def test_season_zero_excluded_from_response(self, http: AsyncClient) -> None:
        """Season 0 (Specials) is not included in the seasons list in the response."""
        mock_show = _make_tmdb_show(
            seasons=[
                TmdbSeasonInfo(season_number=0, name="Specials", episode_count=2, air_date="2019-01-01"),
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=10, air_date="2020-01-01"),
            ]
        )

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            resp = await http.get(f"/api/show/{TMDB_ID}")

        assert resp.status_code == 200
        data = resp.json()
        season_numbers = [s["season_number"] for s in data["seasons"]]
        assert 0 not in season_numbers
        assert 1 in season_numbers

    async def test_full_response_shape(self, http: AsyncClient) -> None:
        """GET /api/show/{tmdb_id} response contains all required top-level fields."""
        mock_show = _make_tmdb_show()

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            resp = await http.get(f"/api/show/{TMDB_ID}")

        assert resp.status_code == 200
        data = resp.json()
        for field in (
            "tmdb_id", "imdb_id", "title", "year", "overview",
            "poster_url", "backdrop_url", "show_status", "vote_average",
            "genres", "seasons", "is_subscribed", "quality_profiles", "default_profile",
        ):
            assert field in data, f"Missing field: {field}"


# ---------------------------------------------------------------------------
# POST /api/show/add
# ---------------------------------------------------------------------------


class TestAddSeasons:
    """Tests for POST /api/show/add."""

    def _add_body(
        self,
        *,
        seasons: list[int] = [1],
        subscribe: bool = False,
        quality_profile: str | None = None,
        tmdb_id: int = TMDB_ID,
    ) -> dict:
        return {
            "tmdb_id": tmdb_id,
            "imdb_id": "tt9999001",
            "title": "Test Show",
            "year": 2020,
            "seasons": seasons,
            "quality_profile": quality_profile,
            "subscribe": subscribe,
        }

    async def test_returns_400_when_no_seasons_and_no_subscribe(
        self, http: AsyncClient
    ) -> None:
        """Returns 400 Bad Request when seasons list is empty and subscribe is False."""
        resp = await http.post("/api/show/add", json=self._add_body(seasons=[], subscribe=False))

        assert resp.status_code == 400
        assert "season" in resp.json()["detail"].lower() or "subscribe" in resp.json()["detail"].lower()

    async def test_creates_season_items_and_returns_result(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """POST /api/show/add creates MediaItems and returns correct created_items count."""
        from sqlalchemy import select

        resp = await http.post("/api/show/add", json=self._add_body(seasons=[1, 2]))

        assert resp.status_code == 200
        data = resp.json()
        assert data["created_items"] == 2
        assert data["skipped_seasons"] == []

        rows = list((await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == TMDB_ID_STR)
        )).scalars().all())
        assert len(rows) == 2

    async def test_skips_seasons_with_existing_items(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """POST /api/show/add skips seasons that already have a MediaItem in the DB."""
        await _make_show_item(session, season=1, state=QueueState.COMPLETE)

        resp = await http.post("/api/show/add", json=self._add_body(seasons=[1, 2]))

        assert resp.status_code == 200
        data = resp.json()
        assert data["created_items"] == 1
        assert data["skipped_seasons"] == [1]

    async def test_creates_subscription_when_subscribe_true(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """POST /api/show/add with subscribe=True creates a MonitoredShow record."""
        from sqlalchemy import select

        resp = await http.post(
            "/api/show/add",
            json=self._add_body(seasons=[1], subscribe=True),
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["subscription_status"] == "created"

        row = (await session.execute(
            select(MonitoredShow).where(MonitoredShow.tmdb_id == TMDB_ID)
        )).scalar_one_or_none()
        assert row is not None
        assert row.enabled is True

    async def test_subscription_status_none_when_subscribe_false(
        self, http: AsyncClient
    ) -> None:
        """POST /api/show/add returns subscription_status='none' when subscribe=False."""
        resp = await http.post(
            "/api/show/add",
            json=self._add_body(seasons=[1], subscribe=False),
        )

        assert resp.status_code == 200
        assert resp.json()["subscription_status"] == "none"

    async def test_subscribe_without_seasons_is_valid(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """POST /api/show/add with empty seasons but subscribe=True returns 200."""
        from sqlalchemy import select

        resp = await http.post(
            "/api/show/add",
            json=self._add_body(seasons=[], subscribe=True),
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["created_items"] == 0
        assert data["subscription_status"] == "created"

        row = (await session.execute(
            select(MonitoredShow).where(MonitoredShow.tmdb_id == TMDB_ID)
        )).scalar_one_or_none()
        assert row is not None

    async def test_created_items_are_season_packs_in_wanted_state(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Items created by /api/show/add are WANTED season packs with source='show_detail'."""
        from sqlalchemy import select

        resp = await http.post("/api/show/add", json=self._add_body(seasons=[3]))

        assert resp.status_code == 200

        rows = list((await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == TMDB_ID_STR)
        )).scalars().all())
        assert len(rows) == 1
        item = rows[0]
        assert item.state == QueueState.WANTED
        assert item.is_season_pack is True
        assert item.episode is None
        assert item.season == 3
        assert item.source == "show_detail"
        assert item.media_type == MediaType.SHOW

    async def test_quality_profile_applied_to_items(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """quality_profile from the request is stored on created MediaItems."""
        from sqlalchemy import select

        resp = await http.post(
            "/api/show/add",
            json=self._add_body(seasons=[1], quality_profile="standard"),
        )

        assert resp.status_code == 200

        rows = list((await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == TMDB_ID_STR)
        )).scalars().all())
        assert rows[0].quality_profile == "standard"

    async def test_response_shape(self, http: AsyncClient) -> None:
        """POST /api/show/add response contains created_items, skipped_seasons, subscription_status."""
        resp = await http.post("/api/show/add", json=self._add_body(seasons=[1]))

        assert resp.status_code == 200
        data = resp.json()
        assert "created_items" in data
        assert "skipped_seasons" in data
        assert "subscription_status" in data
        assert isinstance(data["created_items"], int)
        assert isinstance(data["skipped_seasons"], list)
        assert isinstance(data["subscription_status"], str)

    async def test_all_skipped_returns_zero_created(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """When all requested seasons already exist, created_items=0 and all in skipped_seasons."""
        await _make_show_item(session, season=1)
        await _make_show_item(session, season=2)

        resp = await http.post("/api/show/add", json=self._add_body(seasons=[1, 2]))

        assert resp.status_code == 200
        data = resp.json()
        assert data["created_items"] == 0
        assert set(data["skipped_seasons"]) == {1, 2}

    async def test_imdb_id_stored_on_created_items(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """The imdb_id from the request body is stored on created MediaItems."""
        from sqlalchemy import select

        body = self._add_body(seasons=[1])
        body["imdb_id"] = "tt1234500"

        resp = await http.post("/api/show/add", json=body)

        assert resp.status_code == 200

        rows = list((await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == TMDB_ID_STR)
        )).scalars().all())
        assert rows[0].imdb_id == "tt1234500"


# ---------------------------------------------------------------------------
# PUT /api/show/{tmdb_id}/subscribe
# ---------------------------------------------------------------------------


class TestToggleSubscribe:
    """Tests for PUT /api/show/{tmdb_id}/subscribe."""

    def _subscribe_body(
        self,
        *,
        enabled: bool = True,
        quality_profile: str | None = None,
        imdb_id: str | None = "tt9999001",
        title: str = "Test Show",
        year: int | None = 2020,
    ) -> dict:
        return {
            "enabled": enabled,
            "quality_profile": quality_profile,
            "imdb_id": imdb_id,
            "title": title,
            "year": year,
        }

    async def test_creates_new_subscription(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """PUT /api/show/{tmdb_id}/subscribe creates a MonitoredShow when none exists."""
        from sqlalchemy import select

        resp = await http.put(
            f"/api/show/{TMDB_ID}/subscribe",
            json=self._subscribe_body(enabled=True),
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "created"
        assert data["enabled"] is True

        row = (await session.execute(
            select(MonitoredShow).where(MonitoredShow.tmdb_id == TMDB_ID)
        )).scalar_one_or_none()
        assert row is not None
        assert row.enabled is True

    async def test_toggles_existing_subscription_off(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """PUT /api/show/{tmdb_id}/subscribe toggles an enabled record to disabled."""
        from sqlalchemy import select

        await _make_monitored_show(session, tmdb_id=TMDB_ID, enabled=True)

        resp = await http.put(
            f"/api/show/{TMDB_ID}/subscribe",
            json=self._subscribe_body(enabled=False),
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "updated"
        assert data["enabled"] is False

        row = (await session.execute(
            select(MonitoredShow).where(MonitoredShow.tmdb_id == TMDB_ID)
        )).scalar_one_or_none()
        assert row is not None
        assert row.enabled is False

    async def test_toggles_existing_subscription_on(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """PUT /api/show/{tmdb_id}/subscribe re-enables a disabled record."""
        await _make_monitored_show(session, tmdb_id=TMDB_ID, enabled=False)

        resp = await http.put(
            f"/api/show/{TMDB_ID}/subscribe",
            json=self._subscribe_body(enabled=True),
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "updated"
        assert data["enabled"] is True

    async def test_returns_unchanged_when_already_enabled(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """PUT /api/show/{tmdb_id}/subscribe returns 'unchanged' when state matches."""
        await _make_monitored_show(session, tmdb_id=TMDB_ID, enabled=True)

        resp = await http.put(
            f"/api/show/{TMDB_ID}/subscribe",
            json=self._subscribe_body(enabled=True),
        )

        assert resp.status_code == 200
        assert resp.json()["status"] == "unchanged"

    async def test_returns_unchanged_when_disabling_nonexistent_record(
        self, http: AsyncClient
    ) -> None:
        """PUT /api/show/{tmdb_id}/subscribe returns 'unchanged' when disabling with no record."""
        resp = await http.put(
            f"/api/show/{TMDB_ID}/subscribe",
            json=self._subscribe_body(enabled=False),
        )

        assert resp.status_code == 200
        assert resp.json()["status"] == "unchanged"

    async def test_response_shape(self, http: AsyncClient) -> None:
        """PUT /api/show/{tmdb_id}/subscribe response has 'status' and 'enabled' fields."""
        resp = await http.put(
            f"/api/show/{TMDB_ID}/subscribe",
            json=self._subscribe_body(enabled=True),
        )

        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data
        assert "enabled" in data
        assert isinstance(data["status"], str)
        assert isinstance(data["enabled"], bool)

    async def test_enabled_field_reflects_request_value(
        self, http: AsyncClient
    ) -> None:
        """PUT response 'enabled' always reflects the requested enabled value."""
        for enabled in (True, False):
            resp = await http.put(
                f"/api/show/{TMDB_ID}/subscribe",
                json=self._subscribe_body(enabled=enabled),
            )
            # Reset state between iterations
            # Just verify the field matches the requested value
            assert resp.json()["enabled"] == enabled

    async def test_quality_profile_stored_when_creating(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Quality profile from request is stored when creating a new subscription."""
        from sqlalchemy import select

        resp = await http.put(
            f"/api/show/{TMDB_ID}/subscribe",
            json=self._subscribe_body(enabled=True, quality_profile="standard"),
        )

        assert resp.status_code == 200

        row = (await session.execute(
            select(MonitoredShow).where(MonitoredShow.tmdb_id == TMDB_ID)
        )).scalar_one_or_none()
        assert row is not None
        assert row.quality_profile == "standard"

    async def test_different_tmdb_ids_are_independent(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Subscribing to one show does not affect another show's subscription."""
        from sqlalchemy import select

        await _make_monitored_show(session, tmdb_id=99999, enabled=True)

        # Subscribe to TMDB_ID (12345)
        resp = await http.put(
            f"/api/show/{TMDB_ID}/subscribe",
            json=self._subscribe_body(enabled=True),
        )

        assert resp.status_code == 200
        assert resp.json()["status"] == "created"

        # The pre-existing show (99999) should be unaffected
        row = (await session.execute(
            select(MonitoredShow).where(MonitoredShow.tmdb_id == 99999)
        )).scalar_one_or_none()
        assert row is not None
        assert row.enabled is True
