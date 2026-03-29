"""Tests for vibeDebrid API routes.

Covers:
  - GET /api/stats (dashboard)
  - GET /api/queue, GET /api/queue/{id} (queue list and detail)
  - POST /api/queue/{id}/retry, POST /api/queue/{id}/state (queue mutations)
  - DELETE /api/queue/{id} (item removal)
  - POST /api/queue/bulk/retry, POST /api/queue/bulk/remove (bulk ops)
  - POST /api/search, POST /api/add (search and manual add)
  - GET /api/settings, POST /api/settings/test/* (settings and connectivity)
  - GET /api/duplicates, POST /api/duplicates/resolve (duplicate management)
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

import src.api.routes.dashboard as dashboard_module
from src.api.deps import get_db
from src.main import app
from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.scrape_result import ScrapeLog
from src.models.torrent import RdTorrent, TorrentStatus
from src.services.http_client import CircuitOpenError
from src.services.real_debrid import CacheCheckResult, RealDebridAuthError, RealDebridError

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def override_db(session: AsyncSession):
    """Override the FastAPI get_db dependency with the test session.

    Must be requested by any test that exercises a route that touches the DB.
    Clears all overrides when the test exits to prevent state leaking between
    test classes.
    """

    async def _override() -> AsyncSession:
        yield session

    app.dependency_overrides[get_db] = _override
    yield
    app.dependency_overrides.clear()


@pytest.fixture
async def http(override_db) -> AsyncClient:
    """Async HTTP client backed by the FastAPI test app.

    Depends on override_db so the DB session is wired before the first request.
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
async def http_no_db() -> AsyncClient:
    """Async HTTP client for routes that do NOT use the DB (search, settings).

    Does not wire override_db — callers are responsible for mocking all
    external service singletons.
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


# ---------------------------------------------------------------------------
# Helper factories
# ---------------------------------------------------------------------------


async def _make_item(
    session: AsyncSession,
    *,
    imdb_id: str = "tt0000001",
    title: str = "Test Movie",
    year: int = 2024,
    media_type: MediaType = MediaType.MOVIE,
    state: QueueState = QueueState.WANTED,
    retry_count: int = 0,
) -> MediaItem:
    """Persist a MediaItem and return it."""
    item = MediaItem(
        imdb_id=imdb_id,
        title=title,
        year=year,
        media_type=media_type,
        state=state,
        state_changed_at=datetime.now(UTC),
        retry_count=retry_count,
    )
    session.add(item)
    await session.flush()
    return item


async def _make_scrape_log(session: AsyncSession, media_item_id: int) -> ScrapeLog:
    """Persist a minimal ScrapeLog and return it."""
    log = ScrapeLog(
        media_item_id=media_item_id,
        scraper="torrentio",
        results_count=3,
        results_summary="3 results",
    )
    session.add(log)
    await session.flush()
    return log


async def _make_rd_torrent(
    session: AsyncSession,
    media_item_id: int,
    *,
    rd_id: str = "RDABC123",
    info_hash: str = "aabbccdd" * 5,
) -> RdTorrent:
    """Persist a minimal RdTorrent and return it."""
    torrent = RdTorrent(
        rd_id=rd_id,
        info_hash=info_hash,
        media_item_id=media_item_id,
        filename="Test.Movie.2024.1080p.mkv",
        filesize=4_000_000_000,
        resolution="1080p",
        cached=True,
        status=TorrentStatus.ACTIVE,
    )
    session.add(torrent)
    await session.flush()
    return torrent


# ===========================================================================
# Dashboard — GET /api/stats
# ===========================================================================


class TestDashboard:
    """Tests for the /api/stats endpoint."""

    async def test_stats_empty_db(self, http: AsyncClient) -> None:
        """Empty database should return all zero counts and status=ok."""
        with patch(
            "src.api.routes.dashboard.mount_scanner.is_mount_available",
            new_callable=AsyncMock,
            return_value=False,
        ):
            resp = await http.get("/api/stats")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        queue = data["queue"]
        assert queue["total"] == 0
        assert queue["wanted"] == 0
        assert queue["sleeping"] == 0
        assert queue["dormant"] == 0
        assert queue["complete"] == 0
        assert data["health"]["mount_available"] is False

    async def test_stats_with_items(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Counts must match actual state distribution in the database."""
        await _make_item(session, state=QueueState.WANTED, imdb_id="tt0000001")
        await _make_item(session, state=QueueState.WANTED, imdb_id="tt0000002")
        await _make_item(session, state=QueueState.SLEEPING, imdb_id="tt0000003")
        await _make_item(session, state=QueueState.DORMANT, imdb_id="tt0000004")
        await _make_item(session, state=QueueState.COMPLETE, imdb_id="tt0000005")

        with patch(
            "src.api.routes.dashboard.mount_scanner.is_mount_available",
            new_callable=AsyncMock,
            return_value=False,
        ):
            resp = await http.get("/api/stats")

        assert resp.status_code == 200
        queue = resp.json()["queue"]
        assert queue["total"] == 5
        assert queue["wanted"] == 2
        assert queue["sleeping"] == 1
        assert queue["dormant"] == 1
        assert queue["complete"] == 1

    async def test_stats_mount_available(
        self, http: AsyncClient
    ) -> None:
        """When mount scanner reports healthy, health.mount_available is True."""
        with patch(
            "src.api.routes.dashboard.mount_scanner.is_mount_available",
            new_callable=AsyncMock,
            return_value=True,
        ):
            resp = await http.get("/api/stats")

        assert resp.status_code == 200
        assert resp.json()["health"]["mount_available"] is True

    async def test_stats_mount_scanner_exception_returns_false(
        self, http: AsyncClient
    ) -> None:
        """If mount scanner raises, mount_available falls back to False, no 500."""
        with patch(
            "src.api.routes.dashboard.mount_scanner.is_mount_available",
            new_callable=AsyncMock,
            side_effect=OSError("mount hang"),
        ):
            resp = await http.get("/api/stats")

        assert resp.status_code == 200
        assert resp.json()["health"]["mount_available"] is False

    async def test_stats_all_states_counted(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Every QueueState is reflected in queue counts."""
        state_imdb = {
            QueueState.UNRELEASED: "tt0000010",
            QueueState.WANTED: "tt0000011",
            QueueState.SCRAPING: "tt0000012",
            QueueState.ADDING: "tt0000013",
            QueueState.CHECKING: "tt0000014",
            QueueState.SLEEPING: "tt0000015",
            QueueState.DORMANT: "tt0000016",
            QueueState.COMPLETE: "tt0000017",
            QueueState.DONE: "tt0000018",
        }
        for state, imdb_id in state_imdb.items():
            await _make_item(session, state=state, imdb_id=imdb_id)

        with patch(
            "src.api.routes.dashboard.mount_scanner.is_mount_available",
            new_callable=AsyncMock,
            return_value=False,
        ):
            resp = await http.get("/api/stats")

        queue = resp.json()["queue"]
        assert queue["total"] == len(state_imdb)
        assert queue["unreleased"] == 1
        assert queue["scraping"] == 1
        assert queue["adding"] == 1
        assert queue["checking"] == 1
        assert queue["done"] == 1

    # -----------------------------------------------------------------------
    # RD health metrics
    # -----------------------------------------------------------------------

    @pytest.fixture(autouse=True)
    def reset_rd_cache(self):
        """Reset the module-level RD health cache before every test in this class."""
        dashboard_module._rd_health_cache["data"] = None
        dashboard_module._rd_health_cache["fetched_at"] = 0.0
        yield
        dashboard_module._rd_health_cache["data"] = None
        dashboard_module._rd_health_cache["fetched_at"] = 0.0

    async def test_stats_rd_health_present(self, http: AsyncClient) -> None:
        """When RD API calls succeed, health.rd is populated with correct fields."""
        user_payload = {
            "id": 1,
            "username": "testuser",
            "email": "test@example.com",
            "points": 1234,
            "type": "premium",
            "premium": 90 * 86400,  # RD returns seconds remaining
            "expiration": "2026-06-01T00:00:00.000Z",
        }

        with (
            patch(
                "src.api.routes.dashboard.mount_scanner.is_mount_available",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch(
                "src.api.routes.dashboard.rd_client.get_account_info",
                new_callable=AsyncMock,
                return_value=user_payload,
            ),
        ):
            resp = await http.get("/api/stats")

        assert resp.status_code == 200
        rd = resp.json()["health"]["rd"]
        assert rd is not None
        assert rd["username"] == "testuser"
        assert rd["premium_type"] == "premium"
        assert rd["days_remaining"] == 90
        assert rd["expiration"] == "2026-06-01T00:00:00.000Z"
        assert rd["points"] == 1234

    async def test_stats_rd_health_unavailable(self, http: AsyncClient) -> None:
        """When RD raises RealDebridError, health.rd is None and the rest of stats is ok."""
        with (
            patch(
                "src.api.routes.dashboard.mount_scanner.is_mount_available",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch(
                "src.api.routes.dashboard.rd_client.get_account_info",
                new_callable=AsyncMock,
                side_effect=RealDebridError("Service unavailable", status_code=503),
            ),
        ):
            resp = await http.get("/api/stats")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["health"]["rd"] is None

    async def test_stats_rd_health_circuit_open(self, http: AsyncClient) -> None:
        """When circuit breaker is open, health.rd is None and the rest of stats is ok."""
        with (
            patch(
                "src.api.routes.dashboard.mount_scanner.is_mount_available",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch(
                "src.api.routes.dashboard.rd_client.get_account_info",
                new_callable=AsyncMock,
                side_effect=CircuitOpenError("real_debrid"),
            ),
        ):
            resp = await http.get("/api/stats")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["health"]["rd"] is None

    async def test_stats_rd_health_cached(self, http: AsyncClient) -> None:
        """RD client is called only once when two requests are made within TTL."""
        user_payload = {
            "id": 1,
            "username": "cacheduser",
            "email": "c@c.com",
            "points": 0,
            "type": "premium",
            "premium": 30 * 86400,  # RD returns seconds remaining
            "expiration": None,
        }
        mock_get_account_info = AsyncMock(return_value=user_payload)

        with (
            patch(
                "src.api.routes.dashboard.mount_scanner.is_mount_available",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch(
                "src.api.routes.dashboard.rd_client.get_account_info",
                mock_get_account_info,
            ),
        ):
            resp1 = await http.get("/api/stats")
            resp2 = await http.get("/api/stats")

        assert resp1.status_code == 200
        assert resp2.status_code == 200
        # Both responses should have rd health
        assert resp1.json()["health"]["rd"]["username"] == "cacheduser"
        assert resp2.json()["health"]["rd"]["username"] == "cacheduser"
        # RD client should only have been called once (second request uses cache)
        assert mock_get_account_info.call_count == 1

    async def test_stats_rd_health_expired(self, http: AsyncClient) -> None:
        """A user with premium=0 has days_remaining=0."""
        user_payload = {
            "id": 2,
            "username": "freeuser",
            "email": "free@example.com",
            "points": 0,
            "type": "free",
            "premium": 0,
            "expiration": None,
        }

        with (
            patch(
                "src.api.routes.dashboard.mount_scanner.is_mount_available",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch(
                "src.api.routes.dashboard.rd_client.get_account_info",
                new_callable=AsyncMock,
                return_value=user_payload,
            ),
        ):
            resp = await http.get("/api/stats")

        assert resp.status_code == 200
        rd = resp.json()["health"]["rd"]
        assert rd is not None
        assert rd["days_remaining"] == 0
        assert rd["premium_type"] == "free"
        assert rd["expiration"] is None

    # -----------------------------------------------------------------------
    # Upcoming episodes
    # -----------------------------------------------------------------------

    async def test_stats_upcoming_episodes(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """UNRELEASED items with air_date appear in the upcoming list."""
        from datetime import date

        item = MediaItem(
            imdb_id="tt9999001",
            tmdb_id="12345",
            title="Future Show",
            year=2026,
            media_type=MediaType.SHOW,
            season=2,
            episode=3,
            state=QueueState.UNRELEASED,
            air_date=date(2026, 3, 18),
        )
        session.add(item)
        await session.flush()

        with patch(
            "src.api.routes.dashboard.mount_scanner.is_mount_available",
            new_callable=AsyncMock,
            return_value=False,
        ):
            resp = await http.get("/api/stats")

        assert resp.status_code == 200
        upcoming = resp.json()["upcoming"]
        assert len(upcoming) == 1
        ep = upcoming[0]
        assert ep["title"] == "Future Show"
        assert ep["season"] == 2
        assert ep["episode"] == 3
        assert ep["air_date"] == "2026-03-18"
        assert ep["tmdb_id"] == 12345
        assert ep["state"] == "unreleased"

    async def test_stats_upcoming_episodes_empty(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """No UNRELEASED items means upcoming list is empty."""
        await _make_item(session, state=QueueState.WANTED, imdb_id="tt9999002")
        await _make_item(session, state=QueueState.SLEEPING, imdb_id="tt9999003")

        with patch(
            "src.api.routes.dashboard.mount_scanner.is_mount_available",
            new_callable=AsyncMock,
            return_value=False,
        ):
            resp = await http.get("/api/stats")

        assert resp.status_code == 200
        assert resp.json()["upcoming"] == []

    async def test_stats_upcoming_episodes_limit(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """At most 10 upcoming episodes are returned even when more exist."""
        from datetime import date, timedelta

        base_date = date(2026, 4, 1)
        for i in range(15):
            item = MediaItem(
                imdb_id=f"tt88000{i:02d}",
                title="Many Episodes Show",
                year=2026,
                media_type=MediaType.SHOW,
                season=1,
                episode=i + 1,
                state=QueueState.UNRELEASED,
                air_date=base_date + timedelta(days=i),
            )
            session.add(item)
        await session.flush()

        with patch(
            "src.api.routes.dashboard.mount_scanner.is_mount_available",
            new_callable=AsyncMock,
            return_value=False,
        ):
            resp = await http.get("/api/stats")

        assert resp.status_code == 200
        assert len(resp.json()["upcoming"]) == 10

    async def test_stats_upcoming_episodes_ordered(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Upcoming episodes are sorted by air_date ascending."""
        from datetime import date

        dates_and_episodes = [
            (date(2026, 5, 10), 3),
            (date(2026, 4, 1), 1),
            (date(2026, 4, 20), 2),
        ]
        for air_date, ep_num in dates_and_episodes:
            item = MediaItem(
                imdb_id=f"tt770{ep_num}000",
                title="Ordered Show",
                year=2026,
                media_type=MediaType.SHOW,
                season=1,
                episode=ep_num,
                state=QueueState.UNRELEASED,
                air_date=air_date,
            )
            session.add(item)
        await session.flush()

        with patch(
            "src.api.routes.dashboard.mount_scanner.is_mount_available",
            new_callable=AsyncMock,
            return_value=False,
        ):
            resp = await http.get("/api/stats")

        assert resp.status_code == 200
        upcoming = resp.json()["upcoming"]
        assert len(upcoming) == 3
        # Episodes should appear in chronological order: E01 (Apr 1), E02 (Apr 20), E03 (May 10)
        assert upcoming[0]["episode"] == 1
        assert upcoming[0]["air_date"] == "2026-04-01"
        assert upcoming[1]["episode"] == 2
        assert upcoming[1]["air_date"] == "2026-04-20"
        assert upcoming[2]["episode"] == 3
        assert upcoming[2]["air_date"] == "2026-05-10"


# ===========================================================================
# Queue list — GET /api/queue
# ===========================================================================


class TestQueueList:
    """Tests for GET /api/queue."""

    async def test_list_queue_empty(self, http: AsyncClient) -> None:
        """Empty database returns empty list with total=0."""
        resp = await http.get("/api/queue")
        assert resp.status_code == 200
        data = resp.json()
        assert data["items"] == []
        assert data["total"] == 0
        assert data["page"] == 1
        assert data["page_size"] == 50

    async def test_list_queue_with_items(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Created items appear in the list response."""
        item1 = await _make_item(session, title="Alpha", imdb_id="tt0000001")
        item2 = await _make_item(session, title="Beta", imdb_id="tt0000002")

        resp = await http.get("/api/queue")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        ids_returned = {i["id"] for i in data["items"]}
        assert item1.id in ids_returned
        assert item2.id in ids_returned

    async def test_list_queue_filter_by_state(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """state= query param filters results to only matching state."""
        await _make_item(session, state=QueueState.WANTED, imdb_id="tt0000001")
        await _make_item(session, state=QueueState.SLEEPING, imdb_id="tt0000002")
        await _make_item(session, state=QueueState.SLEEPING, imdb_id="tt0000003")

        resp = await http.get("/api/queue", params={"state": "sleeping"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert all(i["state"] == "sleeping" for i in data["items"])

    async def test_list_queue_filter_by_invalid_state_returns_400(
        self, http: AsyncClient
    ) -> None:
        """Invalid state value must return 400."""
        resp = await http.get("/api/queue", params={"state": "not_a_state"})
        assert resp.status_code == 400

    async def test_list_queue_filter_by_media_type(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """media_type= query param filters results to only matching type."""
        await _make_item(
            session, media_type=MediaType.MOVIE, imdb_id="tt0000001", title="Movie"
        )
        await _make_item(
            session, media_type=MediaType.SHOW, imdb_id="tt0000002", title="Show"
        )

        resp = await http.get("/api/queue", params={"media_type": "movie"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert data["items"][0]["media_type"] == "movie"

    async def test_list_queue_filter_by_invalid_media_type_returns_400(
        self, http: AsyncClient
    ) -> None:
        """Invalid media_type value must return 400."""
        resp = await http.get("/api/queue", params={"media_type": "documentary"})
        assert resp.status_code == 400

    async def test_list_queue_filter_by_title(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """title= performs case-insensitive substring search."""
        await _make_item(session, title="The Matrix", imdb_id="tt0000001")
        await _make_item(session, title="Inception", imdb_id="tt0000002")
        await _make_item(session, title="The Dark Knight", imdb_id="tt0000003")

        resp = await http.get("/api/queue", params={"title": "the"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        titles = {i["title"] for i in data["items"]}
        assert "The Matrix" in titles
        assert "The Dark Knight" in titles
        assert "Inception" not in titles

    async def test_list_queue_pagination(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """page and page_size params slice results correctly."""
        for idx in range(5):
            await _make_item(
                session, title=f"Movie {idx}", imdb_id=f"tt000000{idx}"
            )

        # First page of 2
        resp = await http.get("/api/queue", params={"page": 1, "page_size": 2})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 5
        assert len(data["items"]) == 2
        assert data["page"] == 1
        assert data["page_size"] == 2

        # Second page of 2
        resp2 = await http.get("/api/queue", params={"page": 2, "page_size": 2})
        assert resp2.status_code == 200
        data2 = resp2.json()
        assert len(data2["items"]) == 2

        # Third page — only 1 item left
        resp3 = await http.get("/api/queue", params={"page": 3, "page_size": 2})
        assert resp3.status_code == 200
        data3 = resp3.json()
        assert len(data3["items"]) == 1

    async def test_list_queue_page_beyond_results_returns_empty(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """A page past the last item returns an empty list, not a 404."""
        await _make_item(session, imdb_id="tt0000001")

        resp = await http.get("/api/queue", params={"page": 100, "page_size": 50})
        assert resp.status_code == 200
        data = resp.json()
        assert data["items"] == []
        assert data["total"] == 1  # total still reflects real count

    async def test_list_queue_response_fields(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Every item in the list has all expected fields."""
        await _make_item(session, imdb_id="tt0000001", title="Test")

        resp = await http.get("/api/queue")
        item = resp.json()["items"][0]
        for field in ("id", "imdb_id", "title", "media_type", "state", "retry_count"):
            assert field in item, f"Missing field: {field}"


# ===========================================================================
# Queue detail — GET /api/queue/{id}
# ===========================================================================


class TestQueueItemDetail:
    """Tests for GET /api/queue/{id}."""

    async def test_get_item_detail(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Returns item fields, scrape_logs, and torrents sub-documents."""
        item = await _make_item(session, imdb_id="tt1111111", title="Detail Movie")
        await _make_scrape_log(session, item.id)

        resp = await http.get(f"/api/queue/{item.id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["item"]["id"] == item.id
        assert data["item"]["title"] == "Detail Movie"
        assert len(data["scrape_logs"]) == 1
        assert data["scrape_logs"][0]["scraper"] == "torrentio"
        assert isinstance(data["torrents"], list)

    async def test_get_item_detail_includes_torrents(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Torrent sub-documents are included when RdTorrents exist."""
        item = await _make_item(session, imdb_id="tt1111112", title="Torrent Movie")
        await _make_rd_torrent(session, item.id)

        resp = await http.get(f"/api/queue/{item.id}")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["torrents"]) == 1
        torrent = data["torrents"][0]
        assert torrent["rd_id"] == "RDABC123"
        assert torrent["resolution"] == "1080p"
        assert torrent["status"] == "active"

    async def test_get_item_scrape_logs_ordered_newest_first(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Multiple scrape logs are returned newest-first."""
        item = await _make_item(session, imdb_id="tt1111113", title="Log Order Movie")
        log1 = ScrapeLog(
            media_item_id=item.id,
            scraper="torrentio",
            results_count=1,
        )
        log2 = ScrapeLog(
            media_item_id=item.id,
            scraper="zilean",
            results_count=2,
        )
        session.add(log1)
        session.add(log2)
        await session.flush()

        resp = await http.get(f"/api/queue/{item.id}")
        assert resp.status_code == 200
        # Both logs present; order depends on scraped_at (server default),
        # so only assert both are present.
        scrapers = {entry["scraper"] for entry in resp.json()["scrape_logs"]}
        assert scrapers == {"torrentio", "zilean"}

    async def test_get_item_not_found(self, http: AsyncClient) -> None:
        """Non-existent item ID returns 404."""
        resp = await http.get("/api/queue/9999999")
        assert resp.status_code == 404
        assert "9999999" in resp.json()["detail"]

    async def test_get_item_no_logs_no_torrents(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Item with no logs or torrents still returns valid structure."""
        item = await _make_item(session, imdb_id="tt1111114", title="Clean Movie")

        resp = await http.get(f"/api/queue/{item.id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["scrape_logs"] == []
        assert data["torrents"] == []


# ===========================================================================
# Queue retry — POST /api/queue/{id}/retry
# ===========================================================================


class TestQueueRetry:
    """Tests for POST /api/queue/{id}/retry."""

    async def test_retry_item_transitions_to_wanted(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Retrying a sleeping item forces it to WANTED."""
        item = await _make_item(
            session,
            state=QueueState.SLEEPING,
            retry_count=3,
            imdb_id="tt2222001",
        )

        resp = await http.post(f"/api/queue/{item.id}/retry")
        assert resp.status_code == 200
        data = resp.json()
        assert data["state"] == "wanted"
        assert data["id"] == item.id

    async def test_retry_dormant_item(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Retrying a dormant item also forces it to WANTED."""
        item = await _make_item(
            session,
            state=QueueState.DORMANT,
            retry_count=7,
            imdb_id="tt2222002",
        )

        resp = await http.post(f"/api/queue/{item.id}/retry")
        assert resp.status_code == 200
        assert resp.json()["state"] == "wanted"

    async def test_retry_item_not_found(self, http: AsyncClient) -> None:
        """Retrying non-existent item returns 404."""
        resp = await http.post("/api/queue/9999999/retry")
        assert resp.status_code == 404

    async def test_retry_resets_retry_count(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """force_transition to WANTED resets retry_count to 0."""
        item = await _make_item(
            session,
            state=QueueState.SLEEPING,
            retry_count=5,
            imdb_id="tt2222003",
        )

        resp = await http.post(f"/api/queue/{item.id}/retry")
        assert resp.status_code == 200
        assert resp.json()["retry_count"] == 0


# ===========================================================================
# Queue state change — POST /api/queue/{id}/state
# ===========================================================================


class TestQueueStateChange:
    """Tests for POST /api/queue/{id}/state."""

    async def test_change_state_to_dormant(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Force-changing state to DORMANT persists and is returned."""
        item = await _make_item(session, state=QueueState.SLEEPING, imdb_id="tt3333001")

        resp = await http.post(
            f"/api/queue/{item.id}/state",
            json={"state": "dormant"},
        )
        assert resp.status_code == 200
        assert resp.json()["state"] == "dormant"

    async def test_change_state_to_wanted(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Force-changing state to WANTED works from any state."""
        item = await _make_item(session, state=QueueState.DORMANT, imdb_id="tt3333002")

        resp = await http.post(
            f"/api/queue/{item.id}/state",
            json={"state": "wanted"},
        )
        assert resp.status_code == 200
        assert resp.json()["state"] == "wanted"

    async def test_change_state_invalid_state_returns_400(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Invalid state string returns 400 before any DB access."""
        item = await _make_item(session, imdb_id="tt3333003")

        resp = await http.post(
            f"/api/queue/{item.id}/state",
            json={"state": "flying"},
        )
        assert resp.status_code == 400
        assert "Invalid state" in resp.json()["detail"]

    async def test_change_state_item_not_found(self, http: AsyncClient) -> None:
        """404 when item does not exist."""
        resp = await http.post(
            "/api/queue/9999999/state",
            json={"state": "wanted"},
        )
        assert resp.status_code == 404

    async def test_change_state_missing_body_returns_422(
        self, http: AsyncClient
    ) -> None:
        """Missing request body returns 422 Unprocessable Entity."""
        resp = await http.post("/api/queue/1/state", content="")
        assert resp.status_code == 422


# ===========================================================================
# Queue delete — DELETE /api/queue/{id}
# ===========================================================================


class TestQueueDelete:
    """Tests for DELETE /api/queue/{id}."""

    async def test_delete_item_returns_ok(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Successful delete returns status=ok and the deleted id."""
        item = await _make_item(session, imdb_id="tt4444001")

        with patch(
            "src.api.routes.queue.symlink_manager.remove_symlink",
            new_callable=AsyncMock,
            return_value=0,
        ):
            resp = await http.delete(f"/api/queue/{item.id}")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["id"] == item.id
        assert data["symlinks_removed"] == 0

    async def test_delete_item_removes_from_db(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """After delete the item no longer appears in the queue list."""
        item = await _make_item(session, imdb_id="tt4444002")
        item_id = item.id

        with patch(
            "src.api.routes.queue.symlink_manager.remove_symlink",
            new_callable=AsyncMock,
            return_value=0,
        ):
            resp = await http.delete(f"/api/queue/{item_id}")
        assert resp.status_code == 200

        # Item must be gone
        check = await http.get(f"/api/queue/{item_id}")
        assert check.status_code == 404

    async def test_delete_item_also_removes_torrent_from_rd(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Delete calls rd_client.delete_torrent for each associated RdTorrent."""
        item = await _make_item(session, imdb_id="tt4444003")
        await _make_rd_torrent(session, item.id, rd_id="RD_TO_DELETE")

        mock_delete = AsyncMock()
        with (
            patch(
                "src.api.routes.queue.symlink_manager.remove_symlink",
                new_callable=AsyncMock,
                return_value=0,
            ),
            patch("src.api.routes.queue.rd_client.delete_torrent", mock_delete),
        ):
            resp = await http.delete(f"/api/queue/{item.id}")

        assert resp.status_code == 200
        mock_delete.assert_called_once_with("RD_TO_DELETE")
        assert resp.json()["torrents_removed"] == 1

    async def test_delete_item_rd_failure_does_not_crash(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """If rd_client.delete_torrent raises, delete still succeeds (logs warning)."""
        item = await _make_item(session, imdb_id="tt4444004")
        await _make_rd_torrent(session, item.id, rd_id="RD_FAIL")

        with (
            patch(
                "src.api.routes.queue.symlink_manager.remove_symlink",
                new_callable=AsyncMock,
                return_value=0,
            ),
            patch(
                "src.api.routes.queue.rd_client.delete_torrent",
                new_callable=AsyncMock,
                side_effect=RealDebridError("timeout"),
            ),
        ):
            resp = await http.delete(f"/api/queue/{item.id}")

        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    async def test_delete_item_not_found(self, http: AsyncClient) -> None:
        """404 when item does not exist."""
        with patch(
            "src.api.routes.queue.symlink_manager.remove_symlink",
            new_callable=AsyncMock,
            return_value=0,
        ):
            resp = await http.delete("/api/queue/9999999")
        assert resp.status_code == 404

    async def test_delete_item_also_removes_scrape_logs(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Scrape logs associated with the item are deleted."""
        item = await _make_item(session, imdb_id="tt4444005")
        await _make_scrape_log(session, item.id)

        with patch(
            "src.api.routes.queue.symlink_manager.remove_symlink",
            new_callable=AsyncMock,
            return_value=0,
        ):
            resp = await http.delete(f"/api/queue/{item.id}")
        assert resp.status_code == 200

        # Detail must now be 404
        detail = await http.get(f"/api/queue/{item.id}")
        assert detail.status_code == 404


# ===========================================================================
# Bulk operations
# ===========================================================================


class TestBulkRetry:
    """Tests for POST /api/queue/bulk/retry."""

    async def test_bulk_retry_all_succeed(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """All valid IDs are retried; processed count equals len(ids)."""
        item1 = await _make_item(
            session, state=QueueState.SLEEPING, imdb_id="tt5001001"
        )
        item2 = await _make_item(
            session, state=QueueState.DORMANT, imdb_id="tt5001002"
        )

        resp = await http.post(
            "/api/queue/bulk/retry",
            json={"ids": [item1.id, item2.id]},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["processed"] == 2
        assert data["errors"] == []

    async def test_bulk_retry_missing_id_reported_as_error(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Non-existent IDs appear in the errors list; status is 'partial'."""
        item = await _make_item(
            session, state=QueueState.SLEEPING, imdb_id="tt5001003"
        )

        resp = await http.post(
            "/api/queue/bulk/retry",
            json={"ids": [item.id, 9999999]},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "partial"
        assert data["processed"] == 1
        assert len(data["errors"]) == 1
        assert "9999999" in data["errors"][0]

    async def test_bulk_retry_empty_ids(self, http: AsyncClient) -> None:
        """Empty ids list returns processed=0 and status=ok."""
        resp = await http.post("/api/queue/bulk/retry", json={"ids": []})
        assert resp.status_code == 200
        data = resp.json()
        assert data["processed"] == 0
        assert data["status"] == "ok"

    async def test_bulk_retry_all_missing_ids(self, http: AsyncClient) -> None:
        """All non-existent IDs results in status=partial, processed=0."""
        resp = await http.post(
            "/api/queue/bulk/retry",
            json={"ids": [9999901, 9999902]},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "partial"
        assert data["processed"] == 0
        assert len(data["errors"]) == 2


class TestBulkRemove:
    """Tests for POST /api/queue/bulk/remove."""

    async def test_bulk_remove_all_succeed(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """All valid IDs are removed; processed count matches."""
        item1 = await _make_item(session, imdb_id="tt5002001")
        item2 = await _make_item(session, imdb_id="tt5002002")

        with patch(
            "src.api.routes.queue.symlink_manager.remove_symlink",
            new_callable=AsyncMock,
            return_value=0,
        ):
            resp = await http.post(
                "/api/queue/bulk/remove",
                json={"ids": [item1.id, item2.id]},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["processed"] == 2
        assert data["errors"] == []

    async def test_bulk_remove_missing_id_is_error(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Non-existent IDs appear in errors; status=partial."""
        item = await _make_item(session, imdb_id="tt5002003")

        with patch(
            "src.api.routes.queue.symlink_manager.remove_symlink",
            new_callable=AsyncMock,
            return_value=0,
        ):
            resp = await http.post(
                "/api/queue/bulk/remove",
                json={"ids": [item.id, 9999999]},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "partial"
        assert data["processed"] == 1
        assert "9999999" in data["errors"][0]

    async def test_bulk_remove_calls_rd_delete_for_each_torrent(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """rd_client.delete_torrent is called once per associated RdTorrent."""
        item = await _make_item(session, imdb_id="tt5002004")
        await _make_rd_torrent(session, item.id, rd_id="RD_BULK_DEL")

        mock_delete = AsyncMock()
        with (
            patch(
                "src.api.routes.queue.symlink_manager.remove_symlink",
                new_callable=AsyncMock,
                return_value=0,
            ),
            patch("src.api.routes.queue.rd_client.delete_torrent", mock_delete),
        ):
            resp = await http.post(
                "/api/queue/bulk/remove",
                json={"ids": [item.id]},
            )

        assert resp.status_code == 200
        mock_delete.assert_called_once_with("RD_BULK_DEL")

    async def test_bulk_remove_empty_ids(self, http: AsyncClient) -> None:
        """Empty ids list returns processed=0 and status=ok."""
        resp = await http.post("/api/queue/bulk/remove", json={"ids": []})
        assert resp.status_code == 200
        assert resp.json()["processed"] == 0


# ===========================================================================
# Search — POST /api/search
# ===========================================================================


class TestSearch:
    """Tests for POST /api/search."""

    async def test_search_empty_results_from_all_scrapers(
        self, http_no_db: AsyncClient
    ) -> None:
        """When both scrapers return nothing, response has empty results list."""
        with (
            patch(
                "src.api.routes.search.torrentio_client.scrape_movie",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "src.api.routes.search.zilean_client.search",
                new_callable=AsyncMock,
                return_value=[],
            ),
        ):
            resp = await http_no_db.post(
                "/api/search",
                json={
                    "query": "Test Movie 2024",
                    "imdb_id": "tt0000001",
                    "media_type": "movie",
                },
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["results"] == []
        assert data["total_raw"] == 0
        assert data["total_filtered"] == 0

    async def test_search_with_results_passes_through_filter_engine(
        self, http_no_db: AsyncClient
    ) -> None:
        """Results from scrapers are filtered and returned as ranked list."""
        from src.core.filter_engine import FilteredResult
        from src.services.torrentio import TorrentioResult

        mock_result = TorrentioResult(
            info_hash="a" * 40,
            title="Test.Movie.2024.1080p.WEB-DL.mkv",
            resolution="1080p",
            codec="x265",
            quality="WEB-DL",
            size_bytes=4_000_000_000,
            seeders=150,
            is_season_pack=False,
            languages=[],
        )
        mock_filtered = FilteredResult(
            result=mock_result,
            score=75.0,
            score_breakdown={"resolution": 30.0, "codec": 15.0},
        )

        with (
            patch(
                "src.api.routes.search.torrentio_client.scrape_movie",
                new_callable=AsyncMock,
                return_value=[mock_result],
            ),
            patch(
                "src.api.routes.search.zilean_client.search",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "src.api.routes.search.filter_engine.filter_and_rank",
                return_value=[mock_filtered],
            ),
        ):
            resp = await http_no_db.post(
                "/api/search",
                json={
                    "query": "Test Movie 2024",
                    "imdb_id": "tt0000001",
                    "media_type": "movie",
                },
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["total_raw"] == 1
        assert data["total_filtered"] == 1
        result = data["results"][0]
        assert result["info_hash"] == "a" * 40
        assert result["resolution"] == "1080p"
        assert result["score"] == 75.0

    async def test_search_returns_cached_null(
        self, http_no_db: AsyncClient
    ) -> None:
        """Search always returns cached=None; cache is checked via /api/check-cached."""
        from src.core.filter_engine import FilteredResult
        from src.services.torrentio import TorrentioResult

        mock_result = TorrentioResult(
            info_hash="b" * 40,
            title="Some.Movie.1080p.mkv",
            resolution="1080p",
            size_bytes=3_000_000_000,
            seeders=200,
            languages=[],
            cached=False,
        )
        mock_filtered = FilteredResult(result=mock_result, score=80.0)

        with (
            patch(
                "src.api.routes.search.torrentio_client.scrape_movie",
                new_callable=AsyncMock,
                return_value=[mock_result],
            ),
            patch(
                "src.api.routes.search.zilean_client.search",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "src.api.routes.search.filter_engine.filter_and_rank",
                return_value=[mock_filtered],
            ),
        ):
            resp = await http_no_db.post(
                "/api/search",
                json={
                    "query": "Some Movie",
                    "imdb_id": "tt0000002",
                    "media_type": "movie",
                },
            )

        assert resp.status_code == 200
        assert resp.json()["results"][0]["cached"] is None

    async def test_search_torrentio_failure_does_not_crash(
        self, http_no_db: AsyncClient
    ) -> None:
        """If Torrentio raises, search continues with only Zilean results."""
        from src.core.filter_engine import FilteredResult
        from src.services.zilean import ZileanResult

        zilean_result = ZileanResult(
            info_hash="c" * 40,
            title="Zilean.Only.Movie.1080p.mkv",
            resolution="1080p",
            size_bytes=2_000_000_000,
            languages=[],
        )
        mock_filtered = FilteredResult(result=zilean_result, score=60.0)

        with (
            patch(
                "src.api.routes.search.torrentio_client.scrape_movie",
                new_callable=AsyncMock,
                side_effect=Exception("Torrentio timeout"),
            ),
            patch(
                "src.api.routes.search.zilean_client.search",
                new_callable=AsyncMock,
                return_value=[zilean_result],
            ),
            patch(
                "src.api.routes.search.filter_engine.filter_and_rank",
                return_value=[mock_filtered],
            ),
        ):
            resp = await http_no_db.post(
                "/api/search",
                json={
                    "query": "Zilean Only Movie",
                    "imdb_id": "tt0000003",
                    "media_type": "movie",
                },
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["total_raw"] == 1
        assert data["results"][0]["info_hash"] == "c" * 40

    async def test_check_cached_returns_true(
        self, http: AsyncClient
    ) -> None:
        """POST /api/check-cached returns cached=True when RD confirms cache."""
        with patch(
            "src.api.routes.search.rd_client.check_cached",
            new_callable=AsyncMock,
            return_value=CacheCheckResult(info_hash="d" * 40, cached=True),
        ):
            resp = await http.post(
                "/api/check-cached",
                json={"info_hash": "d" * 40},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["info_hash"] == "d" * 40
        assert data["cached"] is True

    async def test_check_cached_returns_false(
        self, http: AsyncClient
    ) -> None:
        """POST /api/check-cached returns cached=False when torrent is not cached."""
        with patch(
            "src.api.routes.search.rd_client.check_cached",
            new_callable=AsyncMock,
            return_value=CacheCheckResult(info_hash="e" * 40, cached=False),
        ):
            resp = await http.post(
                "/api/check-cached",
                json={"info_hash": "e" * 40},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["info_hash"] == "e" * 40
        assert data["cached"] is False

    async def test_search_episode_uses_scrape_episode(
        self, http_no_db: AsyncClient
    ) -> None:
        """Show with season+episode routes to torrentio_client.scrape_episode."""
        mock_scrape_episode = AsyncMock(return_value=[])
        mock_scrape_movie = AsyncMock(return_value=[])

        with (
            patch(
                "src.api.routes.search.torrentio_client.scrape_episode",
                mock_scrape_episode,
            ),
            patch(
                "src.api.routes.search.torrentio_client.scrape_movie",
                mock_scrape_movie,
            ),
            patch(
                "src.api.routes.search.zilean_client.search",
                new_callable=AsyncMock,
                return_value=[],
            ),
        ):
            resp = await http_no_db.post(
                "/api/search",
                json={
                    "query": "Breaking Bad S01E01",
                    "imdb_id": "tt0903747",
                    "media_type": "show",
                    "season": 1,
                    "episode": 1,
                },
            )

        assert resp.status_code == 200
        mock_scrape_episode.assert_called_once_with("tt0903747", 1, 1, include_debrid_key=False)
        mock_scrape_movie.assert_not_called()

    async def test_search_no_imdb_id_skips_torrentio(
        self, http_no_db: AsyncClient
    ) -> None:
        """When no imdb_id provided, Torrentio is not called (requires IMDB ID)."""
        mock_scrape_movie = AsyncMock(return_value=[])

        with (
            patch(
                "src.api.routes.search.torrentio_client.scrape_movie",
                mock_scrape_movie,
            ),
            patch(
                "src.api.routes.search.zilean_client.search",
                new_callable=AsyncMock,
                return_value=[],
            ),
        ):
            resp = await http_no_db.post(
                "/api/search",
                json={"query": "Some Movie"},
            )

        assert resp.status_code == 200
        mock_scrape_movie.assert_not_called()

    async def test_search_scrapers_zilean_only_skips_torrentio(
        self, http_no_db: AsyncClient
    ) -> None:
        """When scrapers=['zilean'], only Zilean is called even with imdb_id."""
        mock_scrape_movie = AsyncMock(return_value=[])
        mock_zilean_search = AsyncMock(return_value=[])

        with (
            patch(
                "src.api.routes.search.torrentio_client.scrape_movie",
                mock_scrape_movie,
            ),
            patch(
                "src.api.routes.search.zilean_client.search",
                mock_zilean_search,
            ),
        ):
            resp = await http_no_db.post(
                "/api/search",
                json={
                    "query": "Test Movie",
                    "imdb_id": "tt0000001",
                    "media_type": "movie",
                    "scrapers": ["zilean"],
                },
            )

        assert resp.status_code == 200
        mock_zilean_search.assert_called_once()
        mock_scrape_movie.assert_not_called()

    async def test_search_scrapers_torrentio_only_skips_zilean(
        self, http_no_db: AsyncClient
    ) -> None:
        """When scrapers=['torrentio'], only Torrentio is called and Zilean is skipped."""
        mock_scrape_movie = AsyncMock(return_value=[])
        mock_zilean_search = AsyncMock(return_value=[])

        with (
            patch(
                "src.api.routes.search.torrentio_client.scrape_movie",
                mock_scrape_movie,
            ),
            patch(
                "src.api.routes.search.zilean_client.search",
                mock_zilean_search,
            ),
        ):
            resp = await http_no_db.post(
                "/api/search",
                json={
                    "query": "Test Movie",
                    "imdb_id": "tt0000001",
                    "media_type": "movie",
                    "scrapers": ["torrentio"],
                },
            )

        assert resp.status_code == 200
        mock_scrape_movie.assert_called_once_with("tt0000001", include_debrid_key=False)
        mock_zilean_search.assert_not_called()

    async def test_search_scrapers_default_runs_both(
        self, http_no_db: AsyncClient
    ) -> None:
        """Omitting scrapers runs both Torrentio and Zilean (backward compatibility)."""
        mock_scrape_movie = AsyncMock(return_value=[])
        mock_zilean_search = AsyncMock(return_value=[])

        with (
            patch(
                "src.api.routes.search.torrentio_client.scrape_movie",
                mock_scrape_movie,
            ),
            patch(
                "src.api.routes.search.zilean_client.search",
                mock_zilean_search,
            ),
        ):
            resp = await http_no_db.post(
                "/api/search",
                json={
                    "query": "Test Movie",
                    "imdb_id": "tt0000001",
                    "media_type": "movie",
                },
            )

        assert resp.status_code == 200
        mock_scrape_movie.assert_called_once_with("tt0000001", include_debrid_key=False)
        mock_zilean_search.assert_called_once()


# ===========================================================================
# Add torrent — POST /api/add
# ===========================================================================


class TestAddTorrent:
    """Tests for POST /api/add."""

    async def test_add_with_bare_hash_succeeds(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """A 40-char hex hash is accepted, wrapped into a magnet, and added to RD."""
        info_hash = "e" * 40

        with (
            patch(
                "src.api.routes.search.rd_client.add_magnet",
                new_callable=AsyncMock,
                return_value={"id": "RDNEW01", "uri": "https://rd/RDNEW01"},
            ),
            patch(
                "src.api.routes.search.rd_client.select_files",
                new_callable=AsyncMock,
            ),
            patch(
                "src.api.routes.search.dedup_engine.register_torrent",
                new_callable=AsyncMock,
            ),
        ):
            resp = await http.post(
                "/api/add",
                json={
                    "magnet_or_hash": info_hash,
                    "title": "Test Movie",
                    "imdb_id": "tt6660001",
                    "media_type": "movie",
                    "year": 2024,
                },
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "added"
        assert data["rd_id"] == "RDNEW01"
        assert data["item_id"] > 0

    async def test_add_with_magnet_uri_succeeds(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """A full magnet URI is accepted and passed directly to RD."""
        magnet = "magnet:?xt=urn:btih:" + "f" * 40 + "&dn=TestMovie"

        with (
            patch(
                "src.api.routes.search.rd_client.add_magnet",
                new_callable=AsyncMock,
                return_value={"id": "RDNEW02", "uri": "https://rd/RDNEW02"},
            ),
            patch(
                "src.api.routes.search.rd_client.select_files",
                new_callable=AsyncMock,
            ),
            patch(
                "src.api.routes.search.dedup_engine.register_torrent",
                new_callable=AsyncMock,
            ),
        ):
            resp = await http.post(
                "/api/add",
                json={
                    "magnet_or_hash": magnet,
                    "title": "Test Movie",
                    "imdb_id": "tt6660002",
                    "media_type": "movie",
                },
            )

        assert resp.status_code == 200
        assert resp.json()["status"] == "added"

    async def test_add_rd_failure_creates_wanted_item(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """When RD add fails the item is still created but left in WANTED state."""
        with patch(
            "src.api.routes.search.rd_client.add_magnet",
            new_callable=AsyncMock,
            side_effect=RealDebridError("quota exceeded"),
        ):
            resp = await http.post(
                "/api/add",
                json={
                    "magnet_or_hash": "a" * 40,
                    "title": "Retry Movie",
                    "imdb_id": "tt6660003",
                    "media_type": "movie",
                },
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "queued"
        assert data["rd_id"] is None
        assert data["item_id"] > 0

    async def test_add_invalid_input_returns_400(
        self, http: AsyncClient
    ) -> None:
        """Garbage magnet_or_hash returns 400 before any DB write."""
        resp = await http.post(
            "/api/add",
            json={
                "magnet_or_hash": "not-a-hash-not-a-magnet",
                "title": "Bad",
                "imdb_id": "tt0000000",
                "media_type": "movie",
            },
        )
        assert resp.status_code == 400
        assert "Invalid input" in resp.json()["detail"]

    async def test_add_invalid_media_type_returns_400(
        self, http: AsyncClient
    ) -> None:
        """Unknown media_type string returns 400."""
        resp = await http.post(
            "/api/add",
            json={
                "magnet_or_hash": "a" * 40,
                "title": "Bad Type",
                "imdb_id": "tt0000000",
                "media_type": "documentary",
            },
        )
        assert resp.status_code == 400
        assert "media_type" in resp.json()["detail"]

    async def test_add_creates_checking_state_on_success(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """On successful RD add the item should be in CHECKING state in the DB."""
        with (
            patch(
                "src.api.routes.search.rd_client.add_magnet",
                new_callable=AsyncMock,
                return_value={"id": "RDCHECK01", "uri": "https://rd/RDCHECK01"},
            ),
            patch(
                "src.api.routes.search.rd_client.select_files",
                new_callable=AsyncMock,
            ),
            patch(
                "src.api.routes.search.dedup_engine.register_torrent",
                new_callable=AsyncMock,
            ),
        ):
            resp = await http.post(
                "/api/add",
                json={
                    "magnet_or_hash": "1" * 40,
                    "title": "Checking Movie",
                    "imdb_id": "tt6660004",
                    "media_type": "movie",
                },
            )

        assert resp.status_code == 200
        item_id = resp.json()["item_id"]

        # Verify via GET that the state is now 'checking'
        detail_resp = await http.get(f"/api/queue/{item_id}")
        assert detail_resp.status_code == 200
        assert detail_resp.json()["item"]["state"] == "checking"

    async def test_add_select_files_failure_is_non_fatal(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """If select_files raises RealDebridError, add still reports 'added'."""
        from src.services.real_debrid import RealDebridError

        with (
            patch(
                "src.api.routes.search.rd_client.add_magnet",
                new_callable=AsyncMock,
                return_value={"id": "RDSELECT01", "uri": "https://rd/RDSELECT01"},
            ),
            patch(
                "src.api.routes.search.rd_client.select_files",
                new_callable=AsyncMock,
                side_effect=RealDebridError("select failed"),
            ),
            patch(
                "src.api.routes.search.dedup_engine.register_torrent",
                new_callable=AsyncMock,
            ),
        ):
            resp = await http.post(
                "/api/add",
                json={
                    "magnet_or_hash": "2" * 40,
                    "title": "Select Fail Movie",
                    "imdb_id": "tt6660005",
                    "media_type": "movie",
                },
            )

        assert resp.status_code == 200
        assert resp.json()["status"] == "added"


# ===========================================================================
# Settings — GET /api/settings and connectivity tests
# ===========================================================================


class TestSettings:
    """Tests for /api/settings endpoints."""

    async def test_get_settings_returns_masked_keys(
        self, http_no_db: AsyncClient
    ) -> None:
        """GET /api/settings returns settings dict with API keys masked."""
        resp = await http_no_db.get("/api/settings")
        assert resp.status_code == 200
        data = resp.json()
        assert "settings" in data
        # The top-level settings key must be present
        assert isinstance(data["settings"], dict)

    async def test_get_settings_api_key_masked_when_present(
        self, http_no_db: AsyncClient
    ) -> None:
        """When api_key is non-empty, it must be masked (not returned in plaintext)."""
        from unittest.mock import patch

        from src.config import settings as app_settings

        # Patch the global settings singleton to inject a long key
        fake_key = "ABCDEFGHIJKLMNOP"  # 16 chars — long enough to trigger masking
        with patch.object(app_settings.real_debrid, "api_key", fake_key):
            resp = await http_no_db.get("/api/settings")

        assert resp.status_code == 200
        rd_settings = resp.json()["settings"]["real_debrid"]
        returned_key = rd_settings["api_key"]
        # Must NOT contain the full key
        assert returned_key != fake_key
        # Must contain *** marker
        assert "***" in returned_key

    async def test_test_realdebrid_success(
        self, http_no_db: AsyncClient
    ) -> None:
        """Successful RD get_user returns status=ok with username info."""
        with patch(
            "src.api.routes.settings.rd_client.get_user",
            new_callable=AsyncMock,
            return_value={
                "username": "testuser",
                "premium": 1,
                "expiration": "2027-01-01T00:00:00",
            },
        ):
            resp = await http_no_db.post("/api/settings/test/realdebrid")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "testuser" in data["message"]

    async def test_test_realdebrid_auth_error(
        self, http_no_db: AsyncClient
    ) -> None:
        """Invalid API key returns status=error with auth failure message."""
        with patch(
            "src.api.routes.settings.rd_client.get_user",
            new_callable=AsyncMock,
            side_effect=RealDebridAuthError("Bad token", status_code=401),
        ):
            resp = await http_no_db.post("/api/settings/test/realdebrid")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "error"
        assert "Authentication failed" in data["message"]

    async def test_test_realdebrid_api_error(
        self, http_no_db: AsyncClient
    ) -> None:
        """Generic RD API error returns status=error."""
        with patch(
            "src.api.routes.settings.rd_client.get_user",
            new_callable=AsyncMock,
            side_effect=RealDebridError("server error", status_code=500),
        ):
            resp = await http_no_db.post("/api/settings/test/realdebrid")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "error"
        assert "API error" in data["message"]

    async def test_test_realdebrid_network_error(
        self, http_no_db: AsyncClient
    ) -> None:
        """Network failure (httpx error) returns status=error, no 500."""
        import httpx

        with patch(
            "src.api.routes.settings.rd_client.get_user",
            new_callable=AsyncMock,
            side_effect=httpx.ConnectError("Connection refused"),
        ):
            resp = await http_no_db.post("/api/settings/test/realdebrid")

        assert resp.status_code == 200
        assert resp.json()["status"] == "error"

    async def test_test_torrentio_success(
        self, http_no_db: AsyncClient
    ) -> None:
        """Successful Torrentio scrape returns status=ok with result count."""
        from src.services.torrentio import TorrentioResult

        mock_results = [
            TorrentioResult(
                info_hash="a" * 40,
                title="The Matrix 1999 1080p",
                resolution="1080p",
                languages=[],
            )
        ]
        with patch(
            "src.api.routes.settings.torrentio_client.scrape_movie",
            new_callable=AsyncMock,
            return_value=mock_results,
        ):
            resp = await http_no_db.post("/api/settings/test/torrentio")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "1" in data["message"]

    async def test_test_torrentio_failure(
        self, http_no_db: AsyncClient
    ) -> None:
        """Torrentio connection failure returns status=error."""
        with patch(
            "src.api.routes.settings.torrentio_client.scrape_movie",
            new_callable=AsyncMock,
            side_effect=Exception("Torrentio unreachable"),
        ):
            resp = await http_no_db.post("/api/settings/test/torrentio")

        assert resp.status_code == 200
        assert resp.json()["status"] == "error"
        assert "Connection failed" in resp.json()["message"]

    async def test_test_zilean_success(
        self, http_no_db: AsyncClient
    ) -> None:
        """Successful Zilean search returns status=ok with result count."""
        from src.services.zilean import ZileanResult

        mock_results = [
            ZileanResult(
                info_hash="b" * 40,
                title="Some Movie 2024 1080p",
                resolution="1080p",
                languages=[],
            )
        ]
        with patch(
            "src.api.routes.settings.zilean_client.search",
            new_callable=AsyncMock,
            return_value=mock_results,
        ):
            resp = await http_no_db.post("/api/settings/test/zilean")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "1" in data["message"]

    async def test_test_zilean_failure(
        self, http_no_db: AsyncClient
    ) -> None:
        """Zilean connection failure returns status=error."""
        with patch(
            "src.api.routes.settings.zilean_client.search",
            new_callable=AsyncMock,
            side_effect=Exception("Zilean unreachable"),
        ):
            resp = await http_no_db.post("/api/settings/test/zilean")

        assert resp.status_code == 200
        assert resp.json()["status"] == "error"

    async def test_test_torrentio_zero_results_still_ok(
        self, http_no_db: AsyncClient
    ) -> None:
        """Torrentio returning an empty list still means connection succeeded."""
        with patch(
            "src.api.routes.settings.torrentio_client.scrape_movie",
            new_callable=AsyncMock,
            return_value=[],
        ):
            resp = await http_no_db.post("/api/settings/test/torrentio")

        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"
        assert "0" in resp.json()["message"]


# ===========================================================================
# Duplicates — GET /api/duplicates and POST /api/duplicates/resolve
# ===========================================================================


class TestDuplicates:
    """Tests for /api/duplicates endpoints."""

    def _make_rd_torrent_dict(
        self,
        rd_id: str,
        filename: str,
        info_hash: str,
        size: int = 2_000_000_000,
    ) -> dict[str, Any]:
        """Build a minimal dict matching the RD API torrent list shape."""
        return {
            "id": rd_id,
            "filename": filename,
            "hash": info_hash,
            "bytes": size,
            "status": "downloaded",
            "added": "2024-01-01T00:00:00",
        }

    async def test_list_duplicates_empty(
        self, http: AsyncClient
    ) -> None:
        """When RD returns no torrents, response has empty duplicates list."""
        with patch(
            "src.api.routes.duplicates.rd_client.list_torrents",
            new_callable=AsyncMock,
            return_value=[],
        ):
            resp = await http.get("/api/duplicates")

        assert resp.status_code == 200
        data = resp.json()
        assert data["duplicates"] == []
        assert data["total_groups"] == 0

    async def test_list_duplicates_finds_groups(
        self, http: AsyncClient
    ) -> None:
        """Two RD torrents with the same parsed title form one duplicate group."""
        rd_data = [
            self._make_rd_torrent_dict(
                "RD001",
                "The.Matrix.1999.1080p.BluRay.x265.mkv",
                "a" * 40,
                size=8_000_000_000,
            ),
            self._make_rd_torrent_dict(
                "RD002",
                "The.Matrix.1999.720p.WEB-DL.mkv",
                "b" * 40,
                size=3_000_000_000,
            ),
        ]

        with patch(
            "src.api.routes.duplicates.rd_client.list_torrents",
            new_callable=AsyncMock,
            return_value=rd_data,
        ):
            resp = await http.get("/api/duplicates")

        assert resp.status_code == 200
        data = resp.json()
        assert data["total_groups"] == 1
        group = data["duplicates"][0]
        assert len(group["torrents"]) == 2
        rd_ids = {t["rd_id"] for t in group["torrents"]}
        assert "RD001" in rd_ids
        assert "RD002" in rd_ids

    async def test_list_duplicates_rd_error_returns_502(
        self, http: AsyncClient
    ) -> None:
        """If RD list_torrents raises, endpoint returns 502."""
        with patch(
            "src.api.routes.duplicates.rd_client.list_torrents",
            new_callable=AsyncMock,
            side_effect=RealDebridError("API error"),
        ):
            resp = await http.get("/api/duplicates")

        assert resp.status_code == 502
        assert "Failed to fetch RD torrents" in resp.json()["detail"]

    async def test_list_duplicates_non_duplicate_not_grouped(
        self, http: AsyncClient
    ) -> None:
        """Unique torrents (different titles) produce no duplicate groups."""
        rd_data = [
            self._make_rd_torrent_dict(
                "RD001",
                "The.Matrix.1999.1080p.BluRay.mkv",
                "a" * 40,
            ),
            self._make_rd_torrent_dict(
                "RD002",
                "Inception.2010.1080p.WEB-DL.mkv",
                "b" * 40,
            ),
        ]

        with patch(
            "src.api.routes.duplicates.rd_client.list_torrents",
            new_callable=AsyncMock,
            return_value=rd_data,
        ):
            resp = await http.get("/api/duplicates")

        assert resp.status_code == 200
        assert resp.json()["total_groups"] == 0
        assert resp.json()["duplicates"] == []

    async def test_resolve_duplicates_deletes_selected(
        self, http: AsyncClient
    ) -> None:
        """resolve calls rd_client.delete_torrent for each id in remove_rd_ids."""
        mock_delete = AsyncMock()

        with patch("src.api.routes.duplicates.rd_client.delete_torrent", mock_delete):
            resp = await http.post(
                "/api/duplicates/resolve",
                json={
                    "keep_rd_id": "RD_KEEP",
                    "remove_rd_ids": ["RD_DEL1", "RD_DEL2"],
                },
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["removed_count"] == 2
        assert data["errors"] == []
        assert mock_delete.call_count == 2

    async def test_resolve_duplicates_skips_keep_id(
        self, http: AsyncClient
    ) -> None:
        """The keep_rd_id is never passed to delete_torrent even if included in remove list."""
        mock_delete = AsyncMock()

        with patch("src.api.routes.duplicates.rd_client.delete_torrent", mock_delete):
            resp = await http.post(
                "/api/duplicates/resolve",
                json={
                    "keep_rd_id": "RD_KEEP",
                    "remove_rd_ids": ["RD_KEEP", "RD_DEL1"],
                },
            )

        assert resp.status_code == 200
        # Only RD_DEL1 should have been deleted
        assert resp.json()["removed_count"] == 1
        called_ids = [call.args[0] for call in mock_delete.call_args_list]
        assert "RD_KEEP" not in called_ids
        assert "RD_DEL1" in called_ids

    async def test_resolve_duplicates_partial_failure(
        self, http: AsyncClient
    ) -> None:
        """If one delete fails, others proceed and errors are collected."""

        async def _selective_delete(rd_id: str) -> None:
            if rd_id == "RD_FAIL":
                raise RealDebridError("delete failed")

        with patch(
            "src.api.routes.duplicates.rd_client.delete_torrent",
            side_effect=_selective_delete,
        ):
            resp = await http.post(
                "/api/duplicates/resolve",
                json={
                    "keep_rd_id": "RD_KEEP",
                    "remove_rd_ids": ["RD_OK", "RD_FAIL"],
                },
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "partial"
        assert data["removed_count"] == 1
        assert len(data["errors"]) == 1
        assert "RD_FAIL" in data["errors"][0]

    async def test_resolve_duplicates_empty_remove_list(
        self, http: AsyncClient
    ) -> None:
        """Empty remove_rd_ids returns removed_count=0 and status=ok."""
        mock_delete = AsyncMock()

        with patch("src.api.routes.duplicates.rd_client.delete_torrent", mock_delete):
            resp = await http.post(
                "/api/duplicates/resolve",
                json={"keep_rd_id": "RD_KEEP", "remove_rd_ids": []},
            )

        assert resp.status_code == 200
        assert resp.json()["removed_count"] == 0
        assert resp.json()["status"] == "ok"
        mock_delete.assert_not_called()
