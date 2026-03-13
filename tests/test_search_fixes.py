"""Tests for issue #16 fixes:
  - check_cached returns 429 on RD rate limit (not 500)
  - check_cached returns 502 on RD connection failure (not 500)
  - Items added via search have state_changed_at set via queue_manager
  - Concurrent list mutation in bulk_remove is eliminated
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.deps import get_db
from src.main import app
from src.models.media_item import MediaItem, MediaType, QueueState
from src.services.real_debrid import (
    CacheCheckResult,
    RealDebridError,
    RealDebridRateLimitError,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def override_db(session: AsyncSession):
    """Wire the test session into the FastAPI app's get_db dependency."""

    async def _override() -> AsyncSession:
        yield session

    app.dependency_overrides[get_db] = _override
    yield
    app.dependency_overrides.clear()


@pytest.fixture
async def http(override_db) -> AsyncClient:
    """Async HTTP client with the DB session wired."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


# ---------------------------------------------------------------------------
# check_cached error handling
# ---------------------------------------------------------------------------


class TestCheckCachedErrorHandling:
    """Verify check_cached returns proper HTTP status codes on RD failures."""

    async def test_check_cached_returns_429_on_rate_limit(
        self, http: AsyncClient
    ) -> None:
        """When RD raises RealDebridRateLimitError, endpoint returns 429 not 500."""
        with patch(
            "src.api.routes.search.rd_client.check_cached",
            new_callable=AsyncMock,
            side_effect=RealDebridRateLimitError("Too Many Requests", status_code=429),
        ):
            resp = await http.post(
                "/api/check-cached",
                json={"info_hash": "a" * 40},
            )

        assert resp.status_code == 429
        data = resp.json()
        assert data["info_hash"] == "a" * 40
        assert data["cached"] is None
        assert data["error"] == "rate_limited"

    async def test_check_cached_returns_502_on_rd_error(
        self, http: AsyncClient
    ) -> None:
        """When RD raises RealDebridError, endpoint returns 502 not 500."""
        with patch(
            "src.api.routes.search.rd_client.check_cached",
            new_callable=AsyncMock,
            side_effect=RealDebridError("Internal Server Error", status_code=500),
        ):
            resp = await http.post(
                "/api/check-cached",
                json={"info_hash": "b" * 40},
            )

        assert resp.status_code == 502
        data = resp.json()
        assert data["info_hash"] == "b" * 40
        assert data["cached"] is None
        assert data["error"] == "rd_unavailable"

    async def test_check_cached_returns_502_on_httpx_request_error(
        self, http: AsyncClient
    ) -> None:
        """When a network error occurs (httpx.RequestError), endpoint returns 502."""
        with patch(
            "src.api.routes.search.rd_client.check_cached",
            new_callable=AsyncMock,
            side_effect=httpx.ConnectError("Connection refused"),
        ):
            resp = await http.post(
                "/api/check-cached",
                json={"info_hash": "c" * 40},
            )

        assert resp.status_code == 502
        data = resp.json()
        assert data["info_hash"] == "c" * 40
        assert data["cached"] is None
        assert data["error"] == "rd_unavailable"

    async def test_check_cached_returns_502_on_httpx_timeout(
        self, http: AsyncClient
    ) -> None:
        """When a timeout occurs (httpx.TimeoutException), endpoint returns 502."""
        with patch(
            "src.api.routes.search.rd_client.check_cached",
            new_callable=AsyncMock,
            side_effect=httpx.TimeoutException("Read timeout"),
        ):
            resp = await http.post(
                "/api/check-cached",
                json={"info_hash": "d" * 40},
            )

        assert resp.status_code == 502
        data = resp.json()
        assert data["error"] == "rd_unavailable"

    async def test_check_cached_rate_limit_does_not_propagate_to_500(
        self, http: AsyncClient
    ) -> None:
        """Regression: RealDebridRateLimitError must never produce a 500 response."""
        with patch(
            "src.api.routes.search.rd_client.check_cached",
            new_callable=AsyncMock,
            side_effect=RealDebridRateLimitError("rate limit"),
        ):
            resp = await http.post(
                "/api/check-cached",
                json={"info_hash": "e" * 40},
            )

        assert resp.status_code != 500

    async def test_check_cached_invalid_hash_returns_false_without_rd_call(
        self, http: AsyncClient
    ) -> None:
        """A malformed hash returns cached=False immediately without calling RD."""
        mock_rd = AsyncMock()
        with patch("src.api.routes.search.rd_client.check_cached", mock_rd):
            resp = await http.post(
                "/api/check-cached",
                json={"info_hash": "not-a-valid-hash"},
            )

        assert resp.status_code == 200
        assert resp.json()["cached"] is False
        mock_rd.assert_not_called()


# ---------------------------------------------------------------------------
# add_torrent: state_changed_at via queue_manager.transition
# ---------------------------------------------------------------------------


class TestAddTorrentStateTransition:
    """Verify that add_torrent uses queue_manager so state_changed_at is set."""

    async def test_add_sets_state_changed_at_on_checking(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Items transitioned to CHECKING via queue_manager have state_changed_at set."""
        with (
            patch(
                "src.api.routes.search.rd_client.add_magnet",
                new_callable=AsyncMock,
                return_value={"id": "RDFIX01", "uri": "https://rd/RDFIX01"},
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
                    "title": "State Changed Movie",
                    "imdb_id": "tt9990001",
                    "media_type": "movie",
                    "year": 2024,
                },
            )

        assert resp.status_code == 200
        item_id = resp.json()["item_id"]

        # Confirm state via detail endpoint
        detail = await http.get(f"/api/queue/{item_id}")
        assert detail.status_code == 200
        item_data = detail.json()["item"]
        assert item_data["state"] == "checking"
        # state_changed_at must be set (queue_manager.transition always writes it)
        assert item_data["state_changed_at"] is not None

    async def test_add_sse_event_published_on_checking_transition(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """SSE event is published when transition() is used (not direct assignment)."""
        published_events: list = []

        from src.core import event_bus as _event_bus_mod

        original_publish = _event_bus_mod.event_bus.publish

        def _capture(event):
            published_events.append(event)
            return original_publish(event)

        with (
            patch(
                "src.api.routes.search.rd_client.add_magnet",
                new_callable=AsyncMock,
                return_value={"id": "RDFIX02", "uri": "https://rd/RDFIX02"},
            ),
            patch(
                "src.api.routes.search.rd_client.select_files",
                new_callable=AsyncMock,
            ),
            patch(
                "src.api.routes.search.dedup_engine.register_torrent",
                new_callable=AsyncMock,
            ),
            patch.object(_event_bus_mod.event_bus, "publish", side_effect=_capture),
        ):
            resp = await http.post(
                "/api/add",
                json={
                    "magnet_or_hash": "2" * 40,
                    "title": "SSE Event Movie",
                    "imdb_id": "tt9990002",
                    "media_type": "movie",
                    "year": 2024,
                },
            )

        assert resp.status_code == 200
        # At least one event should have been published for the CHECKING transition
        checking_events = [e for e in published_events if e.new_state == "checking"]
        assert len(checking_events) >= 1

    async def test_add_rd_failure_uses_force_transition_not_direct_assignment(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """When RD add fails the item lands in WANTED with state_changed_at set."""
        with patch(
            "src.api.routes.search.rd_client.add_magnet",
            new_callable=AsyncMock,
            side_effect=RealDebridError("quota exceeded"),
        ):
            resp = await http.post(
                "/api/add",
                json={
                    "magnet_or_hash": "3" * 40,
                    "title": "Failure Movie",
                    "imdb_id": "tt9990003",
                    "media_type": "movie",
                },
            )

        assert resp.status_code == 200
        assert resp.json()["status"] == "queued"
        item_id = resp.json()["item_id"]

        detail = await http.get(f"/api/queue/{item_id}")
        assert detail.status_code == 200
        item_data = detail.json()["item"]
        assert item_data["state"] == "wanted"
        assert item_data["state_changed_at"] is not None


# ---------------------------------------------------------------------------
# bulk_remove: concurrent gather result collection (no shared-state mutation)
# ---------------------------------------------------------------------------


class TestBulkRemoveConcurrency:
    """Verify bulk_remove collects failure info from gather() results, not append()."""

    async def test_bulk_remove_reports_failed_rd_deletions(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """When one RD deletion fails the error is captured in the response."""
        # Create two items in the DB
        item1 = MediaItem(
            imdb_id="tt8880001",
            title="Bulk Movie 1",
            year=2024,
            media_type=MediaType.MOVIE,
            state=QueueState.COMPLETE,
            state_changed_at=datetime.now(timezone.utc),
            retry_count=0,
        )
        item2 = MediaItem(
            imdb_id="tt8880002",
            title="Bulk Movie 2",
            year=2024,
            media_type=MediaType.MOVIE,
            state=QueueState.COMPLETE,
            state_changed_at=datetime.now(timezone.utc),
            retry_count=0,
        )
        session.add(item1)
        session.add(item2)
        await session.flush()

        from src.models.torrent import RdTorrent, TorrentStatus

        torrent1 = RdTorrent(
            rd_id="RD_BULK_001",
            info_hash="1" * 40,
            media_item_id=item1.id,
            filename="bulk1.mkv",
            status=TorrentStatus.ACTIVE,
        )
        torrent2 = RdTorrent(
            rd_id="RD_BULK_002",
            info_hash="2" * 40,
            media_item_id=item2.id,
            filename="bulk2.mkv",
            status=TorrentStatus.ACTIVE,
        )
        session.add(torrent1)
        session.add(torrent2)
        await session.flush()

        async def _fail_first(rd_id: str) -> None:
            if rd_id == "RD_BULK_001":
                raise RealDebridError("torrent not found")

        with (
            patch(
                "src.api.routes.queue.rd_client.delete_torrent",
                side_effect=_fail_first,
            ),
            patch(
                "src.api.routes.queue.symlink_manager.remove_symlink",
                new_callable=AsyncMock,
                return_value=0,
            ),
        ):
            resp = await http.post(
                "/api/queue/bulk/remove",
                json={"ids": [item1.id, item2.id]},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["processed"] == 2
        # The failed RD deletion should be reflected in errors
        assert data["status"] == "partial"
        assert any("RD torrent" in e for e in data["errors"])

    async def test_bulk_remove_all_rd_success_has_no_errors(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """When all RD deletions succeed the response has status=ok and no errors."""
        item = MediaItem(
            imdb_id="tt8880003",
            title="Bulk Movie 3",
            year=2024,
            media_type=MediaType.MOVIE,
            state=QueueState.COMPLETE,
            state_changed_at=datetime.now(timezone.utc),
            retry_count=0,
        )
        session.add(item)
        await session.flush()

        from src.models.torrent import RdTorrent, TorrentStatus

        torrent = RdTorrent(
            rd_id="RD_BULK_003",
            info_hash="3" * 40,
            media_item_id=item.id,
            filename="bulk3.mkv",
            status=TorrentStatus.ACTIVE,
        )
        session.add(torrent)
        await session.flush()

        with (
            patch(
                "src.api.routes.queue.rd_client.delete_torrent",
                new_callable=AsyncMock,
            ),
            patch(
                "src.api.routes.queue.symlink_manager.remove_symlink",
                new_callable=AsyncMock,
                return_value=0,
            ),
        ):
            resp = await http.post(
                "/api/queue/bulk/remove",
                json={"ids": [item.id]},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["processed"] == 1
        assert data["errors"] == []

    async def test_bulk_remove_concurrent_failures_all_captured(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """When multiple concurrent RD deletions fail all are captured correctly.

        This is the key regression test for the data race: the old code did
        rd_failed.append(rd_id) inside concurrent tasks, which is not safe
        when tasks can interleave. The new code uses gather() return values.
        """
        # Create 3 items whose RD torrents all fail to delete
        items = []
        for i, imdb in enumerate(["tt8881001", "tt8881002", "tt8881003"]):
            itm = MediaItem(
                imdb_id=imdb,
                title=f"Race Movie {i}",
                year=2024,
                media_type=MediaType.MOVIE,
                state=QueueState.COMPLETE,
                state_changed_at=datetime.now(timezone.utc),
                retry_count=0,
            )
            session.add(itm)
            await session.flush()
            items.append(itm)

        from src.models.torrent import RdTorrent, TorrentStatus

        rd_ids = [f"RD_RACE_00{i}" for i in range(3)]
        for idx, (itm, rd_id) in enumerate(zip(items, rd_ids)):
            t = RdTorrent(
                rd_id=rd_id,
                info_hash=str(idx) * 40,
                media_item_id=itm.id,
                filename=f"race{idx}.mkv",
                status=TorrentStatus.ACTIVE,
            )
            session.add(t)
        await session.flush()

        with (
            patch(
                "src.api.routes.queue.rd_client.delete_torrent",
                new_callable=AsyncMock,
                side_effect=RealDebridError("all fail"),
            ),
            patch(
                "src.api.routes.queue.symlink_manager.remove_symlink",
                new_callable=AsyncMock,
                return_value=0,
            ),
        ):
            resp = await http.post(
                "/api/queue/bulk/remove",
                json={"ids": [itm.id for itm in items]},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["processed"] == 3
        # All 3 RD failures must be captured in a single error message
        assert any("3" in e and "RD torrent" in e for e in data["errors"])
