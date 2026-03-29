"""Tests for issue #17: race condition and performance fixes.

Covers:
  - Fix 1: _try_acquire uses atomic acquire_nowait pattern (no TOCTOU window)
  - Fix 2: bulk_remove commits DB before RD deletion (DB-first ordering)
  - Fix 3: verify_symlinks batches stat checks into one to_thread call
  - Fix 4: _derive_scene_seasons uses asyncio.gather for TMDB season fetches
  - Fix 5: mount_scanner uses rowcount instead of fetchall for stale row count
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.deps import get_db
from src.api.routes.tools import _migration_lock, _try_acquire
from src.config import settings
from src.core.show_manager import ShowManager
from src.core.symlink_manager import SymlinkManager
from src.main import app
from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.symlink import Symlink
from src.models.torrent import RdTorrent, TorrentStatus
from src.services.tmdb import TmdbEpisodeInfo, TmdbSeasonDetail, TmdbSeasonInfo

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def override_db(session: AsyncSession):
    """Override FastAPI get_db dependency with the test session."""

    async def _override() -> AsyncSession:
        yield session

    app.dependency_overrides[get_db] = _override
    yield
    app.dependency_overrides.clear()


@pytest.fixture
async def http(override_db) -> AsyncClient:
    """Async HTTP client backed by the FastAPI test app with DB override."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def _make_item(session: AsyncSession, *, imdb_id: str = "tt1111111") -> MediaItem:
    item = MediaItem(
        imdb_id=imdb_id,
        title="Test Item",
        year=2024,
        media_type=MediaType.MOVIE,
        state=QueueState.WANTED,
        state_changed_at=datetime.now(UTC),
        retry_count=0,
    )
    session.add(item)
    await session.flush()
    return item


async def _make_rd_torrent(
    session: AsyncSession, media_item_id: int, rd_id: str = "RD123"
) -> RdTorrent:
    torrent = RdTorrent(
        media_item_id=media_item_id,
        rd_id=rd_id,
        status=TorrentStatus.ACTIVE,
        filename="Test.Movie.mkv",
        filesize=1_000_000,
        cached=True,
    )
    session.add(torrent)
    await session.flush()
    return torrent


# ---------------------------------------------------------------------------
# Fix 1: _try_acquire — atomic lock acquisition (no TOCTOU)
# ---------------------------------------------------------------------------


class TestTryAcquire:
    """Tests for the _try_acquire context manager in tools.py."""

    async def test_acquire_succeeds_when_lock_free(self) -> None:
        """_try_acquire yields normally when the lock is not held."""
        lock = asyncio.Lock()
        entered = False
        async with _try_acquire(lock, "already running"):
            entered = True
        assert entered
        assert not lock.locked()

    async def test_raises_409_when_lock_held(self) -> None:
        """_try_acquire raises HTTP 409 when another coroutine holds the lock."""
        from fastapi import HTTPException

        lock = asyncio.Lock()
        await lock.acquire()  # simulate a concurrent operation holding the lock
        try:
            with pytest.raises(HTTPException) as exc_info:
                async with _try_acquire(lock, "busy"):
                    pass
            assert exc_info.value.status_code == 409
            assert "busy" in exc_info.value.detail
        finally:
            lock.release()

    async def test_lock_released_after_normal_exit(self) -> None:
        """Lock is released after a successful run so subsequent calls succeed."""
        lock = asyncio.Lock()
        async with _try_acquire(lock, "busy"):
            pass
        # Lock must be free now
        assert not lock.locked()
        # Second call must also succeed
        async with _try_acquire(lock, "busy"):
            pass

    async def test_lock_released_after_exception_in_body(self) -> None:
        """Lock is released even if the body raises an exception."""
        lock = asyncio.Lock()
        with pytest.raises(ValueError):
            async with _try_acquire(lock, "busy"):
                raise ValueError("body error")
        assert not lock.locked()

    async def test_concurrent_requests_second_gets_409(self) -> None:
        """When two coroutines race, the second gets HTTP 409."""
        from fastapi import HTTPException

        lock = asyncio.Lock()
        results: list[str] = []

        async def _task(task_id: str) -> None:
            try:
                async with _try_acquire(lock, "busy"):
                    await asyncio.sleep(0)  # yield so the second task can try
                    results.append(f"{task_id}:ok")
            except HTTPException:
                results.append(f"{task_id}:409")

        await asyncio.gather(_task("A"), _task("B"))
        # One must succeed and the other must get 409
        assert results.count("A:ok") + results.count("B:ok") == 1
        assert results.count("A:409") + results.count("B:409") == 1

    async def test_migration_execute_returns_409_when_lock_held(
        self, http: AsyncClient, tmp_path: Path
    ) -> None:
        """POST /api/tools/migration/execute returns 409 if already running."""
        movies_dir = tmp_path / "movies"
        shows_dir = tmp_path / "shows"
        movies_dir.mkdir()
        shows_dir.mkdir()

        # Hold the migration lock to simulate a concurrent run
        await _migration_lock.acquire()
        try:
            response = await http.post(
                "/api/tools/migration/execute",
                json={
                    "movies_path": str(movies_dir),
                    "shows_path": str(shows_dir),
                },
            )
        finally:
            _migration_lock.release()

        assert response.status_code == 409


# ---------------------------------------------------------------------------
# Fix 2: bulk_remove — DB commit before RD deletion
# ---------------------------------------------------------------------------


class TestBulkRemoveOrdering:
    """Tests that bulk_remove commits DB changes before deleting from RD."""

    async def test_db_committed_before_rd_delete_is_called(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """session.commit() is called before rd_client.delete_torrent."""
        item = await _make_item(session, imdb_id="tt7001001")
        await _make_rd_torrent(session, item.id, rd_id="RD_ORDER_TEST")

        call_order: list[str] = []

        original_commit = session.commit

        async def _tracked_commit(*args, **kwargs):
            call_order.append("commit")
            return await original_commit(*args, **kwargs)

        async def _tracked_delete(rd_id: str) -> None:
            call_order.append(f"rd_delete:{rd_id}")

        with (
            patch(
                "src.api.routes.queue.symlink_manager.remove_symlink",
                new_callable=AsyncMock,
                return_value=0,
            ),
            patch("src.api.routes.queue.rd_client.delete_torrent", side_effect=_tracked_delete),
        ):
            # Patch commit on the session object that the route uses via dependency
            session.commit = _tracked_commit  # type: ignore[method-assign]
            resp = await http.post(
                "/api/queue/bulk/remove", json={"ids": [item.id]}
            )

        assert resp.status_code == 200
        # commit must appear before any rd_delete
        commit_idx = call_order.index("commit")
        rd_idx = next(
            i for i, e in enumerate(call_order) if e.startswith("rd_delete:")
        )
        assert commit_idx < rd_idx, (
            f"Expected commit before rd_delete but got order: {call_order}"
        )

    async def test_rd_delete_failure_does_not_undo_db_removal(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """If RD deletion fails, the DB changes are already committed (item gone)."""
        item = await _make_item(session, imdb_id="tt7001002")
        await _make_rd_torrent(session, item.id, rd_id="RD_FAIL_TEST")

        with (
            patch(
                "src.api.routes.queue.symlink_manager.remove_symlink",
                new_callable=AsyncMock,
                return_value=0,
            ),
            patch(
                "src.api.routes.queue.rd_client.delete_torrent",
                side_effect=Exception("RD API down"),
            ),
        ):
            resp = await http.post(
                "/api/queue/bulk/remove", json={"ids": [item.id]}
            )

        assert resp.status_code == 200
        data = resp.json()
        # processed count is 1 — the DB work succeeded
        assert data["processed"] == 1
        # There is an error message about the RD failure
        assert any("RD torrent" in e for e in data["errors"])
        # The item is no longer in DB (commit happened before RD)
        from sqlalchemy import select as sa_select

        row = (
            await session.execute(
                sa_select(MediaItem).where(MediaItem.id == item.id)
            )
        ).scalar_one_or_none()
        assert row is None


# ---------------------------------------------------------------------------
# Fix 3: verify_symlinks — single batch thread dispatch
# ---------------------------------------------------------------------------


class TestVerifySymlinksBatch:
    """Tests that verify_symlinks uses a single batched asyncio.to_thread call."""

    @pytest.fixture(autouse=True)
    def _patch_zurg_mount(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        monkeypatch.setattr(settings.paths, "zurg_mount", str(tmp_path))

    async def _insert_symlink(
        self,
        session: AsyncSession,
        *,
        source_path: str,
        target_path: str,
        valid: bool = True,
    ) -> Symlink:
        link = Symlink(
            source_path=source_path,
            target_path=target_path,
            valid=valid,
        )
        session.add(link)
        await session.flush()
        return link

    async def test_single_to_thread_call_for_multiple_symlinks(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """With N symlinks, asyncio.to_thread is called exactly once for stat checks."""
        for i in range(5):
            source = tmp_path / f"real{i}.mkv"
            source.write_bytes(b"x")
            target = tmp_path / f"link{i}.mkv"
            os.symlink(str(source), str(target))
            await self._insert_symlink(
                session,
                source_path=str(source),
                target_path=str(target),
                valid=True,
            )

        manager = SymlinkManager()
        to_thread_calls: list = []

        original_to_thread = asyncio.to_thread

        async def _counting_to_thread(fn, *args, **kwargs):
            to_thread_calls.append(fn)
            return await original_to_thread(fn, *args, **kwargs)

        with patch("src.core.symlink_manager.asyncio.to_thread", side_effect=_counting_to_thread):
            result = await manager.verify_symlinks(session)

        assert result.valid_count == 5
        # The batch function (_check_batch) should be the only to_thread call
        # for stat work.  Filter by name to count only the batch check, not
        # the pre-flight mount check.
        batch_calls = [f for f in to_thread_calls if callable(f) and f.__name__ == "_check_batch"]
        assert len(batch_calls) == 1, (
            f"Expected 1 batch to_thread call, got {len(batch_calls)}: {to_thread_calls}"
        )

    async def test_empty_symlink_list_skips_to_thread(
        self, session: AsyncSession
    ) -> None:
        """With no valid symlinks, the batch to_thread call is skipped entirely."""
        manager = SymlinkManager()
        to_thread_calls: list = []

        original_to_thread = asyncio.to_thread

        async def _counting_to_thread(fn, *args, **kwargs):
            to_thread_calls.append(fn)
            return await original_to_thread(fn, *args, **kwargs)

        with patch("src.core.symlink_manager.asyncio.to_thread", side_effect=_counting_to_thread):
            result = await manager.verify_symlinks(session)

        assert result.total_checked == 0
        batch_calls = [f for f in to_thread_calls if callable(f) and f.__name__ == "_check_batch"]
        assert len(batch_calls) == 0

    async def test_batch_correctly_detects_broken_and_valid(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """Batch check correctly separates valid from broken symlinks."""
        # Good symlink
        source_good = tmp_path / "good.mkv"
        source_good.write_bytes(b"x")
        target_good = tmp_path / "link_good.mkv"
        os.symlink(str(source_good), str(target_good))
        await self._insert_symlink(
            session, source_path=str(source_good), target_path=str(target_good)
        )

        # Dangling symlink (source deleted)
        source_bad = tmp_path / "bad.mkv"
        source_bad.write_bytes(b"x")
        target_bad = tmp_path / "link_bad.mkv"
        os.symlink(str(source_bad), str(target_bad))
        source_bad.unlink()
        link_bad_row = await self._insert_symlink(
            session, source_path=str(source_bad), target_path=str(target_bad)
        )

        manager = SymlinkManager()
        result = await manager.verify_symlinks(session)

        assert result.valid_count == 1
        assert result.broken_count == 1
        await session.refresh(link_bad_row)
        assert link_bad_row.valid is False


# ---------------------------------------------------------------------------
# Fix 4: _derive_scene_seasons — asyncio.gather for TMDB season fetches
# ---------------------------------------------------------------------------


class TestDeriveSceneSeasonsGather:
    """Tests that _derive_scene_seasons fetches TMDB seasons concurrently."""

    TMDB_ID = 99001
    TVDB_ID = 88001

    def _make_season_detail(self, season_num: int, ep_count: int) -> TmdbSeasonDetail:
        return TmdbSeasonDetail(
            season_number=season_num,
            name=f"Season {season_num}",
            episodes=[
                TmdbEpisodeInfo(episode_number=i, air_date="2020-01-01")
                for i in range(1, ep_count + 1)
            ],
        )

    async def test_multiple_seasons_fetched_concurrently(
        self, session: AsyncSession
    ) -> None:
        """When N seasons exist, asyncio.gather is used (not sequential awaits)."""
        seasons = [
            TmdbSeasonInfo(season_number=i, name=f"S{i}", episode_count=5, air_date="2020-01-01")
            for i in range(1, 4)
        ]
        xem_map = {1: (2, 1)}  # non-empty so XEM path is entered

        details = {i: self._make_season_detail(i, 5) for i in range(1, 4)}

        gather_calls: list[int] = []

        async def _mock_get_season(tmdb_id: int, season_num: int) -> TmdbSeasonDetail:
            gather_calls.append(season_num)
            return details[season_num]

        sm = ShowManager()
        with (
            patch(
                "src.core.show_manager.xem_mapper.get_absolute_scene_map",
                new_callable=AsyncMock,
                return_value=xem_map,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                side_effect=_mock_get_season,
            ),
        ):
            result = await sm._derive_scene_seasons(
                session, self.TMDB_ID, self.TVDB_ID, seasons
            )

        assert result is not None
        # All three seasons must have been fetched
        assert sorted(gather_calls) == [1, 2, 3]

    async def test_gather_semaphore_limits_concurrency(
        self, session: AsyncSession
    ) -> None:
        """At most 5 TMDB season-detail requests run concurrently."""
        n_seasons = 8
        seasons = [
            TmdbSeasonInfo(
                season_number=i, name=f"S{i}", episode_count=2, air_date="2020-01-01"
            )
            for i in range(1, n_seasons + 1)
        ]
        xem_map = {1: (2, 1)}
        details = {i: self._make_season_detail(i, 2) for i in range(1, n_seasons + 1)}

        concurrent_count = 0
        max_concurrent = 0

        async def _mock_get_season(tmdb_id: int, season_num: int) -> TmdbSeasonDetail:
            nonlocal concurrent_count, max_concurrent
            concurrent_count += 1
            max_concurrent = max(max_concurrent, concurrent_count)
            await asyncio.sleep(0)  # yield so other coroutines can start
            concurrent_count -= 1
            return details[season_num]

        sm = ShowManager()
        with (
            patch(
                "src.core.show_manager.xem_mapper.get_absolute_scene_map",
                new_callable=AsyncMock,
                return_value=xem_map,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                side_effect=_mock_get_season,
            ),
        ):
            await sm._derive_scene_seasons(session, self.TMDB_ID, self.TVDB_ID, seasons)

        assert max_concurrent <= 5

    async def test_season_zero_excluded_from_concurrent_fetch(
        self, session: AsyncSession
    ) -> None:
        """Season 0 (Specials) is never passed to get_season_details."""
        seasons = [
            TmdbSeasonInfo(season_number=0, name="Specials", episode_count=3, air_date="2020-01-01"),
            TmdbSeasonInfo(season_number=1, name="S1", episode_count=4, air_date="2020-01-01"),
        ]
        xem_map = {1: (2, 1)}
        s1_detail = self._make_season_detail(1, 4)

        fetched_seasons: list[int] = []

        async def _mock_get_season(tmdb_id: int, season_num: int):
            fetched_seasons.append(season_num)
            return s1_detail

        sm = ShowManager()
        with (
            patch(
                "src.core.show_manager.xem_mapper.get_absolute_scene_map",
                new_callable=AsyncMock,
                return_value=xem_map,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                side_effect=_mock_get_season,
            ),
        ):
            await sm._derive_scene_seasons(session, self.TMDB_ID, self.TVDB_ID, seasons)

        assert 0 not in fetched_seasons
        assert 1 in fetched_seasons

    async def test_absolute_offset_correct_with_concurrent_fetch(
        self, session: AsyncSession
    ) -> None:
        """Episode absolute offset accumulates correctly even with concurrent fetches."""
        seasons = [
            TmdbSeasonInfo(season_number=1, name="S1", episode_count=3, air_date="2020-01-01"),
            TmdbSeasonInfo(season_number=2, name="S2", episode_count=3, air_date="2021-01-01"),
        ]
        # Map absolute episode 4 (S2E1) to scene S3E1
        xem_map = {4: (3, 1)}

        s1_detail = self._make_season_detail(1, 3)
        s2_detail = self._make_season_detail(2, 3)

        async def _mock_get_season(tmdb_id: int, season_num: int):
            return s1_detail if season_num == 1 else s2_detail

        sm = ShowManager()
        with (
            patch(
                "src.core.show_manager.xem_mapper.get_absolute_scene_map",
                new_callable=AsyncMock,
                return_value=xem_map,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                side_effect=_mock_get_season,
            ),
        ):
            result = await sm._derive_scene_seasons(session, self.TMDB_ID, self.TVDB_ID, seasons)

        assert result is not None
        # S2E1 is absolute 4 → scene S3E1
        scene_seasons = {g.scene_season for g in result}
        assert 3 in scene_seasons


# ---------------------------------------------------------------------------
# Fix 5: mount_scanner — rowcount instead of fetchall
# ---------------------------------------------------------------------------


class TestMountScannerRowcount:
    """Tests that mount_scanner uses rowcount not fetchall for stale entry count."""

    async def test_scan_returns_correct_files_removed_count(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """scan() files_removed reflects actual deleted row count via rowcount.

        Uses a real temp dir: scan it once to add entries, remove the files,
        then scan again — the second scan removes the now-stale entries.
        """
        import os as _os
        import tempfile as _tempfile

        from src.core.mount_scanner import MountScanner

        scanner = MountScanner()
        with _tempfile.TemporaryDirectory() as tmpdir:
            # Create two video files and scan them in.
            f1 = _os.path.join(tmpdir, "Movie.A.2020.mkv")
            f2 = _os.path.join(tmpdir, "Movie.B.2021.mkv")
            open(f1, "wb").write(b"fake")
            open(f2, "wb").write(b"fake")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                first = await scanner.scan(session)
                assert first.files_added == 2
                await session.flush()

                # Remove both video files — they are now stale.
                # Keep a non-video file so the directory is non-empty
                # (empty dir = is_mount_available returns False).
                _os.remove(f1)
                _os.remove(f2)
                open(_os.path.join(tmpdir, "placeholder.txt"), "w").close()

                second = await scanner.scan(session)

        # rowcount must reflect the actual number of deleted rows (2).
        assert second.files_removed == 2

    async def test_clear_index_returns_correct_count_via_rowcount(
        self, session: AsyncSession
    ) -> None:
        """clear_index() returns the correct deletion count using rowcount."""
        from src.core.mount_scanner import MountScanner
        from src.models.mount_index import MountIndex

        scanner = MountScanner()

        for i in range(3):
            entry = MountIndex(
                filepath=f"/mnt/zurg/clear{i}.mkv",
                filename=f"clear{i}.mkv",
                parsed_title=f"clear {i}",
                last_seen_at=datetime.now(UTC),
            )
            session.add(entry)
        await session.flush()

        count = await scanner.clear_index(session)
        assert count == 3
