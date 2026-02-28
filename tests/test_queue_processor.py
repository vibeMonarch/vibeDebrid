"""Tests for the _job_queue_processor scheduled job in src/main.py.

Covers the four stages introduced beyond the original WANTED→SCRAPING pipeline:

  Stage 2: ADDING → check RD torrent status → CHECKING when downloaded
  Stage 3: CHECKING → mount lookup → symlink creation → COMPLETE
  Stage 4: COMPLETE (older than 1 hour) → DONE

Group 1 — Stage 2: ADDING → CHECKING (4 tests)
Group 2 — Stage 3: CHECKING → COMPLETE (3 tests)
Group 3 — Stage 4: COMPLETE → DONE (2 tests)
Group 4 — Full integration: multiple states in one run (1 test)

All external singletons are mocked — no real network traffic is generated.
asyncio_mode = "auto" (set in pyproject.toml), so no @pytest.mark.asyncio needed.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.main import _job_queue_processor
from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.mount_index import MountIndex
from src.models.torrent import RdTorrent, TorrentStatus


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def _make_media_item(
    session: AsyncSession,
    *,
    state: QueueState,
    title: str = "Test Movie",
    imdb_id: str = "tt0000001",
    season: int | None = None,
    episode: int | None = None,
    state_changed_at: datetime | None = None,
) -> MediaItem:
    """Persist a MediaItem with the given state to the test session."""
    item = MediaItem(
        imdb_id=imdb_id,
        title=title,
        year=2024,
        media_type=MediaType.MOVIE if season is None else MediaType.SHOW,
        season=season,
        episode=episode,
        state=state,
        state_changed_at=state_changed_at or _utcnow(),
        retry_count=0,
    )
    session.add(item)
    await session.flush()
    return item


async def _make_rd_torrent(
    session: AsyncSession,
    *,
    media_item_id: int,
    rd_id: str = "RD_ABC123",
    status: TorrentStatus = TorrentStatus.ACTIVE,
    info_hash: str | None = None,
) -> RdTorrent:
    """Persist an RdTorrent linked to the given media item."""
    torrent = RdTorrent(
        rd_id=rd_id,
        info_hash=info_hash,
        media_item_id=media_item_id,
        status=status,
    )
    session.add(torrent)
    await session.flush()
    return torrent


def _make_mount_match(filepath: str = "/mnt/zurg/movies/Test Movie (2024)/movie.mkv") -> MagicMock:
    """Return a mock MountIndex-like object with a filepath attribute."""
    match = MagicMock(spec=MountIndex)
    match.filepath = filepath
    return match


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_session(session: AsyncSession) -> AsyncSession:
    """Wrap the test session so _job_queue_processor can use it as its own.

    _job_queue_processor calls session.commit() and session.close() at the end.
    Replacing those with no-ops keeps the test session alive and usable after
    the job exits.
    """
    session.commit = AsyncMock()
    session.close = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def patch_async_session(mock_session: AsyncSession):
    """Patch src.main.async_session to return the test session as a context-less callable.

    The job calls ``session = async_session()`` and then uses the result as a
    plain AsyncSession (not a context manager), so we return the session directly.
    """
    with patch("src.main.async_session", return_value=mock_session):
        yield mock_session


@pytest.fixture
def patch_process_queue(patch_async_session):
    """Patch queue_manager.process_queue to return a no-op summary dict.

    This silences the timer-based Stage 0 transitions so tests can focus on
    the Stage 2/3/4 behaviour.
    """
    with patch(
        "src.main.queue_manager.process_queue",
        new_callable=AsyncMock,
        return_value={"unreleased_advanced": 0, "retries_triggered": 0},
    ) as mock:
        yield mock


@pytest.fixture
def patch_scrape_pipeline(patch_process_queue):
    """Patch scrape_pipeline.run to be a silent no-op.

    Also patches queue_manager.transition so Stage 1 (WANTED → SCRAPING) does
    not interfere with tests that only care about Stages 2-4.  Because patching
    transition would break the real transition calls used in Stages 2-4, we use
    a side_effect that wraps the real implementation.
    """
    with patch("src.main.scrape_pipeline.run", new_callable=AsyncMock) as mock:
        yield mock


# ---------------------------------------------------------------------------
# Convenience: a fixture that patches everything Stage 1 touches so tests for
# Stages 2-4 start with a clean slate.  Each test that needs it lists it as a
# dependency; those that want to test something specific override individually.
# ---------------------------------------------------------------------------


@pytest.fixture
def job_patches(patch_async_session, patch_process_queue, patch_scrape_pipeline):
    """Activate all the common patches for a Stage 2/3/4 test run."""
    return {
        "session": patch_async_session,
        "process_queue": patch_process_queue,
        "scrape_pipeline": patch_scrape_pipeline,
    }


# ---------------------------------------------------------------------------
# Group 1: Stage 2 — ADDING → CHECKING
# ---------------------------------------------------------------------------


class TestStage2AddingToChecking:
    """Stage 2: items in ADDING state are polled against Real-Debrid."""

    async def test_downloaded_torrent_transitions_to_checking(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """ADDING item whose RD torrent reports 'downloaded' transitions to CHECKING."""
        item = await _make_media_item(session, state=QueueState.ADDING)
        await _make_rd_torrent(session, media_item_id=item.id, rd_id="RD_DONE")

        with patch(
            "src.main.rd_client.get_torrent_info",
            new_callable=AsyncMock,
            return_value={"status": "downloaded", "id": "RD_DONE"},
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.CHECKING

    async def test_non_downloaded_torrent_stays_adding(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """ADDING item whose RD torrent reports a non-downloaded status stays ADDING."""
        item = await _make_media_item(session, state=QueueState.ADDING)
        await _make_rd_torrent(session, media_item_id=item.id, rd_id="RD_WAIT")

        with patch(
            "src.main.rd_client.get_torrent_info",
            new_callable=AsyncMock,
            return_value={"status": "downloading", "id": "RD_WAIT"},
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.ADDING

    async def test_adding_item_with_no_active_torrent_is_skipped(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """ADDING item that has no ACTIVE RdTorrent row is skipped (stays ADDING)."""
        item = await _make_media_item(session, state=QueueState.ADDING)
        # Deliberately create a REMOVED torrent — should not be matched
        await _make_rd_torrent(
            session,
            media_item_id=item.id,
            rd_id="RD_OLD",
            status=TorrentStatus.REMOVED,
        )

        with patch(
            "src.main.rd_client.get_torrent_info",
            new_callable=AsyncMock,
        ) as mock_rd:
            await _job_queue_processor()

        # rd_client should never have been called because no ACTIVE torrent exists
        mock_rd.assert_not_called()
        await session.refresh(item)
        assert item.state == QueueState.ADDING

    async def test_rd_api_failure_leaves_item_adding(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """If get_torrent_info raises, the item stays ADDING and the error is swallowed."""
        item = await _make_media_item(session, state=QueueState.ADDING)
        await _make_rd_torrent(session, media_item_id=item.id, rd_id="RD_ERR")

        with patch(
            "src.main.rd_client.get_torrent_info",
            new_callable=AsyncMock,
            side_effect=RuntimeError("RD API timeout"),
        ):
            # _job_queue_processor must not raise even when RD fails
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.ADDING


# ---------------------------------------------------------------------------
# Group 2: Stage 3 — CHECKING → COMPLETE
# ---------------------------------------------------------------------------


class TestStage3CheckingToComplete:
    """Stage 3: items in CHECKING state are verified via mount scanner."""

    async def test_checking_item_found_in_mount_transitions_to_complete(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """CHECKING item found in the mount gets a symlink and moves to COMPLETE."""
        item = await _make_media_item(session, state=QueueState.CHECKING)
        mount_match = _make_mount_match("/mnt/zurg/movies/Test Movie (2024)/movie.mkv")

        with (
            patch(
                "src.main.mount_scanner.lookup",
                new_callable=AsyncMock,
                return_value=[mount_match],
            ),
            patch(
                "src.main.symlink_manager.create_symlink",
                new_callable=AsyncMock,
            ) as mock_symlink,
        ):
            await _job_queue_processor()

        # Symlink should have been created with the first match's filepath
        mock_symlink.assert_awaited_once()
        call_args = mock_symlink.call_args
        assert call_args.args[2] == "/mnt/zurg/movies/Test Movie (2024)/movie.mkv"

        await session.refresh(item)
        assert item.state == QueueState.COMPLETE

    async def test_checking_item_not_in_mount_stays_checking(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """CHECKING item not yet in the mount stays CHECKING for the next cycle."""
        item = await _make_media_item(session, state=QueueState.CHECKING)

        with (
            patch(
                "src.main.mount_scanner.lookup",
                new_callable=AsyncMock,
                return_value=[],  # empty — file not mounted yet
            ),
            patch(
                "src.main.symlink_manager.create_symlink",
                new_callable=AsyncMock,
            ) as mock_symlink,
        ):
            await _job_queue_processor()

        mock_symlink.assert_not_awaited()
        await session.refresh(item)
        assert item.state == QueueState.CHECKING

    async def test_symlink_creation_failure_leaves_item_checking(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """If create_symlink raises, the item stays CHECKING and the error is swallowed."""
        item = await _make_media_item(session, state=QueueState.CHECKING)
        mount_match = _make_mount_match("/mnt/zurg/movies/Test Movie (2024)/movie.mkv")

        with (
            patch(
                "src.main.mount_scanner.lookup",
                new_callable=AsyncMock,
                return_value=[mount_match],
            ),
            patch(
                "src.main.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                side_effect=OSError("Permission denied"),
            ),
        ):
            # Must not propagate the OSError to the caller
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.CHECKING

    async def test_mount_lookup_uses_item_title_season_episode(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """mount_scanner.lookup is called with the item's title, season, and episode."""
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            title="Breaking Bad",
            season=3,
            episode=7,
            imdb_id="tt0903747",
        )

        with (
            patch(
                "src.main.mount_scanner.lookup",
                new_callable=AsyncMock,
                return_value=[],
            ) as mock_lookup,
            patch("src.main.symlink_manager.create_symlink", new_callable=AsyncMock),
        ):
            await _job_queue_processor()

        mock_lookup.assert_awaited_once()
        _, kwargs = mock_lookup.call_args
        assert kwargs["title"] == "Breaking Bad"
        assert kwargs["season"] == 3
        assert kwargs["episode"] == 7

    async def test_symlink_uses_first_match_when_multiple_results(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """When the mount returns multiple matches, only the first filepath is used."""
        item = await _make_media_item(session, state=QueueState.CHECKING)
        first_match = _make_mount_match("/mnt/zurg/movies/Test Movie/movie.2160p.mkv")
        second_match = _make_mount_match("/mnt/zurg/movies/Test Movie/movie.1080p.mkv")

        with (
            patch(
                "src.main.mount_scanner.lookup",
                new_callable=AsyncMock,
                return_value=[first_match, second_match],
            ),
            patch(
                "src.main.symlink_manager.create_symlink",
                new_callable=AsyncMock,
            ) as mock_symlink,
        ):
            await _job_queue_processor()

        call_args = mock_symlink.call_args
        assert call_args.args[2] == "/mnt/zurg/movies/Test Movie/movie.2160p.mkv"
        await session.refresh(item)
        assert item.state == QueueState.COMPLETE


# ---------------------------------------------------------------------------
# Group 3: Stage 4 — COMPLETE → DONE
# ---------------------------------------------------------------------------


class TestStage4CompleteToDone:
    """Stage 4: COMPLETE items older than 1 hour advance to DONE."""

    async def test_complete_item_older_than_one_hour_transitions_to_done(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """COMPLETE item with state_changed_at > 1 hour ago is transitioned to DONE."""
        two_hours_ago = _utcnow() - timedelta(hours=2)
        item = await _make_media_item(
            session,
            state=QueueState.COMPLETE,
            state_changed_at=two_hours_ago,
        )

        with (
            patch("src.main.mount_scanner.lookup", new_callable=AsyncMock, return_value=[]),
            patch("src.main.symlink_manager.create_symlink", new_callable=AsyncMock),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.DONE

    async def test_complete_item_less_than_one_hour_old_stays_complete(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """COMPLETE item with state_changed_at < 1 hour ago is left in COMPLETE."""
        five_minutes_ago = _utcnow() - timedelta(minutes=5)
        item = await _make_media_item(
            session,
            state=QueueState.COMPLETE,
            state_changed_at=five_minutes_ago,
        )

        with (
            patch("src.main.mount_scanner.lookup", new_callable=AsyncMock, return_value=[]),
            patch("src.main.symlink_manager.create_symlink", new_callable=AsyncMock),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.COMPLETE

    async def test_complete_item_exactly_at_boundary_transitions_to_done(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """COMPLETE item with state_changed_at exactly 1 hour ago satisfies the <= predicate."""
        # Subtract a tiny extra delta so the timestamp is reliably <= one_hour_ago
        just_over_one_hour_ago = _utcnow() - timedelta(hours=1, seconds=1)
        item = await _make_media_item(
            session,
            state=QueueState.COMPLETE,
            state_changed_at=just_over_one_hour_ago,
        )

        with (
            patch("src.main.mount_scanner.lookup", new_callable=AsyncMock, return_value=[]),
            patch("src.main.symlink_manager.create_symlink", new_callable=AsyncMock),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.DONE


# ---------------------------------------------------------------------------
# Group 4: Full integration — multiple states processed in one run
# ---------------------------------------------------------------------------


class TestFullIntegration:
    """All four stages run in sequence in a single _job_queue_processor call."""

    async def test_multiple_items_in_different_states_all_advance_correctly(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """All four stages run sequentially in one job cycle; items advance as far as they can.

        Important design note: Stages 2 and 3 share a session and run back-to-back.
        An item that transitions ADDING → CHECKING in Stage 2 will be picked up by
        Stage 3 in the same cycle when the mount lookup succeeds, advancing it all
        the way to COMPLETE in one job run.  This is intentional behaviour.

        Setup:
        - adding_item:    ADDING with a downloaded RD torrent + mount match
                          → expected COMPLETE (ADDING → CHECKING → COMPLETE in one run)
        - adding_no_mount: ADDING with a downloaded RD torrent but no mount match
                          → expected CHECKING (stops after Stage 2)
        - checking_item:  already CHECKING with a mount match → expected COMPLETE
        - old_complete:   COMPLETE, 2 h old                  → expected DONE
        - fresh_complete: COMPLETE, 5 min old                → stays COMPLETE
        - wanted_item:    WANTED                             → SCRAPING (Stage 1 transition runs;
                          only scrape_pipeline.run is mocked, not queue_manager.transition)
        """
        # ADDING item whose torrent is downloaded AND appears on the mount right away
        adding_item = await _make_media_item(
            session, state=QueueState.ADDING, title="Adding Movie", imdb_id="tt1111111"
        )
        await _make_rd_torrent(session, media_item_id=adding_item.id, rd_id="RD_READY")

        # ADDING item whose torrent is downloaded but NOT yet on the mount
        adding_no_mount = await _make_media_item(
            session, state=QueueState.ADDING, title="Adding No Mount", imdb_id="tt1111112"
        )
        await _make_rd_torrent(session, media_item_id=adding_no_mount.id, rd_id="RD_READY2")

        # CHECKING item that is present on the mount
        checking_item = await _make_media_item(
            session, state=QueueState.CHECKING, title="Checking Movie", imdb_id="tt2222222"
        )

        # COMPLETE item that is old enough to move to DONE
        old_complete_item = await _make_media_item(
            session,
            state=QueueState.COMPLETE,
            title="Old Complete Movie",
            imdb_id="tt3333333",
            state_changed_at=_utcnow() - timedelta(hours=3),
        )

        # COMPLETE item that is too recent to move to DONE
        fresh_complete_item = await _make_media_item(
            session,
            state=QueueState.COMPLETE,
            title="Fresh Complete Movie",
            imdb_id="tt4444444",
            state_changed_at=_utcnow() - timedelta(minutes=10),
        )

        # WANTED item — scrape_pipeline.run is mocked (no-op), but queue_manager.transition
        # is NOT mocked, so Stage 1 will still advance this item to SCRAPING.
        wanted_item = await _make_media_item(
            session, state=QueueState.WANTED, title="Wanted Movie", imdb_id="tt5555555"
        )

        mount_match = _make_mount_match("/mnt/zurg/movies/match.mkv")

        async def _fake_lookup(sess, *, title, season, episode):
            # Only "Adding No Mount" returns empty; all others get a match
            if title == "Adding No Mount":
                return []
            return [mount_match]

        with (
            patch(
                "src.main.rd_client.get_torrent_info",
                new_callable=AsyncMock,
                return_value={"status": "downloaded"},
            ),
            patch(
                "src.main.mount_scanner.lookup",
                new_callable=AsyncMock,
                side_effect=_fake_lookup,
            ),
            patch(
                "src.main.symlink_manager.create_symlink",
                new_callable=AsyncMock,
            ),
        ):
            await _job_queue_processor()

        await session.refresh(adding_item)
        await session.refresh(adding_no_mount)
        await session.refresh(checking_item)
        await session.refresh(old_complete_item)
        await session.refresh(fresh_complete_item)
        await session.refresh(wanted_item)

        # ADDING + mount match: advances two stages in one run (ADDING → CHECKING → COMPLETE)
        assert adding_item.state == QueueState.COMPLETE, (
            "ADDING item with a mount match advances all the way to COMPLETE in one cycle"
        )
        # ADDING + no mount match: stops at CHECKING after Stage 2
        assert adding_no_mount.state == QueueState.CHECKING, (
            "ADDING item without a mount match stops at CHECKING"
        )
        assert checking_item.state == QueueState.COMPLETE, "CHECKING should advance to COMPLETE"
        assert old_complete_item.state == QueueState.DONE, "old COMPLETE should advance to DONE"
        assert fresh_complete_item.state == QueueState.COMPLETE, "fresh COMPLETE should stay"
        # Stage 1 calls the real queue_manager.transition (only scrape_pipeline.run is mocked),
        # so the WANTED item is advanced to SCRAPING by _job_queue_processor.
        assert wanted_item.state == QueueState.SCRAPING, "WANTED advances to SCRAPING via Stage 1"

    async def test_one_stage_failure_does_not_abort_subsequent_stages(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """An error in Stage 2 does not prevent Stage 3 or Stage 4 from running.

        The ADDING item's RD call fails; the CHECKING item should still get its
        symlink, and the old COMPLETE item should still advance to DONE.
        """
        adding_item = await _make_media_item(
            session, state=QueueState.ADDING, title="Broken Adding", imdb_id="tt6666661"
        )
        await _make_rd_torrent(session, media_item_id=adding_item.id, rd_id="RD_BROKEN")

        checking_item = await _make_media_item(
            session, state=QueueState.CHECKING, title="OK Checking", imdb_id="tt6666662"
        )

        old_complete_item = await _make_media_item(
            session,
            state=QueueState.COMPLETE,
            title="Old Complete",
            imdb_id="tt6666663",
            state_changed_at=_utcnow() - timedelta(hours=2),
        )

        mount_match = _make_mount_match("/mnt/zurg/OK Checking (2024)/movie.mkv")

        with (
            patch(
                "src.main.rd_client.get_torrent_info",
                new_callable=AsyncMock,
                side_effect=RuntimeError("RD down"),
            ),
            patch(
                "src.main.mount_scanner.lookup",
                new_callable=AsyncMock,
                return_value=[mount_match],
            ),
            patch("src.main.symlink_manager.create_symlink", new_callable=AsyncMock),
        ):
            await _job_queue_processor()

        await session.refresh(adding_item)
        await session.refresh(checking_item)
        await session.refresh(old_complete_item)

        assert adding_item.state == QueueState.ADDING, "failed Stage 2 item should stay ADDING"
        assert checking_item.state == QueueState.COMPLETE, "Stage 3 should still complete"
        assert old_complete_item.state == QueueState.DONE, "Stage 4 should still complete"

    async def test_empty_queue_completes_without_error(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """Running the job against an empty queue must succeed silently."""
        with (
            patch("src.main.rd_client.get_torrent_info", new_callable=AsyncMock),
            patch(
                "src.main.mount_scanner.lookup", new_callable=AsyncMock, return_value=[]
            ),
            patch("src.main.symlink_manager.create_symlink", new_callable=AsyncMock),
        ):
            await _job_queue_processor()  # must not raise

    async def test_session_commit_called_exactly_once_on_success(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """session.commit() is called exactly once at the end of a successful run."""
        mock_session = job_patches["session"]

        with (
            patch(
                "src.main.rd_client.get_torrent_info", new_callable=AsyncMock, return_value={}
            ),
            patch(
                "src.main.mount_scanner.lookup", new_callable=AsyncMock, return_value=[]
            ),
            patch("src.main.symlink_manager.create_symlink", new_callable=AsyncMock),
        ):
            await _job_queue_processor()

        mock_session.commit.assert_awaited_once()
        mock_session.close.assert_awaited_once()

    async def test_session_rollback_and_close_called_on_unexpected_error(
        self, session: AsyncSession
    ) -> None:
        """If an unexpected top-level exception occurs, rollback and close are both called.

        We force an error early by making queue_manager.process_queue raise so
        the outer except clause is triggered.
        """
        session.commit = AsyncMock()
        session.close = AsyncMock()
        session.rollback = AsyncMock()

        with (
            patch("src.main.async_session", return_value=session),
            patch(
                "src.main.queue_manager.process_queue",
                new_callable=AsyncMock,
                side_effect=RuntimeError("DB exploded"),
            ),
        ):
            await _job_queue_processor()  # must not propagate

        session.rollback.assert_awaited_once()
        session.close.assert_awaited_once()
        session.commit.assert_not_awaited()


# ---------------------------------------------------------------------------
# Group 5: Stage 2 edge cases
# ---------------------------------------------------------------------------


class TestStage2EdgeCases:
    """Additional edge cases for the ADDING → CHECKING stage."""

    async def test_adding_item_with_replaced_torrent_is_skipped(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """ADDING item with only a REPLACED (not ACTIVE) torrent is skipped."""
        item = await _make_media_item(session, state=QueueState.ADDING)
        await _make_rd_torrent(
            session,
            media_item_id=item.id,
            rd_id="RD_REPLACED",
            status=TorrentStatus.REPLACED,
        )

        with patch(
            "src.main.rd_client.get_torrent_info", new_callable=AsyncMock
        ) as mock_rd:
            await _job_queue_processor()

        mock_rd.assert_not_called()
        await session.refresh(item)
        assert item.state == QueueState.ADDING

    async def test_multiple_adding_items_each_polled_independently(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """Each ADDING item is polled independently; partial success is allowed."""
        ready_item = await _make_media_item(
            session, state=QueueState.ADDING, title="Ready", imdb_id="tt7777771"
        )
        await _make_rd_torrent(session, media_item_id=ready_item.id, rd_id="RD_READY")

        waiting_item = await _make_media_item(
            session, state=QueueState.ADDING, title="Waiting", imdb_id="tt7777772"
        )
        await _make_rd_torrent(session, media_item_id=waiting_item.id, rd_id="RD_WAIT")

        async def _fake_rd(rd_id: str) -> dict:
            if rd_id == "RD_READY":
                return {"status": "downloaded"}
            return {"status": "downloading"}

        with patch(
            "src.main.rd_client.get_torrent_info",
            new_callable=AsyncMock,
            side_effect=_fake_rd,
        ):
            await _job_queue_processor()

        await session.refresh(ready_item)
        await session.refresh(waiting_item)
        assert ready_item.state == QueueState.CHECKING
        assert waiting_item.state == QueueState.ADDING

    async def test_rd_response_missing_status_key_leaves_item_adding(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """If the RD response dict has no 'status' key, the item stays ADDING."""
        item = await _make_media_item(session, state=QueueState.ADDING)
        await _make_rd_torrent(session, media_item_id=item.id, rd_id="RD_WEIRD")

        with patch(
            "src.main.rd_client.get_torrent_info",
            new_callable=AsyncMock,
            return_value={},  # no 'status' key
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.ADDING


# ---------------------------------------------------------------------------
# Group 6: Stage 3 edge cases
# ---------------------------------------------------------------------------


class TestStage3EdgeCases:
    """Additional edge cases for the CHECKING → COMPLETE stage."""

    async def test_mount_scanner_error_leaves_item_checking(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """If mount_scanner.lookup raises, the item stays CHECKING."""
        item = await _make_media_item(session, state=QueueState.CHECKING)

        with (
            patch(
                "src.main.mount_scanner.lookup",
                new_callable=AsyncMock,
                side_effect=RuntimeError("Mount unavailable"),
            ),
            patch("src.main.symlink_manager.create_symlink", new_callable=AsyncMock),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.CHECKING

    async def test_multiple_checking_items_processed_independently(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """Each CHECKING item is processed independently; one missing does not affect others."""
        found_item = await _make_media_item(
            session, state=QueueState.CHECKING, title="Found Movie", imdb_id="tt8888881"
        )
        missing_item = await _make_media_item(
            session, state=QueueState.CHECKING, title="Missing Movie", imdb_id="tt8888882"
        )

        mount_match = _make_mount_match("/mnt/zurg/Found Movie (2024)/found.mkv")

        async def _fake_lookup(session, *, title, season, episode):
            if title == "Found Movie":
                return [mount_match]
            return []

        with (
            patch("src.main.mount_scanner.lookup", new_callable=AsyncMock, side_effect=_fake_lookup),
            patch("src.main.symlink_manager.create_symlink", new_callable=AsyncMock),
        ):
            await _job_queue_processor()

        await session.refresh(found_item)
        await session.refresh(missing_item)
        assert found_item.state == QueueState.COMPLETE
        assert missing_item.state == QueueState.CHECKING
