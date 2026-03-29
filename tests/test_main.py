"""Tests for helpers defined in src/main.py.

Covers:
  Section A — _find_torrent_for_item (4 tests)
    - Direct media_item_id lookup succeeds → returns the torrent
    - Direct lookup fails, fallback via scrape_log info_hash → returns torrent
    - No torrent and no scrape_log → returns None
    - Scrape_log exists but info_hash not present in rd_torrents → returns None

All tests use the in-memory SQLite session fixture from conftest.py.
No scheduler, no HTTP calls, no FastAPI app startup is required.
asyncio_mode = "auto" (set in pyproject.toml), so no @pytest.mark.asyncio needed.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession

from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.scrape_result import ScrapeLog
from src.models.torrent import RdTorrent, TorrentStatus

# ---------------------------------------------------------------------------
# Lazy import helper — avoids triggering FastAPI app init at collection time
# ---------------------------------------------------------------------------


def _import_find_torrent():
    """Import _find_torrent_for_item without triggering app startup side-effects."""
    from src.core.queue_processor import _find_torrent_for_item  # noqa: PLC0415

    return _find_torrent_for_item


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


async def _make_item(session: AsyncSession, *, imdb_id: str = "tt1111111") -> MediaItem:
    """Create and flush a minimal MediaItem in CHECKING state."""
    item = MediaItem(
        imdb_id=imdb_id,
        title="Find Torrent Test Item",
        year=2024,
        media_type=MediaType.MOVIE,
        state=QueueState.CHECKING,
        state_changed_at=datetime.now(UTC),
        retry_count=0,
    )
    session.add(item)
    await session.flush()
    return item


async def _make_torrent(
    session: AsyncSession,
    *,
    rd_id: str,
    info_hash: str,
    media_item_id: int | None,
    status: TorrentStatus = TorrentStatus.ACTIVE,
) -> RdTorrent:
    """Create and flush an RdTorrent row."""
    torrent = RdTorrent(
        rd_id=rd_id,
        info_hash=info_hash.lower(),
        media_item_id=media_item_id,
        filename=f"{rd_id}.mkv",
        filesize=2 * 1024**3,
        status=status,
    )
    session.add(torrent)
    await session.flush()
    return torrent


async def _make_scrape_log(
    session: AsyncSession,
    *,
    media_item_id: int,
    info_hash: str,
) -> ScrapeLog:
    """Create and flush a pipeline ScrapeLog with the given info_hash in selected_result."""
    log = ScrapeLog(
        media_item_id=media_item_id,
        scraper="pipeline",
        query_params=json.dumps({"title": "Find Torrent Test Item", "step": "add_rd"}),
        results_count=1,
        results_summary=json.dumps([]),
        selected_result=json.dumps({"info_hash": info_hash, "rd_id": "RD_LOG"}),
        duration_ms=500,
    )
    session.add(log)
    await session.flush()
    return log


# ---------------------------------------------------------------------------
# Section A: _find_torrent_for_item
# ---------------------------------------------------------------------------


class TestFindTorrentForItem:
    """Integration tests for the _find_torrent_for_item helper in src/main.py.

    Uses the real in-memory SQLite session so DB queries execute normally.
    """

    # ------------------------------------------------------------------
    # Direct lookup path
    # ------------------------------------------------------------------

    async def test_direct_lookup_returns_torrent(self, session: AsyncSession) -> None:
        """RdTorrent with matching media_item_id exists → returned directly."""
        _find_torrent_for_item = _import_find_torrent()

        item = await _make_item(session)
        torrent = await _make_torrent(
            session,
            rd_id="RD_DIRECT",
            info_hash="a" * 40,
            media_item_id=item.id,
        )

        result = await _find_torrent_for_item(session, item)

        assert result is not None
        assert result.rd_id == "RD_DIRECT"
        assert result.id == torrent.id

    async def test_direct_lookup_ignores_removed_torrents(self, session: AsyncSession) -> None:
        """REMOVED torrents with matching media_item_id are not returned by direct lookup.

        The direct lookup queries for ACTIVE status only, so a REMOVED torrent
        should not satisfy the direct lookup.  If there is also no pipeline
        scrape_log with a valid hash, None is returned.
        """
        _find_torrent_for_item = _import_find_torrent()

        item = await _make_item(session)
        await _make_torrent(
            session,
            rd_id="RD_REMOVED",
            info_hash="b" * 40,
            media_item_id=item.id,
            status=TorrentStatus.REMOVED,
        )

        result = await _find_torrent_for_item(session, item)

        assert result is None

    # ------------------------------------------------------------------
    # Fallback via scrape_log path
    # ------------------------------------------------------------------

    async def test_fallback_via_scrape_log_returns_torrent(
        self, session: AsyncSession
    ) -> None:
        """No direct match, but pipeline scrape_log has info_hash → torrent found via hash."""
        _find_torrent_for_item = _import_find_torrent()

        item = await _make_item(session)
        shared_hash = "c" * 40

        # The torrent belongs to a DIFFERENT item (e.g. season-pack shared by multiple items)
        other_item = await _make_item(session, imdb_id="tt2222222")
        torrent = await _make_torrent(
            session,
            rd_id="RD_FALLBACK",
            info_hash=shared_hash,
            media_item_id=other_item.id,
        )

        # Pipeline scrape_log for our item points at the shared hash
        await _make_scrape_log(session, media_item_id=item.id, info_hash=shared_hash)

        result = await _find_torrent_for_item(session, item)

        assert result is not None
        assert result.rd_id == "RD_FALLBACK"
        assert result.id == torrent.id

    async def test_fallback_uses_most_recent_scrape_log(
        self, session: AsyncSession
    ) -> None:
        """When multiple pipeline scrape_log entries exist, the newest one is used."""
        _find_torrent_for_item = _import_find_torrent()

        item = await _make_item(session)
        old_hash = "d" * 40
        new_hash = "e" * 40

        # Two torrents — only the one matching the newest log should be returned
        await _make_torrent(
            session,
            rd_id="RD_OLD",
            info_hash=old_hash,
            media_item_id=None,
        )
        torrent_new = await _make_torrent(
            session,
            rd_id="RD_NEW",
            info_hash=new_hash,
            media_item_id=None,
        )

        # Insert older log first, newer log second (higher auto-increment id)
        await _make_scrape_log(session, media_item_id=item.id, info_hash=old_hash)
        await _make_scrape_log(session, media_item_id=item.id, info_hash=new_hash)

        result = await _find_torrent_for_item(session, item)

        assert result is not None
        assert result.rd_id == "RD_NEW"
        assert result.id == torrent_new.id

    # ------------------------------------------------------------------
    # None cases
    # ------------------------------------------------------------------

    async def test_returns_none_when_no_torrent_and_no_scrape_log(
        self, session: AsyncSession
    ) -> None:
        """No RdTorrent and no ScrapeLog exist for the item → returns None."""
        _find_torrent_for_item = _import_find_torrent()

        item = await _make_item(session)

        result = await _find_torrent_for_item(session, item)

        assert result is None

    async def test_returns_none_when_scrape_log_hash_not_in_rd_torrents(
        self, session: AsyncSession
    ) -> None:
        """ScrapeLog references an info_hash that has no matching RdTorrent → None."""
        _find_torrent_for_item = _import_find_torrent()

        item = await _make_item(session)
        ghost_hash = "f" * 40

        # Log points at a hash that was never registered (e.g. torrent was deleted)
        await _make_scrape_log(session, media_item_id=item.id, info_hash=ghost_hash)

        result = await _find_torrent_for_item(session, item)

        assert result is None

    async def test_returns_none_when_scrape_log_selected_result_has_no_hash(
        self, session: AsyncSession
    ) -> None:
        """ScrapeLog.selected_result exists but contains no info_hash key → None."""
        _find_torrent_for_item = _import_find_torrent()

        item = await _make_item(session)

        # Log with a selected_result that has no info_hash field
        log = ScrapeLog(
            media_item_id=item.id,
            scraper="pipeline",
            query_params=None,
            results_count=0,
            selected_result=json.dumps({"action": "no_hash_here"}),
            duration_ms=100,
        )
        session.add(log)
        await session.flush()

        result = await _find_torrent_for_item(session, item)

        assert result is None

    async def test_returns_none_when_scrape_log_selected_result_is_malformed_json(
        self, session: AsyncSession
    ) -> None:
        """ScrapeLog.selected_result is not valid JSON → handled gracefully → None."""
        _find_torrent_for_item = _import_find_torrent()

        item = await _make_item(session)

        log = ScrapeLog(
            media_item_id=item.id,
            scraper="pipeline",
            query_params=None,
            results_count=0,
            selected_result="NOT VALID JSON {{{",
            duration_ms=100,
        )
        session.add(log)
        await session.flush()

        result = await _find_torrent_for_item(session, item)

        assert result is None

    async def test_fallback_ignores_non_pipeline_scrape_logs(
        self, session: AsyncSession
    ) -> None:
        """Only scrape_log rows with scraper='pipeline' are used in the fallback."""
        _find_torrent_for_item = _import_find_torrent()

        item = await _make_item(session)
        some_hash = "9" * 40

        # Torrent exists for the hash
        await _make_torrent(
            session,
            rd_id="RD_SHOULD_NOT_FIND",
            info_hash=some_hash,
            media_item_id=None,
        )

        # Log is from a different scraper (e.g. "zilean"), not "pipeline"
        log = ScrapeLog(
            media_item_id=item.id,
            scraper="zilean",
            query_params=None,
            results_count=1,
            selected_result=json.dumps({"info_hash": some_hash}),
            duration_ms=200,
        )
        session.add(log)
        await session.flush()

        result = await _find_torrent_for_item(session, item)

        # Should be None — the zilean log is not consulted by the fallback
        assert result is None
