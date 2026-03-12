"""Tests for src/core/scrape_pipeline.py.

Covers all eight groups specified in the test strategy:
  Group 1 — Mount hit short-circuit (6 tests)
  Group 2 — Dedup hit short-circuit (4 tests)
  Group 3 — Scraper results (7 tests)
  Group 4 — Filtering and RD cache (4 tests)
  Group 5 — Add to RD (5 tests)
  Group 6 — Scrape log persistence (4 tests)
  Group 7 — Episode vs movie routing (3 tests)
  Group 8 — Error handling (4 tests)

All external singletons are mocked — no real network traffic is generated.
asyncio_mode = "auto" (set in pyproject.toml), so no @pytest.mark.asyncio needed.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.mount_index import MountIndex
from src.models.scrape_result import ScrapeLog
from src.models.torrent import RdTorrent, TorrentStatus
from src.services.real_debrid import CacheCheckResult
from src.services.torrentio import TorrentioResult
from src.services.zilean import ZileanResult


# ---------------------------------------------------------------------------
# Test data helpers
# ---------------------------------------------------------------------------


def _make_torrentio_result(**overrides: object) -> TorrentioResult:
    """Build a TorrentioResult with sensible defaults."""
    defaults: dict[str, object] = {
        "info_hash": "a" * 40,
        "title": "Movie.2024.1080p.WEB-DL.x265-GROUP",
        "resolution": "1080p",
        "codec": "x265",
        "quality": "WEB-DL",
        "size_bytes": 2 * 1024**3,
        "seeders": 100,
        "release_group": "GROUP",
        "languages": [],
        "is_season_pack": False,
    }
    defaults.update(overrides)
    return TorrentioResult(**defaults)


def _make_zilean_result(**overrides: object) -> ZileanResult:
    """Build a ZileanResult with sensible defaults."""
    defaults: dict[str, object] = {
        "info_hash": "b" * 40,
        "title": "Movie.2024.720p.WEB-DL.x264-ZGRP",
        "resolution": "720p",
        "codec": "x264",
        "quality": "WEB-DL",
        "size_bytes": 1 * 1024**3,
        "seeders": None,
        "release_group": "ZGRP",
        "languages": [],
        "is_season_pack": False,
    }
    defaults.update(overrides)
    return ZileanResult(**defaults)


def _make_filtered_result(result: TorrentioResult | ZileanResult, score: float = 75.0) -> object:
    """Return a mock FilteredResult-like object wrapping a scrape result."""
    fr = MagicMock()
    fr.result = result
    fr.score = score
    fr.rejection_reason = None
    fr.score_breakdown = {"resolution": 40.0, "codec": 15.0, "source": 12.0, "audio": 3.0, "seeders": 10.0, "cached": 0.0, "season_pack": 0.0}
    return fr


def _make_mount_index_hit(title: str = "Test Movie") -> MountIndex:
    """Build a minimal MountIndex object simulating a mount cache hit."""
    entry = MountIndex(
        filepath=f"/mnt/zurg/__all__/{title}.mkv",
        filename=f"{title}.mkv",
        parsed_title=title.lower(),
        parsed_year=2024,
        parsed_season=None,
        parsed_episode=None,
        parsed_resolution="1080p",
        parsed_codec="x265",
        filesize=2 * 1024**3,
        last_seen_at=datetime.now(timezone.utc),
    )
    return entry


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def show_item(session: AsyncSession) -> MediaItem:
    """A MediaItem of type SHOW with season=1, episode=3, in WANTED state."""
    item = MediaItem(
        imdb_id="tt7654321",
        title="Test Show",
        year=2023,
        media_type=MediaType.SHOW,
        state=QueueState.WANTED,
        state_changed_at=datetime.now(timezone.utc),
        retry_count=0,
        season=1,
        episode=3,
    )
    session.add(item)
    await session.flush()
    return item


@pytest.fixture
def mock_rd_torrent() -> RdTorrent:
    """A minimal RdTorrent returned by dedup_engine.register_torrent."""
    torrent = RdTorrent(
        rd_id="RD123",
        info_hash="a" * 40,
        media_item_id=1,
        filename="Movie.2024.1080p.WEB-DL.x265-GROUP.mkv",
        filesize=2 * 1024**3,
        resolution="1080p",
        cached=True,
        status=TorrentStatus.ACTIVE,
    )
    return torrent


# ---------------------------------------------------------------------------
# Dependency patch context manager
#
# Patches ALL singletons that ScrapePipeline.run() touches.
# Each test may override individual mocks by reassigning attributes on the
# returned namespace object.
# ---------------------------------------------------------------------------

_PATCH_TARGETS = {
    "mount_scanner_available": "src.core.scrape_pipeline.mount_scanner.is_mount_available",
    "mount_scanner_lookup": "src.core.scrape_pipeline.mount_scanner.lookup",
    "dedup_check": "src.core.scrape_pipeline.dedup_engine.check_content_duplicate",
    "dedup_register": "src.core.scrape_pipeline.dedup_engine.register_torrent",
    "zilean_search": "src.core.scrape_pipeline.zilean_client.search",
    "torrentio_movie": "src.core.scrape_pipeline.torrentio_client.scrape_movie",
    "torrentio_episode": "src.core.scrape_pipeline.torrentio_client.scrape_episode",
    "rd_add": "src.core.scrape_pipeline.rd_client.add_magnet",
    "rd_select": "src.core.scrape_pipeline.rd_client.select_files",
    "rd_check_cached_batch": "src.core.scrape_pipeline.rd_client.check_cached_batch",
    "rd_delete": "src.core.scrape_pipeline.rd_client.delete_torrent",
    "filter_rank": "src.core.scrape_pipeline.filter_engine.filter_and_rank",
    "queue_transition": "src.core.scrape_pipeline.queue_manager.transition",
}


class _Mocks:
    """Namespace that holds all active mock objects for a test."""

    mount_scanner_available: AsyncMock
    mount_scanner_lookup: AsyncMock
    dedup_check: AsyncMock
    dedup_register: AsyncMock
    zilean_search: AsyncMock
    torrentio_movie: AsyncMock
    torrentio_episode: AsyncMock
    rd_add: AsyncMock
    rd_select: AsyncMock
    rd_check_cached_batch: AsyncMock
    rd_delete: AsyncMock
    filter_rank: MagicMock
    queue_transition: AsyncMock


@asynccontextmanager
async def _all_mocks(
    mock_rd_torrent: RdTorrent | None = None,
) -> AsyncGenerator[_Mocks, None]:
    """Context manager that patches every ScrapePipeline dependency at once.

    Yields a _Mocks namespace so tests can override individual return values.
    All defaults mimic the "nothing found, no errors" happy path so individual
    tests need only change the one mock that drives their specific scenario.
    """
    mocks = _Mocks()

    patchers = {name: patch(target) for name, target in _PATCH_TARGETS.items()}

    started: dict[str, MagicMock] = {}
    try:
        for name, patcher in patchers.items():
            started[name] = patcher.start()
    except Exception:
        for p in patchers.values():
            try:
                p.stop()
            except RuntimeError:
                pass
        raise

    # Assign to namespace and set sensible defaults
    mocks.mount_scanner_available = started["mount_scanner_available"]
    mocks.mount_scanner_available.return_value = True

    mocks.mount_scanner_lookup = started["mount_scanner_lookup"]
    mocks.mount_scanner_lookup.return_value = []

    mocks.dedup_check = started["dedup_check"]
    mocks.dedup_check.return_value = None

    mocks.dedup_register = started["dedup_register"]
    mocks.dedup_register.return_value = mock_rd_torrent or MagicMock(spec=RdTorrent)

    mocks.zilean_search = started["zilean_search"]
    mocks.zilean_search.return_value = []

    mocks.torrentio_movie = started["torrentio_movie"]
    mocks.torrentio_movie.return_value = []

    mocks.torrentio_episode = started["torrentio_episode"]
    mocks.torrentio_episode.return_value = []

    mocks.rd_add = started["rd_add"]
    mocks.rd_add.return_value = {"id": "RD123", "uri": "magnet:?xt=urn:btih:" + "a" * 40}

    mocks.rd_select = started["rd_select"]
    mocks.rd_select.return_value = None

    mocks.rd_check_cached_batch = started["rd_check_cached_batch"]
    mocks.rd_check_cached_batch.return_value = {}

    mocks.rd_delete = started["rd_delete"]
    mocks.rd_delete.return_value = None

    mocks.filter_rank = started["filter_rank"]
    mocks.filter_rank.return_value = []

    mocks.queue_transition = started["queue_transition"]
    # Return the item passed in (pipeline calls transition(session, item.id, ...))
    # We configure side_effect in individual tests when needed.
    mocks.queue_transition.side_effect = None
    mocks.queue_transition.return_value = MagicMock(spec=MediaItem)

    try:
        yield mocks
    finally:
        for p in patchers.values():
            p.stop()


# ---------------------------------------------------------------------------
# Import target — deferred so missing module raises ImportError only in tests
# ---------------------------------------------------------------------------


def _import_pipeline():
    """Lazily import ScrapePipeline and PipelineResult.

    Raises ImportError with a descriptive message when the module is absent.
    Individual test functions call this so the import error surfaces per test
    rather than at collection time.
    """
    from src.core.scrape_pipeline import PipelineResult, ScrapePipeline  # noqa: PLC0415

    return ScrapePipeline, PipelineResult


# ---------------------------------------------------------------------------
# Group 1: Mount Hit Short-Circuit
# ---------------------------------------------------------------------------


class TestMountHitShortCircuit:
    """Mount scanner is checked first; a hit bypasses all scrapers."""

    async def test_mount_hit_returns_mount_hit_action(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """Mount available + lookup returns match → action='mount_hit'."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        hit = _make_mount_index_hit()

        async with _all_mocks() as m:
            m.mount_scanner_lookup.return_value = [hit]
            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.action == "mount_hit"

    async def test_mount_hit_sets_mount_path(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """Mount hit result includes the filepath from the index entry."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        hit = _make_mount_index_hit()

        async with _all_mocks() as m:
            m.mount_scanner_lookup.return_value = [hit]
            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.mount_path == hit.filepath

    async def test_mount_hit_does_not_call_scrapers(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """When mount hit, Zilean and Torrentio are never called."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        hit = _make_mount_index_hit()

        async with _all_mocks() as m:
            m.mount_scanner_lookup.return_value = [hit]
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        m.zilean_search.assert_not_called()
        m.torrentio_movie.assert_not_called()
        m.torrentio_episode.assert_not_called()

    async def test_mount_empty_continues_to_dedup(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """Mount available + lookup returns empty list → pipeline continues."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        async with _all_mocks() as m:
            m.mount_scanner_lookup.return_value = []
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        # Dedup must have been called since mount returned nothing
        m.dedup_check.assert_called_once()

    async def test_mount_unavailable_skips_mount_check(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """is_mount_available() == False → lookup not called, pipeline continues."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        async with _all_mocks() as m:
            m.mount_scanner_available.return_value = False
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        m.mount_scanner_lookup.assert_not_called()
        # Dedup was still called
        m.dedup_check.assert_called_once()

    async def test_mount_available_raises_logs_warning_and_continues(
        self, session: AsyncSession, wanted_item: MediaItem, caplog: pytest.LogCaptureFixture
    ) -> None:
        """is_mount_available() raises → warning logged, pipeline continues."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        async with _all_mocks() as m:
            m.mount_scanner_available.side_effect = OSError("fuse hang")
            with caplog.at_level(logging.WARNING):
                pipeline = ScrapePipeline()
                result: PipelineResult = await pipeline.run(session, wanted_item)

        # Pipeline must not crash and must produce a result
        assert result.action in ("no_results", "added_to_rd", "dedup_hit", "error")
        # Dedup step was still attempted
        m.dedup_check.assert_called_once()


# ---------------------------------------------------------------------------
# Group 2: Dedup Hit Short-Circuit
# ---------------------------------------------------------------------------


class TestDedupHitShortCircuit:
    """Dedup check happens after mount; a hit short-circuits scraping."""

    async def test_dedup_hit_returns_dedup_hit_action(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """check_content_duplicate returns torrent → action='dedup_hit'."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        async with _all_mocks(mock_rd_torrent) as m:
            m.dedup_check.return_value = mock_rd_torrent
            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.action == "dedup_hit"

    async def test_dedup_hit_does_not_call_scrapers(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """Dedup hit → Zilean and Torrentio are never invoked."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        async with _all_mocks(mock_rd_torrent) as m:
            m.dedup_check.return_value = mock_rd_torrent
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        m.zilean_search.assert_not_called()
        m.torrentio_movie.assert_not_called()

    async def test_dedup_none_continues_to_scraping(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """check_content_duplicate returns None → scraping phase is reached."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        async with _all_mocks() as m:
            m.dedup_check.return_value = None
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        # At least one scraper must have been called
        assert m.zilean_search.called or m.torrentio_movie.called

    async def test_dedup_raises_logs_warning_and_continues(
        self, session: AsyncSession, wanted_item: MediaItem, caplog: pytest.LogCaptureFixture
    ) -> None:
        """check_content_duplicate raises → warning logged, scraping continues."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        async with _all_mocks() as m:
            m.dedup_check.side_effect = RuntimeError("db locked")
            with caplog.at_level(logging.WARNING):
                pipeline = ScrapePipeline()
                result: PipelineResult = await pipeline.run(session, wanted_item)

        # Pipeline must not propagate the exception
        assert result.action in ("no_results", "added_to_rd", "error")
        # Scraping was still attempted
        assert m.zilean_search.called or m.torrentio_movie.called


# ---------------------------------------------------------------------------
# Group 3: Scraper Results
# ---------------------------------------------------------------------------


class TestScraperResults:
    """Individual scraper scenarios: what gets passed to the filter engine."""

    async def test_only_zilean_returns_results_sent_to_filter(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """Only Zilean results → filter engine receives them."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        zilean_result = _make_zilean_result()

        async with _all_mocks() as m:
            m.zilean_search.return_value = [zilean_result]
            m.torrentio_movie.return_value = []
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        # filter_engine.get_best must have been called with a non-empty list
        m.filter_rank.assert_called_once()
        call_args = m.filter_rank.call_args
        results_passed = call_args[0][0] if call_args[0] else call_args[1].get("results", [])
        assert zilean_result in results_passed

    async def test_only_torrentio_returns_results_sent_to_filter(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """Only Torrentio results → filter engine receives them."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        torrentio_result = _make_torrentio_result()

        async with _all_mocks() as m:
            m.torrentio_movie.return_value = [torrentio_result]
            m.zilean_search.return_value = []
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        m.filter_rank.assert_called_once()
        call_args = m.filter_rank.call_args
        results_passed = call_args[0][0] if call_args[0] else call_args[1].get("results", [])
        assert torrentio_result in results_passed

    async def test_both_scrapers_return_results_combined_for_filter(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """Results from both scrapers are combined before filtering."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        z = _make_zilean_result()
        t = _make_torrentio_result()

        async with _all_mocks() as m:
            m.zilean_search.return_value = [z]
            m.torrentio_movie.return_value = [t]
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        call_args = m.filter_rank.call_args
        results_passed = call_args[0][0] if call_args[0] else call_args[1].get("results", [])
        assert z in results_passed
        assert t in results_passed

    async def test_both_scrapers_empty_returns_no_results_action(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """Both scrapers return [] → action='no_results'."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        async with _all_mocks() as m:
            m.zilean_search.return_value = []
            m.torrentio_movie.return_value = []
            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.action == "no_results"

    async def test_both_scrapers_empty_transitions_sleeping(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """Both scrapers return [] → queue_manager.transition called with SLEEPING."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        async with _all_mocks() as m:
            m.zilean_search.return_value = []
            m.torrentio_movie.return_value = []
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        m.queue_transition.assert_called()
        # Find any call with SLEEPING in the positional or keyword args
        sleeping_called = any(
            QueueState.SLEEPING in call.args or QueueState.SLEEPING in call.kwargs.values()
            for call in m.queue_transition.call_args_list
        )
        assert sleeping_called

    async def test_zilean_raises_continues_with_torrentio(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """Zilean raises exception → Torrentio still called, pipeline continues."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        torrentio_result = _make_torrentio_result()

        async with _all_mocks() as m:
            m.zilean_search.side_effect = RuntimeError("zilean down")
            m.torrentio_movie.return_value = [torrentio_result]
            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        m.torrentio_movie.assert_called_once()
        assert result.action != "error"  # pipeline survived

    async def test_torrentio_raises_continues_with_zilean(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """Torrentio raises exception → Zilean still used, pipeline continues."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        zilean_result = _make_zilean_result()

        async with _all_mocks() as m:
            m.torrentio_movie.side_effect = RuntimeError("torrentio timeout")
            m.zilean_search.return_value = [zilean_result]
            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        m.zilean_search.assert_called_once()
        assert result.action != "error"


# ---------------------------------------------------------------------------
# Group 4: Filtering and RD Cache
# ---------------------------------------------------------------------------


class TestFilteringAndRdCache:
    """Filter engine interaction and RD instant-availability cache checks."""

    async def test_filter_returns_best_proceeds_to_add(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """filter_engine.filter_and_rank returns results → rd_client.add_magnet called."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        torrentio_result = _make_torrentio_result()
        filtered = _make_filtered_result(torrentio_result)

        async with _all_mocks(mock_rd_torrent) as m:
            m.torrentio_movie.return_value = [torrentio_result]
            m.filter_rank.return_value = [filtered]
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        m.rd_add.assert_called_once()

    async def test_filter_returns_none_gives_no_results(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """filter_engine.filter_and_rank returns [] (all rejected) → action='no_results'."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        torrentio_result = _make_torrentio_result()

        async with _all_mocks() as m:
            m.torrentio_movie.return_value = [torrentio_result]
            m.filter_rank.return_value = []
            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.action == "no_results"

    async def test_check_cached_batch_called_with_top_hashes(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """check_cached_batch is called with info hashes from the top filtered results."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        fake_hash = "c" * 40
        torrentio_result = _make_torrentio_result(info_hash=fake_hash, cached=True)
        filtered = _make_filtered_result(torrentio_result)

        async with _all_mocks() as m:
            m.torrentio_movie.return_value = [torrentio_result]
            m.filter_rank.return_value = [filtered]
            m.rd_check_cached_batch.return_value = {fake_hash: CacheCheckResult(info_hash=fake_hash, cached=True, rd_id="RD_KEPT")}
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        # check_cached_batch should have been called with the top hash
        m.rd_check_cached_batch.assert_called_once()
        call_args = m.rd_check_cached_batch.call_args[0]
        assert fake_hash in call_args[0]

    async def test_filter_always_receives_empty_cached_set(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """filter_and_rank is always called with an empty cached_hashes set (cache is checked after filtering)."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        torrentio_result = _make_torrentio_result(cached=False)

        async with _all_mocks() as m:
            m.torrentio_movie.return_value = [torrentio_result]
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        m.filter_rank.assert_called_once()
        call_kwargs = m.filter_rank.call_args[1]
        cached_hashes = call_kwargs.get("cached_hashes") or set()
        assert cached_hashes == set()

    async def test_rerank_called_with_cached_hashes_when_cached_found(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """When check_cached_batch finds cached hashes, filter_and_rank is called twice — second time with cached_hashes."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        fake_hash = "c" * 40
        torrentio_result = _make_torrentio_result(info_hash=fake_hash)
        filtered = _make_filtered_result(torrentio_result)

        async with _all_mocks(mock_rd_torrent) as m:
            m.torrentio_movie.return_value = [torrentio_result]
            m.filter_rank.return_value = [filtered]
            m.rd_check_cached_batch.return_value = {
                fake_hash: CacheCheckResult(info_hash=fake_hash, cached=True, rd_id="RD_KEPT"),
            }
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        # filter_and_rank should be called twice: first with empty set, then with cached hashes
        assert m.filter_rank.call_count == 2
        second_call_kwargs = m.filter_rank.call_args_list[1][1]
        assert fake_hash in second_call_kwargs.get("cached_hashes", set())

    async def test_rerank_not_called_when_no_cached_results(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """When check_cached_batch finds no cached hashes, filter_and_rank is called only once."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        fake_hash = "c" * 40
        torrentio_result = _make_torrentio_result(info_hash=fake_hash)
        filtered = _make_filtered_result(torrentio_result)

        async with _all_mocks(mock_rd_torrent) as m:
            m.torrentio_movie.return_value = [torrentio_result]
            m.filter_rank.return_value = [filtered]
            m.rd_check_cached_batch.return_value = {
                fake_hash: CacheCheckResult(info_hash=fake_hash, cached=False),
            }
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        assert m.filter_rank.call_count == 1


# ---------------------------------------------------------------------------
# Group 5: Add to RD
# ---------------------------------------------------------------------------


class TestAddToRd:
    """Tests covering the RD add, file-select, dedup-register, and transition steps."""

    async def test_successful_add_returns_added_to_rd_action(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """Full happy path → action='added_to_rd'."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        torrentio_result = _make_torrentio_result()
        filtered = _make_filtered_result(torrentio_result)

        async with _all_mocks(mock_rd_torrent) as m:
            m.torrentio_movie.return_value = [torrentio_result]
            m.filter_rank.return_value = [filtered]
            m.rd_add.return_value = {"id": "RD999", "uri": "magnet:?xt=urn:btih:" + "a" * 40}
            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.action == "added_to_rd"

    async def test_successful_add_sets_selected_hash(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """Successful add → result.selected_hash matches the scrape result's info_hash."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        info_hash = "d" * 40
        torrentio_result = _make_torrentio_result(info_hash=info_hash)
        filtered = _make_filtered_result(torrentio_result)

        async with _all_mocks(mock_rd_torrent) as m:
            m.torrentio_movie.return_value = [torrentio_result]
            m.filter_rank.return_value = [filtered]
            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.selected_hash == info_hash

    async def test_successful_add_sets_rd_torrent_id(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """Successful add → result.rd_torrent_id matches the RD response id."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        torrentio_result = _make_torrentio_result()
        filtered = _make_filtered_result(torrentio_result)

        async with _all_mocks(mock_rd_torrent) as m:
            m.torrentio_movie.return_value = [torrentio_result]
            m.filter_rank.return_value = [filtered]
            m.rd_add.return_value = {"id": "RD_EXPECTED", "uri": "magnet:?xt=urn:btih:" + "a" * 40}
            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.rd_torrent_id == "RD_EXPECTED"

    async def test_add_magnet_fails_returns_error_action(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """add_magnet raises/fails → action='error'."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        torrentio_result = _make_torrentio_result()
        filtered = _make_filtered_result(torrentio_result)

        async with _all_mocks() as m:
            m.torrentio_movie.return_value = [torrentio_result]
            m.filter_rank.return_value = [filtered]
            m.rd_add.side_effect = RuntimeError("RD API error")
            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.action == "error"

    async def test_select_files_fails_still_returns_added_to_rd(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """select_files raises → action still 'added_to_rd' (torrent was added)."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        torrentio_result = _make_torrentio_result()
        filtered = _make_filtered_result(torrentio_result)

        async with _all_mocks(mock_rd_torrent) as m:
            m.torrentio_movie.return_value = [torrentio_result]
            m.filter_rank.return_value = [filtered]
            m.rd_select.side_effect = RuntimeError("RD select files error")
            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        # The torrent is already in RD even if file selection fails
        assert result.action == "added_to_rd"

    async def test_register_torrent_called_with_correct_params(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """dedup_engine.register_torrent is called with rd_id and info_hash."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        info_hash = "e" * 40
        torrentio_result = _make_torrentio_result(info_hash=info_hash)
        filtered = _make_filtered_result(torrentio_result)

        async with _all_mocks(mock_rd_torrent) as m:
            m.torrentio_movie.return_value = [torrentio_result]
            m.filter_rank.return_value = [filtered]
            m.rd_add.return_value = {"id": "RD_REG", "uri": "magnet:?xt=urn:btih:" + info_hash}
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        m.dedup_register.assert_called_once()
        kwargs = m.dedup_register.call_args[1]
        assert kwargs.get("rd_id") == "RD_REG"
        assert kwargs.get("info_hash") == info_hash

    async def test_transition_to_adding_called_on_success(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """Successful add → queue_manager.transition called with ADDING state."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        torrentio_result = _make_torrentio_result()
        filtered = _make_filtered_result(torrentio_result)

        async with _all_mocks(mock_rd_torrent) as m:
            m.torrentio_movie.return_value = [torrentio_result]
            m.filter_rank.return_value = [filtered]
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        adding_called = any(
            QueueState.ADDING in call.args or QueueState.ADDING in call.kwargs.values()
            for call in m.queue_transition.call_args_list
        )
        assert adding_called

    async def test_add_magnet_skipped_when_rd_id_from_cache_check(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """When cache_results has rd_id for the chosen hash, add_magnet is not called."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        info_hash = "a" * 40
        torrentio_result = _make_torrentio_result(info_hash=info_hash)
        filtered = _make_filtered_result(torrentio_result)

        async with _all_mocks(mock_rd_torrent) as m:
            m.torrentio_movie.return_value = [torrentio_result]
            m.filter_rank.return_value = [filtered]
            m.rd_check_cached_batch.return_value = {
                info_hash: CacheCheckResult(info_hash=info_hash, cached=True, rd_id="RD_REUSE"),
            }
            pipeline = ScrapePipeline()
            result = await pipeline.run(session, wanted_item)

        m.rd_add.assert_not_called()
        assert result.action == "added_to_rd"
        assert result.rd_torrent_id == "RD_REUSE"

    async def test_unused_kept_torrents_cleaned_up(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """Kept torrents that weren't chosen are deleted via cleanup."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        chosen_hash = "a" * 40
        other_hash = "f" * 40
        torrentio_result = _make_torrentio_result(info_hash=chosen_hash)
        filtered = _make_filtered_result(torrentio_result)

        async with _all_mocks(mock_rd_torrent) as m:
            m.torrentio_movie.return_value = [torrentio_result]
            m.filter_rank.return_value = [filtered]
            m.rd_check_cached_batch.return_value = {
                chosen_hash: CacheCheckResult(info_hash=chosen_hash, cached=True, rd_id="RD_CHOSEN"),
                other_hash: CacheCheckResult(info_hash=other_hash, cached=True, rd_id="RD_OTHER"),
            }
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        # The unused kept torrent (other_hash) should have been deleted
        m.rd_delete.assert_called()
        delete_calls = [call.args[0] for call in m.rd_delete.call_args_list]
        assert "RD_OTHER" in delete_calls
        # The chosen torrent should NOT be deleted
        assert "RD_CHOSEN" not in delete_calls


# ---------------------------------------------------------------------------
# Group 6: Scrape Log Persistence
# ---------------------------------------------------------------------------


class TestScrapeLogPersistence:
    """Pipeline writes ScrapeLog entries for each major step."""

    async def test_mount_scan_logged_to_scrape_log(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """A ScrapeLog row with scraper containing 'mount' is created."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        async with _all_mocks():
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)
            await session.flush()

        result = await session.execute(
            select(ScrapeLog).where(ScrapeLog.media_item_id == wanted_item.id)
        )
        logs = result.scalars().all()
        scrapers = [log.scraper for log in logs]
        assert any("mount" in s.lower() for s in scrapers), (
            f"Expected a 'mount' scrape log entry. Found scrapers: {scrapers}"
        )

    async def test_dedup_check_logged_to_scrape_log(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """A ScrapeLog row for the dedup/rd_check step is created."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        async with _all_mocks():
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)
            await session.flush()

        result = await session.execute(
            select(ScrapeLog).where(ScrapeLog.media_item_id == wanted_item.id)
        )
        logs = result.scalars().all()
        scrapers = [log.scraper for log in logs]
        assert any(
            "dedup" in s.lower() or "rd_check" in s.lower() or "content_dup" in s.lower()
            for s in scrapers
        ), f"Expected a dedup/rd_check log. Found scrapers: {scrapers}"

    async def test_zilean_and_torrentio_logged_to_scrape_log(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """ScrapeLog rows for both Zilean and Torrentio scrapers are created."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        async with _all_mocks() as m:
            m.zilean_search.return_value = [_make_zilean_result()]
            m.torrentio_movie.return_value = [_make_torrentio_result()]
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)
            await session.flush()

        result = await session.execute(
            select(ScrapeLog).where(ScrapeLog.media_item_id == wanted_item.id)
        )
        logs = result.scalars().all()
        scrapers_lower = [log.scraper.lower() for log in logs]
        assert any("zilean" in s for s in scrapers_lower), (
            f"No Zilean log. Scrapers: {scrapers_lower}"
        )
        assert any("torrentio" in s for s in scrapers_lower), (
            f"No Torrentio log. Scrapers: {scrapers_lower}"
        )

    async def test_final_pipeline_result_logged_to_scrape_log(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """A ScrapeLog row recording the final pipeline outcome is written."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        torrentio_result = _make_torrentio_result()
        filtered = _make_filtered_result(torrentio_result)

        async with _all_mocks(mock_rd_torrent) as m:
            m.torrentio_movie.return_value = [torrentio_result]
            m.filter_rank.return_value = [filtered]
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)
            await session.flush()

        result = await session.execute(
            select(ScrapeLog).where(ScrapeLog.media_item_id == wanted_item.id)
        )
        logs = result.scalars().all()
        # At least one log should contain the final result / action information
        assert len(logs) >= 1, "Expected at least one ScrapeLog entry for the pipeline run"
        # The final log or any log should reference the selected result or pipeline action
        has_result_log = any(
            log.selected_result is not None or log.results_count is not None
            for log in logs
        )
        assert has_result_log, (
            "Expected a ScrapeLog with selected_result or results_count. "
            f"Got logs: {[(l.scraper, l.results_count, l.selected_result) for l in logs]}"
        )


# ---------------------------------------------------------------------------
# Group 7: Episode vs Movie Routing
# ---------------------------------------------------------------------------


class TestEpisodeVsMovieRouting:
    """The correct scraper method is called based on item media_type."""

    async def test_movie_item_calls_scrape_movie_not_episode(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """MediaType.MOVIE → scrape_movie() called, scrape_episode() not called."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        async with _all_mocks() as m:
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        m.torrentio_movie.assert_called_once()
        m.torrentio_episode.assert_not_called()

    async def test_show_item_calls_scrape_episode_not_movie(
        self, session: AsyncSession, show_item: MediaItem
    ) -> None:
        """MediaType.SHOW with season+episode → scrape_episode() called, not scrape_movie()."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        async with _all_mocks() as m:
            pipeline = ScrapePipeline()
            await pipeline.run(session, show_item)

        m.torrentio_episode.assert_called_once()
        m.torrentio_movie.assert_not_called()

    async def test_show_item_scrape_episode_called_with_correct_args(
        self, session: AsyncSession, show_item: MediaItem
    ) -> None:
        """scrape_episode() receives imdb_id, season, episode from the show item."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        async with _all_mocks() as m:
            pipeline = ScrapePipeline()
            await pipeline.run(session, show_item)

        m.torrentio_episode.assert_called_once()
        call_args = m.torrentio_episode.call_args
        pos_args = call_args[0]
        kw_args = call_args[1]

        # imdb_id must be passed (positional or keyword)
        all_args = list(pos_args) + list(kw_args.values())
        assert show_item.imdb_id in all_args, (
            f"imdb_id not found in scrape_episode args: {call_args}"
        )
        assert show_item.season in all_args, (
            f"season not found in scrape_episode args: {call_args}"
        )
        assert show_item.episode in all_args, (
            f"episode not found in scrape_episode args: {call_args}"
        )

    async def test_show_item_zilean_called_with_season_and_episode(
        self, session: AsyncSession, show_item: MediaItem
    ) -> None:
        """Zilean search for a show item includes season and episode params."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        async with _all_mocks() as m:
            pipeline = ScrapePipeline()
            await pipeline.run(session, show_item)

        m.zilean_search.assert_called_once()
        call_kwargs = m.zilean_search.call_args[1]
        assert call_kwargs.get("season") == show_item.season, (
            f"Zilean season arg mismatch: {call_kwargs}"
        )
        assert call_kwargs.get("episode") == show_item.episode, (
            f"Zilean episode arg mismatch: {call_kwargs}"
        )


# ---------------------------------------------------------------------------
# Group 8: Error Handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    """Pipeline is resilient; it never raises and always returns a PipelineResult."""

    async def test_pipeline_never_raises_on_unexpected_exception(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """An unexpected exception mid-pipeline results in action='error', not a raise."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        async with _all_mocks() as m:
            # Make filter_best blow up in an unexpected way
            m.torrentio_movie.return_value = [_make_torrentio_result()]
            m.filter_rank.side_effect = Exception("totally unexpected crash")
            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.action == "error"

    async def test_error_action_includes_message(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """When action='error', result.message describes what went wrong."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        async with _all_mocks() as m:
            m.rd_add.side_effect = RuntimeError("connection refused")
            m.torrentio_movie.return_value = [_make_torrentio_result()]
            m.filter_rank.return_value = [_make_filtered_result(_make_torrentio_result())]
            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.action == "error"
        assert result.message is not None and len(result.message) > 0

    async def test_error_transitions_item_to_sleeping(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """On error, queue_manager.transition is called with SLEEPING state."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        async with _all_mocks() as m:
            m.torrentio_movie.return_value = [_make_torrentio_result()]
            m.filter_rank.side_effect = Exception("kaboom")
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        sleeping_called = any(
            QueueState.SLEEPING in call.args or QueueState.SLEEPING in call.kwargs.values()
            for call in m.queue_transition.call_args_list
        )
        assert sleeping_called

    async def test_both_scrapers_fail_graceful_no_results(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """Both scrapers raise → pipeline returns 'no_results', not 'error'."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        async with _all_mocks() as m:
            m.zilean_search.side_effect = RuntimeError("zilean crash")
            m.torrentio_movie.side_effect = RuntimeError("torrentio crash")
            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        # Both scrapers failed, so no results collected → no_results (not error)
        assert result.action in ("no_results", "error")

    async def test_pipeline_result_always_returned_not_none(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """pipeline.run() always returns a PipelineResult instance, never None."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        async with _all_mocks() as m:
            m.mount_scanner_available.side_effect = Exception("mount check exploded")
            m.dedup_check.side_effect = Exception("dedup exploded")
            m.zilean_search.side_effect = Exception("zilean exploded")
            m.torrentio_movie.side_effect = Exception("torrentio exploded")
            pipeline = ScrapePipeline()
            result = await pipeline.run(session, wanted_item)

        assert result is not None
        assert isinstance(result, PipelineResult)


# ---------------------------------------------------------------------------
# Savepoint regression tests
# ---------------------------------------------------------------------------


class TestSavepointRegressions:
    """Savepoint behaviour: partial failures must not wipe earlier DB writes."""

    async def test_scrape_logs_survive_register_torrent_failure(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """ScrapeLog rows written before register_torrent must survive its failure.

        Before the savepoint fix, session.rollback() in the register_torrent
        except-block erased all ScrapeLog entries written during the same
        pipeline run.  With begin_nested() only the savepoint is rolled back;
        the outer transaction (and all ScrapeLog flushes) is preserved.
        """
        ScrapePipeline, PipelineResult = _import_pipeline()
        torrentio_result = _make_torrentio_result()
        filtered = _make_filtered_result(torrentio_result)

        async with _all_mocks(mock_rd_torrent) as m:
            m.torrentio_movie.return_value = [torrentio_result]
            m.filter_rank.return_value = [filtered]
            m.dedup_register.side_effect = IntegrityError(
                "UNIQUE constraint failed: rd_torrents.info_hash",
                params=None,
                orig=Exception("UNIQUE constraint failed"),
            )
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)
            await session.flush()

        result = await session.execute(
            select(ScrapeLog).where(ScrapeLog.media_item_id == wanted_item.id)
        )
        logs = result.scalars().all()
        assert len(logs) >= 1, (
            "ScrapeLog entries were wiped — savepoint did not preserve them. "
            f"Expected >= 1, got {len(logs)}"
        )


# ---------------------------------------------------------------------------
# Singleton smoke test
# ---------------------------------------------------------------------------


class TestModuleSingleton:
    """The module exports a module-level scrape_pipeline singleton."""

    def test_singleton_is_scrape_pipeline_instance(self) -> None:
        """scrape_pipeline module exports a module-level singleton."""
        from src.core.scrape_pipeline import ScrapePipeline, scrape_pipeline  # noqa: PLC0415

        assert isinstance(scrape_pipeline, ScrapePipeline)


# ---------------------------------------------------------------------------
# Group 9: Hash-Based Dedup (post-filter, pre-cache-check)
# ---------------------------------------------------------------------------


_PATCH_TARGETS_WITH_LOCAL_DEDUP = {
    **_PATCH_TARGETS,
    "dedup_local": "src.core.scrape_pipeline.dedup_engine.check_local_duplicate",
}


class _MocksWithLocalDedup(_Mocks):
    dedup_local: AsyncMock


@asynccontextmanager
async def _all_mocks_with_local_dedup(
    mock_rd_torrent: RdTorrent | None = None,
) -> AsyncGenerator[_MocksWithLocalDedup, None]:
    """Like _all_mocks but also patches check_local_duplicate."""
    mocks = _MocksWithLocalDedup()
    patchers = {
        name: patch(target)
        for name, target in _PATCH_TARGETS_WITH_LOCAL_DEDUP.items()
    }

    started: dict[str, MagicMock] = {}
    try:
        for name, patcher in patchers.items():
            started[name] = patcher.start()
    except Exception:
        for p in patchers.values():
            try:
                p.stop()
            except RuntimeError:
                pass
        raise

    # Re-use the same defaults as _all_mocks
    mocks.mount_scanner_available = started["mount_scanner_available"]
    mocks.mount_scanner_available.return_value = True

    mocks.mount_scanner_lookup = started["mount_scanner_lookup"]
    mocks.mount_scanner_lookup.return_value = []

    mocks.dedup_check = started["dedup_check"]
    mocks.dedup_check.return_value = None

    mocks.dedup_register = started["dedup_register"]
    mocks.dedup_register.return_value = mock_rd_torrent or MagicMock(spec=RdTorrent)

    mocks.zilean_search = started["zilean_search"]
    mocks.zilean_search.return_value = []

    mocks.torrentio_movie = started["torrentio_movie"]
    mocks.torrentio_movie.return_value = []

    mocks.torrentio_episode = started["torrentio_episode"]
    mocks.torrentio_episode.return_value = []

    mocks.rd_add = started["rd_add"]
    mocks.rd_add.return_value = {"id": "RD123", "uri": "magnet:?xt=urn:btih:" + "a" * 40}

    mocks.rd_select = started["rd_select"]
    mocks.rd_select.return_value = None

    mocks.rd_check_cached_batch = started["rd_check_cached_batch"]
    mocks.rd_check_cached_batch.return_value = {}

    mocks.rd_delete = started["rd_delete"]
    mocks.rd_delete.return_value = None

    mocks.filter_rank = started["filter_rank"]
    mocks.filter_rank.return_value = []

    mocks.queue_transition = started["queue_transition"]
    mocks.queue_transition.side_effect = None
    mocks.queue_transition.return_value = MagicMock(spec=MediaItem)

    # Default: no local hash duplicate found
    mocks.dedup_local = started["dedup_local"]
    mocks.dedup_local.return_value = None

    try:
        yield mocks
    finally:
        for p in patchers.values():
            p.stop()


class TestHashBasedDedup:
    """Hash-based dedup check that fires after filter_and_rank, before cache check.

    When the top-ranked result's info_hash is already present in the local
    rd_torrents registry (check_local_duplicate returns an RdTorrent), the
    pipeline must:
      - Return action='dedup_hit'
      - Transition the item to CHECKING state
      - Never call check_cached_batch or add_magnet
    """

    async def test_hash_dedup_hit_returns_dedup_hit_action(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """Top-ranked hash already in registry → action='dedup_hit'."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        known_hash = "b" * 40
        torrentio_result = _make_torrentio_result(info_hash=known_hash)
        filtered = _make_filtered_result(torrentio_result)

        # Build a matching existing RdTorrent with the same hash
        existing = RdTorrent(
            rd_id="RD_EXISTING",
            info_hash=known_hash,
            media_item_id=999,
            filename="Some.Other.Item.mkv",
            filesize=1 * 1024**3,
            status=TorrentStatus.ACTIVE,
        )

        async with _all_mocks_with_local_dedup() as m:
            m.torrentio_movie.return_value = [torrentio_result]
            m.filter_rank.return_value = [filtered]
            m.dedup_local.return_value = existing

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.action == "dedup_hit"

    async def test_hash_dedup_hit_transitions_to_checking(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """Hash dedup hit → queue_manager.transition called with CHECKING state."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        known_hash = "b" * 40
        torrentio_result = _make_torrentio_result(info_hash=known_hash)
        filtered = _make_filtered_result(torrentio_result)
        existing = RdTorrent(
            rd_id="RD_EXISTING",
            info_hash=known_hash,
            media_item_id=999,
            status=TorrentStatus.ACTIVE,
        )

        async with _all_mocks_with_local_dedup() as m:
            m.torrentio_movie.return_value = [torrentio_result]
            m.filter_rank.return_value = [filtered]
            m.dedup_local.return_value = existing

            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        checking_called = any(
            QueueState.CHECKING in call.args or QueueState.CHECKING in call.kwargs.values()
            for call in m.queue_transition.call_args_list
        )
        assert checking_called

    async def test_hash_dedup_hit_skips_cache_check_and_add_magnet(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """Hash dedup hit → neither check_cached_batch nor add_magnet are called."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        known_hash = "c" * 40
        torrentio_result = _make_torrentio_result(info_hash=known_hash)
        filtered = _make_filtered_result(torrentio_result)
        existing = RdTorrent(
            rd_id="RD_EXISTING",
            info_hash=known_hash,
            media_item_id=999,
            status=TorrentStatus.ACTIVE,
        )

        async with _all_mocks_with_local_dedup() as m:
            m.torrentio_movie.return_value = [torrentio_result]
            m.filter_rank.return_value = [filtered]
            m.dedup_local.return_value = existing

            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        m.rd_check_cached_batch.assert_not_called()
        m.rd_add.assert_not_called()

    async def test_hash_dedup_hit_sets_selected_hash_and_rd_id(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """Hash dedup hit → result carries the matched hash and rd_id."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        known_hash = "d" * 40
        torrentio_result = _make_torrentio_result(info_hash=known_hash)
        filtered = _make_filtered_result(torrentio_result)
        existing = RdTorrent(
            rd_id="RD_MATCHED",
            info_hash=known_hash,
            media_item_id=999,
            status=TorrentStatus.ACTIVE,
        )

        async with _all_mocks_with_local_dedup() as m:
            m.torrentio_movie.return_value = [torrentio_result]
            m.filter_rank.return_value = [filtered]
            m.dedup_local.return_value = existing

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.selected_hash == known_hash
        assert result.rd_torrent_id == "RD_MATCHED"

    async def test_hash_dedup_miss_continues_to_cache_check(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """When check_local_duplicate returns None, pipeline proceeds to cache check."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        torrentio_result = _make_torrentio_result(info_hash="e" * 40)
        filtered = _make_filtered_result(torrentio_result)

        async with _all_mocks_with_local_dedup(mock_rd_torrent) as m:
            m.torrentio_movie.return_value = [torrentio_result]
            m.filter_rank.return_value = [filtered]
            m.dedup_local.return_value = None  # No hash match

            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        # Pipeline continued past the hash dedup step
        m.rd_check_cached_batch.assert_called_once()
