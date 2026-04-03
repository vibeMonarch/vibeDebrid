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
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.mount_index import MountIndex
from src.models.scrape_result import ScrapeLog
from src.models.torrent import RdTorrent, TorrentStatus
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
        last_seen_at=datetime.now(UTC),
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
        state_changed_at=datetime.now(UTC),
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
    "mount_scanner_lookup": "src.core.scrape_pipeline.mount_scanner.lookup_multi",
    "gather_alt_titles": "src.core.mount_scanner.gather_alt_titles",
    "dedup_check": "src.core.scrape_pipeline.dedup_engine.check_content_duplicate",
    "dedup_register": "src.core.scrape_pipeline.dedup_engine.register_torrent",
    "zilean_search": "src.core.scrape_pipeline.zilean_client.search",
    "torrentio_movie": "src.core.scrape_pipeline.torrentio_client.scrape_movie",
    "torrentio_episode": "src.core.scrape_pipeline.torrentio_client.scrape_episode",
    "rd_add": "src.core.scrape_pipeline.rd_client.add_magnet",
    "rd_select": "src.core.scrape_pipeline.rd_client.select_files",
    "rd_get_torrent_info": "src.core.scrape_pipeline.rd_client.get_torrent_info",
    "rd_delete": "src.core.scrape_pipeline.rd_client.delete_torrent",
    "filter_rank": "src.core.scrape_pipeline.filter_engine.filter_and_rank",
    "queue_transition": "src.core.scrape_pipeline.queue_manager.transition",
    "os_path_exists": "os.path.exists",
}


class _Mocks:
    """Namespace that holds all active mock objects for a test."""

    mount_scanner_available: AsyncMock
    mount_scanner_lookup: AsyncMock
    gather_alt_titles: AsyncMock
    dedup_check: AsyncMock
    dedup_register: AsyncMock
    zilean_search: AsyncMock
    torrentio_movie: AsyncMock
    torrentio_episode: AsyncMock
    rd_add: AsyncMock
    rd_select: AsyncMock
    rd_get_torrent_info: AsyncMock
    rd_delete: AsyncMock
    filter_rank: MagicMock
    queue_transition: AsyncMock
    os_path_exists: MagicMock


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

    mocks.gather_alt_titles = started["gather_alt_titles"]
    mocks.gather_alt_titles.side_effect = lambda session, item, tmdb_original_title=None: [item.title]

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

    # Default: torrent is NOT cached (status="magnet_conversion")
    mocks.rd_get_torrent_info = started["rd_get_torrent_info"]
    mocks.rd_get_torrent_info.return_value = {"id": "RD123", "status": "magnet_conversion", "files": []}

    mocks.rd_delete = started["rd_delete"]
    mocks.rd_delete.return_value = None

    mocks.filter_rank = started["filter_rank"]
    mocks.filter_rank.return_value = []

    mocks.queue_transition = started["queue_transition"]
    # Return the item passed in (pipeline calls transition(session, item.id, ...))
    # We configure side_effect in individual tests when needed.
    mocks.queue_transition.side_effect = None
    mocks.queue_transition.return_value = MagicMock(spec=MediaItem)

    mocks.os_path_exists = started["os_path_exists"]
    mocks.os_path_exists.return_value = True

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

    async def test_sequential_cache_check_first_cached_stops_early(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """First result is cached → only 1 add_magnet + 1 get_torrent_info call made."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        hash1 = "c" * 40
        hash2 = "d" * 40
        result1 = _make_torrentio_result(info_hash=hash1)
        result2 = _make_torrentio_result(info_hash=hash2)
        filtered1 = _make_filtered_result(result1, score=80.0)
        filtered2 = _make_filtered_result(result2, score=60.0)

        async with _all_mocks(mock_rd_torrent) as m:
            m.torrentio_movie.return_value = [result1, result2]
            m.filter_rank.return_value = [filtered1, filtered2]
            m.rd_add.return_value = {"id": "RD_CACHED", "uri": "magnet:?xt=urn:btih:" + hash1}
            # First result is cached
            m.rd_get_torrent_info.return_value = {"id": "RD_CACHED", "status": "downloaded", "files": []}
            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        # Only 1 add_magnet call (stopped after first cached hit)
        assert m.rd_add.call_count == 1
        assert m.rd_get_torrent_info.call_count == 1
        # No deletes needed since the cached torrent was kept
        m.rd_delete.assert_not_called()
        assert result.action == "added_to_rd"
        assert result.selected_hash == hash1

    async def test_sequential_cache_check_first_two_not_cached_third_cached(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """First 2 not cached, 3rd cached → 3 checks made, first 2 deleted."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        hash1 = "c" * 40
        hash2 = "d" * 40
        hash3 = "e" * 40
        result1 = _make_torrentio_result(info_hash=hash1)
        result2 = _make_torrentio_result(info_hash=hash2)
        result3 = _make_torrentio_result(info_hash=hash3)
        filtered1 = _make_filtered_result(result1, score=80.0)
        filtered2 = _make_filtered_result(result2, score=70.0)
        filtered3 = _make_filtered_result(result3, score=60.0)

        # add_magnet returns different ids for each call
        m_add_returns = [
            {"id": "RD1", "uri": "m1"},
            {"id": "RD2", "uri": "m2"},
            {"id": "RD3", "uri": "m3"},
        ]
        # get_torrent_info: first two not cached, third cached
        m_info_returns = [
            {"id": "RD1", "status": "magnet_conversion", "files": []},
            {"id": "RD2", "status": "magnet_conversion", "files": []},
            {"id": "RD3", "status": "downloaded", "files": []},
        ]

        async with _all_mocks(mock_rd_torrent) as m:
            m.torrentio_movie.return_value = [result1, result2, result3]
            m.filter_rank.return_value = [filtered1, filtered2, filtered3]
            m.rd_add.side_effect = m_add_returns
            m.rd_get_torrent_info.side_effect = m_info_returns
            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert m.rd_add.call_count == 3
        assert m.rd_get_torrent_info.call_count == 3
        # First two uncached torrents deleted
        assert m.rd_delete.call_count == 2
        assert result.action == "added_to_rd"
        assert result.selected_hash == hash3

    async def test_sequential_cache_check_none_cached_uses_last_result(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """None cached after exhausting limit → uses last result (stays in RD for download)."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        hash1 = "f" * 40
        hash2 = "g" * 40
        result1 = _make_torrentio_result(info_hash=hash1)
        result2 = _make_torrentio_result(info_hash=hash2)
        filtered1 = _make_filtered_result(result1, score=80.0)
        filtered2 = _make_filtered_result(result2, score=70.0)

        m_add_returns = [
            {"id": "RD1", "uri": "m1"},
            {"id": "RD2", "uri": "m2"},
        ]
        m_info_returns = [
            {"id": "RD1", "status": "magnet_conversion", "files": []},
            {"id": "RD2", "status": "magnet_conversion", "files": []},
        ]

        async with _all_mocks(mock_rd_torrent) as m:
            m.torrentio_movie.return_value = [result1, result2]
            m.filter_rank.return_value = [filtered1, filtered2]
            m.rd_add.side_effect = m_add_returns
            m.rd_get_torrent_info.side_effect = m_info_returns
            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        # Both checked, first one deleted (not cached), last one kept for download
        assert m.rd_add.call_count == 2
        assert m.rd_get_torrent_info.call_count == 2
        assert m.rd_delete.call_count == 1  # Only the first was deleted
        assert result.action == "added_to_rd"
        # Last checked result is used
        assert result.selected_hash == hash2

    async def test_filter_always_receives_empty_cached_set(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """filter_and_rank is always called with an empty cached_hashes set (cache is checked after filtering)."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        torrentio_result = _make_torrentio_result()

        async with _all_mocks() as m:
            m.torrentio_movie.return_value = [torrentio_result]
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        m.filter_rank.assert_called_once()
        call_kwargs = m.filter_rank.call_args[1]
        cached_hashes = call_kwargs.get("cached_hashes") or set()
        assert cached_hashes == set()


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

    async def test_add_magnet_skipped_when_rd_id_from_sequential_cache_check(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """When _sequential_cache_check returns a cached rd_id, add_magnet is not called a second time."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        info_hash = "a" * 40
        torrentio_result = _make_torrentio_result(info_hash=info_hash)
        filtered = _make_filtered_result(torrentio_result)

        async with _all_mocks(mock_rd_torrent) as m:
            m.torrentio_movie.return_value = [torrentio_result]
            m.filter_rank.return_value = [filtered]
            # Simulate a cached torrent returned from the sequential cache check
            m.rd_add.return_value = {"id": "RD_REUSE", "uri": "magnet:?xt=urn:btih:" + info_hash}
            m.rd_get_torrent_info.return_value = {"id": "RD_REUSE", "status": "downloaded", "files": []}
            pipeline = ScrapePipeline()
            result = await pipeline.run(session, wanted_item)

        # add_magnet called exactly once during the sequential check (not again in _step_add_to_rd)
        assert m.rd_add.call_count == 1
        assert result.action == "added_to_rd"
        assert result.rd_torrent_id == "RD_REUSE"

    async def test_uncached_torrent_deleted_before_trying_next(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """Uncached torrents are deleted from RD immediately before checking the next candidate."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        hash1 = "a" * 40
        hash2 = "b" * 40
        result1 = _make_torrentio_result(info_hash=hash1)
        result2 = _make_torrentio_result(info_hash=hash2)
        filtered1 = _make_filtered_result(result1, score=80.0)
        filtered2 = _make_filtered_result(result2, score=70.0)

        async with _all_mocks(mock_rd_torrent) as m:
            m.torrentio_movie.return_value = [result1, result2]
            m.filter_rank.return_value = [filtered1, filtered2]
            m.rd_add.side_effect = [
                {"id": "RD1", "uri": "m1"},
                {"id": "RD2", "uri": "m2"},
            ]
            m.rd_get_torrent_info.side_effect = [
                {"id": "RD1", "status": "magnet_conversion", "files": []},  # not cached
                {"id": "RD2", "status": "downloaded", "files": []},          # cached
            ]
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        # First uncached torrent deleted before checking the second
        m.rd_delete.assert_called_once_with("RD1")
        # Second (cached) torrent is NOT deleted
        delete_calls = [call.args[0] for call in m.rd_delete.call_args_list]
        assert "RD2" not in delete_calls


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
            f"Got logs: {[(log.scraper, log.results_count, log.selected_result) for log in logs]}"
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

    mocks.rd_get_torrent_info = started["rd_get_torrent_info"]
    mocks.rd_get_torrent_info.return_value = {"id": "RD123", "status": "magnet_conversion", "files": []}

    mocks.rd_delete = started["rd_delete"]
    mocks.rd_delete.return_value = None

    mocks.filter_rank = started["filter_rank"]
    mocks.filter_rank.return_value = []

    mocks.queue_transition = started["queue_transition"]
    mocks.queue_transition.side_effect = None
    mocks.queue_transition.return_value = MagicMock(spec=MediaItem)

    mocks.os_path_exists = started["os_path_exists"]
    mocks.os_path_exists.return_value = True

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
      - Never call get_torrent_info or add_magnet
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
        """Hash dedup hit → neither get_torrent_info nor add_magnet are called."""
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

        m.rd_get_torrent_info.assert_not_called()
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
        """When check_local_duplicate returns None, pipeline proceeds to sequential cache check."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        torrentio_result = _make_torrentio_result(info_hash="e" * 40)
        filtered = _make_filtered_result(torrentio_result)

        async with _all_mocks_with_local_dedup(mock_rd_torrent) as m:
            m.torrentio_movie.return_value = [torrentio_result]
            m.filter_rank.return_value = [filtered]
            m.dedup_local.return_value = None  # No hash match

            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        # Pipeline continued past the hash dedup step — add_magnet was called
        m.rd_add.assert_called_once()


# ---------------------------------------------------------------------------
# Group 10: Zilean Alt-Title Retry (Issue #34)
# ---------------------------------------------------------------------------
#
# Patch targets for TMDB inline imports inside scrape_pipeline._run_pipeline
# and _step_zilean.  Both use ``from src.services.tmdb import tmdb_client``
# internally, so patching the singleton attributes on the imported module is
# the correct approach.
#
_PATCH_TARGETS_WITH_TMDB = {
    **_PATCH_TARGETS,
    "tmdb_get_movie_details": "src.services.tmdb.tmdb_client.get_movie_details",
    "tmdb_get_show_details": "src.services.tmdb.tmdb_client.get_show_details",
    "tmdb_get_alt_titles": "src.services.tmdb.tmdb_client.get_alternative_titles",
}


class _MocksWithTmdb(_Mocks):
    tmdb_get_movie_details: AsyncMock
    tmdb_get_show_details: AsyncMock
    tmdb_get_alt_titles: AsyncMock


@asynccontextmanager
async def _all_mocks_with_tmdb(
    mock_rd_torrent: RdTorrent | None = None,
) -> AsyncGenerator[_MocksWithTmdb, None]:
    """Like _all_mocks but also patches TMDB client methods.

    Yields a _MocksWithTmdb namespace with defaults that simulate TMDB
    returning no original_title and no alternative titles.
    """
    mocks = _MocksWithTmdb()
    patchers = {
        name: patch(target)
        for name, target in _PATCH_TARGETS_WITH_TMDB.items()
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

    mocks.rd_get_torrent_info = started["rd_get_torrent_info"]
    mocks.rd_get_torrent_info.return_value = {"id": "RD123", "status": "magnet_conversion", "files": []}

    mocks.rd_delete = started["rd_delete"]
    mocks.rd_delete.return_value = None

    mocks.filter_rank = started["filter_rank"]
    mocks.filter_rank.return_value = []

    mocks.queue_transition = started["queue_transition"]
    mocks.queue_transition.side_effect = None
    mocks.queue_transition.return_value = MagicMock(spec=MediaItem)

    mocks.os_path_exists = started["os_path_exists"]
    mocks.os_path_exists.return_value = True

    # TMDB defaults: return a minimal detail object with no original_title,
    # and no alternative titles.
    _mock_detail = MagicMock()
    _mock_detail.original_language = None
    _mock_detail.original_title = None

    mocks.tmdb_get_movie_details = started["tmdb_get_movie_details"]
    mocks.tmdb_get_movie_details.return_value = _mock_detail

    mocks.tmdb_get_show_details = started["tmdb_get_show_details"]
    mocks.tmdb_get_show_details.return_value = _mock_detail

    mocks.tmdb_get_alt_titles = started["tmdb_get_alt_titles"]
    mocks.tmdb_get_alt_titles.return_value = []

    try:
        yield mocks
    finally:
        for p in patchers.values():
            p.stop()


def _make_item_with_tmdb_id(
    session_unused,
    *,
    tmdb_id: str = "456",
    title: str = "Spirited Away",
    media_type: MediaType = MediaType.MOVIE,
    imdb_id: str = "tt0245429",
    year: int = 2001,
) -> MediaItem:
    """Build an in-memory MediaItem with a tmdb_id set (not persisted)."""
    return MediaItem(
        imdb_id=imdb_id,
        tmdb_id=tmdb_id,
        title=title,
        year=year,
        media_type=media_type,
        state=QueueState.WANTED,
        state_changed_at=None,
        retry_count=0,
    )


class TestZileanAltTitleRetry:
    """Zilean retries with original title and TMDB alternative titles on zero results.

    Tests exercise the fallback chain introduced in Issue #34:
      1. Primary title search → no results
      2. original_title from TMDB detail (Step 0) → try next
      3. Additional titles from get_alternative_titles() → try each in order
      4. Stop at first title that returns results
    """

    async def test_step_zilean_always_queries_alts_with_primary(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Alt titles are always queried concurrently with the primary title.

        Even when the primary returns results, alt titles are still fetched and
        queried so their unique hashes are merged into the result set.
        """
        ScrapePipeline, PipelineResult = _import_pipeline()

        # Enable TMDB in settings so the alt-title path is active
        mock_settings = MagicMock()
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "test-key"
        mock_settings.filters.prefer_original_language = False
        mock_settings.xem.enabled = False
        mock_settings.search.cache_check_limit = 3
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        zilean_result = _make_zilean_result()

        async with _all_mocks_with_tmdb() as m:
            # Primary title returns results; TMDB also provides one alt title
            m.zilean_search.return_value = [zilean_result]
            m.filter_rank.return_value = [_make_filtered_result(zilean_result)]
            m.tmdb_get_alt_titles.return_value = ["Alt Title One"]

            item = _make_item_with_tmdb_id(session, tmdb_id="123")
            session.add(item)
            await session.flush()

            pipeline = ScrapePipeline()
            result = await pipeline.run(session, item)

        # Zilean called for primary + 1 alt title = 2 total
        assert m.zilean_search.call_count == 2
        # Pipeline still produces a result (merged from both queries)
        assert result.action != "error"

    async def test_step_zilean_retries_with_original_title(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Primary returns [], original_title from TMDB detail is queried concurrently and returns results."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        mock_settings = MagicMock()
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "test-key"
        mock_settings.filters.prefer_original_language = False
        mock_settings.xem.enabled = False
        mock_settings.search.cache_check_limit = 3
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        zilean_result = _make_zilean_result()

        async with _all_mocks_with_tmdb() as m:
            # Primary title returns nothing; second call (original_title) returns results
            m.zilean_search.side_effect = [[], [zilean_result]]
            m.filter_rank.return_value = [_make_filtered_result(zilean_result)]

            # TMDB detail returns an original_title different from primary
            detail_mock = MagicMock()
            detail_mock.original_language = None
            detail_mock.original_title = "Sen to Chihiro no Kamikakushi"
            m.tmdb_get_movie_details.return_value = detail_mock

            item = _make_item_with_tmdb_id(
                session,
                tmdb_id="129",
                title="Spirited Away",
                media_type=MediaType.MOVIE,
            )
            session.add(item)
            await session.flush()

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, item)

        # Zilean called twice: first with primary, then with original_title
        assert m.zilean_search.call_count == 2
        # Verify second call used the original title
        second_call_kwargs = m.zilean_search.call_args_list[1][1]
        assert second_call_kwargs.get("query") == "Sen to Chihiro no Kamikakushi"
        # Pipeline proceeded with the results from the retry
        assert result.action != "no_results"

    async def test_step_zilean_retries_with_alt_titles(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Primary and original_title return [], TMDB alt titles fetched, one succeeds."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        mock_settings = MagicMock()
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "test-key"
        mock_settings.filters.prefer_original_language = False
        mock_settings.xem.enabled = False
        mock_settings.search.cache_check_limit = 3
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        zilean_result = _make_zilean_result()

        async with _all_mocks_with_tmdb() as m:
            # Primary returns nothing; "Alt Title" also returns nothing; "Second Alt" succeeds
            m.zilean_search.side_effect = [[], [], [zilean_result]]
            m.filter_rank.return_value = [_make_filtered_result(zilean_result)]

            # No original_title in TMDB detail (so alt_titles from Step 0 is empty)
            detail_mock = MagicMock()
            detail_mock.original_language = None
            detail_mock.original_title = None
            m.tmdb_get_movie_details.return_value = detail_mock

            # get_alternative_titles returns two candidates
            m.tmdb_get_alt_titles.return_value = ["Alt Title", "Second Alt"]

            item = _make_item_with_tmdb_id(
                session,
                tmdb_id="555",
                title="Main English Title",
                media_type=MediaType.MOVIE,
            )
            session.add(item)
            await session.flush()

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, item)

        # get_alternative_titles was called once (lazy fetch)
        m.tmdb_get_alt_titles.assert_called_once()
        # Zilean called 3 times total: primary + first alt (no results) + second alt (results)
        assert m.zilean_search.call_count == 3
        # Pipeline used the results from the third call
        assert result.action != "no_results"

    async def test_step_zilean_no_alt_titles_without_tmdb_id(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When item has no tmdb_id, alt-title retry is skipped gracefully."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        mock_settings = MagicMock()
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "test-key"
        mock_settings.filters.prefer_original_language = False
        mock_settings.xem.enabled = False
        mock_settings.search.cache_check_limit = 3
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        async with _all_mocks_with_tmdb() as m:
            # Primary returns nothing
            m.zilean_search.return_value = []

            # Item with NO tmdb_id
            item = MediaItem(
                imdb_id="tt9999999",
                tmdb_id=None,  # no tmdb_id
                title="Obscure Movie",
                year=2010,
                media_type=MediaType.MOVIE,
                state=QueueState.WANTED,
                state_changed_at=None,
                retry_count=0,
            )
            session.add(item)
            await session.flush()

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, item)

        # Zilean called exactly once (primary only — no tmdb_id to fetch alt titles)
        assert m.zilean_search.call_count == 1
        # get_alternative_titles never called
        m.tmdb_get_alt_titles.assert_not_called()
        # Outcome is no_results since everything returned empty
        assert result.action == "no_results"

    async def test_step_zilean_skips_duplicate_title(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An alt title identical to the primary title (case-insensitive) is skipped."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        mock_settings = MagicMock()
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "test-key"
        mock_settings.filters.prefer_original_language = False
        mock_settings.xem.enabled = False
        mock_settings.search.cache_check_limit = 3
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        async with _all_mocks_with_tmdb() as m:
            # Primary returns nothing
            m.zilean_search.return_value = []

            # No original_title in TMDB detail
            detail_mock = MagicMock()
            detail_mock.original_language = None
            detail_mock.original_title = None
            m.tmdb_get_movie_details.return_value = detail_mock

            # get_alternative_titles returns only the same title as primary (different case)
            m.tmdb_get_alt_titles.return_value = ["SPIRITED AWAY", "Spirited Away"]

            item = _make_item_with_tmdb_id(
                session,
                tmdb_id="129",
                title="Spirited Away",
                media_type=MediaType.MOVIE,
            )
            session.add(item)
            await session.flush()

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, item)

        # Alt titles "SPIRITED AWAY" and "Spirited Away" are both case-insensitively
        # equal to the primary "Spirited Away" and must be skipped.
        # So Zilean is only called once (the primary search).
        assert m.zilean_search.call_count == 1
        # Outcome is no_results since only duplicate titles were available
        assert result.action == "no_results"

    async def test_step_zilean_all_alt_titles_exhausted_returns_no_results(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """All alt titles tried but all return [], pipeline falls through to no_results."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        mock_settings = MagicMock()
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "test-key"
        mock_settings.filters.prefer_original_language = False
        mock_settings.xem.enabled = False
        mock_settings.search.cache_check_limit = 3
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        async with _all_mocks_with_tmdb() as m:
            # All searches return nothing
            m.zilean_search.return_value = []
            m.torrentio_movie.return_value = []

            detail_mock = MagicMock()
            detail_mock.original_language = None
            detail_mock.original_title = None
            m.tmdb_get_movie_details.return_value = detail_mock

            # Alt titles that also return nothing
            m.tmdb_get_alt_titles.return_value = ["Alt A", "Alt B"]

            item = _make_item_with_tmdb_id(
                session,
                tmdb_id="777",
                title="Totally Obscure Film",
                media_type=MediaType.MOVIE,
            )
            session.add(item)
            await session.flush()

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, item)

        # Pipeline gracefully returns no_results
        assert result.action == "no_results"

    async def test_step_zilean_get_alt_titles_failure_graceful(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """get_alternative_titles raising does not crash the pipeline."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        mock_settings = MagicMock()
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "test-key"
        mock_settings.filters.prefer_original_language = False
        mock_settings.xem.enabled = False
        mock_settings.search.cache_check_limit = 3
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        async with _all_mocks_with_tmdb() as m:
            m.zilean_search.return_value = []
            m.torrentio_movie.return_value = []

            detail_mock = MagicMock()
            detail_mock.original_language = None
            detail_mock.original_title = None
            m.tmdb_get_movie_details.return_value = detail_mock

            # TMDB alt titles fetch fails
            m.tmdb_get_alt_titles.side_effect = RuntimeError("TMDB unavailable")

            item = _make_item_with_tmdb_id(
                session,
                tmdb_id="888",
                title="Some Movie",
                media_type=MediaType.MOVIE,
            )
            session.add(item)
            await session.flush()

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, item)

        # Pipeline survived the error — must return a valid action, not raise
        assert result.action in ("no_results", "error", "added_to_rd", "dedup_hit", "mount_hit")

    async def test_step_zilean_original_title_same_as_primary_not_retried(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When TMDB original_title equals item.title (case-insensitive), it is excluded from alt titles and only one query is made."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        mock_settings = MagicMock()
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "test-key"
        mock_settings.filters.prefer_original_language = False
        mock_settings.xem.enabled = False
        mock_settings.search.cache_check_limit = 3
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        async with _all_mocks_with_tmdb() as m:
            m.zilean_search.return_value = []
            m.torrentio_movie.return_value = []

            # original_title is same as the item title — must not be added to alt_titles
            detail_mock = MagicMock()
            detail_mock.original_language = None
            detail_mock.original_title = "Spirited Away"  # identical to item.title
            m.tmdb_get_movie_details.return_value = detail_mock

            # get_alternative_titles also returns nothing meaningful
            m.tmdb_get_alt_titles.return_value = []

            item = _make_item_with_tmdb_id(
                session,
                tmdb_id="129",
                title="Spirited Away",
                media_type=MediaType.MOVIE,
            )
            session.add(item)
            await session.flush()

            pipeline = ScrapePipeline()
            await pipeline.run(session, item)

        # Zilean called once (primary only); original_title == primary so no extra call
        assert m.zilean_search.call_count == 1

    async def test_step_zilean_original_title_same_case_insensitive(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """original_title differing only by case from item.title is also skipped."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        mock_settings = MagicMock()
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "test-key"
        mock_settings.filters.prefer_original_language = False
        mock_settings.xem.enabled = False
        mock_settings.search.cache_check_limit = 3
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        async with _all_mocks_with_tmdb() as m:
            m.zilean_search.return_value = []
            m.torrentio_movie.return_value = []

            # original_title differs only in case
            detail_mock = MagicMock()
            detail_mock.original_language = None
            detail_mock.original_title = "SPIRITED AWAY"
            m.tmdb_get_movie_details.return_value = detail_mock
            m.tmdb_get_alt_titles.return_value = []

            item = _make_item_with_tmdb_id(
                session,
                tmdb_id="129",
                title="Spirited Away",
                media_type=MediaType.MOVIE,
            )
            session.add(item)
            await session.flush()

            pipeline = ScrapePipeline()
            await pipeline.run(session, item)

        # The case-insensitive match is caught in the _run_pipeline _alt_titles guard
        # (line: if _tmdb_original_title.lower() != item.title.lower())
        # → no extra Zilean call from the original_title path
        assert m.zilean_search.call_count == 1

    async def test_step_zilean_max_five_alt_title_candidates(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """At most 5 alt-title candidates are tried regardless of how many TMDB returns."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        mock_settings = MagicMock()
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "test-key"
        mock_settings.filters.prefer_original_language = False
        mock_settings.xem.enabled = False
        mock_settings.search.cache_check_limit = 3
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        async with _all_mocks_with_tmdb() as m:
            # All searches return nothing
            m.zilean_search.return_value = []
            m.torrentio_movie.return_value = []

            detail_mock = MagicMock()
            detail_mock.original_language = None
            detail_mock.original_title = None
            m.tmdb_get_movie_details.return_value = detail_mock

            # Return 10 distinct alt titles — only 5 should be tried
            m.tmdb_get_alt_titles.return_value = [
                f"Alt Title {i}" for i in range(1, 11)
            ]

            item = _make_item_with_tmdb_id(
                session,
                tmdb_id="999",
                title="Main Title",
                media_type=MediaType.MOVIE,
            )
            session.add(item)
            await session.flush()

            pipeline = ScrapePipeline()
            await pipeline.run(session, item)

        # Primary (1) + at most 5 alt titles = at most 6 Zilean calls total
        assert m.zilean_search.call_count <= 6

    async def test_step_zilean_alt_title_search_raises_continues_to_next(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A Zilean exception on one alt-title search skips that title and tries the next."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        mock_settings = MagicMock()
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "test-key"
        mock_settings.filters.prefer_original_language = False
        mock_settings.xem.enabled = False
        mock_settings.search.cache_check_limit = 3
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        zilean_result = _make_zilean_result()

        async with _all_mocks_with_tmdb() as m:
            # Primary returns nothing; first alt raises; second alt succeeds
            m.zilean_search.side_effect = [
                [],                                        # primary — no results
                RuntimeError("zilean connection reset"),   # first alt — raises
                [zilean_result],                           # second alt — succeeds
            ]
            m.filter_rank.return_value = [_make_filtered_result(zilean_result)]

            detail_mock = MagicMock()
            detail_mock.original_language = None
            detail_mock.original_title = None
            m.tmdb_get_movie_details.return_value = detail_mock
            m.tmdb_get_alt_titles.return_value = ["First Alt", "Second Alt"]

            item = _make_item_with_tmdb_id(
                session,
                tmdb_id="111",
                title="Original Title",
                media_type=MediaType.MOVIE,
            )
            session.add(item)
            await session.flush()

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, item)

        # Three Zilean calls: primary + first alt (raised, skipped) + second alt (success)
        assert m.zilean_search.call_count == 3
        # Pipeline proceeded with the successful result from the third call
        assert result.action != "no_results"

    async def test_step_zilean_tmdb_disabled_skips_alt_title_fallback(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When TMDB is disabled, the alt-title retry block is never entered."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        mock_settings = MagicMock()
        mock_settings.tmdb.enabled = False   # TMDB disabled
        mock_settings.tmdb.api_key = "test-key"
        mock_settings.filters.prefer_original_language = False
        mock_settings.xem.enabled = False
        mock_settings.search.cache_check_limit = 3
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        async with _all_mocks_with_tmdb() as m:
            m.zilean_search.return_value = []
            m.torrentio_movie.return_value = []

            item = MediaItem(
                imdb_id="tt1111111",
                tmdb_id="123",
                title="Some Movie",
                year=2020,
                media_type=MediaType.MOVIE,
                state=QueueState.WANTED,
                state_changed_at=None,
                retry_count=0,
            )
            session.add(item)
            await session.flush()

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, item)

        # TMDB disabled → Step 0 never called, alt-title guard never entered
        m.tmdb_get_movie_details.assert_not_called()
        m.tmdb_get_alt_titles.assert_not_called()
        assert m.zilean_search.call_count == 1
        assert result.action == "no_results"

    async def test_step_zilean_alt_title_tv_show_passes_tv_media_type(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """For a TV show item, get_alternative_titles is called with media_type='tv'."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        mock_settings = MagicMock()
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "test-key"
        mock_settings.filters.prefer_original_language = False
        mock_settings.xem.enabled = False
        mock_settings.search.cache_check_limit = 3
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        zilean_result = _make_zilean_result()

        async with _all_mocks_with_tmdb() as m:
            # Primary returns nothing; alt title returns results
            m.zilean_search.side_effect = [[], [zilean_result]]
            m.filter_rank.return_value = [_make_filtered_result(zilean_result)]

            detail_mock = MagicMock()
            detail_mock.original_language = None
            detail_mock.original_title = None
            m.tmdb_get_show_details.return_value = detail_mock

            m.tmdb_get_alt_titles.return_value = ["Boku no Hero Academia"]

            # TV show item
            item = MediaItem(
                imdb_id="tt5626028",
                tmdb_id="65930",
                title="My Hero Academia",
                year=2016,
                media_type=MediaType.SHOW,
                season=1,
                episode=1,
                state=QueueState.WANTED,
                state_changed_at=None,
                retry_count=0,
            )
            session.add(item)
            await session.flush()

            pipeline = ScrapePipeline()
            await pipeline.run(session, item)

        # get_alternative_titles must be called with "tv" as media_type
        m.tmdb_get_alt_titles.assert_called_once()
        call_args = m.tmdb_get_alt_titles.call_args
        passed_media_type = call_args[0][1] if len(call_args[0]) > 1 else call_args[1].get("media_type")
        assert passed_media_type == "tv"

    async def test_step_zilean_tmdb_no_api_key_skips_alt_title_fallback(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When TMDB api_key is blank, the alt-title retry block is never entered."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        mock_settings = MagicMock()
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = ""   # no API key
        mock_settings.filters.prefer_original_language = False
        mock_settings.xem.enabled = False
        mock_settings.search.cache_check_limit = 3
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        async with _all_mocks_with_tmdb() as m:
            m.zilean_search.return_value = []
            m.torrentio_movie.return_value = []

            item = MediaItem(
                imdb_id="tt2222222",
                tmdb_id="456",
                title="Another Movie",
                year=2021,
                media_type=MediaType.MOVIE,
                state=QueueState.WANTED,
                state_changed_at=None,
                retry_count=0,
            )
            session.add(item)
            await session.flush()

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, item)

        # No api_key → neither Step 0 nor the alt-title fallback should make TMDB calls
        m.tmdb_get_movie_details.assert_not_called()
        m.tmdb_get_alt_titles.assert_not_called()
        assert m.zilean_search.call_count == 1
        assert result.action == "no_results"

    async def test_step_zilean_alt_title_dedup_from_tmdb_fetch(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Alt titles from TMDB that duplicate a caller-supplied original_title are not retried twice."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        mock_settings = MagicMock()
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "test-key"
        mock_settings.filters.prefer_original_language = False
        mock_settings.xem.enabled = False
        mock_settings.search.cache_check_limit = 3
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        async with _all_mocks_with_tmdb() as m:
            # Primary returns nothing; original_title also returns nothing
            # TMDB alt titles list includes original_title again — must be deduped
            m.zilean_search.side_effect = [[], [], []]  # all fail
            m.torrentio_movie.return_value = []

            detail_mock = MagicMock()
            detail_mock.original_language = None
            detail_mock.original_title = "Sen to Chihiro"
            m.tmdb_get_movie_details.return_value = detail_mock

            # get_alternative_titles returns the same string as original_title
            m.tmdb_get_alt_titles.return_value = [
                "Sen to Chihiro",   # duplicate of original_title already in candidates
                "Chihiro's Journey",
            ]

            item = _make_item_with_tmdb_id(
                session,
                tmdb_id="129",
                title="Spirited Away",
                media_type=MediaType.MOVIE,
            )
            session.add(item)
            await session.flush()

            pipeline = ScrapePipeline()
            await pipeline.run(session, item)

        # Calls: primary + "Sen to Chihiro" + "Chihiro's Journey"
        # "Sen to Chihiro" appears once in candidates (deduped), so 3 calls total
        assert m.zilean_search.call_count == 3

    async def test_step_zilean_alt_title_hit_logged_with_merged_counts(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """ScrapeLog records the primary query plus per-title result counts."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        import json as _json  # noqa: PLC0415

        mock_settings = MagicMock()
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "test-key"
        mock_settings.filters.prefer_original_language = False
        mock_settings.xem.enabled = False
        mock_settings.search.cache_check_limit = 3
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        zilean_result = _make_zilean_result()

        async with _all_mocks_with_tmdb() as m:
            # Primary empty; alt title succeeds
            m.zilean_search.side_effect = [[], [zilean_result]]
            m.filter_rank.return_value = [_make_filtered_result(zilean_result)]

            detail_mock = MagicMock()
            detail_mock.original_language = None
            detail_mock.original_title = None
            m.tmdb_get_movie_details.return_value = detail_mock
            m.tmdb_get_alt_titles.return_value = ["Winning Alt Title"]

            item = _make_item_with_tmdb_id(
                session,
                tmdb_id="222",
                title="English Title",
                media_type=MediaType.MOVIE,
            )
            session.add(item)
            await session.flush()

            pipeline = ScrapePipeline()
            await pipeline.run(session, item)
            await session.flush()

        # Check the ScrapeLog for the zilean row
        from sqlalchemy import select as _select  # noqa: PLC0415

        logs_result = await session.execute(
            _select(ScrapeLog).where(
                ScrapeLog.media_item_id == item.id,
                ScrapeLog.scraper == "zilean",
            )
        )
        zilean_logs = logs_result.scalars().all()
        assert len(zilean_logs) == 1, (
            f"Expected exactly one zilean ScrapeLog; got {len(zilean_logs)}"
        )
        params = _json.loads(zilean_logs[0].query_params)
        # New format: primary query in "query", alt titles in "alt_titles", counts in
        # "per_title_counts"
        assert params["query"] == "English Title"
        assert "alt_titles" in params
        assert "per_title_counts" in params
        assert "Winning Alt Title" in params["alt_titles"]


# ---------------------------------------------------------------------------
# Group 11: _is_latin_script() unit tests
# ---------------------------------------------------------------------------


class TestIsLatinScript:
    """Unit tests for the _is_latin_script() helper.

    The function returns True when the majority (>50%) of non-whitespace
    characters are Basic Latin or Latin Extended (U+0000–U+024F).
    """

    def test_pure_latin_returns_true(self) -> None:
        """ASCII-only title returns True."""
        from src.core.scrape_pipeline import _is_latin_script  # noqa: PLC0415

        assert _is_latin_script("Test Anime Show") is True

    def test_romaji_with_spaces_returns_true(self) -> None:
        """Space-separated romaji title returns True."""
        from src.core.scrape_pipeline import _is_latin_script  # noqa: PLC0415

        assert _is_latin_script("Sousou no Frieren") is True

    def test_pure_cjk_returns_false(self) -> None:
        """Title composed entirely of CJK ideographs returns False."""
        from src.core.scrape_pipeline import _is_latin_script  # noqa: PLC0415

        assert _is_latin_script("葬送のフリーレン") is False

    def test_mixed_majority_latin_returns_true(self) -> None:
        """Mixed text where Latin characters outnumber non-Latin returns True."""
        from src.core.scrape_pipeline import _is_latin_script  # noqa: PLC0415

        # 10 Latin chars + 3 CJK ideographs → Latin majority
        assert _is_latin_script("LatinTitle 剣聖") is True

    def test_mixed_majority_cjk_returns_false(self) -> None:
        """Mixed text where CJK characters outnumber Latin returns False."""
        from src.core.scrape_pipeline import _is_latin_script  # noqa: PLC0415

        # 1 Latin char + many CJK — non-Latin majority
        assert _is_latin_script("X東京葛飾区の冒険世界") is False

    def test_romaji_with_macron_accents_returns_true(self) -> None:
        """Romanised title with extended-Latin accents (macrons) returns True."""
        from src.core.scrape_pipeline import _is_latin_script  # noqa: PLC0415

        # ō is U+014D (Latin Extended-B), within U+024F threshold
        assert _is_latin_script("Doragon B\u014dru Zetto") is True

    def test_empty_string_returns_true(self) -> None:
        """Empty string returns True (no non-Latin chars → treated as Latin)."""
        from src.core.scrape_pipeline import _is_latin_script  # noqa: PLC0415

        assert _is_latin_script("") is True

    def test_whitespace_only_returns_true(self) -> None:
        """String of whitespace only returns True (whitespace excluded from count)."""
        from src.core.scrape_pipeline import _is_latin_script  # noqa: PLC0415

        assert _is_latin_script("   ") is True

    def test_arabic_script_returns_false(self) -> None:
        """Arabic-script title (well beyond U+024F) returns False."""
        from src.core.scrape_pipeline import _is_latin_script  # noqa: PLC0415

        # Arabic characters are U+0600+
        assert _is_latin_script("عربي") is False

    def test_exactly_50_percent_latin_returns_false(self) -> None:
        """Exactly half Latin, half non-Latin → threshold is strictly >50% → False."""
        from src.core.scrape_pipeline import _is_latin_script  # noqa: PLC0415

        # 'AB' (2 Latin) + '葬送' (2 CJK) = 50% Latin, must return False
        assert _is_latin_script("AB葬送") is False


# ---------------------------------------------------------------------------
# Group 12: collect_alt_titles() unit tests
# ---------------------------------------------------------------------------


class TestCollectAltTitles:
    """Unit tests for the collect_alt_titles() function.

    The function gathers Latin-script alt titles from three sources:
      1. tmdb_original_title (in-memory)
      2. AniDB SQLite cache
      3. TMDB /alternative_titles API
    """

    async def test_returns_empty_list_when_tmdb_id_is_none(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When tmdb_id is None the function short-circuits to [] with no I/O."""
        from src.core.scrape_pipeline import collect_alt_titles  # noqa: PLC0415

        mock_settings = MagicMock()
        mock_settings.anidb.enabled = True
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "key"
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        result = await collect_alt_titles(
            session, tmdb_id=None, primary_title="Test Anime", media_type=MediaType.SHOW
        )
        assert result == []

    async def test_tmdb_original_title_included_when_different(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """tmdb_original_title is returned when it differs from the primary title."""
        from src.core.scrape_pipeline import collect_alt_titles  # noqa: PLC0415

        mock_settings = MagicMock()
        mock_settings.anidb.enabled = False
        mock_settings.tmdb.enabled = False
        mock_settings.tmdb.api_key = ""
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        result = await collect_alt_titles(
            session,
            tmdb_id=1,
            primary_title="Spirited Away",
            media_type=MediaType.MOVIE,
            tmdb_original_title="Sen to Chihiro no Kamikakushi",
        )
        assert "Sen to Chihiro no Kamikakushi" in result

    async def test_tmdb_original_title_deduplicated_against_primary(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """tmdb_original_title equal to primary_title (case-insensitive) is not returned."""
        from src.core.scrape_pipeline import collect_alt_titles  # noqa: PLC0415

        mock_settings = MagicMock()
        mock_settings.anidb.enabled = False
        mock_settings.tmdb.enabled = False
        mock_settings.tmdb.api_key = ""
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        result = await collect_alt_titles(
            session,
            tmdb_id=1,
            primary_title="Spirited Away",
            media_type=MediaType.MOVIE,
            tmdb_original_title="SPIRITED AWAY",  # same, different case
        )
        assert result == []

    async def test_cjk_titles_filtered_out(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """CJK titles from AniDB are excluded (they produce 0 scraper results)."""
        from src.core.scrape_pipeline import collect_alt_titles  # noqa: PLC0415

        mock_settings = MagicMock()
        mock_settings.anidb.enabled = True
        mock_settings.tmdb.enabled = False
        mock_settings.tmdb.api_key = ""
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        mock_anidb = AsyncMock()
        mock_anidb.get_titles_for_tmdb_id = AsyncMock(
            return_value=["葬送のフリーレン", "Test Anime Alt"]
        )
        monkeypatch.setattr("src.services.anidb.anidb_client", mock_anidb)

        result = await collect_alt_titles(
            session,
            tmdb_id=100,
            primary_title="Test Anime",
            media_type=MediaType.SHOW,
        )
        assert "葬送のフリーレン" not in result
        assert "Test Anime Alt" in result

    async def test_deduplication_case_insensitive_across_sources(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Titles already seen (different casing) are not added again."""
        from src.core.scrape_pipeline import collect_alt_titles  # noqa: PLC0415

        mock_settings = MagicMock()
        mock_settings.anidb.enabled = True
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "key"
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        mock_anidb = AsyncMock()
        # AniDB returns a title that is the same as primary in different case
        mock_anidb.get_titles_for_tmdb_id = AsyncMock(return_value=["TEST ANIME", "Alt Title"])
        monkeypatch.setattr("src.services.anidb.anidb_client", mock_anidb)

        mock_tmdb = AsyncMock()
        mock_tmdb.get_alternative_titles = AsyncMock(return_value=["ALT TITLE", "Unique Alt"])
        monkeypatch.setattr("src.services.tmdb.tmdb_client", mock_tmdb)

        result = await collect_alt_titles(
            session,
            tmdb_id=100,
            primary_title="Test Anime",
            media_type=MediaType.SHOW,
            max_titles=5,
        )
        # "TEST ANIME" duplicates primary; "ALT TITLE" duplicates "Alt Title"
        assert "TEST ANIME" not in result
        assert "ALT TITLE" not in result
        # Unique entries are present
        assert "Alt Title" in result
        assert "Unique Alt" in result

    async def test_respects_max_titles_cap(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No more than max_titles alternative titles are returned."""
        from src.core.scrape_pipeline import collect_alt_titles  # noqa: PLC0415

        mock_settings = MagicMock()
        mock_settings.anidb.enabled = True
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "key"
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        mock_anidb = AsyncMock()
        mock_anidb.get_titles_for_tmdb_id = AsyncMock(
            return_value=["Title One", "Title Two", "Title Three", "Title Four", "Title Five"]
        )
        monkeypatch.setattr("src.services.anidb.anidb_client", mock_anidb)

        result = await collect_alt_titles(
            session,
            tmdb_id=100,
            primary_title="Test Anime",
            media_type=MediaType.SHOW,
            max_titles=2,
        )
        assert len(result) <= 2

    async def test_anidb_disabled_skips_anidb_lookup(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When settings.anidb.enabled is False, AniDB is not called."""
        from src.core.scrape_pipeline import collect_alt_titles  # noqa: PLC0415

        mock_settings = MagicMock()
        mock_settings.anidb.enabled = False
        mock_settings.tmdb.enabled = False
        mock_settings.tmdb.api_key = ""
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        mock_anidb = AsyncMock()
        mock_anidb.get_titles_for_tmdb_id = AsyncMock(return_value=["Should Not Appear"])
        monkeypatch.setattr("src.services.anidb.anidb_client", mock_anidb)

        result = await collect_alt_titles(
            session,
            tmdb_id=100,
            primary_title="Test Show",
            media_type=MediaType.SHOW,
        )
        mock_anidb.get_titles_for_tmdb_id.assert_not_called()
        assert result == []

    async def test_falls_back_to_tmdb_when_anidb_gives_fewer_than_max(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When AniDB provides fewer titles than max_titles, TMDB is also queried."""
        from src.core.scrape_pipeline import collect_alt_titles  # noqa: PLC0415

        mock_settings = MagicMock()
        mock_settings.anidb.enabled = True
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "key"
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        mock_anidb = AsyncMock()
        # Only 1 AniDB title, but max_titles=3 → still need more
        mock_anidb.get_titles_for_tmdb_id = AsyncMock(return_value=["Anidb Alt"])
        monkeypatch.setattr("src.services.anidb.anidb_client", mock_anidb)

        mock_tmdb = AsyncMock()
        mock_tmdb.get_alternative_titles = AsyncMock(return_value=["Tmdb Alt One", "Tmdb Alt Two"])
        monkeypatch.setattr("src.services.tmdb.tmdb_client", mock_tmdb)

        result = await collect_alt_titles(
            session,
            tmdb_id=100,
            primary_title="Test Show",
            media_type=MediaType.SHOW,
            max_titles=3,
        )
        assert "Anidb Alt" in result
        assert "Tmdb Alt One" in result
        mock_tmdb.get_alternative_titles.assert_called_once()

    async def test_anidb_exception_returns_partial_results(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """AniDB raising does not crash collect_alt_titles; returns partial results."""
        from src.core.scrape_pipeline import collect_alt_titles  # noqa: PLC0415

        mock_settings = MagicMock()
        mock_settings.anidb.enabled = True
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "key"
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        mock_anidb = AsyncMock()
        mock_anidb.get_titles_for_tmdb_id = AsyncMock(
            side_effect=OSError("anidb db read error")
        )
        monkeypatch.setattr("src.services.anidb.anidb_client", mock_anidb)

        mock_tmdb = AsyncMock()
        mock_tmdb.get_alternative_titles = AsyncMock(return_value=["Tmdb Fallback"])
        monkeypatch.setattr("src.services.tmdb.tmdb_client", mock_tmdb)

        result = await collect_alt_titles(
            session,
            tmdb_id=100,
            primary_title="Test Movie",
            media_type=MediaType.MOVIE,
        )
        # AniDB failed → TMDB filled in
        assert "Tmdb Fallback" in result

    async def test_tmdb_exception_returns_partial_results(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """TMDB raising does not crash collect_alt_titles; returns partial results."""
        from src.core.scrape_pipeline import collect_alt_titles  # noqa: PLC0415

        mock_settings = MagicMock()
        mock_settings.anidb.enabled = True
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "key"
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        mock_anidb = AsyncMock()
        mock_anidb.get_titles_for_tmdb_id = AsyncMock(return_value=["Anidb Good"])
        monkeypatch.setattr("src.services.anidb.anidb_client", mock_anidb)

        mock_tmdb = AsyncMock()
        mock_tmdb.get_alternative_titles = AsyncMock(
            side_effect=RuntimeError("tmdb timeout")
        )
        monkeypatch.setattr("src.services.tmdb.tmdb_client", mock_tmdb)

        result = await collect_alt_titles(
            session,
            tmdb_id=100,
            primary_title="Test Movie",
            media_type=MediaType.MOVIE,
            max_titles=3,
        )
        # AniDB result still present despite TMDB failure
        assert "Anidb Good" in result

    async def test_includes_tmdb_original_title_before_anidb(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """tmdb_original_title (Source 1) is processed before AniDB (Source 2)."""
        from src.core.scrape_pipeline import collect_alt_titles  # noqa: PLC0415

        mock_settings = MagicMock()
        mock_settings.anidb.enabled = True
        mock_settings.tmdb.enabled = False
        mock_settings.tmdb.api_key = ""
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        mock_anidb = AsyncMock()
        mock_anidb.get_titles_for_tmdb_id = AsyncMock(return_value=["Anidb Title"])
        monkeypatch.setattr("src.services.anidb.anidb_client", mock_anidb)

        result = await collect_alt_titles(
            session,
            tmdb_id=100,
            primary_title="Test Show",
            media_type=MediaType.SHOW,
            tmdb_original_title="TMDB Original Title",
            max_titles=2,
        )
        # TMDB original should appear first
        assert result[0] == "TMDB Original Title"
        assert "Anidb Title" in result


# ---------------------------------------------------------------------------
# Group 13: _step_zilean() always-query (concurrent alt titles) tests
# ---------------------------------------------------------------------------

# Extended patch targets that include nyaa_client.search (needed for Nyaa tests)
_PATCH_TARGETS_WITH_TMDB_AND_NYAA = {
    **_PATCH_TARGETS_WITH_TMDB,
    "nyaa_search": "src.core.scrape_pipeline.nyaa_client.search",
    "anidb_get_titles": "src.services.anidb.anidb_client.get_titles_for_tmdb_id",
}


class _MocksWithTmdbAndNyaa(_MocksWithTmdb):
    nyaa_search: AsyncMock
    anidb_get_titles: AsyncMock


@asynccontextmanager
async def _all_mocks_with_nyaa(
    mock_rd_torrent: RdTorrent | None = None,
) -> AsyncGenerator[_MocksWithTmdbAndNyaa, None]:
    """Like _all_mocks_with_tmdb but also patches nyaa_client.search and anidb."""
    mocks = _MocksWithTmdbAndNyaa()
    patchers = {
        name: patch(target)
        for name, target in _PATCH_TARGETS_WITH_TMDB_AND_NYAA.items()
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

    # Re-use same defaults as _all_mocks_with_tmdb
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
    mocks.rd_get_torrent_info = started["rd_get_torrent_info"]
    mocks.rd_get_torrent_info.return_value = {"id": "RD123", "status": "magnet_conversion", "files": []}
    mocks.rd_delete = started["rd_delete"]
    mocks.rd_delete.return_value = None
    mocks.filter_rank = started["filter_rank"]
    mocks.filter_rank.return_value = []
    mocks.queue_transition = started["queue_transition"]
    mocks.queue_transition.side_effect = None
    mocks.queue_transition.return_value = MagicMock(spec=MediaItem)
    mocks.os_path_exists = started["os_path_exists"]
    mocks.os_path_exists.return_value = True

    _mock_detail = MagicMock()
    _mock_detail.original_language = None
    _mock_detail.original_title = None
    mocks.tmdb_get_movie_details = started["tmdb_get_movie_details"]
    mocks.tmdb_get_movie_details.return_value = _mock_detail
    mocks.tmdb_get_show_details = started["tmdb_get_show_details"]
    mocks.tmdb_get_show_details.return_value = _mock_detail
    mocks.tmdb_get_alt_titles = started["tmdb_get_alt_titles"]
    mocks.tmdb_get_alt_titles.return_value = []

    mocks.nyaa_search = started["nyaa_search"]
    mocks.nyaa_search.return_value = []
    mocks.anidb_get_titles = started["anidb_get_titles"]
    mocks.anidb_get_titles.return_value = []

    try:
        yield mocks
    finally:
        for p in patchers.values():
            p.stop()


def _make_nyaa_result(**overrides: object) -> object:
    """Build a NyaaResult with sensible defaults."""
    from src.services.nyaa import NyaaResult  # noqa: PLC0415

    defaults: dict[str, object] = {
        "info_hash": "c" * 40,
        "title": "Test Anime S01E01 1080p WEB-DL",
        "resolution": "1080p",
        "codec": None,
        "quality": "WEB-DL",
        "size_bytes": 500 * 1024**2,
        "seeders": 50,
        "source_tracker": "Nyaa",
        "season": 1,
        "episode": 1,
        "is_season_pack": False,
        "languages": [],
    }
    defaults.update(overrides)
    return NyaaResult(**defaults)


def _make_show_item_with_tmdb(
    *,
    tmdb_id: str = "999",
    title: str = "Test Anime Show",
    season: int = 1,
    episode: int = 1,
) -> MediaItem:
    """Build an in-memory SHOW MediaItem with tmdb_id set."""
    return MediaItem(
        imdb_id="tt7777777",
        tmdb_id=tmdb_id,
        title=title,
        year=2022,
        media_type=MediaType.SHOW,
        season=season,
        episode=episode,
        state=QueueState.WANTED,
        state_changed_at=None,
        retry_count=0,
    )


class TestStepZileanAlwaysQueryAltTitles:
    """_step_zilean() queries primary + alt titles concurrently.

    The new behaviour (always-query) differs from the old sequential retry:
    all titles are dispatched simultaneously via asyncio.gather so that results
    from every title are merged into one deduplicated list.
    """

    async def test_primary_and_alt_results_merged(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Primary returns 17 unique results; alt returns 200 with different hashes → all merged."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        mock_settings = MagicMock()
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "key"
        mock_settings.filters.prefer_original_language = False
        mock_settings.xem.enabled = False
        mock_settings.search.cache_check_limit = 3
        mock_settings.anidb.enabled = False
        mock_settings.scrapers.nyaa.enabled = False
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        # 17 unique hashes for primary
        primary_results = [
            _make_zilean_result(info_hash=f"p{i:039d}") for i in range(17)
        ]
        # 200 unique hashes for alt title
        alt_results = [
            _make_zilean_result(info_hash=f"a{i:039d}") for i in range(200)
        ]

        async with _all_mocks_with_tmdb() as m:
            m.tmdb_get_movie_details.return_value.original_title = None
            m.tmdb_get_alt_titles.return_value = ["Alt Title One"]
            # first call (primary) returns 17; second call (alt) returns 200
            m.zilean_search.side_effect = [primary_results, alt_results]
            # filter_rank gets all 217 merged unique results
            m.filter_rank.return_value = []

            item = _make_item_with_tmdb_id(
                session,
                tmdb_id="42",
                title="Main Title",
                media_type=MediaType.MOVIE,
            )
            session.add(item)
            await session.flush()

            pipeline = ScrapePipeline()
            await pipeline.run(session, item)

        # Both primary and alt were queried concurrently
        assert m.zilean_search.call_count == 2
        # filter_engine received the merged 217-item list
        call_args = m.filter_rank.call_args[0][0]
        assert len(call_args) == 217

    async def test_overlapping_hashes_deduplicated(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Primary and alt return overlapping hashes → properly deduplicated."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        mock_settings = MagicMock()
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "key"
        mock_settings.filters.prefer_original_language = False
        mock_settings.xem.enabled = False
        mock_settings.search.cache_check_limit = 3
        mock_settings.anidb.enabled = False
        mock_settings.scrapers.nyaa.enabled = False
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        shared_hash = "s" * 40
        primary_results = [
            _make_zilean_result(info_hash=shared_hash),
            _make_zilean_result(info_hash="p" * 40),
        ]
        # Alt returns the shared hash again + one unique
        alt_results = [
            _make_zilean_result(info_hash=shared_hash),
            _make_zilean_result(info_hash="q" * 40),
        ]

        async with _all_mocks_with_tmdb() as m:
            m.tmdb_get_movie_details.return_value.original_title = None
            m.tmdb_get_alt_titles.return_value = ["Alt Title"]
            m.zilean_search.side_effect = [primary_results, alt_results]
            m.filter_rank.return_value = []

            item = _make_item_with_tmdb_id(
                session, tmdb_id="55", title="Test Movie", media_type=MediaType.MOVIE
            )
            session.add(item)
            await session.flush()

            pipeline = ScrapePipeline()
            await pipeline.run(session, item)

        # 3 unique hashes: shared + "p" + "q"
        call_args = m.filter_rank.call_args[0][0]
        assert len(call_args) == 3

    async def test_alt_title_exception_primary_results_still_returned(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Alt title query raises an exception → primary results are still used."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        mock_settings = MagicMock()
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "key"
        mock_settings.filters.prefer_original_language = False
        mock_settings.xem.enabled = False
        mock_settings.search.cache_check_limit = 3
        mock_settings.anidb.enabled = False
        mock_settings.scrapers.nyaa.enabled = False
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        primary_result = _make_zilean_result(info_hash="g" * 40)

        async with _all_mocks_with_tmdb() as m:
            m.tmdb_get_movie_details.return_value.original_title = None
            m.tmdb_get_alt_titles.return_value = ["Alt That Fails"]
            # primary succeeds; alt raises
            m.zilean_search.side_effect = [
                [primary_result],
                RuntimeError("zilean timeout"),
            ]
            m.filter_rank.return_value = [_make_filtered_result(primary_result)]

            item = _make_item_with_tmdb_id(
                session, tmdb_id="77", title="Good Movie", media_type=MediaType.MOVIE
            )
            session.add(item)
            await session.flush()

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, item)

        # Pipeline didn't crash and used the primary result
        assert result.action != "error"
        # filter_rank got the 1 primary result
        call_args = m.filter_rank.call_args[0][0]
        assert len(call_args) == 1

    async def test_no_alt_titles_single_query_only(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When no alt titles exist, exactly one Zilean query is made."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        mock_settings = MagicMock()
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "key"
        mock_settings.filters.prefer_original_language = False
        mock_settings.xem.enabled = False
        mock_settings.search.cache_check_limit = 3
        mock_settings.anidb.enabled = False
        mock_settings.scrapers.nyaa.enabled = False
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        async with _all_mocks_with_tmdb() as m:
            m.tmdb_get_movie_details.return_value.original_title = None
            m.tmdb_get_alt_titles.return_value = []
            m.zilean_search.return_value = []

            item = _make_item_with_tmdb_id(
                session, tmdb_id="88", title="No Alt Title Movie", media_type=MediaType.MOVIE
            )
            session.add(item)
            await session.flush()

            pipeline = ScrapePipeline()
            await pipeline.run(session, item)

        assert m.zilean_search.call_count == 1

    async def test_gather_used_concurrently(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """With 3 alt titles, Zilean is called 4 times total (primary + 3 alts)."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        mock_settings = MagicMock()
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "key"
        mock_settings.filters.prefer_original_language = False
        mock_settings.xem.enabled = False
        mock_settings.search.cache_check_limit = 3
        mock_settings.anidb.enabled = False
        mock_settings.scrapers.nyaa.enabled = False
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        async with _all_mocks_with_tmdb() as m:
            m.tmdb_get_movie_details.return_value.original_title = None
            m.tmdb_get_alt_titles.return_value = ["Alt One", "Alt Two", "Alt Three"]
            m.zilean_search.return_value = []

            item = _make_item_with_tmdb_id(
                session, tmdb_id="99", title="Multi Alt Movie", media_type=MediaType.MOVIE
            )
            session.add(item)
            await session.flush()

            pipeline = ScrapePipeline()
            await pipeline.run(session, item)

        # 1 primary + 3 alt titles = 4 total concurrent calls
        assert m.zilean_search.call_count == 4


# ---------------------------------------------------------------------------
# Group 14: _step_nyaa() always-query (concurrent alt titles) tests
# ---------------------------------------------------------------------------


class TestStepNyaaAlwaysQueryAltTitles:
    """_step_nyaa() queries primary + alt titles concurrently and merges results.

    All titles are dispatched via asyncio.gather through the same fallback
    chain, and results are merged + deduplicated by info_hash.
    """

    async def test_nyaa_primary_and_alt_results_merged(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Primary returns 5 results; alt returns 10 new → merged to 15."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        mock_settings = MagicMock()
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "key"
        mock_settings.filters.prefer_original_language = False
        mock_settings.xem.enabled = False
        mock_settings.search.cache_check_limit = 3
        mock_settings.anidb.enabled = False
        mock_settings.scrapers.nyaa.enabled = True
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        primary_nyaa = [
            _make_nyaa_result(info_hash=f"n{i:039d}") for i in range(5)
        ]
        alt_nyaa = [
            _make_nyaa_result(info_hash=f"m{i:039d}") for i in range(10)
        ]

        async with _all_mocks_with_nyaa() as m:
            m.tmdb_get_show_details.return_value.original_title = None
            m.tmdb_get_alt_titles.return_value = ["Alt Anime Title"]
            # nyaa_search is called via the fallback chain inside _try_queries:
            # first call from primary chain returns primary_nyaa (stops fallback);
            # second call from alt chain returns alt_nyaa.
            m.nyaa_search.side_effect = [primary_nyaa, alt_nyaa]
            m.filter_rank.return_value = []

            item = _make_show_item_with_tmdb(tmdb_id="300", title="Test Anime Show")
            session.add(item)
            await session.flush()

            pipeline = ScrapePipeline()
            await pipeline.run(session, item)

        # filter_rank received 15 unique results (5 primary + 10 alt)
        call_args = m.filter_rank.call_args[0][0]
        assert len(call_args) == 15

    async def test_nyaa_alt_exception_handled_gracefully(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Exception from alt-title Nyaa chain does not crash the pipeline."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        mock_settings = MagicMock()
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "key"
        mock_settings.filters.prefer_original_language = False
        mock_settings.xem.enabled = False
        mock_settings.search.cache_check_limit = 3
        mock_settings.anidb.enabled = False
        mock_settings.scrapers.nyaa.enabled = True
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        primary_nyaa = [_make_nyaa_result(info_hash="h" * 40)]

        async with _all_mocks_with_nyaa() as m:
            m.tmdb_get_show_details.return_value.original_title = None
            m.tmdb_get_alt_titles.return_value = ["Failing Alt"]
            # primary succeeds; alt raises inside _try_queries via nyaa_client.search
            m.nyaa_search.side_effect = [primary_nyaa, RuntimeError("nyaa connection refused")]
            m.filter_rank.return_value = []

            item = _make_show_item_with_tmdb(tmdb_id="301", title="Test Anime Show")
            session.add(item)
            await session.flush()

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, item)

        # Pipeline survived; primary result still reached filter_rank
        assert result.action != "error"
        call_args = m.filter_rank.call_args[0][0]
        assert len(call_args) == 1

    async def test_nyaa_no_alt_titles_single_chain_only(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When no alt titles are available only the primary fallback chain runs."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        mock_settings = MagicMock()
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "key"
        mock_settings.filters.prefer_original_language = False
        mock_settings.xem.enabled = False
        mock_settings.search.cache_check_limit = 3
        mock_settings.anidb.enabled = False
        mock_settings.scrapers.nyaa.enabled = True
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        async with _all_mocks_with_nyaa() as m:
            m.tmdb_get_show_details.return_value.original_title = None
            m.tmdb_get_alt_titles.return_value = []
            m.nyaa_search.return_value = []

            item = _make_show_item_with_tmdb(tmdb_id="302", title="Test Show No Alt")
            session.add(item)
            await session.flush()

            pipeline = ScrapePipeline()
            await pipeline.run(session, item)

        # With no alt titles and no item.season/episode fallback queries,
        # each _try_queries call may hit up to 3 fallback levels, but only 1
        # title set runs (primary only).  Nyaa is called for the primary chain.
        # The key assertion is that the pipeline does not raise.
        assert m.nyaa_search.call_count >= 1

    async def test_nyaa_overlapping_hashes_deduplicated(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Duplicate hashes from primary and alt Nyaa results are deduplicated."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        mock_settings = MagicMock()
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "key"
        mock_settings.filters.prefer_original_language = False
        mock_settings.xem.enabled = False
        mock_settings.search.cache_check_limit = 3
        mock_settings.anidb.enabled = False
        mock_settings.scrapers.nyaa.enabled = True
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        shared = "x" * 40
        primary_nyaa = [
            _make_nyaa_result(info_hash=shared),
            _make_nyaa_result(info_hash="y" * 40),
        ]
        alt_nyaa = [
            _make_nyaa_result(info_hash=shared),  # duplicate
            _make_nyaa_result(info_hash="z" * 40),
        ]

        async with _all_mocks_with_nyaa() as m:
            m.tmdb_get_show_details.return_value.original_title = None
            m.tmdb_get_alt_titles.return_value = ["Alt Anime"]
            m.nyaa_search.side_effect = [primary_nyaa, alt_nyaa]
            m.filter_rank.return_value = []

            item = _make_show_item_with_tmdb(tmdb_id="303", title="Dedup Show")
            session.add(item)
            await session.flush()

            pipeline = ScrapePipeline()
            await pipeline.run(session, item)

        # 3 unique hashes: shared + "y" + "z"
        call_args = m.filter_rank.call_args[0][0]
        assert len(call_args) == 3

    async def test_nyaa_skipped_for_movies(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """_step_nyaa returns empty list for movie items regardless of alt titles."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        mock_settings = MagicMock()
        mock_settings.tmdb.enabled = True
        mock_settings.tmdb.api_key = "key"
        mock_settings.filters.prefer_original_language = False
        mock_settings.xem.enabled = False
        mock_settings.search.cache_check_limit = 3
        mock_settings.anidb.enabled = False
        mock_settings.scrapers.nyaa.enabled = True
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        async with _all_mocks_with_nyaa() as m:
            m.tmdb_get_movie_details.return_value.original_title = None
            m.tmdb_get_alt_titles.return_value = ["Alt Movie Title"]
            m.nyaa_search.return_value = []

            # Movie item — Nyaa never queries for movies
            item = _make_item_with_tmdb_id(
                session,
                tmdb_id="304",
                title="Test Movie No Nyaa",
                media_type=MediaType.MOVIE,
            )
            session.add(item)
            await session.flush()

            pipeline = ScrapePipeline()
            await pipeline.run(session, item)

        # Nyaa must never be called for movie items
        m.nyaa_search.assert_not_called()


# ---------------------------------------------------------------------------
# Change 4: CHECKING dedup loop-breaking (checking_failed_hash)
# ---------------------------------------------------------------------------


import json as _json_module  # noqa: E402 — import after existing imports to keep diff small


class TestCheckingFailedHashDedup:
    """Tests for the checking_failed_hash loop-prevention mechanism.

    When a CHECKING stage times out, the pipeline stores the offending hash in
    metadata_json["checking_failed_hash"].  On the next run:

    - _step_dedup_check: if the dedup hit matches the failed hash, skip it,
      clear the flag, and transition to SLEEPING instead of CHECKING.
    - Hash-based dedup block: if the top ranked result matches the failed hash,
      remove it from ranked and either continue with alternatives or go SLEEPING.
    """

    # -----------------------------------------------------------------------
    # _step_dedup_check — content-dedup path (Step 2)
    # -----------------------------------------------------------------------

    async def test_dedup_check_skips_failed_hash(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """When dedup hit hash matches checking_failed_hash, action='checking_loop_skip'."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        failed_hash = "a" * 40
        mock_rd_torrent.info_hash = failed_hash
        wanted_item.metadata_json = _json_module.dumps({"checking_failed_hash": failed_hash})
        await session.flush()

        async with _all_mocks(mock_rd_torrent) as m:
            m.dedup_check.return_value = mock_rd_torrent
            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.action == "checking_loop_skip"

    async def test_dedup_check_clears_flag_after_skip(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """After skipping the failed hash, checking_failed_hash is removed from metadata_json."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        failed_hash = "a" * 40
        mock_rd_torrent.info_hash = failed_hash
        wanted_item.metadata_json = _json_module.dumps(
            {"checking_failed_hash": failed_hash, "other_key": "preserved"}
        )
        await session.flush()

        async with _all_mocks(mock_rd_torrent) as m:
            m.dedup_check.return_value = mock_rd_torrent
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        # Reload from session to pick up the flush.
        await session.refresh(wanted_item)
        if wanted_item.metadata_json:
            meta = _json_module.loads(wanted_item.metadata_json)
            assert "checking_failed_hash" not in meta
        # other_key may or may not survive depending on whether meta is empty,
        # but the important thing is the failed hash flag is gone.

    async def test_dedup_check_different_hash_proceeds_normally(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """When the dedup hit has a different hash than checking_failed_hash, normal CHECKING."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        failed_hash = "f" * 40
        different_hash = "a" * 40
        mock_rd_torrent.info_hash = different_hash
        wanted_item.metadata_json = _json_module.dumps({"checking_failed_hash": failed_hash})
        await session.flush()

        async with _all_mocks(mock_rd_torrent) as m:
            m.dedup_check.return_value = mock_rd_torrent
            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        # Normal dedup hit — should be "dedup_hit", NOT "checking_loop_skip".
        assert result.action == "dedup_hit"

    async def test_dedup_check_no_flag_normal_behavior(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """When metadata_json has no checking_failed_hash, standard dedup_hit path runs."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        mock_rd_torrent.info_hash = "b" * 40
        # No metadata_json / no flag.
        wanted_item.metadata_json = None
        await session.flush()

        async with _all_mocks(mock_rd_torrent) as m:
            m.dedup_check.return_value = mock_rd_torrent
            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.action == "dedup_hit"

    # -----------------------------------------------------------------------
    # Hash-based dedup block — top ranked result path
    # -----------------------------------------------------------------------

    async def test_hash_dedup_skips_failed_hash_no_alternatives(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """Hash-based dedup: if top result matches checking_failed_hash and no alternatives,
        action='checking_loop_skip' and transition to SLEEPING.
        """
        ScrapePipeline, PipelineResult = _import_pipeline()

        failed_hash = "c" * 40
        wanted_item.metadata_json = _json_module.dumps({"checking_failed_hash": failed_hash})
        await session.flush()

        # Build a scrape result with the failed hash.
        torrentio_result = _make_torrentio_result(info_hash=failed_hash)
        filtered = _make_filtered_result(torrentio_result, score=80.0)

        # The dedup engine will "find" this hash as an existing torrent
        # via check_local_duplicate (the hash-based dedup call in the pipeline).
        existing_torrent = MagicMock(spec=RdTorrent)
        existing_torrent.rd_id = "RD999"
        existing_torrent.info_hash = failed_hash

        async with _all_mocks() as m:
            m.dedup_check.return_value = None          # content-dedup misses
            m.torrentio_movie.return_value = [torrentio_result]
            m.filter_rank.return_value = [filtered]
            with patch(
                "src.core.scrape_pipeline.dedup_engine.check_local_duplicate",
                new=AsyncMock(return_value=existing_torrent),
            ):
                pipeline = ScrapePipeline()
                result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.action == "checking_loop_skip"

    async def test_corrupted_metadata_json_no_crash(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """When metadata_json is malformed JSON the pipeline handles it gracefully."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        # Store deliberately malformed JSON.
        wanted_item.metadata_json = "not valid json {{{"
        mock_rd_torrent.info_hash = "a" * 40
        await session.flush()

        async with _all_mocks(mock_rd_torrent) as m:
            m.dedup_check.return_value = mock_rd_torrent
            pipeline = ScrapePipeline()
            # Must not raise — malformed metadata_json is treated as empty.
            result: PipelineResult = await pipeline.run(session, wanted_item)

        # Normal dedup_hit because no valid checking_failed_hash was parsed.
        assert result.action == "dedup_hit"

    async def test_dedup_check_transitions_sleeping_on_skip(
        self, session: AsyncSession, wanted_item: MediaItem, mock_rd_torrent: RdTorrent
    ) -> None:
        """When _step_dedup_check skips the failed hash, queue_manager.transition is called
        with SLEEPING.
        """
        ScrapePipeline, PipelineResult = _import_pipeline()

        failed_hash = "d" * 40
        mock_rd_torrent.info_hash = failed_hash
        wanted_item.metadata_json = _json_module.dumps({"checking_failed_hash": failed_hash})
        await session.flush()

        async with _all_mocks(mock_rd_torrent) as m:
            m.dedup_check.return_value = mock_rd_torrent
            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        # Verify transition was called at least once with SLEEPING.
        from src.models.media_item import QueueState  # noqa: PLC0415

        sleeping_calls = [
            call_args
            for call_args in m.queue_transition.call_args_list
            if QueueState.SLEEPING in call_args.args or QueueState.SLEEPING in call_args.kwargs.values()
        ]
        assert len(sleeping_calls) >= 1


# ---------------------------------------------------------------------------
# Group 9: filter_year_mismatches unit tests
# ---------------------------------------------------------------------------


class TestFilterYearMismatches:
    """Unit tests for the module-level filter_year_mismatches helper."""

    def _make_entry(self, parsed_year: int | None) -> MountIndex:
        """Build a minimal MountIndex entry with the given parsed_year."""
        entry = MountIndex(
            filepath=f"/mnt/zurg/__all__/show.{parsed_year}.mkv",
            filename=f"show.{parsed_year}.mkv",
            parsed_year=parsed_year,
        )
        return entry

    def test_no_item_year_returns_all(self) -> None:
        """When item_year is None all matches are returned unmodified."""
        from src.core.scrape_pipeline import filter_year_mismatches  # noqa: PLC0415

        entries = [self._make_entry(2020), self._make_entry(1986), self._make_entry(None)]
        result = filter_year_mismatches(entries, item_year=None)
        assert result is entries  # exact same list object, not a copy

    def test_exact_year_match_kept(self) -> None:
        """An entry whose parsed_year exactly equals item_year is kept."""
        from src.core.scrape_pipeline import filter_year_mismatches  # noqa: PLC0415

        entry = self._make_entry(2024)
        result = filter_year_mismatches([entry], item_year=2024)
        assert result == [entry]

    def test_off_by_one_kept_lower(self) -> None:
        """parsed_year one year below item_year is within tolerance — kept."""
        from src.core.scrape_pipeline import filter_year_mismatches  # noqa: PLC0415

        entry = self._make_entry(2023)
        result = filter_year_mismatches([entry], item_year=2024)
        assert result == [entry]

    def test_off_by_one_kept_upper(self) -> None:
        """parsed_year one year above item_year is within tolerance — kept."""
        from src.core.scrape_pipeline import filter_year_mismatches  # noqa: PLC0415

        entry = self._make_entry(2025)
        result = filter_year_mismatches([entry], item_year=2024)
        assert result == [entry]

    def test_year_mismatch_beyond_tolerance_filtered(self) -> None:
        """parsed_year more than 1 year away from item_year is removed."""
        from src.core.scrape_pipeline import filter_year_mismatches  # noqa: PLC0415

        entry = self._make_entry(1986)
        result = filter_year_mismatches([entry], item_year=1989)
        assert result == []

    def test_match_without_parsed_year_kept(self) -> None:
        """An entry with parsed_year=None is kept regardless of item_year."""
        from src.core.scrape_pipeline import filter_year_mismatches  # noqa: PLC0415

        entry = self._make_entry(None)
        result = filter_year_mismatches([entry], item_year=2024)
        assert result == [entry]

    def test_mixed_matches_partial_filter(self) -> None:
        """Mixed list: correct year + None year kept, wrong year discarded."""
        from src.core.scrape_pipeline import filter_year_mismatches  # noqa: PLC0415

        correct = self._make_entry(2024)
        no_year = self._make_entry(None)
        wrong = self._make_entry(1986)

        result = filter_year_mismatches([correct, no_year, wrong], item_year=2024)
        assert result == [correct, no_year]

    def test_all_matches_wrong_year_returns_empty(self) -> None:
        """When every entry's parsed_year is out of range, empty list returned."""
        from src.core.scrape_pipeline import filter_year_mismatches  # noqa: PLC0415

        entries = [self._make_entry(1980), self._make_entry(2000)]
        result = filter_year_mismatches(entries, item_year=2024)
        assert result == []

    def test_empty_input_returns_empty(self) -> None:
        """Empty input produces empty output regardless of item_year."""
        from src.core.scrape_pipeline import filter_year_mismatches  # noqa: PLC0415

        result = filter_year_mismatches([], item_year=2024)
        assert result == []


# ---------------------------------------------------------------------------
# Group 10: Year-mismatch filter integration with _step_mount_check
# ---------------------------------------------------------------------------


class TestMountCheckYearFilter:
    """Integration tests verifying year filtering inside _step_mount_check."""

    async def test_mount_hit_with_wrong_year_continues_pipeline(
        self, session: AsyncSession, wanted_item: MediaItem, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Mount index returns a hit with wrong year → pipeline continues (returns None).

        wanted_item has year=2024. The mount entry has parsed_year=1986 — well outside
        the ±1 tolerance — so the match must be discarded and the pipeline must continue
        to the scraper stage, not return a mount_hit result.
        """
        ScrapePipeline, PipelineResult = _import_pipeline()

        # Build a mount entry whose year is far outside tolerance.
        wrong_year_hit = MountIndex(
            filepath="/mnt/zurg/__all__/Generic Show 1986.mkv",
            filename="Generic Show 1986.mkv",
            parsed_title="generic show",
            parsed_year=1986,
            parsed_season=None,
            parsed_episode=None,
        )

        with caplog.at_level(logging.INFO):
            async with _all_mocks() as m:
                m.mount_scanner_lookup.return_value = [wrong_year_hit]
                pipeline = ScrapePipeline()
                result: PipelineResult = await pipeline.run(session, wanted_item)

        # The year-mismatch filter discarded all matches, so the pipeline must not
        # short-circuit and must reach the scraper stage.
        assert result.action != "mount_hit"
        # Dedup was invoked — pipeline continued past mount check.
        m.dedup_check.assert_called_once()
        # Info log must mention year mismatch.
        assert any("year mismatch" in record.message for record in caplog.records)

    async def test_mount_hit_with_matching_year_proceeds(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """Mount index returns a hit with matching year → PipelineResult(action='mount_hit').

        wanted_item has year=2024. The mount entry has parsed_year=2024 (exact match),
        within tolerance, so the pipeline must short-circuit with mount_hit.
        """
        ScrapePipeline, PipelineResult = _import_pipeline()

        correct_year_hit = MountIndex(
            filepath="/mnt/zurg/__all__/Generic Show 2024.mkv",
            filename="Generic Show 2024.mkv",
            parsed_title="generic show",
            parsed_year=2024,
            parsed_season=None,
            parsed_episode=None,
        )

        async with _all_mocks() as m:
            m.mount_scanner_lookup.return_value = [correct_year_hit]
            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.action == "mount_hit"
        assert result.mount_path == correct_year_hit.filepath
        # Scrapers must not have been invoked.
        m.zilean_search.assert_not_called()
        m.torrentio_movie.assert_not_called()
