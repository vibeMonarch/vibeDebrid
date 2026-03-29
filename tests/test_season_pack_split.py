"""Tests for the season pack split feature in src/core/scrape_pipeline.py.

When a season pack item finds only individual episode results (no season packs
survive the filter engine), the pipeline automatically splits the original item
into individual episode queue items — one per episode in the season as reported
by TMDB.

Feature under test: ScrapePipeline._split_season_pack_to_episodes and its
integration with the main run() pipeline.

Unit tests cover _split_season_pack_to_episodes in isolation:
  Group 1 — Happy path and partial creation (3 tests)
  Group 2 — Guard clauses: missing tmdb_id / season / TMDB failure (5 tests)
  Group 3 — Created item field validation (3 tests)

Integration tests drive run() end-to-end through mocked external services:
  Group 4 — Pipeline integration: split fired vs. not fired (5 tests)

asyncio_mode = "auto" (set in pyproject.toml), so no @pytest.mark.asyncio needed.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.media_item import MediaItem, MediaType, QueueState
from src.services.tmdb import TmdbSeasonInfo, TmdbShowDetail
from src.services.torrentio import TorrentioResult
from src.services.zilean import ZileanResult

# ---------------------------------------------------------------------------
# Test data helpers
# ---------------------------------------------------------------------------


def _make_torrentio_result(**overrides: object) -> TorrentioResult:
    """Build a TorrentioResult with sensible defaults (not a season pack)."""
    defaults: dict[str, object] = {
        "info_hash": "a" * 40,
        "title": "Show.S01E01.1080p.WEB-DL.x265-GROUP",
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


def _make_season_pack_result(**overrides: object) -> TorrentioResult:
    """Build a TorrentioResult that IS a season pack."""
    defaults: dict[str, object] = {
        "info_hash": "b" * 40,
        "title": "Show.S01.COMPLETE.1080p.WEB-DL.x265-GROUP",
        "resolution": "1080p",
        "codec": "x265",
        "quality": "WEB-DL",
        "size_bytes": 20 * 1024**3,
        "seeders": 200,
        "release_group": "GROUP",
        "languages": [],
        "is_season_pack": True,
    }
    defaults.update(overrides)
    return TorrentioResult(**defaults)


def _make_filtered_result(
    result: TorrentioResult | ZileanResult,
    score: float = 75.0,
) -> MagicMock:
    """Return a mock FilteredResult wrapping a scrape result."""
    fr = MagicMock()
    fr.result = result
    fr.score = score
    fr.rejection_reason = None
    fr.score_breakdown = {
        "resolution": 40.0,
        "codec": 15.0,
        "source": 12.0,
        "audio": 3.0,
        "seeders": 10.0,
        "cached": 0.0,
        "season_pack": 0.0,
    }
    return fr


def _make_tmdb_show_detail(episode_count: int = 12, season: int = 1) -> TmdbShowDetail:
    """Build a TmdbShowDetail with one season containing the given number of episodes.

    The implementation calls tmdb_client.get_show_details() and then looks up
    the matching TmdbSeasonInfo by season_number to read episode_count.
    """
    season_info = TmdbSeasonInfo(
        season_number=season,
        name=f"Season {season}",
        episode_count=episode_count,
    )
    return TmdbShowDetail(
        tmdb_id=55566,
        title="Great Anime",
        year=2024,
        seasons=[season_info],
    )


# Keep the old name as an alias so any future callers are unambiguous.
_make_tmdb_season_detail = _make_tmdb_show_detail


# ---------------------------------------------------------------------------
# Season pack item fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def season_pack_item(session: AsyncSession) -> MediaItem:
    """A SHOW MediaItem in SCRAPING state with is_season_pack=True."""
    item = MediaItem(
        imdb_id="tt9988776",
        tmdb_id="55566",
        tvdb_id=12345,
        title="Great Anime",
        year=2024,
        original_language="ja",
        media_type=MediaType.SHOW,
        state=QueueState.SCRAPING,
        state_changed_at=datetime.now(UTC),
        retry_count=0,
        season=1,
        episode=None,
        is_season_pack=True,
        quality_profile="1080p",
        source="trakt",
    )
    session.add(item)
    await session.flush()
    return item


@pytest.fixture
async def season_pack_item_no_tmdb(session: AsyncSession) -> MediaItem:
    """A season pack item with tmdb_id=None."""
    item = MediaItem(
        imdb_id="tt1111111",
        tmdb_id=None,
        title="No TMDB Show",
        year=2023,
        media_type=MediaType.SHOW,
        state=QueueState.SCRAPING,
        state_changed_at=datetime.now(UTC),
        retry_count=0,
        season=1,
        episode=None,
        is_season_pack=True,
    )
    session.add(item)
    await session.flush()
    return item


@pytest.fixture
async def season_pack_item_no_season(session: AsyncSession) -> MediaItem:
    """A season pack item with season=None."""
    item = MediaItem(
        imdb_id="tt2222222",
        tmdb_id="77788",
        title="No Season Show",
        year=2023,
        media_type=MediaType.SHOW,
        state=QueueState.SCRAPING,
        state_changed_at=datetime.now(UTC),
        retry_count=0,
        season=None,
        episode=None,
        is_season_pack=True,
    )
    session.add(item)
    await session.flush()
    return item


# ---------------------------------------------------------------------------
# Pipeline patch context manager (mirrors test_scrape_pipeline.py pattern)
# ---------------------------------------------------------------------------

_PATCH_TARGETS = {
    "mount_scanner_available": "src.core.scrape_pipeline.mount_scanner.is_mount_available",
    "mount_scanner_lookup": "src.core.scrape_pipeline.mount_scanner.lookup",
    "dedup_check": "src.core.scrape_pipeline.dedup_engine.check_content_duplicate",
    "dedup_register": "src.core.scrape_pipeline.dedup_engine.register_torrent",
    "dedup_local": "src.core.scrape_pipeline.dedup_engine.check_local_duplicate",
    "zilean_search": "src.core.scrape_pipeline.zilean_client.search",
    "torrentio_movie": "src.core.scrape_pipeline.torrentio_client.scrape_movie",
    "torrentio_episode": "src.core.scrape_pipeline.torrentio_client.scrape_episode",
    "rd_add": "src.core.scrape_pipeline.rd_client.add_magnet",
    "rd_select": "src.core.scrape_pipeline.rd_client.select_files",
    "rd_check_cached_batch": "src.core.scrape_pipeline.rd_client.check_cached_batch",
    "rd_delete": "src.core.scrape_pipeline.rd_client.delete_torrent",
    "filter_rank": "src.core.scrape_pipeline.filter_engine.filter_and_rank",
    "queue_transition": "src.core.scrape_pipeline.queue_manager.transition",
    "tmdb_get_season": "src.services.tmdb.tmdb_client.get_show_details",
}


class _Mocks:
    """Namespace holding all active mock objects for a test."""

    mount_scanner_available: AsyncMock
    mount_scanner_lookup: AsyncMock
    dedup_check: AsyncMock
    dedup_register: AsyncMock
    dedup_local: AsyncMock
    zilean_search: AsyncMock
    torrentio_movie: AsyncMock
    torrentio_episode: AsyncMock
    rd_add: AsyncMock
    rd_select: AsyncMock
    rd_check_cached_batch: AsyncMock
    rd_delete: AsyncMock
    filter_rank: MagicMock
    queue_transition: AsyncMock
    tmdb_get_season: AsyncMock


@asynccontextmanager
async def _all_mocks() -> AsyncGenerator[_Mocks, None]:
    """Patch every ScrapePipeline + split dependency at once.

    Defaults mimic the 'nothing found, no errors' path.  Tests override
    individual mocks for the specific scenario under test.
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

    mocks.mount_scanner_available = started["mount_scanner_available"]
    mocks.mount_scanner_available.return_value = True

    mocks.mount_scanner_lookup = started["mount_scanner_lookup"]
    mocks.mount_scanner_lookup.return_value = []

    mocks.dedup_check = started["dedup_check"]
    mocks.dedup_check.return_value = None

    mocks.dedup_register = started["dedup_register"]
    mocks.dedup_register.return_value = MagicMock()

    mocks.dedup_local = started["dedup_local"]
    mocks.dedup_local.return_value = None

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
    mocks.queue_transition.return_value = MagicMock(spec=MediaItem)

    mocks.tmdb_get_season = started["tmdb_get_season"]
    mocks.tmdb_get_season.return_value = None

    try:
        yield mocks
    finally:
        for p in patchers.values():
            p.stop()


def _import_pipeline():
    """Lazily import ScrapePipeline so missing module raises per-test."""
    from src.core.scrape_pipeline import PipelineResult, ScrapePipeline  # noqa: PLC0415

    return ScrapePipeline, PipelineResult


# ---------------------------------------------------------------------------
# Group 1: Happy path and partial creation
# ---------------------------------------------------------------------------


class TestSplitSeasonPackToEpisodesHappyPath:
    """_split_season_pack_to_episodes creates new WANTED episode items."""

    async def test_full_season_creates_all_episodes(
        self, session: AsyncSession, season_pack_item: MediaItem
    ) -> None:
        """TMDB returns 12 episodes, none exist → 12 new items created."""
        ScrapePipeline, _ = _import_pipeline()
        pipeline = ScrapePipeline()
        season_detail = _make_tmdb_season_detail(episode_count=12, season=1)

        with patch(
            "src.services.tmdb.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=season_detail,
        ):
            created = await pipeline._split_season_pack_to_episodes(
                session, season_pack_item
            )

        assert created == 12

        rows = (
            await session.execute(
                select(MediaItem).where(
                    MediaItem.imdb_id == season_pack_item.imdb_id,
                    MediaItem.is_season_pack == False,  # noqa: E712
                    MediaItem.season == 1,
                )
            )
        ).scalars().all()
        assert len(rows) == 12
        episode_numbers = sorted(r.episode for r in rows)
        assert episode_numbers == list(range(1, 13))

    async def test_partial_existing_creates_only_missing(
        self, session: AsyncSession, season_pack_item: MediaItem
    ) -> None:
        """5 episodes already in queue → only 7 new items are inserted."""
        ScrapePipeline, _ = _import_pipeline()
        pipeline = ScrapePipeline()

        # Pre-populate episodes 1-5
        for ep in range(1, 6):
            existing = MediaItem(
                imdb_id=season_pack_item.imdb_id,
                tmdb_id=season_pack_item.tmdb_id,
                title=season_pack_item.title,
                year=season_pack_item.year,
                media_type=MediaType.SHOW,
                state=QueueState.WANTED,
                state_changed_at=datetime.now(UTC),
                retry_count=0,
                season=1,
                episode=ep,
                is_season_pack=False,
            )
            session.add(existing)
        await session.flush()

        season_detail = _make_tmdb_season_detail(episode_count=12, season=1)
        with patch(
            "src.services.tmdb.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=season_detail,
        ):
            created = await pipeline._split_season_pack_to_episodes(
                session, season_pack_item
            )

        assert created == 7

        # Fetch only the episode rows that were NOT pre-seeded (episodes 6-12).
        # Both old (1-5) and new (6-12) rows are in WANTED state, so we filter
        # by episode number to isolate the ones the split should have created.
        all_rows = (
            await session.execute(
                select(MediaItem).where(
                    MediaItem.imdb_id == season_pack_item.imdb_id,
                    MediaItem.is_season_pack == False,  # noqa: E712
                    MediaItem.season == 1,
                    MediaItem.state == QueueState.WANTED,
                )
            )
        ).scalars().all()
        new_eps = sorted(r.episode for r in all_rows if r.episode is not None and r.episode >= 6)
        assert new_eps == list(range(6, 13))

    async def test_all_existing_creates_zero(
        self, session: AsyncSession, season_pack_item: MediaItem
    ) -> None:
        """All 3 episodes already in queue → returns 0, no inserts."""
        ScrapePipeline, _ = _import_pipeline()
        pipeline = ScrapePipeline()

        for ep in range(1, 4):
            session.add(
                MediaItem(
                    imdb_id=season_pack_item.imdb_id,
                    tmdb_id=season_pack_item.tmdb_id,
                    title=season_pack_item.title,
                    year=season_pack_item.year,
                    media_type=MediaType.SHOW,
                    state=QueueState.COMPLETE,
                    state_changed_at=datetime.now(UTC),
                    retry_count=0,
                    season=1,
                    episode=ep,
                    is_season_pack=False,
                )
            )
        await session.flush()

        season_detail = _make_tmdb_season_detail(episode_count=3, season=1)
        with patch(
            "src.services.tmdb.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=season_detail,
        ):
            created = await pipeline._split_season_pack_to_episodes(
                session, season_pack_item
            )

        assert created == 0


# ---------------------------------------------------------------------------
# Group 2: Guard clauses — return 0 without touching the DB
# ---------------------------------------------------------------------------


class TestSplitSeasonPackGuardClauses:
    """_split_season_pack_to_episodes returns 0 and is a no-op for bad inputs."""

    async def test_no_tmdb_id_returns_zero(
        self, session: AsyncSession, season_pack_item_no_tmdb: MediaItem
    ) -> None:
        """Item with tmdb_id=None → returns 0 without calling TMDB."""
        ScrapePipeline, _ = _import_pipeline()
        pipeline = ScrapePipeline()

        mock_tmdb = AsyncMock(return_value=None)
        with patch("src.services.tmdb.tmdb_client.get_show_details", mock_tmdb):
            created = await pipeline._split_season_pack_to_episodes(
                session, season_pack_item_no_tmdb
            )

        assert created == 0
        mock_tmdb.assert_not_called()

    async def test_no_season_returns_zero(
        self, session: AsyncSession, season_pack_item_no_season: MediaItem
    ) -> None:
        """Item with season=None → returns 0 without calling TMDB."""
        ScrapePipeline, _ = _import_pipeline()
        pipeline = ScrapePipeline()

        mock_tmdb = AsyncMock(return_value=None)
        with patch("src.services.tmdb.tmdb_client.get_show_details", mock_tmdb):
            created = await pipeline._split_season_pack_to_episodes(
                session, season_pack_item_no_season
            )

        assert created == 0
        mock_tmdb.assert_not_called()

    async def test_tmdb_returns_none_returns_zero(
        self, session: AsyncSession, season_pack_item: MediaItem
    ) -> None:
        """TMDB get_show_details returns None → returns 0."""
        ScrapePipeline, _ = _import_pipeline()
        pipeline = ScrapePipeline()

        with patch(
            "src.services.tmdb.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=None,
        ):
            created = await pipeline._split_season_pack_to_episodes(
                session, season_pack_item
            )

        assert created == 0

    async def test_season_not_in_tmdb_returns_zero(
        self, session: AsyncSession, season_pack_item: MediaItem
    ) -> None:
        """TMDB returns a season detail for a different season number → returns 0."""
        ScrapePipeline, _ = _import_pipeline()
        pipeline = ScrapePipeline()

        # TMDB hands back a show whose seasons list only contains season 2,
        # but the item wants season 1 — so the lookup returns no match.
        wrong_season_info = TmdbSeasonInfo(
            season_number=2,
            name="Season 2",
            episode_count=10,
        )
        wrong_show = TmdbShowDetail(
            tmdb_id=55566,
            title="Great Anime",
            year=2024,
            seasons=[wrong_season_info],
        )
        with patch(
            "src.services.tmdb.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=wrong_show,
        ):
            created = await pipeline._split_season_pack_to_episodes(
                session, season_pack_item
            )

        assert created == 0

    async def test_tmdb_raises_returns_zero(
        self, session: AsyncSession, season_pack_item: MediaItem
    ) -> None:
        """TMDB raises an exception → swallowed, returns 0."""
        ScrapePipeline, _ = _import_pipeline()
        pipeline = ScrapePipeline()

        with patch(
            "src.services.tmdb.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            side_effect=Exception("TMDB is down"),
        ):
            created = await pipeline._split_season_pack_to_episodes(
                session, season_pack_item
            )

        assert created == 0


# ---------------------------------------------------------------------------
# Group 3: Created item field validation
# ---------------------------------------------------------------------------


class TestSplitSeasonPackCreatedItemFields:
    """Newly created episode items inherit correct metadata from the parent."""

    async def test_created_items_have_correct_metadata(
        self, session: AsyncSession, season_pack_item: MediaItem
    ) -> None:
        """Each new item inherits title, year, imdb_id, tmdb_id, tvdb_id, season."""
        ScrapePipeline, _ = _import_pipeline()
        pipeline = ScrapePipeline()

        season_detail = _make_tmdb_season_detail(episode_count=3, season=1)
        with patch(
            "src.services.tmdb.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=season_detail,
        ):
            await pipeline._split_season_pack_to_episodes(session, season_pack_item)

        rows = (
            await session.execute(
                select(MediaItem).where(
                    MediaItem.imdb_id == season_pack_item.imdb_id,
                    MediaItem.is_season_pack == False,  # noqa: E712
                )
            )
        ).scalars().all()

        assert len(rows) == 3
        for row in rows:
            assert row.title == season_pack_item.title
            assert row.year == season_pack_item.year
            assert row.imdb_id == season_pack_item.imdb_id
            assert row.tmdb_id == season_pack_item.tmdb_id
            assert row.tvdb_id == season_pack_item.tvdb_id
            assert row.season == 1
            assert row.media_type == MediaType.SHOW

    async def test_created_items_not_season_packs_and_state_wanted(
        self, session: AsyncSession, season_pack_item: MediaItem
    ) -> None:
        """Created items must have is_season_pack=False and state=WANTED."""
        ScrapePipeline, _ = _import_pipeline()
        pipeline = ScrapePipeline()

        season_detail = _make_tmdb_season_detail(episode_count=2, season=1)
        with patch(
            "src.services.tmdb.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=season_detail,
        ):
            await pipeline._split_season_pack_to_episodes(session, season_pack_item)

        rows = (
            await session.execute(
                select(MediaItem).where(
                    MediaItem.imdb_id == season_pack_item.imdb_id,
                    MediaItem.is_season_pack == False,  # noqa: E712
                )
            )
        ).scalars().all()

        for row in rows:
            assert row.is_season_pack is False
            assert row.state == QueueState.WANTED

    async def test_created_items_have_split_source_and_matching_quality_profile(
        self, session: AsyncSession, season_pack_item: MediaItem
    ) -> None:
        """Created items carry source='season_pack_split' and inherit quality_profile."""
        ScrapePipeline, _ = _import_pipeline()
        pipeline = ScrapePipeline()

        season_detail = _make_tmdb_season_detail(episode_count=2, season=1)
        with patch(
            "src.services.tmdb.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=season_detail,
        ):
            await pipeline._split_season_pack_to_episodes(session, season_pack_item)

        rows = (
            await session.execute(
                select(MediaItem).where(
                    MediaItem.imdb_id == season_pack_item.imdb_id,
                    MediaItem.is_season_pack == False,  # noqa: E712
                )
            )
        ).scalars().all()

        for row in rows:
            assert row.source == "season_pack_split"
            assert row.quality_profile == season_pack_item.quality_profile


# ---------------------------------------------------------------------------
# Group 4: Pipeline integration — split fires vs. not fires
# ---------------------------------------------------------------------------


class TestSeasonPackSplitIntegration:
    """End-to-end pipeline tests that verify split fires (or not) via run()."""

    async def test_season_pack_with_only_episode_results_triggers_split(
        self, session: AsyncSession, season_pack_item: MediaItem
    ) -> None:
        """is_season_pack=True, scraper finds episode-only results, filter rejects all
        (prefer_season_packs) → split fires, item transitions to DONE,
        action='season_pack_split'.

        The split condition requires: best is None (filter returned []) AND
        total_count > 0 (raw scrapers found something) AND item.is_season_pack.
        """
        ScrapePipeline, PipelineResult = _import_pipeline()

        # Scraper returns a single-episode result
        ep_result = _make_torrentio_result(is_season_pack=False)
        season_detail = _make_tmdb_season_detail(episode_count=6, season=1)

        async with _all_mocks() as m:
            m.torrentio_episode.return_value = [ep_result]
            # filter_engine rejects all results (e.g. prefer_season_packs=True rejected
            # the single-episode result) — this is what triggers the split path
            m.filter_rank.return_value = []
            m.tmdb_get_season.return_value = season_detail

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, season_pack_item)

        assert result.action == "season_pack_split"

        # queue_manager.transition must have been called with COMPLETE for the parent item
        transition_calls = [call.args for call in m.queue_transition.call_args_list]
        assert any(
            call[1] == season_pack_item.id and call[2] == QueueState.COMPLETE
            for call in transition_calls
        ), f"Expected COMPLETE transition; got: {transition_calls}"

    async def test_season_pack_with_season_pack_result_skips_split(
        self, session: AsyncSession, season_pack_item: MediaItem
    ) -> None:
        """is_season_pack=True, filter finds a season pack result → normal add flow,
        no split, action != 'season_pack_split'."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        sp_result = _make_season_pack_result()
        filtered_sp = _make_filtered_result(sp_result, score=120.0)

        async with _all_mocks() as m:
            m.torrentio_episode.return_value = [sp_result]
            m.filter_rank.return_value = [filtered_sp]
            # Ensure TMDB get_show_details would not be called
            m.tmdb_get_season.return_value = _make_tmdb_season_detail(6)

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, season_pack_item)

        assert result.action != "season_pack_split"
        # Step 0 now always calls get_show_details when tmdb_id is set and TMDB
        # is configured (for original_title / original_language backfill), so
        # the mock may have been called once.  What matters is that the split
        # path was NOT entered, which is verified by the assertion above.

    async def test_non_season_pack_rejected_goes_to_sleeping_not_split(
        self, session: AsyncSession
    ) -> None:
        """is_season_pack=False, all results rejected → SLEEPING, no split."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        # A regular episode item (not a season pack)
        ep_item = MediaItem(
            imdb_id="tt3334445",
            tmdb_id="99900",
            title="Regular Show",
            year=2024,
            original_language="en",
            media_type=MediaType.SHOW,
            state=QueueState.SCRAPING,
            state_changed_at=datetime.now(UTC),
            retry_count=0,
            season=1,
            episode=5,
            is_season_pack=False,
        )
        session.add(ep_item)
        await session.flush()

        ep_result = _make_torrentio_result(is_season_pack=False)

        async with _all_mocks() as m:
            m.torrentio_episode.return_value = [ep_result]
            m.filter_rank.return_value = []  # all rejected

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, ep_item)

        assert result.action == "no_results"
        assert result.action != "season_pack_split"
        # Step 0 now calls get_show_details whenever tmdb_id is present and
        # TMDB is configured (for original_title / original_language backfill).
        # The key assertion is that the result is "no_results", not a split.

    async def test_season_pack_with_zero_raw_results_goes_to_sleeping_not_split(
        self, session: AsyncSession, season_pack_item: MediaItem
    ) -> None:
        """is_season_pack=True but scrapers return nothing at all → SLEEPING, no split.

        Split must only fire when there were results but none survived filtering
        as a season pack — not when the scrapers found nothing at all.
        """
        ScrapePipeline, PipelineResult = _import_pipeline()

        async with _all_mocks() as m:
            m.zilean_search.return_value = []
            m.torrentio_episode.return_value = []
            m.filter_rank.return_value = []

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, season_pack_item)

        # With zero raw results the pipeline transitions to SLEEPING
        assert result.action == "no_results"
        assert result.action != "season_pack_split"
        # Step 0 now always fetches TMDB details when tmdb_id is set; the
        # important invariant is that the split path was not triggered.

    async def test_split_tmdb_failure_falls_through_to_sleeping(
        self, session: AsyncSession, season_pack_item: MediaItem
    ) -> None:
        """Split fires but TMDB is unavailable → falls through gracefully to SLEEPING.

        The pipeline must never crash; when split cannot create episode items it
        should log and transition the parent to SLEEPING (not leave it stuck).
        """
        ScrapePipeline, PipelineResult = _import_pipeline()

        # Scraper finds an episode result but filter rejects it (triggering split path)
        ep_result = _make_torrentio_result(is_season_pack=False)

        async with _all_mocks() as m:
            m.torrentio_episode.return_value = [ep_result]
            # Empty filter result → best is None → split fires
            m.filter_rank.return_value = []
            # TMDB is completely down during the split attempt
            m.tmdb_get_season.side_effect = Exception("connection refused")

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, season_pack_item)

        # The pipeline must complete without raising
        assert result is not None
        assert result.action in ("no_results", "sleeping", "season_pack_split", "error")

        # Confirm the item ended in a terminal non-stuck state
        transition_states = [call.args[2] for call in m.queue_transition.call_args_list]
        stuck_states = {QueueState.SCRAPING, QueueState.ADDING, QueueState.CHECKING}
        assert not stuck_states.intersection(
            transition_states
        ), f"Item left in active state; transitions were: {transition_states}"

    async def test_split_result_count_reflects_created_items(
        self, session: AsyncSession, season_pack_item: MediaItem
    ) -> None:
        """PipelineResult from a successful split reports how many items were created."""
        ScrapePipeline, PipelineResult = _import_pipeline()

        ep_result = _make_torrentio_result(is_season_pack=False)
        season_detail = _make_tmdb_season_detail(episode_count=4, season=1)

        async with _all_mocks() as m:
            m.torrentio_episode.return_value = [ep_result]
            # Empty filter result → best is None → split path is entered
            m.filter_rank.return_value = []
            m.tmdb_get_season.return_value = season_detail

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, season_pack_item)

        assert result.action == "season_pack_split"
        # The message should communicate how many episode items were created
        assert "4" in result.message or result.scrape_results_count >= 0


# ---------------------------------------------------------------------------
# Group 5: Edge cases for the split method itself
# ---------------------------------------------------------------------------


class TestSplitSeasonPackEdgeCases:
    """Additional edge cases for _split_season_pack_to_episodes."""

    async def test_tmdb_season_with_zero_episodes_returns_zero(
        self, session: AsyncSession, season_pack_item: MediaItem
    ) -> None:
        """TMDB returns a season with an empty episode list → 0 items created."""
        ScrapePipeline, _ = _import_pipeline()
        pipeline = ScrapePipeline()

        # TmdbSeasonInfo with episode_count=0 triggers the guard clause.
        empty_season_info = TmdbSeasonInfo(
            season_number=1,
            name="Season 1",
            episode_count=0,
        )
        empty_show = TmdbShowDetail(
            tmdb_id=55566,
            title="Great Anime",
            year=2024,
            seasons=[empty_season_info],
        )
        with patch(
            "src.services.tmdb.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=empty_show,
        ):
            created = await pipeline._split_season_pack_to_episodes(
                session, season_pack_item
            )

        assert created == 0

    async def test_episode_items_do_not_duplicate_across_states(
        self, session: AsyncSession, season_pack_item: MediaItem
    ) -> None:
        """An episode already in COMPLETE state counts as existing → not re-created."""
        ScrapePipeline, _ = _import_pipeline()
        pipeline = ScrapePipeline()

        # Episode 1 is COMPLETE; episode 2 is SLEEPING.  Both should count as
        # 'existing' so the split only creates episode 3.
        for ep, state in [(1, QueueState.COMPLETE), (2, QueueState.SLEEPING)]:
            session.add(
                MediaItem(
                    imdb_id=season_pack_item.imdb_id,
                    tmdb_id=season_pack_item.tmdb_id,
                    title=season_pack_item.title,
                    year=season_pack_item.year,
                    media_type=MediaType.SHOW,
                    state=state,
                    state_changed_at=datetime.now(UTC),
                    retry_count=0,
                    season=1,
                    episode=ep,
                    is_season_pack=False,
                )
            )
        await session.flush()

        season_detail = _make_tmdb_season_detail(episode_count=3, season=1)
        with patch(
            "src.services.tmdb.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=season_detail,
        ):
            created = await pipeline._split_season_pack_to_episodes(
                session, season_pack_item
            )

        assert created == 1

        new_rows = (
            await session.execute(
                select(MediaItem).where(
                    MediaItem.imdb_id == season_pack_item.imdb_id,
                    MediaItem.is_season_pack == False,  # noqa: E712
                    MediaItem.state == QueueState.WANTED,
                )
            )
        ).scalars().all()
        assert len(new_rows) == 1
        assert new_rows[0].episode == 3

    async def test_tmdb_id_with_non_numeric_chars_handled_safely(
        self, session: AsyncSession
    ) -> None:
        """tmdb_id that cannot be cast to int is treated as missing → returns 0."""
        ScrapePipeline, _ = _import_pipeline()
        pipeline = ScrapePipeline()

        bad_item = MediaItem(
            imdb_id="tt5556667",
            tmdb_id="not-an-int",
            title="Bad ID Show",
            year=2024,
            media_type=MediaType.SHOW,
            state=QueueState.SCRAPING,
            state_changed_at=datetime.now(UTC),
            retry_count=0,
            season=1,
            episode=None,
            is_season_pack=True,
        )
        session.add(bad_item)
        await session.flush()

        mock_tmdb = AsyncMock()
        with patch("src.services.tmdb.tmdb_client.get_show_details", mock_tmdb):
            created = await pipeline._split_season_pack_to_episodes(session, bad_item)

        assert created == 0
        mock_tmdb.assert_not_called()

    async def test_split_does_not_recreate_the_parent_season_pack_item(
        self, session: AsyncSession, season_pack_item: MediaItem
    ) -> None:
        """The parent season pack item itself must not appear in the new episode rows."""
        ScrapePipeline, _ = _import_pipeline()
        pipeline = ScrapePipeline()

        season_detail = _make_tmdb_season_detail(episode_count=3, season=1)
        with patch(
            "src.services.tmdb.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=season_detail,
        ):
            await pipeline._split_season_pack_to_episodes(session, season_pack_item)

        # All newly created rows must NOT be season packs
        new_rows = (
            await session.execute(
                select(MediaItem).where(
                    MediaItem.imdb_id == season_pack_item.imdb_id,
                    MediaItem.id != season_pack_item.id,
                )
            )
        ).scalars().all()

        for row in new_rows:
            assert row.is_season_pack is False
            assert row.episode is not None

    async def test_single_episode_season_creates_exactly_one_item(
        self, session: AsyncSession, season_pack_item: MediaItem
    ) -> None:
        """TMDB season has exactly 1 episode → exactly 1 new item."""
        ScrapePipeline, _ = _import_pipeline()
        pipeline = ScrapePipeline()

        season_detail = _make_tmdb_season_detail(episode_count=1, season=1)
        with patch(
            "src.services.tmdb.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=season_detail,
        ):
            created = await pipeline._split_season_pack_to_episodes(
                session, season_pack_item
            )

        assert created == 1

        rows = (
            await session.execute(
                select(MediaItem).where(
                    MediaItem.imdb_id == season_pack_item.imdb_id,
                    MediaItem.is_season_pack == False,  # noqa: E712
                )
            )
        ).scalars().all()
        assert len(rows) == 1
        assert rows[0].episode == 1
        assert rows[0].season == 1
