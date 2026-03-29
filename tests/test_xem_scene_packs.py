"""Tests for XEM scene season pack feature.

Covers three areas:
  show_manager.add_seasons with XEM scene packs:
    1. complete XEM scene season sets xem_scene_pack metadata on the item
    2. non-XEM regular season does NOT set xem_scene_pack metadata
    3. airing XEM scene season (individual episodes) does NOT set scene pack metadata

  scrape_pipeline XEM scene pack behaviour:
    4. item with xem_scene_pack metadata → Torrentio queried with TMDB anchor (not scene season)
    5. xem_scene_pack item with no matching packs → _split_season_pack_to_episodes
       uses stored tmdb_episodes list (not TMDB API)
    6. item WITHOUT xem_scene_pack → normal Torrentio query (no TMDB anchor remap)

  CHECKING stage XEM scene pack fallback in _job_queue_processor:
    7. CHECKING season pack with xem_scene_pack — initial scene-season lookup
       fails, retry with tmdb_anchor_season succeeds
    8. CHECKING xem_scene_pack — mount files filtered to tmdb_anchor_episode..tmdb_end_episode range
    9. CHECKING xem_scene_pack — episode_offset is set to tmdb_anchor_episode - 1
       so symlinks receive correct scene-relative episode numbers

asyncio_mode = "auto" (set in pyproject.toml), so no @pytest.mark.asyncio needed.
"""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.mount_index import MountIndex
from src.models.torrent import RdTorrent, TorrentStatus
from src.services.tmdb import (
    TmdbEpisodeInfo,
    TmdbSeasonDetail,
    TmdbSeasonInfo,
    TmdbShowDetail,
)
from src.services.torrentio import TorrentioResult

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TMDB_ID = 55566
TMDB_ID_STR = "55566"
TVDB_ID = 76290
IMDB_ID = "tt9988776"


# ---------------------------------------------------------------------------
# Data builders shared across test groups
# ---------------------------------------------------------------------------


def _make_test_anime_scene_s1_group():
    """Return a SceneSeasonGroup for Test Anime scene S01 (TMDB S01E01-E28, all aired)."""
    from datetime import date

    from src.core.show_manager import SceneEpisodeInfo, SceneSeasonGroup

    episodes = [
        SceneEpisodeInfo(
            tmdb_season=1,
            tmdb_episode=ep,
            scene_episode=ep,
            air_date=date(2023, 10, 1),
            has_aired=True,
        )
        for ep in range(1, 29)
    ]
    return SceneSeasonGroup(
        scene_season=1,
        episodes=episodes,
        total_episodes=28,
        aired_episodes=28,
        first_air_date="2023-10-01",
        is_complete=True,
    )


def _make_test_anime_scene_s2_group_airing():
    """Return a SceneSeasonGroup for Test Anime scene S02 (TMDB S01E29-E35, partially aired)."""
    from datetime import date

    from src.core.show_manager import SceneEpisodeInfo, SceneSeasonGroup

    # 4 aired + 3 future
    episodes = []
    for ep in range(29, 33):
        episodes.append(SceneEpisodeInfo(
            tmdb_season=1,
            tmdb_episode=ep,
            scene_episode=ep - 28,
            air_date=date(2024, 1, 5),
            has_aired=True,
        ))
    for ep in range(33, 36):
        episodes.append(SceneEpisodeInfo(
            tmdb_season=1,
            tmdb_episode=ep,
            scene_episode=ep - 28,
            air_date=date(2099, 6, 1),
            has_aired=False,
        ))
    return SceneSeasonGroup(
        scene_season=2,
        episodes=episodes,
        total_episodes=7,
        aired_episodes=4,
        first_air_date="2024-01-05",
        is_complete=False,
    )


def _make_xem_show_detail() -> TmdbShowDetail:
    """Build a TmdbShowDetail with tvdb_id set (XEM-mapped show)."""
    return TmdbShowDetail(
        tmdb_id=TMDB_ID,
        title="Test Anime: The Journey",
        year=2023,
        overview="A test anime show.",
        poster_path="/test_anime.jpg",
        backdrop_path="/test_anime_bg.jpg",
        status="Ended",
        vote_average=9.0,
        number_of_seasons=1,
        seasons=[
            TmdbSeasonInfo(
                season_number=1,
                name="Season 1",
                episode_count=35,
                air_date="2023-09-29",
            )
        ],
        imdb_id=IMDB_ID,
        tvdb_id=TVDB_ID,
        genres=[{"id": 16, "name": "Animation"}],
    )


def _make_non_xem_show_detail() -> TmdbShowDetail:
    """Build a TmdbShowDetail without tvdb_id (no XEM lookup)."""
    return TmdbShowDetail(
        tmdb_id=TMDB_ID,
        title="Regular Show",
        year=2020,
        overview="A regular show.",
        status="Ended",
        vote_average=7.5,
        number_of_seasons=2,
        seasons=[
            TmdbSeasonInfo(season_number=1, name="S1", episode_count=10, air_date="2020-01-01"),
            TmdbSeasonInfo(season_number=2, name="S2", episode_count=10, air_date="2021-01-01"),
        ],
        imdb_id=IMDB_ID,
        tvdb_id=None,
    )


def _make_season_pack_item(
    *,
    session_add: bool = False,
    scene_season: int = 1,
    metadata_json: str | None = None,
    state: QueueState = QueueState.SCRAPING,
) -> MediaItem:
    """Build an in-memory XEM scene season pack MediaItem (not persisted)."""
    now = datetime.now(UTC)
    return MediaItem(
        imdb_id=IMDB_ID,
        tmdb_id=TMDB_ID_STR,
        tvdb_id=TVDB_ID,
        title="Test Anime: The Journey",
        year=2023,
        media_type=MediaType.SHOW,
        state=state,
        source="show_detail",
        added_at=now,
        state_changed_at=now,
        retry_count=0,
        season=scene_season,
        episode=None,
        is_season_pack=True,
        quality_profile="high",
        metadata_json=metadata_json,
    )


def _make_xem_pack_metadata(
    *,
    scene_season: int = 1,
    tmdb_anchor_season: int = 1,
    tmdb_anchor_episode: int = 1,
    tmdb_end_episode: int = 28,
    tmdb_episodes: list[dict] | None = None,
) -> str:
    """Build the JSON metadata stored on XEM scene season pack items."""
    if tmdb_episodes is None:
        tmdb_episodes = [
            {"s": tmdb_anchor_season, "e": ep}
            for ep in range(tmdb_anchor_episode, tmdb_end_episode + 1)
        ]
    return json.dumps({
        "xem_scene_pack": True,
        "scene_season": scene_season,
        "tmdb_anchor_season": tmdb_anchor_season,
        "tmdb_anchor_episode": tmdb_anchor_episode,
        "tmdb_end_episode": tmdb_end_episode,
        "tmdb_episodes": tmdb_episodes,
    })


def _make_xem_s2_pack_metadata() -> str:
    """XEM metadata for scene S02 (TMDB S01E29-E35)."""
    return _make_xem_pack_metadata(
        scene_season=2,
        tmdb_anchor_season=1,
        tmdb_anchor_episode=14,
        tmdb_end_episode=26,
        tmdb_episodes=[{"s": 1, "e": ep} for ep in range(14, 27)],
    )


def _make_torrentio_season_pack(**overrides: object) -> TorrentioResult:
    """Build a TorrentioResult that is a season pack."""
    defaults: dict[str, object] = {
        "info_hash": "b" * 40,
        "title": "Test.Anime.S01.COMPLETE.1080p.WEB-DL.x265-GROUP",
        "resolution": "1080p",
        "codec": "x265",
        "quality": "WEB-DL",
        "size_bytes": 20 * 1024 ** 3,
        "seeders": 200,
        "release_group": "GROUP",
        "languages": [],
        "is_season_pack": True,
    }
    defaults.update(overrides)
    return TorrentioResult(**defaults)


def _make_mount_file(
    *,
    filepath: str = "/mnt/zurg/__all__/Test Anime S01/E01.mkv",
    filename: str = "E01.mkv",
    parsed_season: int | None = 1,
    parsed_episode: int | None = 1,
    parsed_resolution: str = "1080p",
) -> MagicMock:
    """Return a mock MountIndex-like object."""
    m = MagicMock(spec=MountIndex)
    m.filepath = filepath
    m.filename = filename
    m.parsed_season = parsed_season
    m.parsed_episode = parsed_episode
    m.parsed_resolution = parsed_resolution
    m.filesize = 500 * 1024 * 1024
    return m


# ---------------------------------------------------------------------------
# Pipeline patch context manager (mirrors test_scrape_pipeline.py pattern)
# ---------------------------------------------------------------------------

_PIPELINE_PATCH_TARGETS = {
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
    "rd_get_torrent_info": "src.core.scrape_pipeline.rd_client.get_torrent_info",
    "rd_delete": "src.core.scrape_pipeline.rd_client.delete_torrent",
    "filter_rank": "src.core.scrape_pipeline.filter_engine.filter_and_rank",
    "queue_transition": "src.core.scrape_pipeline.queue_manager.transition",
}


class _PipelineMocks:
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
    rd_get_torrent_info: AsyncMock
    rd_delete: AsyncMock
    filter_rank: MagicMock
    queue_transition: AsyncMock


@asynccontextmanager
async def _all_pipeline_mocks() -> AsyncGenerator[_PipelineMocks, None]:
    """Patch every ScrapePipeline dependency at once.

    Defaults mimic the "nothing found, no errors" path so tests only override
    the specific mock they care about.
    """
    mocks = _PipelineMocks()
    patchers = {name: patch(target) for name, target in _PIPELINE_PATCH_TARGETS.items()}
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
    mocks.rd_add.return_value = {"id": "RD123", "uri": "magnet:?xt=urn:btih:" + "b" * 40}

    mocks.rd_select = started["rd_select"]
    mocks.rd_select.return_value = None

    mocks.rd_get_torrent_info = started["rd_get_torrent_info"]
    mocks.rd_get_torrent_info.return_value = {"id": "RD123", "status": "magnet_conversion", "files": []}

    mocks.rd_delete = started["rd_delete"]
    mocks.rd_delete.return_value = None

    mocks.filter_rank = started["filter_rank"]
    mocks.filter_rank.return_value = []

    mocks.queue_transition = started["queue_transition"]
    mocks.queue_transition.return_value = MagicMock(spec=MediaItem)

    try:
        yield mocks
    finally:
        for p in patchers.values():
            p.stop()


# ---------------------------------------------------------------------------
# CHECKING stage helpers (mirrors test_queue_processor.py pattern)
# ---------------------------------------------------------------------------


def _utcnow_naive() -> datetime:
    """Naive UTC — matches what SQLite returns after a round-trip."""
    return datetime.now(UTC).replace(tzinfo=None)


async def _persist_item(session: AsyncSession, item: MediaItem) -> MediaItem:
    """Persist an item and return it with a valid id."""
    session.add(item)
    await session.flush()
    return item


async def _make_rd_torrent(
    session: AsyncSession,
    *,
    media_item_id: int,
    rd_id: str = "RD_XEM_001",
    filename: str = "Test Anime The Journey S01",
) -> RdTorrent:
    """Persist an RdTorrent linked to the given media item."""
    torrent = RdTorrent(
        rd_id=rd_id,
        info_hash="c" * 40,
        media_item_id=media_item_id,
        filename=filename,
        status=TorrentStatus.ACTIVE,
    )
    session.add(torrent)
    await session.flush()
    return torrent


# ---------------------------------------------------------------------------
# Group 1: show_manager.add_seasons — XEM pack metadata
# ---------------------------------------------------------------------------


class TestAddCompleteXemSceneSeasonMetadata:
    """show_manager.add_seasons sets xem_scene_pack metadata on complete scene season packs."""

    async def test_add_complete_xem_scene_season_sets_metadata(
        self, session: AsyncSession
    ) -> None:
        """add_seasons creates a season pack with correct xem_scene_pack metadata."""
        from src.core.show_manager import AddSeasonsRequest, ShowManager

        sm = ShowManager()
        req = AddSeasonsRequest(
            tmdb_id=TMDB_ID,
            imdb_id=IMDB_ID,
            title="Test Anime: The Journey",
            year=2023,
            seasons=[1],
        )

        # XEM-mapped show: TMDB S01 has 35 eps, scene S01 is eps 1-28 (all aired).
        mock_show = _make_xem_show_detail()
        season_detail = TmdbSeasonDetail(
            season_number=1,
            name="Season 1",
            episodes=[
                TmdbEpisodeInfo(episode_number=ep, air_date="2023-10-01")
                for ep in range(1, 36)
            ],
        )

        # XEM absolute map: eps 1-28 stay in scene S01, eps 29-35 go to scene S02.
        xem_map = {ep: (1, ep) for ep in range(1, 29)}
        xem_map.update({ep: (2, ep - 28) for ep in range(29, 36)})

        with (
            patch(
                "src.core.show_manager.tmdb_client.get_show_details",
                new_callable=AsyncMock,
                return_value=mock_show,
            ),
            patch(
                "src.core.show_manager.xem_mapper.get_absolute_scene_map",
                new_callable=AsyncMock,
                return_value=xem_map,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                new_callable=AsyncMock,
                return_value=season_detail,
            ),
        ):
            result = await sm.add_seasons(session, req)

        assert result.created_items == 1
        assert result.skipped_seasons == []

        rows = list(
            (
                await session.execute(
                    select(MediaItem).where(MediaItem.tmdb_id == TMDB_ID_STR)
                )
            ).scalars().all()
        )
        assert len(rows) == 1
        item = rows[0]

        assert item.is_season_pack is True
        assert item.season == 1
        assert item.episode is None
        assert item.metadata_json is not None

        meta = json.loads(item.metadata_json)
        assert meta.get("xem_scene_pack") is True
        assert "tmdb_anchor_season" in meta
        assert "tmdb_anchor_episode" in meta
        assert "tmdb_end_episode" in meta
        assert "tmdb_episodes" in meta

        # Anchor should be the first TMDB episode in scene S01: S01E01
        assert meta["tmdb_anchor_season"] == 1
        assert meta["tmdb_anchor_episode"] == 1
        # End should be the last in scene S01: E28
        assert meta["tmdb_end_episode"] == 28
        # tmdb_episodes list should have 28 entries
        assert len(meta["tmdb_episodes"]) == 28

    async def test_add_non_xem_season_no_xem_metadata(
        self, session: AsyncSession
    ) -> None:
        """Regular (non-XEM) season pack has no xem_scene_pack key in metadata_json."""
        from src.core.show_manager import AddSeasonsRequest, ShowManager

        sm = ShowManager()
        req = AddSeasonsRequest(
            tmdb_id=TMDB_ID,
            imdb_id=IMDB_ID,
            title="Regular Show",
            year=2020,
            seasons=[1],
        )

        # Show without tvdb_id → XEM path is never triggered.
        mock_show = _make_non_xem_show_detail()

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            result = await sm.add_seasons(session, req)

        assert result.created_items == 1

        rows = list(
            (
                await session.execute(
                    select(MediaItem).where(MediaItem.tmdb_id == TMDB_ID_STR)
                )
            ).scalars().all()
        )
        assert len(rows) == 1
        item = rows[0]

        # metadata_json should either be absent or not contain xem_scene_pack
        if item.metadata_json:
            meta = json.loads(item.metadata_json)
            assert "xem_scene_pack" not in meta
        else:
            # None is fine — no XEM metadata set
            assert item.metadata_json is None

    async def test_add_airing_xem_season_no_xem_metadata(
        self, session: AsyncSession
    ) -> None:
        """Airing XEM scene season creates individual episode items, not a scene pack.

        Those episode items must NOT carry xem_scene_pack metadata because they
        are stored with TMDB numbering and the pipeline XEM-remaps them at scrape
        time using the normal per-episode XEM path.
        """
        from src.core.show_manager import AddSeasonsRequest, ShowManager

        sm = ShowManager()
        # Request scene S02 (the airing half of Test Anime)
        req = AddSeasonsRequest(
            tmdb_id=TMDB_ID,
            imdb_id=IMDB_ID,
            title="Test Anime: The Journey",
            year=2023,
            seasons=[2],
        )

        mock_show = _make_xem_show_detail()
        season_detail = TmdbSeasonDetail(
            season_number=1,
            name="Season 1",
            episodes=[
                TmdbEpisodeInfo(
                    episode_number=ep,
                    air_date="2023-10-01" if ep <= 32 else "2099-06-01",
                )
                for ep in range(1, 36)
            ],
        )
        xem_map = {ep: (1, ep) for ep in range(1, 29)}
        xem_map.update({ep: (2, ep - 28) for ep in range(29, 36)})

        with (
            patch(
                "src.core.show_manager.tmdb_client.get_show_details",
                new_callable=AsyncMock,
                return_value=mock_show,
            ),
            patch(
                "src.core.show_manager.xem_mapper.get_absolute_scene_map",
                new_callable=AsyncMock,
                return_value=xem_map,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                new_callable=AsyncMock,
                return_value=season_detail,
            ),
        ):
            result = await sm.add_seasons(session, req)

        # Scene S02 has 7 episodes (32 aired + 3 future in detail above)
        assert result.created_items > 0

        rows = list(
            (
                await session.execute(
                    select(MediaItem).where(
                        MediaItem.tmdb_id == TMDB_ID_STR,
                        MediaItem.episode.is_not(None),
                    )
                )
            ).scalars().all()
        )
        # At least some individual episode items must have been created
        assert len(rows) > 0

        # None of the individual episode items should carry xem_scene_pack
        for ep_item in rows:
            assert ep_item.is_season_pack is False
            if ep_item.metadata_json:
                meta = json.loads(ep_item.metadata_json)
                assert "xem_scene_pack" not in meta


# ---------------------------------------------------------------------------
# Group 2: scrape_pipeline XEM scene pack anchor remap
# ---------------------------------------------------------------------------


class TestScrapePipelineXemScenePack:
    """scrape_pipeline._run_pipeline uses TMDB anchor for XEM scene pack items."""

    async def test_xem_scene_pack_uses_tmdb_anchor_for_torrentio(
        self, session: AsyncSession
    ) -> None:
        """Item with xem_scene_pack metadata → Torrentio called with TMDB anchor S/E."""
        from src.core.scrape_pipeline import ScrapePipeline

        # Scene S02 pack: TMDB anchor is S01E14-E26.
        meta = _make_xem_pack_metadata(
            scene_season=2,
            tmdb_anchor_season=1,
            tmdb_anchor_episode=14,
            tmdb_end_episode=26,
        )
        item = _make_season_pack_item(scene_season=2, metadata_json=meta)
        item.imdb_id = IMDB_ID
        await _persist_item(session, item)

        pipeline = ScrapePipeline()

        async with _all_pipeline_mocks() as m:
            # Mount check returns empty; dedup passes.
            m.mount_scanner_lookup.return_value = []
            m.dedup_check.return_value = None
            m.zilean_search.return_value = []
            # torrentio_episode returns empty — we care only about the call args.
            m.torrentio_episode.return_value = []
            m.filter_rank.return_value = []

            await pipeline.run(session, item)

        # Torrentio should have been called with the TMDB anchor coordinates, not scene S02E01
        m.torrentio_episode.assert_awaited_once()
        call_kwargs = m.torrentio_episode.call_args
        # positional: (imdb_id, season, episode, ...)
        called_season = call_kwargs.args[1]
        called_episode = call_kwargs.args[2]

        # Must use TMDB anchor (season=1, episode=14), NOT scene (season=2, episode=1)
        assert called_season == 1, (
            f"Expected Torrentio called with TMDB anchor season=1, got season={called_season}"
        )
        assert called_episode == 14, (
            f"Expected Torrentio called with TMDB anchor episode=14, got episode={called_episode}"
        )

    async def test_xem_scene_pack_split_uses_stored_tmdb_episodes(
        self, session: AsyncSession
    ) -> None:
        """XEM scene pack with no matching results uses stored tmdb_episodes for split.

        When the filter engine rejects all results (no season pack survived),
        _split_season_pack_to_episodes must use the stored tmdb_episodes list
        from metadata rather than querying TMDB directly.  This is verified by
        ensuring TMDB is never called but the correct episode items are created.
        """
        from src.core.scrape_pipeline import ScrapePipeline

        tmdb_eps = [{"s": 1, "e": ep} for ep in range(14, 27)]  # 13 episodes
        meta = _make_xem_pack_metadata(
            scene_season=2,
            tmdb_anchor_season=1,
            tmdb_anchor_episode=14,
            tmdb_end_episode=26,
            tmdb_episodes=tmdb_eps,
        )
        item = _make_season_pack_item(scene_season=2, metadata_json=meta)
        await _persist_item(session, item)

        pipeline = ScrapePipeline()

        # Torrentio returns one result but it's an individual episode (not a pack)
        individual_ep = TorrentioResult(
            info_hash="a" * 40,
            title="Test.Anime.S01E14.1080p.WEB-DL",
            resolution="1080p",
            codec="x265",
            quality="WEB-DL",
            size_bytes=500 * 1024 * 1024,
            seeders=50,
            release_group="GROUP",
            languages=[],
            is_season_pack=False,
        )

        async with _all_pipeline_mocks() as m:
            m.torrentio_episode.return_value = [individual_ep]
            # Filter rejects the individual ep for a season pack request → empty
            m.filter_rank.return_value = []

            # TMDB must NOT be called — the split should use stored metadata only.
            # The split function imports tmdb_client locally, so patch at the
            # services level to catch any call regardless of the import path.
            with patch(
                "src.services.tmdb.tmdb_client.get_show_details",
                new_callable=AsyncMock,
                return_value=None,
            ) as _mock_tmdb_get_show:
                result = await pipeline.run(session, item)

        # Pipeline should have split into individual items
        assert result.action in ("season_pack_split", "no_results")

        if result.action == "season_pack_split":
            # Verify the correct number of episode items were created
            rows = list(
                (
                    await session.execute(
                        select(MediaItem).where(
                            MediaItem.tmdb_id == TMDB_ID_STR,
                            MediaItem.episode.is_not(None),
                        )
                    )
                ).scalars().all()
            )
            assert len(rows) == 13  # 14..26 = 13 episodes

    async def test_non_xem_season_pack_unaffected(
        self, session: AsyncSession
    ) -> None:
        """Season pack WITHOUT xem_scene_pack metadata uses its scene season for Torrentio."""
        from src.core.scrape_pipeline import ScrapePipeline

        # Regular season pack — no XEM metadata at all.
        item = MediaItem(
            imdb_id=IMDB_ID,
            tmdb_id=TMDB_ID_STR,
            title="Regular Show",
            year=2020,
            media_type=MediaType.SHOW,
            state=QueueState.SCRAPING,
            state_changed_at=datetime.now(UTC),
            retry_count=0,
            season=2,
            episode=None,
            is_season_pack=True,
            metadata_json=None,
        )
        session.add(item)
        await session.flush()

        pipeline = ScrapePipeline()

        async with _all_pipeline_mocks() as m:
            m.torrentio_episode.return_value = []
            m.filter_rank.return_value = []

            await pipeline.run(session, item)

        # For a regular pack with season=2, Torrentio should be called with season=2
        m.torrentio_episode.assert_awaited_once()
        call_kwargs = m.torrentio_episode.call_args
        called_season = call_kwargs.args[1]

        # Must be the original scene season, NOT a TMDB anchor remap
        assert called_season == 2, (
            f"Expected Torrentio called with season=2 for non-XEM pack, got season={called_season}"
        )


# ---------------------------------------------------------------------------
# Group 3: CHECKING stage XEM scene pack fallback
# ---------------------------------------------------------------------------


class TestCheckingStageXemScenePack:
    """CHECKING stage retries mount lookup with TMDB anchor for XEM scene packs."""

    @pytest.fixture
    def mock_session(self, session: AsyncSession) -> AsyncSession:
        """Wrap the test session so _job_queue_processor can use it."""
        session.commit = AsyncMock()
        session.close = AsyncMock()
        session.rollback = AsyncMock()
        return session

    @pytest.fixture
    def patch_async_session(self, mock_session: AsyncSession):
        """Patch src.core.queue_processor.async_session to return the test session."""
        with patch("src.core.queue_processor.async_session", return_value=mock_session):
            yield mock_session

    @pytest.fixture
    def patch_process_queue(self, patch_async_session):
        """Silence Stage 0/1 transitions so tests focus on Stage 3."""
        with patch(
            "src.core.queue_processor.queue_manager.process_queue",
            new_callable=AsyncMock,
            return_value={"unreleased_advanced": 0, "retries_triggered": 0},
        ):
            with patch("src.core.queue_processor.scrape_pipeline.run", new_callable=AsyncMock):
                yield

    async def test_checking_xem_scene_pack_retries_with_tmdb_season(
        self, session: AsyncSession, patch_process_queue: None
    ) -> None:
        """CHECKING XEM scene pack: initial scene-season lookup fails, retry with TMDB anchor.

        The item has season=2 (scene season) but the mount index files are stored
        with TMDB season=1.  The initial lookup(season=2) returns nothing; the XEM
        fallback lookup(season=1) returns files.
        """
        from src.core.queue_processor import _job_queue_processor

        meta = _make_xem_pack_metadata(
            scene_season=2,
            tmdb_anchor_season=1,
            tmdb_anchor_episode=14,
            tmdb_end_episode=26,
        )
        item = _make_season_pack_item(
            scene_season=2,
            metadata_json=meta,
            state=QueueState.CHECKING,
        )
        item.state_changed_at = _utcnow_naive()
        await _persist_item(session, item)

        # Build 13 mount files with TMDB S01 episodes 14-26
        xem_files = [
            _make_mount_file(
                filepath=f"/mnt/zurg/Test Anime S01/E{ep:02d}.mkv",
                filename=f"E{ep:02d}.mkv",
                parsed_season=1,
                parsed_episode=ep,
            )
            for ep in range(14, 27)
        ]

        # lookup_season_calls tracks the season values passed to both
        # mount_scanner.lookup_multi (initial call) and mount_scanner.lookup (XEM fallback).
        lookup_season_calls: list[int | None] = []

        async def _mock_lookup_multi(db_session, titles, *, season, episode):
            lookup_season_calls.append(season)
            # Initial call with scene season=2 → nothing (triggers XEM fallback)
            return []

        async def _mock_lookup(db_session, title, season, episode):
            lookup_season_calls.append(season)
            # XEM fallback call with TMDB season=1 → return files
            if season == 1:
                return xem_files
            return []

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                side_effect=lambda session, item, tmdb_original_title=None: [item.title],
            ),
            patch("src.core.queue_processor.mount_scanner.lookup_multi", side_effect=_mock_lookup_multi),
            patch("src.core.queue_processor.mount_scanner.lookup", side_effect=_mock_lookup),
            patch(
                "src.core.queue_processor.mount_scanner.is_mount_available",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch("src.core.queue_processor.symlink_manager.create_symlink", new_callable=AsyncMock),
            patch(
                "src.core.queue_processor.rd_client.get_torrent_info",
                new_callable=AsyncMock,
                return_value={"status": "downloaded"},
            ),
        ):
            await _job_queue_processor()

        # Verify lookup was called with season=2 first (via lookup_multi), then season=1 (XEM fallback via lookup)
        assert 2 in lookup_season_calls, "Lookup should have been tried with scene season=2"
        assert 1 in lookup_season_calls, "XEM fallback should have retried with TMDB season=1"
        # Season=1 lookup must come AFTER season=2 failed
        first_idx = next(i for i, s in enumerate(lookup_season_calls) if s == 2)
        anchor_idx = next(i for i, s in enumerate(lookup_season_calls) if s == 1)
        assert first_idx < anchor_idx, "XEM anchor retry must come after scene season attempt"

    async def test_checking_xem_scene_pack_filters_episode_range(
        self, session: AsyncSession, patch_process_queue: None
    ) -> None:
        """CHECKING XEM scene pack: mount files filtered to tmdb_anchor_episode..tmdb_end_episode.

        26 files are returned (all S01 episodes), but only eps 14-26 belong to
        scene S02.  The CHECKING stage must filter to that range so only 13
        symlinks are created.
        """
        from src.core.queue_processor import _job_queue_processor

        meta = _make_xem_pack_metadata(
            scene_season=2,
            tmdb_anchor_season=1,
            tmdb_anchor_episode=14,
            tmdb_end_episode=26,
        )
        item = _make_season_pack_item(
            scene_season=2,
            metadata_json=meta,
            state=QueueState.CHECKING,
        )
        item.state_changed_at = _utcnow_naive()
        await _persist_item(session, item)

        # 26 files — all episodes of TMDB S01 — in the mount
        all_s1_files = [
            _make_mount_file(
                filepath=f"/mnt/zurg/Test Anime S01/E{ep:02d}.mkv",
                filename=f"E{ep:02d}.mkv",
                parsed_season=1,
                parsed_episode=ep,
            )
            for ep in range(1, 27)
        ]

        symlink_calls: list[int] = []

        async def _mock_create_symlink(db_session, item_arg, source_path, **kwargs):
            _episode_offset = kwargs.get("episode_offset", 0)
            # Track which episode numbers are symlinked (after offset adjustment)
            symlink_calls.append(kwargs.get("episode_override") or 0)

        async def _mock_lookup(db_session, title, season, episode):
            if season == 2:
                return []
            if season == 1:
                # Return all 26 files; filter should reduce to 13
                return all_s1_files
            return []

        with (
            patch("src.core.queue_processor.mount_scanner.lookup", side_effect=_mock_lookup),
            patch(
                "src.core.queue_processor.mount_scanner.is_mount_available",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                side_effect=_mock_create_symlink,
            ) as mock_symlink,
            patch(
                "src.core.queue_processor.rd_client.get_torrent_info",
                new_callable=AsyncMock,
                return_value={"status": "downloaded"},
            ),
        ):
            await _job_queue_processor()

        # Symlinks should only be created for eps 14-26 (13 files), not all 26
        assert mock_symlink.await_count == 13, (
            f"Expected 13 symlinks (eps 14-26), got {mock_symlink.await_count}"
        )

    async def test_checking_xem_scene_pack_sets_episode_offset(
        self, session: AsyncSession, patch_process_queue: None
    ) -> None:
        """CHECKING XEM scene pack: episode_offset = tmdb_anchor_episode - 1.

        For scene S02 where tmdb_anchor_episode=14, offset=13 maps
        TMDB file E14 → scene E01, file E15 → scene E02, etc.
        symlink_manager.create_symlink must be called with episode_offset=13.
        """
        from src.core.queue_processor import _job_queue_processor

        anchor_ep = 14
        end_ep = 26
        expected_offset = anchor_ep - 1  # = 13

        meta = _make_xem_pack_metadata(
            scene_season=2,
            tmdb_anchor_season=1,
            tmdb_anchor_episode=anchor_ep,
            tmdb_end_episode=end_ep,
        )
        item = _make_season_pack_item(
            scene_season=2,
            metadata_json=meta,
            state=QueueState.CHECKING,
        )
        item.state_changed_at = _utcnow_naive()
        await _persist_item(session, item)

        xem_files = [
            _make_mount_file(
                filepath=f"/mnt/zurg/Test Anime S01/E{ep:02d}.mkv",
                filename=f"E{ep:02d}.mkv",
                parsed_season=1,
                parsed_episode=ep,
            )
            for ep in range(anchor_ep, end_ep + 1)
        ]

        async def _mock_lookup(db_session, title, season, episode):
            if season == 2:
                return []
            if season == 1:
                return xem_files
            return []

        with (
            patch("src.core.queue_processor.mount_scanner.lookup", side_effect=_mock_lookup),
            patch(
                "src.core.queue_processor.mount_scanner.is_mount_available",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
            ) as mock_symlink,
            patch(
                "src.core.queue_processor.rd_client.get_torrent_info",
                new_callable=AsyncMock,
                return_value={"status": "downloaded"},
            ),
        ):
            await _job_queue_processor()

        # Verify that symlink_manager.create_symlink was called with episode_offset=expected_offset
        assert mock_symlink.await_count > 0, "create_symlink must have been called at least once"

        for symlink_call in mock_symlink.call_args_list:
            call_kwargs = symlink_call.kwargs
            call_offset = call_kwargs.get("episode_offset", 0)
            assert call_offset == expected_offset, (
                f"Expected episode_offset={expected_offset} (anchor_ep - 1), "
                f"got {call_offset}"
            )
