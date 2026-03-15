"""Tests for the season pack feature in vibeDebrid.

Covers:
  - MediaItem model: is_season_pack field defaults and persistence
  - AddRequest schema: is_season_pack field validation and defaults
  - SearchResultItem schema: season, episode, is_season_pack fields
  - POST /api/add: season pack sets episode=None on MediaItem
  - POST /api/add: non-season-pack preserves episode on MediaItem
  - scrape_pipeline: uses episode=1 as anchor when item.episode is None
  - scrape_pipeline: does not skip shows that have season but no episode

asyncio_mode = "auto" is set in pyproject.toml so no @pytest.mark.asyncio needed.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from sqlalchemy import select

from src.api.deps import get_db
from src.api.routes.search import AddRequest, SearchResultItem
from src.main import app
from src.models.media_item import MediaItem, MediaType, QueueState
from src.services.torrentio import TorrentioResult


# ---------------------------------------------------------------------------
# Fixtures (local — shared fixtures come from conftest.py)
# ---------------------------------------------------------------------------


@pytest.fixture
def override_db(session: AsyncSession):
    """Override the FastAPI get_db dependency with the test session."""

    async def _override() -> AsyncSession:
        yield session

    app.dependency_overrides[get_db] = _override
    yield
    app.dependency_overrides.clear()


@pytest.fixture
async def http(override_db) -> AsyncClient:
    """Async HTTP client backed by the FastAPI test app with DB wired."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


def _valid_hash(char: str = "a") -> str:
    """Return a valid 40-character hex info hash using the given character."""
    return char * 40


def _patch_add_torrent_externals(rd_id: str = "RDSP001"):
    """Context-manager stack that mocks all external calls in the add endpoint."""
    return (
        patch(
            "src.api.routes.search.rd_client.add_magnet",
            new_callable=AsyncMock,
            return_value={"id": rd_id, "uri": f"https://rd/{rd_id}"},
        ),
        patch(
            "src.api.routes.search.rd_client.select_files",
            new_callable=AsyncMock,
        ),
        patch(
            "src.api.routes.search.dedup_engine.register_torrent",
            new_callable=AsyncMock,
        ),
    )


# ===========================================================================
# Test 1 — MediaItem model: is_season_pack field
# ===========================================================================


class TestMediaItemSeasonPackField:
    """Verify is_season_pack is persisted correctly by the ORM model."""

    async def test_default_is_false(self, session: AsyncSession) -> None:
        """A MediaItem created without is_season_pack defaults to False."""
        item = MediaItem(
            title="Test Show",
            media_type=MediaType.SHOW,
            state=QueueState.WANTED,
            state_changed_at=datetime.now(timezone.utc),
            retry_count=0,
        )
        session.add(item)
        await session.flush()

        assert item.is_season_pack is False

    async def test_explicit_false_stored(self, session: AsyncSession) -> None:
        """Explicitly setting is_season_pack=False is stored as False."""
        item = MediaItem(
            title="Test Show",
            media_type=MediaType.SHOW,
            state=QueueState.WANTED,
            state_changed_at=datetime.now(timezone.utc),
            retry_count=0,
            is_season_pack=False,
        )
        session.add(item)
        await session.flush()

        assert item.is_season_pack is False

    async def test_season_pack_flag_persisted(self, session: AsyncSession) -> None:
        """is_season_pack=True is stored and readable from the session."""
        item = MediaItem(
            title="Breaking Bad Season 1",
            media_type=MediaType.SHOW,
            state=QueueState.WANTED,
            state_changed_at=datetime.now(timezone.utc),
            retry_count=0,
            season=1,
            episode=None,
            is_season_pack=True,
        )
        session.add(item)
        await session.flush()

        assert item.is_season_pack is True

    async def test_season_pack_episode_can_be_none(self, session: AsyncSession) -> None:
        """A season pack item is valid with episode=None and season set."""
        item = MediaItem(
            title="The Wire Season 2",
            media_type=MediaType.SHOW,
            state=QueueState.WANTED,
            state_changed_at=datetime.now(timezone.utc),
            retry_count=0,
            season=2,
            episode=None,
            is_season_pack=True,
        )
        session.add(item)
        await session.flush()

        assert item.season == 2
        assert item.episode is None
        assert item.is_season_pack is True

    async def test_non_season_pack_has_episode(self, session: AsyncSession) -> None:
        """A regular episode item has both season and episode set."""
        item = MediaItem(
            title="The Wire",
            media_type=MediaType.SHOW,
            state=QueueState.WANTED,
            state_changed_at=datetime.now(timezone.utc),
            retry_count=0,
            season=2,
            episode=3,
            is_season_pack=False,
        )
        session.add(item)
        await session.flush()

        assert item.season == 2
        assert item.episode == 3
        assert item.is_season_pack is False


# ===========================================================================
# Test 2 — AddRequest schema validation
# ===========================================================================


class TestAddRequestSchema:
    """Verify AddRequest Pydantic schema handles is_season_pack correctly."""

    def test_is_season_pack_defaults_to_false(self) -> None:
        """AddRequest without is_season_pack defaults to False."""
        req = AddRequest(
            magnet_or_hash=_valid_hash("b"),
            title="Some Movie",
        )
        assert req.is_season_pack is False

    def test_is_season_pack_can_be_set_true(self) -> None:
        """AddRequest accepts is_season_pack=True."""
        req = AddRequest(
            magnet_or_hash=_valid_hash("c"),
            title="The Sopranos Season 3",
            media_type="show",
            season=3,
            is_season_pack=True,
        )
        assert req.is_season_pack is True

    def test_is_season_pack_with_episode_provided(self) -> None:
        """AddRequest allows both episode and is_season_pack at schema level."""
        req = AddRequest(
            magnet_or_hash=_valid_hash("d"),
            title="The Sopranos Season 3",
            media_type="show",
            season=3,
            episode=5,
            is_season_pack=True,
        )
        # Schema does not strip episode — the endpoint handler does
        assert req.episode == 5
        assert req.is_season_pack is True

    def test_season_and_episode_are_optional(self) -> None:
        """season and episode fields are optional; AddRequest is still valid."""
        req = AddRequest(
            magnet_or_hash=_valid_hash("e"),
            title="Mystery Movie",
        )
        assert req.season is None
        assert req.episode is None


# ===========================================================================
# Test 3 — SearchResultItem schema
# ===========================================================================


class TestSearchResultItemSchema:
    """Verify SearchResultItem includes season/episode/is_season_pack fields."""

    def test_season_pack_fields_present_with_defaults(self) -> None:
        """SearchResultItem can be constructed with minimal fields; season fields default."""
        result = SearchResultItem(
            info_hash=_valid_hash("f"),
            title="Some Pack",
        )
        assert result.season is None
        assert result.episode is None
        assert result.is_season_pack is False

    def test_season_pack_result_fields(self) -> None:
        """A season pack result has is_season_pack=True, season set, episode None."""
        result = SearchResultItem(
            info_hash=_valid_hash("1"),
            title="Breaking Bad S01 Complete",
            resolution="1080p",
            is_season_pack=True,
            season=1,
            episode=None,
            seeders=150,
        )
        assert result.is_season_pack is True
        assert result.season == 1
        assert result.episode is None

    def test_episode_result_fields(self) -> None:
        """A regular episode result has is_season_pack=False and both S/E values."""
        result = SearchResultItem(
            info_hash=_valid_hash("2"),
            title="Breaking Bad S01E03",
            resolution="1080p",
            is_season_pack=False,
            season=1,
            episode=3,
        )
        assert result.is_season_pack is False
        assert result.season == 1
        assert result.episode == 3

    def test_all_season_pack_fields_are_serialisable(self) -> None:
        """SearchResultItem round-trips through model_dump without loss."""
        result = SearchResultItem(
            info_hash=_valid_hash("3"),
            title="Oz S02 1080p",
            is_season_pack=True,
            season=2,
            episode=None,
            score=8.5,
        )
        data = result.model_dump()
        assert data["is_season_pack"] is True
        assert data["season"] == 2
        assert data["episode"] is None
        assert data["score"] == 8.5


# ===========================================================================
# Test 4 — POST /api/add: season pack sets episode=None
# ===========================================================================


class TestAddTorrentSeasonPackEpisodeNone:
    """When is_season_pack=True the endpoint must store episode=None."""

    async def test_season_pack_clears_episode(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """is_season_pack=True with episode=5 → MediaItem.episode is None.

        The API response (GET /api/queue/{id}) confirms episode=None;
        is_season_pack is verified directly via the DB session because the
        MediaItemResponse schema does not expose that field.
        """
        add_magnet, select_files, register = _patch_add_torrent_externals("RDSP010")

        with add_magnet, select_files, register:
            resp = await http.post(
                "/api/add",
                json={
                    "magnet_or_hash": _valid_hash("a"),
                    "title": "The Wire Season 1",
                    "imdb_id": "tt0306414",
                    "media_type": "show",
                    "year": 2002,
                    "season": 1,
                    "episode": 5,
                    "is_season_pack": True,
                },
            )

        assert resp.status_code == 200
        item_id = resp.json()["item_id"]

        # Confirm episode=None via the HTTP response
        detail_resp = await http.get(f"/api/queue/{item_id}")
        assert detail_resp.status_code == 200
        item_data = detail_resp.json()["item"]
        assert item_data["episode"] is None

        # Confirm is_season_pack=True directly from the DB
        row = await session.get(MediaItem, item_id)
        assert row is not None
        assert row.is_season_pack is True

    async def test_season_pack_without_episode_stays_none(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """is_season_pack=True without episode → MediaItem.episode remains None."""
        add_magnet, select_files, register = _patch_add_torrent_externals("RDSP011")

        with add_magnet, select_files, register:
            resp = await http.post(
                "/api/add",
                json={
                    "magnet_or_hash": _valid_hash("b"),
                    "title": "The Wire Season 2",
                    "imdb_id": "tt0306414",
                    "media_type": "show",
                    "year": 2003,
                    "season": 2,
                    "is_season_pack": True,
                },
            )

        assert resp.status_code == 200
        item_id = resp.json()["item_id"]

        # API confirms episode is None
        detail_resp = await http.get(f"/api/queue/{item_id}")
        assert detail_resp.status_code == 200
        assert detail_resp.json()["item"]["episode"] is None

        # DB confirms is_season_pack=True
        row = await session.get(MediaItem, item_id)
        assert row is not None
        assert row.is_season_pack is True

    async def test_season_pack_season_preserved(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Season number is kept even when episode is cleared on a season pack."""
        add_magnet, select_files, register = _patch_add_torrent_externals("RDSP012")

        with add_magnet, select_files, register:
            resp = await http.post(
                "/api/add",
                json={
                    "magnet_or_hash": _valid_hash("c"),
                    "title": "Oz Season 3",
                    "imdb_id": "tt0103767",
                    "media_type": "show",
                    "season": 3,
                    "episode": 1,
                    "is_season_pack": True,
                },
            )

        assert resp.status_code == 200
        item_id = resp.json()["item_id"]

        detail_resp = await http.get(f"/api/queue/{item_id}")
        assert detail_resp.status_code == 200
        item_data = detail_resp.json()["item"]
        assert item_data["season"] == 3
        assert item_data["episode"] is None


# ===========================================================================
# Test 5 — POST /api/add: non-season-pack preserves episode
# ===========================================================================


class TestAddTorrentEpisodePreserved:
    """When is_season_pack=False (or absent) the episode number is kept."""

    async def test_episode_preserved_when_not_season_pack(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """is_season_pack=False with episode=5 → MediaItem.episode is 5."""
        add_magnet, select_files, register = _patch_add_torrent_externals("RDEP001")

        with add_magnet, select_files, register:
            resp = await http.post(
                "/api/add",
                json={
                    "magnet_or_hash": _valid_hash("d"),
                    "title": "The Wire",
                    "imdb_id": "tt0306414",
                    "media_type": "show",
                    "year": 2002,
                    "season": 1,
                    "episode": 5,
                    "is_season_pack": False,
                },
            )

        assert resp.status_code == 200
        item_id = resp.json()["item_id"]

        # API confirms episode is preserved
        detail_resp = await http.get(f"/api/queue/{item_id}")
        assert detail_resp.status_code == 200
        assert detail_resp.json()["item"]["episode"] == 5

        # DB confirms is_season_pack=False
        row = await session.get(MediaItem, item_id)
        assert row is not None
        assert row.is_season_pack is False

    async def test_episode_preserved_when_flag_omitted(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Omitting is_season_pack (defaults False) leaves episode intact."""
        add_magnet, select_files, register = _patch_add_torrent_externals("RDEP002")

        with add_magnet, select_files, register:
            resp = await http.post(
                "/api/add",
                json={
                    "magnet_or_hash": _valid_hash("e"),
                    "title": "Sopranos",
                    "imdb_id": "tt0141842",
                    "media_type": "show",
                    "season": 4,
                    "episode": 7,
                },
            )

        assert resp.status_code == 200
        item_id = resp.json()["item_id"]

        # API confirms episode is intact
        detail_resp = await http.get(f"/api/queue/{item_id}")
        assert detail_resp.status_code == 200
        assert detail_resp.json()["item"]["episode"] == 7

        # DB confirms is_season_pack defaulted to False
        row = await session.get(MediaItem, item_id)
        assert row is not None
        assert row.is_season_pack is False

    async def test_season_and_episode_both_stored_correctly(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Both season and episode are stored for a regular episode item."""
        add_magnet, select_files, register = _patch_add_torrent_externals("RDEP003")

        with add_magnet, select_files, register:
            resp = await http.post(
                "/api/add",
                json={
                    "magnet_or_hash": _valid_hash("f"),
                    "title": "Breaking Bad",
                    "imdb_id": "tt0903747",
                    "media_type": "show",
                    "season": 3,
                    "episode": 10,
                    "is_season_pack": False,
                },
            )

        assert resp.status_code == 200
        item_id = resp.json()["item_id"]

        detail_resp = await http.get(f"/api/queue/{item_id}")
        assert detail_resp.status_code == 200
        item_data = detail_resp.json()["item"]
        assert item_data["season"] == 3
        assert item_data["episode"] == 10


# ===========================================================================
# Test 6 — scrape_pipeline: season pack uses episode=1 as Torrentio anchor
# ===========================================================================


class TestScrapePipelineSeasonPack:
    """Verify the scrape pipeline handles season packs in _step_torrentio."""

    async def test_season_pack_uses_episode_1_as_anchor(
        self, session: AsyncSession
    ) -> None:
        """When episode is None (season pack), Torrentio is called with episode=1."""
        from src.core.scrape_pipeline import scrape_pipeline

        item = MediaItem(
            imdb_id="tt0903747",
            title="Breaking Bad",
            media_type=MediaType.SHOW,
            state=QueueState.SCRAPING,
            state_changed_at=datetime.now(timezone.utc),
            retry_count=0,
            season=2,
            episode=None,  # season pack
            is_season_pack=True,
        )
        session.add(item)
        await session.flush()

        mock_result = TorrentioResult(
            info_hash=_valid_hash("1"),
            title="Breaking.Bad.S02.1080p",
            resolution="1080p",
            seeders=100,
            is_season_pack=True,
        )

        with (
            patch(
                "src.core.scrape_pipeline.mount_scanner.is_mount_available",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch(
                "src.core.scrape_pipeline.dedup_engine.check_content_duplicate",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch(
                "src.core.scrape_pipeline.zilean_client.search",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "src.core.scrape_pipeline.torrentio_client.scrape_episode",
                new_callable=AsyncMock,
                return_value=[mock_result],
            ) as mock_scrape,
            patch(
                "src.core.scrape_pipeline.rd_client.check_cached_batch",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch(
                "src.core.scrape_pipeline.rd_client.add_magnet",
                new_callable=AsyncMock,
                return_value={"id": "RDPIPE01"},
            ),
            patch(
                "src.core.scrape_pipeline.rd_client.select_files",
                new_callable=AsyncMock,
            ),
            patch(
                "src.core.scrape_pipeline.dedup_engine.register_torrent",
                new_callable=AsyncMock,
            ),
        ):
            await scrape_pipeline.run(session, item)

        # episode=None → pipeline should pass episode=1 to scrape_episode
        mock_scrape.assert_called_once_with("tt0903747", 2, 1)

    async def test_regular_episode_uses_actual_episode_number(
        self, session: AsyncSession
    ) -> None:
        """When episode is set, Torrentio is called with that episode number."""
        from src.core.scrape_pipeline import scrape_pipeline

        item = MediaItem(
            imdb_id="tt0903747",
            title="Breaking Bad",
            media_type=MediaType.SHOW,
            state=QueueState.SCRAPING,
            state_changed_at=datetime.now(timezone.utc),
            retry_count=0,
            season=2,
            episode=7,
            is_season_pack=False,
        )
        session.add(item)
        await session.flush()

        with (
            patch(
                "src.core.scrape_pipeline.mount_scanner.is_mount_available",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch(
                "src.core.scrape_pipeline.dedup_engine.check_content_duplicate",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch(
                "src.core.scrape_pipeline.zilean_client.search",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "src.core.scrape_pipeline.torrentio_client.scrape_episode",
                new_callable=AsyncMock,
                return_value=[],
            ) as mock_scrape,
            patch(
                "src.core.scrape_pipeline.rd_client.check_cached_batch",
                new_callable=AsyncMock,
                return_value={},
            ),
        ):
            await scrape_pipeline.run(session, item)

        mock_scrape.assert_called_once_with("tt0903747", 2, 7)

    async def test_show_without_season_skips_torrentio(
        self, session: AsyncSession
    ) -> None:
        """A show item with season=None (bad data) must skip Torrentio entirely."""
        from src.core.scrape_pipeline import scrape_pipeline

        item = MediaItem(
            imdb_id="tt0903747",
            title="Breaking Bad",
            media_type=MediaType.SHOW,
            state=QueueState.SCRAPING,
            state_changed_at=datetime.now(timezone.utc),
            retry_count=0,
            season=None,
            episode=None,
            is_season_pack=False,
        )
        session.add(item)
        await session.flush()

        with (
            patch(
                "src.core.scrape_pipeline.mount_scanner.is_mount_available",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch(
                "src.core.scrape_pipeline.dedup_engine.check_content_duplicate",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch(
                "src.core.scrape_pipeline.zilean_client.search",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "src.core.scrape_pipeline.torrentio_client.scrape_episode",
                new_callable=AsyncMock,
                return_value=[],
            ) as mock_scrape,
        ):
            result = await scrape_pipeline.run(session, item)

        # Torrentio must NOT be called when season is unknown
        mock_scrape.assert_not_called()
        assert result.action in ("no_results", "error")

    async def test_season_pack_does_not_skip_torrentio(
        self, session: AsyncSession
    ) -> None:
        """A season pack (episode=None, season set) must NOT skip Torrentio."""
        from src.core.scrape_pipeline import scrape_pipeline

        item = MediaItem(
            imdb_id="tt0141842",
            title="The Sopranos",
            media_type=MediaType.SHOW,
            state=QueueState.SCRAPING,
            state_changed_at=datetime.now(timezone.utc),
            retry_count=0,
            season=4,
            episode=None,
            is_season_pack=True,
        )
        session.add(item)
        await session.flush()

        with (
            patch(
                "src.core.scrape_pipeline.mount_scanner.is_mount_available",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch(
                "src.core.scrape_pipeline.dedup_engine.check_content_duplicate",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch(
                "src.core.scrape_pipeline.zilean_client.search",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "src.core.scrape_pipeline.torrentio_client.scrape_episode",
                new_callable=AsyncMock,
                return_value=[],
            ) as mock_scrape,
        ):
            await scrape_pipeline.run(session, item)

        # season=4 means Torrentio SHOULD be called (with episode=1 as anchor)
        mock_scrape.assert_called_once_with("tt0141842", 4, 1)


# ===========================================================================
# Test 7 — CHECKING stage: season pack deduplication of mount matches
# ===========================================================================


def _mount_index_entry(
    filepath: str,
    parsed_episode: int | None,
    parsed_resolution: str | None,
    filesize: int | None = None,
) -> object:
    """Create a lightweight stand-in for a MountIndex row for use as a mock return value.

    The CHECKING stage in _job_queue_processor only accesses four attributes on
    each mount result: ``filepath``, ``parsed_episode``, ``parsed_resolution``,
    and ``filesize``. Using SimpleNamespace avoids SQLAlchemy instrumentation
    problems that arise when constructing ORM instances outside a session.

    Args:
        filepath: Absolute path string for the mount index entry.
        parsed_episode: Parsed episode number (or None).
        parsed_resolution: Parsed resolution string (e.g. "1080p", "2160p").
        filesize: File size in bytes (or None).

    Returns:
        A SimpleNamespace with the four fields the CHECKING stage reads.
    """
    from types import SimpleNamespace

    return SimpleNamespace(
        filepath=filepath,
        parsed_episode=parsed_episode,
        parsed_resolution=parsed_resolution,
        filesize=filesize,
    )


def _make_session_factory(session: AsyncSession):
    """Return a callable that mimics async_session() for testing _job_queue_processor.

    _job_queue_processor calls ``session = async_session()`` and then later
    calls ``await session.commit()`` and ``await session.close()``.  To keep
    the test session alive and rolled-back by conftest, we suppress commit and
    close by replacing them with no-op AsyncMocks.

    Args:
        session: The test AsyncSession to inject.

    Returns:
        A synchronous callable (matching how async_session is called) that
        returns the patched test session.
    """
    session.commit = AsyncMock()  # type: ignore[method-assign]
    session.close = AsyncMock()  # type: ignore[method-assign]
    session.rollback = AsyncMock()  # type: ignore[method-assign]

    def _factory():
        return session

    return _factory


class TestSeasonPackCheckingDedup:
    """Verify the CHECKING stage deduplicates mount matches by episode number."""

    async def test_multiple_releases_per_episode_creates_one_symlink_each(
        self, session: AsyncSession
    ) -> None:
        """Six mount matches (2 per episode) → create_symlink called exactly 3 times.

        The CHECKING stage must group matches by parsed_episode and pick one
        representative per episode before calling create_symlink. When two
        releases exist for each of episodes 1, 2, and 3 the stage must produce
        exactly three symlink calls, not six.
        """
        from src.main import _job_queue_processor

        # Persist a season pack item in CHECKING state.
        item = MediaItem(
            title="Breaking Bad",
            media_type=MediaType.SHOW,
            state=QueueState.CHECKING,
            state_changed_at=datetime.now(timezone.utc),
            retry_count=0,
            season=1,
            episode=None,
            is_season_pack=True,
        )
        session.add(item)
        await session.flush()

        # Six mount matches: 2 resolutions × 3 episodes.
        mock_matches = [
            _mount_index_entry("/mnt/bb/S01E01.1080p.mkv", parsed_episode=1, parsed_resolution="1080p", filesize=2_000_000_000),
            _mount_index_entry("/mnt/bb/S01E01.2160p.mkv", parsed_episode=1, parsed_resolution="2160p", filesize=8_000_000_000),
            _mount_index_entry("/mnt/bb/S01E02.1080p.mkv", parsed_episode=2, parsed_resolution="1080p", filesize=2_100_000_000),
            _mount_index_entry("/mnt/bb/S01E02.2160p.mkv", parsed_episode=2, parsed_resolution="2160p", filesize=8_100_000_000),
            _mount_index_entry("/mnt/bb/S01E03.1080p.mkv", parsed_episode=3, parsed_resolution="1080p", filesize=2_200_000_000),
            _mount_index_entry("/mnt/bb/S01E03.2160p.mkv", parsed_episode=3, parsed_resolution="2160p", filesize=8_200_000_000),
        ]

        mock_create_symlink = AsyncMock()

        with (
            patch("src.main.async_session", _make_session_factory(session)),
            patch(
                "src.main.mount_scanner.lookup",
                new_callable=AsyncMock,
                return_value=mock_matches,
            ),
            patch(
                "src.main.symlink_manager.create_symlink",
                mock_create_symlink,
            ),
            # Stage 0: suppress queue_manager.process_queue so no state-machine
            # side-effects interfere with CHECKING items in this test.
            patch(
                "src.main.queue_manager.process_queue",
                new_callable=AsyncMock,
                return_value={"unreleased_advanced": 0, "retries_triggered": 0},
            ),
            # Stage 3 transition (CHECKING → COMPLETE) must succeed; let the
            # real queue_manager run against the test DB.
        ):
            await _job_queue_processor()

        # One symlink per episode — never one per mount match.
        assert mock_create_symlink.call_count == 3

    async def test_preferred_resolution_wins_over_higher_resolution(
        self, session: AsyncSession
    ) -> None:
        """When requested_resolution is "1080p" the 1080p file is symlinked, not 2160p.

        The dedup key function boosts the match whose parsed_resolution equals
        item.requested_resolution. Even though 2160p ranks higher by default
        resolution ordering, an exact preference match must take priority.
        """
        from src.main import _job_queue_processor

        item = MediaItem(
            title="The Wire",
            media_type=MediaType.SHOW,
            state=QueueState.CHECKING,
            state_changed_at=datetime.now(timezone.utc),
            retry_count=0,
            season=2,
            episode=None,
            is_season_pack=True,
            requested_resolution="1080p",
        )
        session.add(item)
        await session.flush()

        # Two candidates for episode 1: the higher-resolution 2160p file and
        # the preferred 1080p file.
        entry_2160p = _mount_index_entry(
            "/mnt/wire/S02E01.2160p.mkv",
            parsed_episode=1,
            parsed_resolution="2160p",
            filesize=10_000_000_000,
        )
        entry_1080p = _mount_index_entry(
            "/mnt/wire/S02E01.1080p.mkv",
            parsed_episode=1,
            parsed_resolution="1080p",
            filesize=4_000_000_000,
        )

        captured_paths: list[str] = []

        async def _capture_symlink(sess, media_item, source_path: str, episode_offset: int = 0):
            captured_paths.append(source_path)

        with (
            patch("src.main.async_session", _make_session_factory(session)),
            patch(
                "src.main.mount_scanner.lookup",
                new_callable=AsyncMock,
                return_value=[entry_2160p, entry_1080p],
            ),
            patch(
                "src.main.symlink_manager.create_symlink",
                side_effect=_capture_symlink,
            ),
            patch(
                "src.main.queue_manager.process_queue",
                new_callable=AsyncMock,
                return_value={"unreleased_advanced": 0, "retries_triggered": 0},
            ),
        ):
            await _job_queue_processor()

        # Exactly one symlink should be created for this single episode.
        assert len(captured_paths) == 1, (
            f"Expected 1 symlink call but got {len(captured_paths)}: {captured_paths}"
        )
        # The 1080p file must be chosen because it matches requested_resolution.
        assert captured_paths[0] == "/mnt/wire/S02E01.1080p.mkv", (
            f"Expected 1080p path but got: {captured_paths[0]!r}"
        )
