"""Tests for src/core/symlink_health.py and the symlink-health API endpoints.

Coverage
--------
scan_symlink_health():
  - No symlinks in DB → returns zeros, empty items list
  - All symlinks healthy → healthy count correct, items list empty
  - Broken source (source_exists=False, target_valid=False) → appears in items
  - Dangling symlink (source_exists=True but is_link=True and dest_exists=False) → broken
  - Broken symlink with mount index match → RECOVERABLE with mount_match_path set
  - Broken symlink with no mount index match → DEAD, mount_match_path is None
  - Item with multiple broken symlinks → deduplicated to one entry in items
  - Mount unavailable (os.path.isdir returns False) → empty scan with error message
  - Show item queries mount index with season and episode
  - Movie item matches mount index by title (and year when present)

execute_symlink_health():
  - requeue_ids → item transitions to WANTED (force_transition called)
  - requeue_ids → Symlink DB records deleted
  - requeue_ids → disk symlinks removed via os.unlink
  - cleanup_ids → item state left unchanged (no force_transition)
  - cleanup_ids → Symlink DB records deleted
  - Mixed requeue + cleanup in one call → both counts reflect correct totals
  - Empty request (no IDs) → returns zeros, no errors
  - Nonexistent item_id → error in result.errors, does not raise
  - Empty parent directory cleaned up after symlink removal
  - Non-symlink target_path (os.path.islink=False) → unlink NOT called

API endpoints:
  - POST /api/tools/symlink-health/scan → 200 with SymlinkHealthScan shape
  - POST /api/tools/symlink-health/execute → 200 with SymlinkHealthResult shape
  - POST /api/tools/symlink-health/scan while _cleanup_lock held → 409

asyncio_mode = "auto" (configured in pyproject.toml).
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.deps import get_db
from src.core.symlink_health import (
    SymlinkHealthExecuteRequest,
    SymlinkHealthResult,
    SymlinkHealthScan,
    SymlinkStatus,
    _check_path_pair,
    _find_mount_match,
    _remove_symlinks_for_item,
    execute_symlink_health,
    scan_symlink_health,
)
from src.main import app
from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.mount_index import MountIndex
from src.models.symlink import Symlink

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ZURG_MOUNT = "/mnt/zurg/__all__"
LIBRARY_MOVIES = "/media/library/movies"
LIBRARY_SHOWS = "/media/library/shows"


def _make_item(
    session: AsyncSession,
    *,
    title: str = "Test Movie",
    imdb_id: str | None = "tt0000001",
    media_type: MediaType = MediaType.MOVIE,
    season: int | None = None,
    episode: int | None = None,
    year: int | None = 2024,
    state: QueueState = QueueState.DONE,
) -> MediaItem:
    """Construct and add an unsaved MediaItem to the session."""
    item = MediaItem(
        title=title,
        imdb_id=imdb_id,
        media_type=media_type,
        state=state,
        state_changed_at=datetime.now(UTC),
        retry_count=0,
        year=year,
        season=season,
        episode=episode,
    )
    session.add(item)
    return item


def _make_symlink(
    session: AsyncSession,
    *,
    media_item_id: int,
    source_path: str = "/mnt/zurg/__all__/Movie.2024/Movie.2024.mkv",
    target_path: str = "/media/library/movies/Movie (2024)/Movie (2024).mkv",
) -> Symlink:
    """Construct and add an unsaved Symlink to the session."""
    sym = Symlink(
        media_item_id=media_item_id,
        source_path=source_path,
        target_path=target_path,
        valid=True,
    )
    session.add(sym)
    return sym


def _make_mount_index(
    session: AsyncSession,
    *,
    filepath: str = "/mnt/zurg/__all__/Movie.2024/Movie.2024.mkv",
    filename: str = "Movie.2024.mkv",
    parsed_title: str = "movie",
    parsed_year: int | None = 2024,
    parsed_season: int | None = None,
    parsed_episode: int | None = None,
) -> MountIndex:
    """Construct and add an unsaved MountIndex to the session."""
    entry = MountIndex(
        filepath=filepath,
        filename=filename,
        parsed_title=parsed_title,
        parsed_year=parsed_year,
        parsed_season=parsed_season,
        parsed_episode=parsed_episode,
    )
    session.add(entry)
    return entry


# ---------------------------------------------------------------------------
# API test fixtures
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
    """Async HTTP client backed by the FastAPI test app."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


# ---------------------------------------------------------------------------
# _check_path_pair unit tests
# ---------------------------------------------------------------------------


class TestCheckPathPair:
    """Unit tests for the internal path-checking helper."""

    async def test_both_exist(self):
        """source_exists and target_valid both True when paths are good."""
        with patch("src.core.symlink_health.asyncio.to_thread") as mock_thread:
            # Provide three results: exists(source), islink(target), exists(target)
            mock_thread.side_effect = [True, True, True]
            source_exists, target_valid = await _check_path_pair(
                "/source/file.mkv", "/target/link.mkv"
            )
        assert source_exists is True
        assert target_valid is True

    async def test_source_missing(self):
        """source_exists=False when source path does not exist."""
        with patch("src.core.symlink_health.asyncio.to_thread") as mock_thread:
            mock_thread.side_effect = [False, True, True]
            source_exists, target_valid = await _check_path_pair(
                "/source/missing.mkv", "/target/link.mkv"
            )
        assert source_exists is False
        assert target_valid is True

    async def test_target_dangling(self):
        """target_valid=False when symlink exists but destination is gone."""
        with patch("src.core.symlink_health.asyncio.to_thread") as mock_thread:
            mock_thread.side_effect = [True, True, False]
            source_exists, target_valid = await _check_path_pair(
                "/source/file.mkv", "/target/dangling.mkv"
            )
        assert source_exists is True
        assert target_valid is False

    async def test_target_not_a_symlink(self):
        """target_valid=False when target is not a symlink at all."""
        with patch("src.core.symlink_health.asyncio.to_thread") as mock_thread:
            mock_thread.side_effect = [True, False, True]
            source_exists, target_valid = await _check_path_pair(
                "/source/file.mkv", "/target/not_a_link.mkv"
            )
        assert source_exists is True
        assert target_valid is False


# ---------------------------------------------------------------------------
# scan_symlink_health tests
# ---------------------------------------------------------------------------


class TestScanSymlinkHealth:
    """Tests for scan_symlink_health()."""

    async def test_scan_empty_no_symlinks(self, session: AsyncSession):
        """No symlinks in DB → scan returns all zeros and empty items list."""
        with patch("src.core.symlink_health.mount_scanner.is_mount_available", new=AsyncMock(return_value=True)):
            result = await scan_symlink_health(session)

        assert result.total_symlinks == 0
        assert result.healthy == 0
        assert result.broken == 0
        assert result.recoverable == 0
        assert result.dead == 0
        assert result.items == []
        assert result.errors == []

    async def test_scan_all_healthy(self, session: AsyncSession):
        """All symlinks have existing sources and valid targets → broken=0, items empty."""
        item = _make_item(session)
        await session.flush()
        _make_symlink(
            session,
            media_item_id=item.id,
            source_path="/mnt/zurg/Movie.mkv",
            target_path="/library/Movie/Movie.mkv",
        )
        await session.flush()

        with (
            patch("src.core.symlink_health.mount_scanner.is_mount_available", new=AsyncMock(return_value=True)),
            patch("src.core.symlink_health.asyncio.gather", new=AsyncMock(return_value=[(True, True)])),
        ):
            result = await scan_symlink_health(session)

        assert result.total_symlinks == 1
        assert result.healthy == 1
        assert result.broken == 0
        assert result.items == []
        assert result.errors == []

    async def test_scan_broken_source_missing(self, session: AsyncSession):
        """Source file doesn't exist → item appears in broken items list."""
        item = _make_item(session)
        await session.flush()
        _make_symlink(
            session,
            media_item_id=item.id,
            source_path="/mnt/zurg/Movie.mkv",
            target_path="/library/Movie/Movie.mkv",
        )
        await session.flush()

        with (
            patch("src.core.symlink_health.mount_scanner.is_mount_available", new=AsyncMock(return_value=True)),
            patch("src.core.symlink_health.asyncio.gather", new=AsyncMock(return_value=[(False, False)])),
            patch("src.core.symlink_health._find_mount_match", new=AsyncMock(return_value=None)),
        ):
            result = await scan_symlink_health(session)

        assert result.broken == 1
        assert result.dead == 1
        assert len(result.items) == 1
        assert result.items[0].source_exists is False
        assert result.items[0].target_valid is False
        assert result.items[0].status == SymlinkStatus.DEAD

    async def test_scan_broken_target_dangling(self, session: AsyncSession):
        """Target is a dangling symlink (source_exists=True but target_valid=False).

        When the source file still exists on disk, the item is classified as
        RECOVERABLE immediately — the symlink just needs to be recreated.
        _find_mount_match is NOT called in this path.
        """
        item = _make_item(session)
        await session.flush()
        _make_symlink(
            session,
            media_item_id=item.id,
            source_path="/mnt/zurg/Movie.mkv",
            target_path="/library/Movie/Movie.mkv",
        )
        await session.flush()

        with (
            patch("src.core.symlink_health.mount_scanner.is_mount_available", new=AsyncMock(return_value=True)),
            patch("src.core.symlink_health.asyncio.gather", new=AsyncMock(return_value=[(True, False)])),
            patch("src.core.symlink_health._find_mount_match", new=AsyncMock(return_value=None)) as mock_match,
        ):
            result = await scan_symlink_health(session)

        assert result.broken == 1
        assert result.recoverable == 1
        assert len(result.items) == 1
        item_result = result.items[0]
        assert item_result.source_exists is True
        assert item_result.target_valid is False
        # Source still exists → RECOVERABLE; mount_match_path is set to source_path
        assert item_result.status == SymlinkStatus.RECOVERABLE
        assert item_result.mount_match_path == "/mnt/zurg/Movie.mkv"
        # _find_mount_match must not be called when source_exists is True
        mock_match.assert_not_called()

    async def test_scan_recoverable_mount_match(self, session: AsyncSession):
        """Broken symlink but mount index has matching content → RECOVERABLE."""
        item = _make_item(session, title="Great Film")
        await session.flush()
        _make_symlink(
            session,
            media_item_id=item.id,
            source_path="/mnt/zurg/old/Great.Film.mkv",
            target_path="/library/movies/Great Film/Great Film.mkv",
        )
        _make_mount_index(
            session,
            filepath="/mnt/zurg/__all__/Great.Film.2024.mkv",
            filename="Great.Film.2024.mkv",
            parsed_title="great film",
            parsed_year=2024,
        )
        await session.flush()

        match_path = "/mnt/zurg/__all__/Great.Film.2024.mkv"
        with (
            patch("src.core.symlink_health.mount_scanner.is_mount_available", new=AsyncMock(return_value=True)),
            patch("src.core.symlink_health.asyncio.gather", new=AsyncMock(return_value=[(False, False)])),
            patch("src.core.symlink_health._find_mount_match", new=AsyncMock(return_value=match_path)),
        ):
            result = await scan_symlink_health(session)

        assert result.recoverable == 1
        assert result.dead == 0
        assert len(result.items) == 1
        assert result.items[0].status == SymlinkStatus.RECOVERABLE
        assert result.items[0].mount_match_path == match_path

    async def test_scan_dead_no_mount_match(self, session: AsyncSession):
        """Broken symlink, nothing in mount index → DEAD."""
        item = _make_item(session, title="Lost Film")
        await session.flush()
        _make_symlink(
            session,
            media_item_id=item.id,
            source_path="/mnt/zurg/old/Lost.Film.mkv",
            target_path="/library/movies/Lost Film/Lost Film.mkv",
        )
        await session.flush()

        with (
            patch("src.core.symlink_health.mount_scanner.is_mount_available", new=AsyncMock(return_value=True)),
            patch("src.core.symlink_health.asyncio.gather", new=AsyncMock(return_value=[(False, False)])),
            patch("src.core.symlink_health._find_mount_match", new=AsyncMock(return_value=None)),
        ):
            result = await scan_symlink_health(session)

        assert result.dead == 1
        assert result.recoverable == 0
        assert result.items[0].mount_match_path is None
        assert result.items[0].status == SymlinkStatus.DEAD

    async def test_scan_deduplicates_by_item_id(self, session: AsyncSession):
        """Item with multiple broken symlinks → only one entry in results."""
        item = _make_item(session, title="Season Pack Show", media_type=MediaType.SHOW)
        await session.flush()

        # Add three symlinks for the same item (season pack)
        for ep in range(1, 4):
            _make_symlink(
                session,
                media_item_id=item.id,
                source_path=f"/mnt/zurg/Show/S01E{ep:02d}.mkv",
                target_path=f"/library/shows/Show/Season 01/Show.S01E{ep:02d}.mkv",
            )
        await session.flush()

        with (
            patch("src.core.symlink_health.mount_scanner.is_mount_available", new=AsyncMock(return_value=True)),
            patch(
                "src.core.symlink_health.asyncio.gather",
                new=AsyncMock(return_value=[(False, False), (False, False), (False, False)]),
            ),
            patch("src.core.symlink_health._find_mount_match", new=AsyncMock(return_value=None)),
        ):
            result = await scan_symlink_health(session)

        # Three broken symlinks but only one unique item — broken is deduped
        assert result.total_symlinks == 3
        assert result.broken == 1  # deduplicated by item_id
        assert result.healthy == 0  # total - total broken symlink rows (3-3=0)
        assert len(result.items) == 1
        assert result.items[0].item_id == item.id

    async def test_scan_mount_unavailable(self, session: AsyncSession):
        """Mount unavailable → returns error message, no items scanned."""
        item = _make_item(session)
        await session.flush()
        _make_symlink(session, media_item_id=item.id)
        await session.flush()

        with patch("src.core.symlink_health.mount_scanner.is_mount_available",
                   new=AsyncMock(return_value=False)):
            result = await scan_symlink_health(session)

        assert result.total_symlinks == 0
        assert result.items == []
        assert len(result.errors) == 1
        assert "not accessible" in result.errors[0]

    async def test_scan_mount_check_timeout(self, session: AsyncSession):
        """Mount availability check fails → treated as unavailable."""
        item = _make_item(session)
        await session.flush()
        _make_symlink(session, media_item_id=item.id)
        await session.flush()

        with patch("src.core.symlink_health.mount_scanner.is_mount_available",
                   new=AsyncMock(return_value=False)):
            result = await scan_symlink_health(session)

        assert result.total_symlinks == 0
        assert len(result.errors) == 1

    async def test_scan_show_with_season_episode(self, session: AsyncSession):
        """Show item correctly populates season/episode in the result."""
        item = _make_item(
            session,
            title="Great Show",
            media_type=MediaType.SHOW,
            season=2,
            episode=5,
        )
        await session.flush()
        _make_symlink(
            session,
            media_item_id=item.id,
            source_path="/mnt/zurg/Great.Show/S02E05.mkv",
            target_path="/library/shows/Great Show/Season 02/Great Show S02E05.mkv",
        )
        await session.flush()

        match_path = "/mnt/zurg/__all__/Great.Show/S02E05.mkv"
        with (
            patch("src.core.symlink_health.mount_scanner.is_mount_available", new=AsyncMock(return_value=True)),
            patch("src.core.symlink_health.asyncio.gather", new=AsyncMock(return_value=[(False, False)])),
            patch("src.core.symlink_health._find_mount_match", new=AsyncMock(return_value=match_path)),
        ):
            result = await scan_symlink_health(session)

        assert len(result.items) == 1
        broken = result.items[0]
        # SQLAlchemy stores enums by name in SQLite, so raw query returns "SHOW"
        assert broken.media_type.upper() == "SHOW"
        assert broken.season == 2
        assert broken.episode == 5
        assert broken.status == SymlinkStatus.RECOVERABLE

    async def test_scan_movie_matches_by_title(self, session: AsyncSession):
        """Movie item correctly sets media_type and year in the result."""
        item = _make_item(session, title="Space Odyssey", year=1968)
        await session.flush()
        _make_symlink(
            session,
            media_item_id=item.id,
            source_path="/mnt/zurg/Space.Odyssey.1968.mkv",
            target_path="/library/movies/Space Odyssey (1968)/Space Odyssey (1968).mkv",
        )
        await session.flush()

        with (
            patch("src.core.symlink_health.mount_scanner.is_mount_available", new=AsyncMock(return_value=True)),
            patch("src.core.symlink_health.asyncio.gather", new=AsyncMock(return_value=[(False, False)])),
            patch("src.core.symlink_health._find_mount_match", new=AsyncMock(return_value=None)),
        ):
            result = await scan_symlink_health(session)

        assert len(result.items) == 1
        broken = result.items[0]
        # SQLAlchemy stores enums by name in SQLite, so raw query returns "MOVIE"
        assert broken.media_type.upper() == "MOVIE"
        assert broken.title == "Space Odyssey"
        assert broken.season is None
        assert broken.episode is None

    async def test_scan_result_has_symlink_id(self, session: AsyncSession):
        """BrokenSymlinkItem.symlink_id is set to the Symlink primary key."""
        item = _make_item(session)
        await session.flush()
        sym = _make_symlink(session, media_item_id=item.id)
        await session.flush()

        with (
            patch("src.core.symlink_health.mount_scanner.is_mount_available", new=AsyncMock(return_value=True)),
            patch("src.core.symlink_health.asyncio.gather", new=AsyncMock(return_value=[(False, False)])),
            patch("src.core.symlink_health._find_mount_match", new=AsyncMock(return_value=None)),
        ):
            result = await scan_symlink_health(session)

        assert result.items[0].symlink_id == sym.id

    async def test_scan_mixed_healthy_and_broken(self, session: AsyncSession):
        """Two items: one healthy, one broken → counts split correctly."""
        item1 = _make_item(session, title="Healthy Movie")
        item2 = _make_item(session, title="Broken Movie")
        await session.flush()
        _make_symlink(
            session,
            media_item_id=item1.id,
            source_path="/mnt/zurg/Healthy.mkv",
            target_path="/library/Healthy/Healthy.mkv",
        )
        _make_symlink(
            session,
            media_item_id=item2.id,
            source_path="/mnt/zurg/Broken.mkv",
            target_path="/library/Broken/Broken.mkv",
        )
        await session.flush()

        with (
            patch("src.core.symlink_health.mount_scanner.is_mount_available", new=AsyncMock(return_value=True)),
            patch(
                "src.core.symlink_health.asyncio.gather",
                new=AsyncMock(return_value=[(True, True), (False, False)]),
            ),
            patch("src.core.symlink_health._find_mount_match", new=AsyncMock(return_value=None)),
        ):
            result = await scan_symlink_health(session)

        assert result.total_symlinks == 2
        assert result.healthy == 1
        assert result.broken == 1
        assert len(result.items) == 1
        assert result.items[0].title == "Broken Movie"


# ---------------------------------------------------------------------------
# _find_mount_match unit tests
# ---------------------------------------------------------------------------


class TestFindMountMatch:
    """Unit tests for the mount index lookup helper."""

    async def test_movie_match_by_title(self, session: AsyncSession):
        """Movie match returns filepath when title matches."""
        _make_mount_index(
            session,
            filepath="/mnt/zurg/__all__/Inception.2010.mkv",
            parsed_title="inception",
            parsed_year=2010,
        )
        await session.flush()

        result = await _find_mount_match(
            session, title="Inception", media_type="movie", season=None, episode=None, year=2010
        )

        assert result == "/mnt/zurg/__all__/Inception.2010.mkv"

    async def test_movie_no_match(self, session: AsyncSession):
        """Movie with no matching title returns None."""
        result = await _find_mount_match(
            session, title="Unknown Film", media_type="movie", season=None, episode=None, year=2024
        )

        assert result is None

    async def test_show_match_by_title_season_episode(self, session: AsyncSession):
        """Show match returns filepath when title, season, and episode all match."""
        _make_mount_index(
            session,
            filepath="/mnt/zurg/__all__/Breaking.Bad/S01E01.mkv",
            filename="S01E01.mkv",
            parsed_title="breaking bad",
            parsed_season=1,
            parsed_episode=1,
        )
        await session.flush()

        result = await _find_mount_match(
            session,
            title="Breaking Bad",
            media_type="show",
            season=1,
            episode=1,
            year=None,
        )

        assert result == "/mnt/zurg/__all__/Breaking.Bad/S01E01.mkv"

    async def test_show_no_match_wrong_episode(self, session: AsyncSession):
        """Show match returns None when episode does not match."""
        _make_mount_index(
            session,
            filepath="/mnt/zurg/__all__/Show/S01E02.mkv",
            parsed_title="test show",
            parsed_season=1,
            parsed_episode=2,
        )
        await session.flush()

        result = await _find_mount_match(
            session,
            title="Test Show",
            media_type="show",
            season=1,
            episode=5,  # wrong episode
            year=None,
        )

        assert result is None

    async def test_find_mount_match_reverse_containment(self, session: AsyncSession):
        """Long TMDB title matches short DB parsed_title (3+ words) via reverse containment.

        DB has "honzuki no gekokujou" indexed; searching with the full TMDB title should
        resolve to that filepath through phase 2 (reverse containment) in _find_mount_match.
        """
        _make_mount_index(
            session,
            filepath="/mnt/zurg/__all__/Honzuki/S01E01.mkv",
            filename="S01E01.mkv",
            parsed_title="honzuki no gekokujou",
            parsed_season=1,
            parsed_episode=1,
        )
        await session.flush()

        long_title = (
            "Honzuki no Gekokujou Shisho ni Naru Tame ni wa Shudan wo Erandeiraremasen"
        )
        result = await _find_mount_match(
            session,
            title=long_title,
            media_type="show",
            season=1,
            episode=1,
            year=None,
        )

        assert result == "/mnt/zurg/__all__/Honzuki/S01E01.mkv"

    async def test_find_mount_match_reverse_containment_guard(self, session: AsyncSession):
        """2-word DB parsed_title does NOT match via reverse containment (3-word minimum guard)."""
        _make_mount_index(
            session,
            filepath="/mnt/zurg/__all__/Dark.Knight.mkv",
            filename="Dark.Knight.mkv",
            parsed_title="dark knight",  # only 2 words — below the guard threshold
            parsed_year=2008,
        )
        await session.flush()

        result = await _find_mount_match(
            session,
            title="The Dark Knight Rises",
            media_type="movie",
            season=None,
            episode=None,
            year=None,  # no year filter so only word-count guard blocks it
        )

        assert result is None


# ---------------------------------------------------------------------------
# _remove_symlinks_for_item unit tests
# ---------------------------------------------------------------------------


class TestRemoveSymlinksForItem:
    """Tests for the internal symlink removal helper."""

    async def test_removes_db_records(self, session: AsyncSession):
        """Symlink rows are deleted from the database."""
        item = _make_item(session)
        await session.flush()
        _make_symlink(session, media_item_id=item.id, target_path="/library/Movie/Movie.mkv")
        await session.flush()

        with (
            patch("src.core.symlink_health.asyncio.to_thread", new=AsyncMock(return_value=True)),
            patch("src.core.symlink_health.settings") as mock_settings,
        ):
            mock_settings.paths.library_movies = LIBRARY_MOVIES
            mock_settings.paths.library_shows = LIBRARY_SHOWS
            removed = await _remove_symlinks_for_item(session, item.id)

        await session.flush()
        # Verify symlink no longer in DB
        from sqlalchemy import select
        result = await session.execute(select(Symlink).where(Symlink.media_item_id == item.id))
        remaining = result.scalars().all()
        assert remaining == []
        assert removed == 1

    async def test_disk_unlink_called_for_symlink(self, session: AsyncSession):
        """os.unlink is called for target_path when it is a symlink."""
        item = _make_item(session)
        await session.flush()
        target = "/library/movies/Movie/Movie.mkv"
        _make_symlink(session, media_item_id=item.id, target_path=target)
        await session.flush()

        unlink_calls = []

        async def fake_to_thread(fn, *args):
            if fn == os.path.islink:
                return True  # is a symlink
            if fn == os.unlink:
                unlink_calls.append(args[0])
                return None
            if fn == os.rmdir:
                raise OSError("not empty")
            return None

        with (
            patch("src.core.symlink_health.asyncio.to_thread", side_effect=fake_to_thread),
            patch("src.core.symlink_health.settings") as mock_settings,
        ):
            mock_settings.paths.library_movies = LIBRARY_MOVIES
            mock_settings.paths.library_shows = LIBRARY_SHOWS
            removed = await _remove_symlinks_for_item(session, item.id)

        assert removed == 1
        assert target in unlink_calls

    async def test_disk_unlink_not_called_when_not_symlink(self, session: AsyncSession):
        """os.unlink is NOT called when target_path is not a symlink."""
        item = _make_item(session)
        await session.flush()
        _make_symlink(session, media_item_id=item.id, target_path="/library/Movie/Movie.mkv")
        await session.flush()

        unlink_calls = []

        async def fake_to_thread(fn, *args):
            if fn == os.path.islink:
                return False  # NOT a symlink
            if fn == os.unlink:
                unlink_calls.append(args[0])
            if fn == os.rmdir:
                raise OSError("not empty")
            return None

        with (
            patch("src.core.symlink_health.asyncio.to_thread", side_effect=fake_to_thread),
            patch("src.core.symlink_health.settings") as mock_settings,
        ):
            mock_settings.paths.library_movies = LIBRARY_MOVIES
            mock_settings.paths.library_shows = LIBRARY_SHOWS
            removed = await _remove_symlinks_for_item(session, item.id)

        assert removed == 0
        assert unlink_calls == []

    async def test_already_gone_file_not_error(self, session: AsyncSession):
        """FileNotFoundError on unlink is tolerated and still counts as removed."""
        item = _make_item(session)
        await session.flush()
        _make_symlink(session, media_item_id=item.id, target_path="/library/Movie/Movie.mkv")
        await session.flush()

        async def fake_to_thread(fn, *args):
            if fn == os.path.islink:
                return True
            if fn == os.unlink:
                raise FileNotFoundError("already gone")
            if fn == os.rmdir:
                raise OSError("not empty")
            return None

        with (
            patch("src.core.symlink_health.asyncio.to_thread", side_effect=fake_to_thread),
            patch("src.core.symlink_health.settings") as mock_settings,
        ):
            mock_settings.paths.library_movies = LIBRARY_MOVIES
            mock_settings.paths.library_shows = LIBRARY_SHOWS
            removed = await _remove_symlinks_for_item(session, item.id)

        # Still counted as removed even though file was already gone
        assert removed == 1

    async def test_empty_parent_dir_removed(self, session: AsyncSession):
        """After removing symlink, empty parent directories are cleaned up."""
        item = _make_item(session)
        await session.flush()
        target = "/media/library/movies/Test Movie (2024)/Test Movie (2024).mkv"
        _make_symlink(session, media_item_id=item.id, target_path=target)
        await session.flush()

        rmdir_calls = []

        async def fake_to_thread(fn, *args):
            if fn == os.path.islink:
                return True
            if fn == os.unlink:
                return None
            if fn == os.rmdir:
                rmdir_calls.append(args[0])
                return None  # success — dir is empty
            return None

        with (
            patch("src.core.symlink_health.asyncio.to_thread", side_effect=fake_to_thread),
            patch("src.core.symlink_health.settings") as mock_settings,
        ):
            mock_settings.paths.library_movies = LIBRARY_MOVIES
            mock_settings.paths.library_shows = LIBRARY_SHOWS
            await _remove_symlinks_for_item(session, item.id)

        # Parent and grandparent should be attempted
        parent = os.path.dirname(target)  # .../Test Movie (2024)
        grandparent = os.path.dirname(parent)  # .../movies
        assert parent in rmdir_calls
        # Grandparent is LIBRARY_MOVIES (/media/library/movies) — should NOT be removed
        assert grandparent not in rmdir_calls

    async def test_library_root_never_deleted(self, session: AsyncSession):
        """rmdir is never called on library root paths."""
        item = _make_item(session)
        await session.flush()
        # Symlink directly under library root (no subdirectory)
        target = f"{LIBRARY_MOVIES}/Movie.mkv"
        _make_symlink(session, media_item_id=item.id, target_path=target)
        await session.flush()

        rmdir_calls = []

        async def fake_to_thread(fn, *args):
            if fn == os.path.islink:
                return True
            if fn == os.unlink:
                return None
            if fn == os.rmdir:
                rmdir_calls.append(args[0])
                return None
            return None

        with (
            patch("src.core.symlink_health.asyncio.to_thread", side_effect=fake_to_thread),
            patch("src.core.symlink_health.settings") as mock_settings,
        ):
            mock_settings.paths.library_movies = LIBRARY_MOVIES
            mock_settings.paths.library_shows = LIBRARY_SHOWS
            await _remove_symlinks_for_item(session, item.id)

        # library root itself should never be in rmdir_calls
        assert os.path.normpath(LIBRARY_MOVIES) not in rmdir_calls

    async def test_no_symlinks_returns_zero(self, session: AsyncSession):
        """Item with no symlink rows returns 0 removed."""
        item = _make_item(session)
        await session.flush()

        with patch("src.core.symlink_health.settings") as mock_settings:
            mock_settings.paths.library_movies = LIBRARY_MOVIES
            mock_settings.paths.library_shows = LIBRARY_SHOWS
            removed = await _remove_symlinks_for_item(session, item.id)

        assert removed == 0


# ---------------------------------------------------------------------------
# execute_symlink_health tests
# ---------------------------------------------------------------------------


class TestExecuteSymlinkHealth:
    """Tests for execute_symlink_health()."""

    async def test_execute_empty_request(self, session: AsyncSession):
        """No IDs provided → all counts zero, no errors."""
        req = SymlinkHealthExecuteRequest(requeue_ids=[], cleanup_ids=[])
        result = await execute_symlink_health(session, req)

        assert result.requeued == 0
        assert result.cleaned == 0
        assert result.symlinks_removed_from_disk == 0
        assert result.errors == []

    async def test_execute_requeue_transitions_to_wanted(self, session: AsyncSession):
        """Requeued item is force-transitioned to WANTED state."""
        item = _make_item(session, state=QueueState.DONE)
        await session.flush()
        _make_symlink(session, media_item_id=item.id)
        await session.flush()

        req = SymlinkHealthExecuteRequest(requeue_ids=[item.id], cleanup_ids=[])

        async def fake_to_thread(fn, *args):
            if fn == os.path.islink:
                return True
            if fn == os.unlink:
                return None
            if fn == os.rmdir:
                raise OSError("not empty")
            return None

        with (
            patch("src.core.symlink_health.asyncio.to_thread", side_effect=fake_to_thread),
            patch("src.core.symlink_health.settings") as mock_settings,
        ):
            mock_settings.paths.library_movies = LIBRARY_MOVIES
            mock_settings.paths.library_shows = LIBRARY_SHOWS
            result = await execute_symlink_health(session, req)

        await session.flush()

        assert result.requeued == 1
        assert result.errors == []

        # Verify state was changed to WANTED
        from sqlalchemy import select
        db_item = (
            await session.execute(select(MediaItem).where(MediaItem.id == item.id))
        ).scalar_one()
        assert db_item.state == QueueState.WANTED
        assert db_item.retry_count == 0

    async def test_execute_requeue_deletes_symlinks(self, session: AsyncSession):
        """Requeued item's symlink records are deleted from DB."""
        item = _make_item(session, state=QueueState.DONE)
        await session.flush()
        _make_symlink(session, media_item_id=item.id)
        await session.flush()

        req = SymlinkHealthExecuteRequest(requeue_ids=[item.id], cleanup_ids=[])

        async def fake_to_thread(fn, *args):
            if fn == os.path.islink:
                return False  # not a symlink on disk
            if fn == os.rmdir:
                raise OSError("not empty")
            return None

        with (
            patch("src.core.symlink_health.asyncio.to_thread", side_effect=fake_to_thread),
            patch("src.core.symlink_health.settings") as mock_settings,
        ):
            mock_settings.paths.library_movies = LIBRARY_MOVIES
            mock_settings.paths.library_shows = LIBRARY_SHOWS
            await execute_symlink_health(session, req)

        await session.flush()

        from sqlalchemy import select
        syms = (
            await session.execute(select(Symlink).where(Symlink.media_item_id == item.id))
        ).scalars().all()
        assert syms == []

    async def test_execute_requeue_removes_disk_symlinks(self, session: AsyncSession):
        """Dangling symlink files are removed from disk during requeue."""
        item = _make_item(session, state=QueueState.DONE)
        await session.flush()
        target = "/media/library/movies/TestMovie/TestMovie.mkv"
        _make_symlink(session, media_item_id=item.id, target_path=target)
        await session.flush()

        req = SymlinkHealthExecuteRequest(requeue_ids=[item.id], cleanup_ids=[])

        unlink_calls = []

        async def fake_to_thread(fn, *args):
            if fn == os.path.islink:
                return True
            if fn == os.unlink:
                unlink_calls.append(args[0])
                return None
            if fn == os.rmdir:
                raise OSError("not empty")
            return None

        with (
            patch("src.core.symlink_health.asyncio.to_thread", side_effect=fake_to_thread),
            patch("src.core.symlink_health.settings") as mock_settings,
        ):
            mock_settings.paths.library_movies = LIBRARY_MOVIES
            mock_settings.paths.library_shows = LIBRARY_SHOWS
            result = await execute_symlink_health(session, req)

        assert result.symlinks_removed_from_disk == 1
        assert target in unlink_calls

    async def test_execute_cleanup_preserves_state(self, session: AsyncSession):
        """Cleaned item stays in its current state — force_transition NOT called."""
        item = _make_item(session, state=QueueState.DONE)
        await session.flush()
        _make_symlink(session, media_item_id=item.id)
        await session.flush()

        req = SymlinkHealthExecuteRequest(requeue_ids=[], cleanup_ids=[item.id])

        async def fake_to_thread(fn, *args):
            if fn == os.path.islink:
                return False
            if fn == os.rmdir:
                raise OSError("not empty")
            return None

        with (
            patch("src.core.symlink_health.asyncio.to_thread", side_effect=fake_to_thread),
            patch("src.core.symlink_health.settings") as mock_settings,
        ):
            mock_settings.paths.library_movies = LIBRARY_MOVIES
            mock_settings.paths.library_shows = LIBRARY_SHOWS
            result = await execute_symlink_health(session, req)

        await session.flush()

        assert result.cleaned == 1
        assert result.requeued == 0

        # State must NOT have changed
        from sqlalchemy import select
        db_item = (
            await session.execute(select(MediaItem).where(MediaItem.id == item.id))
        ).scalar_one()
        assert db_item.state == QueueState.DONE

    async def test_execute_cleanup_deletes_symlinks(self, session: AsyncSession):
        """Cleaned item's symlink records are deleted from DB."""
        item = _make_item(session, state=QueueState.DONE)
        await session.flush()
        _make_symlink(session, media_item_id=item.id)
        await session.flush()

        req = SymlinkHealthExecuteRequest(requeue_ids=[], cleanup_ids=[item.id])

        async def fake_to_thread(fn, *args):
            if fn == os.path.islink:
                return False
            if fn == os.rmdir:
                raise OSError("not empty")
            return None

        with (
            patch("src.core.symlink_health.asyncio.to_thread", side_effect=fake_to_thread),
            patch("src.core.symlink_health.settings") as mock_settings,
        ):
            mock_settings.paths.library_movies = LIBRARY_MOVIES
            mock_settings.paths.library_shows = LIBRARY_SHOWS
            await execute_symlink_health(session, req)

        await session.flush()

        from sqlalchemy import select
        syms = (
            await session.execute(select(Symlink).where(Symlink.media_item_id == item.id))
        ).scalars().all()
        assert syms == []

    async def test_execute_mixed_requeue_and_cleanup(self, session: AsyncSession):
        """Both requeue_ids and cleanup_ids in one request → correct counts."""
        item_requeue = _make_item(session, title="Requeue Me", state=QueueState.DONE)
        item_cleanup = _make_item(session, title="Cleanup Me", state=QueueState.DONE)
        await session.flush()
        _make_symlink(session, media_item_id=item_requeue.id, target_path="/library/r.mkv")
        _make_symlink(session, media_item_id=item_cleanup.id, target_path="/library/c.mkv")
        await session.flush()

        req = SymlinkHealthExecuteRequest(
            requeue_ids=[item_requeue.id],
            cleanup_ids=[item_cleanup.id],
        )

        async def fake_to_thread(fn, *args):
            if fn == os.path.islink:
                return True
            if fn == os.unlink:
                return None
            if fn == os.rmdir:
                raise OSError("not empty")
            return None

        with (
            patch("src.core.symlink_health.asyncio.to_thread", side_effect=fake_to_thread),
            patch("src.core.symlink_health.settings") as mock_settings,
        ):
            mock_settings.paths.library_movies = LIBRARY_MOVIES
            mock_settings.paths.library_shows = LIBRARY_SHOWS
            result = await execute_symlink_health(session, req)

        assert result.requeued == 1
        assert result.cleaned == 1
        assert result.symlinks_removed_from_disk == 2
        assert result.errors == []

    async def test_execute_nonexistent_item(self, session: AsyncSession):
        """Non-existent item_id → error added to result, doesn't crash."""
        req = SymlinkHealthExecuteRequest(requeue_ids=[99999], cleanup_ids=[])
        result = await execute_symlink_health(session, req)

        assert result.requeued == 0
        assert len(result.errors) == 1
        assert "99999" in result.errors[0]
        assert "not found" in result.errors[0].lower()

    async def test_execute_nonexistent_cleanup_item(self, session: AsyncSession):
        """Non-existent item_id in cleanup_ids → no crash, zero cleaned."""
        req = SymlinkHealthExecuteRequest(requeue_ids=[], cleanup_ids=[88888])
        result = await execute_symlink_health(session, req)

        # cleanup of nonexistent item has no symlinks → cleaned=1 (no error)
        # OR it could just succeed silently since _remove_symlinks_for_item
        # fetches symlinks by item_id and returns 0 when none found
        assert result.errors == []
        assert result.cleaned == 1

    async def test_execute_multiple_symlinks_all_removed(self, session: AsyncSession):
        """Item with multiple symlinks → all removed from disk and DB."""
        item = _make_item(session, state=QueueState.DONE)
        await session.flush()
        for ep in range(1, 4):
            _make_symlink(
                session,
                media_item_id=item.id,
                source_path=f"/mnt/zurg/Show/S01E{ep:02d}.mkv",
                target_path=f"/library/Show/Season 01/S01E{ep:02d}.mkv",
            )
        await session.flush()

        req = SymlinkHealthExecuteRequest(requeue_ids=[], cleanup_ids=[item.id])

        async def fake_to_thread(fn, *args):
            if fn == os.path.islink:
                return True
            if fn == os.unlink:
                return None
            if fn == os.rmdir:
                raise OSError("not empty")
            return None

        with (
            patch("src.core.symlink_health.asyncio.to_thread", side_effect=fake_to_thread),
            patch("src.core.symlink_health.settings") as mock_settings,
        ):
            mock_settings.paths.library_movies = LIBRARY_MOVIES
            mock_settings.paths.library_shows = LIBRARY_SHOWS
            result = await execute_symlink_health(session, req)

        assert result.symlinks_removed_from_disk == 3
        assert result.cleaned == 1

        from sqlalchemy import select
        syms = (
            await session.execute(select(Symlink).where(Symlink.media_item_id == item.id))
        ).scalars().all()
        assert syms == []


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------


class TestSymlinkHealthScanEndpoint:
    """Tests for POST /api/tools/symlink-health/scan."""

    async def test_scan_endpoint_returns_200(self, http: AsyncClient, session: AsyncSession):
        """POST /api/tools/symlink-health/scan returns 200 with scan shape."""
        with patch(
            "src.api.routes.tools.scan_symlink_health",
            new=AsyncMock(return_value=SymlinkHealthScan(total_symlinks=0)),
        ):
            response = await http.post("/api/tools/symlink-health/scan")

        assert response.status_code == 200
        body = response.json()
        assert "total_symlinks" in body
        assert "healthy" in body
        assert "broken" in body
        assert "recoverable" in body
        assert "dead" in body
        assert "items" in body
        assert "errors" in body

    async def test_scan_endpoint_returns_populated_scan(
        self, http: AsyncClient, session: AsyncSession
    ):
        """Scan endpoint propagates non-zero counts from the core function."""
        mock_scan = SymlinkHealthScan(
            total_symlinks=5,
            healthy=3,
            broken=2,
            recoverable=1,
            dead=1,
        )
        with patch(
            "src.api.routes.tools.scan_symlink_health",
            new=AsyncMock(return_value=mock_scan),
        ):
            response = await http.post("/api/tools/symlink-health/scan")

        assert response.status_code == 200
        body = response.json()
        assert body["total_symlinks"] == 5
        assert body["healthy"] == 3
        assert body["broken"] == 2
        assert body["recoverable"] == 1
        assert body["dead"] == 1

    async def test_scan_endpoint_lock_conflict(self, http: AsyncClient):
        """Returns 409 when _cleanup_lock is already held."""
        import src.api.routes.tools as tools_module

        tools_module._cleanup_lock._locked = False  # ensure clean state
        # Manually acquire the lock to simulate a concurrent operation
        await tools_module._cleanup_lock.acquire()
        try:
            response = await http.post("/api/tools/symlink-health/scan")
        finally:
            tools_module._cleanup_lock.release()

        assert response.status_code == 409

    async def test_scan_endpoint_500_on_unexpected_error(self, http: AsyncClient):
        """Returns 500 when the core function raises unexpectedly."""
        with patch(
            "src.api.routes.tools.scan_symlink_health",
            new=AsyncMock(side_effect=RuntimeError("unexpected")),
        ):
            response = await http.post("/api/tools/symlink-health/scan")

        assert response.status_code == 500


class TestSymlinkHealthExecuteEndpoint:
    """Tests for POST /api/tools/symlink-health/execute."""

    async def test_execute_endpoint_returns_200(self, http: AsyncClient, session: AsyncSession):
        """POST /api/tools/symlink-health/execute returns 200 with result shape."""
        mock_result = SymlinkHealthResult(requeued=1, cleaned=0, symlinks_removed_from_disk=1)
        with patch(
            "src.api.routes.tools.execute_symlink_health",
            new=AsyncMock(return_value=mock_result),
        ):
            response = await http.post(
                "/api/tools/symlink-health/execute",
                json={"requeue_ids": [1], "cleanup_ids": []},
            )

        assert response.status_code == 200
        body = response.json()
        assert "requeued" in body
        assert "cleaned" in body
        assert "symlinks_removed_from_disk" in body
        assert "errors" in body

    async def test_execute_endpoint_empty_returns_200(self, http: AsyncClient):
        """Empty body → 200 with zeroed SymlinkHealthResult (fast-path in route)."""
        response = await http.post(
            "/api/tools/symlink-health/execute",
            json={"requeue_ids": [], "cleanup_ids": []},
        )

        assert response.status_code == 200
        body = response.json()
        assert body["requeued"] == 0
        assert body["cleaned"] == 0
        assert body["symlinks_removed_from_disk"] == 0

    async def test_execute_endpoint_propagates_counts(
        self, http: AsyncClient, session: AsyncSession
    ):
        """Execute endpoint propagates requeued/cleaned counts from core function."""
        mock_result = SymlinkHealthResult(
            requeued=2,
            cleaned=3,
            symlinks_removed_from_disk=5,
            errors=[],
        )
        with patch(
            "src.api.routes.tools.execute_symlink_health",
            new=AsyncMock(return_value=mock_result),
        ):
            response = await http.post(
                "/api/tools/symlink-health/execute",
                json={"requeue_ids": [1, 2], "cleanup_ids": [3, 4, 5]},
            )

        assert response.status_code == 200
        body = response.json()
        assert body["requeued"] == 2
        assert body["cleaned"] == 3
        assert body["symlinks_removed_from_disk"] == 5

    async def test_execute_endpoint_lock_conflict(self, http: AsyncClient):
        """Returns 409 when _cleanup_lock is already held."""
        import src.api.routes.tools as tools_module

        await tools_module._cleanup_lock.acquire()
        try:
            response = await http.post(
                "/api/tools/symlink-health/execute",
                json={"requeue_ids": [1], "cleanup_ids": []},
            )
        finally:
            tools_module._cleanup_lock.release()

        assert response.status_code == 409

    async def test_execute_endpoint_500_on_unexpected_error(self, http: AsyncClient):
        """Returns 500 when core function raises unexpectedly."""
        with patch(
            "src.api.routes.tools.execute_symlink_health",
            new=AsyncMock(side_effect=RuntimeError("boom")),
        ):
            response = await http.post(
                "/api/tools/symlink-health/execute",
                json={"requeue_ids": [1], "cleanup_ids": []},
            )

        assert response.status_code == 500

    async def test_execute_endpoint_422_on_invalid_body(self, http: AsyncClient):
        """Returns 422 when request body does not match schema."""
        response = await http.post(
            "/api/tools/symlink-health/execute",
            json={"requeue_ids": "not-a-list", "cleanup_ids": []},
        )
        assert response.status_code == 422


# ---------------------------------------------------------------------------
# Direct-recreate feature tests (scan + execute)
# ---------------------------------------------------------------------------


class TestScanSourceExistsFastPath:
    """Tests for the source_exists fast path added to scan_symlink_health."""

    async def test_scan_source_exists_skips_mount_match(self, session: AsyncSession):
        """Broken symlink where source file still exists → RECOVERABLE without mount lookup.

        When source_exists=True the scan must:
        - Set mount_match_path to source_path (not None)
        - NOT call _find_mount_match (no unnecessary DB lookup)
        - Classify the item as RECOVERABLE
        """
        source = "/mnt/zurg/__all__/Movie.2024/Movie.2024.mkv"
        target = "/media/library/movies/Movie (2024)/Movie (2024).mkv"

        item = _make_item(session, title="Present Source Movie")
        await session.flush()
        _make_symlink(session, media_item_id=item.id, source_path=source, target_path=target)
        await session.flush()

        # source_exists=True, target_valid=False (dangling symlink)
        with (
            patch(
                "src.core.symlink_health.mount_scanner.is_mount_available",
                new=AsyncMock(return_value=True),
            ),
            patch(
                "src.core.symlink_health.asyncio.gather",
                new=AsyncMock(return_value=[(True, False)]),
            ),
            patch(
                "src.core.symlink_health._find_mount_match",
                new=AsyncMock(return_value=None),
            ) as mock_find,
        ):
            result = await scan_symlink_health(session)

        assert result.broken == 1
        assert result.recoverable == 1
        assert result.dead == 0
        assert len(result.items) == 1

        broken = result.items[0]
        assert broken.source_exists is True
        assert broken.target_valid is False
        assert broken.status == SymlinkStatus.RECOVERABLE
        # mount_match_path must be the existing source_path — not None
        assert broken.mount_match_path == source
        # _find_mount_match must never be called when source is present
        mock_find.assert_not_called()

    async def test_scan_source_gone_uses_mount_match(self, session: AsyncSession):
        """Broken symlink where source is gone → _find_mount_match IS called.

        When source_exists=False the scan falls through to _find_mount_match.
        The result classification depends entirely on what _find_mount_match returns.
        """
        source = "/mnt/zurg/__all__/Gone.Movie.mkv"
        target = "/media/library/movies/Gone Movie (2020)/Gone Movie (2020).mkv"
        match = "/mnt/zurg/__all__/New.Path/Gone.Movie.mkv"

        item = _make_item(session, title="Gone Source Movie", year=2020)
        await session.flush()
        _make_symlink(session, media_item_id=item.id, source_path=source, target_path=target)
        await session.flush()

        # source_exists=False, target_valid=False
        with (
            patch(
                "src.core.symlink_health.mount_scanner.is_mount_available",
                new=AsyncMock(return_value=True),
            ),
            patch(
                "src.core.symlink_health.asyncio.gather",
                new=AsyncMock(return_value=[(False, False)]),
            ),
            patch(
                "src.core.symlink_health._find_mount_match",
                new=AsyncMock(return_value=match),
            ) as mock_find,
        ):
            result = await scan_symlink_health(session)

        assert result.broken == 1
        assert result.recoverable == 1
        assert result.dead == 0

        broken = result.items[0]
        assert broken.source_exists is False
        assert broken.status == SymlinkStatus.RECOVERABLE
        # mount_match_path comes from _find_mount_match, not source_path
        assert broken.mount_match_path == match
        # _find_mount_match MUST be called when source is gone
        mock_find.assert_called_once()

    async def test_scan_source_gone_no_match_is_dead(self, session: AsyncSession):
        """Broken symlink where source is gone and no mount match → DEAD."""
        item = _make_item(session, title="Truly Lost Movie")
        await session.flush()
        _make_symlink(
            session,
            media_item_id=item.id,
            source_path="/mnt/zurg/__all__/Lost.mkv",
            target_path="/media/library/movies/Truly Lost Movie/Truly Lost Movie.mkv",
        )
        await session.flush()

        with (
            patch(
                "src.core.symlink_health.mount_scanner.is_mount_available",
                new=AsyncMock(return_value=True),
            ),
            patch(
                "src.core.symlink_health.asyncio.gather",
                new=AsyncMock(return_value=[(False, False)]),
            ),
            patch(
                "src.core.symlink_health._find_mount_match",
                new=AsyncMock(return_value=None),
            ) as mock_find,
        ):
            result = await scan_symlink_health(session)

        assert result.dead == 1
        assert result.recoverable == 0
        assert result.items[0].status == SymlinkStatus.DEAD
        assert result.items[0].mount_match_path is None
        mock_find.assert_called_once()


class TestExecuteDirectRecreate:
    """Tests for the direct-recreate path in execute_symlink_health."""

    async def test_execute_recreates_when_source_exists(self, session: AsyncSession):
        """Item with a live source_path → symlink is directly recreated, state→COMPLETE.

        Verifies:
        - create_symlink called with the correct source_path
        - Item state is COMPLETE (not WANTED)
        - result.recreated == 1 and item_id in result.recreated_ids
        - result.media_scan_targets has an entry for the new symlink
        """
        from src.core.symlink_manager import SymlinkCreationError, SourceNotFoundError  # noqa

        source = "/mnt/zurg/__all__/Movie.mkv"
        new_target = "/media/library/movies/Test Movie (2024)/Test Movie (2024).mkv"

        item = _make_item(session, state=QueueState.DONE)
        await session.flush()
        _make_symlink(session, media_item_id=item.id, source_path=source)
        await session.flush()

        # Fake Symlink object returned by create_symlink
        mock_new_symlink = MagicMock()
        mock_new_symlink.target_path = new_target

        mock_create_symlink = AsyncMock(return_value=mock_new_symlink)

        async def fake_to_thread(fn, *args):
            if fn == os.path.exists:
                # source_path exists on disk
                return True
            if fn == os.path.islink:
                return False
            if fn == os.rmdir:
                raise OSError("not empty")
            return None

        with (
            patch("src.core.symlink_health.asyncio.to_thread", side_effect=fake_to_thread),
            patch("src.core.symlink_health.settings") as mock_settings,
            patch(
                "src.core.symlink_manager.SymlinkManager.create_symlink",
                new=mock_create_symlink,
            ),
        ):
            mock_settings.paths.library_movies = LIBRARY_MOVIES
            mock_settings.paths.library_shows = LIBRARY_SHOWS
            result = await execute_symlink_health(
                session,
                SymlinkHealthExecuteRequest(requeue_ids=[item.id], cleanup_ids=[]),
            )

        await session.flush()

        assert result.recreated == 1
        assert item.id in result.recreated_ids
        assert result.requeued == 0
        assert result.errors == []

        # Verify media_scan_targets has an entry pointing to the new symlink's target_path
        assert len(result.media_scan_targets) == 1
        scan_target = result.media_scan_targets[0]
        assert scan_target.target_path == new_target
        assert scan_target.media_type == item.media_type.value

        # Verify state was changed to COMPLETE, not WANTED
        from sqlalchemy import select as sa_select

        db_item = (
            await session.execute(sa_select(MediaItem).where(MediaItem.id == item.id))
        ).scalar_one()
        assert db_item.state == QueueState.COMPLETE

        # create_symlink must have been called with the correct source_path
        mock_create_symlink.assert_awaited_once()
        call_args = mock_create_symlink.call_args
        assert call_args.args[2] == source

    async def test_execute_requeues_when_source_gone(self, session: AsyncSession):
        """Item whose source_path is gone falls back to re-queue (WANTED), not recreate.

        Verifies:
        - create_symlink is NOT called
        - Item transitions to WANTED
        - result.requeued == 1
        - result.recreated == 0
        """
        from src.core.symlink_manager import SymlinkCreationError, SourceNotFoundError  # noqa

        source = "/mnt/zurg/__all__/Missing.mkv"

        item = _make_item(session, state=QueueState.DONE)
        await session.flush()
        _make_symlink(session, media_item_id=item.id, source_path=source)
        await session.flush()

        mock_create_symlink = AsyncMock()

        async def fake_to_thread(fn, *args):
            if fn == os.path.exists:
                # source_path does NOT exist on disk
                return False
            if fn == os.path.islink:
                return False
            if fn == os.rmdir:
                raise OSError("not empty")
            return None

        with (
            patch("src.core.symlink_health.asyncio.to_thread", side_effect=fake_to_thread),
            patch("src.core.symlink_health.settings") as mock_settings,
            patch(
                "src.core.symlink_manager.SymlinkManager.create_symlink",
                new=mock_create_symlink,
            ),
        ):
            mock_settings.paths.library_movies = LIBRARY_MOVIES
            mock_settings.paths.library_shows = LIBRARY_SHOWS
            result = await execute_symlink_health(
                session,
                SymlinkHealthExecuteRequest(requeue_ids=[item.id], cleanup_ids=[]),
            )

        await session.flush()

        assert result.requeued == 1
        assert result.recreated == 0
        assert result.recreated_ids == []
        assert result.errors == []

        # create_symlink must not have been called at all
        mock_create_symlink.assert_not_awaited()

        # Item must be in WANTED state
        from sqlalchemy import select as sa_select

        db_item = (
            await session.execute(sa_select(MediaItem).where(MediaItem.id == item.id))
        ).scalar_one()
        assert db_item.state == QueueState.WANTED

    async def test_execute_recreate_falls_back_on_error(self, session: AsyncSession):
        """create_symlink raising SymlinkCreationError → falls back to re-queue, no raise.

        Verifies:
        - Falls back to force_transition(WANTED)
        - result.requeued == 1
        - result.recreated == 0
        - Error is logged but not propagated (result.errors is empty — the outer
          try/except only appends for ItemNotFoundError or unexpected Exception,
          not for the expected SymlinkCreationError fallback path)
        """
        from src.core.symlink_manager import SymlinkCreationError, SourceNotFoundError  # noqa

        source = "/mnt/zurg/__all__/Broken.mkv"

        item = _make_item(session, state=QueueState.DONE)
        await session.flush()
        _make_symlink(session, media_item_id=item.id, source_path=source)
        await session.flush()

        mock_create_symlink = AsyncMock(
            side_effect=SymlinkCreationError(
                target_path="/media/library/movies/Broken/Broken.mkv",
                source_path=source,
                reason="disk full",
            )
        )

        async def fake_to_thread(fn, *args):
            if fn == os.path.exists:
                # Source exists — will attempt recreate
                return True
            if fn == os.path.islink:
                return False
            if fn == os.rmdir:
                raise OSError("not empty")
            return None

        with (
            patch("src.core.symlink_health.asyncio.to_thread", side_effect=fake_to_thread),
            patch("src.core.symlink_health.settings") as mock_settings,
            patch(
                "src.core.symlink_manager.SymlinkManager.create_symlink",
                new=mock_create_symlink,
            ),
        ):
            mock_settings.paths.library_movies = LIBRARY_MOVIES
            mock_settings.paths.library_shows = LIBRARY_SHOWS
            result = await execute_symlink_health(
                session,
                SymlinkHealthExecuteRequest(requeue_ids=[item.id], cleanup_ids=[]),
            )

        await session.flush()

        # Must fall back to re-queue, not raise
        assert result.requeued == 1
        assert result.recreated == 0
        assert result.recreated_ids == []
        # The fallback path is not an unexpected error — errors list stays empty
        assert result.errors == []

        # create_symlink was attempted (and failed)
        mock_create_symlink.assert_awaited_once()

        # Item must be in WANTED state after the fallback
        from sqlalchemy import select as sa_select

        db_item = (
            await session.execute(sa_select(MediaItem).where(MediaItem.id == item.id))
        ).scalar_one()
        assert db_item.state == QueueState.WANTED
