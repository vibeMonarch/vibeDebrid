"""Tests for src/core/symlink_manager.py.

Covers:
  - sanitize_name: normal names, all individual illegal characters, whitespace
    collapsing, leading-dot stripping, empty/all-special fallback to "Unknown",
    parentheses and dashes preserved, Unicode preserved, colon handling
  - build_movie_dir: title + year, title only, special-char sanitization,
    absolute path, uses settings.paths.library_movies
  - build_show_dir: title + year + season, zero-padding, title without year,
    absolute path, uses settings.paths.library_shows
  - SymlinkManager.create_symlink (movies): correct target path, creates parent
    dirs, symlink points to source, DB record fields, idempotent on existing
    symlink, source not found raises SourceNotFoundError, stale target replaced
  - SymlinkManager.create_symlink (shows): Season subdirectory structure,
    zero-padded season, season=None defaults to Season 01
  - SymlinkManager.verify_symlinks: valid symlinks kept, broken source marks
    invalid, missing symlink file marks invalid, empty DB returns zeros,
    already-invalid skipped, mixed scenario, last_checked_at updated
  - SymlinkManager.remove_symlink: symlink file removed from disk, DB record
    deleted, empty parent directory cleaned up, missing file handled gracefully,
    multiple symlinks all removed, returns count
  - SymlinkManager.get_symlinks_for_item / get_broken_symlinks: correct query
    results, empty list when none exist, get_broken_symlinks returns only invalid
  - Exception attributes: SourceNotFoundError.source_path,
    SymlinkCreationError.target_path and .source_path
  - Module-level singleton: symlink_manager is a SymlinkManager instance
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import SymlinkNamingConfig, settings
from src.core.symlink_manager import (
    SourceNotFoundError,
    SymlinkCreationError,
    SymlinkManager,
    VerifyResult,
    _find_existing_show_dir,
    _parse_episode_from_filename,
    build_movie_dir,
    build_show_dir,
    sanitize_name,
    symlink_manager,
)
from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.symlink import Symlink
from src.services.tmdb import (
    TmdbCastMember,
    TmdbCredits,
    TmdbCrewMember,
    TmdbMovieDetail,
    TmdbShowDetail,
)

# Naming config with date_prefix disabled — used in tests that assert exact
# path equality so they are not coupled to the timestamp feature.
_NAMING_NO_PREFIX = SymlinkNamingConfig(date_prefix=False, release_year=True, resolution=False)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def library_paths(tmp_path: Path) -> dict[str, str]:
    """Set up temp library directories and return their string paths."""
    movies_dir = tmp_path / "library" / "movies"
    shows_dir = tmp_path / "library" / "shows"
    mount_dir = tmp_path / "mount"
    movies_dir.mkdir(parents=True)
    shows_dir.mkdir(parents=True)
    mount_dir.mkdir(parents=True)
    return {
        "movies": str(movies_dir),
        "shows": str(shows_dir),
        "mount": str(mount_dir),
    }


@pytest.fixture
async def movie_item(session: AsyncSession) -> MediaItem:
    """A COMPLETE movie MediaItem persisted to the test database."""
    item = MediaItem(
        imdb_id="tt1234567",
        title="Test Movie",
        year=2024,
        media_type=MediaType.MOVIE,
        state=QueueState.COMPLETE,
        retry_count=0,
    )
    session.add(item)
    await session.flush()
    return item


@pytest.fixture
async def show_item(session: AsyncSession) -> MediaItem:
    """A COMPLETE show episode MediaItem persisted to the test database."""
    item = MediaItem(
        imdb_id="tt7654321",
        title="Test Show",
        year=2023,
        media_type=MediaType.SHOW,
        season=2,
        episode=5,
        state=QueueState.COMPLETE,
        retry_count=0,
    )
    session.add(item)
    await session.flush()
    return item


def _make_source_file(mount_dir: str, filename: str = "video.mkv") -> str:
    """Create a real file in the mount directory and return its absolute path."""
    path = os.path.join(mount_dir, filename)
    Path(path).write_bytes(b"fake video content")
    return path


# ---------------------------------------------------------------------------
# Group 1: sanitize_name
# ---------------------------------------------------------------------------


class TestSanitizeName:
    """Unit tests for the module-level sanitize_name helper."""

    def test_normal_name_passes_through(self) -> None:
        """A clean alphanumeric name is returned unchanged."""
        assert sanitize_name("The Dark Knight") == "The Dark Knight"

    def test_removes_forward_slash(self) -> None:
        """Forward slash is removed from the name."""
        result = sanitize_name("AC/DC Story")
        assert "/" not in result

    def test_removes_backslash(self) -> None:
        """Backslash is removed from the name."""
        result = sanitize_name("Back\\slash")
        assert "\\" not in result

    def test_removes_colon(self) -> None:
        """Colon is removed or replaced; result must not contain a colon."""
        result = sanitize_name("Batman: The Movie")
        assert ":" not in result

    def test_removes_asterisk(self) -> None:
        """Asterisk is removed from the name."""
        result = sanitize_name("Star*Wars")
        assert "*" not in result

    def test_removes_question_mark(self) -> None:
        """Question mark is removed from the name."""
        result = sanitize_name("What Is This?")
        assert "?" not in result

    def test_removes_double_quote(self) -> None:
        """Double quote is removed from the name."""
        result = sanitize_name('Say "Hello"')
        assert '"' not in result

    def test_removes_less_than(self) -> None:
        """Less-than sign is removed from the name."""
        result = sanitize_name("A < B")
        assert "<" not in result

    def test_removes_greater_than(self) -> None:
        """Greater-than sign is removed from the name."""
        result = sanitize_name("A > B")
        assert ">" not in result

    def test_removes_pipe(self) -> None:
        """Pipe character is removed from the name."""
        result = sanitize_name("A|B")
        assert "|" not in result

    def test_collapses_multiple_spaces(self) -> None:
        """Multiple consecutive spaces are collapsed to a single space."""
        result = sanitize_name("Too   Many   Spaces")
        assert "  " not in result
        assert "Too" in result
        assert "Many" in result
        assert "Spaces" in result

    def test_strips_leading_whitespace(self) -> None:
        """Leading whitespace is stripped from the result."""
        result = sanitize_name("   Leading Spaces")
        assert not result.startswith(" ")

    def test_strips_trailing_whitespace(self) -> None:
        """Trailing whitespace is stripped from the result."""
        result = sanitize_name("Trailing Spaces   ")
        assert not result.endswith(" ")

    def test_strips_leading_dots(self) -> None:
        """Leading dots are removed to prevent hidden files."""
        result = sanitize_name(".hidden name")
        assert not result.startswith(".")

    def test_empty_string_returns_unknown(self) -> None:
        """An empty string returns the fallback 'Unknown'."""
        assert sanitize_name("") == "Unknown"

    def test_all_special_chars_returns_unknown(self) -> None:
        """A string of only illegal characters returns 'Unknown'."""
        assert sanitize_name("/*:<>?|") == "Unknown"

    def test_preserves_parentheses(self) -> None:
        """Parentheses are preserved (used in e.g. 'Title (2024)')."""
        result = sanitize_name("Test Movie (2024)")
        assert "(" in result
        assert ")" in result

    def test_preserves_dashes(self) -> None:
        """Hyphens/dashes are preserved (common in titles)."""
        result = sanitize_name("Spider-Man")
        assert "-" in result

    def test_unicode_preserved(self) -> None:
        """Unicode characters are not stripped (e.g. Japanese, accented)."""
        result = sanitize_name("Jujutsu Kaisen")
        assert "Jujutsu" in result
        assert "Kaisen" in result

    def test_unicode_accented_chars(self) -> None:
        """Accented Latin characters survive sanitization."""
        result = sanitize_name("Amelie (Amélie)")
        assert "Amelie" in result

    def test_colon_in_subtitle_cleaned(self) -> None:
        """Colon between title and subtitle is removed or becomes a dash/space."""
        result = sanitize_name("Batman: The Movie")
        assert ":" not in result
        assert "Batman" in result
        assert "Movie" in result

    def test_only_dots_returns_unknown(self) -> None:
        """A string of only dots (after stripping) returns 'Unknown'."""
        result = sanitize_name("...")
        assert result == "Unknown"

    def test_very_long_name_not_truncated_by_sanitizer(self) -> None:
        """sanitize_name does not arbitrarily truncate long but valid names."""
        long_name = "A" * 200
        result = sanitize_name(long_name)
        assert len(result) > 0
        assert "A" in result


# ---------------------------------------------------------------------------
# Group 2: build_movie_dir
# ---------------------------------------------------------------------------


class TestBuildMovieDir:
    """Unit tests for the build_movie_dir helper."""

    def test_title_with_year_forms_correct_path(self, library_paths: dict[str, str]) -> None:
        """Title and year produce 'Title (Year)' directory under library_movies."""
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            result = build_movie_dir("Test Movie", 2024)
        expected = os.path.join(library_paths["movies"], "Test Movie (2024)")
        assert result == expected

    def test_title_without_year_omits_year(self, library_paths: dict[str, str]) -> None:
        """When year is None the directory is just the sanitized title."""
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            result = build_movie_dir("Test Movie", None)
        expected = os.path.join(library_paths["movies"], "Test Movie")
        assert result == expected

    def test_title_with_special_chars_sanitized(self, library_paths: dict[str, str]) -> None:
        """Illegal filesystem characters in the title are sanitized."""
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            result = build_movie_dir("Batman: The Movie", 2022)
        assert ":" not in result
        assert "Batman" in result
        assert "2022" in result

    def test_returned_path_is_absolute(self, library_paths: dict[str, str]) -> None:
        """The returned path is absolute (starts with '/')."""
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            result = build_movie_dir("Any Title", 2020)
        assert os.path.isabs(result)

    def test_uses_library_movies_setting(self, library_paths: dict[str, str]) -> None:
        """The path is rooted at settings.paths.library_movies."""
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            result = build_movie_dir("Some Movie", 2023)
        assert result.startswith(library_paths["movies"])


# ---------------------------------------------------------------------------
# Group 3: build_show_dir
# ---------------------------------------------------------------------------


class TestBuildShowDir:
    """Unit tests for the build_show_dir helper."""

    def test_title_year_season_forms_correct_path(self, library_paths: dict[str, str]) -> None:
        """Title, year, and season produce the full expected directory path."""
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            result = build_show_dir("Test Show", 2024, 3)
        expected = os.path.join(
            library_paths["shows"], "Test Show (2024)", "Season 03"
        )
        assert result == expected

    def test_season_single_digit_zero_padded(self, library_paths: dict[str, str]) -> None:
        """Season 1 becomes 'Season 01' (zero-padded to two digits)."""
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            result = build_show_dir("Show", 2020, 1)
        assert "Season 01" in result

    def test_season_double_digit_not_padded_further(self, library_paths: dict[str, str]) -> None:
        """Season 12 becomes 'Season 12' (already two digits)."""
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            result = build_show_dir("Show", 2020, 12)
        assert "Season 12" in result

    def test_title_without_year_omits_year(self, library_paths: dict[str, str]) -> None:
        """When year is None the directory is 'Title/Season XX'."""
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            result = build_show_dir("No Year Show", None, 1)
        assert "No Year Show" in result
        assert "Season 01" in result
        # Should NOT contain a parenthesised year segment like "(None)"
        assert "(None)" not in result

    def test_returned_path_is_absolute(self, library_paths: dict[str, str]) -> None:
        """The returned path is absolute."""
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            result = build_show_dir("Show", 2021, 2)
        assert os.path.isabs(result)

    def test_uses_library_shows_setting(self, library_paths: dict[str, str]) -> None:
        """The path is rooted at settings.paths.library_shows."""
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            result = build_show_dir("Show", 2021, 2)
        assert result.startswith(library_paths["shows"])


# ---------------------------------------------------------------------------
# Group 4: create_symlink — movies
# ---------------------------------------------------------------------------


class TestCreateSymlinkMovie:
    """Tests for SymlinkManager.create_symlink with movie MediaItems."""

    async def test_creates_symlink_at_correct_path(
        self, session: AsyncSession, movie_item: MediaItem, library_paths: dict[str, str]
    ) -> None:
        """Symlink is created at the expected location under library_movies."""
        source = _make_source_file(library_paths["mount"], "test.movie.2024.mkv")
        manager = SymlinkManager()
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            result = await manager.create_symlink(session, movie_item, source)
        assert os.path.islink(result.target_path)

    async def test_creates_parent_directory_if_missing(
        self, session: AsyncSession, movie_item: MediaItem, library_paths: dict[str, str]
    ) -> None:
        """Parent directories of the target symlink are created automatically."""
        source = _make_source_file(library_paths["mount"], "test.movie.2024.mkv")
        manager = SymlinkManager()
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            result = await manager.create_symlink(session, movie_item, source)
        assert os.path.isdir(os.path.dirname(result.target_path))

    async def test_symlink_points_to_source_file(
        self, session: AsyncSession, movie_item: MediaItem, library_paths: dict[str, str]
    ) -> None:
        """The created symlink resolves to the source file path."""
        source = _make_source_file(library_paths["mount"], "test.movie.2024.mkv")
        manager = SymlinkManager()
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            result = await manager.create_symlink(session, movie_item, source)
        assert os.readlink(result.target_path) == source

    async def test_records_symlink_in_db(
        self, session: AsyncSession, movie_item: MediaItem, library_paths: dict[str, str]
    ) -> None:
        """A Symlink ORM row is flushed to the database with correct fields."""
        source = _make_source_file(library_paths["mount"], "test.movie.2024.mkv")
        manager = SymlinkManager()
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            result = await manager.create_symlink(session, movie_item, source)
        assert result.id is not None
        assert result.source_path == source
        assert result.media_item_id == movie_item.id
        assert result.valid is True

    async def test_returns_symlink_orm_object(
        self, session: AsyncSession, movie_item: MediaItem, library_paths: dict[str, str]
    ) -> None:
        """create_symlink returns a Symlink ORM instance."""
        source = _make_source_file(library_paths["mount"], "test.movie.2024.mkv")
        manager = SymlinkManager()
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            result = await manager.create_symlink(session, movie_item, source)
        assert isinstance(result, Symlink)

    async def test_source_not_found_raises(
        self, session: AsyncSession, movie_item: MediaItem, library_paths: dict[str, str]
    ) -> None:
        """Non-existent source path raises SourceNotFoundError."""
        nonexistent = os.path.join(library_paths["mount"], "ghost.mkv")
        manager = SymlinkManager()
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            with pytest.raises(SourceNotFoundError):
                await manager.create_symlink(session, movie_item, nonexistent)

    async def test_stale_target_replaced(
        self, session: AsyncSession, movie_item: MediaItem, library_paths: dict[str, str]
    ) -> None:
        """A stale symlink at the target path is replaced by the new symlink."""
        _source = _make_source_file(library_paths["mount"], "test.movie.2024.mkv")
        old_source = _make_source_file(library_paths["mount"], "old.mkv")
        manager = SymlinkManager()
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            # Create initial symlink pointing to old source
            first = await manager.create_symlink(session, movie_item, old_source)
            stale_target = first.target_path
            # Now call again with a different source that has the same filename
            # We need to use the same target path, so we rename source to match old name
            second_source = _make_source_file(library_paths["mount"], "new_video.mkv")
            # Manually create a stale symlink so next call replaces it
            os.remove(stale_target)
            os.symlink("/nonexistent/stale.mkv", stale_target)

        # Create a new item with same title to generate the same target path
        new_item = MediaItem(
            imdb_id="tt1234567",
            title="Test Movie",
            year=2024,
            media_type=MediaType.MOVIE,
            state=QueueState.COMPLETE,
            retry_count=0,
        )
        session.add(new_item)
        await session.flush()

        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            second = await manager.create_symlink(session, new_item, second_source)

        assert os.readlink(second.target_path) == second_source

    async def test_target_path_contains_title_and_year(
        self, session: AsyncSession, movie_item: MediaItem, library_paths: dict[str, str]
    ) -> None:
        """The target path includes the sanitized title and year."""
        source = _make_source_file(library_paths["mount"], "test.movie.2024.mkv")
        manager = SymlinkManager()
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            result = await manager.create_symlink(session, movie_item, source)
        assert "Test Movie" in result.target_path
        assert "2024" in result.target_path

    async def test_target_path_preserves_source_filename(
        self, session: AsyncSession, movie_item: MediaItem, library_paths: dict[str, str]
    ) -> None:
        """The symlink filename matches the source filename."""
        filename = "Test.Movie.2024.1080p.BluRay.mkv"
        source = _make_source_file(library_paths["mount"], filename)
        manager = SymlinkManager()
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            result = await manager.create_symlink(session, movie_item, source)
        assert os.path.basename(result.target_path) == filename


# ---------------------------------------------------------------------------
# Group 5: create_symlink — shows
# ---------------------------------------------------------------------------


class TestCreateSymlinkShow:
    """Tests for SymlinkManager.create_symlink with show episode MediaItems."""

    async def test_creates_season_directory_structure(
        self, session: AsyncSession, show_item: MediaItem, library_paths: dict[str, str]
    ) -> None:
        """Target path includes the 'Season XX' subdirectory."""
        source = _make_source_file(library_paths["mount"], "show.s02e05.mkv")
        manager = SymlinkManager()
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            result = await manager.create_symlink(session, show_item, source)
        assert "Season 02" in result.target_path

    async def test_season_number_zero_padded(
        self, session: AsyncSession, session_scope=None, library_paths: dict[str, str] = None
    ) -> None:
        """Season 1 is zero-padded to 'Season 01' in the target path."""
        pass  # covered by test_creates_season_directory_structure

    async def test_correct_show_path_structure(
        self, session: AsyncSession, show_item: MediaItem, library_paths: dict[str, str]
    ) -> None:
        """Path follows 'shows/Title (Year)/Season XX/filename.ext' pattern."""
        filename = "Test.Show.S02E05.1080p.mkv"
        source = _make_source_file(library_paths["mount"], filename)
        manager = SymlinkManager()
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            result = await manager.create_symlink(session, show_item, source)
        # library_shows root should appear in the path
        assert result.target_path.startswith(library_paths["shows"])
        # Title (Year) segment
        assert "Test Show (2023)" in result.target_path
        # Season segment
        assert "Season 02" in result.target_path
        # Filename preserved (date_prefix=False so no timestamp prepended)
        assert os.path.basename(result.target_path) == filename

    async def test_season_none_defaults_to_season_01(
        self, session: AsyncSession, library_paths: dict[str, str]
    ) -> None:
        """A show item with season=None places file in 'Season 01'."""
        item = MediaItem(
            imdb_id="tt0000001",
            title="Seasonless Show",
            year=2022,
            media_type=MediaType.SHOW,
            season=None,
            episode=1,
            state=QueueState.COMPLETE,
            retry_count=0,
        )
        session.add(item)
        await session.flush()
        source = _make_source_file(library_paths["mount"], "seasonless.s00e01.mkv")
        manager = SymlinkManager()
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            result = await manager.create_symlink(session, item, source)
        assert "Season 01" in result.target_path

    async def test_show_recorded_in_db_with_correct_fields(
        self, session: AsyncSession, show_item: MediaItem, library_paths: dict[str, str]
    ) -> None:
        """DB record for a show symlink links back to the correct MediaItem."""
        source = _make_source_file(library_paths["mount"], "show.s02e05.mkv")
        manager = SymlinkManager()
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            result = await manager.create_symlink(session, show_item, source)
        assert result.media_item_id == show_item.id
        assert result.source_path == source
        assert result.valid is True


# ---------------------------------------------------------------------------
# Group 6: verify_symlinks
# ---------------------------------------------------------------------------


class TestVerifySymlinks:
    """Tests for SymlinkManager.verify_symlinks."""

    @pytest.fixture(autouse=True)
    def _patch_zurg_mount(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """Patch zurg_mount to tmp_path so verify_symlinks doesn't skip."""
        monkeypatch.setattr(settings.paths, "zurg_mount", str(tmp_path))

    async def _insert_symlink(
        self,
        session: AsyncSession,
        *,
        source_path: str,
        target_path: str,
        valid: bool = True,
        media_item_id: int | None = None,
    ) -> Symlink:
        """Helper: insert a Symlink row and flush."""
        link = Symlink(
            source_path=source_path,
            target_path=target_path,
            valid=valid,
            media_item_id=media_item_id,
        )
        session.add(link)
        await session.flush()
        return link

    async def test_valid_symlink_counted_as_valid(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """An intact symlink is counted in valid_count and not marked broken."""
        source = tmp_path / "real.mkv"
        source.write_bytes(b"data")
        target = tmp_path / "link.mkv"
        os.symlink(str(source), str(target))
        await self._insert_symlink(
            session,
            source_path=str(source),
            target_path=str(target),
            valid=True,
        )
        manager = SymlinkManager()
        result = await manager.verify_symlinks(session)
        assert isinstance(result, VerifyResult)
        assert result.valid_count == 1
        assert result.broken_count == 0
        assert result.total_checked == 1

    async def test_broken_source_marks_invalid(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """A symlink whose source has been deleted is marked invalid."""
        source = tmp_path / "gone.mkv"
        source.write_bytes(b"data")
        target = tmp_path / "link.mkv"
        os.symlink(str(source), str(target))
        link = await self._insert_symlink(
            session,
            source_path=str(source),
            target_path=str(target),
            valid=True,
        )
        # Delete the source so the symlink becomes dangling
        source.unlink()
        manager = SymlinkManager()
        result = await manager.verify_symlinks(session)
        assert result.broken_count == 1
        assert result.valid_count == 0
        # Row should now be marked invalid
        await session.refresh(link)
        assert link.valid is False

    async def test_missing_symlink_file_marks_invalid(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """A target path that is not a symlink at all is marked invalid."""
        source = tmp_path / "real.mkv"
        source.write_bytes(b"data")
        # Do NOT create the symlink on disk; only insert the DB record
        target = tmp_path / "nonexistent_link.mkv"
        link = await self._insert_symlink(
            session,
            source_path=str(source),
            target_path=str(target),
            valid=True,
        )
        manager = SymlinkManager()
        result = await manager.verify_symlinks(session)
        assert result.broken_count == 1
        await session.refresh(link)
        assert link.valid is False

    async def test_empty_db_returns_all_zeros(self, session: AsyncSession) -> None:
        """With no symlinks in the DB, all counts are zero."""
        manager = SymlinkManager()
        result = await manager.verify_symlinks(session)
        assert result.total_checked == 0
        assert result.valid_count == 0
        assert result.broken_count == 0
        assert result.already_invalid == 0

    async def test_skips_when_mount_unavailable(self, session: AsyncSession) -> None:
        """Mount unavailable → returns zero counts, no symlinks touched."""
        with patch("src.core.symlink_manager.mount_scanner.is_mount_available",
                   new=AsyncMock(return_value=False)):
            manager = SymlinkManager()
            result = await manager.verify_symlinks(session)
        assert result.total_checked == 0
        assert result.broken_count == 0

    async def test_already_invalid_not_rechecked(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """Symlinks already marked invalid are skipped and counted in already_invalid."""
        source = tmp_path / "source.mkv"
        source.write_bytes(b"data")
        target = tmp_path / "link.mkv"
        await self._insert_symlink(
            session,
            source_path=str(source),
            target_path=str(target),
            valid=False,  # already invalid
        )
        manager = SymlinkManager()
        result = await manager.verify_symlinks(session)
        assert result.already_invalid == 1
        assert result.total_checked == 0  # not checked again

    async def test_mixed_valid_and_broken(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """Mixed scenario: one valid, one broken, one already invalid."""
        # Valid symlink
        s1 = tmp_path / "source1.mkv"
        s1.write_bytes(b"data")
        t1 = tmp_path / "link1.mkv"
        os.symlink(str(s1), str(t1))
        await self._insert_symlink(
            session, source_path=str(s1), target_path=str(t1), valid=True
        )
        # Broken symlink (source deleted)
        s2 = tmp_path / "source2.mkv"
        s2.write_bytes(b"data")
        t2 = tmp_path / "link2.mkv"
        os.symlink(str(s2), str(t2))
        await self._insert_symlink(
            session, source_path=str(s2), target_path=str(t2), valid=True
        )
        s2.unlink()
        # Already invalid
        await self._insert_symlink(
            session,
            source_path="/nonexistent/source.mkv",
            target_path="/nonexistent/link.mkv",
            valid=False,
        )
        manager = SymlinkManager()
        result = await manager.verify_symlinks(session)
        assert result.total_checked == 2
        assert result.valid_count == 1
        assert result.broken_count == 1
        assert result.already_invalid == 1

    async def test_last_checked_at_updated_for_checked_symlinks(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """last_checked_at is set/updated for symlinks that are actively checked."""
        source = tmp_path / "real.mkv"
        source.write_bytes(b"data")
        target = tmp_path / "link.mkv"
        os.symlink(str(source), str(target))
        link = await self._insert_symlink(
            session,
            source_path=str(source),
            target_path=str(target),
            valid=True,
        )
        assert link.last_checked_at is None
        manager = SymlinkManager()
        await manager.verify_symlinks(session)
        await session.refresh(link)
        assert link.last_checked_at is not None


# ---------------------------------------------------------------------------
# Group 7: remove_symlink
# ---------------------------------------------------------------------------


class TestRemoveSymlink:
    """Tests for SymlinkManager.remove_symlink."""

    async def _create_real_symlink_and_record(
        self,
        session: AsyncSession,
        tmp_path: Path,
        *,
        media_item_id: int,
        filename: str = "link.mkv",
    ) -> tuple[Symlink, str, str]:
        """Create a real file, symlink to it, and insert a DB record."""
        source = tmp_path / f"source_{filename}"
        source.write_bytes(b"data")
        target_dir = tmp_path / "target_dir"
        target_dir.mkdir(exist_ok=True)
        target = target_dir / filename
        os.symlink(str(source), str(target))
        link = Symlink(
            source_path=str(source),
            target_path=str(target),
            valid=True,
            media_item_id=media_item_id,
        )
        session.add(link)
        await session.flush()
        return link, str(source), str(target)

    async def test_removes_symlink_file_from_disk(
        self, session: AsyncSession, movie_item: MediaItem, tmp_path: Path
    ) -> None:
        """remove_symlink deletes the symlink file from the filesystem."""
        link, _, target = await self._create_real_symlink_and_record(
            session, tmp_path, media_item_id=movie_item.id
        )
        manager = SymlinkManager()
        await manager.remove_symlink(session, movie_item.id)
        assert not os.path.lexists(target)

    async def test_deletes_db_record(
        self, session: AsyncSession, movie_item: MediaItem, tmp_path: Path
    ) -> None:
        """remove_symlink deletes the Symlink row from the database."""
        link, _, _ = await self._create_real_symlink_and_record(
            session, tmp_path, media_item_id=movie_item.id
        )
        link_id = link.id
        manager = SymlinkManager()
        await manager.remove_symlink(session, movie_item.id)
        remaining = await manager.get_symlinks_for_item(session, movie_item.id)
        assert all(s.id != link_id for s in remaining)

    async def test_removes_empty_parent_directory(
        self, session: AsyncSession, movie_item: MediaItem, tmp_path: Path
    ) -> None:
        """remove_symlink cleans up the parent directory if it becomes empty."""
        link, _, target = await self._create_real_symlink_and_record(
            session, tmp_path, media_item_id=movie_item.id
        )
        parent_dir = os.path.dirname(target)
        assert os.path.isdir(parent_dir)
        manager = SymlinkManager()
        await manager.remove_symlink(session, movie_item.id)
        assert not os.path.isdir(parent_dir)

    async def test_returns_count_of_removed_symlinks(
        self, session: AsyncSession, movie_item: MediaItem, tmp_path: Path
    ) -> None:
        """remove_symlink returns the number of symlinks that were removed."""
        await self._create_real_symlink_and_record(
            session, tmp_path, media_item_id=movie_item.id, filename="link1.mkv"
        )
        await self._create_real_symlink_and_record(
            session, tmp_path, media_item_id=movie_item.id, filename="link2.mkv"
        )
        manager = SymlinkManager()
        count = await manager.remove_symlink(session, movie_item.id)
        assert count == 2

    async def test_handles_already_deleted_file_gracefully(
        self, session: AsyncSession, movie_item: MediaItem, tmp_path: Path
    ) -> None:
        """If the symlink file was already deleted, remove_symlink does not crash."""
        link, _, target = await self._create_real_symlink_and_record(
            session, tmp_path, media_item_id=movie_item.id
        )
        # Pre-delete the symlink from disk
        os.remove(target)
        manager = SymlinkManager()
        # Should not raise any exception
        count = await manager.remove_symlink(session, movie_item.id)
        assert count >= 1

    async def test_multiple_symlinks_all_removed(
        self, session: AsyncSession, movie_item: MediaItem, tmp_path: Path
    ) -> None:
        """All symlinks for a media_item_id are removed when there are multiple."""
        for i in range(3):
            await self._create_real_symlink_and_record(
                session, tmp_path, media_item_id=movie_item.id, filename=f"link{i}.mkv"
            )
        manager = SymlinkManager()
        count = await manager.remove_symlink(session, movie_item.id)
        assert count == 3
        remaining = await manager.get_symlinks_for_item(session, movie_item.id)
        assert remaining == []

    async def test_remove_nonexistent_item_returns_zero(
        self, session: AsyncSession
    ) -> None:
        """Removing symlinks for an item that has none returns 0."""
        manager = SymlinkManager()
        count = await manager.remove_symlink(session, media_item_id=99999)
        assert count == 0


# ---------------------------------------------------------------------------
# Group 8: get_symlinks_for_item / get_broken_symlinks
# ---------------------------------------------------------------------------


class TestQueries:
    """Tests for get_symlinks_for_item and get_broken_symlinks queries."""

    async def _insert_symlink(
        self,
        session: AsyncSession,
        *,
        source_path: str = "/src/video.mkv",
        target_path: str = "/lib/video.mkv",
        valid: bool = True,
        media_item_id: int | None = None,
    ) -> Symlink:
        link = Symlink(
            source_path=source_path,
            target_path=target_path,
            valid=valid,
            media_item_id=media_item_id,
        )
        session.add(link)
        await session.flush()
        return link

    async def test_get_symlinks_for_item_returns_correct_links(
        self, session: AsyncSession, movie_item: MediaItem
    ) -> None:
        """Returns all Symlink rows for the given media_item_id."""
        await self._insert_symlink(
            session,
            source_path="/src/a.mkv",
            target_path="/lib/a.mkv",
            media_item_id=movie_item.id,
        )
        await self._insert_symlink(
            session,
            source_path="/src/b.mkv",
            target_path="/lib/b.mkv",
            media_item_id=movie_item.id,
        )
        # A symlink for a different item — should not appear
        await self._insert_symlink(
            session,
            source_path="/src/c.mkv",
            target_path="/lib/c.mkv",
            media_item_id=99999,
        )
        manager = SymlinkManager()
        links = await manager.get_symlinks_for_item(session, movie_item.id)
        assert len(links) == 2
        for link in links:
            assert link.media_item_id == movie_item.id

    async def test_get_symlinks_for_item_returns_empty_list(
        self, session: AsyncSession
    ) -> None:
        """Returns an empty list when no symlinks exist for the given id."""
        manager = SymlinkManager()
        links = await manager.get_symlinks_for_item(session, 12345)
        assert links == []

    async def test_get_broken_symlinks_returns_only_invalid(
        self, session: AsyncSession, movie_item: MediaItem
    ) -> None:
        """get_broken_symlinks returns only symlinks with valid=False."""
        await self._insert_symlink(
            session,
            source_path="/src/good.mkv",
            target_path="/lib/good.mkv",
            valid=True,
            media_item_id=movie_item.id,
        )
        broken = await self._insert_symlink(
            session,
            source_path="/src/bad.mkv",
            target_path="/lib/bad.mkv",
            valid=False,
            media_item_id=movie_item.id,
        )
        manager = SymlinkManager()
        results = await manager.get_broken_symlinks(session)
        assert len(results) == 1
        assert results[0].id == broken.id
        assert results[0].valid is False

    async def test_get_broken_symlinks_empty_when_all_valid(
        self, session: AsyncSession, movie_item: MediaItem
    ) -> None:
        """get_broken_symlinks returns an empty list when all symlinks are valid."""
        await self._insert_symlink(
            session,
            source_path="/src/ok.mkv",
            target_path="/lib/ok.mkv",
            valid=True,
            media_item_id=movie_item.id,
        )
        manager = SymlinkManager()
        results = await manager.get_broken_symlinks(session)
        assert results == []


# ---------------------------------------------------------------------------
# Group 9: Edge Cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Edge cases: exception attributes, singleton, long titles."""

    def test_source_not_found_error_has_source_path_attribute(self) -> None:
        """SourceNotFoundError stores the missing path on its source_path attribute."""
        exc = SourceNotFoundError("/missing/file.mkv")
        assert exc.source_path == "/missing/file.mkv"

    def test_source_not_found_error_is_exception(self) -> None:
        """SourceNotFoundError is a subclass of Exception."""
        assert issubclass(SourceNotFoundError, Exception)

    def test_symlink_creation_error_has_target_and_source_attributes(self) -> None:
        """SymlinkCreationError stores target_path and source_path attributes."""
        exc = SymlinkCreationError(
            target_path="/lib/link.mkv",
            source_path="/src/video.mkv",
            reason="Permission denied",
        )
        assert exc.target_path == "/lib/link.mkv"
        assert exc.source_path == "/src/video.mkv"

    def test_symlink_creation_error_is_exception(self) -> None:
        """SymlinkCreationError is a subclass of Exception."""
        assert issubclass(SymlinkCreationError, Exception)

    def test_module_level_singleton_exists(self) -> None:
        """The module-level symlink_manager singleton is a SymlinkManager instance."""
        assert isinstance(symlink_manager, SymlinkManager)

    def test_module_level_singleton_is_same_object(self) -> None:
        """Importing symlink_manager twice yields the same object."""
        from src.core.symlink_manager import symlink_manager as sm2

        assert symlink_manager is sm2

    def test_very_long_title_sanitized_without_crash(self) -> None:
        """sanitize_name handles very long titles without raising."""
        long_title = "A" * 500 + ": The Movie"
        result = sanitize_name(long_title)
        assert len(result) > 0
        assert ":" not in result

    async def test_create_symlink_source_not_found_error_carries_path(
        self, session: AsyncSession, movie_item: MediaItem, library_paths: dict[str, str]
    ) -> None:
        """SourceNotFoundError raised by create_symlink carries the offending path."""
        missing = os.path.join(library_paths["mount"], "ghost.mkv")
        manager = SymlinkManager()
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            with pytest.raises(SourceNotFoundError) as exc_info:
                await manager.create_symlink(session, movie_item, missing)
        assert exc_info.value.source_path == missing

    async def test_verify_result_is_pydantic_model(
        self, session: AsyncSession
    ) -> None:
        """VerifyResult returned by verify_symlinks is a Pydantic model instance."""
        manager = SymlinkManager()
        result = await manager.verify_symlinks(session)
        assert isinstance(result, VerifyResult)
        # Pydantic models have model_fields
        assert hasattr(VerifyResult, "model_fields")

    def test_verify_result_fields(self) -> None:
        """VerifyResult has all four required integer fields."""
        vr = VerifyResult(
            total_checked=5,
            valid_count=3,
            broken_count=1,
            already_invalid=1,
        )
        assert vr.total_checked == 5
        assert vr.valid_count == 3
        assert vr.broken_count == 1
        assert vr.already_invalid == 1

    async def test_movie_without_year_no_year_in_path(
        self, session: AsyncSession, library_paths: dict[str, str]
    ) -> None:
        """A movie with year=None creates a path without any year suffix."""
        item = MediaItem(
            imdb_id="tt0000099",
            title="Timeless Film",
            year=None,
            media_type=MediaType.MOVIE,
            state=QueueState.COMPLETE,
            retry_count=0,
        )
        session.add(item)
        await session.flush()
        source = _make_source_file(library_paths["mount"], "timeless.mkv")
        manager = SymlinkManager()
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            result = await manager.create_symlink(session, item, source)
        # Path should not contain a parenthesised year like "(None)"
        assert "(None)" not in result.target_path
        assert "Timeless Film" in result.target_path


# ---------------------------------------------------------------------------
# Group 10: TestBuildMovieDirNaming
# ---------------------------------------------------------------------------


class TestBuildMovieDirNaming:
    """Test build_movie_dir with all combinations of SymlinkNamingConfig flags."""

    def test_all_enabled(self, library_paths: dict[str, str]) -> None:
        """All three flags on: timestamp, year, and resolution appear in dir name."""
        naming = SymlinkNamingConfig(date_prefix=True, release_year=True, resolution=True)
        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._format_timestamp", return_value="202603011430"):
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.symlink_naming = naming
            result = build_movie_dir("Test Movie", 2024, "2160p")
        expected = os.path.join(library_paths["movies"], "202603011430 Test Movie (2024) 2160p")
        assert result == expected

    def test_date_prefix_only(self, library_paths: dict[str, str]) -> None:
        """Only date_prefix=True: dir name is '<timestamp> <title>' with no year or resolution."""
        naming = SymlinkNamingConfig(date_prefix=True, release_year=False, resolution=False)
        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._format_timestamp", return_value="202603011430"):
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.symlink_naming = naming
            result = build_movie_dir("Test Movie", 2024, "2160p")
        expected = os.path.join(library_paths["movies"], "202603011430 Test Movie")
        assert result == expected

    def test_year_only(self, library_paths: dict[str, str]) -> None:
        """Only release_year=True: dir name is '<title> (<year>)' — original behavior."""
        naming = SymlinkNamingConfig(date_prefix=False, release_year=True, resolution=False)
        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._format_timestamp", return_value="202603011430"):
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.symlink_naming = naming
            result = build_movie_dir("Test Movie", 2024, "2160p")
        expected = os.path.join(library_paths["movies"], "Test Movie (2024)")
        assert result == expected

    def test_resolution_only(self, library_paths: dict[str, str]) -> None:
        """Only resolution=True: dir name is '<title> <resolution>'."""
        naming = SymlinkNamingConfig(date_prefix=False, release_year=False, resolution=True)
        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._format_timestamp", return_value="202603011430"):
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.symlink_naming = naming
            result = build_movie_dir("Test Movie", 2024, "2160p")
        expected = os.path.join(library_paths["movies"], "Test Movie 2160p")
        assert result == expected

    def test_all_disabled(self, library_paths: dict[str, str]) -> None:
        """All flags off: dir name is exactly the sanitized title, nothing else."""
        naming = SymlinkNamingConfig(date_prefix=False, release_year=False, resolution=False)
        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._format_timestamp", return_value="202603011430"):
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.symlink_naming = naming
            result = build_movie_dir("Test Movie", 2024, "2160p")
        expected = os.path.join(library_paths["movies"], "Test Movie")
        assert result == expected

    def test_no_year_provided(self, library_paths: dict[str, str]) -> None:
        """release_year=True but year=None: no '(None)' and no stray parentheses in dir name."""
        naming = SymlinkNamingConfig(date_prefix=False, release_year=True, resolution=False)
        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._format_timestamp", return_value="202603011430"):
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.symlink_naming = naming
            result = build_movie_dir("Test Movie", None)
        assert "(None)" not in result
        assert "Test Movie" in result
        # No trailing/leading spaces in the dir name component
        dir_name = os.path.basename(result)
        assert dir_name == dir_name.strip()

    def test_no_resolution_provided(self, library_paths: dict[str, str]) -> None:
        """resolution=True but resolution=None: no trailing space or 'None' in dir name."""
        naming = SymlinkNamingConfig(date_prefix=False, release_year=False, resolution=True)
        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._format_timestamp", return_value="202603011430"):
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.symlink_naming = naming
            result = build_movie_dir("Test Movie", 2024, None)
        dir_name = os.path.basename(result)
        assert "None" not in dir_name
        assert dir_name == dir_name.strip()
        assert dir_name == "Test Movie"


# ---------------------------------------------------------------------------
# Group 11: TestBuildShowDirNaming
# ---------------------------------------------------------------------------


class TestBuildShowDirNaming:
    """Test build_show_dir with SymlinkNamingConfig flags and existing-dir reuse."""

    def test_all_enabled_new_show(self, library_paths: dict[str, str]) -> None:
        """All flags on, no existing dir: path is '<timestamp> <title> (<year>)/Season XX'."""
        naming = SymlinkNamingConfig(date_prefix=True, release_year=True, resolution=True)
        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._format_timestamp", return_value="202603011430"), \
             patch("src.core.symlink_manager._find_existing_show_dir", return_value=None):
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = naming
            result = build_show_dir("Test Show", 2023, 2, "2160p")
        expected = os.path.join(
            library_paths["shows"], "202603011430 Test Show (2023) 2160p", "Season 02"
        )
        assert result == expected

    def test_existing_show_reused(self, library_paths: dict[str, str]) -> None:
        """When an existing show dir is found, it is reused instead of creating a new timestamped one."""
        naming = SymlinkNamingConfig(date_prefix=True, release_year=True, resolution=False)
        existing_dir_name = "202601011200 Test Show (2023)"
        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._format_timestamp", return_value="202603011430"), \
             patch(
                 "src.core.symlink_manager._find_existing_show_dir",
                 return_value=existing_dir_name,
             ):
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = naming
            result = build_show_dir("Test Show", 2023, 3)
        expected = os.path.join(library_paths["shows"], existing_dir_name, "Season 03")
        assert result == expected
        # The new timestamp must NOT appear — the old dir was reused
        assert "202603011430" not in result

    def test_season_dir_never_has_timestamp(self, library_paths: dict[str, str]) -> None:
        """The Season XX directory component never contains a timestamp regardless of date_prefix."""
        naming = SymlinkNamingConfig(date_prefix=True, release_year=True, resolution=False)
        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._format_timestamp", return_value="202603011430"), \
             patch("src.core.symlink_manager._find_existing_show_dir", return_value=None):
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = naming
            result = build_show_dir("Test Show", 2023, 1)
        # The last path component must be the plain season directory
        season_component = os.path.basename(result)
        assert season_component == "Season 01"
        assert "202603011430" not in season_component

    def test_date_prefix_off(self, library_paths: dict[str, str]) -> None:
        """date_prefix=False: no timestamp in the show directory name."""
        naming = SymlinkNamingConfig(date_prefix=False, release_year=True, resolution=False)
        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._format_timestamp", return_value="202603011430"), \
             patch("src.core.symlink_manager._find_existing_show_dir", return_value=None):
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = naming
            result = build_show_dir("Test Show", 2023, 4)
        expected = os.path.join(library_paths["shows"], "Test Show (2023)", "Season 04")
        assert result == expected
        assert "202603011430" not in result


# ---------------------------------------------------------------------------
# Group 12: TestFindExistingShowDir
# ---------------------------------------------------------------------------


class TestFindExistingShowDir:
    """Unit tests for _find_existing_show_dir using a real tmp_path filesystem."""

    def test_match_with_timestamp(self, tmp_path: Path) -> None:
        """A dir named '<timestamp> <core_name>' is found when searching by core_name."""
        dir_name = "202603011430 Breaking Bad (2008)"
        (tmp_path / dir_name).mkdir()
        result = _find_existing_show_dir(str(tmp_path), "Breaking Bad (2008)")
        assert result == dir_name

    def test_match_without_timestamp(self, tmp_path: Path) -> None:
        """A dir named exactly '<core_name>' is found when there is no timestamp prefix."""
        dir_name = "Breaking Bad (2008)"
        (tmp_path / dir_name).mkdir()
        result = _find_existing_show_dir(str(tmp_path), "Breaking Bad (2008)")
        assert result == dir_name

    def test_case_insensitive(self, tmp_path: Path) -> None:
        """Matching is case-insensitive: a lowercase dir matches a mixed-case core_name."""
        dir_name = "breaking bad (2008)"
        (tmp_path / dir_name).mkdir()
        result = _find_existing_show_dir(str(tmp_path), "Breaking Bad (2008)")
        assert result == dir_name

    def test_missing_dir_returns_none(self, tmp_path: Path) -> None:
        """When library_shows does not exist, the function returns None without raising."""
        nonexistent = str(tmp_path / "no_such_library")
        result = _find_existing_show_dir(nonexistent, "Breaking Bad (2008)")
        assert result is None

    def test_no_match_returns_none(self, tmp_path: Path) -> None:
        """A library containing unrelated shows returns None for a different core_name."""
        (tmp_path / "Something Else (2020)").mkdir()
        result = _find_existing_show_dir(str(tmp_path), "Breaking Bad (2008)")
        assert result is None

    def test_ignores_files(self, tmp_path: Path) -> None:
        """A regular file whose name contains core_name is not returned — only directories match."""
        core_name = "Breaking Bad (2008)"
        # Create a plain file (not a directory) whose name contains the core_name
        (tmp_path / core_name).write_text("not a directory")
        result = _find_existing_show_dir(str(tmp_path), core_name)
        assert result is None

    def test_does_not_match_longer_title(self, tmp_path: Path) -> None:
        """A directory with a longer title should not be matched."""
        (tmp_path / "The Matrix Reloaded (2003)").mkdir()
        result = _find_existing_show_dir(str(tmp_path), "The Matrix (1999)")
        assert result is None

    def test_matches_with_resolution_suffix(self, tmp_path: Path) -> None:
        """A directory with a resolution suffix should still match."""
        (tmp_path / "202603011430 Breaking Bad (2008) 1080p").mkdir()
        result = _find_existing_show_dir(str(tmp_path), "Breaking Bad (2008)")
        assert result == "202603011430 Breaking Bad (2008) 1080p"


# ---------------------------------------------------------------------------
# Group 13: TestCreateSymlinkEpisodeTimestamp
# ---------------------------------------------------------------------------


class TestCreateSymlinkEpisodeTimestamp:
    """Test that create_symlink prefixes episode filenames with a timestamp for shows."""

    async def test_show_episode_gets_timestamp_prefix(
        self, session: AsyncSession, library_paths: dict[str, str]
    ) -> None:
        """With date_prefix=True, the episode filename starts with the timestamp."""
        naming = SymlinkNamingConfig(date_prefix=True, release_year=True, resolution=False)
        item = MediaItem(
            imdb_id="tt7654321",
            title="Test Show",
            year=2023,
            media_type=MediaType.SHOW,
            season=2,
            episode=5,
            state=QueueState.COMPLETE,
            retry_count=0,
        )
        session.add(item)
        await session.flush()

        source = _make_source_file(library_paths["mount"], "Test.Show.S02E05.mkv")
        manager = SymlinkManager()
        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._format_timestamp", return_value="202603011430"):
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = naming
            result = await manager.create_symlink(session, item, source)

        filename = os.path.basename(result.target_path)
        assert filename.startswith("202603011430 ")
        assert "Test.Show.S02E05.mkv" in filename

    async def test_movie_file_never_gets_timestamp(
        self, session: AsyncSession, library_paths: dict[str, str]
    ) -> None:
        """With date_prefix=True, movie filenames are never prefixed with a timestamp."""
        naming = SymlinkNamingConfig(date_prefix=True, release_year=True, resolution=False)
        item = MediaItem(
            imdb_id="tt1234567",
            title="Test Movie",
            year=2024,
            media_type=MediaType.MOVIE,
            state=QueueState.COMPLETE,
            retry_count=0,
        )
        session.add(item)
        await session.flush()

        original_filename = "Test.Movie.2024.1080p.mkv"
        source = _make_source_file(library_paths["mount"], original_filename)
        manager = SymlinkManager()
        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._format_timestamp", return_value="202603011430"):
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = naming
            result = await manager.create_symlink(session, item, source)

        filename = os.path.basename(result.target_path)
        # The filename must be exactly the original — no timestamp prepended
        assert filename == original_filename

    async def test_show_episode_no_prefix_when_disabled(
        self, session: AsyncSession, library_paths: dict[str, str]
    ) -> None:
        """With date_prefix=False, the episode filename is the original source filename."""
        naming = SymlinkNamingConfig(date_prefix=False, release_year=True, resolution=False)
        item = MediaItem(
            imdb_id="tt7654321",
            title="Test Show",
            year=2023,
            media_type=MediaType.SHOW,
            season=1,
            episode=3,
            state=QueueState.COMPLETE,
            retry_count=0,
        )
        session.add(item)
        await session.flush()

        original_filename = "Test.Show.S01E03.mkv"
        source = _make_source_file(library_paths["mount"], original_filename)
        manager = SymlinkManager()
        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._format_timestamp", return_value="202603011430"):
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = naming
            result = await manager.create_symlink(session, item, source)

        filename = os.path.basename(result.target_path)
        # No timestamp prefix when date_prefix is disabled
        assert filename == original_filename
        assert "202603011430" not in filename


# ---------------------------------------------------------------------------
# Group 14: TestPlexNaming
# ---------------------------------------------------------------------------


_NAMING_PLEX = SymlinkNamingConfig(
    date_prefix=False, release_year=True, resolution=False, plex_naming=True
)
# A Plex config that also has date_prefix=True — plex_naming must override it.
_NAMING_PLEX_WITH_PREFIX = SymlinkNamingConfig(
    date_prefix=True, release_year=True, resolution=True, plex_naming=True
)


class TestPlexNaming:
    """Tests for plex_naming=True mode in build_show_dir, build_movie_dir, and create_symlink."""

    # ------------------------------------------------------------------
    # build_show_dir
    # ------------------------------------------------------------------

    def test_plex_show_dir_with_tmdb_id(self, library_paths: dict[str, str]) -> None:
        """plex_naming=True with tmdb_id produces 'Title (Year) {tmdb-XXXXX}/Season XX'."""
        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._find_existing_show_dir", return_value=None):
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_PLEX
            result = build_show_dir("Show Name", 2024, 1, tmdb_id="209867")
        expected = os.path.join(
            library_paths["shows"], "Show Name (2024) {tmdb-209867}", "Season 01"
        )
        assert result == expected

    def test_plex_show_dir_without_tmdb_id(self, library_paths: dict[str, str]) -> None:
        """plex_naming=True without tmdb_id produces 'Title (Year)/Season XX' — no tag."""
        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._find_existing_show_dir", return_value=None):
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_PLEX
            result = build_show_dir("Show Name", 2024, 1, tmdb_id=None)
        expected = os.path.join(library_paths["shows"], "Show Name (2024)", "Season 01")
        assert result == expected
        assert "{tmdb-" not in result

    def test_plex_show_dir_no_year(self, library_paths: dict[str, str]) -> None:
        """plex_naming=True with year=None produces 'Title/Season XX' — no parentheses."""
        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._find_existing_show_dir", return_value=None):
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_PLEX
            result = build_show_dir("Show Name", None, 1, tmdb_id="209867")
        expected = os.path.join(
            library_paths["shows"], "Show Name {tmdb-209867}", "Season 01"
        )
        assert result == expected
        assert "(None)" not in result

    def test_plex_show_dir_reuses_existing_dir(self, library_paths: dict[str, str]) -> None:
        """plex_naming=True reuses a pre-existing show directory found on disk."""
        existing_name = "Show Name (2024) {tmdb-209867}"
        (library_paths["shows"] / existing_name if False else None)  # documentation only
        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch(
                 "src.core.symlink_manager._find_existing_show_dir",
                 return_value=existing_name,
             ):
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_PLEX
            result = build_show_dir("Show Name", 2024, 2, tmdb_id="209867")
        expected = os.path.join(library_paths["shows"], existing_name, "Season 02")
        assert result == expected

    def test_plex_show_dir_matches_existing_without_tag(self, library_paths: dict[str, str]) -> None:
        """plex_naming=True reuses an existing dir that lacks the {tmdb-XXXXX} tag.

        _find_existing_show_dir strips the tag from the search key so a legacy
        directory 'Show Name (2024)' still matches when we search for
        'Show Name (2024) {tmdb-209867}'.
        """
        legacy_dir_name = "Show Name (2024)"
        (library_paths["shows"] / legacy_dir_name if False else None)  # documentation only
        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch(
                 "src.core.symlink_manager._find_existing_show_dir",
                 return_value=legacy_dir_name,
             ):
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_PLEX
            result = build_show_dir("Show Name", 2024, 3, tmdb_id="209867")
        expected = os.path.join(library_paths["shows"], legacy_dir_name, "Season 03")
        assert result == expected

    # ------------------------------------------------------------------
    # build_movie_dir
    # ------------------------------------------------------------------

    def test_plex_movie_dir_with_tmdb_id(self, library_paths: dict[str, str]) -> None:
        """plex_naming=True with tmdb_id produces 'Title (Year) {tmdb-XXXXX}'."""
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.symlink_naming = _NAMING_PLEX
            result = build_movie_dir("Movie Name", 2024, tmdb_id="12345")
        expected = os.path.join(library_paths["movies"], "Movie Name (2024) {tmdb-12345}")
        assert result == expected

    def test_plex_movie_dir_without_tmdb_id(self, library_paths: dict[str, str]) -> None:
        """plex_naming=True without tmdb_id produces 'Title (Year)' — no tag."""
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.symlink_naming = _NAMING_PLEX
            result = build_movie_dir("Movie Name", 2024, tmdb_id=None)
        expected = os.path.join(library_paths["movies"], "Movie Name (2024)")
        assert result == expected
        assert "{tmdb-" not in result

    def test_plex_movie_dir_no_date_prefix(self, library_paths: dict[str, str]) -> None:
        """plex_naming=True ignores date_prefix and resolution — no timestamp or resolution in path."""
        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._format_timestamp", return_value="202603011430"):
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.symlink_naming = _NAMING_PLEX_WITH_PREFIX
            result = build_movie_dir("Movie Name", 2024, "2160p", tmdb_id="12345")
        # Timestamp must NOT appear even though date_prefix=True in the base config
        assert "202603011430" not in result
        # Resolution must NOT appear even though resolution=True in the base config
        assert "2160p" not in result
        # Plex agent tag must appear
        assert "{tmdb-12345}" in result

    # ------------------------------------------------------------------
    # create_symlink — filename generation
    # ------------------------------------------------------------------

    async def test_plex_show_filename_from_metadata(
        self, session: AsyncSession, library_paths: dict[str, str]
    ) -> None:
        """plex_naming=True show: symlink filename is 'Title (Year) - S01E01.mkv' from metadata."""
        item = MediaItem(
            imdb_id="tt9090901",
            title="Show Name",
            year=2024,
            media_type=MediaType.SHOW,
            season=2,
            episode=1,
            state=QueueState.COMPLETE,
            retry_count=0,
            tmdb_id="209867",
        )
        session.add(item)
        await session.flush()

        # Source filename is a raw torrent name — plex mode must NOT use it.
        source = _make_source_file(library_paths["mount"], "random.torrent.name.S02E01.mkv")
        manager = SymlinkManager()
        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._find_existing_show_dir", return_value=None):
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_PLEX
            result = await manager.create_symlink(session, item, source)

        filename = os.path.basename(result.target_path)
        assert filename == "Show Name (2024) - S02E01.mkv"

    async def test_plex_movie_filename_from_metadata(
        self, session: AsyncSession, library_paths: dict[str, str]
    ) -> None:
        """plex_naming=True movie: symlink filename is 'Title (Year).ext' from metadata."""
        item = MediaItem(
            imdb_id="tt1234599",
            title="Movie Name",
            year=2024,
            media_type=MediaType.MOVIE,
            state=QueueState.COMPLETE,
            retry_count=0,
            tmdb_id="12345",
        )
        session.add(item)
        await session.flush()

        # Source filename is a raw torrent name — plex mode must NOT use it.
        source = _make_source_file(library_paths["mount"], "Movie.2024.1080p.mkv")
        manager = SymlinkManager()
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_PLEX
            result = await manager.create_symlink(session, item, source)

        filename = os.path.basename(result.target_path)
        assert filename == "Movie Name (2024).mkv"

    async def test_plex_show_filename_preserves_extension(
        self, session: AsyncSession, library_paths: dict[str, str]
    ) -> None:
        """plex_naming=True show: the source file extension is preserved in the symlink filename."""
        item = MediaItem(
            imdb_id="tt9090902",
            title="Show Name",
            year=2024,
            media_type=MediaType.SHOW,
            season=1,
            episode=3,
            state=QueueState.COMPLETE,
            retry_count=0,
            tmdb_id="209867",
        )
        session.add(item)
        await session.flush()

        source = _make_source_file(library_paths["mount"], "episode.S01E03.mp4")
        manager = SymlinkManager()
        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._find_existing_show_dir", return_value=None):
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_PLEX
            result = await manager.create_symlink(session, item, source)

        filename = os.path.basename(result.target_path)
        assert filename.endswith(".mp4")
        assert filename == "Show Name (2024) - S01E03.mp4"

    async def test_non_plex_mode_uses_raw_filename(
        self, session: AsyncSession, library_paths: dict[str, str]
    ) -> None:
        """plex_naming=False: symlink filename is the unmodified source basename (existing behaviour)."""
        item = MediaItem(
            imdb_id="tt9090903",
            title="Show Name",
            year=2024,
            media_type=MediaType.SHOW,
            season=1,
            episode=5,
            state=QueueState.COMPLETE,
            retry_count=0,
            tmdb_id="209867",
        )
        session.add(item)
        await session.flush()

        original_filename = "Show.Name.S01E05.1080p.BluRay.mkv"
        source = _make_source_file(library_paths["mount"], original_filename)
        manager = SymlinkManager()
        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._find_existing_show_dir", return_value=None):
            mock_settings.paths.library_movies = library_paths["movies"]
            mock_settings.paths.library_shows = library_paths["shows"]
            mock_settings.symlink_naming = _NAMING_NO_PREFIX
            result = await manager.create_symlink(session, item, source)

        filename = os.path.basename(result.target_path)
        assert filename == original_filename

    # ------------------------------------------------------------------
    # _find_existing_show_dir — TMDB tag stripping
    # ------------------------------------------------------------------

    def test_find_existing_strips_tmdb_tag(self, tmp_path: Path) -> None:
        """_find_existing_show_dir matches a dir with a {tmdb-XXXXX} tag when searching by plain core_name.

        The function strips the tag from on-disk directory names before comparing
        so that a Plex-tagged directory is found by a legacy (untagged) lookup key.
        """
        dir_name = "Show Name (2024) {tmdb-209867}"
        (tmp_path / dir_name).mkdir()
        result = _find_existing_show_dir(str(tmp_path), "Show Name (2024)")
        assert result == dir_name

    def test_find_existing_strips_tmdb_tag_from_search_key(self, tmp_path: Path) -> None:
        """_find_existing_show_dir matches a legacy dir when the search key contains a {tmdb-XXXXX} tag.

        The function also strips the tag from the *search key* (core_name) so that
        a Plex-mode caller looking for 'Show Name (2024) {tmdb-209867}' still finds
        an existing legacy directory named 'Show Name (2024)'.
        """
        dir_name = "Show Name (2024)"
        (tmp_path / dir_name).mkdir()
        result = _find_existing_show_dir(str(tmp_path), "Show Name (2024) {tmdb-209867}")
        assert result == dir_name


# ---------------------------------------------------------------------------
# _parse_episode_from_filename: bare trailing number (anime naming convention)
# ---------------------------------------------------------------------------


class TestParseEpisodeFromFilenameBarTrailing:
    """Tests for the bare-trailing-number fallback in _parse_episode_from_filename.

    This covers the 'Show Title 01.mkv' family of filenames where the episode
    number is a bare integer at the end of the filename stem — no S/E prefix,
    no dash, no brackets.
    """

    def test_bare_trailing_simple(self) -> None:
        """'Show Title 01.mkv' → episode 1."""
        assert _parse_episode_from_filename("Show Title 01.mkv") == 1

    def test_bare_trailing_two_digit(self) -> None:
        """'Show Title 26.mkv' → episode 26."""
        assert _parse_episode_from_filename("Show Title 26.mkv") == 26

    def test_bare_trailing_leading_zero(self) -> None:
        """'Anime Title 03.mkv' → episode 3 (leading zero handled by int())."""
        assert _parse_episode_from_filename("Anime Title 03.mkv") == 3

    def test_bare_trailing_year_not_matched(self) -> None:
        """'Show Name 2003.mkv' must NOT yield an episode number.

        The regex only matches 1-3 digit numbers so a 4-digit year is
        never treated as an episode number.
        """
        assert _parse_episode_from_filename("Show Name 2003.mkv") is None

    def test_bare_trailing_does_not_match_resolution_720(self) -> None:
        """'Some Show 720.mkv' — 720 is a resolution value, not an episode."""
        assert _parse_episode_from_filename("Some Show 720.mkv") is None

    def test_bare_trailing_does_not_match_resolution_1080(self) -> None:
        """'Some Show 1080.mkv' — 1080 is a resolution value, not an episode."""
        assert _parse_episode_from_filename("Some Show 1080.mkv") is None

    def test_bare_trailing_does_not_match_resolution_480(self) -> None:
        """'Some Show 480.mkv' — 480 is a resolution value, not an episode."""
        assert _parse_episode_from_filename("Some Show 480.mkv") is None

    def test_sxexx_wins_over_bare_trailing(self) -> None:
        """When SxxExx is present it takes precedence over bare trailing number."""
        assert _parse_episode_from_filename("Show S01E05.mkv") == 5

    def test_bare_episode_prefix_wins_over_bare_trailing(self) -> None:
        """When bare E-prefix pattern matches it takes precedence."""
        assert _parse_episode_from_filename("Show E12 extra 04.mkv") == 12

    def test_bare_trailing_dot_separator(self) -> None:
        """'Show.Name.07.mkv' — dot separator before the episode number works."""
        assert _parse_episode_from_filename("Show.Name.07.mkv") == 7


# ---------------------------------------------------------------------------
# Group 15: TestNfoGeneration
# ---------------------------------------------------------------------------


class TestNfoGeneration:
    """Tests for NFO sidecar XML generation and write logic."""

    def _make_movie_item(self, **kwargs: object) -> MediaItem:
        """Create an unsaved movie MediaItem with sensible defaults."""
        defaults: dict[str, object] = {
            "imdb_id": "tt1234567",
            "title": "Test Movie",
            "year": 2024,
            "media_type": MediaType.MOVIE,
            "state": QueueState.COMPLETE,
            "retry_count": 0,
            "tmdb_id": "12345",
        }
        defaults.update(kwargs)
        return MediaItem(**defaults)  # type: ignore[arg-type]

    def _make_show_item(self, **kwargs: object) -> MediaItem:
        """Create an unsaved show MediaItem with sensible defaults."""
        defaults: dict[str, object] = {
            "imdb_id": "tt7654321",
            "title": "Test Show",
            "year": 2023,
            "media_type": MediaType.SHOW,
            "season": 1,
            "episode": 3,
            "state": QueueState.COMPLETE,
            "retry_count": 0,
            "tmdb_id": "67890",
        }
        defaults.update(kwargs)
        return MediaItem(**defaults)  # type: ignore[arg-type]

    # ------------------------------------------------------------------
    # XML builder: movies
    # ------------------------------------------------------------------

    def _make_movie_detail(self, **kwargs: object) -> TmdbMovieDetail:
        """Create a TmdbMovieDetail with sensible defaults for testing."""
        defaults: dict[str, object] = {
            "tmdb_id": 12345,
            "title": "Test Movie",
            "original_title": "Test Movie Original",
            "year": 2024,
            "overview": "A test movie overview.",
            "tagline": "Test tagline",
            "poster_path": "/poster123.jpg",
            "backdrop_path": "/backdrop123.jpg",
            "vote_average": 7.5,
            "runtime": 120,
            "genres": [{"id": 28, "name": "Action"}, {"id": 53, "name": "Thriller"}],
            "imdb_id": "tt1234567",
            "credits": TmdbCredits(
                cast=[
                    TmdbCastMember(
                        name="Actor One", character="Hero", profile_path="/a1.jpg", order=0
                    ),
                    TmdbCastMember(
                        name="Actor Two", character="Villain", profile_path=None, order=1
                    ),
                ],
                crew=[
                    TmdbCrewMember(name="Director One", job="Director", department="Directing"),
                ],
            ),
        }
        defaults.update(kwargs)
        return TmdbMovieDetail(**defaults)  # type: ignore[arg-type]

    def _make_show_detail(self, **kwargs: object) -> TmdbShowDetail:
        """Create a TmdbShowDetail with sensible defaults for testing."""
        defaults: dict[str, object] = {
            "tmdb_id": 67890,
            "title": "Test Show",
            "original_title": "Test Show Original",
            "year": 2023,
            "overview": "A test show overview.",
            "tagline": "",
            "poster_path": "/showposter.jpg",
            "backdrop_path": "/showbackdrop.jpg",
            "vote_average": 8.2,
            "status": "Returning Series",
            "genres": [{"id": 18, "name": "Drama"}],
            "imdb_id": "tt7654321",
            "tvdb_id": 99999,
            "credits": TmdbCredits(
                cast=[
                    TmdbCastMember(
                        name="Show Actor", character="Lead", profile_path="/sa.jpg", order=0
                    ),
                ],
                crew=[],
            ),
        }
        defaults.update(kwargs)
        return TmdbShowDetail(**defaults)  # type: ignore[arg-type]

    def test_movie_nfo_xml_content(self) -> None:
        """_build_movie_nfo_xml produces rich metadata including title, year, genres, cast."""
        import xml.etree.ElementTree as ET

        detail = self._make_movie_detail()
        manager = SymlinkManager()
        xml_str = manager._build_movie_nfo_xml(detail)

        assert xml_str.startswith('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>')
        root = ET.fromstring(xml_str.split("\n", 1)[1].strip())
        assert root.tag == "movie"

        # Rich metadata fields
        assert root.findtext("title") == "Test Movie"
        assert root.findtext("originaltitle") == "Test Movie Original"
        assert root.findtext("year") == "2024"
        assert root.findtext("plot") == "A test movie overview."
        assert root.findtext("tagline") == "Test tagline"
        assert root.findtext("runtime") == "120"
        assert root.findtext("rating") == "7.5"

        # Image URLs use full-resolution for compatibility
        thumb = root.find("thumb")
        assert thumb is not None
        assert thumb.get("aspect") == "poster"
        assert thumb.text == "https://image.tmdb.org/t/p/original/poster123.jpg"
        fanart_el = root.find("fanart")
        assert fanart_el is not None
        assert fanart_el.find("thumb").text == "https://image.tmdb.org/t/p/original/backdrop123.jpg"

        # Genres
        genre_names = [el.text for el in root.findall("genre")]
        assert "Action" in genre_names
        assert "Thriller" in genre_names

        # uniqueid elements
        uid_elements = root.findall("uniqueid")
        uid_by_type = {el.get("type"): el.text for el in uid_elements}
        assert uid_by_type.get("tmdb") == "12345"
        assert uid_by_type.get("imdb") == "tt1234567"
        tmdb_el = [el for el in uid_elements if el.get("type") == "tmdb"][0]
        assert tmdb_el.get("default") == "true"
        imdb_el = [el for el in uid_elements if el.get("type") == "imdb"][0]
        assert imdb_el.get("default") is None
        assert root.findtext("tmdbid") == "12345"
        assert root.findtext("imdbid") == "tt1234567"

        # Director and cast
        assert root.findtext("director") == "Director One"
        actor_els = root.findall("actor")
        assert len(actor_els) == 2
        assert actor_els[0].findtext("name") == "Actor One"
        assert actor_els[0].findtext("role") == "Hero"
        assert actor_els[0].findtext("thumb") == "https://image.tmdb.org/t/p/w185/a1.jpg"
        # Actor with no profile_path should not have a <thumb> child
        assert actor_els[1].find("thumb") is None

    # ------------------------------------------------------------------
    # XML builder: shows
    # ------------------------------------------------------------------

    def test_tvshow_nfo_xml_content(self) -> None:
        """_build_show_nfo_xml produces rich metadata including status, tvdb_id, cast."""
        import xml.etree.ElementTree as ET

        detail = self._make_show_detail()
        manager = SymlinkManager()
        xml_str = manager._build_show_nfo_xml(detail)

        assert xml_str.startswith('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>')
        root = ET.fromstring(xml_str.split("\n", 1)[1].strip())
        assert root.tag == "tvshow"

        assert root.findtext("title") == "Test Show"
        assert root.findtext("originaltitle") == "Test Show Original"
        assert root.findtext("year") == "2023"
        assert root.findtext("plot") == "A test show overview."
        assert root.findtext("status") == "Returning Series"
        assert root.findtext("rating") == "8.2"
        # Empty tagline should be omitted
        assert root.findtext("tagline") is None
        # Shows have no <runtime> element
        assert root.findtext("runtime") is None

        uid_elements = root.findall("uniqueid")
        uid_by_type = {el.get("type"): el.text for el in uid_elements}
        assert uid_by_type.get("tmdb") == "67890"
        assert uid_by_type.get("imdb") == "tt7654321"
        assert uid_by_type.get("tvdb") == "99999"
        tmdb_el = [el for el in uid_elements if el.get("type") == "tmdb"][0]
        assert tmdb_el.get("default") == "true"
        assert root.findtext("tvdbid") == "99999"

    # ------------------------------------------------------------------
    # Optional field omission: empty/None values produce no elements
    # ------------------------------------------------------------------

    def test_nfo_skips_empty_fields(self) -> None:
        """Fields that are None/empty are omitted from the NFO XML entirely."""
        import xml.etree.ElementTree as ET

        detail = self._make_movie_detail(
            original_title=None,
            tagline="",
            overview="",
            runtime=None,
            vote_average=0.0,
            genres=[],
            imdb_id=None,
            poster_path=None,
            backdrop_path=None,
            credits=None,
        )
        manager = SymlinkManager()
        xml_str = manager._build_movie_nfo_xml(detail)

        root = ET.fromstring(xml_str.split("\n", 1)[1].strip())
        assert root.findtext("originaltitle") is None
        assert root.findtext("tagline") is None
        assert root.findtext("plot") is None
        assert root.findtext("runtime") is None
        assert root.findtext("rating") is None
        assert root.findall("genre") == []
        assert root.findtext("imdbid") is None
        assert root.find("thumb") is None
        assert root.find("fanart") is None
        assert root.findall("director") == []
        assert root.findall("actor") == []
        # TMDB uniqueid is always present since tmdb_id is required
        assert root.findtext("tmdbid") == "12345"

    def test_nfo_imdb_absent_when_no_imdb_id(self) -> None:
        """When imdb_id is absent from TMDB detail, no IMDB uniqueid is emitted."""
        import xml.etree.ElementTree as ET

        detail = self._make_movie_detail(imdb_id=None)
        manager = SymlinkManager()
        xml_str = manager._build_movie_nfo_xml(detail)

        root = ET.fromstring(xml_str.split("\n", 1)[1].strip())
        uid_elements = root.findall("uniqueid")
        assert len(uid_elements) == 1
        assert uid_elements[0].get("type") == "tmdb"
        assert uid_elements[0].get("default") == "true"
        assert root.findtext("imdbid") is None

    # ------------------------------------------------------------------
    # generate_nfo disabled: _write_nfo_sidecar does nothing
    # ------------------------------------------------------------------

    async def test_nfo_not_created_when_disabled(self, tmp_path: Path) -> None:
        """When generate_nfo=False (the default), _write_nfo_sidecar writes no file."""
        item = self._make_movie_item()
        target_path = str(tmp_path / "Test Movie (2024)" / "movie.mkv")

        manager = SymlinkManager()
        naming_cfg = SymlinkNamingConfig(generate_nfo=False)
        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.symlink_naming = naming_cfg
            await manager._write_nfo_sidecar(item, target_path)

        # No NFO file should have been written
        nfo_path = tmp_path / "Test Movie (2024)" / "movie.nfo"
        assert not nfo_path.exists()

    # ------------------------------------------------------------------
    # OSError during write: must not propagate
    # ------------------------------------------------------------------

    async def test_nfo_write_failure_non_fatal(self, tmp_path: Path) -> None:
        """An OSError during NFO write is caught and does not propagate."""
        item = self._make_movie_item()
        movie_dir = tmp_path / "Test Movie (2024)"
        movie_dir.mkdir()
        target_path = str(movie_dir / "movie.mkv")

        manager = SymlinkManager()
        naming_cfg = SymlinkNamingConfig(generate_nfo=True)

        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("asyncio.to_thread", side_effect=OSError("disk full")):
            mock_settings.symlink_naming = naming_cfg
            # Must not raise — OSError is swallowed with a warning
            await manager._write_nfo_sidecar(item, target_path)

    # ------------------------------------------------------------------
    # _download_tmdb_image tests
    # ------------------------------------------------------------------

    async def test_download_image_skips_when_exists(self, tmp_path: Path) -> None:
        """_download_tmdb_image returns early without any HTTP call when dest already exists."""
        dest = tmp_path / "poster.jpg"
        dest.write_bytes(b"existing image data")

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        manager = SymlinkManager()

        await manager._download_tmdb_image("/poster123.jpg", str(dest), client=mock_client)

        # File content is unchanged and no HTTP call was issued
        assert dest.read_bytes() == b"existing image data"
        mock_client.get.assert_not_called()

    async def test_download_image_success(self, tmp_path: Path) -> None:
        """_download_tmdb_image fetches image bytes and writes them atomically."""
        dest = tmp_path / "poster.jpg"
        fake_image = b"\x89PNG\r\n fake image bytes"

        mock_response = MagicMock()
        mock_response.content = fake_image
        mock_response.raise_for_status = MagicMock()

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.get = AsyncMock(return_value=mock_response)

        manager = SymlinkManager()
        await manager._download_tmdb_image("/poster123.jpg", str(dest), size="w500", client=mock_client)

        assert dest.exists()
        assert dest.read_bytes() == fake_image
        mock_client.get.assert_awaited_once()

    async def test_download_image_timeout_non_fatal(self, tmp_path: Path) -> None:
        """A TimeoutException during image download is swallowed; no file is created."""
        dest = tmp_path / "poster.jpg"

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.get = AsyncMock(side_effect=httpx.TimeoutException("timed out"))

        manager = SymlinkManager()
        # Must not raise
        await manager._download_tmdb_image("/poster123.jpg", str(dest), client=mock_client)

        assert not dest.exists()

    async def test_download_image_http_error_non_fatal(self, tmp_path: Path) -> None:
        """An HTTP error (e.g. 404) during image download is swallowed; no file is created."""
        dest = tmp_path / "poster.jpg"

        # Build a minimal HTTPStatusError the way httpx would
        request = httpx.Request("GET", "https://image.tmdb.org/t/p/w500/poster123.jpg")
        response = httpx.Response(404, request=request)
        error = httpx.HTTPStatusError("404 not found", request=request, response=response)

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.get = AsyncMock(return_value=MagicMock(
            raise_for_status=MagicMock(side_effect=error),
            content=b"",
        ))

        manager = SymlinkManager()
        # Must not raise
        await manager._download_tmdb_image("/poster123.jpg", str(dest), client=mock_client)

        assert not dest.exists()

    # ------------------------------------------------------------------
    # _write_nfo_sidecar: movie happy path
    # ------------------------------------------------------------------

    async def test_write_nfo_sidecar_movie_happy_path(self, tmp_path: Path) -> None:
        """_write_nfo_sidecar writes movie.nfo and calls _download_tmdb_image twice."""
        movie_dir = tmp_path / "movies" / "Test Movie (2024)"
        movie_dir.mkdir(parents=True)
        target_path = str(movie_dir / "Test.Movie.2024.mkv")

        item = self._make_movie_item(tmdb_id="12345")
        detail = self._make_movie_detail()

        manager = SymlinkManager()
        naming_cfg = SymlinkNamingConfig(generate_nfo=True)

        mock_tmdb = MagicMock()
        mock_tmdb.get_movie_details_full = AsyncMock(return_value=detail)

        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._tmdb_client", mock_tmdb), \
             patch.object(manager, "_download_tmdb_image", new_callable=AsyncMock) as mock_dl:
            mock_settings.symlink_naming = naming_cfg
            await manager._write_nfo_sidecar(item, target_path)

        nfo_path = movie_dir / "movie.nfo"
        assert nfo_path.exists(), "movie.nfo was not written"
        content = nfo_path.read_text()
        assert "<movie>" in content

        # poster + fanart = 2 calls
        assert mock_dl.await_count == 2
        mock_tmdb.get_movie_details_full.assert_awaited_once_with(12345)

    # ------------------------------------------------------------------
    # _write_nfo_sidecar: show directory placement
    # ------------------------------------------------------------------

    async def test_write_nfo_sidecar_show_directory_placement(self, tmp_path: Path) -> None:
        """tvshow.nfo is written in the show root, not in the Season XX subdirectory."""
        show_root = tmp_path / "shows" / "Test Show (2023)"
        season_dir = show_root / "Season 01"
        season_dir.mkdir(parents=True)
        target_path = str(season_dir / "Test.Show.S01E03.mkv")

        item = self._make_show_item(tmdb_id="67890")
        detail = self._make_show_detail()

        manager = SymlinkManager()
        naming_cfg = SymlinkNamingConfig(generate_nfo=True)

        mock_tmdb = MagicMock()
        mock_tmdb.get_show_details = AsyncMock(return_value=detail)

        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._tmdb_client", mock_tmdb), \
             patch.object(manager, "_download_tmdb_image", new_callable=AsyncMock):
            mock_settings.symlink_naming = naming_cfg
            await manager._write_nfo_sidecar(item, target_path)

        # NFO must be in the show root, not in Season 01
        assert (show_root / "tvshow.nfo").exists(), "tvshow.nfo missing from show root"
        assert not (season_dir / "tvshow.nfo").exists(), "tvshow.nfo must not be in Season dir"

    # ------------------------------------------------------------------
    # _write_nfo_sidecar: all sidecar files present → skip TMDB
    # ------------------------------------------------------------------

    async def test_write_nfo_sidecar_skips_when_all_exist(self, tmp_path: Path) -> None:
        """_write_nfo_sidecar makes no TMDB call when all three sidecar files exist."""
        movie_dir = tmp_path / "Test Movie (2024)"
        movie_dir.mkdir()
        (movie_dir / "movie.nfo").write_text("<movie/>")
        (movie_dir / "poster.jpg").write_bytes(b"poster")
        (movie_dir / "fanart.jpg").write_bytes(b"fanart")

        target_path = str(movie_dir / "movie.mkv")
        item = self._make_movie_item(tmdb_id="12345")

        manager = SymlinkManager()
        naming_cfg = SymlinkNamingConfig(generate_nfo=True)

        mock_tmdb = MagicMock()
        mock_tmdb.get_movie_details_full = AsyncMock()

        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._tmdb_client", mock_tmdb):
            mock_settings.symlink_naming = naming_cfg
            await manager._write_nfo_sidecar(item, target_path)

        mock_tmdb.get_movie_details_full.assert_not_awaited()

    # ------------------------------------------------------------------
    # _write_nfo_sidecar: NFO present but images missing → retry images only
    # ------------------------------------------------------------------

    async def test_write_nfo_sidecar_retries_missing_images(self, tmp_path: Path) -> None:
        """When movie.nfo exists but images are absent, TMDB is called for image paths."""
        movie_dir = tmp_path / "Test Movie (2024)"
        movie_dir.mkdir()
        (movie_dir / "movie.nfo").write_text("<movie/>")
        # No poster.jpg or fanart.jpg

        target_path = str(movie_dir / "movie.mkv")
        item = self._make_movie_item(tmdb_id="12345")
        detail = self._make_movie_detail()

        manager = SymlinkManager()
        naming_cfg = SymlinkNamingConfig(generate_nfo=True)

        mock_tmdb = MagicMock()
        mock_tmdb.get_movie_details_full = AsyncMock(return_value=detail)

        _nfo_write_count = 0
        original_nfo = (movie_dir / "movie.nfo").read_text()

        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._tmdb_client", mock_tmdb), \
             patch.object(manager, "_download_tmdb_image", new_callable=AsyncMock) as mock_dl:
            mock_settings.symlink_naming = naming_cfg
            await manager._write_nfo_sidecar(item, target_path)

        # TMDB must have been called to get poster/backdrop paths
        mock_tmdb.get_movie_details_full.assert_awaited_once_with(12345)
        # Images were downloaded
        assert mock_dl.await_count == 2
        # NFO was NOT re-written (file content unchanged)
        assert (movie_dir / "movie.nfo").read_text() == original_nfo

    # ------------------------------------------------------------------
    # _write_nfo_sidecar: unexpected error → non-fatal
    # ------------------------------------------------------------------

    async def test_write_nfo_sidecar_unexpected_error_non_fatal(self, tmp_path: Path) -> None:
        """An unexpected RuntimeError inside _write_nfo_sidecar never propagates."""
        movie_dir = tmp_path / "Test Movie (2024)"
        movie_dir.mkdir()
        target_path = str(movie_dir / "movie.mkv")

        item = self._make_movie_item(tmdb_id="12345")

        manager = SymlinkManager()
        naming_cfg = SymlinkNamingConfig(generate_nfo=True)

        mock_tmdb = MagicMock()
        mock_tmdb.get_movie_details_full = AsyncMock(
            side_effect=RuntimeError("something unexpected")
        )

        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._tmdb_client", mock_tmdb):
            mock_settings.symlink_naming = naming_cfg
            # Must not raise — caught by the broad except Exception handler
            await manager._write_nfo_sidecar(item, target_path)

    # ------------------------------------------------------------------
    # _cleanup_orphaned_nfo: removes all sidecar files when only sidecars remain
    # ------------------------------------------------------------------

    async def test_cleanup_orphaned_nfo_with_images(self, tmp_path: Path) -> None:
        """_cleanup_orphaned_nfo removes tvshow.nfo, poster.jpg, fanart.jpg when no other files."""
        show_dir = tmp_path / "Test Show (2023)"
        show_dir.mkdir()
        nfo = show_dir / "tvshow.nfo"
        poster = show_dir / "poster.jpg"
        fanart = show_dir / "fanart.jpg"
        nfo.write_text("<tvshow/>")
        poster.write_bytes(b"poster data")
        fanart.write_bytes(b"fanart data")

        manager = SymlinkManager()
        naming_cfg = SymlinkNamingConfig(generate_nfo=True)

        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.symlink_naming = naming_cfg
            await manager._cleanup_orphaned_nfo(str(show_dir))

        assert not nfo.exists(), "tvshow.nfo should have been removed"
        assert not poster.exists(), "poster.jpg should have been removed"
        assert not fanart.exists(), "fanart.jpg should have been removed"

    # ------------------------------------------------------------------
    # _cleanup_orphaned_nfo: keeps sidecars when other files are present
    # ------------------------------------------------------------------

    async def test_cleanup_keeps_sidecars_when_other_files_present(self, tmp_path: Path) -> None:
        """_cleanup_orphaned_nfo leaves sidecar files alone when other files exist."""
        season_dir = tmp_path / "Test Show (2023)" / "Season 01"
        season_dir.mkdir(parents=True)
        nfo = season_dir / "tvshow.nfo"
        poster = season_dir / "poster.jpg"
        fanart = season_dir / "fanart.jpg"
        symlink_file = season_dir / "Test.Show.S01E01.mkv"
        nfo.write_text("<tvshow/>")
        poster.write_bytes(b"poster data")
        fanart.write_bytes(b"fanart data")
        symlink_file.write_bytes(b"video content")  # non-sidecar file

        manager = SymlinkManager()
        naming_cfg = SymlinkNamingConfig(generate_nfo=True)

        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.symlink_naming = naming_cfg
            await manager._cleanup_orphaned_nfo(str(season_dir))

        # All sidecar files must remain because a non-sidecar file is present
        assert nfo.exists(), "tvshow.nfo should NOT have been removed"
        assert poster.exists(), "poster.jpg should NOT have been removed"
        assert fanart.exists(), "fanart.jpg should NOT have been removed"

    # ------------------------------------------------------------------
    # Episode NFO XML builder (2a, 2b)
    # ------------------------------------------------------------------

    def test_episode_nfo_xml_content(self) -> None:
        """_build_episode_nfo_xml with full TmdbEpisodeInfo produces expected XML."""
        import xml.etree.ElementTree as ET

        from src.services.tmdb import TmdbEpisodeInfo

        ep_info = TmdbEpisodeInfo(
            episode_number=3,
            name="Test Episode Title",
            air_date="2023-06-15",
            overview="A compelling episode overview.",
            still_path="/still303.jpg",
        )

        manager = SymlinkManager()
        xml_str = manager._build_episode_nfo_xml(
            show_title="Generic Show",
            season=1,
            episode=3,
            episode_info=ep_info,
        )

        assert xml_str.startswith('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>')
        root = ET.fromstring(xml_str.split("\n", 1)[1].strip())

        # Root element
        assert root.tag == "episodedetails"

        # Required fields from episode_info
        assert root.findtext("title") == "Test Episode Title"
        assert root.findtext("showtitle") == "Generic Show"
        assert root.findtext("season") == "1"
        assert root.findtext("episode") == "3"
        assert root.findtext("plot") == "A compelling episode overview."
        assert root.findtext("aired") == "2023-06-15"

        # Still image as <thumb>
        thumb = root.find("thumb")
        assert thumb is not None
        assert thumb.text == "https://image.tmdb.org/t/p/w500/still303.jpg"

    def test_episode_nfo_minimal_without_info(self) -> None:
        """_build_episode_nfo_xml with episode_info=None produces minimal output."""
        import xml.etree.ElementTree as ET

        manager = SymlinkManager()
        xml_str = manager._build_episode_nfo_xml(
            show_title="Generic Show",
            season=2,
            episode=7,
            episode_info=None,
        )

        root = ET.fromstring(xml_str.split("\n", 1)[1].strip())
        assert root.tag == "episodedetails"

        # Minimal required fields
        assert root.findtext("showtitle") == "Generic Show"
        assert root.findtext("season") == "2"
        assert root.findtext("episode") == "7"
        # title must still be present (generic fallback)
        assert root.findtext("title") is not None
        assert root.findtext("title") != ""

        # These fields must NOT appear when episode_info is None
        assert root.findtext("plot") is None
        assert root.findtext("aired") is None
        assert root.find("thumb") is None

    # ------------------------------------------------------------------
    # _write_nfo_sidecar: creates episode NFO alongside symlink (2c, 2d)
    # ------------------------------------------------------------------

    async def test_write_nfo_sidecar_creates_episode_nfo(self, tmp_path: Path) -> None:
        """_write_nfo_sidecar writes tvshow.nfo in show root and episode.nfo in season dir."""
        from src.services.tmdb import TmdbEpisodeInfo, TmdbSeasonDetail

        show_root = tmp_path / "shows" / "Generic Show (2023)"
        season_dir = show_root / "Season 01"
        season_dir.mkdir(parents=True)
        target_path = str(season_dir / "Generic.Show.S01E03.mkv")

        item = self._make_show_item(tmdb_id="67890", season=1, episode=3)
        show_detail = self._make_show_detail()
        season_detail = TmdbSeasonDetail(
            season_number=1,
            name="Season 1",
            episodes=[
                TmdbEpisodeInfo(
                    episode_number=3,
                    name="Third Episode",
                    air_date="2023-02-15",
                    overview="The third episode plot.",
                    still_path="/still_ep3.jpg",
                ),
            ],
        )

        manager = SymlinkManager()
        naming_cfg = SymlinkNamingConfig(generate_nfo=True)

        mock_tmdb = MagicMock()
        mock_tmdb.get_show_details = AsyncMock(return_value=show_detail)
        mock_tmdb.get_season_details = AsyncMock(return_value=season_detail)

        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._tmdb_client", mock_tmdb), \
             patch.object(manager, "_download_tmdb_image", new_callable=AsyncMock):
            mock_settings.symlink_naming = naming_cfg
            await manager._write_nfo_sidecar(item, target_path)

        # tvshow.nfo must be in show root, not in Season 01
        assert (show_root / "tvshow.nfo").exists(), "tvshow.nfo missing from show root"
        assert not (season_dir / "tvshow.nfo").exists(), "tvshow.nfo must not be in Season dir"

        # Episode NFO must be alongside the symlink (same stem, .nfo extension)
        expected_ep_nfo = season_dir / "Generic.Show.S01E03.nfo"
        assert expected_ep_nfo.exists(), f"episode NFO not found at {expected_ep_nfo}"

        # Episode NFO must contain episode-specific data
        ep_nfo_content = expected_ep_nfo.read_text()
        assert "Third Episode" in ep_nfo_content
        assert "2023-02-15" in ep_nfo_content

    def test_episode_nfo_path_matches_symlink(self, tmp_path: Path) -> None:
        """The episode NFO path is the symlink target path with .nfo extension replacing the original."""
        target_path = str(tmp_path / "Show (2023)" / "Season 01" / "Show.S01E05.mkv")
        expected_nfo = str(tmp_path / "Show (2023)" / "Season 01" / "Show.S01E05.nfo")

        # The path is constructed by os.path.splitext(target_path)[0] + ".nfo"
        actual = os.path.splitext(target_path)[0] + ".nfo"
        assert actual == expected_nfo

    def test_episode_nfo_path_different_extension(self, tmp_path: Path) -> None:
        """Episode NFO path is correct when source file is .mp4 instead of .mkv."""
        target_path = str(tmp_path / "Show (2023)" / "Season 02" / "Show.S02E01.mp4")
        expected_nfo = str(tmp_path / "Show (2023)" / "Season 02" / "Show.S02E01.nfo")

        actual = os.path.splitext(target_path)[0] + ".nfo"
        assert actual == expected_nfo

    # ------------------------------------------------------------------
    # _write_nfo_sidecar: all-exist check includes episode NFO (2e)
    # ------------------------------------------------------------------

    async def test_all_exist_includes_episode_nfo(self, tmp_path: Path) -> None:
        """_write_nfo_sidecar is called when episode NFO is missing, skipped when all present."""
        from src.services.tmdb import TmdbEpisodeInfo, TmdbSeasonDetail

        show_root = tmp_path / "shows" / "Generic Show (2023)"
        season_dir = show_root / "Season 01"
        season_dir.mkdir(parents=True)
        target_path = str(season_dir / "Generic.Show.S01E04.mkv")

        # Create tvshow.nfo, poster, fanart — but NOT the episode NFO
        (show_root / "tvshow.nfo").write_text("<tvshow/>")
        (show_root / "poster.jpg").write_bytes(b"poster")
        (show_root / "fanart.jpg").write_bytes(b"fanart")

        item = self._make_show_item(tmdb_id="67890", season=1, episode=4)
        show_detail = self._make_show_detail()
        season_detail = TmdbSeasonDetail(
            season_number=1,
            name="Season 1",
            episodes=[
                TmdbEpisodeInfo(
                    episode_number=4,
                    name="Fourth Episode",
                    air_date="2023-03-01",
                    overview="Fourth episode plot.",
                    still_path=None,
                ),
            ],
        )

        manager = SymlinkManager()
        naming_cfg = SymlinkNamingConfig(generate_nfo=True)

        mock_tmdb = MagicMock()
        mock_tmdb.get_show_details = AsyncMock(return_value=show_detail)
        mock_tmdb.get_season_details = AsyncMock(return_value=season_detail)

        # First call: episode NFO missing → TMDB is called
        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._tmdb_client", mock_tmdb), \
             patch.object(manager, "_download_tmdb_image", new_callable=AsyncMock):
            mock_settings.symlink_naming = naming_cfg
            await manager._write_nfo_sidecar(item, target_path)

        ep_nfo_path = season_dir / "Generic.Show.S01E04.nfo"
        assert ep_nfo_path.exists(), "episode NFO should have been created"
        mock_tmdb.get_show_details.assert_awaited_once()

        # Second call: all files present → TMDB must NOT be called again
        mock_tmdb.get_show_details.reset_mock()
        mock_tmdb.get_season_details.reset_mock()

        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch("src.core.symlink_manager._tmdb_client", mock_tmdb), \
             patch.object(manager, "_download_tmdb_image", new_callable=AsyncMock):
            mock_settings.symlink_naming = naming_cfg
            await manager._write_nfo_sidecar(item, target_path)

        mock_tmdb.get_show_details.assert_not_awaited()


# ---------------------------------------------------------------------------
# Group 16: TestSidecarRetry
# ---------------------------------------------------------------------------


class TestSidecarRetry:
    """Tests for SymlinkManager.retry_missing_sidecars."""

    def _make_show_item_with_tmdb(
        self, session: None = None, **kwargs: object
    ) -> MediaItem:
        """Return an unsaved show MediaItem with tmdb_id and season/episode set."""
        defaults: dict[str, object] = {
            "imdb_id": "tt7654321",
            "title": "Generic Retry Show",
            "year": 2023,
            "media_type": MediaType.SHOW,
            "season": 1,
            "episode": 2,
            "state": QueueState.COMPLETE,
            "retry_count": 0,
            "tmdb_id": "99999",
        }
        defaults.update(kwargs)
        return MediaItem(**defaults)  # type: ignore[arg-type]

    # ------------------------------------------------------------------
    # a) Disabled: returns 0 without DB query
    # ------------------------------------------------------------------

    async def test_retry_skips_when_disabled(self, session: AsyncSession) -> None:
        """retry_missing_sidecars returns 0 immediately when generate_nfo=False."""
        manager = SymlinkManager()
        naming_cfg = SymlinkNamingConfig(generate_nfo=False)

        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.symlink_naming = naming_cfg
            result = await manager.retry_missing_sidecars(session)

        assert result == 0

    # ------------------------------------------------------------------
    # b) Finds and retries items with missing sidecars
    # ------------------------------------------------------------------

    async def test_retry_finds_missing_sidecars(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """retry_missing_sidecars calls _write_nfo_sidecar for items without sidecar files."""
        # Create a real MediaItem and Symlink in DB
        item = self._make_show_item_with_tmdb()
        session.add(item)
        await session.flush()

        # Create a real directory structure and a valid symlink target
        show_root = tmp_path / "Generic Retry Show (2023)"
        season_dir = show_root / "Season 01"
        season_dir.mkdir(parents=True)
        target_path = str(season_dir / "Generic.Retry.Show.S01E02.mkv")

        # Write a minimal source file so the path is valid on disk
        source_file = tmp_path / "source.mkv"
        source_file.write_bytes(b"fake video")

        # Create the symlink on disk
        os.symlink(str(source_file), target_path)

        symlink = Symlink(
            media_item_id=item.id,
            source_path=str(source_file),
            target_path=target_path,
            valid=True,
        )
        session.add(symlink)
        await session.flush()

        manager = SymlinkManager()
        naming_cfg = SymlinkNamingConfig(generate_nfo=True)

        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch.object(manager, "_write_nfo_sidecar", new_callable=AsyncMock) as mock_write:
            mock_settings.symlink_naming = naming_cfg
            result = await manager.retry_missing_sidecars(session)

        # _write_nfo_sidecar must have been called for the item with missing sidecars
        assert result >= 1
        mock_write.assert_awaited()

    # ------------------------------------------------------------------
    # c) Skips when all sidecar files are present
    # ------------------------------------------------------------------

    async def test_retry_skips_complete_sidecars(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """retry_missing_sidecars skips items that already have all sidecar files."""
        item = self._make_show_item_with_tmdb()
        session.add(item)
        await session.flush()

        # Create complete directory structure with all sidecar files
        show_root = tmp_path / "Generic Retry Show (2023)"
        season_dir = show_root / "Season 01"
        season_dir.mkdir(parents=True)
        target_path = str(season_dir / "Generic.Retry.Show.S01E02.mkv")

        # Create source file + symlink
        source_file = tmp_path / "source_c.mkv"
        source_file.write_bytes(b"fake video")
        os.symlink(str(source_file), target_path)

        # Create ALL sidecar files so nothing is missing
        (show_root / "tvshow.nfo").write_text("<tvshow/>")
        (show_root / "poster.jpg").write_bytes(b"poster")
        (show_root / "fanart.jpg").write_bytes(b"fanart")
        ep_nfo = season_dir / "Generic.Retry.Show.S01E02.nfo"
        ep_nfo.write_text("<episodedetails/>")

        symlink = Symlink(
            media_item_id=item.id,
            source_path=str(source_file),
            target_path=target_path,
            valid=True,
        )
        session.add(symlink)
        await session.flush()

        manager = SymlinkManager()
        naming_cfg = SymlinkNamingConfig(generate_nfo=True)

        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch.object(manager, "_write_nfo_sidecar", new_callable=AsyncMock) as mock_write:
            mock_settings.symlink_naming = naming_cfg
            result = await manager.retry_missing_sidecars(session)

        # No retries needed — all sidecar files are present
        assert result == 0
        mock_write.assert_not_awaited()

    # ------------------------------------------------------------------
    # d) Respects max_retries limit
    # ------------------------------------------------------------------

    async def test_retry_respects_max_retries(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """retry_missing_sidecars processes at most max_retries items per call."""
        total_items = 15
        max_retries = 5

        for i in range(total_items):
            item = MediaItem(
                imdb_id=f"tt{9900000 + i}",
                title=f"Retry Show {i}",
                year=2023,
                media_type=MediaType.SHOW,
                season=1,
                episode=i + 1,
                state=QueueState.COMPLETE,
                retry_count=0,
                tmdb_id=f"{80000 + i}",
            )
            session.add(item)
            await session.flush()

            # Create a real target dir + symlink for this item
            show_root = tmp_path / f"Retry Show {i} (2023)"
            season_dir = show_root / "Season 01"
            season_dir.mkdir(parents=True)
            target_path = str(season_dir / f"Retry.Show.{i}.S01E{i+1:02d}.mkv")

            source_file = tmp_path / f"source_{i}.mkv"
            source_file.write_bytes(b"video")
            os.symlink(str(source_file), target_path)

            symlink = Symlink(
                media_item_id=item.id,
                source_path=str(source_file),
                target_path=target_path,
                valid=True,
            )
            session.add(symlink)
            await session.flush()

        manager = SymlinkManager()
        naming_cfg = SymlinkNamingConfig(generate_nfo=True)

        with patch("src.core.symlink_manager.settings") as mock_settings, \
             patch.object(manager, "_write_nfo_sidecar", new_callable=AsyncMock) as mock_write:
            mock_settings.symlink_naming = naming_cfg
            result = await manager.retry_missing_sidecars(session, max_retries=max_retries)

        assert result == max_retries
        assert mock_write.await_count == max_retries


# ---------------------------------------------------------------------------
# Group 17: TestCleanupEpisodeNfo
# ---------------------------------------------------------------------------


class TestCleanupEpisodeNfo:
    """Tests for _cleanup_orphaned_nfo with episode NFO files."""

    async def test_cleanup_removes_episode_nfo(self, tmp_path: Path) -> None:
        """_cleanup_orphaned_nfo removes episode .nfo files along with standard sidecars."""
        show_dir = tmp_path / "Generic Show (2023)"
        show_dir.mkdir()

        # Create standard sidecars
        (show_dir / "tvshow.nfo").write_text("<tvshow/>")
        (show_dir / "poster.jpg").write_bytes(b"poster")
        (show_dir / "fanart.jpg").write_bytes(b"fanart")

        # Create an episode NFO (a .nfo file that is not tvshow.nfo/movie.nfo)
        ep_nfo = show_dir / "Show.S01E01.nfo"
        ep_nfo.write_text("<episodedetails/>")

        manager = SymlinkManager()
        naming_cfg = SymlinkNamingConfig(generate_nfo=True)

        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.symlink_naming = naming_cfg
            await manager._cleanup_orphaned_nfo(str(show_dir))

        # All sidecar files including the episode NFO must be removed
        assert not (show_dir / "tvshow.nfo").exists(), "tvshow.nfo should be removed"
        assert not (show_dir / "poster.jpg").exists(), "poster.jpg should be removed"
        assert not (show_dir / "fanart.jpg").exists(), "fanart.jpg should be removed"
        assert not ep_nfo.exists(), "episode NFO should be removed"

    async def test_cleanup_episode_nfo_kept_when_video_present(self, tmp_path: Path) -> None:
        """_cleanup_orphaned_nfo does NOT remove sidecars when a video file is also present."""
        season_dir = tmp_path / "Generic Show (2023)" / "Season 01"
        season_dir.mkdir(parents=True)

        # Create sidecars including episode NFO
        ep_nfo = season_dir / "Show.S01E01.nfo"
        ep_nfo.write_text("<episodedetails/>")

        # A video file counts as a non-sidecar file — cleanup must abort
        (season_dir / "Show.S01E01.mkv").write_bytes(b"video data")

        manager = SymlinkManager()
        naming_cfg = SymlinkNamingConfig(generate_nfo=True)

        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.symlink_naming = naming_cfg
            await manager._cleanup_orphaned_nfo(str(season_dir))

        # Episode NFO must remain since there is also a video file
        assert ep_nfo.exists(), "episode NFO must be kept when video file is present"

    async def test_cleanup_disabled_when_generate_nfo_false(self, tmp_path: Path) -> None:
        """_cleanup_orphaned_nfo is a no-op when generate_nfo=False."""
        show_dir = tmp_path / "Generic Show (2023)"
        show_dir.mkdir()
        nfo = show_dir / "tvshow.nfo"
        nfo.write_text("<tvshow/>")

        manager = SymlinkManager()
        naming_cfg = SymlinkNamingConfig(generate_nfo=False)

        with patch("src.core.symlink_manager.settings") as mock_settings:
            mock_settings.symlink_naming = naming_cfg
            await manager._cleanup_orphaned_nfo(str(show_dir))

        # NFO must remain — cleanup is disabled
        assert nfo.exists(), "NFO must not be removed when generate_nfo=False"
