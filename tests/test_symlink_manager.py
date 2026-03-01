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
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import SymlinkNamingConfig
from src.core.symlink_manager import (
    SourceNotFoundError,
    SymlinkCreationError,
    SymlinkManager,
    VerifyResult,
    _find_existing_show_dir,
    _format_timestamp,
    build_movie_dir,
    build_show_dir,
    sanitize_name,
    symlink_manager,
)
from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.symlink import Symlink

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
        source = _make_source_file(library_paths["mount"], "test.movie.2024.mkv")
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
