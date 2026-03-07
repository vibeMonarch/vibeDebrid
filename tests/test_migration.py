"""Tests for src/core/migration.py and src/api/routes/tools.py.

Covers:
  - parse_media_name: parenthesised year, bracket year, scene names, titles
    starting with numbers, titles with no year, empty input, unicode, edge cases
  - extract_imdb_id: CLI Debrid format, file-only string, multi-digit ID
  - parse_episode_info: standard SxxExx, single-digit, no match
  - parse_season_number: zero-padded, single digit, non-season dir name
  - extract_resolution: 2160p, 1080p, no match
  - scan_library (_scan_movie_dir / _scan_shows_dir): real dirs with tmp_path,
    symlinks, real files, empty dirs, non-video files, mixed content
  - preview_migration: no existing items, title+year duplicates,
    source_path duplicates, to_move detection, summary counts
  - execute_migration: MediaItem + Symlink creation, no Symlink for real files,
    duplicate removal (filesystem + DB), config update, error isolation
  - API routes: GET /tools HTML, POST /api/tools/migration/preview (happy path
    and 400), POST /api/tools/migration/execute (happy path and 400)
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.deps import get_db
from src.core.migration import (
    FoundItem,
    MigrationPreview,
    DuplicateMatch,
    MigrationResult,
    _normalise_title,
    _scan_movie_dir,
    _scan_shows_dir,
    execute_migration,
    extract_imdb_id,
    extract_resolution,
    parse_episode_info,
    parse_media_name,
    parse_season_number,
    preview_migration,
    scan_library,
)
from src.main import app
from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.symlink import Symlink


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def _make_media_item(
    session: AsyncSession,
    *,
    title: str = "Test Movie",
    year: int | None = 2024,
    media_type: MediaType = MediaType.MOVIE,
    state: QueueState = QueueState.COMPLETE,
    season: int | None = None,
    episode: int | None = None,
    imdb_id: str | None = None,
    source: str | None = "manual",
) -> MediaItem:
    item = MediaItem(
        title=title,
        year=year,
        media_type=media_type,
        state=state,
        season=season,
        episode=episode,
        imdb_id=imdb_id,
        source=source,
        added_at=_utcnow(),
        state_changed_at=_utcnow(),
    )
    session.add(item)
    await session.flush()
    return item


async def _make_symlink(
    session: AsyncSession,
    *,
    media_item_id: int,
    source_path: str = "/zurg/source.mkv",
    target_path: str = "/library/movies/Movie/movie.mkv",
    valid: bool = True,
) -> Symlink:
    sl = Symlink(
        media_item_id=media_item_id,
        source_path=source_path,
        target_path=target_path,
        valid=valid,
    )
    session.add(sl)
    await session.flush()
    return sl


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def override_db(session: AsyncSession):
    """Override FastAPI get_db dependency with the test session."""

    async def _override() -> AsyncSession:
        yield session

    app.dependency_overrides[get_db] = _override
    yield
    app.dependency_overrides.clear()


@pytest.fixture
async def http(override_db) -> AsyncClient:
    """Async HTTP client backed by the FastAPI test app with DB override."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


# ---------------------------------------------------------------------------
# Group 1: parse_media_name
# ---------------------------------------------------------------------------


class TestParseMediaName:
    """Unit tests for parse_media_name()."""

    def test_paren_year_simple(self) -> None:
        """Basic 'Title (Year)' pattern returns correct title and year."""
        title, year = parse_media_name("Casino (1995)")
        assert title == "Casino"
        assert year == 1995

    def test_paren_year_with_extras(self) -> None:
        """'Title (Year) [extras]' strips trailing brackets and returns year."""
        title, year = parse_media_name("1917 (2019) [1080p] [WEBRip]")
        assert title == "1917"
        assert year == 2019

    def test_scene_dots_year_resolution(self) -> None:
        """Scene name with dots: title stops before year + resolution token."""
        title, year = parse_media_name("2001.A.Space.Odyssey.1968.1080p.BluRay.x264")
        assert title == "2001 A Space Odyssey"
        assert year == 1968

    def test_scene_spaces_year_resolution(self) -> None:
        """Scene name with spaces: year is extracted and title stripped."""
        title, year = parse_media_name("Anora 2024 1080p WEB-DL HEVC")
        assert title == "Anora"
        assert year == 2024

    def test_no_year_returns_none(self) -> None:
        """Title with no year returns entire name as title, year=None."""
        title, year = parse_media_name("Back To The Future Trilogy")
        assert title == "Back To The Future Trilogy"
        assert year is None

    def test_title_with_article_and_year(self) -> None:
        """Multi-word title with year in parentheses."""
        title, year = parse_media_name("A Dangerous Method (2011)")
        assert title == "A Dangerous Method"
        assert year == 2011

    def test_title_starts_with_number(self) -> None:
        """Title that starts with a number: '30 Rock (2006)'."""
        title, year = parse_media_name("30 Rock (2006)")
        assert title == "30 Rock"
        assert year == 2006

    def test_alphanumeric_title(self) -> None:
        """Title mixing letters and digits: 'M3GAN (2022)'."""
        title, year = parse_media_name("M3GAN (2022)")
        assert title == "M3GAN"
        assert year == 2022

    def test_underscore_sanitized_title(self) -> None:
        """Underscores in title, no year — title returned, year is None."""
        title, year = parse_media_name("Demon Slayer_ Kimetsu no Yaiba")
        assert title  # must be non-empty
        assert year is None

    def test_empty_string(self) -> None:
        """Empty input returns empty title and None year."""
        title, year = parse_media_name("")
        assert title == ""
        assert year is None

    def test_unicode_title(self) -> None:
        """Unicode characters in title are preserved."""
        title, year = parse_media_name("Amélie (2001)")
        assert "m" in title.lower()  # character content preserved
        assert year == 2001

    def test_bad_boys_title(self) -> None:
        """Two-word title with year in parentheses."""
        title, year = parse_media_name("Bad Boys (1995)")
        assert title == "Bad Boys"
        assert year == 1995

    def test_scene_name_with_dots_release_group(self) -> None:
        """Full scene name 'The.Lost.Bus.2025.1080p.WEB.h264-ETHEL' parsed."""
        title, year = parse_media_name("The.Lost.Bus.2025.1080p.WEB.h264-ETHEL")
        assert title == "The Lost Bus"
        assert year == 2025

    def test_bracket_year_pattern(self) -> None:
        """'Title [Year]' bracket pattern is recognised."""
        title, year = parse_media_name("Blade Runner [1982]")
        assert title == "Blade Runner"
        assert year == 1982

    def test_vibeDebrid_timestamp_prefix_stripped(self) -> None:
        """12-digit timestamp prefix is stripped before parsing."""
        title, year = parse_media_name("202603011430 Casino (1995)")
        assert title == "Casino"
        assert year == 1995

    def test_year_only_string_no_title(self) -> None:
        """String that is only a year in parens — title comes from whole name."""
        # "Movie (2020)" — 'Movie' is the title
        title, year = parse_media_name("Movie (2020)")
        assert title == "Movie"
        assert year == 2020

    def test_scene_name_no_stop_token(self) -> None:
        """Scene name without a stop token returns the whole name as title."""
        title, year = parse_media_name("Some.Movie.Without.Year.Or.Resolution")
        # No year found — whole normalised name becomes title
        assert title
        assert year is None

    def test_title_with_colon_sanitized_by_os(self) -> None:
        """Title with underscore replacing colon (OS-sanitized) still parses."""
        title, year = parse_media_name("Mission_ Impossible (1996)")
        assert year == 1996
        assert title  # Some non-empty title extracted


# ---------------------------------------------------------------------------
# Group 2: extract_imdb_id
# ---------------------------------------------------------------------------


class TestExtractImdbId:
    """Unit tests for extract_imdb_id()."""

    def test_cli_debrid_format(self) -> None:
        """Standard CLI Debrid filename format with 7-digit ID."""
        result = extract_imdb_id("Casino (1995) - tt0112641 - 1080p - (release).mkv")
        assert result == "tt0112641"

    def test_no_imdb_returns_none(self) -> None:
        """Filename without tt-pattern returns None."""
        result = extract_imdb_id("Movie.2024.1080p.mkv")
        assert result is None

    def test_eight_digit_id(self) -> None:
        """8-digit IMDB IDs (newer entries) are captured correctly."""
        result = extract_imdb_id("tt12345678")
        assert result == "tt12345678"

    def test_seven_digit_id_in_path(self) -> None:
        """IMDB ID embedded in a directory path is found."""
        result = extract_imdb_id("/library/movies/Interstellar (2014) - tt0816692/")
        assert result == "tt0816692"

    def test_case_insensitive(self) -> None:
        """Uppercase TT prefix is handled (case-insensitive)."""
        result = extract_imdb_id("TT0137523 fight club")
        assert result == "tt0137523"

    def test_six_digit_id_not_matched(self) -> None:
        """IDs shorter than 7 digits are NOT matched (not valid IMDB IDs)."""
        result = extract_imdb_id("tt123456 movie")
        assert result is None


# ---------------------------------------------------------------------------
# Group 3: parse_episode_info
# ---------------------------------------------------------------------------


class TestParseEpisodeInfo:
    """Unit tests for parse_episode_info()."""

    def test_standard_s01e05(self) -> None:
        """Standard SxxExx notation is parsed correctly."""
        season, episode = parse_episode_info("Show.S01E05.1080p.mkv")
        assert season == 1
        assert episode == 5

    def test_underscore_separator(self) -> None:
        """Underscore-separated SxxExx notation is parsed."""
        season, episode = parse_episode_info("Show_S02E10_Title.mkv")
        assert season == 2
        assert episode == 10

    def test_no_episode_returns_none_none(self) -> None:
        """Plain movie filename returns (None, None)."""
        season, episode = parse_episode_info("Movie.2024.mkv")
        assert season is None
        assert episode is None

    def test_single_digit_season_and_episode(self) -> None:
        """Single-digit season and episode numbers are parsed."""
        season, episode = parse_episode_info("show.s1e1.mkv")
        assert season == 1
        assert episode == 1

    def test_three_digit_episode(self) -> None:
        """Three-digit episode numbers (anime) are parsed correctly."""
        season, episode = parse_episode_info("Anime.S01E100.mkv")
        assert season == 1
        assert episode == 100

    def test_lowercase_sxxexx(self) -> None:
        """Lowercase sXXeXX notation is also matched."""
        season, episode = parse_episode_info("show.s03e07.mkv")
        assert season == 3
        assert episode == 7


# ---------------------------------------------------------------------------
# Group 4: parse_season_number
# ---------------------------------------------------------------------------


class TestParseSeasonNumber:
    """Unit tests for parse_season_number()."""

    def test_zero_padded_season(self) -> None:
        """'Season 01' returns integer 1."""
        assert parse_season_number("Season 01") == 1

    def test_single_digit_season(self) -> None:
        """'Season 2' returns integer 2."""
        assert parse_season_number("Season 2") == 2

    def test_two_digit_season(self) -> None:
        """'Season 10' returns integer 10."""
        assert parse_season_number("Season 10") == 10

    def test_specials_returns_none(self) -> None:
        """'Specials' directory name returns None."""
        assert parse_season_number("Specials") is None

    def test_extras_returns_none(self) -> None:
        """'Extras' directory returns None."""
        assert parse_season_number("Extras") is None

    def test_case_insensitive(self) -> None:
        """'season 3' (lowercase) is parsed."""
        assert parse_season_number("season 3") == 3

    def test_no_space_before_number(self) -> None:
        """'Season05' without space is parsed correctly."""
        assert parse_season_number("Season05") == 5


# ---------------------------------------------------------------------------
# Group 5: extract_resolution
# ---------------------------------------------------------------------------


class TestExtractResolution:
    """Unit tests for extract_resolution()."""

    def test_2160p(self) -> None:
        """2160p resolution is detected and returned in lowercase."""
        assert extract_resolution("Movie.2160p.BluRay") == "2160p"

    def test_1080p(self) -> None:
        """1080p resolution is detected."""
        assert extract_resolution("Show.S01E01.1080p") == "1080p"

    def test_720p(self) -> None:
        """720p resolution is detected."""
        assert extract_resolution("file.720p.x264.mkv") == "720p"

    def test_no_resolution_returns_none(self) -> None:
        """Name with no resolution token returns None."""
        assert extract_resolution("no_resolution_here") is None

    def test_resolution_case_insensitive(self) -> None:
        """Mixed-case resolution token (1080P) is normalised to lowercase."""
        assert extract_resolution("Movie.1080P.WEB") == "1080p"


# ---------------------------------------------------------------------------
# Group 6: _normalise_title (internal helper)
# ---------------------------------------------------------------------------


class TestNormaliseTitle:
    """Tests for the internal title normalisation function."""

    def test_strips_punctuation(self) -> None:
        """Punctuation is removed during normalisation."""
        assert _normalise_title("It's a Wonderful Life!") == "its a wonderful life"

    def test_lowercases(self) -> None:
        """Title is lowercased."""
        assert _normalise_title("THE MATRIX") == "the matrix"

    def test_unicode_normalisation(self) -> None:
        """Accented characters are normalised to base ASCII."""
        assert _normalise_title("Amélie") == "amelie"

    def test_collapses_whitespace(self) -> None:
        """Multiple whitespace characters are collapsed to single space."""
        assert _normalise_title("Star  Wars") == "star wars"


# ---------------------------------------------------------------------------
# Group 7: Filesystem scan tests (using tmp_path)
# ---------------------------------------------------------------------------


class TestScanMovieDir:
    """Tests for _scan_movie_dir() using real temporary directories."""

    def test_real_file_movie(self, tmp_path: Path) -> None:
        """A plain video file in a movie subdirectory is discovered."""
        movie_dir = tmp_path / "Casino (1995)"
        movie_dir.mkdir()
        (movie_dir / "Casino.1995.1080p.mkv").write_text("fake video")

        items, errors = _scan_movie_dir(str(tmp_path))

        assert len(errors) == 0
        assert len(items) == 1
        item = items[0]
        assert item.title == "Casino"
        assert item.year == 1995
        assert item.media_type == "movie"
        assert item.is_symlink is False
        assert item.source_path is None
        assert item.resolution == "1080p"

    def test_symlink_movie(self, tmp_path: Path) -> None:
        """A symlink video file — is_symlink=True, source_path populated."""
        source = tmp_path / "source.mkv"
        source.write_text("fake video")

        movie_dir = tmp_path / "library" / "Inception (2010)"
        movie_dir.mkdir(parents=True)
        symlink_path = movie_dir / "Inception.2010.1080p.mkv"
        os.symlink(str(source), str(symlink_path))

        items, errors = _scan_movie_dir(str(tmp_path / "library"))

        assert len(errors) == 0
        assert len(items) == 1
        item = items[0]
        assert item.is_symlink is True
        assert item.source_path == str(source)
        assert item.title == "Inception"
        assert item.year == 2010

    def test_empty_movies_dir(self, tmp_path: Path) -> None:
        """Empty movies directory returns 0 items and no errors."""
        items, errors = _scan_movie_dir(str(tmp_path))
        assert items == []
        assert errors == []

    def test_non_video_files_ignored(self, tmp_path: Path) -> None:
        """Non-video files (.nfo, .srt, .jpg, .txt) are not reported."""
        movie_dir = tmp_path / "Movie (2020)"
        movie_dir.mkdir()
        for ext in (".nfo", ".srt", ".jpg", ".txt", ".sub"):
            (movie_dir / f"Movie{ext}").write_text("metadata")

        items, errors = _scan_movie_dir(str(tmp_path))
        assert items == []
        assert errors == []

    def test_mixed_video_and_metadata_files(self, tmp_path: Path) -> None:
        """Video file is found even when non-video files are also present."""
        movie_dir = tmp_path / "Parasite (2019)"
        movie_dir.mkdir()
        (movie_dir / "Parasite.2019.1080p.mkv").write_text("video")
        (movie_dir / "Parasite.2019.nfo").write_text("nfo")
        (movie_dir / "poster.jpg").write_text("image")

        items, errors = _scan_movie_dir(str(tmp_path))
        assert len(items) == 1
        assert items[0].title == "Parasite"

    def test_nonexistent_path_returns_error(self, tmp_path: Path) -> None:
        """Scanning a nonexistent path returns an error, not an exception."""
        items, errors = _scan_movie_dir(str(tmp_path / "does_not_exist"))
        assert items == []
        assert len(errors) == 1
        assert "Cannot scan movies path" in errors[0]

    def test_imdb_id_extracted_from_filename(self, tmp_path: Path) -> None:
        """IMDB ID in the video filename is extracted."""
        movie_dir = tmp_path / "Casino (1995)"
        movie_dir.mkdir()
        (movie_dir / "Casino (1995) - tt0112641 - 1080p - (WEBRip).mkv").write_text("v")

        items, errors = _scan_movie_dir(str(tmp_path))
        assert len(items) == 1
        assert items[0].imdb_id == "tt0112641"

    def test_multiple_movies(self, tmp_path: Path) -> None:
        """Multiple movie subdirectories each yield one item."""
        for name in ["Alien (1979)", "Blade Runner (1982)", "Dune (2021)"]:
            d = tmp_path / name
            d.mkdir()
            (d / f"{name}.mkv").write_text("video")

        items, errors = _scan_movie_dir(str(tmp_path))
        assert len(items) == 3
        titles = {item.title for item in items}
        assert "Alien" in titles
        assert "Blade Runner" in titles
        assert "Dune" in titles


class TestScanShowsDir:
    """Tests for _scan_shows_dir() using real temporary directories."""

    def test_show_with_season_and_episodes(self, tmp_path: Path) -> None:
        """Standard show structure yields episode items with correct fields."""
        show_dir = tmp_path / "Breaking Bad (2008)"
        season_dir = show_dir / "Season 01"
        season_dir.mkdir(parents=True)
        (season_dir / "Breaking.Bad.S01E01.1080p.mkv").write_text("video")
        (season_dir / "Breaking.Bad.S01E02.1080p.mkv").write_text("video")

        items, errors = _scan_shows_dir(str(tmp_path))

        assert len(errors) == 0
        assert len(items) == 2
        for item in items:
            assert item.title == "Breaking Bad"
            assert item.year == 2008
            assert item.media_type == "show"
            assert item.season == 1
            assert item.resolution == "1080p"

    def test_episode_with_symlink(self, tmp_path: Path) -> None:
        """Symlink episode — is_symlink=True and source_path set."""
        source = tmp_path / "source_ep.mkv"
        source.write_text("video")

        show_dir = tmp_path / "library" / "Severance (2022)"
        season_dir = show_dir / "Season 01"
        season_dir.mkdir(parents=True)
        ep_link = season_dir / "Severance.S01E01.mkv"
        os.symlink(str(source), str(ep_link))

        items, errors = _scan_shows_dir(str(tmp_path / "library"))

        assert len(errors) == 0
        assert len(items) == 1
        assert items[0].is_symlink is True
        assert items[0].source_path == str(source)

    def test_specials_directory_skipped(self, tmp_path: Path) -> None:
        """'Specials' subdirectory does not contribute items (no season number)."""
        show_dir = tmp_path / "Game of Thrones (2011)"
        season_dir = show_dir / "Season 01"
        specials_dir = show_dir / "Specials"
        season_dir.mkdir(parents=True)
        specials_dir.mkdir()
        (season_dir / "GoT.S01E01.mkv").write_text("video")
        (specials_dir / "GoT.S00E01.mkv").write_text("video")

        items, errors = _scan_shows_dir(str(tmp_path))

        assert len(errors) == 0
        # Specials dir is skipped because parse_season_number("Specials") == None
        assert len(items) == 1
        assert items[0].season == 1

    def test_empty_shows_dir(self, tmp_path: Path) -> None:
        """Empty shows directory returns 0 items."""
        items, errors = _scan_shows_dir(str(tmp_path))
        assert items == []
        assert errors == []

    def test_non_video_files_in_season_skipped(self, tmp_path: Path) -> None:
        """Subtitle and metadata files inside season dirs are not reported."""
        show_dir = tmp_path / "The Wire (2002)"
        season_dir = show_dir / "Season 01"
        season_dir.mkdir(parents=True)
        for ext in (".srt", ".nfo", ".jpg"):
            (season_dir / f"episode{ext}").write_text("meta")

        items, errors = _scan_shows_dir(str(tmp_path))
        assert items == []
        assert errors == []

    def test_multiple_seasons(self, tmp_path: Path) -> None:
        """Multiple season directories each contribute episode items."""
        show_dir = tmp_path / "The Sopranos (1999)"
        for s in (1, 2, 3):
            season_dir = show_dir / f"Season {s:02d}"
            season_dir.mkdir(parents=True)
            (season_dir / f"Sopranos.S{s:02d}E01.mkv").write_text("video")

        items, errors = _scan_shows_dir(str(tmp_path))
        seasons = {item.season for item in items}
        assert seasons == {1, 2, 3}


class TestScanLibrary:
    """Tests for the async scan_library() public function."""

    async def test_combines_movies_and_shows(self, tmp_path: Path) -> None:
        """scan_library combines results from both movies and shows paths."""
        movies_dir = tmp_path / "movies"
        shows_dir = tmp_path / "shows"
        movies_dir.mkdir()
        shows_dir.mkdir()

        movie_sub = movies_dir / "Alien (1979)"
        movie_sub.mkdir()
        (movie_sub / "Alien.1979.mkv").write_text("video")

        show_sub = shows_dir / "Firefly (2002)"
        season_dir = show_sub / "Season 01"
        season_dir.mkdir(parents=True)
        (season_dir / "Firefly.S01E01.mkv").write_text("video")

        items, errors = await scan_library(str(movies_dir), str(shows_dir))

        assert len(errors) == 0
        types = {item.media_type for item in items}
        assert "movie" in types
        assert "show" in types

    async def test_invalid_paths_return_errors(self, tmp_path: Path) -> None:
        """Nonexistent paths produce error messages, not exceptions."""
        items, errors = await scan_library(
            str(tmp_path / "no_movies"), str(tmp_path / "no_shows")
        )
        assert items == []
        assert len(errors) == 2


# ---------------------------------------------------------------------------
# Group 8: preview_migration
# ---------------------------------------------------------------------------


class TestPreviewMigration:
    """Tests for preview_migration() with DB session."""

    async def test_no_existing_items_all_new(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """When DB is empty, all found items appear as new (no duplicates)."""
        movies_dir = tmp_path / "movies"
        shows_dir = tmp_path / "shows"
        movies_dir.mkdir()
        shows_dir.mkdir()

        (movies_dir / "Interstellar (2014)").mkdir()
        (movies_dir / "Interstellar (2014)" / "Interstellar.2014.1080p.mkv").write_text("v")

        preview = await preview_migration(session, str(movies_dir), str(shows_dir))

        assert len(preview.found_items) == 1
        assert len(preview.duplicates) == 0
        assert len(preview.to_move) == 0
        assert preview.summary["to_import_count"] == 1
        assert preview.summary["duplicate_count"] == 0

    async def test_title_year_duplicate_detected(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """An existing DB item with matching title+year is flagged as duplicate."""
        await _make_media_item(session, title="Casino", year=1995)

        movies_dir = tmp_path / "movies"
        shows_dir = tmp_path / "shows"
        movies_dir.mkdir()
        shows_dir.mkdir()

        (movies_dir / "Casino (1995)").mkdir()
        (movies_dir / "Casino (1995)" / "Casino.1995.mkv").write_text("v")

        preview = await preview_migration(session, str(movies_dir), str(shows_dir))

        assert len(preview.duplicates) == 1
        dup = preview.duplicates[0]
        assert dup.match_reason == "title_year_match"
        assert dup.existing_title == "Casino"
        assert len(preview.found_items) == 1

    async def test_source_path_duplicate_detected(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """An existing DB item whose symlink source_path matches is flagged as duplicate."""
        # Create the "source" file that the symlink will point to.
        source_file = tmp_path / "zurg_source.mkv"
        source_file.write_text("zurg")

        existing_item = await _make_media_item(session, title="Inception", year=2010)
        await _make_symlink(
            session,
            media_item_id=existing_item.id,
            source_path=str(source_file),
            target_path="/old/library/Inception/Inception.mkv",
        )

        movies_dir = tmp_path / "movies"
        shows_dir = tmp_path / "shows"
        movies_dir.mkdir()
        shows_dir.mkdir()

        movie_subdir = movies_dir / "Inception (2010)"
        movie_subdir.mkdir()
        link_path = movie_subdir / "Inception.2010.mkv"
        os.symlink(str(source_file), str(link_path))

        preview = await preview_migration(session, str(movies_dir), str(shows_dir))

        assert len(preview.duplicates) == 1
        dup = preview.duplicates[0]
        assert dup.match_reason == "same_source_path"

    async def test_to_move_populated_for_items_outside_target(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """vibeDebrid items whose symlinks are NOT under target path appear in to_move."""
        existing_item = await _make_media_item(session, title="Dune", year=2021)
        # Symlink target is outside the target movies_dir.
        await _make_symlink(
            session,
            media_item_id=existing_item.id,
            source_path="/zurg/dune.mkv",
            target_path="/completely/different/path/Dune/Dune.mkv",
        )

        movies_dir = tmp_path / "movies"
        shows_dir = tmp_path / "shows"
        movies_dir.mkdir()
        shows_dir.mkdir()

        # No found items — empty library.
        preview = await preview_migration(session, str(movies_dir), str(shows_dir))

        assert len(preview.to_move) == 1
        assert preview.to_move[0]["title"] == "Dune"

    async def test_item_under_target_not_in_to_move(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """vibeDebrid items already under target paths do NOT appear in to_move."""
        movies_dir = tmp_path / "movies"
        shows_dir = tmp_path / "shows"
        movies_dir.mkdir()
        shows_dir.mkdir()

        existing_item = await _make_media_item(session, title="Alien", year=1979)
        target_inside = str(movies_dir / "Alien (1979)" / "Alien.mkv")
        await _make_symlink(
            session,
            media_item_id=existing_item.id,
            source_path="/zurg/alien.mkv",
            target_path=target_inside,
        )

        preview = await preview_migration(session, str(movies_dir), str(shows_dir))
        assert len(preview.to_move) == 0

    async def test_summary_counts_are_correct(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """Summary dict contains correct counts matching found / duplicate lists."""
        movies_dir = tmp_path / "movies"
        shows_dir = tmp_path / "shows"
        movies_dir.mkdir()
        shows_dir.mkdir()

        # Two movies in library.
        for name in ["Alien (1979)", "Blade Runner (1982)"]:
            d = movies_dir / name
            d.mkdir()
            (d / f"{name}.mkv").write_text("v")

        # One already in DB — becomes duplicate.
        await _make_media_item(session, title="Alien", year=1979)

        preview = await preview_migration(session, str(movies_dir), str(shows_dir))

        assert preview.summary["found_movies"] == 2
        assert preview.summary["duplicate_count"] == 1
        assert preview.summary["to_import_count"] == 1

    async def test_empty_library_empty_preview(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """Scanning empty directories yields an empty preview with zero counts."""
        movies_dir = tmp_path / "movies"
        shows_dir = tmp_path / "shows"
        movies_dir.mkdir()
        shows_dir.mkdir()

        preview = await preview_migration(session, str(movies_dir), str(shows_dir))

        assert preview.found_items == []
        assert preview.duplicates == []
        assert preview.to_move == []
        assert preview.summary["found_movies"] == 0

    async def test_errors_populated_on_unreadable_dir(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """A nonexistent path produces errors in the preview."""
        preview = await preview_migration(
            session,
            str(tmp_path / "missing_movies"),
            str(tmp_path / "missing_shows"),
        )
        assert len(preview.errors) > 0


# ---------------------------------------------------------------------------
# Group 9: execute_migration
# ---------------------------------------------------------------------------


class TestExecuteMigration:
    """Tests for execute_migration() — DB mutations and filesystem operations."""

    async def _empty_preview(
        self, movies_path: str, shows_path: str
    ) -> MigrationPreview:
        return MigrationPreview(
            found_items=[],
            duplicates=[],
            to_move=[],
            errors=[],
            summary={
                "found_movies": 0,
                "found_shows": 0,
                "found_episodes": 0,
                "duplicate_count": 0,
                "to_import_count": 0,
                "to_move_count": 0,
                "error_count": 0,
            },
        )

    async def test_import_creates_media_item_complete(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """Imported FoundItem creates a MediaItem with state=COMPLETE, source='migration'."""
        movies_dir = tmp_path / "movies"
        shows_dir = tmp_path / "shows"
        movies_dir.mkdir()
        shows_dir.mkdir()

        fi = FoundItem(
            title="Parasite",
            year=2019,
            media_type="movie",
            season=None,
            episode=None,
            imdb_id="tt6751668",
            source_path=None,
            target_path=str(movies_dir / "Parasite (2019)" / "Parasite.2019.mkv"),
            is_symlink=False,
            resolution="1080p",
        )
        preview = MigrationPreview(
            found_items=[fi],
            duplicates=[],
            to_move=[],
            errors=[],
            summary={},
        )

        with patch("src.core.migration.CONFIG_FILE") as mock_cfg:
            mock_cfg.exists.return_value = False
            mock_cfg.write_text = MagicMock()
            with patch("src.core.migration.Settings.load") as mock_load:
                mock_load.return_value = MagicMock(model_fields={})
                result = await execute_migration(
                    session, preview, str(movies_dir), str(shows_dir)
                )

        assert result.imported == 1

        from sqlalchemy import select as sa_select

        rows = (await session.execute(sa_select(MediaItem))).scalars().all()
        assert len(rows) == 1
        item = rows[0]
        assert item.title == "Parasite"
        assert item.year == 2019
        assert item.state == QueueState.COMPLETE
        assert item.source == "migration"

    async def test_import_symlink_creates_symlink_record(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """A FoundItem that is_symlink=True creates both MediaItem and Symlink records."""
        movies_dir = tmp_path / "movies"
        shows_dir = tmp_path / "shows"
        movies_dir.mkdir()
        shows_dir.mkdir()

        fi = FoundItem(
            title="Interstellar",
            year=2014,
            media_type="movie",
            season=None,
            episode=None,
            imdb_id=None,
            source_path="/zurg/interstellar.mkv",
            target_path=str(movies_dir / "Interstellar (2014)" / "Interstellar.mkv"),
            is_symlink=True,
            resolution="1080p",
        )
        preview = MigrationPreview(
            found_items=[fi], duplicates=[], to_move=[], errors=[], summary={}
        )

        with patch("src.core.migration.CONFIG_FILE") as mock_cfg:
            mock_cfg.exists.return_value = False
            mock_cfg.write_text = MagicMock()
            with patch("src.core.migration.Settings.load") as mock_load:
                mock_load.return_value = MagicMock(model_fields={})
                await execute_migration(session, preview, str(movies_dir), str(shows_dir))

        from sqlalchemy import select as sa_select

        symlinks = (await session.execute(sa_select(Symlink))).scalars().all()
        assert len(symlinks) == 1
        assert symlinks[0].source_path == "/zurg/interstellar.mkv"

    async def test_import_real_file_no_symlink_record(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """A real file (is_symlink=False) creates MediaItem only — no Symlink record."""
        movies_dir = tmp_path / "movies"
        shows_dir = tmp_path / "shows"
        movies_dir.mkdir()
        shows_dir.mkdir()

        fi = FoundItem(
            title="The Room",
            year=2003,
            media_type="movie",
            season=None,
            episode=None,
            imdb_id=None,
            source_path=None,
            target_path=str(movies_dir / "The Room (2003)" / "The.Room.2003.mkv"),
            is_symlink=False,
            resolution=None,
        )
        preview = MigrationPreview(
            found_items=[fi], duplicates=[], to_move=[], errors=[], summary={}
        )

        with patch("src.core.migration.CONFIG_FILE") as mock_cfg:
            mock_cfg.exists.return_value = False
            mock_cfg.write_text = MagicMock()
            with patch("src.core.migration.Settings.load") as mock_load:
                mock_load.return_value = MagicMock(model_fields={})
                await execute_migration(session, preview, str(movies_dir), str(shows_dir))

        from sqlalchemy import select as sa_select

        symlinks = (await session.execute(sa_select(Symlink))).scalars().all()
        assert symlinks == []

    async def test_duplicate_removal_deletes_media_item(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """Duplicate removal deletes the vibeDebrid MediaItem record from DB."""
        movies_dir = tmp_path / "movies"
        shows_dir = tmp_path / "shows"
        movies_dir.mkdir()
        shows_dir.mkdir()

        existing_item = await _make_media_item(session, title="Oldboy", year=2003)

        found_item = FoundItem(
            title="Oldboy",
            year=2003,
            media_type="movie",
            season=None,
            episode=None,
            imdb_id=None,
            source_path=None,
            target_path=str(movies_dir / "Oldboy (2003)" / "Oldboy.mkv"),
            is_symlink=False,
            resolution=None,
        )
        dup = DuplicateMatch(
            found_item=found_item,
            existing_id=existing_item.id,
            existing_title="Oldboy",
            match_reason="title_year_match",
        )
        preview = MigrationPreview(
            found_items=[found_item],
            duplicates=[dup],
            to_move=[],
            errors=[],
            summary={},
        )

        with patch("src.core.migration.CONFIG_FILE") as mock_cfg:
            mock_cfg.exists.return_value = False
            mock_cfg.write_text = MagicMock()
            with patch("src.core.migration.Settings.load") as mock_load:
                mock_load.return_value = MagicMock(model_fields={})
                result = await execute_migration(
                    session, preview, str(movies_dir), str(shows_dir)
                )

        assert result.duplicates_removed == 1

        from sqlalchemy import select as sa_select

        rows = (await session.execute(sa_select(MediaItem))).scalars().all()
        # The original item should be gone.
        assert not any(row.id == existing_item.id for row in rows)

    async def test_duplicate_removal_unlinks_symlink_file(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """Duplicate removal removes the symlink file from the filesystem."""
        movies_dir = tmp_path / "movies"
        shows_dir = tmp_path / "shows"
        movies_dir.mkdir()
        shows_dir.mkdir()

        # Create a real symlink on disk that the duplicate logic should delete.
        source = tmp_path / "real_source.mkv"
        source.write_text("video")

        old_link_dir = tmp_path / "old_library" / "Alien (1979)"
        old_link_dir.mkdir(parents=True)
        old_link = old_link_dir / "Alien.mkv"
        os.symlink(str(source), str(old_link))

        assert old_link.is_symlink()

        existing_item = await _make_media_item(session, title="Alien", year=1979)
        await _make_symlink(
            session,
            media_item_id=existing_item.id,
            source_path=str(source),
            target_path=str(old_link),
        )

        found_item = FoundItem(
            title="Alien",
            year=1979,
            media_type="movie",
            season=None,
            episode=None,
            imdb_id=None,
            source_path=str(source),
            target_path=str(movies_dir / "Alien (1979)" / "Alien.mkv"),
            is_symlink=True,
            resolution=None,
        )
        dup = DuplicateMatch(
            found_item=found_item,
            existing_id=existing_item.id,
            existing_title="Alien",
            match_reason="same_source_path",
        )
        preview = MigrationPreview(
            found_items=[found_item],
            duplicates=[dup],
            to_move=[],
            errors=[],
            summary={},
        )

        with patch("src.core.migration.CONFIG_FILE") as mock_cfg:
            mock_cfg.exists.return_value = False
            mock_cfg.write_text = MagicMock()
            with patch("src.core.migration.Settings.load") as mock_load:
                mock_load.return_value = MagicMock(model_fields={})
                await execute_migration(session, preview, str(movies_dir), str(shows_dir))

        # The old symlink must have been removed.
        assert not old_link.exists()

    async def test_config_updated_after_execute(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """After execute_migration, config_updated=True and the lock is released."""
        movies_dir = tmp_path / "movies"
        shows_dir = tmp_path / "shows"
        movies_dir.mkdir()
        shows_dir.mkdir()

        preview = MigrationPreview(
            found_items=[],
            duplicates=[],
            to_move=[],
            errors=[],
            summary={},
        )

        written_json: dict[str, Any] = {}

        def _write_text(content: str) -> None:
            written_json.update(json.loads(content))

        with patch("src.core.migration.CONFIG_FILE") as mock_cfg:
            mock_cfg.exists.return_value = False
            mock_cfg.write_text = MagicMock(side_effect=_write_text)
            with patch("src.core.migration.Settings.load") as mock_load:
                mock_load.return_value = MagicMock(model_fields={})
                result = await execute_migration(
                    session, preview, str(movies_dir), str(shows_dir)
                )

        assert result.config_updated is True
        assert written_json.get("paths", {}).get("library_movies") == str(movies_dir)
        assert written_json.get("paths", {}).get("library_shows") == str(shows_dir)

    async def test_empty_migration_returns_zeros(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """An empty preview (nothing to do) returns all-zero result."""
        movies_dir = tmp_path / "movies"
        shows_dir = tmp_path / "shows"
        movies_dir.mkdir()
        shows_dir.mkdir()

        preview = MigrationPreview(
            found_items=[],
            duplicates=[],
            to_move=[],
            errors=[],
            summary={},
        )

        with patch("src.core.migration.CONFIG_FILE") as mock_cfg:
            mock_cfg.exists.return_value = False
            mock_cfg.write_text = MagicMock()
            with patch("src.core.migration.Settings.load") as mock_load:
                mock_load.return_value = MagicMock(model_fields={})
                result = await execute_migration(
                    session, preview, str(movies_dir), str(shows_dir)
                )

        assert result.imported == 0
        assert result.moved == 0
        assert result.duplicates_removed == 0
        assert result.errors == []

    async def test_permission_error_during_import_does_not_crash(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        """A flush error during import is caught — other items still process."""
        movies_dir = tmp_path / "movies"
        shows_dir = tmp_path / "shows"
        movies_dir.mkdir()
        shows_dir.mkdir()

        fi = FoundItem(
            title="Bad Movie",
            year=2000,
            media_type="movie",
            season=None,
            episode=None,
            imdb_id=None,
            source_path=None,
            target_path="/bad/path.mkv",
            is_symlink=False,
            resolution=None,
        )
        preview = MigrationPreview(
            found_items=[fi], duplicates=[], to_move=[], errors=[], summary={}
        )

        # Patch session.flush to raise on first call.
        original_flush = session.flush
        call_count = 0

        async def _flaky_flush(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise OSError("disk full")
            return await original_flush(*args, **kwargs)

        session.flush = _flaky_flush  # type: ignore[method-assign]

        with patch("src.core.migration.CONFIG_FILE") as mock_cfg:
            mock_cfg.exists.return_value = False
            mock_cfg.write_text = MagicMock()
            with patch("src.core.migration.Settings.load") as mock_load:
                mock_load.return_value = MagicMock(model_fields={})
                result = await execute_migration(
                    session, preview, str(movies_dir), str(shows_dir)
                )

        # The error was caught — result has 1 error message, did not raise.
        assert len(result.errors) == 1
        assert result.imported == 0


# ---------------------------------------------------------------------------
# Group 10: API route tests
# ---------------------------------------------------------------------------


class TestToolsAPIRoutes:
    """Integration tests for the tools API routes."""

    async def test_get_tools_page_returns_html(self, http: AsyncClient) -> None:
        """GET /tools returns 200 HTML response."""
        response = await http.get("/tools")
        assert response.status_code == 200
        assert "text/html" in response.headers.get("content-type", "")

    async def test_preview_valid_paths_returns_json(
        self, http: AsyncClient, tmp_path: Path
    ) -> None:
        """POST /api/tools/migration/preview with valid dirs returns preview JSON."""
        movies_dir = tmp_path / "movies"
        shows_dir = tmp_path / "shows"
        movies_dir.mkdir()
        shows_dir.mkdir()

        response = await http.post(
            "/api/tools/migration/preview",
            json={"movies_path": str(movies_dir), "shows_path": str(shows_dir)},
        )

        assert response.status_code == 200
        data = response.json()
        assert "found_items" in data
        assert "duplicates" in data
        assert "to_move" in data
        assert "errors" in data
        assert "summary" in data

    async def test_preview_nonexistent_movies_path_returns_400(
        self, http: AsyncClient, tmp_path: Path
    ) -> None:
        """POST /api/tools/migration/preview with nonexistent movies_path returns 400."""
        shows_dir = tmp_path / "shows"
        shows_dir.mkdir()

        response = await http.post(
            "/api/tools/migration/preview",
            json={
                "movies_path": str(tmp_path / "does_not_exist"),
                "shows_path": str(shows_dir),
            },
        )
        assert response.status_code == 400
        assert "movies_path" in response.json()["detail"]

    async def test_preview_nonexistent_shows_path_returns_400(
        self, http: AsyncClient, tmp_path: Path
    ) -> None:
        """POST /api/tools/migration/preview with nonexistent shows_path returns 400."""
        movies_dir = tmp_path / "movies"
        movies_dir.mkdir()

        response = await http.post(
            "/api/tools/migration/preview",
            json={
                "movies_path": str(movies_dir),
                "shows_path": str(tmp_path / "does_not_exist"),
            },
        )
        assert response.status_code == 400
        assert "shows_path" in response.json()["detail"]

    async def test_execute_valid_paths_returns_result(
        self, http: AsyncClient, tmp_path: Path
    ) -> None:
        """POST /api/tools/migration/execute with valid paths returns result JSON."""
        movies_dir = tmp_path / "movies"
        shows_dir = tmp_path / "shows"
        movies_dir.mkdir()
        shows_dir.mkdir()

        with patch("src.core.migration.CONFIG_FILE") as mock_cfg:
            mock_cfg.exists.return_value = False
            mock_cfg.write_text = MagicMock()
            with patch("src.core.migration.Settings.load") as mock_load:
                mock_load.return_value = MagicMock(model_fields={})
                response = await http.post(
                    "/api/tools/migration/execute",
                    json={
                        "movies_path": str(movies_dir),
                        "shows_path": str(shows_dir),
                    },
                )

        assert response.status_code == 200
        data = response.json()
        assert "imported" in data
        assert "moved" in data
        assert "duplicates_removed" in data
        assert "config_updated" in data
        assert "errors" in data

    async def test_execute_nonexistent_path_returns_400(
        self, http: AsyncClient, tmp_path: Path
    ) -> None:
        """POST /api/tools/migration/execute with bad path returns 400."""
        movies_dir = tmp_path / "movies"
        movies_dir.mkdir()

        response = await http.post(
            "/api/tools/migration/execute",
            json={
                "movies_path": str(movies_dir),
                "shows_path": str(tmp_path / "missing"),
            },
        )
        assert response.status_code == 400

    async def test_preview_response_found_items_schema(
        self, http: AsyncClient, tmp_path: Path
    ) -> None:
        """Each found_item in the preview response has expected keys."""
        movies_dir = tmp_path / "movies"
        shows_dir = tmp_path / "shows"
        movies_dir.mkdir()
        shows_dir.mkdir()

        movie_sub = movies_dir / "The Matrix (1999)"
        movie_sub.mkdir()
        (movie_sub / "The.Matrix.1999.1080p.mkv").write_text("v")

        response = await http.post(
            "/api/tools/migration/preview",
            json={"movies_path": str(movies_dir), "shows_path": str(shows_dir)},
        )

        data = response.json()
        assert len(data["found_items"]) == 1
        item = data["found_items"][0]
        for key in ("title", "year", "media_type", "season", "episode",
                    "imdb_id", "source_path", "target_path", "is_symlink", "resolution"):
            assert key in item, f"Key {key!r} missing from found_item response"
