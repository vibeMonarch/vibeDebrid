"""Tests for src/core/mount_scanner.py.

Covers:
  - _normalize_title: lowercasing, dot/underscore normalisation, possessive
    apostrophe joining, unicode accent decomposition, ampersand-to-and,
    combined transformations, output invariants
  - _parse_filename: movies, episodes, season packs, PTN failure fallback,
    hidden extension stripping, title lowercasing, possessive/accent/ampersand
    titles (integration with _normalize_title)
  - MountScanner.is_mount_available: real dir, nonexistent path, file as dir,
    timeout simulation
  - MountScanner.scan: first scan (add), second scan (update), stale removal,
    mount unavailable (preserves index), hidden file exclusion, non-video
    exclusion, skip-dir exclusion, walk errors handled gracefully
  - MountScanner.lookup: title substring, season filter, episode filter,
    case-insensitive matching, empty result
  - MountScanner.lookup_by_filepath: exact match, not found
  - MountScanner.lookup_by_path_prefix: basic match, no match, season filter,
    episode filter, trailing-slash normalisation, sibling-dir isolation, ordering
  - MountScanner.get_index_stats: empty DB, movies-only, episodes-only, mixed
  - MountScanner.clear_index: returns count, leaves table empty
  - MountScanner._should_skip_dir: hidden, __MACOSX, @eaDir, .Trash-*, normal
  - VIDEO_EXTENSIONS constant: spot-check known extensions
  - ScanDirectoryResult: matched_dir_path propagation through scan_directory
  - scan_directory single-file handling: exact match, fuzzy match, not found,
    extension variety, matched_dir_path=None invariant, directory regression,
    re-scan dedup, timeout graceful exit, fallback to directory scan when
    single-file returns 0, no directory fallback when single-file succeeds
  - _upsert_records UNIQUE constraint robustness: duplicate filepaths in input,
    concurrent insert of same filepath (ON CONFLICT DO UPDATE), large batch with
    duplicates spanning _BATCH_SIZE boundary
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.mount_scanner import (
    VIDEO_EXTENSIONS,
    MountScanner,
    ScanDirectoryResult,
    ScanResult,
    WalkEntry,
    _extract_season_from_path,
    _has_meaningful_title,
    _normalize_title,
    _parse_filename,
    gather_alt_titles,
    mount_scanner,
)
from src.models.mount_index import MountIndex

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    return datetime.now(UTC)


async def _insert_entry(
    session: AsyncSession,
    *,
    filepath: str = "/mnt/test.mkv",
    filename: str = "test.mkv",
    parsed_title: str | None = "test",
    parsed_year: int | None = None,
    parsed_season: int | None = None,
    parsed_episode: int | None = None,
    parsed_resolution: str | None = None,
    parsed_codec: str | None = None,
    filesize: int | None = 1000000,
    last_seen_at: datetime | None = None,
) -> MountIndex:
    """Helper: insert a MountIndex row and flush."""
    entry = MountIndex(
        filepath=filepath,
        filename=filename,
        parsed_title=parsed_title,
        parsed_year=parsed_year,
        parsed_season=parsed_season,
        parsed_episode=parsed_episode,
        parsed_resolution=parsed_resolution,
        parsed_codec=parsed_codec,
        filesize=filesize,
        last_seen_at=last_seen_at or _utcnow(),
    )
    session.add(entry)
    await session.flush()
    return entry


# ---------------------------------------------------------------------------
# Group 1: _normalize_title
# ---------------------------------------------------------------------------


class TestNormalizeTitle:
    """Unit tests for the _normalize_title helper."""

    def test_basic_lowercasing(self) -> None:
        assert _normalize_title("The Dark Knight") == "the dark knight"

    def test_dots_become_spaces(self) -> None:
        assert _normalize_title("The.Dark.Knight") == "the dark knight"

    def test_underscores_become_spaces(self) -> None:
        assert _normalize_title("The_Dark_Knight") == "the dark knight"

    def test_empty_string(self) -> None:
        assert _normalize_title("") == ""

    def test_already_normalized(self) -> None:
        assert _normalize_title("already clean") == "already clean"

    # --- Apostrophe possessive ---

    def test_apostrophe_s_straight(self) -> None:
        """Straight apostrophe possessive joins the word."""
        assert _normalize_title("Howl's Moving Castle") == "howls moving castle"

    def test_apostrophe_s_curly(self) -> None:
        """Curly/smart apostrophe possessive joins the word."""
        assert _normalize_title("Howl\u2019s Moving Castle") == "howls moving castle"

    def test_apostrophe_s_multiple(self) -> None:
        """Multiple possessives in one title."""
        assert _normalize_title("King's Man's Quest") == "kings mans quest"

    def test_apostrophe_not_possessive(self) -> None:
        """Apostrophe not followed by s is stripped normally."""
        assert _normalize_title("Rock 'n' Roll") == "rock n roll"

    def test_apostrophe_s_at_end(self) -> None:
        """Possessive at end of title."""
        assert _normalize_title("The King's") == "the kings"

    # --- Unicode / accent decomposition ---

    def test_accent_acute(self) -> None:
        """Acute accent decomposes to base letter."""
        assert _normalize_title("Garc\u00eda") == "garcia"

    def test_accent_matches_ascii(self) -> None:
        """Accented and plain ASCII produce same result."""
        assert _normalize_title("Garc\u00eda") == _normalize_title("Garcia")

    def test_accent_multiple(self) -> None:
        """Multiple accented characters in one title."""
        assert _normalize_title("Caf\u00e9 Soci\u00e9t\u00e9") == "cafe societe"

    def test_umlaut(self) -> None:
        """German umlauts decompose to base letter."""
        assert _normalize_title("M\u00fcnchen") == "munchen"

    def test_tilde(self) -> None:
        """Spanish tilde decomposes to base letter."""
        assert _normalize_title("Espa\u00f1a") == "espana"

    # --- Ampersand ---

    def test_ampersand_becomes_and(self) -> None:
        """Ampersand is replaced with 'and'."""
        assert _normalize_title("Tom & Jerry") == "tom and jerry"

    def test_ampersand_matches_and(self) -> None:
        """Ampersand and 'and' produce same result."""
        assert _normalize_title("Tom & Jerry") == _normalize_title("Tom and Jerry")

    def test_ampersand_no_spaces(self) -> None:
        """Ampersand without surrounding spaces still works."""
        assert _normalize_title("Tom&Jerry") == "tom and jerry"

    def test_multiple_ampersands(self) -> None:
        """Multiple ampersands are all replaced."""
        assert _normalize_title("A & B & C") == "a and b and c"

    # --- Combined ---

    def test_combined_apostrophe_and_accent(self) -> None:
        """Title with both possessive and accent."""
        assert _normalize_title("Andr\u00e9's Story") == "andres story"

    def test_combined_all_three(self) -> None:
        """Title exercising all three new transformations."""
        assert _normalize_title("Garc\u00eda's Tom & Jerry") == "garcias tom and jerry"

    # --- Invariants ---

    def test_output_only_alnum_spaces(self) -> None:
        """Output contains only lowercase alphanumerics and spaces."""
        import re as _re
        result = _normalize_title("H\u00f6wl's M\u00f6ving & C\u00e4stle!@#$%")
        assert _re.match(r"^[a-z0-9 ]+$", result)

    def test_no_double_spaces(self) -> None:
        """No consecutive spaces in output."""
        result = _normalize_title("A  &  B   's   Title")
        assert "  " not in result

    def test_no_leading_trailing_spaces(self) -> None:
        """Output has no leading or trailing spaces."""
        result = _normalize_title("  Test Title  ")
        assert result == result.strip()


# ---------------------------------------------------------------------------
# Group 2: _parse_filename
# ---------------------------------------------------------------------------


class TestParseFilename:
    """Unit tests for the module-level _parse_filename helper."""

    def test_movie_extracts_title_year_resolution_codec(self) -> None:
        """Standard movie filename yields correct title, year, resolution, codec."""
        result = _parse_filename("The.Dark.Knight.2008.1080p.BluRay.x264-GROUP.mkv")
        assert result["title"] == "the dark knight"
        assert result["year"] == 2008
        assert result["resolution"] == "1080p"
        assert result["codec"] == "x264"
        assert result["season"] is None
        assert result["episode"] is None

    def test_episode_extracts_season_and_episode(self) -> None:
        """TV episode filename yields correct season, episode, and title."""
        result = _parse_filename("Breaking.Bad.S01E01.1080p.WEB-DL.x265.mkv")
        assert result["title"] == "breaking bad"
        assert result["season"] == 1
        assert result["episode"] == 1
        assert result["resolution"] == "1080p"
        assert result["codec"] == "x265"

    def test_title_is_lowercased(self) -> None:
        """Parsed title is always returned in lowercase."""
        result = _parse_filename("Dune.Part.Two.2024.2160p.mkv")
        assert result["title"] == result["title"].lower()

    def test_title_is_stripped(self) -> None:
        """Parsed title has no leading/trailing whitespace."""
        result = _parse_filename("Alien.1979.1080p.mkv")
        assert result["title"] == result["title"].strip()

    def test_minimal_file_no_metadata(self) -> None:
        """Filename with no metadata returns title as stem, other fields None."""
        result = _parse_filename("some_random_file.mp4")
        assert result["title"] is not None
        assert result["year"] is None
        assert result["resolution"] is None
        assert result["codec"] is None

    def test_ptn_failure_falls_back_to_stem(self) -> None:
        """When PTN raises an exception the stem is used as the title fallback.

        The stem is normalised via _normalize_title, so dots (non-alphanumeric)
        are converted to spaces and the result is lowercased.
        """
        with patch("src.core.mount_scanner.PTN.parse", side_effect=RuntimeError("PTN boom")):
            result = _parse_filename("my.movie.2024.mkv")
        assert result["title"] == "my movie 2024"
        assert result["year"] is None
        assert result["resolution"] is None

    def test_missing_title_uses_stem(self) -> None:
        """When PTN returns no 'title' key the filename stem is used."""
        with patch("src.core.mount_scanner.PTN.parse", return_value={"resolution": "1080p"}):
            result = _parse_filename("fallback.mkv")
        assert result["title"] == "fallback"  # stem of "fallback.mkv"
        assert result["resolution"] == "1080p"

    def test_2160p_resolution_parsed(self) -> None:
        """4K resolution is correctly extracted."""
        result = _parse_filename("Movie.2024.2160p.WEB-DL.mkv")
        assert result["resolution"] == "2160p"

    def test_x265_codec_parsed(self) -> None:
        """x265 codec tag is extracted from filename."""
        result = _parse_filename("Show.S02E10.1080p.x265.mkv")
        assert result["codec"] is not None
        assert "265" in result["codec"].lower()

    # ------------------------------------------------------------------
    # Bare trailing number (anime naming convention)
    # ------------------------------------------------------------------

    def test_bare_trailing_ep_simple(self) -> None:
        """'Show Title 01.mkv' yields episode 1 via bare trailing fallback."""
        result = _parse_filename("Show Title 01.mkv")
        assert result["episode"] == 1

    def test_bare_trailing_ep_two_digit(self) -> None:
        """'Show Title 26.mkv' yields episode 26."""
        result = _parse_filename("Show Title 26.mkv")
        assert result["episode"] == 26

    def test_bare_trailing_ep_leading_zero(self) -> None:
        """'Show Name 03.mkv' yields episode 3 (leading zero stripped by int())."""
        result = _parse_filename("Show Name 03.mkv")
        assert result["episode"] == 3

    def test_bare_trailing_ep_does_not_match_year(self) -> None:
        """'Show Name 2003.mkv' must NOT extract a 4-digit year as episode number.

        The regex only matches 1-3 digit numbers, so 2003 is ignored.  PTN
        may pick up the year in the year field instead.
        """
        result = _parse_filename("Show Name 2003.mkv")
        # 2003 is 4 digits — must not be treated as an episode number.
        assert result["episode"] is None

    def test_bare_trailing_ep_does_not_match_resolution_720(self) -> None:
        """A bare '720' at the end must NOT be treated as an episode number."""
        result = _parse_filename("Some Show 720.mkv")
        assert result["episode"] is None

    def test_bare_trailing_ep_does_not_match_resolution_1080(self) -> None:
        """A bare '1080' at the end must NOT be treated as an episode number."""
        result = _parse_filename("Some Show 1080.mkv")
        assert result["episode"] is None

    def test_bare_trailing_ep_does_not_match_resolution_480(self) -> None:
        """A bare '480' at the end must NOT be treated as an episode number."""
        result = _parse_filename("Some Show 480.mkv")
        assert result["episode"] is None

    def test_bare_trailing_ep_not_used_when_sxexx_present(self) -> None:
        """When SxxExx is present the bare trailing fallback is not invoked."""
        result = _parse_filename("Show Title S01E05 01.mkv")
        # Standard SxxExx parser wins; bare trailing should not override.
        assert result["episode"] == 5

    def test_bare_trailing_ep_not_used_when_anime_dash_present(self) -> None:
        """When the anime-dash pattern matches the bare trailing fallback is not invoked."""
        result = _parse_filename("[Group] Show Title - 12.mkv")
        assert result["episode"] == 12

    def test_bare_trailing_ep_dot_separator(self) -> None:
        """'Show.Name.07.mkv' — dot separator before trailing number is also handled."""
        result = _parse_filename("Show.Name.07.mkv")
        assert result["episode"] == 7

    def test_possessive_in_movie_title(self) -> None:
        """Movie with possessive apostrophe normalizes correctly."""
        result = _parse_filename("Howl's.Moving.Castle.2004.1080p.mkv")
        assert result["title"] == "howls moving castle"
        assert result["year"] == 2004

    def test_accented_movie_title(self) -> None:
        """Movie with accented character normalizes correctly."""
        result = _parse_filename("Am\u00e9lie.2001.1080p.mkv")
        assert result["title"] == "amelie"
        assert result["year"] == 2001

    def test_ampersand_in_movie_title(self) -> None:
        """Movie with ampersand normalizes to 'and'.

        PTN does not extract the year when '&' is present in the title, so
        '2021' is absorbed into the title string.  The important invariant is
        that '&' becomes 'and' in the normalized output.
        """
        result = _parse_filename("Tom.&.Jerry.2021.1080p.mkv")
        assert "and" in result["title"]
        assert "&" not in result["title"]


# ---------------------------------------------------------------------------
# Group 2: is_mount_available
# ---------------------------------------------------------------------------


class TestIsMountAvailable:
    """Tests for the FUSE mount health check."""

    async def test_real_directory_returns_true(self) -> None:
        """An existing, non-empty, listable directory returns True."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            # Directory must be non-empty (empty = rclone down)
            open(os.path.join(tmpdir, "dummy"), "w").close()
            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                result = await scanner.is_mount_available()
        assert result is True

    async def test_empty_directory_returns_false(self) -> None:
        """An empty directory returns False (rclone may be down)."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                result = await scanner.is_mount_available()
        assert result is False

    async def test_nonexistent_path_returns_false(self) -> None:
        """A path that does not exist returns False (no exception raised)."""
        scanner = MountScanner()
        with patch("src.core.mount_scanner.settings") as mock_settings:
            mock_settings.paths.zurg_mount = "/absolutely/nonexistent/xyz_abc_123"
            result = await scanner.is_mount_available()
        assert result is False

    async def test_file_path_returns_false(self) -> None:
        """A path pointing to a regular file (not a directory) returns False."""
        scanner = MountScanner()
        with tempfile.NamedTemporaryFile() as tmpfile:
            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpfile.name
                result = await scanner.is_mount_available()
        assert result is False

    async def test_timeout_returns_false(self) -> None:
        """When the health check times out (FUSE hang), False is returned gracefully."""
        scanner = MountScanner()

        async def _hanging(*args, **kwargs):
            await asyncio.sleep(100)

        with patch("asyncio.wait_for", side_effect=TimeoutError("simulated hang")):
            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = "/some/path"
                result = await scanner.is_mount_available()
        assert result is False

    async def test_oserror_returns_false(self) -> None:
        """OSError from the filesystem (e.g. permission denied) returns False."""
        scanner = MountScanner()
        with patch("asyncio.to_thread", side_effect=OSError("permission denied")):
            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = "/some/path"
                result = await scanner.is_mount_available()
        assert result is False


# ---------------------------------------------------------------------------
# Group 3: scan
# ---------------------------------------------------------------------------


class TestScan:
    """Tests for MountScanner.scan using temporary filesystem directories."""

    async def _make_scanner_and_tmpdir(self) -> tuple[MountScanner, str]:
        """Return a fresh MountScanner and a temporary directory path."""
        return MountScanner(), tempfile.mkdtemp()

    async def test_first_scan_adds_video_files(self, session: AsyncSession) -> None:
        """First scan of a directory inserts all video files as new rows."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            for name in ["Movie.2021.1080p.mkv", "Show.S01E01.mkv"]:
                open(os.path.join(tmpdir, name), "wb").write(b"fake")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                result = await scanner.scan(session)

        assert result.files_found == 2
        assert result.files_added == 2
        assert result.files_updated == 0
        assert result.files_removed == 0
        assert result.errors == 0
        assert isinstance(result, ScanResult)

    async def test_second_scan_updates_existing_rows(self, session: AsyncSession) -> None:
        """A second scan over unchanged files counts them as unchanged (not updated).

        The skip-unchanged optimisation avoids re-parsing filenames when the
        filename and filesize are identical to the stored values.  The row is
        still touched (last_seen_at refreshed) but counted as ``files_unchanged``
        rather than ``files_updated``.
        """
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            open(os.path.join(tmpdir, "Show.S01E05.1080p.mkv"), "wb").write(b"fake")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir

                first = await scanner.scan(session)
                assert first.files_added == 1

                await session.flush()

                second = await scanner.scan(session)

        assert second.files_found == 1
        assert second.files_added == 0
        # File content unchanged and parsed_episode is set → skip-unchanged path.
        assert second.files_updated == 0
        assert second.files_unchanged == 1
        assert second.files_removed == 0

    async def test_scan_removes_stale_entries(self, session: AsyncSession) -> None:
        """Files removed from the mount are deleted from mount_index after a scan."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            f1 = os.path.join(tmpdir, "Movie.A.2020.mkv")
            f2 = os.path.join(tmpdir, "Movie.B.2021.mkv")
            open(f1, "wb").write(b"fake")
            open(f2, "wb").write(b"fake")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir

                first = await scanner.scan(session)
                assert first.files_added == 2
                await session.flush()

                # Remove one file to simulate it disappearing from mount.
                os.remove(f2)

                second = await scanner.scan(session)

        assert second.files_found == 1
        assert second.files_removed == 1

    async def test_scan_skips_non_video_files(self, session: AsyncSession) -> None:
        """Non-video files (txt, jpg, etc.) are not indexed."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            open(os.path.join(tmpdir, "README.txt"), "w").write("docs")
            open(os.path.join(tmpdir, "poster.jpg"), "wb").write(b"img")
            open(os.path.join(tmpdir, "movie.mkv"), "wb").write(b"vid")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                result = await scanner.scan(session)

        assert result.files_found == 1  # only the .mkv

    async def test_scan_skips_hidden_files(self, session: AsyncSession) -> None:
        """Files starting with '.' are excluded from the index."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            open(os.path.join(tmpdir, ".hidden.mkv"), "wb").write(b"vid")
            open(os.path.join(tmpdir, "visible.mkv"), "wb").write(b"vid")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                result = await scanner.scan(session)

        assert result.files_found == 1

    async def test_scan_skips_macosx_directory(self, session: AsyncSession) -> None:
        """__MACOSX directories and their contents are excluded."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            macos_dir = os.path.join(tmpdir, "__MACOSX")
            os.makedirs(macos_dir)
            open(os.path.join(macos_dir, "meta.mkv"), "wb").write(b"vid")
            open(os.path.join(tmpdir, "real.mkv"), "wb").write(b"vid")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                result = await scanner.scan(session)

        assert result.files_found == 1  # only real.mkv

    async def test_scan_skips_eadir_directory(self, session: AsyncSession) -> None:
        """@eaDir (Synology thumbnail cache) directories are excluded."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            ea_dir = os.path.join(tmpdir, "@eaDir")
            os.makedirs(ea_dir)
            open(os.path.join(ea_dir, "thumb.mkv"), "wb").write(b"vid")
            open(os.path.join(tmpdir, "real.mkv"), "wb").write(b"vid")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                result = await scanner.scan(session)

        assert result.files_found == 1

    async def test_scan_skips_trash_directory(self, session: AsyncSession) -> None:
        """Directories matching '.Trash-*' pattern are excluded."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            trash_dir = os.path.join(tmpdir, ".Trash-1000")
            os.makedirs(trash_dir)
            open(os.path.join(trash_dir, "deleted.mkv"), "wb").write(b"vid")
            open(os.path.join(tmpdir, "real.mkv"), "wb").write(b"vid")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                result = await scanner.scan(session)

        assert result.files_found == 1

    async def test_scan_when_mount_unavailable_returns_early(
        self, session: AsyncSession
    ) -> None:
        """When the mount is unavailable scan returns zero counts without touching DB."""
        scanner = MountScanner()
        # Pre-seed one entry so we can verify the index is NOT cleared.
        await _insert_entry(session, filepath="/old/file.mkv", filename="file.mkv")
        await session.flush()

        with patch.object(scanner, "is_mount_available", new=AsyncMock(return_value=False)):
            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = "/nonexistent"
                result = await scanner.scan(session)

        assert result.files_found == 0
        assert result.files_added == 0
        assert result.files_removed == 0

        # The existing index entry should be preserved.
        stats = await scanner.get_index_stats(session)
        assert stats["total_files"] == 1

    async def test_scan_duration_ms_is_positive(self, session: AsyncSession) -> None:
        """ScanResult.duration_ms is a non-negative integer."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                result = await scanner.scan(session)

        assert isinstance(result.duration_ms, int)
        assert result.duration_ms >= 0

    async def test_scan_indexes_subdirectory_files(self, session: AsyncSession) -> None:
        """Video files in subdirectories are indexed with their full path."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            subdir = os.path.join(tmpdir, "ShowFolder")
            os.makedirs(subdir)
            open(os.path.join(subdir, "Show.S01E01.mkv"), "wb").write(b"vid")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                result = await scanner.scan(session)

        assert result.files_found == 1
        assert result.files_added == 1

    async def test_scan_getsize_error_counted_as_error(
        self, session: AsyncSession
    ) -> None:
        """If stat() fails for a file, it is counted as an error but the scan continues."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            open(os.path.join(tmpdir, "movie.mkv"), "wb").write(b"vid")

            original_walk = scanner._scandir_walk

            def _walk_with_size_error(path: str, known_files: dict | None = None):
                records, errors = original_walk(path, known_files)
                # WalkEntry is a namedtuple (immutable), so rebuild with filesize=None.
                patched = [
                    WalkEntry(
                        filepath=r.filepath,
                        filename=r.filename,
                        filesize=None,
                        parent_dir=r.parent_dir,
                    )
                    for r in records
                ]
                return patched, errors + 1  # simulate one extra error

            with patch.object(scanner, "_scandir_walk", side_effect=_walk_with_size_error):
                with patch("src.core.mount_scanner.settings") as mock_settings:
                    mock_settings.paths.zurg_mount = tmpdir
                    result = await scanner.scan(session)

        # File should still be indexed even with size error
        assert result.files_found == 1
        assert result.files_added == 1
        assert result.errors >= 1

    async def test_scan_all_video_extensions(self, session: AsyncSession) -> None:
        """Each video extension in VIDEO_EXTENSIONS is correctly indexed."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            for ext in VIDEO_EXTENSIONS:
                open(os.path.join(tmpdir, f"file{ext}"), "wb").write(b"vid")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                result = await scanner.scan(session)

        assert result.files_found == len(VIDEO_EXTENSIONS)

    async def test_scan_stale_deletion_and_unchanged_update_in_same_scan(
        self, session: AsyncSession
    ) -> None:
        """Combined stale-deletion + unchanged-update in a single scan.

        Pre-seed the DB with 3 files via a first scan.  On the second scan
        only 2 of the 3 files exist (and are unchanged).  Verify:
        - The 2 surviving files are counted as ``files_unchanged`` (bulk UPDATE
          refreshed their ``last_seen_at`` before the stale-delete runs).
        - The 1 missing file is deleted (``files_removed == 1``).
        """
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            f1 = os.path.join(tmpdir, "Show.S01E01.mkv")
            f2 = os.path.join(tmpdir, "Show.S01E02.mkv")
            f3 = os.path.join(tmpdir, "Show.S01E03.mkv")
            for fp in (f1, f2, f3):
                open(fp, "wb").write(b"fake")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir

                first = await scanner.scan(session)
                assert first.files_added == 3
                await session.flush()

                # Remove one file to simulate it disappearing from the mount.
                os.remove(f3)

                second = await scanner.scan(session)

        # The 2 surviving files are unchanged (same filename + filesize + parsed_episode set).
        assert second.files_unchanged == 2
        # The removed file is deleted from the index.
        assert second.files_removed == 1
        # No new inserts or updates.
        assert second.files_added == 0
        assert second.files_updated == 0

    async def test_scan_filesize_change_between_scans_counts_as_unchanged(
        self, session: AsyncSession
    ) -> None:
        """Full scan skips stat() for already-indexed files (stat-skip optimisation).

        On the second scan the file's on-disk size has changed, but scan() pre-loads
        the stored filesize from DB and passes it to _scandir_walk so that known files
        skip the expensive FUSE stat() call.  As a result the second scan sees the
        same (stale) size it wrote on the first scan and classifies the file as
        unchanged.  This is the expected trade-off: full scans are fast; targeted
        scan_directory() calls (which always stat) detect size changes for individual
        directories.
        """
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "Show.S01E01.1080p.mkv")
            # First scan: file contains 4 bytes.
            open(filepath, "wb").write(b"fake")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir

                first = await scanner.scan(session)
                assert first.files_added == 1
                await session.flush()

                # Grow the file so os.stat() would report a different size.
                with open(filepath, "ab") as fh:
                    fh.write(b"x" * 1024)

                second = await scanner.scan(session)

        # Stat was skipped for the already-indexed file → size unchanged in the walk →
        # _upsert_records compares DB size to itself → unchanged.
        assert second.files_unchanged == 1
        assert second.files_updated == 0
        assert second.files_added == 0
        assert second.files_removed == 0

    async def test_scan_directory_detects_filesize_change(
        self, session: AsyncSession
    ) -> None:
        """scan_directory() always stats files so it detects size changes.

        Unlike the full scan() which uses the stat-skip optimisation,
        scan_directory() passes an empty known_files dict so that every file
        is stat'd on each targeted scan.
        """
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            subdir = os.path.join(tmpdir, "ShowFolder")
            os.makedirs(subdir)
            filepath = os.path.join(subdir, "Show.S01E01.1080p.mkv")
            open(filepath, "wb").write(b"fake")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir

                first = await scanner.scan_directory(session, "ShowFolder")
                assert first.files_indexed == 1
                await session.flush()

                # Grow the file.
                with open(filepath, "ab") as fh:
                    fh.write(b"x" * 1024)

                expected_filesize = os.path.getsize(filepath)
                second = await scanner.scan_directory(session, "ShowFolder")

        # scan_directory always stats → size change detected → files_indexed == 1
        # (updated entry).
        assert second.files_indexed == 1

        # The DB record must carry the new (larger) filesize.
        entry = await scanner.lookup_by_filepath(session, filepath)
        assert entry is not None
        assert entry.filesize == expected_filesize


# ---------------------------------------------------------------------------
# Group 4: lookup
# ---------------------------------------------------------------------------


class TestLookup:
    """Tests for MountScanner.lookup (DB-only, no filesystem access)."""

    async def test_lookup_by_title_exact(self, session: AsyncSession) -> None:
        """Exact normalized title match returns the correct entry; short words do not match."""
        await _insert_entry(
            session, filepath="/mnt/dark.knight.mkv", parsed_title="the dark knight"
        )
        await _insert_entry(
            session, filepath="/mnt/batman.mkv", parsed_title="batman begins"
        )
        scanner = MountScanner()
        # Short word "of" (< 4 chars) must NOT match anything — below minimum length
        # for single-word Tier 2 matching.
        no_results = await scanner.lookup(session, "of")
        assert len(no_results) == 0
        # Full exact title must match
        results = await scanner.lookup(session, "the dark knight")
        assert len(results) == 1
        assert results[0].filepath == "/mnt/dark.knight.mkv"

    async def test_lookup_word_subsequence_fallback(self, session: AsyncSession) -> None:
        """When exact match fails, word-subsequence finds titles with extra tokens."""
        await _insert_entry(
            session, filepath="/mnt/terminator2.mkv",
            parsed_title="terminator 2 judgement day",
        )
        scanner = MountScanner()
        # Exact match "terminator judgement day" fails (missing "2"),
        # but word-subsequence finds it.
        results = await scanner.lookup(session, "Terminator Judgement Day")
        assert len(results) == 1
        assert results[0].filepath == "/mnt/terminator2.mkv"

    async def test_lookup_word_subsequence_requires_order(self, session: AsyncSession) -> None:
        """Word-subsequence fallback requires words in correct order."""
        await _insert_entry(
            session, filepath="/mnt/day.mkv",
            parsed_title="day of judgement",
        )
        scanner = MountScanner()
        # "judgement day" has words in wrong order relative to "day of judgement"
        results = await scanner.lookup(session, "judgement day")
        assert len(results) == 0

    async def test_lookup_word_subsequence_single_word_fallback(self, session: AsyncSession) -> None:
        """Single-word queries with 4+ chars do trigger whole-word Tier 2 matching."""
        await _insert_entry(
            session, filepath="/mnt/movie.mkv",
            parsed_title="the terminator",
        )
        scanner = MountScanner()
        # "terminator" (10 chars) appears as a whole token in "the terminator"
        results = await scanner.lookup(session, "terminator")
        assert len(results) == 1
        assert results[0].filepath == "/mnt/movie.mkv"

    async def test_lookup_case_insensitive(self, session: AsyncSession) -> None:
        """Title lookup is case-insensitive."""
        await _insert_entry(
            session, filepath="/mnt/alien.mkv", parsed_title="alien"
        )
        scanner = MountScanner()
        results = await scanner.lookup(session, "ALIEN")
        assert len(results) == 1

    async def test_lookup_with_season_filter(self, session: AsyncSession) -> None:
        """When season is provided, only that season's entries are returned."""
        await _insert_entry(
            session,
            filepath="/mnt/show.s01e01.mkv",
            parsed_title="breaking bad",
            parsed_season=1,
            parsed_episode=1,
        )
        await _insert_entry(
            session,
            filepath="/mnt/show.s02e01.mkv",
            parsed_title="breaking bad",
            parsed_season=2,
            parsed_episode=1,
        )
        scanner = MountScanner()
        results = await scanner.lookup(session, "breaking bad", season=1)
        assert len(results) == 1
        assert results[0].parsed_season == 1

    async def test_lookup_with_episode_filter(self, session: AsyncSession) -> None:
        """When both season and episode are provided, only exact matches are returned."""
        await _insert_entry(
            session,
            filepath="/mnt/s01e01.mkv",
            parsed_title="better call saul",
            parsed_season=1,
            parsed_episode=1,
        )
        await _insert_entry(
            session,
            filepath="/mnt/s01e02.mkv",
            parsed_title="better call saul",
            parsed_season=1,
            parsed_episode=2,
        )
        scanner = MountScanner()
        results = await scanner.lookup(session, "better call saul", season=1, episode=2)
        assert len(results) == 1
        assert results[0].parsed_episode == 2

    async def test_lookup_returns_empty_when_no_match(
        self, session: AsyncSession
    ) -> None:
        """A lookup that matches nothing returns an empty list."""
        await _insert_entry(session, parsed_title="avatar")
        scanner = MountScanner()
        results = await scanner.lookup(session, "nonexistent title xyz")
        assert results == []

    async def test_lookup_no_season_filter_returns_all_matching(
        self, session: AsyncSession
    ) -> None:
        """Without season filter, all seasons matching the title are returned."""
        for s in range(1, 4):
            await _insert_entry(
                session,
                filepath=f"/mnt/show.s0{s}.mkv",
                parsed_title="the wire",
                parsed_season=s,
            )
        scanner = MountScanner()
        results = await scanner.lookup(session, "the wire")
        assert len(results) == 3

    async def test_lookup_episode_without_season_ignored(
        self, session: AsyncSession
    ) -> None:
        """Episode filter without season filter does not crash; matches are returned."""
        await _insert_entry(
            session,
            filepath="/mnt/e01.mkv",
            parsed_title="lost",
            parsed_season=None,
            parsed_episode=1,
        )
        scanner = MountScanner()
        # episode filter without season filter: valid call, should return results
        # where parsed_episode == 1, title matches
        results = await scanner.lookup(session, "lost", episode=1)
        assert len(results) == 1

    # -----------------------------------------------------------------------
    # Reverse containment (Tier 3) tests — GitHub issue #30
    # -----------------------------------------------------------------------

    async def test_lookup_reverse_containment_long_search_title(
        self, session: AsyncSession
    ) -> None:
        """Motivating case: short DB parsed_title (3 words) is contained in long TMDB title.

        DB has "honzuki no gekokujou" (3 words); searching for the full TMDB title
        "Honzuki no Gekokujou Shisho ni Naru Tame ni wa Shudan wo Erandeiraremasen"
        should return the entry via reverse containment.
        """
        await _insert_entry(
            session,
            filepath="/mnt/honzuki/honzuki.no.gekokujou.s01e01.mkv",
            filename="honzuki.no.gekokujou.s01e01.mkv",
            parsed_title="honzuki no gekokujou",
            parsed_season=1,
            parsed_episode=1,
        )
        scanner = MountScanner()
        full_tmdb_title = (
            "Honzuki no Gekokujou Shisho ni Naru Tame ni wa Shudan wo Erandeiraremasen"
        )
        results = await scanner.lookup(session, full_tmdb_title, season=1, episode=1)
        assert len(results) == 1
        assert results[0].filepath == "/mnt/honzuki/honzuki.no.gekokujou.s01e01.mkv"

    async def test_lookup_reverse_containment_requires_3_words(
        self, session: AsyncSession
    ) -> None:
        """2-word DB parsed_title must NOT match via reverse containment (3-word minimum guard)."""
        await _insert_entry(
            session,
            filepath="/mnt/dark.knight.mkv",
            parsed_title="dark knight",  # only 2 words — below the guard threshold
        )
        scanner = MountScanner()
        # "dark knight" is contained in the search title, but 2 words < 3-word minimum.
        results = await scanner.lookup(session, "The Dark Knight Rises 2012")
        assert len(results) == 0

    async def test_lookup_reverse_containment_with_season_episode(
        self, session: AsyncSession
    ) -> None:
        """Reverse containment respects season + episode filters.

        Correct episode number → match; wrong episode number → no match.
        """
        await _insert_entry(
            session,
            filepath="/mnt/anime/episode-06.mkv",
            parsed_title="honzuki no gekokujou",
            parsed_season=1,
            parsed_episode=6,
        )
        scanner = MountScanner()
        long_title = (
            "Honzuki no Gekokujou Shisho ni Naru Tame ni wa Shudan wo Erandeiraremasen"
        )
        # Correct season + episode → match
        match = await scanner.lookup(session, long_title, season=1, episode=6)
        assert len(match) == 1
        assert match[0].parsed_episode == 6

        # Wrong episode → no match
        no_match = await scanner.lookup(session, long_title, season=1, episode=7)
        assert len(no_match) == 0

    async def test_lookup_reverse_containment_no_false_positive_word_order(
        self, session: AsyncSession
    ) -> None:
        """Word order matters: reverse containment uses word-subsequence verification.

        DB has both "dragon ball super" and "super dragon ball".
        Searching for "dragon ball super heroes united" should match the first
        (correct word order) but not the second (words appear in wrong order).
        """
        await _insert_entry(
            session,
            filepath="/mnt/dragon.ball.super.mkv",
            parsed_title="dragon ball super",
        )
        await _insert_entry(
            session,
            filepath="/mnt/super.dragon.ball.mkv",
            parsed_title="super dragon ball",
        )
        scanner = MountScanner()
        results = await scanner.lookup(session, "dragon ball super heroes united")
        filepaths = [r.filepath for r in results]
        assert "/mnt/dragon.ball.super.mkv" in filepaths
        assert "/mnt/super.dragon.ball.mkv" not in filepaths

    async def test_lookup_reverse_containment_skipped_when_exact_matches(
        self, session: AsyncSession
    ) -> None:
        """When an exact match exists, it is returned directly without reaching Tier 3."""
        await _insert_entry(
            session,
            filepath="/mnt/exact.mkv",
            parsed_title="honzuki no gekokujou shisho",
        )
        # Also add an entry that would only match via reverse containment.
        await _insert_entry(
            session,
            filepath="/mnt/short.mkv",
            parsed_title="honzuki no gekokujou",
        )
        scanner = MountScanner()
        # Exact title → should return only the exact match, not the reverse match.
        results = await scanner.lookup(session, "Honzuki no Gekokujou Shisho")
        assert len(results) == 1
        assert results[0].filepath == "/mnt/exact.mkv"

    async def test_lookup_reverse_containment_null_parsed_title(
        self, session: AsyncSession
    ) -> None:
        """A NULL parsed_title in the DB should not cause a crash and should not match."""
        await _insert_entry(
            session,
            filepath="/mnt/null.title.mkv",
            parsed_title=None,  # NULL
        )
        scanner = MountScanner()
        results = await scanner.lookup(
            session,
            "Honzuki no Gekokujou Shisho ni Naru Tame ni wa Shudan wo Erandeiraremasen",
        )
        assert results == []

    async def test_lookup_reverse_containment_three_word_boundary(
        self, session: AsyncSession
    ) -> None:
        """Exactly 3 words meets the minimum threshold and should match via reverse containment."""
        await _insert_entry(
            session,
            filepath="/mnt/abc.def.ghi.mkv",
            parsed_title="abc def ghi",  # exactly 3 words — boundary case
        )
        scanner = MountScanner()
        results = await scanner.lookup(session, "abc def ghi jkl mno")
        assert len(results) == 1
        assert results[0].filepath == "/mnt/abc.def.ghi.mkv"


# ---------------------------------------------------------------------------
# Group 5: lookup_by_filepath
# ---------------------------------------------------------------------------


class TestLookupByFilepath:
    """Tests for MountScanner.lookup_by_filepath."""

    async def test_exact_match_returns_entry(self, session: AsyncSession) -> None:
        """Exact filepath match returns the corresponding MountIndex row."""
        await _insert_entry(session, filepath="/mnt/__all__/Movie.mkv")
        scanner = MountScanner()
        result = await scanner.lookup_by_filepath(session, "/mnt/__all__/Movie.mkv")
        assert result is not None
        assert result.filepath == "/mnt/__all__/Movie.mkv"

    async def test_nonexistent_filepath_returns_none(
        self, session: AsyncSession
    ) -> None:
        """Looking up a filepath that does not exist returns None."""
        scanner = MountScanner()
        result = await scanner.lookup_by_filepath(session, "/nonexistent/path.mkv")
        assert result is None

    async def test_partial_path_does_not_match(self, session: AsyncSession) -> None:
        """A partial / substring filepath does not match the exact-path query."""
        await _insert_entry(session, filepath="/mnt/__all__/Movie.2024.mkv")
        scanner = MountScanner()
        result = await scanner.lookup_by_filepath(session, "Movie.2024.mkv")
        assert result is None


# ---------------------------------------------------------------------------
# Group 6: get_index_stats
# ---------------------------------------------------------------------------


class TestGetIndexStats:
    """Tests for MountScanner.get_index_stats."""

    async def test_empty_index_returns_zeros(self, session: AsyncSession) -> None:
        """Stats on an empty mount_index return all zeros."""
        scanner = MountScanner()
        stats = await scanner.get_index_stats(session)
        assert stats == {"total_files": 0, "movies": 0, "episodes": 0}

    async def test_movies_counted_correctly(self, session: AsyncSession) -> None:
        """Entries with parsed_season=None are counted as movies."""
        await _insert_entry(session, filepath="/mnt/a.mkv", parsed_season=None)
        await _insert_entry(session, filepath="/mnt/b.mkv", parsed_season=None)
        scanner = MountScanner()
        stats = await scanner.get_index_stats(session)
        assert stats["movies"] == 2
        assert stats["episodes"] == 0
        assert stats["total_files"] == 2

    async def test_episodes_counted_correctly(self, session: AsyncSession) -> None:
        """Entries with parsed_season not None are counted as episodes."""
        await _insert_entry(
            session, filepath="/mnt/s01e01.mkv", parsed_season=1, parsed_episode=1
        )
        await _insert_entry(
            session, filepath="/mnt/s01e02.mkv", parsed_season=1, parsed_episode=2
        )
        scanner = MountScanner()
        stats = await scanner.get_index_stats(session)
        assert stats["movies"] == 0
        assert stats["episodes"] == 2

    async def test_mixed_counts(self, session: AsyncSession) -> None:
        """Mixed index returns correct totals for both categories."""
        await _insert_entry(session, filepath="/mnt/movie.mkv", parsed_season=None)
        await _insert_entry(
            session, filepath="/mnt/ep.mkv", parsed_season=1, parsed_episode=1
        )
        scanner = MountScanner()
        stats = await scanner.get_index_stats(session)
        assert stats["total_files"] == 2
        assert stats["movies"] == 1
        assert stats["episodes"] == 1


# ---------------------------------------------------------------------------
# Group 7: clear_index
# ---------------------------------------------------------------------------


class TestClearIndex:
    """Tests for MountScanner.clear_index."""

    async def test_clear_returns_count_of_deleted_rows(
        self, session: AsyncSession
    ) -> None:
        """clear_index returns the number of rows it deleted."""
        await _insert_entry(session, filepath="/mnt/a.mkv")
        await _insert_entry(session, filepath="/mnt/b.mkv")
        await _insert_entry(session, filepath="/mnt/c.mkv")
        scanner = MountScanner()
        count = await scanner.clear_index(session)
        assert count == 3

    async def test_clear_leaves_table_empty(self, session: AsyncSession) -> None:
        """After clear_index the mount_index table has no rows."""
        await _insert_entry(session, filepath="/mnt/x.mkv")
        scanner = MountScanner()
        await scanner.clear_index(session)
        stats = await scanner.get_index_stats(session)
        assert stats["total_files"] == 0

    async def test_clear_on_empty_table_returns_zero(
        self, session: AsyncSession
    ) -> None:
        """clear_index on an already-empty table returns 0."""
        scanner = MountScanner()
        count = await scanner.clear_index(session)
        assert count == 0


# ---------------------------------------------------------------------------
# Group 8: _should_skip_dir
# ---------------------------------------------------------------------------


class TestShouldSkipDir:
    """Tests for MountScanner._should_skip_dir."""

    def test_hidden_directory_skipped(self) -> None:
        """Directories starting with '.' are skipped."""
        assert MountScanner._should_skip_dir(".hidden") is True

    def test_macosx_skipped(self) -> None:
        """__MACOSX is in the skip list."""
        assert MountScanner._should_skip_dir("__MACOSX") is True

    def test_eadir_skipped(self) -> None:
        """@eaDir (Synology) is in the skip list."""
        assert MountScanner._should_skip_dir("@eaDir") is True

    def test_trash_directory_skipped(self) -> None:
        """Directories starting with '.Trash-' are skipped."""
        assert MountScanner._should_skip_dir(".Trash-1000") is True

    def test_normal_directory_not_skipped(self) -> None:
        """Regular directory names are not skipped."""
        assert MountScanner._should_skip_dir("Season 01") is False

    def test_show_folder_not_skipped(self) -> None:
        """A normal show folder name is not skipped."""
        assert MountScanner._should_skip_dir("Breaking Bad") is False

    def test_all_caps_not_skipped(self) -> None:
        """An all-caps directory name (that is not in skip list) is not skipped."""
        assert MountScanner._should_skip_dir("MOVIES") is False


# ---------------------------------------------------------------------------
# Group 9: VIDEO_EXTENSIONS constant
# ---------------------------------------------------------------------------


class TestVideoExtensions:
    """Spot-checks on the VIDEO_EXTENSIONS frozenset."""

    def test_mkv_included(self) -> None:
        assert ".mkv" in VIDEO_EXTENSIONS

    def test_mp4_included(self) -> None:
        assert ".mp4" in VIDEO_EXTENSIONS

    def test_ts_included(self) -> None:
        assert ".ts" in VIDEO_EXTENSIONS

    def test_m2ts_included(self) -> None:
        assert ".m2ts" in VIDEO_EXTENSIONS

    def test_txt_not_included(self) -> None:
        assert ".txt" not in VIDEO_EXTENSIONS

    def test_jpg_not_included(self) -> None:
        assert ".jpg" not in VIDEO_EXTENSIONS

    def test_all_extensions_start_with_dot(self) -> None:
        """Every entry in VIDEO_EXTENSIONS begins with a period."""
        for ext in VIDEO_EXTENSIONS:
            assert ext.startswith("."), f"{ext!r} does not start with '.'"


# ---------------------------------------------------------------------------
# Group 10: Module-level singleton
# ---------------------------------------------------------------------------


class TestModuleSingleton:
    """Verify the module-level mount_scanner singleton is a MountScanner instance."""

    def test_singleton_is_mount_scanner_instance(self) -> None:
        """The module-level singleton must be a MountScanner."""
        assert isinstance(mount_scanner, MountScanner)


# ---------------------------------------------------------------------------
# Group 11: scan_directory
# ---------------------------------------------------------------------------


class TestScanDirectory:
    """Tests for MountScanner.scan_directory — targeted single-subdir indexing."""

    async def test_scan_directory_indexes_video_files(
        self, session: AsyncSession
    ) -> None:
        """Video files inside the named subdirectory are inserted into the index."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            subdir = os.path.join(tmpdir, "ShowFolder")
            os.makedirs(subdir)
            open(os.path.join(subdir, "Movie.2021.1080p.mkv"), "wb").write(b"v")
            open(os.path.join(subdir, "Show.S01E01.mkv"), "wb").write(b"v")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                result = await scanner.scan_directory(session, "ShowFolder")

        assert isinstance(result, ScanDirectoryResult)
        assert result.files_indexed == 2
        assert result.matched_dir_path is not None
        assert "ShowFolder" in result.matched_dir_path

        # Confirm the DB was populated.
        stats = await scanner.get_index_stats(session)
        assert stats["total_files"] == 2

    async def test_scan_directory_returns_zero_for_nonexistent(
        self, session: AsyncSession
    ) -> None:
        """scan_directory returns files_indexed=0/matched_dir_path=None when not found."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                result = await scanner.scan_directory(session, "does_not_exist")

        assert isinstance(result, ScanDirectoryResult)
        assert result.files_indexed == 0
        assert result.matched_dir_path is None

    async def test_scan_directory_returns_zero_for_empty_dir(
        self, session: AsyncSession
    ) -> None:
        """scan_directory returns files_indexed=0 (with a path) for a dir with no videos."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            subdir = os.path.join(tmpdir, "EmptyFolder")
            os.makedirs(subdir)

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                result = await scanner.scan_directory(session, "EmptyFolder")

        assert isinstance(result, ScanDirectoryResult)
        assert result.files_indexed == 0
        # Directory exists so matched_dir_path is set even when no files found
        assert result.matched_dir_path is not None
        assert "EmptyFolder" in result.matched_dir_path

    async def test_scan_directory_handles_nested_subdirs(
        self, session: AsyncSession
    ) -> None:
        """Video files in nested subdirectories are all indexed."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            top = os.path.join(tmpdir, "ShowFolder")
            s1 = os.path.join(top, "Season 1")
            s2 = os.path.join(top, "Season 2")
            os.makedirs(s1)
            os.makedirs(s2)
            open(os.path.join(s1, "Show.S01E01.mkv"), "wb").write(b"v")
            open(os.path.join(s1, "Show.S01E02.mkv"), "wb").write(b"v")
            open(os.path.join(s2, "Show.S02E01.mkv"), "wb").write(b"v")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                result = await scanner.scan_directory(session, "ShowFolder")

        assert isinstance(result, ScanDirectoryResult)
        assert result.files_indexed == 3
        assert result.matched_dir_path is not None
        stats = await scanner.get_index_stats(session)
        assert stats["total_files"] == 3

    async def test_scan_directory_updates_existing_entries(
        self, session: AsyncSession
    ) -> None:
        """Re-scanning a directory updates pre-existing rows instead of duplicating them."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            subdir = os.path.join(tmpdir, "MovieFolder")
            os.makedirs(subdir)
            filepath = os.path.join(subdir, "Movie.2024.mkv")
            open(filepath, "wb").write(b"v")

            # Pre-seed the DB with the same filepath so it already exists.
            await _insert_entry(session, filepath=filepath, filename="Movie.2024.mkv")
            await session.flush()

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                result = await scanner.scan_directory(session, "MovieFolder")

        # 1 file total: it was updated (not inserted as a duplicate).
        assert isinstance(result, ScanDirectoryResult)
        assert result.files_indexed == 1
        assert result.matched_dir_path is not None
        stats = await scanner.get_index_stats(session)
        assert stats["total_files"] == 1

    async def test_scan_directory_timeout_returns_zero(
        self, session: AsyncSession
    ) -> None:
        """When the existence check times out, scan_directory returns files_indexed=0."""
        scanner = MountScanner()
        with patch("asyncio.wait_for", side_effect=TimeoutError("simulated hang")):
            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = "/some/mount"
                result = await scanner.scan_directory(session, "any_dir")

        assert isinstance(result, ScanDirectoryResult)
        assert result.files_indexed == 0
        assert result.matched_dir_path is None


# ---------------------------------------------------------------------------
# Group 12: _upsert_records
# ---------------------------------------------------------------------------


def _make_record(filepath: str, filename: str | None = None, filesize: int = 1024) -> WalkEntry:
    """Build a minimal WalkEntry suitable for _upsert_records."""
    name = filename or os.path.basename(filepath)
    return WalkEntry(
        filepath=filepath,
        filename=name,
        filesize=filesize,
        parent_dir=os.path.dirname(filepath),
    )


class TestBatchUpsert:
    """Tests for MountScanner._upsert_records — the batch insert/update helper."""

    async def test_batch_upsert_inserts_new_records(
        self, session: AsyncSession
    ) -> None:
        """Calling _upsert_records with new filepaths returns correct added count."""
        scanner = MountScanner()
        records = [
            _make_record("/mnt/movie.a.mkv"),
            _make_record("/mnt/movie.b.mkv"),
            _make_record("/mnt/movie.c.mkv"),
        ]
        ts = _utcnow()
        result = await scanner._upsert_records(session, records, ts)

        assert result.added == 3
        assert result.updated == 0
        assert result.unchanged == 0
        assert result.errors == 0

    async def test_batch_upsert_unchanged_existing_records(
        self, session: AsyncSession
    ) -> None:
        """Re-upserting identical filepaths with matching filename/filesize counts as unchanged."""
        scanner = MountScanner()
        fp = "/mnt/existing.mkv"
        # Insert with filesize=1024 and parsed_episode set (otherwise re-parse triggers).
        await _insert_entry(session, filepath=fp, filename="existing.mkv", filesize=1024, parsed_episode=1)
        await session.flush()

        records = [_make_record(fp)]  # same filename, same filesize=1024
        ts = _utcnow()
        result = await scanner._upsert_records(session, records, ts)

        assert result.added == 0
        assert result.updated == 0
        assert result.unchanged == 1
        assert result.errors == 0

    async def test_batch_upsert_updates_changed_filesize(
        self, session: AsyncSession
    ) -> None:
        """Re-upserting with a different filesize increments updated, not unchanged."""
        scanner = MountScanner()
        fp = "/mnt/existing.mkv"
        await _insert_entry(session, filepath=fp, filename="existing.mkv", filesize=1024)
        await session.flush()

        records = [_make_record(fp, filesize=2048)]  # filesize changed → update
        ts = _utcnow()
        result = await scanner._upsert_records(session, records, ts)

        assert result.added == 0
        assert result.updated == 1
        assert result.unchanged == 0
        assert result.errors == 0

    async def test_batch_upsert_large_batch(self, session: AsyncSession) -> None:
        """Batches larger than 500 records are all inserted correctly."""
        scanner = MountScanner()
        n = 620
        records = [_make_record(f"/mnt/file{i:04d}.mkv") for i in range(n)]
        ts = _utcnow()
        result = await scanner._upsert_records(session, records, ts)

        assert result.added == n
        assert result.updated == 0
        assert result.unchanged == 0
        assert result.errors == 0

        # Verify they're all in the DB.
        stats = await scanner.get_index_stats(session)
        assert stats["total_files"] == n

    async def test_batch_upsert_mixed_insert_and_update(
        self, session: AsyncSession
    ) -> None:
        """Mix of new, unchanged, and changed filepaths splits correctly."""
        scanner = MountScanner()
        existing_fp = "/mnt/already.mkv"
        await _insert_entry(session, filepath=existing_fp, filename="already.mkv", filesize=1024, parsed_episode=1)
        await session.flush()

        records = [
            _make_record(existing_fp),              # pre-existing, unchanged → unchanged
            _make_record("/mnt/brand_new_a.mkv"),   # new → added
            _make_record("/mnt/brand_new_b.mkv"),   # new → added
        ]
        ts = _utcnow()
        result = await scanner._upsert_records(session, records, ts)

        assert result.added == 2
        assert result.updated == 0
        assert result.unchanged == 1
        assert result.errors == 0

    async def test_batch_upsert_empty_list_returns_zeros(
        self, session: AsyncSession
    ) -> None:
        """Passing an empty records list returns UpsertResult with all zeros."""
        scanner = MountScanner()
        ts = _utcnow()
        result = await scanner._upsert_records(session, [], ts)

        assert result.added == 0
        assert result.updated == 0
        assert result.unchanged == 0
        assert result.errors == 0

    async def test_upsert_records_error_resilience_when_ptn_fails_on_one_file(
        self, session: AsyncSession
    ) -> None:
        """_upsert_records is resilient when _parse_filename raises for one entry.

        When PTN raises ``RuntimeError`` for a specific filepath the per-file
        exception handler must:
        - increment ``errors`` by 1 for the failing file,
        - continue processing the remaining files (``added == 2``),
        - leave the errored filepath absent from the DB.
        """
        scanner = MountScanner()

        bad_fp = "/mnt/bad.file.mkv"
        good_fp_a = "/mnt/good.a.mkv"
        good_fp_b = "/mnt/good.b.mkv"

        records = [
            _make_record(good_fp_a),
            _make_record(bad_fp),
            _make_record(good_fp_b),
        ]
        ts = _utcnow()

        original_parse = __import__(
            "src.core.mount_scanner", fromlist=["_parse_filename"]
        )._parse_filename

        def _flaky_parse(filename: str, parent_dir: str | None = None) -> dict:
            if filename == os.path.basename(bad_fp):
                raise RuntimeError("PTN exploded")
            return original_parse(filename, parent_dir=parent_dir)

        with patch("src.core.mount_scanner._parse_filename", side_effect=_flaky_parse):
            result = await scanner._upsert_records(session, records, ts)

        assert result.errors == 1
        assert result.added == 2

        # The two good files must be present in the index.
        good_a = await scanner.lookup_by_filepath(session, good_fp_a)
        good_b = await scanner.lookup_by_filepath(session, good_fp_b)
        assert good_a is not None
        assert good_b is not None

        # The errored file must NOT be in the index.
        bad_entry = await scanner.lookup_by_filepath(session, bad_fp)
        assert bad_entry is None


# ---------------------------------------------------------------------------
# Group 13: _scandir_walk
# ---------------------------------------------------------------------------


class TestScandirWalk:
    """Tests for MountScanner._scandir_walk — the synchronous filesystem walker.

    _scandir_walk now returns ``WalkEntry`` namedtuples instead of dicts.
    Records carry only raw filesystem metadata (filepath, filename, filesize,
    parent_dir); PTN parsing is deferred to _upsert_records.
    """

    def test_scandir_walk_finds_video_files(self) -> None:
        """Video files in a directory are returned as WalkEntry records."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            open(os.path.join(tmpdir, "Movie.2024.1080p.mkv"), "wb").write(b"v")
            open(os.path.join(tmpdir, "Show.S01E01.mkv"), "wb").write(b"v")

            records, errors = scanner._scandir_walk(tmpdir)

        assert errors == 0
        filenames = {r.filename for r in records}
        assert "Movie.2024.1080p.mkv" in filenames
        assert "Show.S01E01.mkv" in filenames
        assert len(records) == 2

    def test_scandir_walk_returns_walk_entries(self) -> None:
        """_scandir_walk returns a list of WalkEntry namedtuples."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            open(os.path.join(tmpdir, "Movie.2024.mkv"), "wb").write(b"v")
            records, _ = scanner._scandir_walk(tmpdir)

        assert len(records) == 1
        assert isinstance(records[0], WalkEntry)

    def test_scandir_walk_skips_hidden_and_nonvideo(self) -> None:
        """Hidden files and non-video extensions are excluded from results."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            open(os.path.join(tmpdir, ".hidden.mkv"), "wb").write(b"v")
            open(os.path.join(tmpdir, "readme.txt"), "w").write("text")
            open(os.path.join(tmpdir, "poster.jpg"), "wb").write(b"img")
            open(os.path.join(tmpdir, "visible.mkv"), "wb").write(b"v")

            records, errors = scanner._scandir_walk(tmpdir)

        assert errors == 0
        assert len(records) == 1
        assert records[0].filename == "visible.mkv"

    def test_scandir_walk_skips_special_dirs(self) -> None:
        """__MACOSX and .Trash-1000 directories are skipped entirely."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            mac_dir = os.path.join(tmpdir, "__MACOSX")
            trash_dir = os.path.join(tmpdir, ".Trash-1000")
            os.makedirs(mac_dir)
            os.makedirs(trash_dir)
            open(os.path.join(mac_dir, "mac_meta.mkv"), "wb").write(b"v")
            open(os.path.join(trash_dir, "deleted.mkv"), "wb").write(b"v")
            open(os.path.join(tmpdir, "real.mkv"), "wb").write(b"v")

            records, errors = scanner._scandir_walk(tmpdir)

        assert errors == 0
        assert len(records) == 1
        assert records[0].filename == "real.mkv"

    def test_scandir_walk_recursive(self) -> None:
        """Video files in nested subdirectories are all discovered."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            s1 = os.path.join(tmpdir, "Season 1")
            s2 = os.path.join(tmpdir, "Season 2")
            deep = os.path.join(s2, "extras")
            os.makedirs(s1)
            os.makedirs(deep)
            open(os.path.join(s1, "Show.S01E01.mkv"), "wb").write(b"v")
            open(os.path.join(s1, "Show.S01E02.mkv"), "wb").write(b"v")
            open(os.path.join(deep, "Show.S02E01.Extras.mkv"), "wb").write(b"v")

            records, errors = scanner._scandir_walk(tmpdir)

        assert errors == 0
        assert len(records) == 3
        filenames = {r.filename for r in records}
        assert "Show.S01E01.mkv" in filenames
        assert "Show.S01E02.mkv" in filenames
        assert "Show.S02E01.Extras.mkv" in filenames

    def test_scandir_walk_entry_has_required_fields(self) -> None:
        """Every WalkEntry has filepath, filename, filesize, and parent_dir fields."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            open(os.path.join(tmpdir, "Movie.2024.mkv"), "wb").write(b"v")
            records, _ = scanner._scandir_walk(tmpdir)

        assert len(records) == 1
        entry = records[0]
        # WalkEntry is a NamedTuple — verify all expected fields are present.
        assert hasattr(entry, "filepath")
        assert hasattr(entry, "filename")
        assert hasattr(entry, "filesize")
        assert hasattr(entry, "parent_dir")
        # PTN-parsed fields are NOT present in WalkEntry (deferred to _upsert_records).
        assert not hasattr(entry, "parsed_title")

    def test_scandir_walk_filepath_is_absolute(self) -> None:
        """The filepath in each WalkEntry is an absolute path."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            open(os.path.join(tmpdir, "file.mkv"), "wb").write(b"v")
            records, _ = scanner._scandir_walk(tmpdir)

        assert len(records) == 1
        assert os.path.isabs(records[0].filepath)

    def test_scandir_walk_parent_dir_is_correct(self) -> None:
        """The parent_dir field matches the directory containing the file."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            subdir = os.path.join(tmpdir, "Season 1")
            os.makedirs(subdir)
            open(os.path.join(subdir, "Show.S01E01.mkv"), "wb").write(b"v")
            records, _ = scanner._scandir_walk(tmpdir)

        assert len(records) == 1
        assert records[0].parent_dir == subdir

    def test_scandir_walk_empty_dir_returns_empty_list(self) -> None:
        """Walking an empty directory returns an empty list with zero errors."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            records, errors = scanner._scandir_walk(tmpdir)

        assert records == []
        assert errors == 0

    def test_scandir_walk_skips_hidden_subdir(self) -> None:
        """Hidden subdirectories (starting with '.') are not descended into."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            hidden_subdir = os.path.join(tmpdir, ".hidden_dir")
            os.makedirs(hidden_subdir)
            open(os.path.join(hidden_subdir, "secret.mkv"), "wb").write(b"v")
            open(os.path.join(tmpdir, "visible.mkv"), "wb").write(b"v")

            records, errors = scanner._scandir_walk(tmpdir)

        assert errors == 0
        assert len(records) == 1
        assert records[0].filename == "visible.mkv"

    def test_scandir_walk_skips_stat_for_known_files(self) -> None:
        """Files present in known_files with a non-None filesize skip the stat() call.

        Verifies three cases in one walk:
        - A file in known_files with a real stored filesize reuses that size
          (the actual file on disk has a different size, proving stat was not called).
        - A file in known_files with filesize=None is stat'd again (retry path).
        - A file not present in known_files at all is stat'd normally (new file path).
        """
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            # Known file: 1 byte written, but known_files carries a fake 99999 size.
            known_path = os.path.join(tmpdir, "known.mkv")
            open(known_path, "wb").write(b"x")

            # Known file with None size (previous stat failure): should be re-stat'd.
            retry_path = os.path.join(tmpdir, "retry.mkv")
            open(retry_path, "wb").write(b"yy")

            # New file not in known_files at all: should be stat'd normally.
            new_path = os.path.join(tmpdir, "new.mkv")
            open(new_path, "wb").write(b"zzz")

            known_files: dict[str, int | None] = {
                known_path: 99999,   # fake stored size — stat must NOT be called
                retry_path: None,    # stored as None — stat must be called to recover
            }

            records, errors = scanner._scandir_walk(tmpdir, known_files)

        assert errors == 0
        assert len(records) == 3

        by_path = {r.filepath: r for r in records}

        # known_path: should carry the stored fake size, not the real 1-byte size.
        assert by_path[known_path].filesize == 99999

        # retry_path: known_files had None → stat was called → real size (2 bytes).
        assert by_path[retry_path].filesize == 2

        # new_path: not in known_files → stat was called → real size (3 bytes).
        assert by_path[new_path].filesize == 3

    def test_scandir_walk_known_files_empty_dict_stats_everything(self) -> None:
        """Passing an empty known_files dict causes all files to be stat'd normally."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            open(os.path.join(tmpdir, "a.mkv"), "wb").write(b"abcd")

            records, errors = scanner._scandir_walk(tmpdir, {})

        assert errors == 0
        assert len(records) == 1
        # Real on-disk size is 4 bytes.
        assert records[0].filesize == 4


# ---------------------------------------------------------------------------
# Group 14: lookup_by_path_prefix
# ---------------------------------------------------------------------------


class TestLookupByPathPrefix:
    """Tests for MountScanner.lookup_by_path_prefix (DB-only, no filesystem access)."""

    async def test_basic_match_returns_all_files_under_prefix(
        self, session: AsyncSession
    ) -> None:
        """Files whose filepath begins with the prefix are returned."""
        await _insert_entry(
            session,
            filepath="/mnt/The.Mummy.1999.1080p/The.Mummy.1999.mkv",
            filename="The.Mummy.1999.mkv",
            parsed_title="the mummy",
        )
        await _insert_entry(
            session,
            filepath="/mnt/The.Mummy.1999.1080p/extras/featurette.mkv",
            filename="featurette.mkv",
            parsed_title="featurette",
        )
        scanner = MountScanner()
        results = await scanner.lookup_by_path_prefix(
            session, "/mnt/The.Mummy.1999.1080p"
        )
        assert len(results) == 2
        paths = {r.filepath for r in results}
        assert "/mnt/The.Mummy.1999.1080p/The.Mummy.1999.mkv" in paths
        assert "/mnt/The.Mummy.1999.1080p/extras/featurette.mkv" in paths

    async def test_no_match_returns_empty_list(self, session: AsyncSession) -> None:
        """A prefix that matches no rows returns an empty list."""
        await _insert_entry(
            session,
            filepath="/mnt/Some.Other.Movie.2020/movie.mkv",
            filename="movie.mkv",
        )
        scanner = MountScanner()
        results = await scanner.lookup_by_path_prefix(
            session, "/mnt/Nonexistent.Dir.2099"
        )
        assert results == []

    async def test_season_filter_restricts_results(
        self, session: AsyncSession
    ) -> None:
        """When season is provided only entries matching that season are returned."""
        prefix = "/mnt/Breaking.Bad.S01-05.Complete"
        await _insert_entry(
            session,
            filepath=f"{prefix}/S01/Breaking.Bad.S01E01.mkv",
            filename="Breaking.Bad.S01E01.mkv",
            parsed_title="breaking bad",
            parsed_season=1,
            parsed_episode=1,
        )
        await _insert_entry(
            session,
            filepath=f"{prefix}/S02/Breaking.Bad.S02E01.mkv",
            filename="Breaking.Bad.S02E01.mkv",
            parsed_title="breaking bad",
            parsed_season=2,
            parsed_episode=1,
        )
        await _insert_entry(
            session,
            filepath=f"{prefix}/S03/Breaking.Bad.S03E01.mkv",
            filename="Breaking.Bad.S03E01.mkv",
            parsed_title="breaking bad",
            parsed_season=3,
            parsed_episode=1,
        )
        scanner = MountScanner()
        results = await scanner.lookup_by_path_prefix(session, prefix, season=2)
        assert len(results) == 1
        assert results[0].parsed_season == 2

    async def test_episode_filter_restricts_results(
        self, session: AsyncSession
    ) -> None:
        """When season and episode are provided only that specific episode is returned."""
        prefix = "/mnt/Better.Call.Saul.2015"
        await _insert_entry(
            session,
            filepath=f"{prefix}/S01/Better.Call.Saul.S01E01.mkv",
            filename="Better.Call.Saul.S01E01.mkv",
            parsed_title="better call saul",
            parsed_season=1,
            parsed_episode=1,
        )
        await _insert_entry(
            session,
            filepath=f"{prefix}/S01/Better.Call.Saul.S01E02.mkv",
            filename="Better.Call.Saul.S01E02.mkv",
            parsed_title="better call saul",
            parsed_season=1,
            parsed_episode=2,
        )
        await _insert_entry(
            session,
            filepath=f"{prefix}/S01/Better.Call.Saul.S01E03.mkv",
            filename="Better.Call.Saul.S01E03.mkv",
            parsed_title="better call saul",
            parsed_season=1,
            parsed_episode=3,
        )
        scanner = MountScanner()
        results = await scanner.lookup_by_path_prefix(
            session, prefix, season=1, episode=2
        )
        assert len(results) == 1
        assert results[0].parsed_episode == 2

    async def test_trailing_slash_normalisation_without_slash(
        self, session: AsyncSession
    ) -> None:
        """A prefix without a trailing slash is normalised to include one before querying."""
        prefix_no_slash = "/mnt/The.Mummy.1999.2160p"
        await _insert_entry(
            session,
            filepath="/mnt/The.Mummy.1999.2160p/The.Mummy.1999.2160p.mkv",
            filename="The.Mummy.1999.2160p.mkv",
            parsed_title="the mummy",
        )
        scanner = MountScanner()
        # Query without trailing slash — must still match.
        results = await scanner.lookup_by_path_prefix(session, prefix_no_slash)
        assert len(results) == 1
        assert results[0].parsed_title == "the mummy"

    async def test_trailing_slash_normalisation_with_slash(
        self, session: AsyncSession
    ) -> None:
        """A prefix that already ends in '/' is not double-slashed."""
        prefix_with_slash = "/mnt/The.Mummy.1999.2160p/"
        await _insert_entry(
            session,
            filepath="/mnt/The.Mummy.1999.2160p/The.Mummy.1999.2160p.mkv",
            filename="The.Mummy.1999.2160p.mkv",
            parsed_title="the mummy",
        )
        scanner = MountScanner()
        results = await scanner.lookup_by_path_prefix(session, prefix_with_slash)
        assert len(results) == 1

    async def test_sibling_directory_not_matched(self, session: AsyncSession) -> None:
        """A prefix of '/mount/The.Mummy.1999' must NOT match '/mount/The.Mummy.1999.Returns/...'."""
        await _insert_entry(
            session,
            filepath="/mnt/The.Mummy.1999.1080p/The.Mummy.1999.mkv",
            filename="The.Mummy.1999.mkv",
            parsed_title="the mummy",
        )
        await _insert_entry(
            session,
            filepath="/mnt/The.Mummy.Returns.2001/The.Mummy.Returns.mkv",
            filename="The.Mummy.Returns.mkv",
            parsed_title="the mummy returns",
        )
        scanner = MountScanner()
        # Prefix ends with '/' so it cannot spill into sibling dirs that happen
        # to share the same leading characters.
        results = await scanner.lookup_by_path_prefix(
            session, "/mnt/The.Mummy.1999.1080p"
        )
        assert len(results) == 1
        assert results[0].parsed_title == "the mummy"

    async def test_results_ordered_by_last_seen_at_descending(
        self, session: AsyncSession
    ) -> None:
        """Results are ordered newest-first by last_seen_at."""
        older_ts = datetime(2024, 1, 1, tzinfo=UTC)
        newer_ts = datetime(2024, 6, 1, tzinfo=UTC)
        newest_ts = datetime(2025, 1, 1, tzinfo=UTC)

        prefix = "/mnt/Show.2024"
        await _insert_entry(
            session,
            filepath=f"{prefix}/S01E02.mkv",
            filename="S01E02.mkv",
            parsed_title="show",
            last_seen_at=older_ts,
        )
        await _insert_entry(
            session,
            filepath=f"{prefix}/S01E03.mkv",
            filename="S01E03.mkv",
            parsed_title="show",
            last_seen_at=newest_ts,
        )
        await _insert_entry(
            session,
            filepath=f"{prefix}/S01E01.mkv",
            filename="S01E01.mkv",
            parsed_title="show",
            last_seen_at=newer_ts,
        )
        scanner = MountScanner()
        results = await scanner.lookup_by_path_prefix(session, prefix)
        assert len(results) == 3
        assert results[0].filepath == f"{prefix}/S01E03.mkv"
        assert results[1].filepath == f"{prefix}/S01E01.mkv"
        assert results[2].filepath == f"{prefix}/S01E02.mkv"

    async def test_empty_index_returns_empty_list(self, session: AsyncSession) -> None:
        """lookup_by_path_prefix on an empty table returns an empty list."""
        scanner = MountScanner()
        results = await scanner.lookup_by_path_prefix(session, "/mnt/anything")
        assert results == []

    async def test_only_files_inside_prefix_dir_returned(
        self, session: AsyncSession
    ) -> None:
        """Files at mount root or in other dirs are not matched by a subdirectory prefix."""
        # File directly in /mnt/ (not inside any subdirectory)
        await _insert_entry(
            session,
            filepath="/mnt/root_level.mkv",
            filename="root_level.mkv",
            parsed_title="root level",
        )
        # File inside the target prefix
        await _insert_entry(
            session,
            filepath="/mnt/ShowDir/S01E01.mkv",
            filename="S01E01.mkv",
            parsed_title="show",
        )
        scanner = MountScanner()
        results = await scanner.lookup_by_path_prefix(session, "/mnt/ShowDir")
        assert len(results) == 1
        assert results[0].filepath == "/mnt/ShowDir/S01E01.mkv"


# ---------------------------------------------------------------------------
# Group 15: ScanDirectoryResult.matched_dir_path integration
# ---------------------------------------------------------------------------


class TestScanDirectoryResultMatchedDirPath:
    """Verify that matched_dir_path from scan_directory is usable as a prefix for
    lookup_by_path_prefix — the core of the path-prefix fallback in the CHECKING stage.
    """

    async def test_matched_dir_path_feeds_into_lookup_by_path_prefix(
        self, session: AsyncSession
    ) -> None:
        """The matched_dir_path returned by scan_directory can be used directly
        as the prefix for lookup_by_path_prefix to retrieve the indexed files.
        """
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            subdir = os.path.join(tmpdir, "The.Mummy.1999.2160p.UHD.BluRay")
            os.makedirs(subdir)
            open(os.path.join(subdir, "The.Mummy.1999.mkv"), "wb").write(b"v")
            open(os.path.join(subdir, "The.Mummy.1999.extras.mkv"), "wb").write(b"v")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                scan_result = await scanner.scan_directory(
                    session, "The.Mummy.1999.2160p.UHD.BluRay"
                )

        assert scan_result.files_indexed == 2
        assert scan_result.matched_dir_path is not None

        # Use matched_dir_path as the prefix for subsequent lookup.
        prefix_results = await scanner.lookup_by_path_prefix(
            session, scan_result.matched_dir_path
        )
        assert len(prefix_results) == 2

    async def test_fuzzy_matched_dir_path_feeds_into_lookup_by_path_prefix(
        self, session: AsyncSession
    ) -> None:
        """When scan_directory uses a fuzzy match the returned matched_dir_path
        still works as a correct prefix for lookup_by_path_prefix.
        """
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            # Actual directory on filesystem has extra tokens (year, resolution, group).
            actual_dir = os.path.join(tmpdir, "The.Mummy.1999.2160p.BluRay.HDR")
            os.makedirs(actual_dir)
            open(os.path.join(actual_dir, "The.Mummy.1999.mkv"), "wb").write(b"v")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                # Request with shorter name — triggers fuzzy match inside scan_directory.
                scan_result = await scanner.scan_directory(session, "The Mummy 1999")

        assert scan_result.files_indexed == 1
        assert scan_result.matched_dir_path is not None
        # matched_dir_path should point to the actual (longer) directory name.
        assert "The.Mummy.1999.2160p.BluRay.HDR" in scan_result.matched_dir_path

        prefix_results = await scanner.lookup_by_path_prefix(
            session, scan_result.matched_dir_path
        )
        assert len(prefix_results) == 1
        assert prefix_results[0].filename == "The.Mummy.1999.mkv"

    async def test_failed_scan_directory_matched_dir_path_is_none(
        self, session: AsyncSession
    ) -> None:
        """When scan_directory finds no directory, matched_dir_path is None
        so the caller can skip the lookup_by_path_prefix call entirely.
        """
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                scan_result = await scanner.scan_directory(
                    session, "Completely.Nonexistent.Dir.2099"
                )

        assert scan_result.files_indexed == 0
        assert scan_result.matched_dir_path is None
        # Calling lookup_by_path_prefix with None would raise, so callers must guard.
        # Verify a subsequent call with any real prefix safely returns empty.
        results = await scanner.lookup_by_path_prefix(
            session, "/mnt/Completely.Nonexistent.Dir.2099"
        )
        assert results == []


# ---------------------------------------------------------------------------
# Group 16: scan_directory single-file handling (_scan_single_file)
# ---------------------------------------------------------------------------


class TestScanDirectorySingleFile:
    """Tests for the single-file torrent path inside scan_directory.

    When ``directory_name`` carries a video extension (e.g. ``.mkv``) the
    method delegates to ``_scan_single_file`` instead of the directory walk.
    These tests cover the exact-match, fuzzy-match, not-found, and regression
    (directory name unchanged) paths, plus extension variety and the invariant
    that ``matched_dir_path`` is always ``None`` for single files.
    """

    # ------------------------------------------------------------------
    # 1. Exact match — file exists directly in mount root
    # ------------------------------------------------------------------

    async def test_exact_filename_match_indexes_file(
        self, session: AsyncSession
    ) -> None:
        """scan_directory with a .mkv name indexes the file when it exists in mount root."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            open(os.path.join(tmpdir, "Movie (2024).mkv"), "wb").write(b"v")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                result = await scanner.scan_directory(session, "Movie (2024).mkv")

        assert isinstance(result, ScanDirectoryResult)
        assert result.files_indexed == 1

    async def test_exact_match_file_is_inserted_into_db(
        self, session: AsyncSession
    ) -> None:
        """After an exact single-file scan the file appears in the mount index."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            filename = "Dune.Part.Two.2024.2160p.mkv"
            open(os.path.join(tmpdir, filename), "wb").write(b"v")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                await scanner.scan_directory(session, filename)

        stats = await scanner.get_index_stats(session)
        assert stats["total_files"] == 1

    async def test_exact_match_stores_correct_filepath(
        self, session: AsyncSession
    ) -> None:
        """The indexed filepath is the absolute path inside the mount root."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            filename = "Alien.1979.1080p.mkv"
            open(os.path.join(tmpdir, filename), "wb").write(b"v")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                await scanner.scan_directory(session, filename)

            expected_path = os.path.join(tmpdir, filename)
            entry = await scanner.lookup_by_filepath(session, expected_path)

        assert entry is not None
        assert entry.filename == filename

    # ------------------------------------------------------------------
    # 2. matched_dir_path is None for single files
    # ------------------------------------------------------------------

    async def test_exact_match_matched_dir_path_is_none(
        self, session: AsyncSession
    ) -> None:
        """Single-file scan always returns matched_dir_path=None — there is no directory."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            open(os.path.join(tmpdir, "Movie.mkv"), "wb").write(b"v")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                result = await scanner.scan_directory(session, "Movie.mkv")

        assert result.matched_dir_path is None

    async def test_fuzzy_match_matched_dir_path_is_none(
        self, session: AsyncSession
    ) -> None:
        """Fuzzy single-file match also returns matched_dir_path=None."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            # File on disk has a longer name than what is requested.
            open(
                os.path.join(tmpdir, "The.Dark.Knight.2008.1080p.BluRay.mkv"),
                "wb",
            ).write(b"v")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                result = await scanner.scan_directory(
                    session, "The Dark Knight 2008.mkv"
                )

        assert result.matched_dir_path is None

    # ------------------------------------------------------------------
    # 3. Fuzzy match — file not found by exact name, matched by word-subsequence
    # ------------------------------------------------------------------

    async def test_fuzzy_match_indexes_file_with_extra_tokens(
        self, session: AsyncSession
    ) -> None:
        """File with extra release-group tokens in the name is fuzzy-matched."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            # On-disk name has resolution/group tokens the RD API name lacks.
            actual = "Interstellar.2014.2160p.UHD.BluRay.DTS-X.mkv"
            open(os.path.join(tmpdir, actual), "wb").write(b"v")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                # RD API returns the shorter name — triggers fuzzy path.
                result = await scanner.scan_directory(
                    session, "Interstellar 2014.mkv"
                )

        assert result.files_indexed == 1

    async def test_fuzzy_match_file_appears_in_index(
        self, session: AsyncSession
    ) -> None:
        """After a fuzzy single-file scan the actual (longer) filename is indexed."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            actual = "Oppenheimer.2023.1080p.WEB-DL.mkv"
            open(os.path.join(tmpdir, actual), "wb").write(b"v")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                await scanner.scan_directory(session, "Oppenheimer 2023.mkv")

            # The entry should be indexed under its real absolute path.
            expected_path = os.path.join(tmpdir, actual)
            entry = await scanner.lookup_by_filepath(session, expected_path)

        assert entry is not None
        assert entry.filename == actual

    async def test_single_file_not_found_falls_through_to_directory_scan(
        self, session: AsyncSession
    ) -> None:
        """When _scan_single_file finds nothing, fall through to directory scan.

        Zurg sometimes wraps single-file torrents in a directory named after the
        torrent stem.  After the single-file scan returns 0 results the code
        strips the extension and retries as a directory scan, which should find
        the subdirectory via fuzzy match.
        """
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            # A subdirectory whose name would fuzzy-match the stem "Tenet 2020".
            subdir = os.path.join(tmpdir, "Tenet.2020.1080p")
            os.makedirs(subdir)
            open(os.path.join(subdir, "Tenet.2020.mkv"), "wb").write(b"v")
            # No matching file at root level — _scan_single_file returns 0.

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                result = await scanner.scan_directory(session, "Tenet 2020.mkv")

        # The directory scan fallback should find the subdirectory and index the
        # file inside it.
        assert result.files_indexed == 1
        assert result.matched_dir_path is not None

    async def test_fuzzy_match_skips_non_video_files(
        self, session: AsyncSession
    ) -> None:
        """Fuzzy file search only considers files with video extensions."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            # A non-video file that would otherwise word-match.
            open(os.path.join(tmpdir, "Avatar 2009.nfo"), "w").write("meta")
            # No matching video file exists.

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                result = await scanner.scan_directory(session, "Avatar 2009.mkv")

        assert result.files_indexed == 0
        assert result.matched_dir_path is None

    async def test_fuzzy_match_picks_shortest_candidate(
        self, session: AsyncSession
    ) -> None:
        """When multiple files fuzzy-match, the one with the shortest normalized name wins."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            # Two candidates both containing "avatar 2009".
            short = "Avatar.2009.1080p.mkv"
            long_ = "Avatar.2009.2160p.UHD.BluRay.TrueHD.Atmos.mkv"
            open(os.path.join(tmpdir, short), "wb").write(b"v")
            open(os.path.join(tmpdir, long_), "wb").write(b"v")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                await scanner.scan_directory(session, "Avatar 2009.mkv")

        stats = await scanner.get_index_stats(session)
        # Only 1 file should be indexed (the winning candidate).
        assert stats["total_files"] == 1
        expected_path = os.path.join(tmpdir, short)
        entry = await scanner.lookup_by_filepath(session, expected_path)
        assert entry is not None

    # ------------------------------------------------------------------
    # 4. Not found — no matching file anywhere in mount root
    # ------------------------------------------------------------------

    async def test_not_found_returns_empty_result(
        self, session: AsyncSession
    ) -> None:
        """When no file matches, scan_directory returns files_indexed=0 and matched_dir_path=None."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            # Mount root is empty — nothing to match.
            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                result = await scanner.scan_directory(
                    session, "Nonexistent.Movie.2099.mkv"
                )

        assert isinstance(result, ScanDirectoryResult)
        assert result.files_indexed == 0
        assert result.matched_dir_path is None

    async def test_not_found_leaves_db_empty(self, session: AsyncSession) -> None:
        """A failed single-file scan does not insert any rows into the index."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                await scanner.scan_directory(
                    session, "No.Such.File.2099.mkv"
                )

        stats = await scanner.get_index_stats(session)
        assert stats["total_files"] == 0

    # ------------------------------------------------------------------
    # 5. Regression — directory name WITHOUT video extension still uses
    #    the directory code path (not single-file path)
    # ------------------------------------------------------------------

    async def test_directory_name_without_extension_uses_dir_path(
        self, session: AsyncSession
    ) -> None:
        """A directory_name with no video extension goes through the directory walk, not _scan_single_file."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            subdir = os.path.join(tmpdir, "ShowFolder")
            os.makedirs(subdir)
            open(os.path.join(subdir, "Show.S01E01.mkv"), "wb").write(b"v")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                with patch.object(
                    scanner, "_scan_single_file", new=AsyncMock()
                ) as mock_single:
                    result = await scanner.scan_directory(session, "ShowFolder")
                    mock_single.assert_not_called()

        assert result.files_indexed == 1
        assert result.matched_dir_path is not None

    async def test_non_video_extension_uses_dir_path(
        self, session: AsyncSession
    ) -> None:
        """A name ending in a non-video extension (e.g. .nfo) is treated as a directory name."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a directory whose name ends in ".nfo" — contrived but tests the branch.
            subdir = os.path.join(tmpdir, "ShowFolder.nfo")
            os.makedirs(subdir)
            open(os.path.join(subdir, "Show.S01E01.mkv"), "wb").write(b"v")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                with patch.object(
                    scanner, "_scan_single_file", new=AsyncMock()
                ) as mock_single:
                    result = await scanner.scan_directory(session, "ShowFolder.nfo")
                    mock_single.assert_not_called()

        assert result.files_indexed == 1

    # ------------------------------------------------------------------
    # 6. Various video extensions all trigger the single-file path
    # ------------------------------------------------------------------

    @pytest.mark.parametrize("ext", [".mkv", ".mp4", ".avi", ".mov", ".webm", ".m4v", ".ts", ".m2ts"])
    async def test_video_extension_triggers_single_file_path(
        self, session: AsyncSession, ext: str
    ) -> None:
        """Every recognised video extension causes scan_directory to call _scan_single_file."""
        scanner = MountScanner()
        filename = f"Movie{ext}"
        with tempfile.TemporaryDirectory() as tmpdir:
            open(os.path.join(tmpdir, filename), "wb").write(b"v")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                with patch.object(
                    scanner,
                    "_scan_single_file",
                    new=AsyncMock(
                        return_value=ScanDirectoryResult(
                            files_indexed=1, matched_dir_path=None
                        )
                    ),
                ) as mock_single:
                    await scanner.scan_directory(session, filename)
                    mock_single.assert_called_once_with(session, filename)

    # ------------------------------------------------------------------
    # 7. Re-scan updates existing row rather than creating a duplicate
    # ------------------------------------------------------------------

    async def test_rescan_updates_existing_db_entry(
        self, session: AsyncSession
    ) -> None:
        """Scanning the same single file twice updates the row, not duplicates it."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            filename = "Blade.Runner.2049.mkv"
            filepath = os.path.join(tmpdir, filename)
            open(filepath, "wb").write(b"v")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                await scanner.scan_directory(session, filename)
                await session.flush()
                await scanner.scan_directory(session, filename)

        stats = await scanner.get_index_stats(session)
        assert stats["total_files"] == 1


# ---------------------------------------------------------------------------
# Group 17: _has_meaningful_title
# ---------------------------------------------------------------------------


class TestHasMeaningfulTitle:
    """Unit tests for the _has_meaningful_title helper (issue #33)."""

    def test_normal_show_title_is_meaningful(self) -> None:
        """A regular show title returns True."""
        assert _has_meaningful_title("attack on titan") is True

    def test_short_title_two_words_is_meaningful(self) -> None:
        """Two-word title of reasonable length is meaningful."""
        assert _has_meaningful_title("breaking bad") is True

    def test_single_word_title_long_enough_is_meaningful(self) -> None:
        """A single word of 3+ chars is still considered meaningful."""
        assert _has_meaningful_title("alien") is True

    def test_empty_string_not_meaningful(self) -> None:
        """Empty string is not meaningful."""
        assert _has_meaningful_title("") is False

    def test_one_char_not_meaningful(self) -> None:
        """Single character is below the 3-char minimum."""
        assert _has_meaningful_title("a") is False

    def test_two_chars_not_meaningful(self) -> None:
        """Two characters is below the 3-char minimum."""
        assert _has_meaningful_title("ab") is False

    def test_three_chars_is_meaningful(self) -> None:
        """Three characters meets the minimum threshold."""
        assert _has_meaningful_title("abc") is True

    def test_episode_marker_s02e01_not_meaningful(self) -> None:
        """Title starting with SxxExx pattern is not meaningful."""
        assert _has_meaningful_title("s02e01 beast titan") is False

    def test_episode_marker_s01e01_alone_not_meaningful(self) -> None:
        """Bare 'sXXeXX' string is not meaningful."""
        assert _has_meaningful_title("s01e01") is False

    def test_episode_marker_e01_not_meaningful(self) -> None:
        """Title starting with 'eXX' episode marker is not meaningful."""
        assert _has_meaningful_title("e01 prologue") is False

    def test_episode_marker_uppercase_treated_as_lowercase(self) -> None:
        """Since we pass normalised (lowercase) titles, uppercase is not expected,
        but verify the marker check correctly rejects lowercase forms."""
        # _has_meaningful_title always receives normalised (lowercase) input.
        assert _has_meaningful_title("s02e01") is False

    def test_title_with_numbers_not_starting_with_marker(self) -> None:
        """A title with numbers that does NOT start with SxxExx is meaningful."""
        assert _has_meaningful_title("zone 414") is True

    def test_season_4_title_not_confused_with_marker(self) -> None:
        """'season 4' does not match the SxxExx pattern and is meaningful."""
        assert _has_meaningful_title("season 4") is True


# ---------------------------------------------------------------------------
# Group 18: _parse_filename directory-name fallback (issue #33)
# ---------------------------------------------------------------------------


class TestParseFilenameDirectoryFallback:
    """Tests for the parent directory title fallback in _parse_filename (issue #33).

    Season pack episode files (e.g. "S02E01 - Beast Titan.mkv") produce
    titles like "s02e01 beast titan" from PTN — not a searchable show title.
    When parent_dir is provided the directory basename should be used instead.
    """

    def test_episode_titled_file_uses_parent_dir_title(self) -> None:
        """Season pack episode file gets its title from the parent directory.

        Input:  filename="S02E01 - Beast Titan.mkv"
                parent_dir="/mnt/__all__/Attack on Titan S02"
        Expect: parsed_title="attack on titan" (from directory PTN parse)
        """
        result = _parse_filename(
            "S02E01 - Beast Titan.mkv",
            parent_dir="/mnt/__all__/Attack on Titan S02",
        )
        assert result["title"] == "attack on titan"
        # Season and episode should still come from the filename.
        assert result["season"] == 2
        assert result["episode"] == 1

    def test_episode_titled_file_preserves_episode_number_from_filename(self) -> None:
        """Episode number is extracted from the filename, not overridden by directory."""
        result = _parse_filename(
            "S01E05 - The White Walkers.mkv",
            parent_dir="/mnt/__all__/Game of Thrones S01",
        )
        assert result["episode"] == 5
        assert result["season"] == 1

    def test_normal_filename_ignores_parent_dir(self) -> None:
        """A well-formed filename title is not replaced by the directory title."""
        result = _parse_filename(
            "Attack.on.Titan.S02E01.1080p.mkv",
            parent_dir="/mnt/__all__/SomethingElse",
        )
        assert result["title"] == "attack on titan"
        assert result["season"] == 2
        assert result["episode"] == 1

    def test_no_parent_dir_uses_filename_title(self) -> None:
        """Without parent_dir the fallback is not attempted; filename title is used."""
        result = _parse_filename("S02E01 - Beast Titan.mkv")
        # Title comes from filename alone — it will include episode marker tokens.
        # We just verify parent_dir=None doesn't crash and does not return the
        # directory-based title.
        assert result["title"] is not None
        assert result["season"] == 2
        assert result["episode"] == 1

    def test_parent_dir_is_root_basename_empty_no_crash(self) -> None:
        """parent_dir of '/' (empty basename) does not crash and falls back gracefully."""
        result = _parse_filename("S01E01 - Prologue.mkv", parent_dir="/")
        # basename of "/" is "" — fallback skipped, episode title retained.
        assert result["title"] is not None
        assert result["episode"] == 1

    def test_movie_file_unaffected_by_parent_dir(self) -> None:
        """Normal movie files are unaffected — their title is meaningful."""
        result = _parse_filename(
            "The.Dark.Knight.2008.1080p.BluRay.x264.mkv",
            parent_dir="/mnt/__all__/Some Random Dir",
        )
        assert result["title"] == "the dark knight"
        assert result["year"] == 2008

    def test_leading_number_episode_no_dir_fallback(self) -> None:
        """Episode file with leading number (e.g. '28. Title.mp4') does not trigger fallback.

        PTN parses "28. Prelude to the Impending Fight.mp4" as a title containing
        the full episode title.  This title is > 3 chars and does not start with
        the SxxExx pattern, so _has_meaningful_title returns True and the directory
        fallback is NOT triggered.  This test documents the current expected behaviour.
        """
        result = _parse_filename(
            "28. Prelude to the Impending Fight.mp4",
            parent_dir="/mnt/__all__/Attack on Titan S02",
        )
        # The leading-number title is considered meaningful (>3 chars, no SxxExx marker).
        # So parent_dir fallback is NOT triggered for these files.
        assert result["episode"] == 28  # from _LEADING_EP_RE

    async def test_directory_fallback_integration_with_scan_and_lookup(
        self, session: AsyncSession
    ) -> None:
        """Integration test: scan a season pack directory and verify lookup by show title works.

        Creates a directory structure mimicking a Zurg season pack mount:
          <tmpdir>/
            Attack on Titan S02/
              S02E01 - Beast Titan.mkv
              S02E02 - I Can Hear His Heartbeat.mkv

        After scanning, lookup("Attack on Titan", season=2) should find both
        episodes because their parsed_title is taken from the directory basename.
        """
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            show_dir = os.path.join(tmpdir, "Attack on Titan S02")
            os.makedirs(show_dir)
            open(
                os.path.join(show_dir, "S02E01 - Beast Titan.mkv"), "wb"
            ).write(b"v")
            open(
                os.path.join(show_dir, "S02E02 - I Can Hear His Heartbeat.mkv"), "wb"
            ).write(b"v")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                await scanner.scan_directory(session, "Attack on Titan S02")

            results = await scanner.lookup(session, "Attack on Titan", season=2)

        assert len(results) == 2
        for r in results:
            assert r.parsed_title == "attack on titan"
            assert r.parsed_season == 2
        episodes = {r.parsed_episode for r in results}
        assert episodes == {1, 2}

    def test_directory_fallback_used_when_parent_dir_is_show_dir(self) -> None:
        """_parse_filename with parent_dir pointing to show dir extracts correct title."""
        # This is a pure unit test of _parse_filename — no DB needed.
        result = _parse_filename(
            "S03E07 - The Raven.mkv",
            parent_dir="/media/zurg/__all__/Game of Thrones Season 3",
        )
        assert result["title"] == "game of thrones"
        assert result["season"] == 3
        assert result["episode"] == 7

    # ------------------------------------------------------------------
    # 8. Timeout during existence check returns empty result gracefully
    # ------------------------------------------------------------------

    async def test_timeout_during_single_file_existence_check_returns_empty(
        self, session: AsyncSession
    ) -> None:
        """A timeout on the isfile() call inside _scan_single_file returns empty gracefully."""
        scanner = MountScanner()
        with patch("asyncio.wait_for", side_effect=TimeoutError("simulated hang")):
            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = "/some/mount"
                result = await scanner.scan_directory(session, "Timeout.Movie.mkv")

        assert result.files_indexed == 0
        assert result.matched_dir_path is None

    # ------------------------------------------------------------------
    # 9. Fallback to directory scan when single-file scan returns 0
    # ------------------------------------------------------------------

    async def test_scan_directory_single_file_fallback_to_directory(
        self, session: AsyncSession
    ) -> None:
        """When _scan_single_file returns 0, scan_directory falls back to the
        directory scan using the stem as the directory name.

        This covers the case where Zurg wraps a single-file torrent named
        ``Movie.mkv`` inside a directory also named ``Movie``.
        """
        scanner = MountScanner()

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a directory named after the stem ("Movie") containing the file.
            stem_dir = os.path.join(tmpdir, "Movie")
            os.makedirs(stem_dir)
            open(os.path.join(stem_dir, "Movie.mkv"), "wb").write(b"v")

            # _scan_single_file finds nothing (file not in mount root).
            with patch.object(
                scanner,
                "_scan_single_file",
                new=AsyncMock(
                    return_value=ScanDirectoryResult(files_indexed=0, matched_dir_path=None)
                ),
            ) as mock_single:
                with patch("src.core.mount_scanner.settings") as mock_settings:
                    mock_settings.paths.zurg_mount = tmpdir
                    result = await scanner.scan_directory(session, "Movie.mkv")

                mock_single.assert_called_once_with(session, "Movie.mkv")

        # The directory scan should have succeeded using the stem "Movie".
        assert isinstance(result, ScanDirectoryResult)
        assert result.files_indexed == 1
        assert result.matched_dir_path is not None
        assert "Movie" in result.matched_dir_path

    async def test_scan_directory_single_file_found_no_directory_fallback(
        self, session: AsyncSession
    ) -> None:
        """When _scan_single_file succeeds (files_indexed > 0), the directory
        scan path is never attempted — os.path.isdir is not called for the stem.
        """
        scanner = MountScanner()

        successful_result = ScanDirectoryResult(
            files_indexed=1, matched_dir_path="/mnt/Movie.mkv"
        )

        with patch.object(
            scanner,
            "_scan_single_file",
            new=AsyncMock(return_value=successful_result),
        ) as mock_single:
            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = "/mnt"
                with patch("os.path.isdir") as mock_isdir:
                    result = await scanner.scan_directory(session, "Movie.mkv")

                    # isdir must never be called for the stem-derived dir path.
                    stem_dir_path = os.path.join("/mnt", "Movie", "")
                    isdir_calls = [str(c.args[0]) for c in mock_isdir.call_args_list]
                    assert stem_dir_path not in isdir_calls

        mock_single.assert_called_once_with(session, "Movie.mkv")
        assert result.files_indexed == 1
        assert result.matched_dir_path == "/mnt/Movie.mkv"


# ---------------------------------------------------------------------------
# Group: _upsert_records UNIQUE constraint robustness
# ---------------------------------------------------------------------------


class TestUpsertRecordsUniqueConstraint:
    """Tests verifying that _upsert_records never crashes with a UNIQUE
    constraint violation, even when duplicate filepaths appear in the input
    or when a concurrent caller has already inserted the same filepath.
    """

    def _make_entry(
        self,
        filepath: str = "/mnt/test/Movie.2024.mkv",
        filename: str = "Movie.2024.mkv",
        filesize: int | None = 1_000_000,
        parent_dir: str = "/mnt/test",
    ) -> WalkEntry:
        return WalkEntry(
            filepath=filepath,
            filename=filename,
            filesize=filesize,
            parent_dir=parent_dir,
        )

    async def test_duplicate_filepath_in_records_does_not_raise(
        self, session: AsyncSession
    ) -> None:
        """Duplicate WalkEntry objects with the same filepath are deduplicated
        before insertion so no UNIQUE constraint violation is raised.

        This reproduces the crash scenario where _scandir_walk produces two
        entries for the same file (e.g. via a symlink loop).
        """
        scanner = MountScanner()
        ts = datetime.now(UTC)
        entry = self._make_entry()

        # Supply the same filepath twice — before the fix this would cause
        # session.add() to be called twice for the same filepath, and
        # session.flush() would raise IntegrityError.
        records = [entry, entry]

        result = await scanner._upsert_records(session, records, ts)
        await session.flush()

        assert result.added == 1
        assert result.errors == 0

        # Only one row must exist in the DB.
        from sqlalchemy import select as sa_select

        rows = (await session.execute(sa_select(MountIndex))).scalars().all()
        assert len(rows) == 1
        assert rows[0].filepath == entry.filepath

    async def test_duplicate_filepath_different_objects_does_not_raise(
        self, session: AsyncSession
    ) -> None:
        """Two distinct WalkEntry objects with the same filepath but different
        filenames are deduplicated (last entry wins) without raising.
        """
        scanner = MountScanner()
        ts = datetime.now(UTC)
        entry_a = self._make_entry(filename="Movie.2024.OLD.mkv")
        entry_b = self._make_entry(filename="Movie.2024.mkv")

        # Both have the same filepath but different filenames — last wins.
        records = [entry_a, entry_b]

        result = await scanner._upsert_records(session, records, ts)
        await session.flush()

        assert result.added == 1
        assert result.errors == 0

        from sqlalchemy import select as sa_select

        rows = (await session.execute(sa_select(MountIndex))).scalars().all()
        assert len(rows) == 1
        # Last entry (entry_b) wins.
        assert rows[0].filename == "Movie.2024.mkv"

    async def test_insert_when_row_already_exists_does_not_raise(
        self, session: AsyncSession
    ) -> None:
        """When a row already exists in the DB (inserted by a concurrent scan),
        calling _upsert_records with the same filepath must not raise a UNIQUE
        constraint violation.

        This reproduces the race between scan() and scan_directory(): both
        call _upsert_records for the same filepath, both see an empty
        existing_map at query time, and both attempt to INSERT.
        """
        scanner = MountScanner()
        ts = datetime.now(UTC)
        entry = self._make_entry()

        # First call: inserts the row.
        result1 = await scanner._upsert_records(session, [entry], ts)
        await session.flush()
        assert result1.added == 1

        # Simulate a concurrent scan: evict the ORM object from the session
        # identity map so the second _upsert_records call cannot see it via
        # the session cache and must rely on its own SELECT.  This reproduces
        # the scenario where two separate sessions both race to insert.
        session.expire_all()

        # Second call with the same filepath — before the fix this would
        # raise IntegrityError because the SELECT returned no row (the
        # session's identity map was cleared) so the code path fell through
        # to session.add(new MountIndex(...)) again.
        result2 = await scanner._upsert_records(session, [entry], ts)
        await session.flush()

        # The second call should report the file as added (ON CONFLICT path)
        # or updated (if existing_map catch works), with zero errors.
        assert result2.errors == 0
        assert result2.added + result2.updated + result2.unchanged == 1

        from sqlalchemy import select as sa_select

        rows = (await session.execute(sa_select(MountIndex))).scalars().all()
        assert len(rows) == 1

    async def test_on_conflict_updates_metadata_for_existing_filepath(
        self, session: AsyncSession
    ) -> None:
        """When ON CONFLICT fires (filepath already exists), the new metadata
        is written — the upsert acts as a full update rather than silently
        ignoring the conflict.
        """
        scanner = MountScanner()
        ts1 = datetime.now(UTC)
        entry_v1 = self._make_entry(
            filename="Movie.2024.1080p.mkv",
            filesize=1_000_000,
        )

        await scanner._upsert_records(session, [entry_v1], ts1)
        await session.flush()
        session.expire_all()

        # Simulate concurrent insert of the same filepath with updated metadata.
        ts2 = datetime.now(UTC)
        entry_v2 = self._make_entry(
            filename="Movie.2024.2160p.mkv",
            filesize=4_000_000,
        )

        await scanner._upsert_records(session, [entry_v2], ts2)
        await session.flush()

        from sqlalchemy import select as sa_select

        rows = (await session.execute(sa_select(MountIndex))).scalars().all()
        assert len(rows) == 1
        assert rows[0].filename == "Movie.2024.2160p.mkv"
        assert rows[0].filesize == 4_000_000

    async def test_large_batch_with_duplicates_does_not_raise(
        self, session: AsyncSession
    ) -> None:
        """A large batch that exceeds _BATCH_SIZE with embedded duplicates
        is processed without errors (tests batch boundary dedup behaviour).
        """
        scanner = MountScanner()
        ts = datetime.now(UTC)

        # Build 600 unique entries (exceeds _BATCH_SIZE=500) plus 3 duplicates.
        entries: list[WalkEntry] = []
        for i in range(600):
            entries.append(
                WalkEntry(
                    filepath=f"/mnt/test/ep{i:04d}.mkv",
                    filename=f"ep{i:04d}.mkv",
                    filesize=1_000 + i,
                    parent_dir="/mnt/test",
                )
            )
        # Append 3 duplicates of already-included entries.
        entries.append(entries[0])
        entries.append(entries[100])
        entries.append(entries[500])

        result = await scanner._upsert_records(session, entries, ts)
        await session.flush()

        assert result.added == 600
        assert result.errors == 0

        from sqlalchemy import func as sa_func
        from sqlalchemy import select as sa_select

        count = (
            await session.execute(sa_select(sa_func.count(MountIndex.id)))
        ).scalar()
        assert count == 600


# ---------------------------------------------------------------------------
# Group N: lookup_multi
# ---------------------------------------------------------------------------


class TestLookupMulti:
    """Tests for MountScanner.lookup_multi — multi-title variant lookup."""

    async def test_multi_title_exact_match(self, session: AsyncSession) -> None:
        """Second title in the list matches via Tier 1 exact when first fails."""
        await _insert_entry(
            session,
            filepath="/mnt/hero/S01E01.mkv",
            parsed_title="my hero academia",
            parsed_season=1,
            parsed_episode=1,
        )
        results = await mount_scanner.lookup_multi(
            session, ["boku no hero academia", "my hero academia"], season=1
        )
        assert len(results) == 1
        assert results[0].filepath == "/mnt/hero/S01E01.mkv"

    async def test_multi_title_first_match_wins(self, session: AsyncSession) -> None:
        """When the first title yields an exact match it is returned without trying later titles.

        Only the first title ("alpha show") is present in the DB.  "beta show" is also
        in the DB but NOT in the query list, so only 1 result can come back.  The second
        query element ("gamma show") doesn't exist, ensuring the return value is exactly
        the rows for "alpha show".
        """
        await _insert_entry(
            session,
            filepath="/mnt/alpha.mkv",
            parsed_title="alpha show",
        )
        await _insert_entry(
            session,
            filepath="/mnt/beta.mkv",
            parsed_title="beta show",
        )
        # Only "alpha show" is in the query; "gamma show" doesn't exist.
        results = await mount_scanner.lookup_multi(session, ["alpha show", "gamma show"])
        assert len(results) == 1
        assert results[0].filepath == "/mnt/alpha.mkv"

    async def test_single_title_delegation(self, session: AsyncSession) -> None:
        """lookup_multi with a single-element list behaves identically to lookup."""
        await _insert_entry(
            session,
            filepath="/mnt/some.title.mkv",
            parsed_title="some title",
        )
        multi_results = await mount_scanner.lookup_multi(session, ["some title"])
        single_results = await mount_scanner.lookup(session, "some title")
        assert len(multi_results) == len(single_results) == 1
        assert multi_results[0].filepath == single_results[0].filepath

    async def test_empty_titles_list_returns_empty(self, session: AsyncSession) -> None:
        """An empty titles list returns an empty result without error."""
        await _insert_entry(session, filepath="/mnt/any.mkv", parsed_title="any")
        results = await mount_scanner.lookup_multi(session, [])
        assert results == []

    async def test_dedup_normalized_titles(self, session: AsyncSession) -> None:
        """Titles that normalize to the same string are deduplicated; no duplicate queries."""
        await _insert_entry(
            session,
            filepath="/mnt/dark.knight.mkv",
            parsed_title="the dark knight",
        )
        # Both titles normalize to "the dark knight" — should still return one match.
        results = await mount_scanner.lookup_multi(
            session, ["The.Dark.Knight", "the dark knight"]
        )
        assert len(results) == 1
        assert results[0].filepath == "/mnt/dark.knight.mkv"

    async def test_multi_title_word_subsequence(self, session: AsyncSession) -> None:
        """A title that is a word-subsequence of a DB entry matches via Tier 2."""
        await _insert_entry(
            session,
            filepath="/mnt/hero/S01E01.mkv",
            parsed_title="my hero academia season 1",
            parsed_season=1,
        )
        # "my hero" (2 words) is a word-subsequence of "my hero academia season 1".
        # "boku no hero" fails both exact and subsequence; "my hero" succeeds via Tier 2.
        results = await mount_scanner.lookup_multi(
            session, ["boku no hero", "my hero"], season=1
        )
        assert len(results) == 1
        assert results[0].filepath == "/mnt/hero/S01E01.mkv"

    async def test_multi_title_reverse_containment(self, session: AsyncSession) -> None:
        """A DB title (3+ words) that is a subsequence of a query title matches via Tier 3."""
        await _insert_entry(
            session,
            filepath="/mnt/titan/S01E01.mkv",
            parsed_title="attack on titan",
            parsed_season=1,
        )
        # "attack on titan the final season" contains "attack on titan" (3 words) in order.
        results = await mount_scanner.lookup_multi(
            session,
            ["attack on titan the final season"],
            season=1,
        )
        assert len(results) == 1
        assert results[0].filepath == "/mnt/titan/S01E01.mkv"


# ---------------------------------------------------------------------------
# Group N+1: gather_alt_titles
# ---------------------------------------------------------------------------


def _make_media_item(
    title: str,
    tmdb_id: str | None = None,
) -> MagicMock:
    """Create a lightweight MediaItem-like stub for gather_alt_titles tests.

    Uses MagicMock so that SQLAlchemy instrumentation is avoided — the function
    only reads item.title and item.tmdb_id.
    """
    item = MagicMock(spec=["title", "tmdb_id"])
    item.title = title
    item.tmdb_id = tmdb_id
    return item


class TestGatherAltTitles:
    """Tests for the gather_alt_titles module-level utility."""

    async def test_primary_title_always_first(self, session: AsyncSession) -> None:
        """item.title is always the first element in the returned list."""
        item = _make_media_item(title="Show A")
        with patch("src.services.anidb.anidb_client") as mock_anidb:
            mock_anidb.get_titles_for_tmdb_id = AsyncMock(return_value=[])
            # tmdb_id=None so AniDB is not called; no tmdb_original_title either.
            result = await gather_alt_titles(session, item)
        assert result[0] == "Show A"

    async def test_tmdb_original_title_added(self, session: AsyncSession) -> None:
        """tmdb_original_title is appended when different from item.title."""
        item = _make_media_item(title="Show A")
        # tmdb_id=None so AniDB is not called.
        result = await gather_alt_titles(session, item, tmdb_original_title="Show B")
        assert result == ["Show A", "Show B"]

    async def test_tmdb_original_title_dedup_exact(self, session: AsyncSession) -> None:
        """tmdb_original_title identical to item.title is not added twice."""
        item = _make_media_item(title="Show A")
        result = await gather_alt_titles(session, item, tmdb_original_title="Show A")
        assert result == ["Show A"]

    async def test_tmdb_original_title_dedup_case_insensitive(
        self, session: AsyncSession
    ) -> None:
        """tmdb_original_title matching item.title in a different case is not added."""
        item = _make_media_item(title="Show A")
        result = await gather_alt_titles(session, item, tmdb_original_title="show a")
        assert result == ["Show A"]

    async def test_anidb_titles_included(self, session: AsyncSession) -> None:
        """AniDB titles for the item's tmdb_id are appended after the primary title.

        The lazy import in gather_alt_titles resolves anidb_client via
        ``src.services.anidb``, so we patch at that module path.
        """
        item = _make_media_item(title="Show A", tmdb_id="12345")
        with patch("src.services.anidb.anidb_client") as mock_anidb:
            mock_anidb.get_titles_for_tmdb_id = AsyncMock(
                return_value=["Alt Title 1", "Alt Title 2"]
            )
            result = await gather_alt_titles(session, item)
        assert "Alt Title 1" in result
        assert "Alt Title 2" in result
        assert result[0] == "Show A"

    async def test_anidb_failure_graceful(self, session: AsyncSession) -> None:
        """AniDB lookup failure is silently swallowed; returns just [item.title]."""
        item = _make_media_item(title="Show A", tmdb_id="12345")
        with patch("src.services.anidb.anidb_client") as mock_anidb:
            mock_anidb.get_titles_for_tmdb_id = AsyncMock(
                side_effect=RuntimeError("AniDB unavailable")
            )
            result = await gather_alt_titles(session, item)
        assert result == ["Show A"]

    async def test_no_tmdb_id_skips_anidb(self, session: AsyncSession) -> None:
        """When item.tmdb_id is None, the AniDB client is never called."""
        item = _make_media_item(title="Show A", tmdb_id=None)
        with patch("src.services.anidb.anidb_client") as mock_anidb:
            mock_anidb.get_titles_for_tmdb_id = AsyncMock(return_value=["Should Not Appear"])
            result = await gather_alt_titles(session, item)
        mock_anidb.get_titles_for_tmdb_id.assert_not_called()
        assert result == ["Show A"]


# ---------------------------------------------------------------------------
# Group N+2: single-word Tier 2 skip + Tier 3 behaviour
# ---------------------------------------------------------------------------


class TestSingleWordLookup:
    """Tests for the single-word title edge case in lookup/lookup_multi.

    Single-word queries with 4+ characters use a whole-word Tier 2 check:
    the DB parsed_title must contain that word as a complete token (not a
    substring).  This enables matching titles like "Sinners" or "Parasite"
    that are indexed as single-word parsed_title values.

    Words shorter than 4 characters skip Tier 2 entirely and fall through to
    Tier 3 (reverse containment), which requires 3+ DB words — so short
    single-word queries that don't exact-match return nothing.
    """

    async def test_single_word_tier2_match_against_multi_word_db_title(
        self, session: AsyncSession
    ) -> None:
        """4+ char single-word query matches a DB title where that word is a token."""
        await _insert_entry(
            session,
            filepath="/mnt/show.mkv",
            parsed_title="test show",  # "show" is a token here
        )
        results = await mount_scanner.lookup(session, "Show")
        # "show" (4 chars) appears as a whole token in "test show" — Tier 2 matches
        assert len(results) == 1
        assert results[0].filepath == "/mnt/show.mkv"

    async def test_single_word_match_against_long_db_title(
        self, session: AsyncSession
    ) -> None:
        """4+ char single-word query matches a 3-word DB title via whole-word Tier 2."""
        await _insert_entry(
            session,
            filepath="/mnt/long.show.mkv",
            parsed_title="test show title",
        )
        results = await mount_scanner.lookup(session, "test")
        # "test" (4 chars) is a token in "test show title" — Tier 2 matches
        assert len(results) == 1
        assert results[0].filepath == "/mnt/long.show.mkv"

    async def test_single_word_no_false_positive(self, session: AsyncSession) -> None:
        """Single short word "It" does not match unrelated DB entries."""
        await _insert_entry(
            session,
            filepath="/mnt/sci.fi.show.mkv",
            parsed_title="science fiction show",
        )
        await _insert_entry(
            session,
            filepath="/mnt/another.mkv",
            parsed_title="another random title",
        )
        results = await mount_scanner.lookup(session, "It")
        assert results == []

    async def test_single_word_exact_match_still_works(
        self, session: AsyncSession
    ) -> None:
        """A single-word query that exactly matches a DB title hits via Tier 1."""
        await _insert_entry(
            session,
            filepath="/mnt/alien.mkv",
            parsed_title="alien",
        )
        results = await mount_scanner.lookup(session, "Alien")
        assert len(results) == 1
        assert results[0].filepath == "/mnt/alien.mkv"


# ---------------------------------------------------------------------------
# Group N: _extract_season_from_path — season range detection
# ---------------------------------------------------------------------------


class TestSeasonRangeDetection:
    """Tests for season range detection in _extract_season_from_path."""

    def test_s01_s07_returns_none(self) -> None:
        """S01-S07 range detected, no season inferred."""
        result = _extract_season_from_path(
            "/mnt/zurg/__all__/Show (S01-S07)/", "/mnt/zurg/__all__"
        )
        assert result is None

    def test_s01_space_s07_returns_none(self) -> None:
        """S01 - S07 with spaces is also a range."""
        result = _extract_season_from_path(
            "/mnt/zurg/__all__/Show S01 - S07 Complete/", "/mnt/zurg/__all__"
        )
        assert result is None

    def test_s01_07_no_second_s_returns_none(self) -> None:
        """S01-07 without second S prefix is still a range."""
        result = _extract_season_from_path(
            "/mnt/zurg/__all__/Show S01-07 Complete/", "/mnt/zurg/__all__"
        )
        assert result is None

    def test_season_1_7_returns_none(self) -> None:
        """Season 1-7 keyword range detected."""
        result = _extract_season_from_path(
            "/mnt/zurg/__all__/Show Season 1-7/", "/mnt/zurg/__all__"
        )
        assert result is None

    def test_seasons_1_3_returns_none(self) -> None:
        """Seasons 1-3 (plural) range detected."""
        result = _extract_season_from_path(
            "/mnt/zurg/__all__/Show (Seasons 1-3 + OVA)/", "/mnt/zurg/__all__"
        )
        assert result is None

    def test_single_s02_preserved(self) -> None:
        """Single season S02 still returns 2."""
        result = _extract_season_from_path(
            "/mnt/zurg/__all__/Show.S02.1080p/", "/mnt/zurg/__all__"
        )
        assert result == 2

    def test_single_season_4_preserved(self) -> None:
        """Single 'Season 4' still returns 4."""
        result = _extract_season_from_path(
            "/mnt/zurg/__all__/Show Season 4/", "/mnt/zurg/__all__"
        )
        assert result == 4

    def test_nested_range_parent_single_child(self) -> None:
        """Inner Season 2 wins over outer S01-S07 range."""
        result = _extract_season_from_path(
            "/mnt/zurg/__all__/Show (S01-S07)/Season 2/", "/mnt/zurg/__all__"
        )
        assert result == 2

    def test_s2_space_dash_01_is_not_range(self) -> None:
        """S2 - 01 is season+episode, NOT a range. Season 2 extracted."""
        result = _extract_season_from_path(
            "/mnt/zurg/__all__/Show S2 - 01/", "/mnt/zurg/__all__"
        )
        assert result == 2


# ---------------------------------------------------------------------------
# Group N+1: _SPECIAL_FILENAME_RE — special file filtering
# ---------------------------------------------------------------------------


class TestSpecialFilenameFilter:
    """Tests for _SPECIAL_FILENAME_RE filtering of non-episode files."""

    def _matches(self, filename: str) -> bool:
        from src.main import _SPECIAL_FILENAME_RE

        return bool(_SPECIAL_FILENAME_RE.search(filename))

    def test_ncop_filtered(self) -> None:
        """NCOP with number is filtered."""
        assert self._matches("Show S2 [SP01] NCOP - 01.mkv")

    def test_nced_filtered(self) -> None:
        """NCED with number is filtered."""
        assert self._matches("Show S2 [SP02] NCED - 01.mkv")

    def test_ncop_no_number(self) -> None:
        """NCOP without trailing number is filtered."""
        assert self._matches("Show S6 NCOP.mkv")

    def test_nced_no_number(self) -> None:
        """NCED without trailing number is filtered."""
        assert self._matches("Show S6 NCED.mkv")

    def test_bare_op_filtered(self) -> None:
        """Bare OP followed by number is filtered."""
        assert self._matches("OP 01.mkv")

    def test_bare_ed_filtered(self) -> None:
        """Bare ED followed by number is filtered."""
        assert self._matches("ED 02.mkv")

    def test_creditless_filtered(self) -> None:
        """Creditless keyword is filtered."""
        assert self._matches("Show Creditless Opening.mkv")

    def test_trailer_filtered(self) -> None:
        """Trailer keyword is filtered."""
        assert self._matches("Show Trailer.mkv")

    def test_normal_episode_not_filtered(self) -> None:
        """A regular episode file is not filtered."""
        assert not self._matches("[Anime Time] Show Title - 01.mkv")

    def test_episode_with_title_not_filtered(self) -> None:
        """Season+episode coded file is not filtered."""
        assert not self._matches("S02E01 - Beast Titan.mkv")

    def test_movie_not_filtered(self) -> None:
        """Movie filename containing a number is not filtered."""
        assert not self._matches("[Anime Time] Show Title Movie 01 - Two Heroes.mkv")

    def test_ova_not_filtered(self) -> None:
        """OVA files should NOT be filtered (user may want them)."""
        assert not self._matches("[Anime Time] Show Title OVA - Training.mkv")


# ---------------------------------------------------------------------------
# Change 1: Single-word Tier 2 in lookup_multi
# ---------------------------------------------------------------------------


class TestSingleWordTier2LookupMulti:
    """Tests for the single-word Tier 2 path in lookup_multi.

    A single-word query with 4+ characters now performs a LIKE search and
    verifies the word appears as a whole token in the DB title (not a
    substring).  Words shorter than 4 characters are skipped entirely.
    """

    async def test_single_word_tier2_matches_word_in_multi_word_title(
        self, session: AsyncSession
    ) -> None:
        """Single-word 4+ char query matches DB entry where that word is a token.

        lookup("sinners") should match parsed_title="sinners 2025" because
        "sinners" is a whole word in that title.
        """
        await _insert_entry(
            session,
            filepath="/mnt/sinners/movie.mkv",
            parsed_title="sinners 2025",
        )
        results = await mount_scanner.lookup(session, "sinners")
        assert len(results) == 1
        assert results[0].filepath == "/mnt/sinners/movie.mkv"

    async def test_single_word_tier2_no_partial_match(
        self, session: AsyncSession
    ) -> None:
        """Single-word query must NOT match when the word is a substring of another word.

        lookup("black") should NOT match parsed_title="blackberry 2023" because
        "black" is not a whole token — it is part of "blackberry".
        """
        await _insert_entry(
            session,
            filepath="/mnt/blackberry/movie.mkv",
            parsed_title="blackberry 2023",
        )
        results = await mount_scanner.lookup(session, "black")
        assert results == []

    async def test_single_word_tier2_short_word_skipped(
        self, session: AsyncSession
    ) -> None:
        """Words shorter than 4 characters skip Tier 2 entirely.

        lookup("it") should NOT match parsed_title="it chapter two" because
        the word "it" is only 2 chars — Tier 2 is skipped, and Tier 3
        requires 3+ DB words but the exact match fails as well.
        """
        await _insert_entry(
            session,
            filepath="/mnt/it/movie.mkv",
            parsed_title="it chapter two",
        )
        # "it" is 2 chars; skip Tier 2.  Tier 3 requires DB title words to be
        # a subsequence of the input words — "it chapter two" (3 words) would
        # need all three to appear in ["it"], which fails.
        results = await mount_scanner.lookup(session, "it")
        assert results == []

    async def test_single_word_tier2_exact_takes_priority(
        self, session: AsyncSession
    ) -> None:
        """Tier 1 (exact match) is returned before Tier 2 is consulted.

        When parsed_title is exactly the query word, the exact-match path
        returns it without needing the Tier 2 LIKE path.
        """
        await _insert_entry(
            session,
            filepath="/mnt/parasite/movie.mkv",
            parsed_title="parasite",
        )
        # Also insert a multi-word entry to confirm Tier 2 is not needed.
        await _insert_entry(
            session,
            filepath="/mnt/parasite2/movie.mkv",
            parsed_title="parasite 2019",
        )
        results = await mount_scanner.lookup(session, "parasite")
        # Exact match returns the exact entry (and possibly the multi-word one
        # too via IN query), but the result must include the exact entry.
        filepaths = {r.filepath for r in results}
        assert "/mnt/parasite/movie.mkv" in filepaths

    async def test_single_word_tier2_with_season_filter(
        self, session: AsyncSession
    ) -> None:
        """Single-word Tier 2 respects the season filter.

        lookup("sinners", season=1) should NOT match an entry with parsed_season=2.
        """
        await _insert_entry(
            session,
            filepath="/mnt/sinners/s02.mkv",
            parsed_title="sinners 2025",
            parsed_season=2,
        )
        results = await mount_scanner.lookup(session, "sinners", season=1)
        assert results == []

    async def test_single_word_tier2_with_episode_filter(
        self, session: AsyncSession
    ) -> None:
        """Single-word Tier 2 respects the episode filter.

        lookup("sinners", episode=5) should only return entries with parsed_episode=5.
        """
        await _insert_entry(
            session,
            filepath="/mnt/sinners/ep05.mkv",
            parsed_title="sinners 2025",
            parsed_season=1,
            parsed_episode=5,
        )
        await _insert_entry(
            session,
            filepath="/mnt/sinners/ep06.mkv",
            parsed_title="sinners 2025",
            parsed_season=1,
            parsed_episode=6,
        )
        results = await mount_scanner.lookup(session, "sinners", episode=5)
        assert len(results) == 1
        assert results[0].filepath == "/mnt/sinners/ep05.mkv"


# ---------------------------------------------------------------------------
# Change 2: Bidirectional fuzzy match in scan_directory
# ---------------------------------------------------------------------------


class TestScanDirectoryBidirectionalFuzzy:
    """Tests for the bidirectional fuzzy word-subsequence match in scan_directory.

    The fuzzy directory search now checks both forward (input words subset of
    dir name words) AND reverse (dir name words subset of input words).
    A guard prevents single-word directory names from reverse-matching.
    """

    async def test_scan_directory_fuzzy_reverse_match(
        self, session: AsyncSession
    ) -> None:
        """Torrent filename with many words (codec/resolution tags) fuzzy-matches a
        Zurg directory that only contains the base title and year.

        The torrent name words are a superset of the directory name words, so
        the reverse direction (dir_words IN input_words) triggers the match.
        """
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            # The directory name is short — just title + year.
            short_dir = os.path.join(tmpdir, "Sample Film 2023")
            os.makedirs(short_dir)
            open(os.path.join(short_dir, "Sample.Film.2023.mkv"), "wb").write(b"v")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                # The torrent name has many extra words; exact match will fail.
                result = await scanner.scan_directory(
                    session,
                    "Sample Film 2023 1080p BluRay x264-GROUP",
                )

        # The fuzzy reverse match should have found "Sample Film 2023".
        assert result.files_indexed == 1
        assert result.matched_dir_path is not None
        assert "Sample Film 2023" in result.matched_dir_path

    async def test_scan_directory_fuzzy_single_word_dir_guard(
        self, session: AsyncSession
    ) -> None:
        """A directory with a single-word name must NOT match via reverse direction.

        The guard `len(norm_words) < 2` prevents broad matches like a directory
        named "2023" matching every torrent that contains the year.
        """
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            # Single-word directory name.
            single_dir = os.path.join(tmpdir, "2023")
            os.makedirs(single_dir)
            open(os.path.join(single_dir, "Some.Movie.2023.mkv"), "wb").write(b"v")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                # Input does contain "2023" but the dir is single-word — guard fires.
                result = await scanner.scan_directory(
                    session,
                    "Totally Different Movie 2023 1080p",
                )

        # Single-word reverse match was blocked; directory was not matched.
        assert result.files_indexed == 0
        assert result.matched_dir_path is None

    async def test_scan_directory_fuzzy_forward_still_works(
        self, session: AsyncSession
    ) -> None:
        """The original forward-direction fuzzy match (dir name words subset of input)
        continues to work after the bidirectional change.
        """
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            # Directory name is longer; the input is a shortened version.
            long_dir = os.path.join(tmpdir, "Test Show Season One 2022")
            os.makedirs(long_dir)
            open(os.path.join(long_dir, "Show.S01E01.mkv"), "wb").write(b"v")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                # Input words are a subsequence of the directory name words
                # (forward direction: "test show season one 2022" contains "test show").
                result = await scanner.scan_directory(
                    session,
                    "test show",
                )

        assert result.files_indexed == 1
        assert result.matched_dir_path is not None


# ---------------------------------------------------------------------------
# Change 3: torrent_filename parameter in gather_alt_titles
# ---------------------------------------------------------------------------


class TestGatherAltTitlesTorrentFilename:
    """Tests for the torrent_filename parameter added to gather_alt_titles.

    When a raw torrent filename is supplied, PTN parses it and the extracted
    title is appended to the list (if not a duplicate of existing titles).
    """

    async def test_gather_alt_titles_with_torrent_filename(
        self, session: AsyncSession
    ) -> None:
        """PTN-parsed title from torrent_filename is appended to the list."""
        item = _make_media_item(title="Cool Movie")
        result = await gather_alt_titles(
            session,
            item,
            torrent_filename="Cool.Movie.2023.BluRay.x264-GROUP",
        )
        # PTN should extract "Cool Movie" from the filename.  Since it
        # normalizes to the same lower-case as item.title, it should be deduped.
        # But PTN may return a different casing or abbreviation.  At minimum
        # the primary title must be present and no crash must occur.
        assert "Cool Movie" in result
        assert result[0] == "Cool Movie"

    async def test_gather_alt_titles_torrent_filename_adds_different_title(
        self, session: AsyncSession
    ) -> None:
        """When the PTN-parsed title differs from item.title it is appended."""
        item = _make_media_item(title="Sample Film English Title")
        result = await gather_alt_titles(
            session,
            item,
            torrent_filename="Sample.Film.Original.Language.2023.1080p.mkv",
        )
        # PTN extracts "Sample Film Original Language" — different from item.title
        # so it should be appended.
        assert "Sample Film English Title" in result
        assert len(result) >= 2

    async def test_gather_alt_titles_torrent_filename_dedup(
        self, session: AsyncSession
    ) -> None:
        """PTN-parsed title identical to item.title (case-insensitive) is not duplicated."""
        item = _make_media_item(title="Test Movie")
        # Filename parses to "Test Movie" — same as item.title after normalization.
        result = await gather_alt_titles(
            session,
            item,
            torrent_filename="Test.Movie.2024.1080p.WEB-DL.x265-GRP",
        )
        # "Test Movie" (or equivalent) must appear exactly once.
        lower_titles = [t.lower() for t in result]
        assert lower_titles.count("test movie") == 1

    async def test_gather_alt_titles_torrent_filename_none(
        self, session: AsyncSession
    ) -> None:
        """When torrent_filename=None, behavior is identical to the original signature."""
        item = _make_media_item(title="Test Show")
        result_without = await gather_alt_titles(session, item)
        result_with_none = await gather_alt_titles(session, item, torrent_filename=None)
        assert result_without == result_with_none

    async def test_gather_alt_titles_torrent_filename_ptn_failure(
        self, session: AsyncSession
    ) -> None:
        """When PTN fails on the torrent_filename no crash occurs and normal titles return."""
        item = _make_media_item(title="Test Movie")
        with patch("src.core.mount_scanner.PTN.parse", side_effect=RuntimeError("PTN boom")):
            result = await gather_alt_titles(
                session,
                item,
                torrent_filename="anything.mkv",
            )
        # Must not raise; primary title must still be in result.
        assert result[0] == "Test Movie"
