"""Tests for src/core/mount_scanner.py.

Covers:
  - _parse_filename: movies, episodes, season packs, PTN failure fallback,
    hidden extension stripping, title lowercasing
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
    re-scan dedup, timeout graceful exit
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.mount_scanner import (
    VIDEO_EXTENSIONS,
    MountScanner,
    ScanDirectoryResult,
    ScanResult,
    _parse_filename,
    mount_scanner,
)
from src.models.mount_index import MountIndex

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


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
# Group 1: _parse_filename
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


# ---------------------------------------------------------------------------
# Group 2: is_mount_available
# ---------------------------------------------------------------------------


class TestIsMountAvailable:
    """Tests for the FUSE mount health check."""

    async def test_real_directory_returns_true(self) -> None:
        """An existing, listable directory returns True."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                result = await scanner.is_mount_available()
        assert result is True

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
        """A second scan over the same files updates rows, does not add duplicates."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            open(os.path.join(tmpdir, "Movie.2024.mkv"), "wb").write(b"fake")

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir

                first = await scanner.scan(session)
                assert first.files_added == 1

                await session.flush()

                second = await scanner.scan(session)

        assert second.files_found == 1
        assert second.files_added == 0
        assert second.files_updated == 1
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

            def _walk_with_size_error(path: str):
                records, errors = original_walk(path)
                # Simulate a stat failure by bumping error count and clearing filesize
                for r in records:
                    r["filesize"] = None
                return records, errors + 1  # simulate one extra error

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


# ---------------------------------------------------------------------------
# Group 4: lookup
# ---------------------------------------------------------------------------


class TestLookup:
    """Tests for MountScanner.lookup (DB-only, no filesystem access)."""

    async def test_lookup_by_title_exact(self, session: AsyncSession) -> None:
        """Exact normalized title match returns the correct entry; partial words do not match."""
        await _insert_entry(
            session, filepath="/mnt/dark.knight.mkv", parsed_title="the dark knight"
        )
        await _insert_entry(
            session, filepath="/mnt/batman.mkv", parsed_title="batman begins"
        )
        scanner = MountScanner()
        # Partial word "dark" must NOT match "the dark knight"
        no_results = await scanner.lookup(session, "dark")
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

    async def test_lookup_word_subsequence_single_word_no_fallback(self, session: AsyncSession) -> None:
        """Single-word queries do not trigger the subsequence fallback."""
        await _insert_entry(
            session, filepath="/mnt/movie.mkv",
            parsed_title="the terminator",
        )
        scanner = MountScanner()
        results = await scanner.lookup(session, "terminator")
        assert len(results) == 0  # exact fails, and single word = no fallback

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


def _make_record(filepath: str, filename: str | None = None) -> dict:
    """Build a minimal file-record dict suitable for _upsert_records."""
    name = filename or os.path.basename(filepath)
    return {
        "filepath": filepath,
        "filename": name,
        "parsed_title": os.path.splitext(name)[0].lower(),
        "parsed_year": None,
        "parsed_season": None,
        "parsed_episode": None,
        "parsed_resolution": None,
        "parsed_codec": None,
        "filesize": 1024,
    }


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
        added, updated, errors = await scanner._upsert_records(session, records, ts)

        assert added == 3
        assert updated == 0
        assert errors == 0

    async def test_batch_upsert_updates_existing_records(
        self, session: AsyncSession
    ) -> None:
        """Re-upserting identical filepaths increments updated, not added."""
        scanner = MountScanner()
        fp = "/mnt/existing.mkv"
        await _insert_entry(session, filepath=fp, filename="existing.mkv")
        await session.flush()

        records = [_make_record(fp)]
        ts = _utcnow()
        added, updated, errors = await scanner._upsert_records(session, records, ts)

        assert added == 0
        assert updated == 1
        assert errors == 0

    async def test_batch_upsert_large_batch(self, session: AsyncSession) -> None:
        """Batches larger than 500 records are all inserted correctly."""
        scanner = MountScanner()
        n = 620
        records = [_make_record(f"/mnt/file{i:04d}.mkv") for i in range(n)]
        ts = _utcnow()
        added, updated, errors = await scanner._upsert_records(session, records, ts)

        assert added == n
        assert updated == 0
        assert errors == 0

        # Verify they're all in the DB.
        stats = await scanner.get_index_stats(session)
        assert stats["total_files"] == n

    async def test_batch_upsert_mixed_insert_and_update(
        self, session: AsyncSession
    ) -> None:
        """Mix of new and pre-existing filepaths splits correctly into added/updated."""
        scanner = MountScanner()
        existing_fp = "/mnt/already.mkv"
        await _insert_entry(session, filepath=existing_fp, filename="already.mkv")
        await session.flush()

        records = [
            _make_record(existing_fp),          # pre-existing → updated
            _make_record("/mnt/brand_new_a.mkv"),  # new → added
            _make_record("/mnt/brand_new_b.mkv"),  # new → added
        ]
        ts = _utcnow()
        added, updated, errors = await scanner._upsert_records(session, records, ts)

        assert added == 2
        assert updated == 1
        assert errors == 0

    async def test_batch_upsert_empty_list_returns_zeros(
        self, session: AsyncSession
    ) -> None:
        """Passing an empty records list returns (0, 0, 0) without touching the DB."""
        scanner = MountScanner()
        ts = _utcnow()
        added, updated, errors = await scanner._upsert_records(session, [], ts)

        assert added == 0
        assert updated == 0
        assert errors == 0


# ---------------------------------------------------------------------------
# Group 13: _scandir_walk
# ---------------------------------------------------------------------------


class TestScandirWalk:
    """Tests for MountScanner._scandir_walk — the synchronous filesystem walker."""

    def test_scandir_walk_finds_video_files(self) -> None:
        """Video files in a directory are returned as records."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            open(os.path.join(tmpdir, "Movie.2024.1080p.mkv"), "wb").write(b"v")
            open(os.path.join(tmpdir, "Show.S01E01.mkv"), "wb").write(b"v")

            records, errors = scanner._scandir_walk(tmpdir)

        assert errors == 0
        filenames = {r["filename"] for r in records}
        assert "Movie.2024.1080p.mkv" in filenames
        assert "Show.S01E01.mkv" in filenames
        assert len(records) == 2

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
        assert records[0]["filename"] == "visible.mkv"

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
        assert records[0]["filename"] == "real.mkv"

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
        filenames = {r["filename"] for r in records}
        assert "Show.S01E01.mkv" in filenames
        assert "Show.S01E02.mkv" in filenames
        assert "Show.S02E01.Extras.mkv" in filenames

    def test_scandir_walk_record_has_required_keys(self) -> None:
        """Every record dict contains all required metadata keys."""
        scanner = MountScanner()
        required_keys = {
            "filepath", "filename", "parsed_title", "parsed_year",
            "parsed_season", "parsed_episode", "parsed_resolution",
            "parsed_codec", "filesize",
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            open(os.path.join(tmpdir, "Movie.2024.mkv"), "wb").write(b"v")
            records, _ = scanner._scandir_walk(tmpdir)

        assert len(records) == 1
        assert required_keys.issubset(records[0].keys())

    def test_scandir_walk_filepath_is_absolute(self) -> None:
        """The filepath in each record is an absolute path."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            open(os.path.join(tmpdir, "file.mkv"), "wb").write(b"v")
            records, _ = scanner._scandir_walk(tmpdir)

        assert len(records) == 1
        assert os.path.isabs(records[0]["filepath"])

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
        assert records[0]["filename"] == "visible.mkv"


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
        older_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
        newer_ts = datetime(2024, 6, 1, tzinfo=timezone.utc)
        newest_ts = datetime(2025, 1, 1, tzinfo=timezone.utc)

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

    async def test_fuzzy_match_skips_directories(
        self, session: AsyncSession
    ) -> None:
        """Fuzzy file search ignores subdirectories in the mount root."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            # A subdirectory whose name would otherwise match.
            subdir = os.path.join(tmpdir, "Tenet.2020.1080p")
            os.makedirs(subdir)
            open(os.path.join(subdir, "Tenet.2020.mkv"), "wb").write(b"v")
            # No matching file at root level.

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                result = await scanner.scan_directory(session, "Tenet 2020.mkv")

        # Directories must not be matched — result should be empty.
        assert result.files_indexed == 0
        assert result.matched_dir_path is None

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
