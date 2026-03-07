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
  - MountScanner.get_index_stats: empty DB, movies-only, episodes-only, mixed
  - MountScanner.clear_index: returns count, leaves table empty
  - MountScanner._should_skip_dir: hidden, __MACOSX, @eaDir, .Trash-*, normal
  - VIDEO_EXTENSIONS constant: spot-check known extensions
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
                count = await scanner.scan_directory(session, "ShowFolder")

        assert count == 2

        # Confirm the DB was populated.
        stats = await scanner.get_index_stats(session)
        assert stats["total_files"] == 2

    async def test_scan_directory_returns_zero_for_nonexistent(
        self, session: AsyncSession
    ) -> None:
        """scan_directory returns 0 without error when the subdirectory does not exist."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                count = await scanner.scan_directory(session, "does_not_exist")

        assert count == 0

    async def test_scan_directory_returns_zero_for_empty_dir(
        self, session: AsyncSession
    ) -> None:
        """scan_directory returns 0 when the named subdirectory contains no video files."""
        scanner = MountScanner()
        with tempfile.TemporaryDirectory() as tmpdir:
            subdir = os.path.join(tmpdir, "EmptyFolder")
            os.makedirs(subdir)

            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = tmpdir
                count = await scanner.scan_directory(session, "EmptyFolder")

        assert count == 0

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
                count = await scanner.scan_directory(session, "ShowFolder")

        assert count == 3
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
                count = await scanner.scan_directory(session, "MovieFolder")

        # 1 file total: it was updated (not inserted as a duplicate).
        assert count == 1
        stats = await scanner.get_index_stats(session)
        assert stats["total_files"] == 1

    async def test_scan_directory_timeout_returns_zero(
        self, session: AsyncSession
    ) -> None:
        """When the existence check times out, scan_directory returns 0 gracefully."""
        scanner = MountScanner()
        with patch("asyncio.wait_for", side_effect=TimeoutError("simulated hang")):
            with patch("src.core.mount_scanner.settings") as mock_settings:
                mock_settings.paths.zurg_mount = "/some/mount"
                count = await scanner.scan_directory(session, "any_dir")

        assert count == 0


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
