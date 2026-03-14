"""Zurg mount file indexer for vibeDebrid.

Periodically scans the rclone/Zurg mount directory to build a local file index
in the ``mount_index`` database table.  Before scraping any queue item, the
pipeline checks this index first — a cache hit bypasses all scraper calls and
jumps straight to symlink creation.

Key design decisions:
- All filesystem operations run via ``asyncio.to_thread`` so the event loop is
  never blocked by FUSE I/O.
- A FUSE mount hang (stale NFS/rclone) is handled with a 5-second timeout on
  the health-check listdir call so it never blocks forever.
- When the mount is unavailable the index is NOT cleared — the last-known state
  is preserved so CHECKING transitions can still be evaluated from the cached
  data.
- PTN is used for filename parsing; failures are caught and logged without
  aborting the whole scan.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, NamedTuple

import PTN
from pydantic import BaseModel
from sqlalchemy import delete, func, literal, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.models.mount_index import MountIndex

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Title normalisation
# ---------------------------------------------------------------------------

_NON_ALNUM_RE = re.compile(r"[^a-z0-9 ]+")
_MULTI_SPACE_RE = re.compile(r" {2,}")

# Anime TV-N naming convention: "TV-2 - 01" means season 2, episode 1.
# Common in Russian fansub releases. PTN doesn't recognise this pattern
# and often misparses "1920x1080" as a season number instead.
_TV_N_EP_RE = re.compile(r"TV-(\d{1,2})\s*[-–]\s*(\d{1,3})")

# Anime "Title - NN" naming convention, e.g.:
#   "[Anime Time] Attack on Titan - 01.mkv"
#   "[ASW] Show Name - 05 [1080p HEVC][ABCD1234].mkv"
# Captures the episode number after " - " (space-dash-space).
_ANIME_DASH_EP_RE = re.compile(r"\s-\s(\d{1,3})(?:\s|$|\[|\()")

# Leading number naming convention for season pack episodes, e.g.:
#   "28. Prelude to the Impending Fight.mp4"
#   "01 - Episode Title.mkv"
# Matches a number at the very start of the filename.
_LEADING_EP_RE = re.compile(r"^(\d{1,3})(?:\.|(?:\s*[-–]\s))")

# Detect titles that look like episode markers rather than real show titles.
# Examples (normalised): "s02e01 beast titan", "s01e01", "e01 prologue".
# If a title starts with this pattern it almost certainly came from an
# episode-titled filename (e.g. "S02E01 - Beast Titan.mkv") and lacks the
# show name entirely.
# Matches:
#   s02e01 ...   — standard SxxExx prefix
#   e01 ...      — bare EXX prefix (no season number before it)
_EPISODE_MARKER_RE = re.compile(r"^(s\d{1,2})?e\d{1,3}\b")


class WalkEntry(NamedTuple):
    """Lightweight record from the filesystem walk phase (no PTN parsing)."""

    filepath: str
    filename: str
    filesize: int | None
    parent_dir: str  # needed for _extract_season_from_path fallback


class UpsertResult(NamedTuple):
    """Result counters from _upsert_records."""

    added: int
    updated: int
    unchanged: int
    errors: int


def _normalize_title(title: str) -> str:
    """Normalize a title for consistent matching.

    Lowercases, strips non-alphanumeric characters (except spaces),
    and collapses multiple spaces.

    Args:
        title: Raw title string (e.g. from TMDB or a torrent filename).

    Returns:
        Normalised lowercase string containing only alphanumerics and
        single spaces.
    """
    result = title.lower().strip()
    result = _NON_ALNUM_RE.sub(" ", result)
    result = _MULTI_SPACE_RE.sub(" ", result)
    return result.strip()


def _is_word_subsequence(query_words: list[str], target_words: list[str]) -> bool:
    """Check if all query words appear in target words in order (subsequence).

    Words must match exactly but need not be consecutive. For example,
    ["terminator", "judgement", "day"] is a subsequence of
    ["terminator", "2", "judgement", "day", "1991", ...].
    """
    it = iter(target_words)
    return all(any(t == q for t in it) for q in query_words)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VIDEO_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".mkv",
        ".mp4",
        ".avi",
        ".mov",
        ".wmv",
        ".flv",
        ".webm",
        ".m4v",
        ".ts",
        ".m2ts",
    }
)

# Directories that should never be indexed regardless of their content.
_SKIP_DIRS: frozenset[str] = frozenset({"__MACOSX", "@eaDir"})

# Pattern prefix for trash directories (e.g. .Trash-1000)
_TRASH_PREFIX: str = ".Trash-"

# Pattern to detect season numbers in directory names, e.g. "Season 4", "S04", "Staffel 2".
_DIR_SEASON_RE = re.compile(r"\b(?:season|staffel)\s*(\d{1,2})\b|\b[Ss](\d{1,2})\b", re.IGNORECASE)

# Timeout in seconds for the FUSE health-check listdir.
_HEALTH_CHECK_TIMEOUT: float = 5.0


# ---------------------------------------------------------------------------
# Output schema
# ---------------------------------------------------------------------------


class ScanResult(BaseModel):
    """Summary of a completed mount scan.

    Attributes:
        files_found: Total video files discovered during the walk.
        files_added: New rows inserted into mount_index.
        files_updated: Existing rows updated (filename or filesize changed).
        files_unchanged: Existing rows whose filename and filesize matched —
            only last_seen_at was refreshed via a bulk UPDATE (no PTN re-parse).
        files_removed: Stale rows deleted (files no longer present in mount).
        duration_ms: Wall-clock time for the full scan, in milliseconds.
        errors: Number of individual file errors that were caught and skipped.
    """

    files_found: int
    files_added: int
    files_updated: int
    files_unchanged: int
    files_removed: int
    duration_ms: int
    errors: int


class ScanDirectoryResult(BaseModel):
    """Result of a targeted single-directory scan.

    Attributes:
        files_indexed: Number of files successfully indexed (inserted or updated).
            Zero for all early-exit and error paths.
        matched_dir_path: The absolute path of the directory that was actually
            scanned.  May differ from the requested directory name when fuzzy
            matching was used (e.g. ``"The.Mummy.1999.2160p..."`` for input
            ``"The Mummy"``).  ``None`` when no directory was scanned.
    """

    files_indexed: int
    matched_dir_path: str | None


# ---------------------------------------------------------------------------
# Internal parsed-file record (plain dict used for thread-safe transfer)
# ---------------------------------------------------------------------------

# Keys: filepath, filename, parsed_title, parsed_year, parsed_season,
#       parsed_episode, parsed_resolution, parsed_codec, filesize


# ---------------------------------------------------------------------------
# MountScanner
# ---------------------------------------------------------------------------


class MountScanner:
    """Stateless file-system indexer for the Zurg rclone mount.

    All methods accept an ``AsyncSession`` and delegate commit responsibility
    to the caller — methods only call ``session.flush()``.  The class holds
    no mutable state beyond what is in the database, so the module-level
    singleton is safe to share across concurrent coroutines.
    """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def is_mount_available(self) -> bool:
        """Check whether the Zurg mount directory exists and is listable.

        Uses a 5-second timeout to guard against FUSE mount hangs.  The check
        consists of two steps:
        1. Verify the configured path is a directory (``os.path.isdir``).
        2. Attempt to list the directory contents (``os.listdir``) to catch
           mounts that appear as directories but hang on reads.

        Returns:
            True when the mount is confirmed reachable, False otherwise.
        """
        mount_path = settings.paths.zurg_mount
        if not mount_path:
            logger.warning(
                "is_mount_available: paths.zurg_mount is not configured — skipping mount check"
            )
            return False

        try:
            is_dir = await asyncio.wait_for(
                asyncio.to_thread(os.path.isdir, mount_path),
                timeout=_HEALTH_CHECK_TIMEOUT,
            )
            if not is_dir:
                logger.warning("Mount not available: %s is not a directory", mount_path)
                return False

            await asyncio.wait_for(
                asyncio.to_thread(os.listdir, mount_path),
                timeout=_HEALTH_CHECK_TIMEOUT,
            )
            return True

        except TimeoutError:
            logger.warning(
                "Mount health check timed out after %.1fs — mount may be hanging: %s",
                _HEALTH_CHECK_TIMEOUT,
                mount_path,
            )
            return False
        except OSError as exc:
            logger.warning("Mount not available: %s — %s", mount_path, exc)
            return False

    async def scan(self, session: AsyncSession) -> ScanResult:
        """Walk the mount directory and upsert all video files into mount_index.

        Steps:
        1. Check mount availability; abort early with zero counts if unavailable.
        2. Walk the mount tree in a thread, parsing each filename via PTN.
        3. Upsert each file into mount_index (insert new, update existing).
        4. Delete stale rows whose ``last_seen_at`` precedes the scan start time.
        5. Return a ScanResult summary.

        Args:
            session: Active async SQLAlchemy session.  The caller owns the
                transaction and must call ``await session.commit()`` afterward.

        Returns:
            A ScanResult describing what was added, updated, removed, and any
            errors encountered.
        """
        start_time = time.monotonic()
        scan_started_at = datetime.now(timezone.utc)

        if not await self.is_mount_available():
            logger.error(
                "scan: mount unavailable at %s — skipping scan to preserve index",
                settings.paths.zurg_mount,
            )
            return ScanResult(
                files_found=0,
                files_added=0,
                files_updated=0,
                files_unchanged=0,
                files_removed=0,
                duration_ms=0,
                errors=0,
            )

        logger.info("scan: starting mount walk at %s", settings.paths.zurg_mount)

        # Pre-load known filepaths to skip stat() on already-indexed files.
        # This avoids one FUSE round-trip (~13ms) per file for the ~3000 files
        # that are already indexed and have not changed.
        known_files = await self._load_known_files(session)
        logger.debug("scan: pre-loaded %d known filepaths from DB", len(known_files))

        # --- Phase 1: walk filesystem in thread (120s timeout guards FUSE hangs) ---
        try:
            file_records, walk_errors = await asyncio.wait_for(
                asyncio.to_thread(self._scandir_walk, settings.paths.zurg_mount, known_files),
                timeout=120.0,
            )
        except TimeoutError:
            logger.error(
                "scan: filesystem walk timed out after 120s — mount may be hanging"
            )
            return ScanResult(
                files_found=0, files_added=0, files_updated=0, files_unchanged=0,
                files_removed=0, duration_ms=int((time.monotonic() - start_time) * 1000),
                errors=1,
            )

        files_found = len(file_records)
        logger.info("scan: discovered %d video files (%d errors during walk)", files_found, walk_errors)

        # --- Phase 2: upsert records into DB ---
        upsert = await self._upsert_records(
            session, file_records, scan_started_at, root_dir=settings.paths.zurg_mount
        )

        # --- Phase 3: remove stale entries ---
        stale_result = await session.execute(
            delete(MountIndex)
            .where(MountIndex.last_seen_at < scan_started_at)
        )
        files_removed = stale_result.rowcount
        await session.flush()

        duration_ms = int((time.monotonic() - start_time) * 1000)
        total_errors = walk_errors + upsert.errors

        logger.info(
            "scan complete: found=%d added=%d updated=%d unchanged=%d removed=%d errors=%d duration=%dms",
            files_found,
            upsert.added,
            upsert.updated,
            upsert.unchanged,
            files_removed,
            total_errors,
            duration_ms,
        )

        return ScanResult(
            files_found=files_found,
            files_added=upsert.added,
            files_updated=upsert.updated,
            files_unchanged=upsert.unchanged,
            files_removed=files_removed,
            duration_ms=duration_ms,
            errors=total_errors,
        )

    async def lookup(
        self,
        session: AsyncSession,
        title: str,
        season: int | None = None,
        episode: int | None = None,
    ) -> list[MountIndex]:
        """Query the mount index for files matching the given title and episode info.

        Uses a three-tier lookup strategy:

        1. **Exact match** on ``parsed_title`` (fast path).
        2. **Forward word-subsequence**: all query words must appear in
           ``parsed_title`` in order (via SQL LIKE + Python verification).
           Handles cases where the queue title omits tokens in the torrent
           filename (e.g. "Terminator Judgement Day" vs "Terminator 2 Judgement
           Day").
        3. **Reverse containment**: ``parsed_title`` is a substring of the
           search title, checked via SQLite ``instr()``.  Handles cases where
           the TMDB title is longer than the torrent's parsed_title (e.g. TMDB
           "Attack on Titan The Final Season" vs indexed "attack on titan").
           Only DB titles with 3+ words are accepted to prevent over-broad
           matches from short titles like "it" or "alien".

        Args:
            session: Active async SQLAlchemy session.
            title: The media title to search for.
            season: If provided, restrict results to this season number.
            episode: If provided, restrict results to this episode number.
                     Only meaningful when ``season`` is also provided.

        Returns:
            List of matching MountIndex rows, ordered by ``last_seen_at`` desc.
        """
        normalized = _normalize_title(title)

        def _apply_filters(stmt):
            if season is not None:
                stmt = stmt.where(MountIndex.parsed_season == season)
            if episode is not None:
                stmt = stmt.where(MountIndex.parsed_episode == episode)
            return stmt.order_by(MountIndex.last_seen_at.desc())

        # Try exact match first (fast path, prevents false positives).
        stmt = select(MountIndex).where(MountIndex.parsed_title == normalized)
        result = await session.execute(_apply_filters(stmt))
        matches = list(result.scalars().all())
        if matches:
            return matches

        # Fallback: word-subsequence via LIKE — all query words must appear in order.
        # Require at least 2 words to prevent overly broad matches (e.g. "dark" matching
        # "the dark knight").
        words = normalized.split()
        if len(words) < 2:
            return []
        like_pattern = "%" + "%".join(words) + "%"
        stmt = select(MountIndex).where(MountIndex.parsed_title.like(like_pattern))
        result = await session.execute(_apply_filters(stmt))
        matches = list(result.scalars().all())
        if matches:
            # Filter in Python: LIKE "%word1%word2%" matches substrings within
            # words (e.g. "%the%" matches inside "theater"), so verify whole-word
            # subsequence here.
            verified = []
            query_words = words
            for m in matches:
                target_words = (m.parsed_title or "").split()
                if _is_word_subsequence(query_words, target_words):
                    verified.append(m)
            if verified:
                logger.info(
                    "lookup: exact match failed for %r, word-subsequence found %d result(s)",
                    title, len(verified),
                )
            return verified

        # Tier 3: reverse containment — the DB's parsed_title is a substring of the
        # search title.  This handles cases where the TMDB title is longer than the
        # torrent's parsed_title (e.g. TMDB "Kimi no Na wa Your Name" vs parsed
        # "your name").  Uses SQLite instr() so the match is pure SQL without raw text.
        # Require 3+ words in the DB parsed_title to avoid broad matches from short
        # tokens like "it" or "alien".
        stmt = (
            select(MountIndex)
            .where(MountIndex.parsed_title.is_not(None))
            .where(func.instr(literal(normalized), MountIndex.parsed_title) > 0)
            .limit(50)
        )
        result = await session.execute(_apply_filters(stmt))
        matches = list(result.scalars().all())
        if matches:
            verified = []
            for m in matches:
                db_words = (m.parsed_title or "").split()
                if len(db_words) < 3:
                    continue
                # Confirm every DB word appears in the search title in order.
                if _is_word_subsequence(db_words, words):
                    verified.append(m)
            if verified:
                logger.info(
                    "lookup: exact+subsequence failed for %r, reverse containment found %d result(s)",
                    title,
                    len(verified),
                )
            return verified

        return []

    async def lookup_by_filepath(
        self, session: AsyncSession, filepath: str
    ) -> MountIndex | None:
        """Look up a mount index entry by its exact filepath.

        Args:
            session: Active async SQLAlchemy session.
            filepath: The full absolute path to match exactly.

        Returns:
            The matching MountIndex entry, or None if not found.
        """
        result = await session.execute(
            select(MountIndex).where(MountIndex.filepath == filepath)
        )
        return result.scalar_one_or_none()

    async def get_index_stats(self, session: AsyncSession) -> dict[str, int]:
        """Return aggregate counts from the mount index.

        Runs three lightweight COUNT queries: total, movies (season IS NULL),
        and episodes (season IS NOT NULL).

        Args:
            session: Active async SQLAlchemy session.

        Returns:
            Dict with keys:
            - ``total_files``: all rows in mount_index.
            - ``movies``: rows where ``parsed_season`` IS NULL.
            - ``episodes``: rows where ``parsed_season`` IS NOT NULL.
        """
        total_result = await session.execute(
            select(func.count(MountIndex.id))
        )
        total = int(total_result.scalar() or 0)

        movies_result = await session.execute(
            select(func.count(MountIndex.id)).where(
                MountIndex.parsed_season.is_(None)
            )
        )
        movies = int(movies_result.scalar() or 0)

        episodes_result = await session.execute(
            select(func.count(MountIndex.id)).where(
                MountIndex.parsed_season.isnot(None)
            )
        )
        episodes = int(episodes_result.scalar() or 0)

        return {
            "total_files": total,
            "movies": movies,
            "episodes": episodes,
        }

    async def clear_index(self, session: AsyncSession) -> int:
        """Delete all entries from the mount_index table.

        Intended for manual reset via the admin API.  Normal scan operations
        should never call this — stale files are removed incrementally by scan().

        Args:
            session: Active async SQLAlchemy session.

        Returns:
            Number of rows deleted.
        """
        result = await session.execute(
            delete(MountIndex)
        )
        count = result.rowcount
        await session.flush()

        logger.warning("clear_index: deleted %d rows from mount_index", count)
        return count

    async def scan_directory(
        self, session: AsyncSession, directory_name: str
    ) -> ScanDirectoryResult:
        """Index all video files under a single named subdirectory of the mount.

        Unlike ``scan()``, this method is additive — it never deletes stale
        rows.  It is intended for targeted refreshes (e.g. after a new torrent
        is added to RD) rather than full periodic sweeps.

        When the exact directory is not found, a fuzzy word-subsequence match
        is attempted against all subdirectories of the mount root.  The actual
        directory path used (which may differ from ``directory_name`` after
        fuzzy matching) is returned in the result so the caller can use it for
        path-prefix based lookups.

        Args:
            session: Active async SQLAlchemy session.  The caller owns the
                transaction and must call ``await session.commit()`` afterward.
            directory_name: Basename of the subdirectory inside the Zurg mount
                root (e.g. ``"__all__"`` or a specific show folder).

        Returns:
            A ``ScanDirectoryResult`` with ``files_indexed`` set to the number
            of files successfully indexed (inserted or updated) and
            ``matched_dir_path`` set to the absolute path of the scanned
            directory (after any fuzzy match).  Both values are ``0``/``None``
            for all early-exit and error paths.
        """
        _empty = ScanDirectoryResult(files_indexed=0, matched_dir_path=None)

        mount_root = settings.paths.zurg_mount
        if not mount_root:
            logger.warning("scan_directory: zurg_mount is not configured, skipping")
            return _empty

        # ------------------------------------------------------------------
        # Single-file torrent handling
        # If directory_name has a video extension it refers to a file that
        # lives directly in the mount root, not a subdirectory.
        # ------------------------------------------------------------------
        _ext = os.path.splitext(directory_name)[1].lower()
        if _ext in VIDEO_EXTENSIONS:
            result = await self._scan_single_file(session, directory_name)
            if result.files_indexed > 0:
                return result
            # Single-file not found in mount root — fall through to directory
            # scan using the stem (Zurg may wrap single-file torrents in a
            # directory named after the torrent).
            original_name = directory_name
            directory_name = os.path.splitext(directory_name)[0]
            logger.info(
                "scan_directory: single-file scan found nothing for %r, trying directory scan with stem %r",
                original_name,
                directory_name,
            )

        dir_path = os.path.join(mount_root, directory_name, "")

        try:
            exists = await asyncio.wait_for(
                asyncio.to_thread(os.path.isdir, dir_path),
                timeout=_HEALTH_CHECK_TIMEOUT,
            )
        except TimeoutError:
            logger.warning(
                "scan_directory: existence check timed out for %r — skipping",
                dir_path,
            )
            return _empty

        if not exists:
            logger.warning(
                "scan_directory: directory %s not found — attempting fuzzy match",
                dir_path,
            )
            normalized_input = _normalize_title(directory_name)

            def _find_fuzzy_match() -> str | None:
                try:
                    scanner = os.scandir(mount_root)
                except OSError as exc:
                    logger.warning(
                        "scan_directory: cannot list mount root %s — %s",
                        mount_root,
                        exc,
                    )
                    return None
                input_words = normalized_input.split()
                if not input_words:
                    return None
                # (normalized_name, original_name, full_path)
                candidates: list[tuple[str, str, str]] = []
                with scanner:
                    for entry in scanner:
                        if not entry.is_dir(follow_symlinks=False):
                            continue
                        norm = _normalize_title(entry.name)
                        norm_words = norm.split()
                        if not _is_word_subsequence(input_words, norm_words):
                            continue
                        candidates.append((norm, entry.name, entry.path))
                if not candidates:
                    return None
                if len(candidates) > 1:
                    logger.warning(
                        "scan_directory: %d fuzzy candidates for %r: %s",
                        len(candidates),
                        directory_name,
                        [c[1] for c in candidates],
                    )
                # Pick shortest normalized name (closest match); break ties alphabetically
                candidates.sort(key=lambda c: (len(c[0]), c[0]))
                return candidates[0][2]

            try:
                matched_path = await asyncio.wait_for(
                    asyncio.to_thread(_find_fuzzy_match),
                    timeout=_HEALTH_CHECK_TIMEOUT,
                )
            except TimeoutError:
                logger.warning(
                    "scan_directory: fuzzy match timed out for %r — skipping",
                    directory_name,
                )
                return _empty

            if matched_path is None:
                logger.warning(
                    "scan_directory: no fuzzy match found for %r in %s",
                    directory_name,
                    mount_root,
                )
                return _empty

            logger.info(
                "scan_directory: fuzzy matched %r -> %s",
                directory_name,
                matched_path,
            )
            dir_path = matched_path

        scan_timestamp = datetime.now(timezone.utc)

        try:
            # Targeted scan covers at most ~20 files — stat overhead is negligible,
            # so pass an empty known_files dict (no pre-load needed here).
            file_records, walk_errors = await asyncio.wait_for(
                asyncio.to_thread(self._scandir_walk, dir_path, {}),
                timeout=30.0,
            )
        except TimeoutError:
            logger.warning(
                "scan_directory: walk timed out after 30s for %s", dir_path,
            )
            return _empty

        if walk_errors:
            logger.warning(
                "scan_directory: %d errors during walk of %s", walk_errors, dir_path
            )

        upsert = await self._upsert_records(
            session, file_records, scan_timestamp, root_dir=dir_path
        )

        total_indexed = upsert.added + upsert.updated + upsert.unchanged

        logger.info(
            "scan_directory: indexed %d files from %s (added=%d updated=%d unchanged=%d errors=%d)",
            total_indexed,
            dir_path,
            upsert.added,
            upsert.updated,
            upsert.unchanged,
            upsert.errors,
        )
        return ScanDirectoryResult(files_indexed=total_indexed, matched_dir_path=dir_path)

    async def lookup_by_path_prefix(
        self,
        session: AsyncSession,
        path_prefix: str,
        season: int | None = None,
        episode: int | None = None,
    ) -> list[MountIndex]:
        """Query the mount index for all files whose filepath begins with path_prefix.

        This is a fallback for cases where PTN parses individual video file names
        differently from the queue item title (e.g. BluRay disc rips with numeric
        filenames, or episodes named by episode title instead of show title).
        After a successful ``scan_directory`` the directory path is known, so
        all indexed files beneath it can be retrieved directly by path prefix
        without relying on parsed metadata.

        The prefix is normalised to end with ``/`` before the LIKE query so
        that a prefix of ``/mnt/The.Mummy.1999.../`` cannot accidentally match
        a sibling directory like ``/mnt/The.Mummy.Returns.../``.

        Args:
            session: Active async SQLAlchemy session.
            path_prefix: Absolute directory path to use as the prefix filter.
                A trailing ``/`` is appended if not already present.
            season: If provided, restrict results to this season number.
            episode: If provided, restrict results to this episode number.
                Only meaningful when ``season`` is also provided.

        Returns:
            List of matching MountIndex rows, ordered by ``last_seen_at`` desc.
        """
        if not path_prefix.endswith("/"):
            path_prefix = path_prefix + "/"

        # Escape LIKE wildcards so directory names containing literal '%' or '_'
        # are matched as ordinary characters rather than SQL pattern tokens.
        escaped = path_prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        like_pattern = escaped + "%"
        stmt = select(MountIndex).where(
            MountIndex.filepath.like(like_pattern, escape="\\")
        )
        if season is not None:
            stmt = stmt.where(MountIndex.parsed_season == season)
        if episode is not None:
            stmt = stmt.where(MountIndex.parsed_episode == episode)
        stmt = stmt.order_by(MountIndex.last_seen_at.desc())

        result = await session.execute(stmt)
        matches = list(result.scalars().all())
        if matches:
            logger.info(
                "lookup_by_path_prefix: found %d result(s) for prefix %r (season=%s episode=%s)",
                len(matches),
                path_prefix,
                season,
                episode,
            )
        return matches

    # ------------------------------------------------------------------
    # Private helpers — single-file torrents
    # ------------------------------------------------------------------

    async def _scan_single_file(
        self, session: AsyncSession, filename: str
    ) -> ScanDirectoryResult:
        """Index a single video file that lives directly in the mount root.

        Used when the RD torrent name includes a video extension (single-file
        torrent) rather than being a directory name.  Tries an exact path
        lookup first, then falls back to fuzzy word-subsequence matching
        against all non-directory entries in the mount root.

        Args:
            session: Active async SQLAlchemy session.
            filename: The bare filename (with extension) as returned by the RD
                API, e.g.
                ``"Frieren Beyond Journey's End (2023) S02E01 (...).mkv"``.

        Returns:
            A ``ScanDirectoryResult`` with ``files_indexed=1`` on success and
            ``matched_dir_path=None`` (there is no directory to report).
            Returns ``(files_indexed=0, matched_dir_path=None)`` on failure.
        """
        _empty = ScanDirectoryResult(files_indexed=0, matched_dir_path=None)
        mount_root = settings.paths.zurg_mount
        if not mount_root:
            logger.warning("_scan_single_file: zurg_mount is not configured, skipping")
            return _empty
        file_path = os.path.join(mount_root, filename)

        # --- Exact path check ---
        try:
            exists = await asyncio.wait_for(
                asyncio.to_thread(os.path.isfile, file_path),
                timeout=_HEALTH_CHECK_TIMEOUT,
            )
        except TimeoutError:
            logger.warning(
                "_scan_single_file: existence check timed out for %r — skipping",
                file_path,
            )
            return _empty

        if not exists:
            logger.warning(
                "_scan_single_file: file %r not found — attempting fuzzy match",
                file_path,
            )
            normalized_input = _normalize_title(os.path.splitext(filename)[0])
            input_words = normalized_input.split()
            if not input_words:
                return _empty

            def _find_fuzzy_file() -> str | None:
                try:
                    scanner = os.scandir(mount_root)
                except OSError as exc:
                    logger.warning(
                        "_scan_single_file: cannot list mount root %s — %s",
                        mount_root,
                        exc,
                    )
                    return None

                candidates: list[tuple[str, str, str]] = []
                with scanner:
                    for entry in scanner:
                        if entry.is_dir(follow_symlinks=False):
                            continue
                        entry_ext = os.path.splitext(entry.name)[1].lower()
                        if entry_ext not in VIDEO_EXTENSIONS:
                            continue
                        stem = os.path.splitext(entry.name)[0]
                        norm = _normalize_title(stem)
                        norm_words = norm.split()
                        if not _is_word_subsequence(input_words, norm_words):
                            continue
                        candidates.append((norm, entry.name, entry.path))

                if not candidates:
                    return None
                if len(candidates) > 1:
                    logger.warning(
                        "_scan_single_file: %d fuzzy candidates for %r: %s",
                        len(candidates),
                        filename,
                        [c[1] for c in candidates],
                    )
                # Shortest normalized name wins; break ties alphabetically.
                candidates.sort(key=lambda c: (len(c[0]), c[0]))
                return candidates[0][2]

            try:
                matched_path: str | None = await asyncio.wait_for(
                    asyncio.to_thread(_find_fuzzy_file),
                    timeout=_HEALTH_CHECK_TIMEOUT,
                )
            except TimeoutError:
                logger.warning(
                    "_scan_single_file: fuzzy match timed out for %r — skipping",
                    filename,
                )
                return _empty

            if matched_path is None:
                logger.warning(
                    "_scan_single_file: no fuzzy match found for %r in %s",
                    filename,
                    mount_root,
                )
                return _empty

            logger.info(
                "_scan_single_file: fuzzy matched %r -> %s",
                filename,
                matched_path,
            )
            file_path = matched_path

        # --- Parse and upsert the single file ---
        actual_filename = os.path.basename(file_path)

        def _stat_file() -> int | None:
            try:
                return os.stat(file_path, follow_symlinks=True).st_size
            except OSError as exc:
                logger.warning(
                    "_scan_single_file: cannot stat %r — %s", file_path, exc
                )
                return None

        try:
            filesize: int | None = await asyncio.wait_for(
                asyncio.to_thread(_stat_file),
                timeout=_HEALTH_CHECK_TIMEOUT,
            )
        except TimeoutError:
            logger.warning(
                "_scan_single_file: stat timed out for %r — using None filesize",
                file_path,
            )
            filesize = None

        walk_entry = WalkEntry(
            filepath=file_path,
            filename=actual_filename,
            filesize=filesize,
            parent_dir=os.path.dirname(file_path),
        )
        scan_timestamp = datetime.now(timezone.utc)
        upsert = await self._upsert_records(
            session, [walk_entry], scan_timestamp, root_dir=settings.paths.zurg_mount
        )

        total_indexed = upsert.added + upsert.updated + upsert.unchanged
        logger.info(
            "_scan_single_file: indexed %d file(s) from %s (added=%d updated=%d unchanged=%d errors=%d)",
            total_indexed,
            file_path,
            upsert.added,
            upsert.updated,
            upsert.unchanged,
            upsert.errors,
        )
        return ScanDirectoryResult(files_indexed=total_indexed, matched_dir_path=None)

    # ------------------------------------------------------------------
    # Private helpers — async DB
    # ------------------------------------------------------------------

    async def _load_known_files(self, session: AsyncSession) -> dict[str, int | None]:
        """Pre-load filepath -> filesize map from mount_index for stat-skip optimisation.

        Called once at the start of a full scan so that the walk phase can skip
        the expensive FUSE ``stat()`` call for files already present in the DB
        with a valid (non-None) filesize.

        Args:
            session: Active async SQLAlchemy session.

        Returns:
            Dict mapping absolute filepath strings to their stored filesize
            (which may be ``None`` when the previous stat failed).
        """
        result = await session.execute(
            select(MountIndex.filepath, MountIndex.filesize)
        )
        return {row.filepath: row.filesize for row in result.all()}

    async def _upsert_records(
        self,
        session: AsyncSession,
        records: list[WalkEntry],
        scan_timestamp: datetime,
        root_dir: str = "",
    ) -> UpsertResult:
        """Batch-upsert file records into mount_index with skip-unchanged optimisation.

        Fetches only ``filepath``, ``filename``, and ``filesize`` columns for
        comparison first.  Files whose filename and filesize are identical to
        the stored values are marked unchanged and receive only a bulk
        ``last_seen_at`` timestamp update — PTN is never re-run on them.  Only
        new or changed files are parsed and fully upserted.

        Args:
            session: Active async SQLAlchemy session.
            records: List of ``WalkEntry`` namedtuples as produced by
                ``_scandir_walk``.
            scan_timestamp: Timestamp to write into ``last_seen_at`` for every
                touched row.
            root_dir: Walk root used for ``_extract_season_from_path`` fallback.
                Pass an empty string to skip the path-based season inference.

        Returns:
            An ``UpsertResult`` with ``added``, ``updated``, ``unchanged``, and
            ``errors`` counts.
        """
        if not records:
            return UpsertResult(added=0, updated=0, unchanged=0, errors=0)

        _BATCH_SIZE = 500

        # Build filepath -> WalkEntry map.
        record_map: dict[str, WalkEntry] = {r.filepath: r for r in records}
        all_filepaths = list(record_map.keys())

        # Phase 1: Fetch ONLY comparison columns (not full ORM objects).
        existing_map: dict[str, tuple[str, int | None]] = {}
        for i in range(0, len(all_filepaths), _BATCH_SIZE):
            batch = all_filepaths[i : i + _BATCH_SIZE]
            result = await session.execute(
                select(
                    MountIndex.filepath,
                    MountIndex.filename,
                    MountIndex.filesize,
                ).where(MountIndex.filepath.in_(batch))
            )
            for row in result.all():
                existing_map[row.filepath] = (row.filename, row.filesize)

        # Phase 2: Classify records into unchanged vs new/changed.
        unchanged_filepaths: list[str] = []
        changed_or_new: list[WalkEntry] = []

        for filepath, entry in record_map.items():
            existing = existing_map.get(filepath)
            if existing is not None:
                old_filename, old_filesize = existing
                # Both match → skip re-parse; None == None is True in Python
                # which is correct: if stat failed both times treat as unchanged.
                if old_filename == entry.filename and old_filesize == entry.filesize:
                    unchanged_filepaths.append(filepath)
                    continue
            changed_or_new.append(entry)

        # Phase 3: Batch-update last_seen_at for unchanged files.
        files_unchanged = len(unchanged_filepaths)
        for i in range(0, len(unchanged_filepaths), _BATCH_SIZE):
            batch = unchanged_filepaths[i : i + _BATCH_SIZE]
            await session.execute(
                update(MountIndex)
                .where(MountIndex.filepath.in_(batch))
                .values(last_seen_at=scan_timestamp)
            )

        # Phase 4: Parse and upsert changed/new files.
        # Fetch full ORM objects only for files that exist and have changed.
        changed_filepaths = [
            e.filepath for e in changed_or_new if e.filepath in existing_map
        ]
        changed_orm_map: dict[str, MountIndex] = {}
        for i in range(0, len(changed_filepaths), _BATCH_SIZE):
            batch = changed_filepaths[i : i + _BATCH_SIZE]
            result = await session.execute(
                select(MountIndex).where(MountIndex.filepath.in_(batch))
            )
            for row in result.scalars().all():
                changed_orm_map[row.filepath] = row

        files_added = 0
        files_updated = 0
        upsert_errors = 0

        for entry in changed_or_new:
            try:
                # Parse filename now (deferred from walk phase).
                # Pass parent_dir so that episode-titled files (e.g.
                # "S02E01 - Beast Titan.mkv") can inherit the show title from
                # their parent directory name ("Attack on Titan S02").
                parsed = _parse_filename(entry.filename, parent_dir=entry.parent_dir)

                # Apply season-from-path fallback.
                if parsed.get("season") is None and root_dir:
                    dir_season = _extract_season_from_path(entry.parent_dir, root_dir)
                    if dir_season is not None:
                        parsed["season"] = dir_season

                existing_obj = changed_orm_map.get(entry.filepath)
                if existing_obj is None:
                    # New file — INSERT.
                    new_entry = MountIndex(
                        filepath=entry.filepath,
                        filename=entry.filename,
                        parsed_title=parsed.get("title"),
                        parsed_year=parsed.get("year"),
                        parsed_season=parsed.get("season"),
                        parsed_episode=parsed.get("episode"),
                        parsed_resolution=parsed.get("resolution"),
                        parsed_codec=parsed.get("codec"),
                        filesize=entry.filesize,
                        last_seen_at=scan_timestamp,
                    )
                    session.add(new_entry)
                    files_added += 1
                else:
                    # Changed file — UPDATE all fields.
                    existing_obj.filename = entry.filename
                    existing_obj.parsed_title = parsed.get("title")
                    existing_obj.parsed_year = parsed.get("year")
                    existing_obj.parsed_season = parsed.get("season")
                    existing_obj.parsed_episode = parsed.get("episode")
                    existing_obj.parsed_resolution = parsed.get("resolution")
                    existing_obj.parsed_codec = parsed.get("codec")
                    existing_obj.filesize = entry.filesize
                    existing_obj.last_seen_at = scan_timestamp
                    files_updated += 1
            except Exception as exc:  # noqa: BLE001 — per-file resilience
                logger.warning(
                    "_upsert_records: DB upsert failed for %r — %s",
                    entry.filepath,
                    exc,
                )
                upsert_errors += 1

        await session.flush()
        return UpsertResult(
            added=files_added,
            updated=files_updated,
            unchanged=files_unchanged,
            errors=upsert_errors,
        )

    # ------------------------------------------------------------------
    # Private helpers (run in thread)
    # ------------------------------------------------------------------

    def _scandir_walk(
        self,
        dir_path: str,
        known_files: dict[str, int | None] | None = None,
    ) -> tuple[list[WalkEntry], int]:
        """Recursively walk a directory tree using ``os.scandir``.

        Prefer ``os.scandir`` over ``os.walk`` because ``DirEntry.stat()``
        reuses inode metadata already fetched by the underlying ``readdir``
        syscall on Linux, avoiding one extra FUSE round-trip per file compared
        to a separate ``os.path.getsize`` call.

        This method is designed to run inside ``asyncio.to_thread`` — it is
        synchronous and may block on I/O.  PTN filename parsing is intentionally
        deferred to the async ``_upsert_records`` phase so that unchanged files
        (same filename + filesize) never trigger a PTN re-parse.

        When ``known_files`` is provided, files already present in the DB with
        a non-None filesize skip the expensive FUSE ``stat()`` call entirely —
        the stored size is reused instead.  Only new files or files whose
        stored filesize is ``None`` (previous stat failure) are stat'd.

        Args:
            dir_path: Absolute path to start the recursive walk from.
            known_files: Optional filepath -> filesize mapping pre-loaded from
                mount_index.  Pass ``None`` or an empty dict to stat every
                file unconditionally (backward-compatible default).

        Returns:
            A 2-tuple ``(records, error_count)`` where ``records`` is a list
            of ``WalkEntry`` namedtuples containing only raw filesystem
            metadata (filepath, filename, filesize, parent_dir), and
            ``error_count`` is the number of individual file errors encountered
            (file skipped but scan continues).
        """
        if known_files is None:
            known_files = {}

        records: list[WalkEntry] = []
        error_count = 0
        stack: list[str] = [dir_path]

        while stack:
            current_dir = stack.pop()
            try:
                entries = list(os.scandir(current_dir))
            except OSError as exc:
                logger.warning("_scandir_walk: cannot scandir %r — %s", current_dir, exc)
                error_count += 1
                continue

            for entry in entries:
                # Fast path: if this path is already in the DB, we know it's
                # a video file (not a directory).  Skip both the is_dir()
                # check and the stat() call — on FUSE mounts, is_dir() can
                # trigger an implicit stat when d_type is DT_UNKNOWN.
                known_size = known_files.get(entry.path)
                if known_size is not None:
                    filesize: int | None = known_size
                    records.append(
                        WalkEntry(
                            filepath=entry.path,
                            filename=entry.name,
                            filesize=filesize,
                            parent_dir=current_dir,
                        )
                    )
                    continue

                if entry.is_dir(follow_symlinks=False):
                    if not self._should_skip_dir(entry.name):
                        stack.append(entry.path)
                    continue

                # Regular file (or symlink to file — follow it).
                filename = entry.name

                # Skip hidden files.
                if filename.startswith("."):
                    continue

                ext = os.path.splitext(filename)[1].lower()
                if ext not in VIDEO_EXTENSIONS:
                    continue

                # New file or previous stat failure (filesize=None) — must stat.
                try:
                    filesize = entry.stat(follow_symlinks=True).st_size
                except OSError as exc:
                    logger.warning(
                        "_scandir_walk: cannot stat %r — %s", entry.path, exc
                    )
                    filesize = None
                    error_count += 1

                records.append(
                    WalkEntry(
                        filepath=entry.path,
                        filename=filename,
                        filesize=filesize,
                        parent_dir=current_dir,
                    )
                )

        return records, error_count

    @staticmethod
    def _should_skip_dir(dirname: str) -> bool:
        """Return True when a directory should be excluded from the walk.

        Skips hidden directories, known macOS/Synology noise directories, and
        Trash directories.

        Args:
            dirname: The directory basename to evaluate.

        Returns:
            True if the directory should be excluded from the walk.
        """
        if dirname.startswith("."):
            return True
        if dirname in _SKIP_DIRS:
            return True
        if dirname.startswith(_TRASH_PREFIX):
            return True
        return False


# ---------------------------------------------------------------------------
# PTN parsing helper
# ---------------------------------------------------------------------------


def _extract_season_from_path(current_dir: str, root_dir: str) -> int | None:
    """Infer a season number from path components between root_dir and current_dir.

    Checks each directory component of the relative path from ``root_dir`` to
    ``current_dir`` for common season directory naming patterns such as
    "Season 4", "S04", or "Staffel 2".

    Args:
        current_dir: Absolute path of the directory currently being scanned.
        root_dir: Absolute path of the walk root (the ``dir_path`` argument
            passed to ``_scandir_walk``).

    Returns:
        The parsed season number as an integer if a pattern matched, else None.
    """
    try:
        rel = os.path.relpath(current_dir, root_dir)
    except ValueError:
        # os.path.relpath raises ValueError on Windows when paths are on
        # different drives; guard defensively even on Linux.
        return None

    for component in rel.split(os.sep):
        match = _DIR_SEASON_RE.search(component)
        if match:
            # Two alternative groups: group(1) for "season/staffel", group(2) for "S##"
            return int(match.group(1) or match.group(2))
    return None


def _has_meaningful_title(normalized_title: str) -> bool:
    """Return True when a normalised title looks like a real show/movie title.

    Returns False when the title appears to be only an episode marker (e.g.
    ``"s02e01 beast titan"`` parsed from ``"S02E01 - Beast Titan.mkv"``),
    is empty, or is too short to be useful.  In those cases the caller should
    fall back to the parent directory name for the title.

    Args:
        normalized_title: A title string already processed by
            ``_normalize_title`` (lowercase, alphanumeric + spaces only).

    Returns:
        True if the title is meaningful, False if a directory-name fallback
        should be attempted.
    """
    if not normalized_title:
        return False
    # Very short titles (< 3 chars) are not useful for matching.
    if len(normalized_title) < 3:
        return False
    # Title starts with an episode marker like "s02e01" or "e01".
    if _EPISODE_MARKER_RE.match(normalized_title):
        return False
    return True


def _parse_filename(filename: str, parent_dir: str | None = None) -> dict[str, Any]:
    """Parse a torrent filename using PTN, returning extracted metadata.

    Extracts title, year, season, episode, resolution, and codec from the
    filename.  PTN failures are caught and the filename stem is used as the
    title fallback so one bad file never aborts the entire scan.

    When the filename-derived title is not meaningful (e.g. an episode-titled
    file like ``"S02E01 - Beast Titan.mkv"`` whose PTN title becomes
    ``"s02e01 beast titan"``), the parent directory basename is PTN-parsed to
    extract the real show title (e.g. ``"Attack on Titan S02"`` →
    ``"attack on titan"``).  Season and episode numbers from the filename
    parse are always preferred over directory-derived values because they are
    more granular.

    Args:
        filename: The raw filename string (including extension).
        parent_dir: Optional absolute path of the directory containing this
            file.  When provided and the filename title is not meaningful, the
            directory basename is used as a title fallback.

    Returns:
        Dict with keys ``title``, ``year``, ``season``, ``episode``,
        ``resolution``, ``codec``.  Missing fields are None.
    """
    try:
        parsed = PTN.parse(filename)
    except Exception as exc:  # noqa: BLE001 — PTN may raise anything
        logger.warning("_parse_filename: PTN failed on %r — %s", filename, exc)
        stem = os.path.splitext(filename)[0]
        return {
            "title": _normalize_title(stem),
            "year": None,
            "season": None,
            "episode": None,
            "resolution": None,
            "codec": None,
        }

    raw_title: str = parsed.get("title") or os.path.splitext(filename)[0]

    season = parsed.get("season")
    episode = parsed.get("episode")

    # Post-PTN correction: anime "TV-N - NN" naming convention.
    # PTN misparses these (e.g. season=20 from "1920x1080"), so override
    # with the explicit TV-N season and episode when the pattern matches.
    tv_match = _TV_N_EP_RE.search(os.path.splitext(filename)[0])
    if tv_match:
        season = int(tv_match.group(1))
        episode = int(tv_match.group(2))

    # Fallback: anime "[Group] Title - NN" naming convention.
    # PTN often fails to extract episode numbers from these filenames.
    if episode is None:
        anime_match = _ANIME_DASH_EP_RE.search(os.path.splitext(filename)[0])
        if anime_match:
            episode = int(anime_match.group(1))

    # Fallback: leading number convention for season pack episodes.
    # "28. Prelude to the Impending Fight.mp4" → episode 28
    if episode is None:
        leading_match = _LEADING_EP_RE.match(os.path.splitext(filename)[0])
        if leading_match:
            episode = int(leading_match.group(1))

    normalized_title = _normalize_title(raw_title)

    # Directory-name title fallback for season pack episode files.
    # When episode files are named by episode title rather than show title
    # (e.g. "S02E01 - Beast Titan.mkv"), PTN produces a title like
    # "s02e01 beast titan" which cannot match a lookup for "Attack on Titan".
    # If the parent directory is available and the filename title is not
    # meaningful, derive the show title from the directory basename instead.
    #
    # PTN does not reliably strip season markers from directory names like
    # "Attack on Titan S02" → PTN title = "Attack on Titan S02" (unchanged).
    # So we strip known season/episode markers from the basename before
    # normalising, using _DIR_SEASON_RE to detect and remove the suffix.
    if not _has_meaningful_title(normalized_title) and parent_dir:
        dir_basename = os.path.basename(parent_dir.rstrip("/"))
        if dir_basename:
            # Strip trailing season markers (e.g. "S02", "Season 2", "Staffel 3").
            # _DIR_SEASON_RE matches at word boundaries so we replace with space
            # and re-strip; multiple markers are handled by the loop.
            dir_title_raw = _DIR_SEASON_RE.sub(" ", dir_basename)
            dir_title = _normalize_title(dir_title_raw)
            if _has_meaningful_title(dir_title):
                logger.debug(
                    "_parse_filename: filename title %r not meaningful, "
                    "using parent dir title %r (dir=%r)",
                    normalized_title,
                    dir_title,
                    dir_basename,
                )
                normalized_title = dir_title

    return {
        "title": normalized_title,
        "year": parsed.get("year"),
        "season": season,
        "episode": episode,
        "resolution": parsed.get("resolution"),
        "codec": parsed.get("codec"),
    }


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

mount_scanner = MountScanner()
