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
from typing import Any

import PTN
from pydantic import BaseModel
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.models.mount_index import MountIndex

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Title normalisation
# ---------------------------------------------------------------------------

_NON_ALNUM_RE = re.compile(r"[^a-z0-9 ]+")
_MULTI_SPACE_RE = re.compile(r" {2,}")


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
        files_updated: Existing rows updated (last_seen_at / filesize refreshed).
        files_removed: Stale rows deleted (files no longer present in mount).
        duration_ms: Wall-clock time for the full scan, in milliseconds.
        errors: Number of individual file errors that were caught and skipped.
    """

    files_found: int
    files_added: int
    files_updated: int
    files_removed: int
    duration_ms: int
    errors: int


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
                files_removed=0,
                duration_ms=0,
                errors=0,
            )

        logger.info("scan: starting mount walk at %s", settings.paths.zurg_mount)

        # --- Phase 1: walk filesystem in thread (120s timeout guards FUSE hangs) ---
        try:
            file_records, walk_errors = await asyncio.wait_for(
                asyncio.to_thread(self._scandir_walk, settings.paths.zurg_mount),
                timeout=120.0,
            )
        except TimeoutError:
            logger.error(
                "scan: filesystem walk timed out after 120s — mount may be hanging"
            )
            return ScanResult(
                files_found=0, files_added=0, files_updated=0,
                files_removed=0, duration_ms=int((time.monotonic() - start_time) * 1000),
                errors=1,
            )

        files_found = len(file_records)
        logger.info("scan: discovered %d video files (%d errors during walk)", files_found, walk_errors)

        # --- Phase 2: upsert records into DB ---
        files_added, files_updated, upsert_errors = await self._upsert_records(
            session, file_records, scan_started_at
        )

        # --- Phase 3: remove stale entries ---
        stale_result = await session.execute(
            delete(MountIndex)
            .where(MountIndex.last_seen_at < scan_started_at)
            .returning(MountIndex.id)
        )
        files_removed = len(stale_result.fetchall())
        await session.flush()

        duration_ms = int((time.monotonic() - start_time) * 1000)
        total_errors = walk_errors + upsert_errors

        logger.info(
            "scan complete: found=%d added=%d updated=%d removed=%d errors=%d duration=%dms",
            files_found,
            files_added,
            files_updated,
            files_removed,
            total_errors,
            duration_ms,
        )

        return ScanResult(
            files_found=files_found,
            files_added=files_added,
            files_updated=files_updated,
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

        Performs a case-insensitive substring match on ``parsed_title``.
        Optionally filters by exact ``parsed_season`` and ``parsed_episode``.
        This is the fast DB-only check used before every scrape attempt.

        Args:
            session: Active async SQLAlchemy session.
            title: The media title to search for (matched as a substring).
            season: If provided, restrict results to this season number.
            episode: If provided, restrict results to this episode number.
                     Only meaningful when ``season`` is also provided.

        Returns:
            List of matching MountIndex rows, ordered by ``last_seen_at`` desc.
        """
        normalized = _normalize_title(title)
        stmt = select(MountIndex).where(
            MountIndex.parsed_title.ilike(f"%{normalized}%")
        )

        if season is not None:
            stmt = stmt.where(MountIndex.parsed_season == season)

        if episode is not None:
            stmt = stmt.where(MountIndex.parsed_episode == episode)

        stmt = stmt.order_by(MountIndex.last_seen_at.desc())

        result = await session.execute(stmt)
        return list(result.scalars().all())

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
            delete(MountIndex).returning(MountIndex.id)
        )
        count = len(result.fetchall())
        await session.flush()

        logger.warning("clear_index: deleted %d rows from mount_index", count)
        return count

    async def scan_directory(self, session: AsyncSession, directory_name: str) -> int:
        """Index all video files under a single named subdirectory of the mount.

        Unlike ``scan()``, this method is additive — it never deletes stale
        rows.  It is intended for targeted refreshes (e.g. after a new torrent
        is added to RD) rather than full periodic sweeps.

        Args:
            session: Active async SQLAlchemy session.  The caller owns the
                transaction and must call ``await session.commit()`` afterward.
            directory_name: Basename of the subdirectory inside the Zurg mount
                root (e.g. ``"__all__"`` or a specific show folder).

        Returns:
            Number of files successfully indexed (inserted or updated).
            Returns 0 if the directory does not exist or the existence check
            times out.
        """
        dir_path = os.path.join(settings.paths.zurg_mount, directory_name, "")

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
            return 0

        if not exists:
            logger.warning(
                "scan_directory: directory %s not found — attempting fuzzy match",
                dir_path,
            )
            normalized_input = _normalize_title(directory_name)
            mount_root = settings.paths.zurg_mount

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
                # (normalized_name, original_name, full_path)
                candidates: list[tuple[str, str, str]] = []
                with scanner:
                    for entry in scanner:
                        if not entry.is_dir(follow_symlinks=False):
                            continue
                        norm = _normalize_title(entry.name)
                        if not norm.startswith(normalized_input):
                            continue
                        # Word-boundary check: reject mid-word prefix matches
                        # (e.g. "predator" won't match "predators"). Note: this
                        # does NOT prevent "predator" matching "predator 2" —
                        # disambiguation relies on the shortest-name sort below.
                        rest = norm[len(normalized_input):]
                        if rest and not rest.startswith(" "):
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
                return 0

            if matched_path is None:
                logger.warning(
                    "scan_directory: no fuzzy match found for %r in %s",
                    directory_name,
                    mount_root,
                )
                return 0

            logger.info(
                "scan_directory: fuzzy matched %r -> %s",
                directory_name,
                matched_path,
            )
            dir_path = matched_path

        scan_timestamp = datetime.now(timezone.utc)

        try:
            file_records, walk_errors = await asyncio.wait_for(
                asyncio.to_thread(self._scandir_walk, dir_path),
                timeout=30.0,
            )
        except TimeoutError:
            logger.warning(
                "scan_directory: walk timed out after 30s for %s", dir_path,
            )
            return 0

        if walk_errors:
            logger.warning(
                "scan_directory: %d errors during walk of %s", walk_errors, dir_path
            )

        files_added, files_updated, upsert_errors = await self._upsert_records(
            session, file_records, scan_timestamp
        )

        total_indexed = files_added + files_updated
        logger.info(
            "scan_directory: indexed %d files from %s (added=%d updated=%d errors=%d)",
            total_indexed,
            dir_path,
            files_added,
            files_updated,
            upsert_errors,
        )
        return total_indexed

    # ------------------------------------------------------------------
    # Private helpers — async DB
    # ------------------------------------------------------------------

    async def _upsert_records(
        self,
        session: AsyncSession,
        records: list[dict[str, Any]],
        scan_timestamp: datetime,
    ) -> tuple[int, int, int]:
        """Batch-upsert file records into mount_index.

        Fetches all pre-existing rows whose ``filepath`` matches any record in
        the provided list using batched ``WHERE filepath IN (...)`` queries
        (batch size 500).  Set-based lookups then determine whether each record
        needs an INSERT or an UPDATE, avoiding N individual SELECT queries.

        Args:
            session: Active async SQLAlchemy session.
            records: List of file metadata dicts as produced by
                ``_scandir_walk``.  Each dict must have a ``"filepath"`` key.
            scan_timestamp: Timestamp to write into ``last_seen_at`` for every
                touched row.

        Returns:
            A 3-tuple ``(files_added, files_updated, upsert_errors)``.
        """
        if not records:
            return 0, 0, 0

        _BATCH_SIZE = 500

        # Build a filepath -> record map for fast lookup.
        record_map: dict[str, dict[str, Any]] = {r["filepath"]: r for r in records}
        all_filepaths = list(record_map.keys())

        # Fetch existing rows in batches.
        existing_map: dict[str, MountIndex] = {}
        for i in range(0, len(all_filepaths), _BATCH_SIZE):
            batch = all_filepaths[i : i + _BATCH_SIZE]
            result = await session.execute(
                select(MountIndex).where(MountIndex.filepath.in_(batch))
            )
            for row in result.scalars().all():
                existing_map[row.filepath] = row

        files_added = 0
        files_updated = 0
        upsert_errors = 0

        for filepath, record in record_map.items():
            try:
                existing = existing_map.get(filepath)
                if existing is None:
                    new_entry = MountIndex(
                        filepath=record["filepath"],
                        filename=record["filename"],
                        parsed_title=record["parsed_title"],
                        parsed_year=record["parsed_year"],
                        parsed_season=record["parsed_season"],
                        parsed_episode=record["parsed_episode"],
                        parsed_resolution=record["parsed_resolution"],
                        parsed_codec=record["parsed_codec"],
                        filesize=record["filesize"],
                        last_seen_at=scan_timestamp,
                    )
                    session.add(new_entry)
                    files_added += 1
                else:
                    existing.filename = record["filename"]
                    existing.parsed_title = record["parsed_title"]
                    existing.parsed_year = record["parsed_year"]
                    existing.parsed_season = record["parsed_season"]
                    existing.parsed_episode = record["parsed_episode"]
                    existing.parsed_resolution = record["parsed_resolution"]
                    existing.parsed_codec = record["parsed_codec"]
                    existing.filesize = record["filesize"]
                    existing.last_seen_at = scan_timestamp
                    files_updated += 1
            except Exception as exc:  # noqa: BLE001 — per-file resilience
                logger.warning(
                    "_upsert_records: DB upsert failed for %r — %s",
                    filepath,
                    exc,
                )
                upsert_errors += 1

        await session.flush()
        return files_added, files_updated, upsert_errors

    # ------------------------------------------------------------------
    # Private helpers (run in thread)
    # ------------------------------------------------------------------

    def _scandir_walk(
        self, dir_path: str
    ) -> tuple[list[dict[str, Any]], int]:
        """Recursively walk a directory tree using ``os.scandir`` and parse video filenames.

        Prefer ``os.scandir`` over ``os.walk`` because ``DirEntry.stat()``
        reuses inode metadata already fetched by the underlying ``readdir``
        syscall on Linux, avoiding one extra FUSE round-trip per file compared
        to a separate ``os.path.getsize`` call.

        This method is designed to run inside ``asyncio.to_thread`` — it is
        synchronous and may block on I/O.

        Args:
            dir_path: Absolute path to start the recursive walk from.

        Returns:
            A 2-tuple ``(records, error_count)`` where ``records`` is a list
            of dicts containing parsed file metadata, and ``error_count`` is
            the number of individual file errors encountered (file skipped but
            scan continues).  Each dict has keys: ``filepath``, ``filename``,
            ``parsed_title``, ``parsed_year``, ``parsed_season``,
            ``parsed_episode``, ``parsed_resolution``, ``parsed_codec``,
            ``filesize``.
        """
        records: list[dict[str, Any]] = []
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

                try:
                    filesize: int | None = entry.stat(follow_symlinks=True).st_size
                except OSError as exc:
                    logger.warning(
                        "_scandir_walk: cannot stat %r — %s", entry.path, exc
                    )
                    filesize = None
                    error_count += 1

                parsed = _parse_filename(filename)
                record: dict[str, Any] = {
                    "filepath": entry.path,
                    "filename": filename,
                    "parsed_title": parsed.get("title"),
                    "parsed_year": parsed.get("year"),
                    "parsed_season": parsed.get("season"),
                    "parsed_episode": parsed.get("episode"),
                    "parsed_resolution": parsed.get("resolution"),
                    "parsed_codec": parsed.get("codec"),
                    "filesize": filesize,
                }
                records.append(record)

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


def _parse_filename(filename: str) -> dict[str, Any]:
    """Parse a torrent filename using PTN, returning extracted metadata.

    Extracts title, year, season, episode, resolution, and codec from the
    filename.  PTN failures are caught and the filename stem is used as the
    title fallback so one bad file never aborts the entire scan.

    The returned title is stripped and lowercased for consistent index queries.

    Args:
        filename: The raw filename string (including extension).

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

    return {
        "title": _normalize_title(raw_title),
        "year": parsed.get("year"),
        "season": parsed.get("season"),
        "episode": parsed.get("episode"),
        "resolution": parsed.get("resolution"),
        "codec": parsed.get("codec"),
    }


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

mount_scanner = MountScanner()
