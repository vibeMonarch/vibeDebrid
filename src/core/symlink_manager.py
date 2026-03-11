"""Symlink creation and health-check module for vibeDebrid.

Creates an organized library structure of symlinks that point into the Zurg
rclone mount.  Plex scans the organized library; symlinks resolve to the
actual content files served by Zurg.

Library layout::

    library/
      movies/
        Movie Name (2024)/
          Movie.Name.2024.2160p.WEB-DL.mkv -> /zurg/mount/__all__/actual.mkv
      shows/
        Show Name (2024)/
          Season 01/
            Show.S01E01.1080p.mkv -> /zurg/mount/__all__/actual.mkv

Key design rules (CLAUDE.md / SPEC.md):
- All paths are absolute host filesystem paths — never container-internal paths.
- All filesystem I/O uses ``asyncio.to_thread`` to avoid blocking the event loop.
- ``session.flush()`` is called but never ``session.commit()`` — the caller owns
  the transaction.
- Race conditions between the source-exists check and symlink creation are
  handled gracefully with specific ``OSError`` catching.
- Parent directory cleanup never walks above the library root.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import datetime, timezone

from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.models.media_item import MediaItem, MediaType
from src.models.symlink import Symlink

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------


class SourceNotFoundError(Exception):
    """Raised when the source file does not exist on disk.

    Attributes:
        source_path: The absolute path that was expected but not found.
    """

    def __init__(self, source_path: str) -> None:
        self.source_path = source_path
        super().__init__(f"Source file not found: {source_path}")


class SymlinkCreationError(Exception):
    """Raised when symlink creation fails at the OS level.

    Attributes:
        target_path: The symlink path that failed to be created.
        source_path: The intended symlink destination (the real file).
        reason: Human-readable explanation of the failure.
    """

    def __init__(self, target_path: str, source_path: str, reason: str) -> None:
        self.target_path = target_path
        self.source_path = source_path
        super().__init__(
            f"Failed to create symlink {target_path} -> {source_path}: {reason}"
        )


# ---------------------------------------------------------------------------
# Output schema
# ---------------------------------------------------------------------------


class VerifyResult(BaseModel):
    """Summary returned by SymlinkManager.verify_symlinks.

    Attributes:
        total_checked: Number of symlinks inspected (those marked valid=True).
        valid_count: Symlinks confirmed healthy during this check.
        broken_count: Symlinks newly found to be broken (marked invalid=False).
        already_invalid: Symlinks already marked invalid — skipped this run.
    """

    total_checked: int
    valid_count: int
    broken_count: int
    already_invalid: int


# ---------------------------------------------------------------------------
# Name sanitization
# ---------------------------------------------------------------------------

# Characters that are illegal or problematic on common filesystems.
# Colon is replaced with a dash; all others are dropped.
_COLON_RE = re.compile(r":")
_ILLEGAL_CHARS_RE = re.compile(r'[/\\*?"<>|]')
# Collapse multiple consecutive spaces or dashes into a single instance.
_MULTI_SPACE_RE = re.compile(r" {2,}")
_MULTI_DASH_RE = re.compile(r"-{2,}")
# Used by _find_existing_show_dir to strip decorations before exact matching.
_TIMESTAMP_PREFIX_RE = re.compile(r"^\d{12}\s+")
_RESOLUTION_SUFFIX_RE = re.compile(r"\s+\d{3,4}p$", re.IGNORECASE)
_TMDB_TAG_RE = re.compile(r"\s*\{(?:tmdb|tvdb|imdb)-[^}]+\}")


# Regex patterns for extracting episode numbers from filenames (season pack use)
_EPISODE_RE = re.compile(r"[Ss]\d{1,2}[Ee](\d{1,3})")
_BARE_EPISODE_RE = re.compile(r"[\s._-][Ee](\d{2,3})(?:\b|[\s._-])")


def _parse_episode_from_filename(filename: str) -> int | None:
    """Extract episode number from a filename using PTN then regex fallback.

    Used for season pack files where the media item has no episode number set,
    so the episode must be inferred from the individual file's name.

    Args:
        filename: Basename of the source file (e.g. ``"Show.S01E05.mkv"``).

    Returns:
        The episode number as an integer, or ``None`` when no episode number
        could be parsed.
    """
    try:
        import PTN  # type: ignore[import-untyped]

        parsed = PTN.parse(filename)
        if parsed and parsed.get("episode") is not None:
            return int(parsed["episode"])
    except Exception:
        pass

    # Regex fallback: SxxExx pattern first, then bare E-prefixed number.
    match = _EPISODE_RE.search(filename)
    if match:
        return int(match.group(1))
    match = _BARE_EPISODE_RE.search(filename)
    if match:
        return int(match.group(1))
    return None


def sanitize_name(name: str) -> str:
    """Sanitize a string for use as a filesystem directory or file name.

    Replaces characters that are illegal or problematic on common filesystems.
    Colons are replaced with a dash; other illegal characters (``/ \\ * ? " < > |``)
    are removed entirely.  Multiple consecutive spaces or dashes are collapsed to
    one.  Leading/trailing whitespace and dots are stripped (leading dots would
    hide files on Unix).  An empty result is replaced with ``"Unknown"``.

    Only the *name* portion is processed — file extensions must be stripped
    before calling this function if the caller wants to preserve them.

    Args:
        name: Raw string to sanitize.

    Returns:
        A filesystem-safe version of *name*, or ``"Unknown"`` when the result
        would otherwise be empty.
    """
    if not name:
        return "Unknown"

    sanitized = _COLON_RE.sub("-", name)
    sanitized = _ILLEGAL_CHARS_RE.sub("", sanitized)
    sanitized = _MULTI_SPACE_RE.sub(" ", sanitized)
    sanitized = _MULTI_DASH_RE.sub("-", sanitized)
    # Strip leading/trailing whitespace, dots, and dashes.
    # Dots hide files on Unix; leading/trailing dashes are aesthetically wrong
    # and can occur when a colon was the only printable character (e.g. ":").
    sanitized = sanitized.strip(" .-")

    return sanitized if sanitized else "Unknown"


# ---------------------------------------------------------------------------
# Path building helpers
# ---------------------------------------------------------------------------


def _format_timestamp() -> str:
    """Return current local time as YYYYMMDDHHMM string for user-facing paths."""
    # Local time is intentional: directory names are user-facing, so they
    # should reflect the user's clock rather than UTC.
    return datetime.now().strftime("%Y%m%d%H%M")


def _find_existing_show_dir(library_shows: str, core_name: str) -> str | None:
    """Scan library_shows for a directory matching core_name (case-insensitive).

    Strips any 12-digit timestamp prefix, resolution suffix, and Plex agent tags
    (e.g. ``{tmdb-12345}``) before comparing so that directories created under
    different naming modes can still be matched and reused.

    Args:
        library_shows: Absolute path to the shows library root.
        core_name: The normalised show name to look for (already stripped of
            timestamp/resolution/agent-tag decorations).

    Returns:
        The existing directory *name* (not full path), or ``None`` when no match
        is found.
    """
    try:
        entries = os.listdir(library_shows)
    except FileNotFoundError:
        return None
    # Also strip agent tags from the search key so callers that pass a Plex
    # core_name (which includes ``{tmdb-XXXXX}``) still match legacy dirs.
    lower_core = _TMDB_TAG_RE.sub("", core_name).strip().lower()
    for entry in entries:
        stripped = _TIMESTAMP_PREFIX_RE.sub("", entry)
        stripped = _RESOLUTION_SUFFIX_RE.sub("", stripped)
        stripped = _TMDB_TAG_RE.sub("", stripped).strip()
        if stripped.lower() == lower_core:
            full_path = os.path.join(library_shows, entry)
            if os.path.isdir(full_path):
                return entry
    return None


def _build_plex_show_dir_name(safe_title: str, year: int | None, tmdb_id: str | None) -> str:
    """Build a Plex-compatible show directory name.

    Format: ``Sanitized Title (Year) {tmdb-XXXXX}`` when *tmdb_id* is provided,
    or ``Sanitized Title (Year)`` when it is not.  No date prefix or resolution
    suffix is included — Plex does not use those decorations.

    Args:
        safe_title: Already-sanitized show title.
        year: First-air year, or ``None`` when unknown.
        tmdb_id: Numeric TMDB identifier as a string, or ``None``.

    Returns:
        A Plex-compatible directory name string (not a full path).
    """
    parts: list[str] = [safe_title]
    if year is not None:
        parts.append(f"({year})")
    name = " ".join(parts)
    if tmdb_id is not None and tmdb_id.isdigit():
        name = f"{name} {{tmdb-{tmdb_id}}}"
    return name


def build_movie_dir(title: str, year: int | None, resolution: str | None = None, tmdb_id: str | None = None) -> str:
    """Build the organized library directory path for a movie.

    When ``settings.symlink_naming.plex_naming`` is ``True`` the directory
    uses Plex agent-tag format (``Title (Year) {tmdb-XXXXX}``).  No date
    prefix or resolution suffix is added in that mode regardless of the
    individual ``date_prefix`` / ``resolution`` flags.

    Args:
        title: Movie title (will be sanitized).
        year: Release year, or None when unknown.
        resolution: Requested resolution (e.g. "2160p"), included when enabled.
        tmdb_id: Numeric TMDB identifier as a string, used when plex_naming is
            True to add the ``{tmdb-XXXXX}`` agent tag.

    Returns:
        Absolute path like ``/path/to/library/movies/202603011430 Movie Name (2024) 2160p``
        or ``/path/to/library/movies/Movie Name (2024) {tmdb-12345}`` in Plex mode.
    """
    naming = settings.symlink_naming
    safe_title = sanitize_name(title)

    if naming.plex_naming:
        parts: list[str] = [safe_title]
        if year is not None:
            parts.append(f"({year})")
        dir_name = " ".join(parts)
        if tmdb_id is not None and tmdb_id.isdigit():
            dir_name = f"{dir_name} {{tmdb-{tmdb_id}}}"
    else:
        parts = []
        if naming.date_prefix:
            parts.append(_format_timestamp())
        parts.append(safe_title)
        if naming.release_year and year is not None:
            parts.append(f"({year})")
        if naming.resolution and resolution:
            parts.append(resolution)
        dir_name = " ".join(parts)

    return os.path.join(settings.paths.library_movies, dir_name)


def build_show_dir(title: str, year: int | None, season: int, resolution: str | None = None, tmdb_id: str | None = None) -> str:
    """Build the organized library directory path for a show episode.

    When ``settings.symlink_naming.plex_naming`` is ``True`` the show directory
    uses Plex agent-tag format (``Title (Year) {tmdb-XXXXX}``).  Season
    subdirectories are always ``Season XX`` regardless of naming mode.

    Args:
        title: Show title (will be sanitized).
        year: First-air year, or None when unknown.
        season: Season number (zero-padded to two digits in the output).
        resolution: Requested resolution (e.g. "2160p"), included when enabled.
        tmdb_id: Numeric TMDB identifier as a string, used when plex_naming is
            True to add the ``{tmdb-XXXXX}`` agent tag.

    Returns:
        Absolute path like ``/path/to/library/shows/202603011430 Show Name (2024)/Season 01``
        or ``/path/to/library/shows/Show Name (2024) {tmdb-12345}/Season 01`` in Plex mode.
    """
    naming = settings.symlink_naming
    safe_title = sanitize_name(title)

    if naming.plex_naming:
        core_name = _build_plex_show_dir_name(safe_title, year, tmdb_id)
        # Reuse an existing directory even if it was created under a different
        # naming scheme (e.g. without a tmdb tag, or with a timestamp prefix).
        existing = _find_existing_show_dir(settings.paths.library_shows, core_name)
        show_dir = existing if existing is not None else core_name
    else:
        # Build core_name for matching existing directories (legacy mode).
        core_parts: list[str] = [safe_title]
        if naming.release_year and year is not None:
            core_parts.append(f"({year})")
        core_name = " ".join(core_parts)

        existing = _find_existing_show_dir(settings.paths.library_shows, core_name)
        if existing is not None:
            show_dir = existing
        else:
            parts: list[str] = []
            if naming.date_prefix:
                parts.append(_format_timestamp())
            parts.append(core_name)
            if naming.resolution and resolution:
                parts.append(resolution)
            show_dir = " ".join(parts)

    season_dir = f"Season {season:02d}"
    return os.path.join(settings.paths.library_shows, show_dir, season_dir)


# ---------------------------------------------------------------------------
# SymlinkManager
# ---------------------------------------------------------------------------


class SymlinkManager:
    """Stateless symlink creation and health-check service.

    All methods that interact with the database accept an ``AsyncSession`` and
    call ``session.flush()`` but never ``session.commit()`` — the caller is
    responsible for transaction management (CLAUDE.md convention).

    All filesystem operations run inside ``asyncio.to_thread`` so the async
    event loop is never blocked.
    """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def create_symlink(
        self,
        session: AsyncSession,
        media_item: MediaItem,
        source_path: str,
    ) -> Symlink:
        """Create an organized library symlink for *media_item* pointing to *source_path*.

        The method validates that the source file exists, builds the correct
        target directory path, creates the directory tree, resolves any
        pre-existing symlink at the target path, creates the new symlink, and
        records the result in the ``symlinks`` table.

        Args:
            session: Caller-managed async database session.
            media_item: ORM object with ``title``, ``year``, ``media_type``,
                ``season``, and ``episode`` populated.
            source_path: Absolute path to the actual file inside the Zurg mount
                (the symlink will *point to* this path).

        Returns:
            The newly created (or found-existing) ``Symlink`` ORM object,
            flushed but not committed.

        Raises:
            SourceNotFoundError: When *source_path* does not exist on disk at
                the time of the call.
            SymlinkCreationError: When the OS-level ``symlink(2)`` call fails
                for a reason other than the target already existing.
        """
        # --- Step 1: validate source exists ---
        source_exists = await asyncio.to_thread(os.path.exists, source_path)
        if not source_exists:
            logger.warning(
                "create_symlink: source not found source_path=%r media_item_id=%s",
                source_path,
                media_item.id,
            )
            raise SourceNotFoundError(source_path)

        # --- Step 2: determine target directory ---
        tmdb_id = str(media_item.tmdb_id) if media_item.tmdb_id else None
        if media_item.media_type == MediaType.MOVIE:
            target_dir = build_movie_dir(
                media_item.title, media_item.year, media_item.requested_resolution, tmdb_id=tmdb_id
            )
        else:
            season = media_item.season if media_item.season is not None else 1
            target_dir = await asyncio.to_thread(
                build_show_dir,
                media_item.title,
                media_item.year,
                season,
                media_item.requested_resolution,
                tmdb_id,
            )

        # --- Step 3: build full target symlink path ---
        naming = settings.symlink_naming
        if naming.plex_naming:
            # Generate Plex-compatible filename from media item metadata.
            ext = os.path.splitext(os.path.basename(source_path))[1]
            safe_title = sanitize_name(media_item.title)
            year_part = f" ({media_item.year})" if media_item.year else ""
            if media_item.media_type == MediaType.SHOW:
                ep_season = media_item.season if media_item.season is not None else 1
                ep_episode = media_item.episode

                # For season packs, media_item.episode is None — infer the
                # episode number from the individual source filename so that
                # each file in the pack gets a distinct symlink name.
                if ep_episode is None:
                    ep_episode = _parse_episode_from_filename(
                        os.path.basename(source_path)
                    )

                if ep_episode is not None:
                    filename = f"{safe_title}{year_part} - S{ep_season:02d}E{ep_episode:02d}{ext}"
                else:
                    # Last resort: keep the raw source filename so at least the
                    # file is accessible, even if it won't match Plex naming.
                    logger.warning(
                        "create_symlink: could not parse episode from %r for season pack "
                        "media_item_id=%s — using raw filename",
                        os.path.basename(source_path),
                        media_item.id,
                    )
                    filename = os.path.basename(source_path)
            else:
                filename = f"{safe_title}{year_part}{ext}"
        else:
            filename = os.path.basename(source_path)
            # For shows with date_prefix enabled, prefix the episode filename.
            if media_item.media_type == MediaType.SHOW and naming.date_prefix:
                filename = f"{_format_timestamp()} {filename}"
        target_path = os.path.join(target_dir, filename)

        logger.debug(
            "create_symlink: target_path=%r -> source_path=%r",
            target_path,
            source_path,
        )

        # --- Step 4: create target directory ---
        await asyncio.to_thread(os.makedirs, target_dir, exist_ok=True)

        # --- Step 5: handle pre-existing symlink at target_path ---
        target_is_symlink = await asyncio.to_thread(os.path.islink, target_path)
        if target_is_symlink:
            existing_target = await asyncio.to_thread(os.readlink, target_path)
            if existing_target == source_path:
                # Same symlink already exists — find or create the DB record.
                logger.info(
                    "create_symlink: symlink already correct target_path=%r -> %r",
                    target_path,
                    source_path,
                )
                return await self._find_or_create_db_record(
                    session, media_item, source_path, target_path
                )
            else:
                # Stale symlink pointing elsewhere — remove it.
                logger.info(
                    "create_symlink: removing stale symlink target_path=%r "
                    "(was -> %r, now -> %r)",
                    target_path,
                    existing_target,
                    source_path,
                )
                await asyncio.to_thread(os.unlink, target_path)

        # --- Step 6: create the symlink ---
        try:
            await asyncio.to_thread(os.symlink, source_path, target_path)
        except FileExistsError:
            # Race condition: another coroutine created the symlink between our
            # islink check and this call.  Re-read and reconcile.
            logger.warning(
                "create_symlink: race condition — target already exists target_path=%r",
                target_path,
            )
            return await self._find_or_create_db_record(
                session, media_item, source_path, target_path
            )
        except OSError as exc:
            raise SymlinkCreationError(target_path, source_path, str(exc)) from exc

        # --- Step 7: record in DB ---
        symlink = Symlink(
            media_item_id=media_item.id,
            source_path=source_path,
            target_path=target_path,
            valid=True,
        )
        session.add(symlink)
        await session.flush()

        logger.info(
            "create_symlink: created %r -> %r (media_item_id=%s)",
            target_path,
            source_path,
            media_item.id,
        )
        return symlink

    async def verify_symlinks(self, session: AsyncSession) -> VerifyResult:
        """Check health of all symlinks currently marked valid=True in the database.

        For each valid symlink the method verifies that:
        1. The ``target_path`` exists and is a symlink (``os.path.islink``).
        2. The resolved destination of the symlink (the ``source_path``) still
           exists on disk (``os.path.exists``).

        Broken symlinks are marked ``valid=False``, removed from disk, and
        their empty parent directories are cleaned up so that Plex detects the
        missing media on its next scan.  Healthy symlinks have
        ``last_checked_at`` updated to the current UTC time.

        A mount-health pre-flight check prevents mass symlink deletion when the
        Zurg/rclone FUSE mount is transiently unavailable.

        Args:
            session: Caller-managed async database session.

        Returns:
            A ``VerifyResult`` summarising the outcome of the health check.
        """
        now = datetime.now(timezone.utc)

        # Pre-flight: verify the Zurg mount root is accessible.  A transient
        # mount outage makes every symlink destination appear missing; deleting
        # them all would be catastrophic.
        mount_root = settings.paths.zurg_mount
        mount_ok = await asyncio.to_thread(os.path.isdir, mount_root)
        if not mount_ok:
            logger.warning(
                "verify_symlinks: mount root %r is not accessible, "
                "skipping verification to avoid mass symlink deletion",
                mount_root,
            )
            return VerifyResult(
                total_checked=0, valid_count=0,
                broken_count=0, already_invalid=0,
            )

        # Count already-invalid so the summary is accurate.
        already_invalid_result = await session.execute(
            select(Symlink).where(Symlink.valid.is_(False))
        )
        already_invalid_count = len(already_invalid_result.scalars().all())

        # Fetch all currently-valid symlinks.
        valid_result = await session.execute(
            select(Symlink).where(Symlink.valid.is_(True))
        )
        valid_symlinks = list(valid_result.scalars().all())

        total_checked = len(valid_symlinks)
        valid_count = 0
        broken_count = 0

        for symlink in valid_symlinks:
            is_link = await asyncio.to_thread(os.path.islink, symlink.target_path)
            if not is_link:
                # The symlink file itself is missing or was replaced by a regular file.
                symlink.valid = False
                broken_count += 1
                logger.warning(
                    "verify_symlinks: broken symlink (not a link) "
                    "target_path=%r media_item_id=%s",
                    symlink.target_path,
                    symlink.media_item_id,
                )
                # Clean up empty parent dirs left behind.
                parent = os.path.dirname(symlink.target_path)
                grandparent = os.path.dirname(parent)
                await self._try_remove_empty_dir(parent)
                await self._try_remove_empty_dir(grandparent)
                continue

            # The symlink exists — now verify its destination.
            dest_exists = await asyncio.to_thread(os.path.exists, symlink.target_path)
            if not dest_exists:
                symlink.valid = False
                broken_count += 1
                logger.warning(
                    "verify_symlinks: broken symlink (destination missing) "
                    "target_path=%r source_path=%r media_item_id=%s",
                    symlink.target_path,
                    symlink.source_path,
                    symlink.media_item_id,
                )
                # Remove the dead symlink from disk so Plex can detect
                # the missing media and clean up its library.
                try:
                    await asyncio.to_thread(os.unlink, symlink.target_path)
                except FileNotFoundError:
                    pass  # Already removed by another process.
                except OSError as exc:
                    logger.warning(
                        "verify_symlinks: could not remove dead symlink %r — %s",
                        symlink.target_path,
                        exc,
                    )
                    continue
                logger.info(
                    "verify_symlinks: removed dead symlink %r",
                    symlink.target_path,
                )
                # Clean up empty parent/grandparent directories.
                parent = os.path.dirname(symlink.target_path)
                grandparent = os.path.dirname(parent)
                await self._try_remove_empty_dir(parent)
                await self._try_remove_empty_dir(grandparent)
            else:
                symlink.last_checked_at = now
                valid_count += 1

        await session.flush()

        result = VerifyResult(
            total_checked=total_checked,
            valid_count=valid_count,
            broken_count=broken_count,
            already_invalid=already_invalid_count,
        )

        logger.info(
            "verify_symlinks: total_checked=%d valid=%d broken=%d already_invalid=%d",
            result.total_checked,
            result.valid_count,
            result.broken_count,
            result.already_invalid,
        )
        return result

    async def remove_symlink(
        self, session: AsyncSession, media_item_id: int
    ) -> int:
        """Remove all symlinks associated with *media_item_id* from disk and the DB.

        For each symlink:
        1. The symlink file on disk is removed (if it exists).
        2. The immediate parent directory is removed if it is empty.
        3. The grandparent directory is removed if it is empty (handles the
           ``Show Name (2024)/Season 01`` two-level structure for shows).
        4. Cleanup never walks above the configured library roots.
        5. The database row is deleted.

        Args:
            session: Caller-managed async database session.
            media_item_id: Primary key of the MediaItem whose symlinks should
                be removed.

        Returns:
            The number of symlinks removed.
        """
        result = await session.execute(
            select(Symlink).where(Symlink.media_item_id == media_item_id)
        )
        symlinks = list(result.scalars().all())

        removed_count = 0
        for symlink in symlinks:
            target_path = symlink.target_path

            # Remove the symlink file if it still exists on disk.
            file_exists = await asyncio.to_thread(os.path.islink, target_path)
            if not file_exists:
                # Also check for a regular file in case the symlink was replaced.
                file_exists = await asyncio.to_thread(os.path.exists, target_path)

            if file_exists:
                try:
                    await asyncio.to_thread(os.unlink, target_path)
                    logger.info(
                        "remove_symlink: removed file target_path=%r", target_path
                    )
                except OSError as exc:
                    logger.warning(
                        "remove_symlink: could not remove target_path=%r — %s",
                        target_path,
                        exc,
                    )

            # Clean up empty ancestor directories (never above library roots).
            parent = os.path.dirname(target_path)
            grandparent = os.path.dirname(parent)

            await self._try_remove_empty_dir(parent)
            await self._try_remove_empty_dir(grandparent)

            # Delete the database record.
            await session.delete(symlink)
            removed_count += 1

        await session.flush()

        logger.info(
            "remove_symlink: removed %d symlinks for media_item_id=%s",
            removed_count,
            media_item_id,
        )
        return removed_count

    async def get_symlinks_for_item(
        self, session: AsyncSession, media_item_id: int
    ) -> list[Symlink]:
        """Return all Symlink rows associated with *media_item_id*.

        Args:
            session: Caller-managed async database session.
            media_item_id: Primary key of the target MediaItem.

        Returns:
            List of Symlink ORM objects (may be empty).
        """
        result = await session.execute(
            select(Symlink).where(Symlink.media_item_id == media_item_id)
        )
        return list(result.scalars().all())

    async def get_broken_symlinks(self, session: AsyncSession) -> list[Symlink]:
        """Return all Symlink rows where valid=False.

        Args:
            session: Caller-managed async database session.

        Returns:
            List of broken Symlink ORM objects (may be empty).
        """
        result = await session.execute(
            select(Symlink).where(Symlink.valid.is_(False))
        )
        return list(result.scalars().all())

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _find_or_create_db_record(
        self,
        session: AsyncSession,
        media_item: MediaItem,
        source_path: str,
        target_path: str,
    ) -> Symlink:
        """Return an existing Symlink DB record or create a new one.

        Used when a correct symlink already exists on disk to ensure the
        database stays consistent without creating duplicate rows.

        Args:
            session: Caller-managed async database session.
            media_item: The associated MediaItem ORM object.
            source_path: Absolute path the symlink points to.
            target_path: Absolute path of the symlink file itself.

        Returns:
            The found or newly created Symlink ORM object, flushed.
        """
        result = await session.execute(
            select(Symlink).where(Symlink.target_path == target_path)
        )
        existing = result.scalar_one_or_none()
        if existing is not None:
            return existing

        symlink = Symlink(
            media_item_id=media_item.id,
            source_path=source_path,
            target_path=target_path,
            valid=True,
        )
        session.add(symlink)
        await session.flush()
        return symlink

    async def _try_remove_empty_dir(self, dir_path: str) -> None:
        """Remove *dir_path* if it is empty and safe to remove.

        Attempts ``os.rmdir`` (which the OS refuses if the directory is
        non-empty).  The call is skipped if *dir_path* is the configured
        library root itself — we never remove the root directories.  A path
        that falls outside all configured library roots is still attempted so
        that callers do not need to pre-patch settings during tests; the OS
        ``rmdir`` syscall is the true safety net (it refuses non-empty dirs).

        Only ``OSError`` is caught — no bare except.

        Args:
            dir_path: Absolute path to the candidate directory.
        """
        library_roots = (
            settings.paths.library_movies,
            settings.paths.library_shows,
        )
        norm_path = os.path.normpath(dir_path)

        # Never remove any of the library roots themselves.
        for root in library_roots:
            if norm_path == os.path.normpath(root):
                return

        try:
            await asyncio.to_thread(os.rmdir, norm_path)
            logger.info("remove_symlink: removed empty dir %r", norm_path)
        except OSError:
            # Not empty, already removed, or permission denied — all acceptable.
            pass


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

symlink_manager = SymlinkManager()
