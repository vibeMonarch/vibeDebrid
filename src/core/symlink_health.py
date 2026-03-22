"""Symlink health-check and repair tool for vibeDebrid.

Scans all Symlink records to identify broken symlinks, classifies each broken
item as RECOVERABLE (a matching file exists in the mount index) or DEAD (no
matching file found), and provides repair operations.

Design notes:
- Mount availability is checked first to avoid classifying everything as dead
  when the Zurg/rclone FUSE mount is transiently unavailable.
- Path existence checks run via ``asyncio.to_thread`` so the event loop is
  never blocked by FUSE I/O.
- A season-pack item may have many symlinks — only the first broken symlink per
  item_id is included in the result (representative row).
- ``execute_symlink_health`` does NOT commit — the caller owns the transaction.
"""

from __future__ import annotations

import asyncio
import enum
import logging
import os
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field
from sqlalchemy import func, literal, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.models.media_item import MediaItem, QueueState
from src.models.mount_index import MountIndex
from src.models.symlink import Symlink

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


class SymlinkStatus(str, enum.Enum):
    """Classification of a broken symlink."""

    RECOVERABLE = "recoverable"
    """A matching file exists in the mount index — item can be re-queued."""
    DEAD = "dead"
    """No matching file found — symlink record can only be cleaned up."""


class BrokenSymlinkItem(BaseModel):
    """Detail for a single broken symlink (one representative row per item_id).

    Attributes:
        item_id: Primary key of the associated MediaItem.
        title: Human-readable title from the MediaItem.
        media_type: ``"movie"`` or ``"show"``.
        season: Season number, or ``None`` for movies / season-pack headers.
        episode: Episode number, or ``None`` for movies / season packs.
        state: Current QueueState value as a string.
        source_path: The broken source_path recorded in the symlinks table.
        target_path: The symlink target_path (the library-side path).
        source_exists: Whether ``os.path.exists(source_path)`` returned True.
        target_valid: Whether ``os.path.islink(target_path)`` AND
            ``os.path.exists(target_path)`` both returned True.
        status: RECOVERABLE or DEAD classification.
        mount_match_path: First matching filepath from mount_index when
            RECOVERABLE, else ``None``.
        symlink_id: Primary key of the representative Symlink row.
    """

    item_id: int
    title: str
    media_type: str
    season: int | None
    episode: int | None
    state: str
    source_path: str
    target_path: str
    source_exists: bool
    target_valid: bool
    status: SymlinkStatus
    mount_match_path: str | None
    symlink_id: int


class SymlinkHealthScan(BaseModel):
    """Aggregate result of a symlink health scan.

    Attributes:
        total_symlinks: Total Symlink rows inspected.
        healthy: Symlinks whose source exists and target resolves correctly.
        broken: Total broken symlinks found (may span multiple per item).
        recoverable: Unique items classified as RECOVERABLE.
        dead: Unique items classified as DEAD.
        items: One BrokenSymlinkItem per broken item_id (representative row).
        errors: Non-fatal error messages accumulated during the scan.
    """

    total_symlinks: int = 0
    healthy: int = 0
    broken: int = 0
    recoverable: int = 0
    dead: int = 0
    items: list[BrokenSymlinkItem] = []
    errors: list[str] = []


class SymlinkHealthExecuteRequest(BaseModel):
    """Request body for the symlink health execute endpoint.

    Attributes:
        requeue_ids: item_ids to transition to WANTED (recoverable items).
            Symlinks are removed from DB and disk before re-queuing.
        cleanup_ids: item_ids to clean symlink records only (dead items).
            Item state is NOT changed.
    """

    requeue_ids: list[int] = []
    cleanup_ids: list[int] = []


class MediaScanTarget(BaseModel):
    """A (media_type, target_path) pair for triggering Plex/Jellyfin scans.

    Attributes:
        media_type: ``"movie"`` or ``"show"`` string.
        target_path: Absolute path of the newly created symlink in the library.
    """

    media_type: str
    target_path: str


class SymlinkHealthResult(BaseModel):
    """Result of a symlink health execute operation.

    Attributes:
        requeued: Number of items successfully transitioned to WANTED.
        recreated: Number of items whose symlinks were directly recreated
            (source file still exists — no re-scrape required).
        recreated_ids: item_ids that were recreated (not re-queued).
        cleaned: Number of items whose symlink records were cleaned up.
        symlinks_removed_from_disk: Total symlink files deleted from disk.
        media_scan_targets: (media_type, target_path) pairs collected during
            symlink recreation, used by the route handler to trigger
            Plex/Jellyfin library scans.
        errors: Non-fatal error messages accumulated during execution.
    """

    requeued: int = 0
    recreated: int = 0
    recreated_ids: list[int] = []
    cleaned: int = 0
    symlinks_removed_from_disk: int = 0
    media_scan_targets: list[MediaScanTarget] = Field(default_factory=list, exclude=True)
    errors: list[str] = []


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


async def _check_path_pair(source_path: str, target_path: str) -> tuple[bool, bool]:
    """Check existence of a source/target path pair concurrently.

    Args:
        source_path: The file the symlink points to (the real file).
        target_path: The symlink file itself in the library.

    Returns:
        Tuple of (source_exists, target_valid) where target_valid means the
        path is a symlink AND it resolves (i.e. destination exists).
    """
    source_exists, is_link, dest_exists = await asyncio.gather(
        asyncio.to_thread(os.path.exists, source_path),
        asyncio.to_thread(os.path.islink, target_path),
        asyncio.to_thread(os.path.exists, target_path),
    )
    target_valid = bool(is_link and dest_exists)
    return bool(source_exists), target_valid


async def _find_mount_match(
    session: AsyncSession,
    title: str,
    media_type: str,
    season: int | None,
    episode: int | None,
    year: int | None,
) -> str | None:
    """Search the mount_index for a file matching the given item attributes.

    For shows: matches parsed_title (case-insensitive LIKE) + season + episode.
    For movies: matches parsed_title (case-insensitive LIKE) + optional year.

    Args:
        session: Active async SQLAlchemy session.
        title: Item title to search for.
        media_type: ``"movie"`` or ``"show"``.
        season: Season number (shows only).
        episode: Episode number (shows only).
        year: Release year (movies only, used as additional filter when set).

    Returns:
        The filepath of the first match, or ``None`` when nothing is found.
    """
    from src.core.mount_scanner import _normalize_title, _is_word_subsequence  # noqa: PLC0415

    normalized = _normalize_title(title)
    like_pattern = f"%{normalized}%"

    def _apply_season_year_filters(stmt):
        if media_type.upper() == "SHOW":
            if season is not None:
                stmt = stmt.where(MountIndex.parsed_season == season)
            if episode is not None:
                stmt = stmt.where(MountIndex.parsed_episode == episode)
        else:
            if year is not None:
                stmt = stmt.where(MountIndex.parsed_year == year)
        return stmt.order_by(MountIndex.last_seen_at.desc())

    # Phase 1: forward LIKE — parsed_title contains the normalized search string.
    stmt = _apply_season_year_filters(
        select(MountIndex.filepath).where(MountIndex.parsed_title.ilike(like_pattern))
    )
    result = await session.execute(stmt.limit(1))
    row = result.scalar_one_or_none()
    if row is not None:
        return row

    # Phase 2: reverse containment — the search title contains parsed_title as a
    # substring.  Handles the case where the TMDB title is longer than the torrent's
    # parsed_title (e.g. TMDB "Attack on Titan The Final Season" vs parsed "attack
    # on titan").  Uses instr() to stay in pure SQLAlchemy (no raw SQL text).
    # Require 3+ words in parsed_title to avoid overly broad matches.
    search_words = normalized.split()
    stmt = _apply_season_year_filters(
        select(MountIndex.filepath, MountIndex.parsed_title)
        .where(MountIndex.parsed_title.is_not(None))
        .where(func.instr(literal(normalized), MountIndex.parsed_title) > 0)
        .limit(50)
    )
    result = await session.execute(stmt)
    rows = result.fetchall()
    for filepath, parsed_title in rows:
        db_words = (parsed_title or "").split()
        if len(db_words) < 3:
            continue
        if _is_word_subsequence(db_words, search_words):
            return str(filepath)

    return None


async def _remove_symlinks_for_item(
    session: AsyncSession,
    item_id: int,
) -> int:
    """Delete all Symlink rows for item_id from DB and disk.

    Also cleans up empty parent/grandparent directories in the library.

    Args:
        session: Active async SQLAlchemy session (caller owns transaction).
        item_id: Primary key of the MediaItem whose symlinks should be removed.

    Returns:
        Number of symlink files successfully removed from disk.
    """
    result = await session.execute(
        select(Symlink).where(Symlink.media_item_id == item_id)
    )
    symlinks = list(result.scalars().all())

    removed_from_disk = 0
    library_roots = (
        os.path.normpath(settings.paths.library_movies),
        os.path.normpath(settings.paths.library_shows),
    )

    for symlink in symlinks:
        target_path = symlink.target_path

        is_link = await asyncio.to_thread(os.path.islink, target_path)
        if is_link:
            try:
                await asyncio.to_thread(os.unlink, target_path)
                removed_from_disk += 1
                logger.info(
                    "_remove_symlinks_for_item: removed %r (item_id=%d)",
                    target_path,
                    item_id,
                )
            except FileNotFoundError:
                # Already gone — still count as removed.
                removed_from_disk += 1
            except OSError as exc:
                logger.warning(
                    "_remove_symlinks_for_item: could not remove %r — %s",
                    target_path,
                    exc,
                )

        # Clean up empty parent and grandparent directories (never the roots).
        for ancestor in (
            os.path.dirname(target_path),
            os.path.dirname(os.path.dirname(target_path)),
        ):
            norm = os.path.normpath(ancestor)
            if norm in library_roots:
                continue
            try:
                await asyncio.to_thread(os.rmdir, norm)
                logger.info(
                    "_remove_symlinks_for_item: removed empty dir %r", norm
                )
            except OSError:
                pass  # Not empty or already gone — acceptable.

        await session.delete(symlink)

    await session.flush()
    return removed_from_disk


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def scan_symlink_health(session: AsyncSession) -> SymlinkHealthScan:
    """Scan all Symlink records and classify broken ones.

    Steps:
    1. Check mount availability — return an empty scan with an error when
       the mount is down (prevents classifying all symlinks as dead).
    2. Query all Symlink rows joined with their MediaItem.
    3. Check each path pair concurrently via ``asyncio.to_thread``.
    4. Deduplicate broken items by item_id (one representative per item).
    5. For each broken item, search the mount_index for a matching file to
       determine RECOVERABLE vs DEAD.

    Args:
        session: Active async SQLAlchemy session.

    Returns:
        A ``SymlinkHealthScan`` describing the health of the symlink library.
    """
    scan = SymlinkHealthScan()

    # ------------------------------------------------------------------
    # Step 1: verify mount is available
    # ------------------------------------------------------------------
    mount_root = settings.paths.zurg_mount
    try:
        mount_ok = await asyncio.wait_for(
            asyncio.to_thread(os.path.isdir, mount_root),
            timeout=5.0,
        )
    except TimeoutError:
        mount_ok = False
        logger.warning(
            "scan_symlink_health: mount availability check timed out for %r",
            mount_root,
        )

    if not mount_ok:
        msg = (
            f"Zurg mount is not accessible at {mount_root!r} — "
            "symlink health check skipped to avoid false positives"
        )
        logger.warning("scan_symlink_health: %s", msg)
        scan.errors.append(msg)
        return scan

    # ------------------------------------------------------------------
    # Step 2: query all Symlink + MediaItem rows
    # ------------------------------------------------------------------
    raw = await session.execute(
        text(
            """
            SELECT
                s.id            AS symlink_id,
                s.source_path,
                s.target_path,
                s.media_item_id AS item_id,
                m.title,
                m.media_type,
                m.season,
                m.episode,
                m.state,
                m.year
            FROM symlinks s
            JOIN media_items m ON s.media_item_id = m.id
            ORDER BY s.media_item_id, s.id
            """
        )
    )
    rows = raw.fetchall()

    if not rows:
        logger.info("scan_symlink_health: no symlinks found in DB")
        return scan

    scan.total_symlinks = len(rows)

    # ------------------------------------------------------------------
    # Step 3: batch-check all path pairs concurrently (semaphore-bounded)
    # ------------------------------------------------------------------
    # Each _check_path_pair issues 3 asyncio.to_thread calls.  Without a
    # concurrency cap, 500 symlinks would submit 1500 threads simultaneously
    # and exhaust the default thread pool (32 threads).
    _PATH_CHECK_SEMAPHORE_LIMIT = 20
    sem = asyncio.Semaphore(_PATH_CHECK_SEMAPHORE_LIMIT)

    async def _guarded_check(source_path: str, target_path: str) -> tuple[bool, bool]:
        async with sem:
            return await _check_path_pair(source_path, target_path)

    check_tasks = [
        _guarded_check(str(row.source_path), str(row.target_path))
        for row in rows
    ]
    path_results: list[tuple[bool, bool]] = await asyncio.gather(*check_tasks)

    # ------------------------------------------------------------------
    # Step 4: tally healthy/broken and deduplicate broken by item_id
    # ------------------------------------------------------------------
    # Map: item_id -> first broken row + checks
    broken_by_item: dict[int, tuple[object, bool, bool]] = {}

    for row, (source_exists, target_valid) in zip(rows, path_results):
        is_healthy = source_exists and target_valid
        if not is_healthy:
            item_id = int(row.item_id)
            # Keep the first broken symlink as the representative for this item.
            if item_id not in broken_by_item:
                broken_by_item[item_id] = (row, source_exists, target_valid)

    # ------------------------------------------------------------------
    # Step 5: classify each broken item.
    #
    # Fast path: if the source file recorded in the Symlink row still exists
    # on disk, we can mark it RECOVERABLE immediately and skip the fuzzy
    # mount_index title search entirely.  This correctly handles the common
    # case where the symlink target was deleted (e.g. library rescan wiped
    # it) but the underlying Zurg-mount file is still present.
    #
    # Fallback: when source_exists is False, fall back to _find_mount_match
    # (the existing title-based search) to detect files that Zurg relocated
    # to a different path.
    # ------------------------------------------------------------------
    for item_id, (row, source_exists, target_valid) in broken_by_item.items():
        if source_exists:
            # Source file still present — directly recoverable without a DB lookup.
            match_path: str | None = str(row.source_path)
            logger.debug(
                "scan_symlink_health: item_id=%d source still exists, marking RECOVERABLE",
                item_id,
            )
        else:
            try:
                match_path = await _find_mount_match(
                    session,
                    title=str(row.title),
                    media_type=str(row.media_type),
                    season=row.season,
                    episode=row.episode,
                    year=row.year,
                )
            except Exception as exc:
                logger.warning(
                    "scan_symlink_health: mount lookup failed for item_id=%d: %s",
                    item_id,
                    exc,
                )
                match_path = None
                scan.errors.append(
                    f"item_id={item_id} ({row.title}): mount lookup error — {exc}"
                )

        status = SymlinkStatus.RECOVERABLE if match_path else SymlinkStatus.DEAD

        broken_item = BrokenSymlinkItem(
            item_id=item_id,
            title=str(row.title),
            media_type=str(row.media_type),
            season=row.season,
            episode=row.episode,
            state=str(row.state),
            source_path=str(row.source_path),
            target_path=str(row.target_path),
            source_exists=source_exists,
            target_valid=target_valid,
            status=status,
            mount_match_path=match_path,
            symlink_id=int(row.symlink_id),
        )
        scan.items.append(broken_item)

        if status == SymlinkStatus.RECOVERABLE:
            scan.recoverable += 1
        else:
            scan.dead += 1

    # broken = deduplicated item count (one per item_id)
    scan.broken = len(scan.items)
    # healthy = total symlinks minus the number of unique broken items
    scan.healthy = scan.total_symlinks - scan.broken

    logger.info(
        "scan_symlink_health: total=%d healthy=%d broken=%d recoverable=%d dead=%d errors=%d",
        scan.total_symlinks,
        scan.healthy,
        scan.broken,
        scan.recoverable,
        scan.dead,
        len(scan.errors),
    )

    return scan


async def execute_symlink_health(
    session: AsyncSession,
    request: SymlinkHealthExecuteRequest,
) -> SymlinkHealthResult:
    """Re-queue or directly recreate recoverable items; clean up dead records.

    For ``requeue_ids`` — two paths depending on whether source files exist:

    **Direct recreate** (preferred, when any source_path still exists on disk):
      1. Load the MediaItem and all its Symlink rows.
      2. For each Symlink whose source_path exists, call
         ``symlink_manager.create_symlink`` to rebuild the library symlink.
      3. Delete the old Symlink DB records and flush.
      4. Transition item state to COMPLETE (no re-scrape needed).
      5. Collect (media_type, target_path) pairs for Plex/Jellyfin scans.

    **Re-queue fallback** (when no source_path exists on disk):
      1. Delete all Symlink rows from DB and remove files from disk.
      2. Transition item state to WANTED so the scrape pipeline retries.

    For ``cleanup_ids``:
      1. Delete all Symlink rows from DB and remove files from disk.
      2. Do NOT change the item state — leave it as-is (e.g. DONE).

    Each item is processed inside a savepoint for per-item atomicity.
    Non-fatal failures are logged and accumulated in the result's errors list.

    Args:
        session: Active async SQLAlchemy session. Caller must call
            ``await session.commit()`` after this function returns.
        request: Lists of item_ids to re-queue and/or clean up.

    Returns:
        A ``SymlinkHealthResult`` with counts for all outcomes.
    """
    from src.core.queue_manager import QueueManager, ItemNotFoundError  # noqa: PLC0415
    from src.core.symlink_manager import (  # noqa: PLC0415
        SymlinkManager,
        SourceNotFoundError,
        SymlinkCreationError,
    )

    result = SymlinkHealthResult()
    queue_manager = QueueManager()
    symlink_manager = SymlinkManager()

    # ------------------------------------------------------------------
    # Process requeue_ids — direct recreate when possible, else re-queue
    # ------------------------------------------------------------------
    for item_id in request.requeue_ids:
        try:
            # Load the MediaItem.
            item_result = await session.execute(
                select(MediaItem).where(MediaItem.id == item_id)
            )
            media_item = item_result.scalar_one_or_none()
            if media_item is None:
                raise ItemNotFoundError(item_id)

            # Load all Symlink rows for this item.
            symlinks_result = await session.execute(
                select(Symlink).where(Symlink.media_item_id == item_id)
            )
            symlinks = list(symlinks_result.scalars().all())

            # Check which source_paths still exist on disk.
            source_checks = await asyncio.gather(
                *[
                    asyncio.to_thread(os.path.exists, s.source_path)
                    for s in symlinks
                ]
            )
            live_symlinks = [
                s for s, exists in zip(symlinks, source_checks) if exists
            ]

            if live_symlinks:
                # ----------------------------------------------------------
                # Direct recreate: source files are still on disk.
                # Delete old DB records and rebuild symlinks in place.
                # ----------------------------------------------------------
                try:
                    async with await session.begin_nested():
                        # Delete all old Symlink DB records for this item.
                        for symlink in symlinks:
                            await session.delete(symlink)
                        await session.flush()

                        # Recreate a symlink for each live source_path.
                        new_targets: list[MediaScanTarget] = []
                        for symlink in live_symlinks:
                            try:
                                new_symlink = await symlink_manager.create_symlink(
                                    session, media_item, symlink.source_path
                                )
                                new_targets.append(
                                    MediaScanTarget(
                                        media_type=media_item.media_type.value,
                                        target_path=new_symlink.target_path,
                                    )
                                )
                            except (SourceNotFoundError, SymlinkCreationError) as exc:
                                logger.warning(
                                    "execute_symlink_health: symlink recreation failed "
                                    "for item_id=%d source=%r — %s (will re-queue)",
                                    item_id,
                                    symlink.source_path,
                                    exc,
                                )
                                # Treat the whole item as a re-queue fallback.
                                raise

                        await queue_manager.force_transition(
                            session, item_id, QueueState.COMPLETE
                        )

                    result.recreated += 1
                    result.recreated_ids.append(item_id)
                    result.media_scan_targets.extend(new_targets)
                    logger.info(
                        "execute_symlink_health: recreated item_id=%d "
                        "(%d symlinks rebuilt)",
                        item_id,
                        len(new_targets),
                    )

                except (SourceNotFoundError, SymlinkCreationError):
                    # Savepoint was rolled back (DB clean) but any symlinks,
                    # NFOs, or directories created on disk by successful
                    # create_symlink calls before the failure remain.  Clean
                    # them up before falling through to re-queue.
                    for target in new_targets:
                        try:
                            tp = target.target_path
                            if await asyncio.to_thread(os.path.islink, tp):
                                await asyncio.to_thread(os.unlink, tp)
                            # Remove companion NFO if present.
                            nfo = os.path.splitext(tp)[0] + ".nfo"
                            if await asyncio.to_thread(os.path.exists, nfo):
                                await asyncio.to_thread(os.unlink, nfo)
                        except OSError as cleanup_exc:
                            logger.warning(
                                "execute_symlink_health: disk cleanup failed "
                                "for %r — %s",
                                target.target_path,
                                cleanup_exc,
                            )
                    new_targets.clear()

                    async with await session.begin_nested():
                        removed = await _remove_symlinks_for_item(session, item_id)
                        result.symlinks_removed_from_disk += removed
                        await queue_manager.force_transition(
                            session, item_id, QueueState.WANTED
                        )
                    result.requeued += 1
                    logger.info(
                        "execute_symlink_health: fallback requeued item_id=%d "
                        "(removed %d symlinks from disk)",
                        item_id,
                        removed,
                    )

            else:
                # ----------------------------------------------------------
                # No source files found — remove symlinks and re-queue.
                # ----------------------------------------------------------
                async with await session.begin_nested():
                    removed = await _remove_symlinks_for_item(session, item_id)
                    result.symlinks_removed_from_disk += removed
                    await queue_manager.force_transition(
                        session, item_id, QueueState.WANTED
                    )
                result.requeued += 1
                logger.info(
                    "execute_symlink_health: requeued item_id=%d "
                    "(removed %d symlinks from disk)",
                    item_id,
                    removed,
                )

        except ItemNotFoundError:
            msg = f"item_id={item_id}: MediaItem not found — skipped"
            logger.warning("execute_symlink_health: %s", msg)
            result.errors.append(msg)
        except Exception as exc:
            msg = f"item_id={item_id}: requeue failed — {exc}"
            logger.error("execute_symlink_health: %s", msg, exc_info=True)
            result.errors.append(msg)

    # ------------------------------------------------------------------
    # Process cleanup_ids — clean symlinks only, leave item state
    # ------------------------------------------------------------------
    for item_id in request.cleanup_ids:
        try:
            async with await session.begin_nested():
                removed = await _remove_symlinks_for_item(session, item_id)
                result.symlinks_removed_from_disk += removed

            result.cleaned += 1
            logger.info(
                "execute_symlink_health: cleaned item_id=%d "
                "(removed %d symlinks from disk)",
                item_id,
                removed,
            )

        except Exception as exc:
            msg = f"item_id={item_id}: cleanup failed — {exc}"
            logger.error("execute_symlink_health: %s", msg)
            result.errors.append(msg)

    logger.info(
        "execute_symlink_health: recreated=%d requeued=%d cleaned=%d "
        "symlinks_removed_from_disk=%d errors=%d",
        result.recreated,
        result.requeued,
        result.cleaned,
        result.symlinks_removed_from_disk,
        len(result.errors),
    )

    return result
