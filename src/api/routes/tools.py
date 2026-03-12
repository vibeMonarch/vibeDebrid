"""Tools page endpoints — library migration and other utility tools."""

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.deps import get_db
from src.core.backfill import (
    BackfillResult,
    DeduplicateResult,
    DuplicateGroup,
    _backfill_lock,
    backfill_tmdb_ids,
    find_duplicates,
    remove_duplicates,
)
from src.core.migration import (
    DuplicateMatch,
    FoundItem,
    MigrationPreview,
    execute_migration,
    preview_migration,
)
from src.core.cleanup import (
    CleanupPreview,
    CleanupResult,
    assess_migration_items,
    build_cleanup_preview,
    execute_cleanup,
)
from src.core.rd_bridge import BridgeResult, bridge_rd_torrents
from src.core.rd_cleanup import (
    RdCleanupExecuteRequest,
    RdCleanupExecuteResult,
    RdCleanupScan,
    execute_rd_cleanup,
    scan_rd_account,
)
from src.core.symlink_health import (
    SymlinkHealthExecuteRequest,
    SymlinkHealthResult,
    SymlinkHealthScan,
    execute_symlink_health,
    scan_symlink_health,
)

logger = logging.getLogger(__name__)

router = APIRouter()

# Prevents two concurrent migration executions from corrupting the database
# or filesystem state simultaneously.
_migration_lock = asyncio.Lock()

# Prevents two concurrent backfill/dedup runs from racing each other.
_cleanup_lock = asyncio.Lock()


@asynccontextmanager
async def _try_acquire(lock: asyncio.Lock, detail: str) -> AsyncGenerator[None, None]:
    """Attempt to acquire *lock* without blocking; raise HTTP 409 if already held.

    Best-effort non-blocking check.  Under asyncio cooperative scheduling,
    the check-then-acquire is effectively atomic for uncontested locks.  If
    two requests arrive simultaneously, the second may block until the first
    completes rather than receiving 409.

    Args:
        lock: The asyncio.Lock to acquire.
        detail: Human-readable message for the 409 response body.

    Raises:
        HTTPException: HTTP 409 when the lock is already held.
        HTTPException: HTTP 409 when the lock is acquired but the body raises
            an HTTPException (re-raised transparently).
    """
    if lock.locked():
        raise HTTPException(status_code=409, detail=detail)
    await lock.acquire()
    try:
        yield
    finally:
        lock.release()


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------


class MigrationPreviewRequest(BaseModel):
    """Request body for the migration preview endpoint."""

    movies_path: str
    shows_path: str


class MigrationExecuteRequest(BaseModel):
    """Request body for the migration execute endpoint."""

    movies_path: str
    shows_path: str


def _found_item_to_dict(fi: FoundItem) -> dict[str, Any]:
    """Serialise a FoundItem to a plain dict for JSON responses."""
    return {
        "title": fi.title,
        "year": fi.year,
        "media_type": fi.media_type,
        "season": fi.season,
        "episode": fi.episode,
        "imdb_id": fi.imdb_id,
        "source_path": fi.source_path,
        "target_path": fi.target_path,
        "is_symlink": fi.is_symlink,
        "resolution": fi.resolution,
    }


def _duplicate_to_dict(dm: DuplicateMatch) -> dict[str, Any]:
    """Serialise a DuplicateMatch to a plain dict for JSON responses."""
    return {
        "found_item": _found_item_to_dict(dm.found_item),
        "existing_id": dm.existing_id,
        "existing_title": dm.existing_title,
        "match_reason": dm.match_reason,
    }


def _preview_to_dict(preview: MigrationPreview) -> dict[str, Any]:
    """Serialise a MigrationPreview to a plain dict for JSON responses."""
    return {
        "found_items": [_found_item_to_dict(fi) for fi in preview.found_items],
        "duplicates": [_duplicate_to_dict(dm) for dm in preview.duplicates],
        "to_move": preview.to_move,
        "errors": preview.errors,
        "summary": preview.summary,
    }


# ---------------------------------------------------------------------------
# Page route
# ---------------------------------------------------------------------------


@router.get("/tools", response_class=HTMLResponse, tags=["pages"])
async def tools_page(request: Request) -> HTMLResponse:
    """Render the Tools page."""
    from src.config import settings
    from src.main import templates

    return templates.TemplateResponse(
        "tools.html",
        {
            "request": request,
            "active_page": "tools",
            "current_movies_path": settings.paths.library_movies,
            "current_shows_path": settings.paths.library_shows,
        },
    )


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------


@router.post("/api/tools/migration/preview", tags=["tools"])
async def migration_preview(
    req: MigrationPreviewRequest,
    session: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Scan library paths and return a preview of what the migration would do.

    Does NOT modify anything — purely informational.

    Args:
        req: Movies and shows root paths to scan.
        session: Injected database session.

    Returns:
        JSON with ``found_items``, ``duplicates``, ``to_move``, ``errors``,
        and a ``summary`` dict.

    Raises:
        HTTPException 400: When either path does not exist or is not a directory.
    """
    # Validate that both paths exist and are directories.
    for label, path in (("movies_path", req.movies_path), ("shows_path", req.shows_path)):
        path_exists = await _path_is_dir(path)
        if not path_exists:
            raise HTTPException(
                status_code=400,
                detail=f"{label} does not exist or is not a directory: {path!r}",
            )

    logger.info(
        "migration_preview: movies=%r shows=%r",
        req.movies_path,
        req.shows_path,
    )

    preview = await preview_migration(session, req.movies_path, req.shows_path)
    return _preview_to_dict(preview)


@router.post("/api/tools/migration/execute", tags=["tools"])
async def migration_execute(
    req: MigrationExecuteRequest,
    session: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Execute the migration: import found items, remove duplicates, move symlinks.

    Runs a full ``preview_migration`` first to get the current state, then
    calls ``execute_migration`` and commits the transaction explicitly.
    A module-level lock prevents concurrent migration runs (returns 409 if
    another execution is already in progress).

    Args:
        req: Movies and shows root paths.
        session: Injected database session.

    Returns:
        JSON with ``imported``, ``moved``, ``duplicates_removed``,
        ``config_updated``, and ``errors``.

    Raises:
        HTTPException 400: When either path does not exist or is not a directory.
    """
    for label, path in (("movies_path", req.movies_path), ("shows_path", req.shows_path)):
        path_exists = await _path_is_dir(path)
        if not path_exists:
            raise HTTPException(
                status_code=400,
                detail=f"{label} does not exist or is not a directory: {path!r}",
            )

    logger.info(
        "migration_execute: movies=%r shows=%r",
        req.movies_path,
        req.shows_path,
    )

    async with _try_acquire(_migration_lock, "Migration already in progress"):
        # Build preview first so execute_migration has the full picture.
        preview = await preview_migration(session, req.movies_path, req.shows_path)

        migration_result = await execute_migration(
            session, preview, req.movies_path, req.shows_path
        )

        try:
            await session.commit()
        except Exception as exc:
            logger.error("migration_execute: commit failed: %s", exc)
            raise HTTPException(
                status_code=500,
                detail=f"Migration completed but database commit failed: {exc}",
            ) from exc

    return {
        "imported": migration_result.imported,
        "moved": migration_result.moved,
        "duplicates_removed": migration_result.duplicates_removed,
        "config_updated": migration_result.config_updated,
        "errors": migration_result.errors,
    }


# ---------------------------------------------------------------------------
# Data Cleanup routes
# ---------------------------------------------------------------------------


class DeduplicateRequest(BaseModel):
    """Request body for the deduplicate endpoint."""

    groups: list[DuplicateGroup]
    """Duplicate groups as returned by GET /api/tools/duplicates."""


@router.post("/api/tools/bridge-rd", tags=["tools"])
async def bridge_rd_torrents_endpoint(
    session: AsyncSession = Depends(get_db),
) -> BridgeResult:
    """Link Real-Debrid account torrents to migrated MediaItems that lack an RdTorrent.

    Fetches all torrents from the RD account, then for each migration item that
    has a Symlink but no RdTorrent record, attempts to find the matching RD torrent
    by comparing the symlink's source directory name against RD filenames.

    Safe to run multiple times — items that already have an RdTorrent record are
    counted as ``already_bridged`` and skipped.  Returns 409 if another cleanup
    operation is already in progress.

    Args:
        session: Injected database session.

    Returns:
        BridgeResult with counts for all outcomes.
    """
    try:
        async with _try_acquire(
            _cleanup_lock, "A data cleanup operation is already in progress"
        ):
            result = await bridge_rd_torrents(session)
            await session.commit()
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("bridge_rd_torrents_endpoint: unexpected error: %s", exc)
        raise HTTPException(
            status_code=500,
            detail=f"Bridge operation failed: {exc}",
        ) from exc

    logger.info(
        "bridge_rd_torrents_endpoint: total_rd=%d migration_items=%d "
        "matched=%d already_bridged=%d unmatched=%d errors=%d",
        result.total_rd_torrents,
        result.total_migration_items,
        result.matched,
        result.already_bridged,
        result.unmatched_items,
        len(result.errors),
    )
    return result


@router.post("/api/tools/backfill", tags=["tools"])
async def run_backfill(
    session: AsyncSession = Depends(get_db),
) -> BackfillResult:
    """Resolve tmdb_id for all items where imdb_id is set but tmdb_id is NULL.

    Safe to run multiple times — items that already have a tmdb_id are untouched.
    The shared ``_backfill_lock`` in ``backfill.py`` prevents concurrent runs
    (whether triggered here or by the startup background task).  Returns 409 if
    one is already in progress.

    Args:
        session: Injected database session.

    Returns:
        BackfillResult with total/resolved/failed/updated_rows counts.
    """
    # Check the shared lock (also held by the background task in main.py).
    # _backfill_lock is checked explicitly because it may be held by the
    # startup background task even when _cleanup_lock is free.
    if _backfill_lock.locked():
        raise HTTPException(
            status_code=409,
            detail="A data cleanup operation is already in progress",
        )

    try:
        async with _try_acquire(
            _cleanup_lock, "A data cleanup operation is already in progress"
        ):
            # backfill_tmdb_ids acquires _backfill_lock internally.
            result = await backfill_tmdb_ids(session)
            await session.commit()
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("run_backfill: unexpected error: %s", exc)
        raise HTTPException(
            status_code=500,
            detail=f"Backfill failed: {exc}",
        ) from exc

    logger.info(
        "run_backfill: total=%d resolved=%d failed=%d rows_updated=%d",
        result.total,
        result.resolved,
        result.failed,
        result.updated_rows,
    )
    return result


@router.get("/api/tools/duplicates", tags=["tools"])
async def get_duplicates(
    session: AsyncSession = Depends(get_db),
) -> list[DuplicateGroup]:
    """Preview duplicate migration items grouped by (imdb_id, season, episode).

    Only considers items with source='migration'.  Does NOT modify anything.

    Args:
        session: Injected database session.

    Returns:
        List of DuplicateGroup objects.
    """
    try:
        groups = await find_duplicates(session)
    except Exception as exc:
        logger.error("get_duplicates: unexpected error: %s", exc)
        raise HTTPException(
            status_code=500,
            detail=f"Duplicate scan failed: {exc}",
        ) from exc

    return groups


@router.post("/api/tools/deduplicate", tags=["tools"])
async def deduplicate(
    req: DeduplicateRequest,
    session: AsyncSession = Depends(get_db),
) -> DeduplicateResult:
    """Delete duplicate MediaItems identified by the provided groups.

    Also removes associated scrape_log, rd_torrents, and symlink records in
    FK-safe order.  Returns 409 if a cleanup operation is already running.

    Args:
        req: Duplicate groups (as returned by GET /api/tools/duplicates).
        session: Injected database session.

    Returns:
        DeduplicateResult with counts of removed/kept/groups.
    """
    if not req.groups:
        return DeduplicateResult()

    try:
        async with _try_acquire(
            _cleanup_lock, "A data cleanup operation is already in progress"
        ):
            result = await remove_duplicates(session, req.groups)
            await session.commit()
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("deduplicate: unexpected error: %s", exc)
        raise HTTPException(
            status_code=500,
            detail=f"Deduplication failed: {exc}",
        ) from exc

    logger.info(
        "deduplicate: removed=%d kept=%d groups=%d rd_to_delete=%d",
        result.removed,
        result.kept,
        result.groups,
        len(result.rd_ids_to_delete),
    )

    # ------------------------------------------------------------------
    # Delete RD torrents from the account after the DB commit.
    # Sequential iteration stops immediately on the first rate-limit
    # response so no further API budget is wasted.  The DB cleanup has
    # already succeeded at this point, so failures here are non-fatal.
    # ------------------------------------------------------------------
    if result.rd_ids_to_delete:
        from src.services.real_debrid import RealDebridClient, RealDebridRateLimitError

        rd_client = RealDebridClient()
        rd_deleted = 0
        rd_failed: list[str] = []
        rate_limited = False

        for rd_id in result.rd_ids_to_delete:
            if rate_limited:
                rd_failed.append(rd_id)
                continue
            try:
                await rd_client.delete_torrent(rd_id)
                rd_deleted += 1
            except RealDebridRateLimitError:
                rate_limited = True
                rd_failed.append(rd_id)
                logger.warning(
                    "deduplicate: RD rate limit hit, stopping deletions "
                    "(%d remaining)",
                    len(result.rd_ids_to_delete) - rd_deleted - len(rd_failed),
                )
            except Exception as exc:
                logger.warning(
                    "deduplicate: RD delete failed rd_id=%s: %s", rd_id, exc
                )
                rd_failed.append(rd_id)

        result.rd_torrents_deleted = rd_deleted
        result.rd_torrents_failed = len(rd_failed)

        logger.info(
            "deduplicate: RD account cleanup — deleted=%d failed=%d",
            result.rd_torrents_deleted,
            result.rd_torrents_failed,
        )

    return result


# ---------------------------------------------------------------------------
# Smart Cleanup routes
# ---------------------------------------------------------------------------


class CleanupExecuteRequest(BaseModel):
    """Request body for the smart cleanup execute endpoint.

    The client only sends a confirmation flag — all item IDs to remove are
    determined server-side by re-running the assessment, so a stale or crafted
    request body cannot cause non-duplicate items to be deleted.
    """

    confirm: bool = True
    """Must be True to proceed (default True for backwards compatibility)."""


@router.get("/api/tools/cleanup/preview", tags=["tools"])
async def cleanup_preview_endpoint(
    session: AsyncSession = Depends(get_db),
) -> CleanupPreview:
    """Assess migration items and build a liveness-aware cleanup plan.

    Checks the Zurg mount filesystem and RD torrent registry to determine
    which duplicate to keep (LIVE > BRIDGED > DEAD).  Does NOT modify
    anything — purely informational.

    Returns 409 when another cleanup operation is already in progress.

    Args:
        session: Injected database session.

    Returns:
        CleanupPreview with groups, liveness counts, and RD deletion plan.
    """
    if _cleanup_lock.locked():
        raise HTTPException(
            status_code=409,
            detail="A data cleanup operation is already in progress",
        )

    try:
        assessed = await assess_migration_items(session)
        preview = await build_cleanup_preview(session, assessed)
    except Exception as exc:
        logger.error("cleanup_preview_endpoint: unexpected error: %s", exc)
        raise HTTPException(
            status_code=500,
            detail=f"Cleanup preview failed: {exc}",
        ) from exc

    logger.info(
        "cleanup_preview_endpoint: groups=%d to_remove=%d rd_to_delete=%d",
        len(preview.groups),
        preview.total_to_remove,
        len(preview.rd_ids_to_delete),
    )
    return preview


@router.post("/api/tools/cleanup/execute", tags=["tools"])
async def cleanup_execute_endpoint(
    req: CleanupExecuteRequest,
    session: AsyncSession = Depends(get_db),
) -> CleanupResult:
    """Execute liveness-aware duplicate cleanup.

    Re-runs the full server-side assessment on every call so the set of items
    to delete is always derived from live database state — the request body
    carries only a confirmation flag and no item IDs.

    Deletes DB records in FK-safe order (scrape_log → rd_torrents → symlinks →
    media_items) inside a savepoint for atomicity, removes broken symlinks from
    disk, then calls the RD API to delete orphaned account torrents
    sequentially (stops on first rate-limit response).

    Returns 409 when another cleanup operation is already in progress.

    Args:
        req: Confirmation flag (confirm=True required to proceed).
        session: Injected database session.

    Returns:
        CleanupResult with counts for all outcomes.
    """
    if not req.confirm:
        raise HTTPException(status_code=400, detail="confirm must be true to proceed")

    try:
        async with _try_acquire(
            _cleanup_lock, "A data cleanup operation is already in progress"
        ):
            # Re-run the full server-side assessment so we never trust client-
            # supplied item IDs.  A stale preview or crafted request body cannot
            # cause non-duplicate items to be deleted.
            assessed = await assess_migration_items(session)
            current_preview = await build_cleanup_preview(session, assessed)

            if not current_preview.groups:
                return CleanupResult()

            result = await execute_cleanup(session, current_preview)
            await session.commit()

    except HTTPException:
        raise
    except Exception as exc:
        await session.rollback()
        logger.error("cleanup_execute_endpoint: unexpected error: %s", exc)
        raise HTTPException(
            status_code=500,
            detail=f"Cleanup execution failed: {exc}",
        ) from exc

    rd_ids_to_delete = current_preview.rd_ids_to_delete

    logger.info(
        "cleanup_execute_endpoint: removed=%d kept=%d symlinks_disk=%d rd_to_delete=%d",
        result.items_removed,
        result.items_kept,
        result.symlinks_deleted_from_disk,
        len(rd_ids_to_delete),
    )

    # ------------------------------------------------------------------
    # Delete RD torrents from the account after the DB commit.
    # Sequential iteration stops immediately on the first rate-limit
    # response so no further API budget is wasted.
    # ------------------------------------------------------------------
    if rd_ids_to_delete:
        from src.services.real_debrid import RealDebridClient, RealDebridRateLimitError

        rd_client = RealDebridClient()
        rd_deleted = 0
        rd_failed: list[str] = []
        rate_limited = False

        for rd_id in rd_ids_to_delete:
            if rate_limited:
                rd_failed.append(rd_id)
                continue
            try:
                await rd_client.delete_torrent(rd_id)
                rd_deleted += 1
            except RealDebridRateLimitError:
                rate_limited = True
                rd_failed.append(rd_id)
                logger.warning(
                    "cleanup_execute_endpoint: RD rate limit hit, stopping deletions "
                    "(%d remaining)",
                    len(rd_ids_to_delete) - rd_deleted - len(rd_failed),
                )
            except Exception as exc:
                logger.warning(
                    "cleanup_execute_endpoint: RD delete failed rd_id=%s: %s",
                    rd_id,
                    exc,
                )
                rd_failed.append(rd_id)

        result.rd_torrents_deleted = rd_deleted
        result.rd_torrents_failed = len(rd_failed)

        logger.info(
            "cleanup_execute_endpoint: RD account cleanup — deleted=%d failed=%d",
            result.rd_torrents_deleted,
            result.rd_torrents_failed,
        )

    return result


# ---------------------------------------------------------------------------
# RD Account Cleanup routes (Phase 2)
# ---------------------------------------------------------------------------


@router.post("/api/tools/rd-cleanup/scan", tags=["tools"])
async def rd_cleanup_scan(
    session: AsyncSession = Depends(get_db),
) -> RdCleanupScan:
    """Fetch and categorize all torrents in the user's Real-Debrid account.

    Assigns every RD account torrent to a category bucket: Protected, Dead,
    Stale, Duplicate, or Orphaned.  Results are cached for up to 5 minutes so
    that a subsequent execute call can reuse the data without a second round-trip.

    This is a read-only operation — nothing is modified.  Returns 409 when
    another cleanup operation is already in progress.

    Args:
        session: Injected database session (read-only).

    Returns:
        RdCleanupScan with per-category summaries and per-torrent detail.
    """
    try:
        async with _try_acquire(
            _cleanup_lock, "Another cleanup operation is in progress"
        ):
            result = await scan_rd_account(session)
    except HTTPException:
        raise
    except Exception as exc:
        from src.services.real_debrid import RealDebridRateLimitError

        if isinstance(exc, RealDebridRateLimitError):
            logger.warning("rd_cleanup_scan: RD rate limit hit: %s", exc)
            raise HTTPException(
                status_code=429,
                detail="Real-Debrid rate limit reached — try again later",
            )
        logger.error("rd_cleanup_scan: unexpected error: %s", exc)
        raise HTTPException(
            status_code=500,
            detail="RD cleanup scan failed — check server logs",
        )

    logger.info(
        "rd_cleanup_scan: total=%d warnings=%d",
        result.total_torrents,
        len(result.warnings),
    )
    return result


@router.post("/api/tools/rd-cleanup/execute", tags=["tools"])
async def rd_cleanup_execute(
    req: RdCleanupExecuteRequest,
    session: AsyncSession = Depends(get_db),
) -> RdCleanupExecuteResult:
    """Delete the specified RD account torrents after safety checks.

    Rejects any rd_id that resolves to a PROTECTED torrent (linked to a local
    MediaItem or Symlink).  Unknown IDs are counted as ``rejected_not_found``
    and skipped.  Deletions are issued sequentially and stop immediately on the
    first rate-limit response.

    After each successful deletion, the local dedup registry is updated via
    ``mark_torrent_removed`` so future scrape decisions stay consistent.

    Returns 400 when ``rd_ids`` is empty, 409 when another cleanup operation is
    already in progress, 429 on a Real-Debrid rate-limit error.

    Args:
        req: List of RD torrent IDs the user wants deleted.
        session: Injected database session.

    Returns:
        RdCleanupExecuteResult with counts for all outcomes.
    """
    if not req.rd_ids:
        raise HTTPException(status_code=400, detail="rd_ids must not be empty")

    try:
        async with _try_acquire(
            _cleanup_lock, "Another cleanup operation is in progress"
        ):
            result = await execute_rd_cleanup(session, req.rd_ids)
            await session.commit()
    except HTTPException:
        raise
    except Exception as exc:
        from src.services.real_debrid import RealDebridRateLimitError

        if isinstance(exc, RealDebridRateLimitError):
            logger.warning("rd_cleanup_execute: RD rate limit hit: %s", exc)
            raise HTTPException(
                status_code=429,
                detail="Real-Debrid rate limit reached — try again later",
            )
        logger.error("rd_cleanup_execute: unexpected error: %s", exc)
        raise HTTPException(
            status_code=500,
            detail="RD cleanup execute failed — check server logs",
        )

    logger.info(
        "rd_cleanup_execute: requested=%d deleted=%d failed=%d "
        "rejected_protected=%d rejected_not_found=%d rate_limited=%s",
        result.requested,
        result.deleted,
        result.failed,
        result.rejected_protected,
        result.rejected_not_found,
        result.rate_limited,
    )
    return result


# ---------------------------------------------------------------------------
# Symlink Health routes
# ---------------------------------------------------------------------------


@router.post("/api/tools/symlink-health/scan", tags=["tools"])
async def symlink_health_scan_endpoint(
    session: AsyncSession = Depends(get_db),
) -> SymlinkHealthScan:
    """Scan symlinks and identify broken ones with recovery classification.

    Checks mount availability first; returns an empty scan with an error
    message when the mount is down to prevent false positives.  Classifies
    each broken item as RECOVERABLE (matching file found in mount_index) or
    DEAD (no match found).  Returns 409 when another cleanup operation is
    already in progress.

    Args:
        session: Injected database session.

    Returns:
        SymlinkHealthScan with counts and per-item detail for broken symlinks.
    """
    try:
        async with _try_acquire(
            _cleanup_lock, "Another cleanup operation is in progress"
        ):
            result = await scan_symlink_health(session)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("symlink_health_scan_endpoint: unexpected error: %s", exc)
        raise HTTPException(
            status_code=500,
            detail="Symlink health scan failed",
        ) from exc

    logger.info(
        "symlink_health_scan_endpoint: total=%d healthy=%d broken=%d "
        "recoverable=%d dead=%d errors=%d",
        result.total_symlinks,
        result.healthy,
        result.broken,
        result.recoverable,
        result.dead,
        len(result.errors),
    )
    return result


@router.post("/api/tools/symlink-health/execute", tags=["tools"])
async def symlink_health_execute_endpoint(
    req: SymlinkHealthExecuteRequest,
    session: AsyncSession = Depends(get_db),
) -> SymlinkHealthResult:
    """Re-queue recoverable items and/or clean up dead symlink records.

    ``requeue_ids`` — removes symlink DB records and disk files, then
    transitions each item to WANTED so the scrape pipeline picks it up again.

    ``cleanup_ids`` — removes symlink DB records and disk files only; item
    state is left unchanged (useful for permanently removing dead records).

    Each item is processed inside a savepoint.  Returns 409 when another
    cleanup operation is already in progress.

    Args:
        req: Lists of item_ids to re-queue and/or clean up.
        session: Injected database session.

    Returns:
        SymlinkHealthResult with counts for all outcomes.
    """
    if not req.requeue_ids and not req.cleanup_ids:
        return SymlinkHealthResult()

    try:
        async with _try_acquire(
            _cleanup_lock, "Another cleanup operation is in progress"
        ):
            result = await execute_symlink_health(session, req)
            await session.commit()
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("symlink_health_execute_endpoint: unexpected error: %s", exc)
        raise HTTPException(
            status_code=500,
            detail="Symlink health execute failed",
        ) from exc

    logger.info(
        "symlink_health_execute_endpoint: requeued=%d cleaned=%d "
        "symlinks_removed_from_disk=%d errors=%d",
        result.requeued,
        result.cleaned,
        result.symlinks_removed_from_disk,
        len(result.errors),
    )
    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


async def _path_is_dir(path: str) -> bool:
    """Return True when *path* exists and is a directory (thread-safe).

    Args:
        path: Absolute filesystem path to check.

    Returns:
        True when the path is an existing directory, False otherwise.
    """
    import asyncio

    return await asyncio.to_thread(os.path.isdir, path)
