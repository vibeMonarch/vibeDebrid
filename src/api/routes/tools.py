"""Tools page endpoints — library migration and other utility tools."""

import asyncio
import logging
import os
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
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

logger = logging.getLogger(__name__)

router = APIRouter()

# Prevents two concurrent migration executions from corrupting the database
# or filesystem state simultaneously.
_migration_lock = asyncio.Lock()

# Prevents two concurrent backfill/dedup runs from racing each other.
_cleanup_lock = asyncio.Lock()


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

    if _migration_lock.locked():
        raise HTTPException(status_code=409, detail="Migration already in progress")

    try:
        await _migration_lock.acquire()

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

    finally:
        _migration_lock.release()

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
    if _backfill_lock.locked() or _cleanup_lock.locked():
        raise HTTPException(
            status_code=409,
            detail="A data cleanup operation is already in progress",
        )

    try:
        await _cleanup_lock.acquire()
        # backfill_tmdb_ids acquires _backfill_lock internally.
        result = await backfill_tmdb_ids(session)
        await session.commit()
    except Exception as exc:
        logger.error("run_backfill: unexpected error: %s", exc)
        raise HTTPException(
            status_code=500,
            detail=f"Backfill failed: {exc}",
        ) from exc
    finally:
        _cleanup_lock.release()

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
    if _cleanup_lock.locked():
        raise HTTPException(
            status_code=409,
            detail="A data cleanup operation is already in progress",
        )

    if not req.groups:
        return DeduplicateResult()

    try:
        await _cleanup_lock.acquire()
        result = await remove_duplicates(session, req.groups)
        await session.commit()
    except Exception as exc:
        logger.error("deduplicate: unexpected error: %s", exc)
        raise HTTPException(
            status_code=500,
            detail=f"Deduplication failed: {exc}",
        ) from exc
    finally:
        _cleanup_lock.release()

    logger.info(
        "deduplicate: removed=%d kept=%d groups=%d",
        result.removed,
        result.kept,
        result.groups,
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
