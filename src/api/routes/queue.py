"""Queue management endpoints."""

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.deps import get_db
from src.core.queue_manager import ItemNotFoundError, queue_manager
from src.core.symlink_manager import symlink_manager
from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.scrape_result import ScrapeLog
from src.models.torrent import RdTorrent
from src.services.real_debrid import rd_client

logger = logging.getLogger(__name__)

router = APIRouter()


# --- Pydantic schemas ---

class MediaItemResponse(BaseModel):
    id: int
    imdb_id: str | None = None
    tmdb_id: str | None = None
    title: str
    year: int | None = None
    media_type: str
    season: int | None = None
    episode: int | None = None
    state: str
    quality_profile: str | None = None
    retry_count: int = 0
    next_retry_at: str | None = None
    state_changed_at: str | None = None
    created_at: str | None = None

    @classmethod
    def from_orm_item(cls, item: MediaItem) -> "MediaItemResponse":
        """Build a response schema from a MediaItem ORM object."""
        return cls(
            id=item.id,
            imdb_id=item.imdb_id,
            tmdb_id=item.tmdb_id,
            title=item.title,
            year=item.year,
            media_type=item.media_type.value,
            season=item.season,
            episode=item.episode,
            state=item.state.value,
            quality_profile=item.quality_profile,
            retry_count=item.retry_count,
            next_retry_at=item.next_retry_at.isoformat() if item.next_retry_at else None,
            state_changed_at=item.state_changed_at.isoformat() if item.state_changed_at else None,
            created_at=item.created_at.isoformat() if item.created_at else None,
        )


class QueueListResponse(BaseModel):
    items: list[MediaItemResponse]
    total: int
    page: int
    page_size: int


class ScrapeLogEntry(BaseModel):
    id: int
    scraper: str
    query_params: str | None = None
    results_count: int | None = None
    results_summary: str | None = None
    selected_result: str | None = None
    duration_ms: int | None = None
    scraped_at: str | None = None


class ItemDetailResponse(BaseModel):
    item: MediaItemResponse
    scrape_logs: list[ScrapeLogEntry]
    torrents: list[dict[str, Any]]


class StateChangeRequest(BaseModel):
    state: str


class BulkRequest(BaseModel):
    ids: list[int]


class BulkResponse(BaseModel):
    status: str
    processed: int
    errors: list[str]


# --- Endpoints ---
# NOTE: /bulk/retry and /bulk/remove are declared BEFORE /{item_id}/retry and
# /{item_id}/state so that FastAPI resolves the literal path segments first.
# Moving them after the parameterised routes causes FastAPI to match "bulk" as
# an integer item_id and return 422.

@router.get("")
async def list_queue(
    state: str | None = Query(None, description="Filter by state"),
    media_type: str | None = Query(None, description="Filter by media type"),
    title: str | None = Query(None, description="Search by title"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    session: AsyncSession = Depends(get_db),
) -> QueueListResponse:
    """List items with filtering (state, media_type, title search) and pagination."""
    stmt = select(MediaItem)

    if state:
        try:
            state_enum = QueueState(state)
            stmt = stmt.where(MediaItem.state == state_enum)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid state: {state}")

    if media_type:
        try:
            type_enum = MediaType(media_type)
            stmt = stmt.where(MediaItem.media_type == type_enum)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid media_type: {media_type}")

    if title:
        escaped = title.replace("%", "\\%").replace("_", "\\_")
        stmt = stmt.where(MediaItem.title.ilike(f"%{escaped}%", escape="\\"))

    # Get total count before pagination
    count_stmt = select(func.count()).select_from(stmt.subquery())
    total_result = await session.execute(count_stmt)
    total = total_result.scalar() or 0

    # Apply ordering and pagination
    offset = (page - 1) * page_size
    stmt = stmt.order_by(MediaItem.state_changed_at.desc().nullslast()).offset(offset).limit(page_size)

    result = await session.execute(stmt)
    items = result.scalars().all()

    logger.debug(
        "list_queue: state=%s media_type=%s title=%s page=%d total=%d",
        state,
        media_type,
        title,
        page,
        total,
    )

    return QueueListResponse(
        items=[MediaItemResponse.from_orm_item(item) for item in items],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.post("/bulk/retry")
async def bulk_retry(
    body: BulkRequest,
    session: AsyncSession = Depends(get_db),
) -> BulkResponse:
    """Bulk retry: force-transition a list of items to WANTED in a single transaction."""
    processed = 0
    errors: list[str] = []

    for item_id in body.ids:
        try:
            await queue_manager.force_transition(session, item_id, QueueState.WANTED)
            processed += 1
        except ItemNotFoundError:
            errors.append(f"Item {item_id} not found")
        except Exception as exc:
            errors.append(f"Item {item_id}: {exc}")

    await session.commit()

    logger.info(
        "bulk_retry: processed=%d errors=%d", processed, len(errors)
    )

    return BulkResponse(
        status="ok" if not errors else "partial",
        processed=processed,
        errors=errors,
    )


@router.post("/bulk/remove")
async def bulk_remove(
    body: BulkRequest,
    session: AsyncSession = Depends(get_db),
) -> BulkResponse:
    """Bulk remove: delete a list of items and their associated data."""
    processed = 0
    errors: list[str] = []

    for item_id in body.ids:
        try:
            result = await session.execute(
                select(MediaItem).where(MediaItem.id == item_id)
            )
            item = result.scalar_one_or_none()
            if item is None:
                errors.append(f"Item {item_id} not found")
                continue

            # Remove symlinks from disk and DB
            await symlink_manager.remove_symlink(session, item_id)

            # Delete associated RD torrents
            torrents_result = await session.execute(
                select(RdTorrent).where(RdTorrent.media_item_id == item_id)
            )
            for torrent in torrents_result.scalars().all():
                if torrent.rd_id:
                    try:
                        await rd_client.delete_torrent(torrent.rd_id)
                    except Exception as exc:
                        logger.warning(
                            "bulk_remove: failed to delete rd torrent rd_id=%s: %s",
                            torrent.rd_id,
                            exc,
                        )
                await session.delete(torrent)

            # Delete scrape logs
            logs_result = await session.execute(
                select(ScrapeLog).where(ScrapeLog.media_item_id == item_id)
            )
            for log in logs_result.scalars().all():
                await session.delete(log)

            await session.delete(item)
            processed += 1

        except Exception as exc:
            errors.append(f"Item {item_id}: {exc}")

    await session.commit()

    logger.info(
        "bulk_remove: processed=%d errors=%d", processed, len(errors)
    )

    return BulkResponse(
        status="ok" if not errors else "partial",
        processed=processed,
        errors=errors,
    )


@router.get("/{item_id}")
async def get_item(
    item_id: int,
    session: AsyncSession = Depends(get_db),
) -> ItemDetailResponse:
    """Item detail with full scrape_log history and associated torrents."""
    result = await session.execute(
        select(MediaItem).where(MediaItem.id == item_id)
    )
    item = result.scalar_one_or_none()
    if item is None:
        raise HTTPException(status_code=404, detail=f"Item {item_id} not found")

    # Fetch scrape logs ordered newest-first
    logs_result = await session.execute(
        select(ScrapeLog)
        .where(ScrapeLog.media_item_id == item_id)
        .order_by(ScrapeLog.scraped_at.desc())
    )
    logs = logs_result.scalars().all()
    scrape_logs = [
        ScrapeLogEntry(
            id=log.id,
            scraper=log.scraper,
            query_params=log.query_params,
            results_count=log.results_count,
            results_summary=log.results_summary,
            selected_result=log.selected_result,
            duration_ms=log.duration_ms,
            scraped_at=log.scraped_at.isoformat() if log.scraped_at else None,
        )
        for log in logs
    ]

    # Fetch associated RD torrents
    torrents_result = await session.execute(
        select(RdTorrent).where(RdTorrent.media_item_id == item_id)
    )
    torrents = [
        {
            "id": t.id,
            "rd_id": t.rd_id,
            "info_hash": t.info_hash,
            "filename": t.filename,
            "filesize": t.filesize,
            "resolution": t.resolution,
            "cached": t.cached,
            "status": t.status.value,
        }
        for t in torrents_result.scalars().all()
    ]

    logger.debug(
        "get_item: id=%d state=%s scrape_logs=%d torrents=%d",
        item_id,
        item.state.value,
        len(scrape_logs),
        len(torrents),
    )

    return ItemDetailResponse(
        item=MediaItemResponse.from_orm_item(item),
        scrape_logs=scrape_logs,
        torrents=torrents,
    )


@router.post("/{item_id}/retry")
async def retry_item(
    item_id: int,
    session: AsyncSession = Depends(get_db),
) -> MediaItemResponse:
    """Force retry a sleeping/dormant item by transitioning to WANTED."""
    try:
        item = await queue_manager.force_transition(session, item_id, QueueState.WANTED)
        await session.commit()
        logger.info("retry_item: item id=%d transitioned to WANTED", item_id)
        return MediaItemResponse.from_orm_item(item)
    except ItemNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/{item_id}/state")
async def change_state(
    item_id: int,
    body: StateChangeRequest,
    session: AsyncSession = Depends(get_db),
) -> MediaItemResponse:
    """Manually change item state (force transition, bypasses validation)."""
    try:
        new_state = QueueState(body.state)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid state: {body.state}")

    try:
        item = await queue_manager.force_transition(session, item_id, new_state)
        await session.commit()
        logger.info(
            "change_state: item id=%d forced to state=%s", item_id, new_state.value
        )
        return MediaItemResponse.from_orm_item(item)
    except ItemNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.delete("/{item_id}")
async def remove_item(
    item_id: int,
    session: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Remove item and all associated symlinks, RD torrents, and scrape logs."""
    result = await session.execute(
        select(MediaItem).where(MediaItem.id == item_id)
    )
    item = result.scalar_one_or_none()
    if item is None:
        raise HTTPException(status_code=404, detail=f"Item {item_id} not found")

    # Remove symlinks from disk and the symlinks table
    symlinks_removed = await symlink_manager.remove_symlink(session, item_id)

    # Delete associated RD torrents from the RD account and then from the DB
    torrents_result = await session.execute(
        select(RdTorrent).where(RdTorrent.media_item_id == item_id)
    )
    rd_torrents = torrents_result.scalars().all()
    for torrent in rd_torrents:
        if torrent.rd_id:
            try:
                await rd_client.delete_torrent(torrent.rd_id)
            except Exception as exc:
                logger.warning(
                    "remove_item: failed to delete rd torrent rd_id=%s: %s",
                    torrent.rd_id,
                    exc,
                )
        await session.delete(torrent)

    # Delete scrape logs
    logs_result = await session.execute(
        select(ScrapeLog).where(ScrapeLog.media_item_id == item_id)
    )
    for log in logs_result.scalars().all():
        await session.delete(log)

    # Delete the media item itself
    await session.delete(item)
    await session.commit()

    logger.info(
        "remove_item: deleted id=%d symlinks_removed=%d torrents_removed=%d",
        item_id,
        symlinks_removed,
        len(rd_torrents),
    )

    return {
        "status": "ok",
        "id": item_id,
        "symlinks_removed": symlinks_removed,
        "torrents_removed": len(rd_torrents),
    }
