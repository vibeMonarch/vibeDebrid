"""Queue management endpoints."""

import asyncio
import json
import logging
import os
import re
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import delete, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.deps import get_db
from src.core.dedup import dedup_engine
from src.core.queue_manager import ItemNotFoundError, queue_manager
from src.core.symlink_manager import symlink_manager
from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.scrape_result import ScrapeLog
from src.models.symlink import Symlink
from src.models.torrent import RdTorrent, TorrentStatus
from src.services.real_debrid import RealDebridError, rd_client

logger = logging.getLogger(__name__)


async def _find_protected_rd_ids(
    session: AsyncSession,
    item_ids: set[int],
    rd_torrents: list[RdTorrent],
) -> set[str]:
    """Determine which RD torrent IDs should NOT be deleted from RD.

    Checks if any OTHER item (not in the delete set) has symlinks pointing
    to the same torrent directory. This prevents deleting a shared RD torrent
    that another item depends on.
    """
    if not rd_torrents:
        return set()

    # Collect target_path directories from items being deleted.
    item_id_list = list(item_ids)
    symlink_result = await session.execute(
        select(Symlink.target_path).where(Symlink.media_item_id.in_(item_id_list))
    )
    target_dirs: set[str] = set()
    for (target_path,) in symlink_result:
        if target_path:
            target_dirs.add(os.path.dirname(target_path))

    if not target_dirs:
        return set()

    # Check if any OTHER item has symlinks in those same directories.
    protected_dirs: set[str] = set()
    for target_dir in target_dirs:
        count_result = await session.execute(
            select(func.count()).select_from(Symlink).where(
                Symlink.media_item_id.not_in(item_id_list),
                Symlink.target_path.like(target_dir + "/%"),
            )
        )
        if count_result.scalar():
            protected_dirs.add(target_dir)

    if not protected_dirs:
        return set()

    # Map RD torrents to their directories via the item's symlinks.
    # Any rd_id whose content lives in a protected directory is protected.
    protected_rd_ids: set[str] = set()
    for torrent in rd_torrents:
        if not torrent.rd_id:
            continue
        # Get this torrent's item symlink directories.
        t_symlinks = await session.execute(
            select(Symlink.target_path).where(
                Symlink.media_item_id == torrent.media_item_id
            )
        )
        for (tp,) in t_symlinks:
            if tp and os.path.dirname(tp) in protected_dirs:
                protected_rd_ids.add(torrent.rd_id)
                break

    if protected_rd_ids:
        logger.info(
            "_find_protected_rd_ids: protecting %d RD torrent(s) shared with "
            "other items: %s",
            len(protected_rd_ids),
            protected_rd_ids,
        )

    return protected_rd_ids

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
    original_language: str | None = None

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
            original_language=item.original_language,
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


class SwitchTorrentRequest(BaseModel):
    magnet_or_hash: str
    release_title: str | None = None
    resolution: str | None = None
    size_bytes: int | None = None
    codec: str | None = None
    quality: str | None = None
    is_season_pack: bool = False


class SwitchTorrentResponse(BaseModel):
    status: str
    item_id: int
    rd_id: str


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
        title_filter = MediaItem.title.ilike(f"%{escaped}%", escape="\\")
        # Also match items sharing the same tmdb_id as any title-matched item,
        # so alternate-title entries (e.g. Japanese name) appear together.
        tmdb_subq = (
            select(MediaItem.tmdb_id)
            .where(title_filter, MediaItem.tmdb_id.is_not(None))
            .distinct()
            .scalar_subquery()
        )
        stmt = stmt.where(or_(title_filter, MediaItem.tmdb_id.in_(tmdb_subq)))

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
    all_rd_torrents: list[RdTorrent] = []
    delete_item_ids: set[int] = set()

    # First pass: collect all RD torrents and validate items exist.
    for item_id in body.ids:
        try:
            result = await session.execute(
                select(MediaItem).where(MediaItem.id == item_id)
            )
            item = result.scalar_one_or_none()
            if item is None:
                errors.append(f"Item {item_id} not found")
                continue
            delete_item_ids.add(item_id)

            torrents_result = await session.execute(
                select(RdTorrent).where(RdTorrent.media_item_id == item_id)
            )
            all_rd_torrents.extend(torrents_result.scalars().all())
        except Exception as exc:
            errors.append(f"Item {item_id}: {exc}")

    # Check for shared-torrent protection BEFORE deleting symlinks.
    protected_rd_ids = await _find_protected_rd_ids(
        session, delete_item_ids, all_rd_torrents
    )
    rd_ids_to_delete: set[str] = set()

    # Second pass: delete items and their associated data.
    for item_id in delete_item_ids:
        try:
            # Collect unprotected RD IDs, remove DB rows
            torrents_result = await session.execute(
                select(RdTorrent).where(RdTorrent.media_item_id == item_id)
            )
            for torrent in torrents_result.scalars().all():
                if torrent.rd_id and torrent.rd_id not in protected_rd_ids:
                    rd_ids_to_delete.add(torrent.rd_id)
                await session.delete(torrent)

            # Remove symlinks from disk and DB
            await symlink_manager.remove_symlink(session, item_id)

            # Delete scrape logs
            logs_result = await session.execute(
                select(ScrapeLog).where(ScrapeLog.media_item_id == item_id)
            )
            for log in logs_result.scalars().all():
                await session.delete(log)

            result = await session.execute(
                select(MediaItem).where(MediaItem.id == item_id)
            )
            item = result.scalar_one_or_none()
            if item:
                await session.delete(item)
            processed += 1

        except Exception as exc:
            errors.append(f"Item {item_id}: {exc}")

    # Commit DB changes first so the records are gone even if RD cleanup fails.
    # RD deletion is fire-and-forget: failures are logged but do not affect the
    # DB state that was already committed.
    await session.commit()

    # Delete RD torrents concurrently (deduped, max 5 at a time).
    # Results are collected from gather() return values to avoid concurrent
    # list mutation — _delete_rd raises on failure so exceptions are the signal.
    sem = asyncio.Semaphore(5)
    rd_ids_list = list(rd_ids_to_delete)

    async def _delete_rd(rd_id: str) -> None:
        async with sem:
            await rd_client.delete_torrent(rd_id)

    rd_failed: list[str] = []
    if rd_ids_list:
        results = await asyncio.gather(*[_delete_rd(rid) for rid in rd_ids_list], return_exceptions=True)
        for rd_id, result in zip(rd_ids_list, results):
            if isinstance(result, Exception):
                logger.warning("bulk_remove: failed to delete rd torrent rd_id=%s: %s", rd_id, result)
                rd_failed.append(rd_id)

    if rd_failed:
        errors.append(f"Failed to delete {len(rd_failed)} RD torrent(s) from account")

    logger.info(
        "bulk_remove: processed=%d rd_deleted=%d/%d errors=%d",
        processed, len(rd_ids_to_delete) - len(rd_failed),
        len(rd_ids_to_delete), len(errors),
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

    # Fetch associated RD torrents (direct FK or via hash-dedup scrape log)
    torrents_result = await session.execute(
        select(RdTorrent).where(RdTorrent.media_item_id == item_id)
    )
    torrent_rows = list(torrents_result.scalars().all())

    # If no direct FK link, check scrape log for hash-dedup entries and look up
    # the shared RdTorrent by info_hash. This covers episodes that skipped
    # register_torrent because their torrent was already registered by another item.
    if not torrent_rows:
        for log in logs:
            if log.selected_result:
                try:
                    sel = json.loads(log.selected_result)
                    if sel.get("action") == "hash_dedup_hit" and sel.get("info_hash"):
                        dedup_result = await session.execute(
                            select(RdTorrent).where(
                                RdTorrent.info_hash == sel["info_hash"].lower()
                            )
                        )
                        dedup_torrent = dedup_result.scalar_one_or_none()
                        if dedup_torrent:
                            torrent_rows.append(dedup_torrent)
                        break
                except (json.JSONDecodeError, TypeError):
                    pass

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
        for t in torrent_rows
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


_MANUAL_TRANSITION_ALLOWED = {QueueState.WANTED, QueueState.SLEEPING, QueueState.DORMANT}


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

    if new_state not in _MANUAL_TRANSITION_ALLOWED:
        allowed = sorted(s.value for s in _MANUAL_TRANSITION_ALLOWED)
        raise HTTPException(
            status_code=400,
            detail=f"Manual transition to {new_state.value!r} is not allowed. Allowed states: {allowed}",
        )

    try:
        item = await queue_manager.force_transition(session, item_id, new_state)
        await session.commit()
        logger.info(
            "change_state: item id=%d forced to state=%s", item_id, new_state.value
        )
        return MediaItemResponse.from_orm_item(item)
    except ItemNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


_HASH_RE = re.compile(r"^[0-9a-fA-F]{40}$")

# States that are safe to switch torrent from (not in-flight)
_SWITCH_ALLOWED_STATES = {
    QueueState.WANTED,
    QueueState.SLEEPING,
    QueueState.DORMANT,
    QueueState.COMPLETE,
    QueueState.DONE,
}


@router.post("/{item_id}/switch-torrent")
async def switch_torrent(
    item_id: int,
    body: SwitchTorrentRequest,
    session: AsyncSession = Depends(get_db),
) -> SwitchTorrentResponse:
    """Manually replace the torrent for an existing queue item.

    Adds the new torrent to Real-Debrid, cleans up the old torrent (symlinks,
    RD account entry unless shared), registers the new one in the dedup
    registry, and transitions the item to CHECKING so the file-validation
    stage can create symlinks.

    Args:
        item_id: Primary key of the MediaItem to update.
        body: New torrent details (hash or magnet, release title, metadata).
        session: Injected async database session.

    Returns:
        SwitchTorrentResponse with status, item_id, and new rd_id.

    Raises:
        HTTPException 404: When the item is not found.
        HTTPException 400: When the item is in an in-flight state, the input
            is invalid, or the hash matches the currently active torrent.
        HTTPException 500: When the Real-Debrid add operation fails.
    """
    # --- Step 1: Fetch item ---
    result = await session.execute(
        select(MediaItem).where(MediaItem.id == item_id)
    )
    item = result.scalar_one_or_none()
    if item is None:
        raise HTTPException(status_code=404, detail=f"Item {item_id} not found")

    # --- Step 2: Validate state ---
    if item.state not in _SWITCH_ALLOWED_STATES:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Cannot switch torrent while item is in state '{item.state.value}'. "
                f"Allowed states: {[s.value for s in _SWITCH_ALLOWED_STATES]}"
            ),
        )

    # --- Step 3: Normalise hash ---
    input_val = body.magnet_or_hash.strip()
    if _HASH_RE.match(input_val):
        info_hash: str = input_val.lower()
        magnet_uri = f"magnet:?xt=urn:btih:{info_hash}"
    elif input_val.startswith("magnet:"):
        magnet_uri = input_val
        hash_match = re.search(r"btih:([0-9a-fA-F]{40})", magnet_uri, re.IGNORECASE)
        if not hash_match:
            raise HTTPException(
                status_code=400,
                detail="Could not extract a 40-character info hash from the magnet URI",
            )
        info_hash = hash_match.group(1).lower()
    else:
        raise HTTPException(
            status_code=400,
            detail="Invalid input: must be a 40-character hex info hash or a magnet URI",
        )

    # --- Step 4: Guard against no-op (same hash already active) ---
    existing_active = await session.execute(
        select(RdTorrent).where(
            RdTorrent.media_item_id == item_id,
            RdTorrent.status == TorrentStatus.ACTIVE,
        )
    )
    old_torrent = existing_active.scalar_one_or_none()
    if old_torrent is not None and old_torrent.info_hash == info_hash:
        raise HTTPException(
            status_code=400,
            detail="Already using this torrent",
        )

    # --- Step 5: Add new torrent to Real-Debrid ---
    try:
        add_response = await rd_client.add_magnet(magnet_uri)
        new_rd_id: str = str(add_response.get("id", ""))
        if not new_rd_id:
            raise HTTPException(
                status_code=500,
                detail="Real-Debrid add_magnet returned an empty torrent ID",
            )
        logger.info(
            "switch_torrent: RD add_magnet succeeded new_rd_id=%s item_id=%d",
            new_rd_id,
            item_id,
        )
    except RealDebridError as exc:
        logger.error(
            "switch_torrent: RD add_magnet failed for item_id=%d: %s", item_id, exc
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to add torrent to Real-Debrid: {exc}",
        ) from exc

    try:
        await rd_client.select_files(new_rd_id, "all")
        logger.info(
            "switch_torrent: select_files succeeded new_rd_id=%s", new_rd_id
        )
    except RealDebridError as exc:
        # select_files failure is non-fatal; RD will queue the torrent anyway.
        logger.warning(
            "switch_torrent: select_files failed new_rd_id=%s: %s", new_rd_id, exc
        )

    # Fetch actual RD filename so CHECKING can find the torrent on the mount.
    # release_title from the search result often differs from RD's directory name.
    rd_filename = body.release_title or "Unknown"
    try:
        rd_info = await rd_client.get_torrent_info(new_rd_id)
        if rd_info.get("filename"):
            rd_filename = rd_info["filename"]
            logger.info(
                "switch_torrent: resolved RD filename=%r for rd_id=%s",
                rd_filename,
                new_rd_id,
            )
    except Exception as exc:
        logger.warning(
            "switch_torrent: could not fetch RD filename for rd_id=%s: %s",
            new_rd_id,
            exc,
        )

    # --- Step 6: Register new torrent in dedup registry FIRST ---
    # Register before cleanup so that if this fails, the old torrent is untouched.
    await dedup_engine.register_torrent(
        session,
        rd_id=new_rd_id,
        info_hash=info_hash,
        magnet_uri=magnet_uri,
        media_item_id=item_id,
        filename=rd_filename,
        filesize=body.size_bytes,
        resolution=body.resolution,
        cached=True,
    )
    logger.debug(
        "switch_torrent: registered new torrent in dedup new_rd_id=%s info_hash=%s",
        new_rd_id,
        info_hash,
    )

    # --- Step 7: Clean up old torrent (symlinks, RD, registry) ---
    # Safe to do now — new torrent is registered, so partial failure won't orphan the item.
    await symlink_manager.remove_symlink(session, item_id)

    # --- Clean up stale mount index entries for the old torrent ---
    # If the old torrent's filename (directory on mount) is known, delete
    # mount_index rows whose filepath contains it. This prevents CHECKING
    # from matching stale files after Zurg auto-recovery recreates them.
    if old_torrent is not None and old_torrent.filename:
        from src.models.mount_index import MountIndex as _MountIndex

        old_dir_name = old_torrent.filename
        # Escape SQL LIKE wildcards (% and _) so torrent names with
        # underscores don't over-match unrelated entries.
        escaped = (
            old_dir_name
            .replace("\\", "\\\\")
            .replace("%", "\\%")
            .replace("_", "\\_")
        )
        # Match both directory entries (.../dirname/file) and single-file
        # entries (.../filename.ext) where the torrent is the final path
        # component with no subdirectory.
        stale_result = await session.execute(
            delete(_MountIndex).where(
                _MountIndex.filepath.like(f"%/{escaped}/%", escape="\\")
                | _MountIndex.filepath.like(f"%/{escaped}", escape="\\")
            )
        )
        purged = stale_result.rowcount
        if purged:
            logger.info(
                "switch_torrent: purged %d mount index entries for old torrent %r",
                purged,
                old_dir_name,
            )

    if old_torrent is not None:
        old_rd_id = old_torrent.rd_id

        # Check whether any OTHER item references the same info_hash.
        shared_count_result = await session.execute(
            select(func.count()).select_from(RdTorrent).where(
                RdTorrent.info_hash == old_torrent.info_hash,
                RdTorrent.status == TorrentStatus.ACTIVE,
                RdTorrent.media_item_id != item_id,
            )
        )
        is_shared = (shared_count_result.scalar() or 0) > 0

        # Always mark old torrent as REPLACED in the registry.
        old_torrent.status = TorrentStatus.REPLACED

        if is_shared:
            # Other items still need this torrent in RD — don't delete it.
            logger.info(
                "switch_torrent: old torrent rd_id=%s is shared — marked REPLACED but kept in RD",
                old_rd_id,
            )
        else:
            # No other items use it — delete from RD account too.
            logger.info(
                "switch_torrent: marking old rd_id=%s as REPLACED for item_id=%d",
                old_rd_id,
                item_id,
            )
            if old_rd_id:
                try:
                    await rd_client.delete_torrent(old_rd_id)
                    logger.info(
                        "switch_torrent: deleted old rd_id=%s from RD", old_rd_id
                    )
                except RealDebridError as exc:
                    logger.warning(
                        "switch_torrent: failed to delete old rd_id=%s from RD: %s",
                        old_rd_id,
                        exc,
                    )

    # --- Step 8: Log to scrape_log ---
    selected_result_payload: dict[str, Any] = {
        "title": body.release_title,
        "info_hash": info_hash,
        "rd_id": new_rd_id,
        "resolution": body.resolution,
        "codec": body.codec,
        "quality": body.quality,
        "size_bytes": body.size_bytes,
        "is_season_pack": body.is_season_pack,
    }
    scrape_log = ScrapeLog(
        media_item_id=item_id,
        scraper="manual_switch",
        query_params=None,
        results_count=1,
        results_summary=None,
        selected_result=json.dumps(selected_result_payload),
        duration_ms=0,
    )
    session.add(scrape_log)

    # --- Step 9: Transition to CHECKING ---
    try:
        await queue_manager.force_transition(session, item_id, QueueState.CHECKING)
    except ItemNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    await session.commit()

    logger.info(
        "switch_torrent: done item_id=%d new_rd_id=%s state=CHECKING",
        item_id,
        new_rd_id,
    )

    # --- Step 10: Return success ---
    return SwitchTorrentResponse(
        status="switched",
        item_id=item_id,
        rd_id=new_rd_id,
    )


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

    # Collect RD torrents and check for shared-torrent protection BEFORE
    # deleting symlinks (we need them to detect shared directories).
    torrents_result = await session.execute(
        select(RdTorrent).where(RdTorrent.media_item_id == item_id)
    )
    rd_torrents = list(torrents_result.scalars().all())
    protected_rd_ids = await _find_protected_rd_ids(
        session, {item_id}, rd_torrents
    )
    rd_ids_to_delete = [
        t.rd_id for t in rd_torrents
        if t.rd_id and t.rd_id not in protected_rd_ids
    ]

    # Remove symlinks from disk and the symlinks table
    symlinks_removed = await symlink_manager.remove_symlink(session, item_id)

    # Remove RdTorrent DB rows
    for torrent in rd_torrents:
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

    # Fire-and-forget RD cleanup after DB is committed (only unprotected)
    for rd_id in rd_ids_to_delete:
        try:
            await rd_client.delete_torrent(rd_id)
        except Exception as exc:
            logger.warning(
                "remove_item: failed to delete rd torrent rd_id=%s: %s",
                rd_id,
                exc,
            )

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
