"""Duplicate manager endpoints."""

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.deps import get_db
from src.core.dedup import DuplicateGroup, dedup_engine
from src.services.real_debrid import RealDebridError, rd_client

logger = logging.getLogger(__name__)

router = APIRouter()


class DuplicatesResponse(BaseModel):
    duplicates: list[DuplicateGroup]
    total_groups: int


class ResolveRequest(BaseModel):
    keep_rd_id: str
    remove_rd_ids: list[str]


class ResolveResponse(BaseModel):
    status: str
    removed_count: int
    errors: list[str]


@router.get("")
async def list_duplicates(
    session: AsyncSession = Depends(get_db),
) -> DuplicatesResponse:
    """Detected duplicates from RD account."""
    try:
        rd_torrents = await rd_client.list_torrents(limit=2500)
    except RealDebridError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch RD torrents: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to connect to Real-Debrid: {exc}") from exc

    groups = await dedup_engine.find_account_duplicates(session, rd_torrents)

    return DuplicatesResponse(
        duplicates=groups,
        total_groups=len(groups),
    )


@router.post("/resolve")
async def resolve_duplicates(
    body: ResolveRequest,
    session: AsyncSession = Depends(get_db),
) -> ResolveResponse:
    """Keep selected torrent, remove others from RD and update dedup records."""
    removed_count = 0
    errors: list[str] = []

    for rd_id in body.remove_rd_ids:
        if rd_id == body.keep_rd_id:
            continue
        try:
            await rd_client.delete_torrent(rd_id)
            removed_count += 1
            logger.info("resolve_duplicates: deleted rd_id=%s", rd_id)
        except RealDebridError as exc:
            error_msg = f"Failed to delete rd_id={rd_id}: {exc}"
            logger.warning("resolve_duplicates: %s", error_msg)
            errors.append(error_msg)
        except Exception as exc:
            error_msg = f"Failed to delete rd_id={rd_id}: {exc}"
            logger.warning("resolve_duplicates: %s", error_msg)
            errors.append(error_msg)

    await session.commit()

    return ResolveResponse(
        status="ok" if not errors else "partial",
        removed_count=removed_count,
        errors=errors,
    )
