"""Show detail page API routes."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.deps import get_db
from src.config import settings
from src.core.show_manager import (
    AddSeasonsRequest,
    AddSeasonsResult,
    ShowDetail,
    show_manager,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/{tmdb_id}")
async def get_show_detail(
    tmdb_id: int,
    session: AsyncSession = Depends(get_db),
) -> ShowDetail:
    """Fetch show details with season status for the show detail page."""
    if not settings.tmdb.api_key:
        raise HTTPException(status_code=503, detail="TMDB API key is not configured")

    detail = await show_manager.get_show_detail(session, tmdb_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Show not found on TMDB")

    return detail


@router.post("/add")
async def add_seasons(
    body: AddSeasonsRequest,
    session: AsyncSession = Depends(get_db),
) -> AddSeasonsResult:
    """Add selected seasons to queue and optionally subscribe."""
    if not body.seasons and not body.subscribe:
        raise HTTPException(
            status_code=400,
            detail="Must select at least one season or enable subscribe",
        )

    result = await show_manager.add_seasons(session, body)
    await session.commit()

    logger.info(
        "show.add_seasons: tmdb_id=%d created=%d skipped=%s sub=%s",
        body.tmdb_id, result.created_items, result.skipped_seasons,
        result.subscription_status,
    )

    return result


class SubscribeRequest(BaseModel):
    """Request to toggle subscription."""

    enabled: bool
    quality_profile: str | None = None
    imdb_id: str | None = None
    title: str = ""
    year: int | None = None


class SubscribeResponse(BaseModel):
    """Response for subscription toggle."""

    status: str
    enabled: bool


@router.put("/{tmdb_id}/subscribe")
async def toggle_subscribe(
    tmdb_id: int,
    body: SubscribeRequest,
    session: AsyncSession = Depends(get_db),
) -> SubscribeResponse:
    """Toggle monitoring subscription for a show."""
    status = await show_manager.set_subscription(
        session, tmdb_id, body.enabled,
        imdb_id=body.imdb_id,
        title=body.title,
        year=body.year,
        quality_profile=body.quality_profile,
    )
    await session.commit()

    logger.info(
        "show.toggle_subscribe: tmdb_id=%d enabled=%s status=%s",
        tmdb_id, body.enabled, status,
    )

    return SubscribeResponse(status=status, enabled=body.enabled)
