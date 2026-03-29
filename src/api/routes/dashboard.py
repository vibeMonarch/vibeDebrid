"""Dashboard endpoints."""

import asyncio
import logging
import time
from typing import Any

import httpx
from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sqlalchemy import asc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.deps import get_db
from src.core.mount_scanner import mount_scanner
from src.models.media_item import MediaItem, QueueState
from src.services.http_client import CircuitOpenError
from src.services.real_debrid import RealDebridError, rd_client

logger = logging.getLogger(__name__)

router = APIRouter(tags=["dashboard"])

# ---------------------------------------------------------------------------
# In-memory RD health cache
# ---------------------------------------------------------------------------

_rd_health_cache: dict[str, Any] = {"data": None, "fetched_at": 0.0}
_rd_health_lock = asyncio.Lock()
_RD_HEALTH_TTL = 900.0  # 15 minutes


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------


class QueueCounts(BaseModel):
    total: int = 0
    unreleased: int = 0
    wanted: int = 0
    scraping: int = 0
    adding: int = 0
    checking: int = 0
    sleeping: int = 0
    dormant: int = 0
    complete: int = 0
    done: int = 0


class RdHealth(BaseModel):
    username: str
    premium_type: str  # "premium" or "free"
    days_remaining: int
    expiration: str | None = None
    points: int = 0


class HealthStatus(BaseModel):
    mount_available: bool = False
    rd: RdHealth | None = None


class UpcomingEpisode(BaseModel):
    """A future episode in UNRELEASED state with a known air date."""

    title: str
    season: int | None = None
    episode: int | None = None
    air_date: str | None = None  # ISO date string e.g. "2026-03-18"
    tmdb_id: int | None = None
    state: str = ""


class StatsResponse(BaseModel):
    status: str = "ok"
    queue: QueueCounts
    health: HealthStatus
    upcoming: list[UpcomingEpisode] = []


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request) -> HTMLResponse:
    """Dashboard page."""
    from src.main import templates

    return templates.TemplateResponse(request, "dashboard.html", context={
        "active_page": "dashboard",
    })


@router.get("/api/stats")
async def stats(session: AsyncSession = Depends(get_db)) -> StatsResponse:
    """Queue counts, system health, recent activity."""
    # Query counts grouped by state
    result = await session.execute(
        select(MediaItem.state, func.count(MediaItem.id)).group_by(MediaItem.state)
    )
    state_counts = {row[0]: row[1] for row in result.all()}

    queue = QueueCounts(
        total=sum(state_counts.values()),
        unreleased=state_counts.get(QueueState.UNRELEASED, 0),
        wanted=state_counts.get(QueueState.WANTED, 0),
        scraping=state_counts.get(QueueState.SCRAPING, 0),
        adding=state_counts.get(QueueState.ADDING, 0),
        checking=state_counts.get(QueueState.CHECKING, 0),
        sleeping=state_counts.get(QueueState.SLEEPING, 0),
        dormant=state_counts.get(QueueState.DORMANT, 0),
        complete=state_counts.get(QueueState.COMPLETE, 0),
        done=state_counts.get(QueueState.DONE, 0),
    )

    # Check mount health
    try:
        mount_available = await mount_scanner.is_mount_available()
    except Exception:
        mount_available = False

    # Fetch RD account health (cached, TTL 15 min, lock prevents concurrent fetches)
    rd_health: RdHealth | None = None
    now = time.monotonic()
    if now - _rd_health_cache["fetched_at"] < _RD_HEALTH_TTL and _rd_health_cache["data"] is not None:
        rd_health = _rd_health_cache["data"]
        logger.debug("stats: RD health cache hit (age=%.0fs)", now - _rd_health_cache["fetched_at"])
    else:
        async with _rd_health_lock:
            # Re-check after acquiring lock (another request may have populated cache)
            now = time.monotonic()
            if now - _rd_health_cache["fetched_at"] < _RD_HEALTH_TTL and _rd_health_cache["data"] is not None:
                rd_health = _rd_health_cache["data"]
            else:
                try:
                    user_info = await rd_client.get_account_info()
                    rd_health = RdHealth(
                        username=user_info.get("username", ""),
                        premium_type=user_info.get("type", "free"),
                        days_remaining=int(user_info.get("premium") or 0) // 86400,
                        expiration=user_info.get("expiration"),
                        points=int(user_info.get("points") or 0),
                    )
                    _rd_health_cache["data"] = rd_health
                    _rd_health_cache["fetched_at"] = now
                    logger.info(
                        "stats: RD health fetched username=%s premium_type=%s days_remaining=%d",
                        rd_health.username,
                        rd_health.premium_type,
                        rd_health.days_remaining,
                    )
                except (
                    RealDebridError,
                    CircuitOpenError,
                    httpx.RequestError,
                    TimeoutError,
                    ValueError,
                    TypeError,
                ) as exc:
                    logger.warning("stats: RD health unavailable (%s: %s)", type(exc).__name__, exc)

    health = HealthStatus(mount_available=mount_available, rd=rd_health)

    # Query upcoming episodes: UNRELEASED items that have a known air_date
    upcoming_result = await session.execute(
        select(MediaItem)
        .where(
            MediaItem.state == QueueState.UNRELEASED,
            MediaItem.air_date.is_not(None),
        )
        .order_by(asc(MediaItem.air_date))
        .limit(10)
    )
    upcoming_items = upcoming_result.scalars().all()

    upcoming: list[UpcomingEpisode] = [
        UpcomingEpisode(
            title=item.title,
            season=item.season,
            episode=item.episode,
            air_date=item.air_date.isoformat() if item.air_date is not None else None,
            tmdb_id=int(item.tmdb_id) if item.tmdb_id and item.tmdb_id.isdigit() else None,
            state=item.state.value,
        )
        for item in upcoming_items
    ]

    return StatsResponse(status="ok", queue=queue, health=health, upcoming=upcoming)
