"""Dashboard endpoints."""

import logging
import time
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.deps import get_db
from src.core.mount_scanner import mount_scanner
from src.models.media_item import MediaItem, QueueState
import httpx

from src.services.http_client import CircuitOpenError
from src.services.real_debrid import RealDebridError, rd_client

logger = logging.getLogger(__name__)

router = APIRouter(tags=["dashboard"])

# ---------------------------------------------------------------------------
# In-memory RD health cache
# ---------------------------------------------------------------------------

_rd_health_cache: dict[str, Any] = {"data": None, "fetched_at": 0.0}
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
    torrent_count: int = 0
    total_bytes: int = 0


class HealthStatus(BaseModel):
    mount_available: bool = False
    rd: RdHealth | None = None


class StatsResponse(BaseModel):
    status: str = "ok"
    queue: QueueCounts
    health: HealthStatus


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request) -> HTMLResponse:
    """Dashboard page."""
    from src.main import templates

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
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

    # Fetch RD account health (cached, TTL 15 min)
    rd_health: RdHealth | None = None
    now = time.monotonic()
    if now - _rd_health_cache["fetched_at"] < _RD_HEALTH_TTL and _rd_health_cache["data"] is not None:
        rd_health = _rd_health_cache["data"]
        logger.debug("stats: RD health cache hit (age=%.0fs)", now - _rd_health_cache["fetched_at"])
    else:
        try:
            user_info = await rd_client.get_account_info()
            torrents = await rd_client.list_torrents(limit=2500)
            torrent_count = len(torrents)
            total_bytes = sum(t.get("bytes", 0) for t in torrents)
            rd_health = RdHealth(
                username=user_info.get("username", ""),
                premium_type=user_info.get("type", "free"),
                days_remaining=int(user_info.get("premium", 0)),
                expiration=user_info.get("expiration"),
                points=int(user_info.get("points", 0)),
                torrent_count=torrent_count,
                total_bytes=total_bytes,
            )
            _rd_health_cache["data"] = rd_health
            _rd_health_cache["fetched_at"] = now
            logger.info(
                "stats: RD health fetched username=%s premium_type=%s days_remaining=%d torrents=%d",
                rd_health.username,
                rd_health.premium_type,
                rd_health.days_remaining,
                rd_health.torrent_count,
            )
        except (
            RealDebridError,
            CircuitOpenError,
            httpx.RequestError,
            TimeoutError,
        ) as exc:
            logger.warning("stats: RD health unavailable (%s: %s)", type(exc).__name__, exc)

    health = HealthStatus(mount_available=mount_available, rd=rd_health)

    return StatsResponse(status="ok", queue=queue, health=health)
