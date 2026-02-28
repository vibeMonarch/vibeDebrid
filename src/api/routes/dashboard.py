"""Dashboard endpoints."""

import logging

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.deps import get_db
from src.core.mount_scanner import mount_scanner
from src.models.media_item import MediaItem, QueueState

logger = logging.getLogger(__name__)

router = APIRouter(tags=["dashboard"])


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


class HealthStatus(BaseModel):
    mount_available: bool = False


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

    health = HealthStatus(mount_available=mount_available)

    return StatsResponse(status="ok", queue=queue, health=health)
