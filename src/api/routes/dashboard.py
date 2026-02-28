"""Dashboard endpoints."""

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter(tags=["dashboard"])


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request) -> HTMLResponse:
    """Dashboard page."""
    return HTMLResponse("<h1>vibeDebrid</h1><p>Dashboard coming soon.</p>")


@router.get("/api/stats")
async def stats() -> dict:
    """Queue counts, system health, recent activity."""
    return {"status": "ok", "queue": {}, "health": {}}
