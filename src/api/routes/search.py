"""Manual search endpoints."""

from fastapi import APIRouter

router = APIRouter()


@router.post("/search")
async def search() -> dict:
    """Search scrapers."""
    return {"results": []}


@router.post("/add")
async def add_torrent() -> dict:
    """Add torrent to RD and queue."""
    return {"action": "add"}
