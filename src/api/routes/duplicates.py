"""Duplicate manager endpoints."""

from fastapi import APIRouter

router = APIRouter()


@router.get("")
async def list_duplicates() -> dict:
    """Detected duplicates."""
    return {"duplicates": []}


@router.post("/resolve")
async def resolve_duplicates() -> dict:
    """Keep selected, remove others."""
    return {"action": "resolve"}
