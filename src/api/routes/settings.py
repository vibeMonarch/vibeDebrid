"""Settings endpoints."""

from fastapi import APIRouter

router = APIRouter()


@router.get("")
async def get_settings() -> dict:
    """Current configuration."""
    return {"settings": {}}


@router.put("")
async def update_settings() -> dict:
    """Update configuration."""
    return {"action": "updated"}


@router.post("/test/realdebrid")
async def test_realdebrid() -> dict:
    """Test RD API connection."""
    return {"status": "not_implemented"}


@router.post("/test/torrentio")
async def test_torrentio() -> dict:
    """Test Torrentio endpoint."""
    return {"status": "not_implemented"}


@router.post("/test/zilean")
async def test_zilean() -> dict:
    """Test Zilean endpoint."""
    return {"status": "not_implemented"}
