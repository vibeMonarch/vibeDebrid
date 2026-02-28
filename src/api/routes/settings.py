"""Settings endpoints."""

import asyncio
import json
import logging
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.config import CONFIG_FILE, Settings, settings
from src.services.real_debrid import RealDebridAuthError, RealDebridError, rd_client
from src.services.torrentio import torrentio_client
from src.services.zilean import zilean_client

logger = logging.getLogger(__name__)

router = APIRouter()


# Pydantic schemas
class TestResult(BaseModel):
    status: str
    message: str


def _mask_api_keys(data: dict[str, Any]) -> dict[str, Any]:
    """Recursively mask fields containing 'api_key', 'token', 'secret'."""
    masked = {}
    for key, value in data.items():
        if isinstance(value, dict):
            masked[key] = _mask_api_keys(value)
        elif any(s in key.lower() for s in ("api_key", "token", "secret")) and isinstance(value, str) and value:
            masked[key] = value[:4] + "***" + value[-4:] if len(value) > 8 else "***"
        else:
            masked[key] = value
    return masked


@router.get("")
async def get_settings() -> dict[str, Any]:
    """Current configuration with masked API keys."""
    data = settings.model_dump()
    return {"settings": _mask_api_keys(data)}


@router.put("")
async def update_settings(body: dict[str, Any]) -> dict[str, Any]:
    """Update configuration by writing to config.json and reloading."""
    # Read existing config
    existing: dict[str, Any] = {}
    if CONFIG_FILE.exists():
        raw = await asyncio.to_thread(CONFIG_FILE.read_text)
        existing = json.loads(raw)

    # Deep merge new values
    def _deep_merge(base: dict, update: dict) -> dict:
        for key, value in update.items():
            if isinstance(value, dict) and isinstance(base.get(key), dict):
                base[key] = _deep_merge(base[key], value)
            else:
                base[key] = value
        return base

    merged = _deep_merge(existing, body)

    # Validate by constructing a Settings instance
    try:
        Settings(**merged)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Invalid settings: {exc}") from exc

    # Write to config.json
    await asyncio.to_thread(CONFIG_FILE.write_text, json.dumps(merged, indent=2))

    logger.info("Settings updated and written to %s", CONFIG_FILE)
    return {"status": "ok", "message": "Settings updated. Restart may be needed for some changes."}


@router.post("/test/realdebrid")
async def test_realdebrid() -> TestResult:
    """Test RD API connection by calling get_user."""
    try:
        user = await rd_client.get_user()
        username = user.get("username", "unknown")
        premium = user.get("premium", 0)
        expiration = user.get("expiration", "unknown")
        return TestResult(
            status="ok",
            message=f"Connected as {username} (premium={premium}, expires={expiration})",
        )
    except RealDebridAuthError as exc:
        return TestResult(status="error", message=f"Authentication failed: {exc}")
    except RealDebridError as exc:
        return TestResult(status="error", message=f"API error: {exc}")
    except Exception as exc:
        return TestResult(status="error", message=f"Connection failed: {exc}")


@router.post("/test/torrentio")
async def test_torrentio() -> TestResult:
    """Test Torrentio endpoint by scraping a known movie (The Matrix tt0133093)."""
    try:
        results = await torrentio_client.scrape_movie("tt0133093")
        return TestResult(
            status="ok",
            message=f"Torrentio reachable, returned {len(results)} results for test query",
        )
    except Exception as exc:
        return TestResult(status="error", message=f"Connection failed: {exc}")


@router.post("/test/zilean")
async def test_zilean() -> TestResult:
    """Test Zilean endpoint by running a simple search."""
    try:
        results = await zilean_client.search("test")
        return TestResult(
            status="ok",
            message=f"Zilean reachable, returned {len(results)} results for test query",
        )
    except Exception as exc:
        return TestResult(status="error", message=f"Connection failed: {exc}")
