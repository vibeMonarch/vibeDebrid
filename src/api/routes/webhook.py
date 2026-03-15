"""Zurg webhook endpoint for on_library_update notifications."""

import asyncio
import logging
import time
from typing import Any

from fastapi import APIRouter

from src.core.mount_scanner import mount_scanner
from src.database import async_session

logger = logging.getLogger(__name__)

router = APIRouter(tags=["webhook"])

_last_scan_triggered: float = 0.0
_DEBOUNCE_SECONDS: float = 10.0
_scan_lock = asyncio.Lock()
_background_tasks: set[asyncio.Task[None]] = set()


async def _run_scan() -> None:
    """Run mount scan in background, holding the lock to prevent overlap."""
    async with _scan_lock:
        try:
            async with async_session() as session:
                result = await mount_scanner.scan(session)
                await session.commit()
                logger.info(
                    "webhook: mount scan complete: files_found=%d added=%d",
                    result.files_found,
                    result.files_added,
                )
        except (OSError, ConnectionError) as exc:
            logger.error("webhook: mount scan failed (I/O): %s", exc)
        except Exception as exc:
            logger.error("webhook: mount scan failed: %s", exc)


@router.post("/api/webhook/zurg")
async def zurg_webhook() -> dict[str, Any]:
    """Handle Zurg on_library_update webhook.

    Triggers an immediate mount scan when Zurg reports library changes.
    Debounces rapid-fire calls with a 10-second cooldown.
    """
    global _last_scan_triggered

    now = time.monotonic()
    if now - _last_scan_triggered < _DEBOUNCE_SECONDS:
        logger.debug("webhook: zurg call debounced (%.1fs since last scan)", now - _last_scan_triggered)
        return {"status": "accepted", "scan_triggered": False, "reason": "debounced"}

    if _scan_lock.locked():
        logger.debug("webhook: zurg call skipped (scan already in progress)")
        return {"status": "accepted", "scan_triggered": False, "reason": "scan_in_progress"}

    _last_scan_triggered = now
    task = asyncio.create_task(_run_scan())
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    logger.info("webhook: zurg library update received, mount scan triggered")
    return {"status": "accepted", "scan_triggered": True}
