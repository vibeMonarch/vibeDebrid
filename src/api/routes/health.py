"""Health check endpoint for monitoring and container health checks."""

import asyncio
import logging

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.deps import get_db
from src.core.mount_scanner import mount_scanner

logger = logging.getLogger(__name__)

router = APIRouter(tags=["health"])


class HealthChecks(BaseModel):
    """Individual check results."""

    database: bool
    zurg_mount: bool


class HealthResponse(BaseModel):
    """Health check response schema."""

    status: str
    checks: HealthChecks


async def _check_database(db: AsyncSession) -> bool:
    """Execute a lightweight query to verify the database is reachable.

    Args:
        db: Async SQLAlchemy session.

    Returns:
        True if the database responded successfully, False otherwise.
    """
    try:
        await db.execute(text("SELECT 1"))
        return True
    except (SQLAlchemyError, OSError):
        logger.warning("Database health check failed", exc_info=True)
        return False


async def _check_zurg_mount() -> bool:
    """Check whether the Zurg/rclone mount is available.

    Returns:
        True if the mount is reachable, False otherwise.
    """
    try:
        return await mount_scanner.is_mount_available()
    except (OSError, TimeoutError):
        logger.warning("Zurg mount health check failed", exc_info=True)
        return False


@router.get(
    "/health",
    responses={200: {"model": HealthResponse}, 503: {"model": HealthResponse}},
)
async def health(db: AsyncSession = Depends(get_db)) -> JSONResponse:
    """Return system health status for monitoring and container health checks.

    The database check is critical — a failure returns HTTP 503. The zurg_mount
    check is non-critical — it is reported but does not affect the HTTP status
    code.

    Returns:
        JSON with overall status and per-check results. HTTP 200 when healthy,
        HTTP 503 when a critical check fails.
    """
    db_ok, mount_ok = await asyncio.gather(
        _check_database(db), _check_zurg_mount()
    )

    checks = HealthChecks(database=db_ok, zurg_mount=mount_ok)
    status = "healthy" if db_ok else "unhealthy"

    logger.debug(
        "Health check: status=%s database=%s zurg_mount=%s",
        status,
        db_ok,
        mount_ok,
    )

    response_body = HealthResponse(status=status, checks=checks)
    http_status = 200 if db_ok else 503

    return JSONResponse(
        content=response_body.model_dump(),
        status_code=http_status,
    )
