"""FastAPI application entrypoint with startup/shutdown hooks and scheduler."""

import logging
import sys
from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from src.config import settings
from src.database import engine, init_db

logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
scheduler = AsyncIOScheduler()


def setup_logging() -> None:
    """Configure structured logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    # Quiet noisy libraries
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def _register_scheduled_jobs() -> None:
    """Register periodic jobs with the scheduler.

    Jobs are stubs until their core modules are implemented. Each job
    calls into the corresponding core module so the scheduler stays thin.
    """
    # Mount scanner: check Zurg mount every N minutes
    # scheduler.add_job(
    #     mount_scanner.scan,
    #     "interval",
    #     minutes=settings.mount_scanner.scan_interval_minutes,
    #     id="mount_scan",
    #     replace_existing=True,
    # )

    # Queue processor: advance items through the state machine
    # scheduler.add_job(
    #     queue_manager.process_queue,
    #     "interval",
    #     minutes=1,
    #     id="queue_process",
    #     replace_existing=True,
    # )

    # Trakt watchlist polling
    # if settings.trakt.enabled:
    #     scheduler.add_job(
    #         trakt.poll_watchlist,
    #         "interval",
    #         minutes=settings.trakt.poll_interval_minutes,
    #         id="trakt_poll",
    #         replace_existing=True,
    #     )

    # Upgrade checker
    # if settings.upgrade.enabled:
    #     scheduler.add_job(
    #         upgrade_manager.check_upgrades,
    #         "interval",
    #         minutes=settings.upgrade.check_interval_minutes,
    #         id="upgrade_check",
    #         replace_existing=True,
    #     )

    logger.info("Scheduled jobs registered (stubs until core modules are implemented)")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan: startup and shutdown hooks."""
    setup_logging()
    logger.info("vibeDebrid starting up")

    # Initialize database (create tables if needed)
    # Import models so Base.metadata knows about all tables
    import src.models  # noqa: F401

    await init_db()
    logger.info("Database initialized")

    # Start scheduler
    _register_scheduled_jobs()
    scheduler.start()
    logger.info("Scheduler started")

    yield

    # Shutdown
    logger.info("vibeDebrid shutting down")
    scheduler.shutdown(wait=False)
    await engine.dispose()
    logger.info("Shutdown complete")


app = FastAPI(
    title="vibeDebrid",
    version="0.1.0",
    description="Real-Debrid media automation system",
    lifespan=lifespan,
)

# Static files (create dir on first run if missing)
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# --- Register routers ---
from src.api.routes.dashboard import router as dashboard_router  # noqa: E402
from src.api.routes.queue import router as queue_router  # noqa: E402
from src.api.routes.search import router as search_router  # noqa: E402
from src.api.routes.settings import router as settings_router  # noqa: E402
from src.api.routes.duplicates import router as duplicates_router  # noqa: E402

app.include_router(dashboard_router)
app.include_router(queue_router, prefix="/api/queue", tags=["queue"])
app.include_router(search_router, prefix="/api", tags=["search"])
app.include_router(settings_router, prefix="/api/settings", tags=["settings"])
app.include_router(duplicates_router, prefix="/api/duplicates", tags=["duplicates"])


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.main:app",
        host=settings.server.host,
        port=settings.server.port,
        reload=True,
    )
