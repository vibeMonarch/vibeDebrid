"""FastAPI application entrypoint with startup/shutdown hooks and scheduler."""

import logging
import sys
from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import select

from src.config import settings
from src.database import engine, init_db, async_session
from src.core.queue_manager import queue_manager
from src.core.scrape_pipeline import scrape_pipeline
from src.core.mount_scanner import mount_scanner
from src.core.symlink_manager import symlink_manager
from src.models.media_item import MediaItem, QueueState
from src.models.torrent import RdTorrent, TorrentStatus
from src.services.real_debrid import rd_client

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


async def _job_mount_scan() -> None:
    """Scheduled job: scan the Zurg mount and update the file index."""
    logger.info("Scheduled job starting: mount_scan")
    session = async_session()
    try:
        result = await mount_scanner.scan(session)
        await session.commit()
        logger.info(
            "Scheduled job complete: mount_scan found=%d added=%d removed=%d",
            result.files_found,
            result.files_added,
            result.files_removed,
        )
    except Exception:
        logger.exception("Scheduled job failed: mount_scan")
        await session.rollback()
    finally:
        await session.close()


async def _job_queue_processor() -> None:
    """Scheduled job: advance queue items through the full state machine.

    Processes items in this order:
    1. UNRELEASED/SLEEPING/DORMANT timer-based transitions (via queue_manager.process_queue)
    2. WANTED → SCRAPING → pipeline
    3. ADDING → check RD torrent status → CHECKING when downloaded
    4. CHECKING → mount lookup → symlink creation → COMPLETE
    5. COMPLETE (older than 1 hour) → DONE
    """
    logger.info("Scheduled job starting: queue_processor")
    session = async_session()
    try:
        stats = await queue_manager.process_queue(session)
        logger.info("Queue transitions applied: %s", stats)

        # --- Stage 1: WANTED → SCRAPING → pipeline ---
        result = await session.execute(
            select(MediaItem).where(MediaItem.state == QueueState.WANTED)
        )
        wanted_items = result.scalars().all()
        logger.info("WANTED items to scrape: %d", len(wanted_items))

        for item in wanted_items:
            try:
                await queue_manager.transition(session, item.id, QueueState.SCRAPING)
                await scrape_pipeline.run(session, item)
            except Exception:
                logger.exception(
                    "Scrape pipeline failed for item id=%d title=%s", item.id, item.title
                )
                try:
                    await queue_manager.transition(session, item.id, QueueState.SLEEPING)
                except Exception:
                    logger.exception(
                        "Failed to transition item id=%d to SLEEPING after pipeline error",
                        item.id,
                    )

        # --- Stage 2: ADDING → check RD status → CHECKING ---
        result = await session.execute(
            select(MediaItem).where(MediaItem.state == QueueState.ADDING)
        )
        adding_items = result.scalars().all()
        logger.info("ADDING items to check: %d", len(adding_items))

        for item in adding_items:
            try:
                # Find the active RD torrent for this item
                torrent_result = await session.execute(
                    select(RdTorrent).where(
                        RdTorrent.media_item_id == item.id,
                        RdTorrent.status == TorrentStatus.ACTIVE,
                    )
                )
                torrent = torrent_result.scalar_one_or_none()
                if torrent is None or torrent.rd_id is None:
                    logger.warning(
                        "ADDING item id=%d has no active RD torrent, skipping",
                        item.id,
                    )
                    continue

                rd_info = await rd_client.get_torrent_info(torrent.rd_id)
                rd_status = rd_info.get("status", "")
                logger.info(
                    "ADDING item id=%d rd_id=%s rd_status=%s",
                    item.id, torrent.rd_id, rd_status,
                )

                if rd_status == "downloaded":
                    await queue_manager.transition(session, item.id, QueueState.CHECKING)
            except Exception:
                logger.exception(
                    "Failed to check RD status for ADDING item id=%d title=%s",
                    item.id, item.title,
                )

        # --- Stage 3: CHECKING → mount lookup → symlink → COMPLETE ---
        result = await session.execute(
            select(MediaItem).where(MediaItem.state == QueueState.CHECKING)
        )
        checking_items = result.scalars().all()
        logger.info("CHECKING items to verify: %d", len(checking_items))

        for item in checking_items:
            try:
                matches = await mount_scanner.lookup(
                    session,
                    title=item.title,
                    season=item.season,
                    episode=item.episode,
                )
                if not matches:
                    logger.info(
                        "CHECKING item id=%d title=%r not found in mount yet, will retry next cycle",
                        item.id, item.title,
                    )
                    continue

                # Use the first (most recent) match
                source_path = matches[0].filepath
                logger.info(
                    "CHECKING item id=%d found in mount: %s", item.id, source_path,
                )

                await symlink_manager.create_symlink(session, item, source_path)
                await queue_manager.transition(session, item.id, QueueState.COMPLETE)
            except Exception:
                logger.exception(
                    "Failed to process CHECKING item id=%d title=%s",
                    item.id, item.title,
                )

        # --- Stage 4: COMPLETE (older than 1 hour) → DONE ---
        one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
        result = await session.execute(
            select(MediaItem).where(
                MediaItem.state == QueueState.COMPLETE,
                MediaItem.state_changed_at <= one_hour_ago,
            )
        )
        complete_items = result.scalars().all()
        logger.info("COMPLETE items ready for DONE: %d", len(complete_items))

        for item in complete_items:
            try:
                await queue_manager.transition(session, item.id, QueueState.DONE)
            except Exception:
                logger.exception(
                    "Failed to transition COMPLETE item id=%d to DONE", item.id,
                )

        await session.commit()
        logger.info("Scheduled job complete: queue_processor")
    except Exception:
        logger.exception("Scheduled job failed: queue_processor")
        await session.rollback()
    finally:
        await session.close()


async def _job_symlink_verifier() -> None:
    """Scheduled job: verify existing symlinks are still valid."""
    logger.info("Scheduled job starting: symlink_verifier")
    session = async_session()
    try:
        result = await symlink_manager.verify_symlinks(session)
        await session.commit()
        logger.info(
            "Scheduled job complete: symlink_verifier checked=%d broken=%d",
            result.total_checked,
            result.broken_count,
        )
    except Exception:
        logger.exception("Scheduled job failed: symlink_verifier")
        await session.rollback()
    finally:
        await session.close()


def _register_scheduled_jobs() -> None:
    """Register periodic jobs with the scheduler.

    All intervals are read from settings — no hardcoded values.
    """
    scheduler.add_job(
        _job_mount_scan,
        "interval",
        minutes=settings.mount_scanner.scan_interval_minutes,
        id="mount_scan",
        replace_existing=True,
    )
    logger.info(
        "Registered job: mount_scan (every %d min)",
        settings.mount_scanner.scan_interval_minutes,
    )

    scheduler.add_job(
        _job_queue_processor,
        "interval",
        minutes=settings.scheduler.queue_processor_minutes,
        id="queue_processor",
        replace_existing=True,
        max_instances=1,
    )
    logger.info(
        "Registered job: queue_processor (every %d min)",
        settings.scheduler.queue_processor_minutes,
    )

    scheduler.add_job(
        _job_symlink_verifier,
        "interval",
        minutes=settings.scheduler.symlink_verifier_minutes,
        id="symlink_verifier",
        replace_existing=True,
    )
    logger.info(
        "Registered job: symlink_verifier (every %d min)",
        settings.scheduler.symlink_verifier_minutes,
    )


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

    # Run mount scan immediately on startup if configured
    if settings.mount_scanner.scan_on_startup:
        logger.info("Running startup mount scan")
        await _job_mount_scan()

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
