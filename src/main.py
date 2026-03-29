"""FastAPI application entrypoint with startup/shutdown hooks and scheduler."""

import asyncio
import logging
import os
import sys
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import text

from src.__version__ import __version__
from src.config import settings
from src.core.mount_scanner import mount_scanner
from src.core.queue_processor import _job_queue_processor
from src.core.symlink_manager import symlink_manager
from src.database import async_session, engine, init_db

logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
templates.env.globals["js_version"] = str(int(time.time()))
templates.env.globals["app_version"] = __version__
scheduler = AsyncIOScheduler()

# Module-level reference to the background backfill task.  Storing it prevents
# the task from being garbage-collected before it completes.
_bg_backfill_task: asyncio.Task[None] | None = None

# Module-level reference to the background AniDB refresh task.
_bg_anidb_task: asyncio.Task[None] | None = None

# Module-level reference to the startup update check task.
_bg_update_task: asyncio.Task[None] | None = None


def setup_logging() -> None:
    """Configure structured logging for the application.

    Writes to stdout and to a rotating log file at
    ``{VIBE_DATA_DIR}/logs/vibedebrid.log`` (5 MB per file, 5 backups).
    The log directory is created if it does not already exist.
    """
    from logging.handlers import RotatingFileHandler

    _log_format = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    _date_format = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(
        level=logging.INFO,
        format=_log_format,
        datefmt=_date_format,
        stream=sys.stdout,
    )

    # File handler — persistent rotating log
    _data_dir = Path(os.environ.get("VIBE_DATA_DIR", "."))
    _log_dir = _data_dir / "logs"
    _log_dir.mkdir(parents=True, exist_ok=True)
    _log_file = _log_dir / "vibedebrid.log"

    _file_handler = RotatingFileHandler(
        _log_file,
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=5,
        encoding="utf-8",
    )
    _file_handler.setFormatter(
        logging.Formatter(fmt=_log_format, datefmt=_date_format)
    )
    logging.getLogger().addHandler(_file_handler)

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


async def _job_check_monitored_shows() -> None:
    """Scheduled job: check monitored shows for new episodes."""
    logger.info("Scheduled job starting: check_monitored_shows")
    session = async_session()
    try:
        from src.core.show_manager import show_manager
        result = await show_manager.check_monitored_shows(session)
        await session.commit()
        logger.info(
            "Scheduled job complete: check_monitored_shows checked=%d new_items=%d",
            result["checked"], result["new_items"],
        )
    except Exception:
        logger.exception("Scheduled job failed: check_monitored_shows")
        await session.rollback()
    finally:
        await session.close()


async def _job_plex_watchlist_sync() -> None:
    """Scheduled job: sync Plex watchlist items to the queue."""
    if not settings.plex.token or not settings.plex.watchlist_sync_enabled:
        return
    logger.info("Scheduled job starting: plex_watchlist_sync")
    from src.core.plex_watchlist import sync_watchlist  # noqa: PLC0415
    session = async_session()
    try:
        result = await sync_watchlist(session)
        await session.commit()
        if result["added"] > 0:
            logger.info(
                "Scheduled job complete: plex_watchlist_sync added=%d skipped=%d errors=%d",
                result["added"],
                result["skipped"],
                result["errors"],
            )
        else:
            logger.debug(
                "Scheduled job complete: plex_watchlist_sync — no new items "
                "(skipped=%d errors=%d)",
                result["skipped"],
                result["errors"],
            )
    except Exception:
        logger.exception("Scheduled job failed: plex_watchlist_sync")
        await session.rollback()
    finally:
        await session.close()


async def _job_anidb_refresh() -> None:
    """Scheduled job: refresh AniDB title dump and Fribb mapping."""
    logger.info("Scheduled job starting: anidb_refresh")
    from src.services.anidb import anidb_client  # noqa: PLC0415
    session = async_session()
    try:
        await anidb_client.refresh_data(session)
        await session.commit()
        logger.info("Scheduled job complete: anidb_refresh")
    except Exception:
        logger.exception("Scheduled job failed: anidb_refresh")
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
        max_instances=1,
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
        max_instances=1,
    )
    logger.info(
        "Registered job: symlink_verifier (every %d min)",
        settings.scheduler.symlink_verifier_minutes,
    )

    scheduler.add_job(
        _job_check_monitored_shows,
        "interval",
        hours=settings.scheduler.monitored_shows_hours,
        id="check_monitored_shows",
        replace_existing=True,
        max_instances=1,
    )
    logger.info(
        "Registered job: check_monitored_shows (every %d hours)",
        settings.scheduler.monitored_shows_hours,
    )

    scheduler.add_job(
        _job_plex_watchlist_sync,
        "interval",
        minutes=max(15, settings.plex.watchlist_poll_minutes),
        id="plex_watchlist_sync",
        replace_existing=True,
        max_instances=1,
    )
    logger.info(
        "Registered job: plex_watchlist_sync (every %d min)",
        max(15, settings.plex.watchlist_poll_minutes),
    )

    if settings.anidb.enabled:
        scheduler.add_job(
            _job_anidb_refresh,
            "interval",
            hours=settings.anidb.refresh_hours,
            id="anidb_refresh",
            replace_existing=True,
            max_instances=1,
        )
        logger.info(
            "Registered job: anidb_refresh (every %d hours)",
            settings.anidb.refresh_hours,
        )

    from src.core.update_checker import check_for_updates as _check_for_updates  # noqa: PLC0415

    scheduler.add_job(
        _check_for_updates,
        "interval",
        hours=6,
        id="update_check",
        replace_existing=True,
        max_instances=1,
    )
    logger.info("Registered job: update_check (every 6 hours)")


def _validate_configured_paths() -> None:
    """Log warnings for any paths that are empty or do not exist on disk.

    Called once at startup.  Missing paths do not abort startup — the system
    degrades gracefully (mount scanner skips, symlink creation raises errors).
    """
    path_labels = {
        "paths.zurg_mount": settings.paths.zurg_mount,
        "paths.library_movies": settings.paths.library_movies,
        "paths.library_shows": settings.paths.library_shows,
    }
    for label, path in path_labels.items():
        if not path:
            logger.warning(
                "Configuration warning: %s is not set. "
                "Update config.json to enable full functionality.",
                label,
            )
        else:
            try:
                exists = Path(path).exists()
            except OSError as exc:
                # Stale FUSE mount points raise OSError (e.g. "Transport
                # endpoint is not connected") instead of returning False.
                logger.warning(
                    "Configuration warning: %s = %r is not accessible (%s). "
                    "The mount may be stale or disconnected.",
                    label,
                    path,
                    exc,
                )
                continue
            if not exists:
                logger.warning(
                    "Configuration warning: %s = %r does not exist on disk. "
                    "Ensure the path is mounted and accessible.",
                    label,
                    path,
                )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan: startup and shutdown hooks."""
    setup_logging()
    logger.info("vibeDebrid starting up")
    _validate_configured_paths()

    # Initialize database (create tables if needed)
    # Import models so Base.metadata knows about all tables
    import src.models  # noqa: F401

    await init_db()
    logger.info("Database initialized")

    # Start scheduler
    _register_scheduled_jobs()
    scheduler.start()
    logger.info("Scheduler started")

    # Run update check at startup (non-blocking, best-effort)
    global _bg_update_task
    from src.core.update_checker import check_for_updates as _check_for_updates  # noqa: PLC0415
    _bg_update_task = asyncio.create_task(_check_for_updates())

    # Background task: backfill tmdb_ids for items that have imdb_id but no tmdb_id.
    # Non-blocking — the app starts immediately and the backfill runs in the background.
    async def _background_backfill() -> None:
        from src.core.backfill import backfill_tmdb_ids  # noqa: PLC0415

        async with async_session() as bf_session:
            try:
                bf_result = await backfill_tmdb_ids(bf_session)
                await bf_session.commit()
                if bf_result.resolved > 0:
                    logger.info(
                        "Backfill complete: resolved %d/%d tmdb_ids (%d rows updated)",
                        bf_result.resolved,
                        bf_result.total,
                        bf_result.updated_rows,
                    )
            except Exception:
                logger.exception("Background backfill failed")
                await bf_session.rollback()

    async with async_session() as check_session:
        backfill_count = (
            await check_session.execute(
                text(
                    "SELECT COUNT(DISTINCT imdb_id) FROM media_items "
                    "WHERE tmdb_id IS NULL AND imdb_id IS NOT NULL"
                )
            )
        ).scalar() or 0

    if backfill_count > 0:
        logger.info(
            "Starting background backfill for %d unique IMDB IDs", backfill_count
        )
        global _bg_backfill_task
        _bg_backfill_task = asyncio.create_task(_background_backfill())

    # Background task: refresh AniDB title data if stale or empty.
    # Non-blocking — the app starts immediately and the refresh runs in the background.
    if settings.anidb.enabled:
        async def _background_anidb_refresh() -> None:
            from src.services.anidb import anidb_client  # noqa: PLC0415
            async with async_session() as anidb_session:
                try:
                    if not await anidb_client.is_data_fresh(anidb_session):
                        logger.info("AniDB data stale or empty, refreshing...")
                        await anidb_client.refresh_data(anidb_session)
                        await anidb_session.commit()
                        logger.info("AniDB data refresh complete")
                    else:
                        logger.info("AniDB data is fresh, skipping refresh")
                except Exception:
                    logger.exception("Background AniDB refresh failed")
                    await anidb_session.rollback()

        global _bg_anidb_task
        _bg_anidb_task = asyncio.create_task(_background_anidb_refresh())

    # Run mount scan on startup only if the index is empty (first-ever boot).
    # When the DB already has indexed files, skip the expensive FUSE walk —
    # the persisted data is still valid, and the scheduled scan will catch changes.
    if settings.mount_scanner.scan_on_startup:
        async with async_session() as session:
            stats = await mount_scanner.get_index_stats(session)
        if stats["total_files"] == 0:
            logger.info("Running startup mount scan (empty index)")
            await _job_mount_scan()
        else:
            logger.info(
                "Skipping startup mount scan — index already has %d files",
                stats["total_files"],
            )

    yield

    # Shutdown
    logger.info("vibeDebrid shutting down")
    scheduler.shutdown(wait=False)
    from src.core.event_bus import event_bus
    event_bus.shutdown()
    from src.services.http_client import close_all as close_http_clients
    await close_http_clients()
    await engine.dispose()
    logger.info("Shutdown complete")


app = FastAPI(
    title="vibeDebrid",
    version=__version__,
    description="Real-Debrid media automation system",
    lifespan=lifespan,
)

from src.middleware.csrf import CSRFMiddleware  # noqa: E402

app.add_middleware(CSRFMiddleware)

# Custom validation error handler: strip 'input' from 422 responses so that
# submitted values (API keys, passwords) are never echoed back in error bodies.
from fastapi import Request as _Request  # noqa: E402
from fastapi.exceptions import RequestValidationError  # noqa: E402
from fastapi.responses import JSONResponse  # noqa: E402


@app.exception_handler(RequestValidationError)
async def _validation_error_handler(_request: _Request, exc: RequestValidationError) -> JSONResponse:
    sanitized = []
    for error in exc.errors():
        entry: dict = {}
        for k, v in error.items():
            if k == "input":
                # Never echo back submitted values
                continue
            if k == "ctx" and isinstance(v, dict):
                # ctx may contain non-serialisable Exception objects
                entry[k] = {ck: str(cv) for ck, cv in v.items()}
            else:
                entry[k] = v
        sanitized.append(entry)
    return JSONResponse(status_code=422, content={"detail": sanitized})

# Static files (create dir on first run if missing)
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# --- Register routers ---
from src.api.routes.dashboard import router as dashboard_router  # noqa: E402
from src.api.routes.discover import router as discover_router  # noqa: E402
from src.api.routes.duplicates import router as duplicates_router  # noqa: E402
from src.api.routes.health import router as health_router  # noqa: E402
from src.api.routes.movie import router as movie_router  # noqa: E402
from src.api.routes.omdb import router as omdb_router  # noqa: E402
from src.api.routes.queue import router as queue_router  # noqa: E402
from src.api.routes.search import router as search_router  # noqa: E402
from src.api.routes.settings import router as settings_router  # noqa: E402
from src.api.routes.show import router as show_router  # noqa: E402
from src.api.routes.sse import router as sse_router  # noqa: E402
from src.api.routes.tools import router as tools_router  # noqa: E402
from src.api.routes.webhook import router as webhook_router  # noqa: E402

app.include_router(dashboard_router)
app.include_router(queue_router, prefix="/api/queue", tags=["queue"])
app.include_router(search_router, prefix="/api", tags=["search"])
app.include_router(settings_router, prefix="/api/settings", tags=["settings"])
app.include_router(duplicates_router, prefix="/api/duplicates", tags=["duplicates"])
app.include_router(discover_router, prefix="/api/discover", tags=["discover"])
app.include_router(sse_router, prefix="/api", tags=["sse"])
app.include_router(tools_router, tags=["tools"])
app.include_router(show_router, prefix="/api/show", tags=["show"])
app.include_router(health_router)
app.include_router(omdb_router, prefix="/api/omdb", tags=["omdb"])
app.include_router(movie_router, prefix="/api/movie", tags=["movie"])
app.include_router(webhook_router)


# --- Page routes (serve Jinja2 templates) ---
from fastapi import Request  # noqa: E402
from fastapi.responses import HTMLResponse  # noqa: E402


@app.get("/queue", response_class=HTMLResponse, tags=["pages"])
async def queue_page(request: Request) -> HTMLResponse:
    """Queue management page."""
    return templates.TemplateResponse("queue.html", {
        "request": request,
        "active_page": "queue",
    })


@app.get("/search", response_class=HTMLResponse, tags=["pages"])
async def search_page(request: Request) -> HTMLResponse:
    """Manual search page."""
    return templates.TemplateResponse("search.html", {
        "request": request,
        "active_page": "search",
        "cache_check_limit": settings.search.cache_check_limit,
    })


@app.get("/settings", response_class=HTMLResponse, tags=["pages"])
async def settings_page(request: Request) -> HTMLResponse:
    """Settings page."""
    return templates.TemplateResponse("settings.html", {
        "request": request,
        "active_page": "settings",
    })


@app.get("/duplicates", response_class=HTMLResponse, tags=["pages"])
async def duplicates_page(request: Request) -> HTMLResponse:
    """Duplicate manager page."""
    return templates.TemplateResponse("duplicates.html", {
        "request": request,
        "active_page": "duplicates",
    })


@app.get("/discover", response_class=HTMLResponse, tags=["pages"])
async def discover_page(request: Request) -> HTMLResponse:
    """Content discovery page."""
    return templates.TemplateResponse("discover.html", {
        "request": request,
        "active_page": "discover",
    })


@app.get("/show/{tmdb_id}", response_class=HTMLResponse, tags=["pages"])
async def show_detail_page(request: Request, tmdb_id: int) -> HTMLResponse:
    """Show detail page for TV shows."""
    return templates.TemplateResponse("show.html", {
        "request": request,
        "active_page": "discover",
        "tmdb_id": tmdb_id,
    })


@app.get("/movie/{tmdb_id}", response_class=HTMLResponse, tags=["pages"])
async def movie_detail_page(request: Request, tmdb_id: int) -> HTMLResponse:
    """Movie detail page."""
    return templates.TemplateResponse("movie.html", {
        "request": request,
        "active_page": "discover",
        "tmdb_id": tmdb_id,
    })


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.main:app",
        host=settings.server.host,
        port=settings.server.port,
        reload=True,
    )
