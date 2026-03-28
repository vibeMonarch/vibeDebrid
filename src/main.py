"""FastAPI application entrypoint with startup/shutdown hooks and scheduler."""

import asyncio
import json
import logging
import os
import re
import sys
import time
from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import delete, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.__version__ import __version__
from src.config import settings
from src.database import engine, init_db, async_session
from src.core.queue_manager import queue_manager
from src.core.scrape_pipeline import scrape_pipeline
from src.core.mount_scanner import mount_scanner, gather_alt_titles
from src.core.symlink_manager import SourceNotFoundError, symlink_manager
from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.symlink import Symlink
from src.models.torrent import RdTorrent, TorrentStatus
from src.services.real_debrid import rd_client, RealDebridError

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

# Creditless openings/endings and other non-episode special files.
# These should never be symlinked as regular episodes in season packs.
_SPECIAL_FILENAME_RE = re.compile(
    r"\bNC(?:OP|ED)\d*\b"
    r"|\b(?:Creditless|Preview|Trailer|Promo)\b"
    r"|\b(?:OP|ED)\s*\d+\b",
    re.IGNORECASE,
)


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


async def _find_torrent_for_item(session: AsyncSession, item: MediaItem) -> RdTorrent | None:
    """Find the active RD torrent for a media item.

    First tries direct media_item_id lookup. Falls back to info_hash
    from the scrape_log when the direct lookup fails (happens when
    multiple items share the same season pack torrent).

    Args:
        session: Async database session.
        item: The MediaItem to find a torrent for.

    Returns:
        An active RdTorrent row, or None if not found.
    """
    import json as _json
    from src.models.scrape_result import ScrapeLog

    # Direct lookup
    result = await session.execute(
        select(RdTorrent).where(
            RdTorrent.media_item_id == item.id,
            RdTorrent.status == TorrentStatus.ACTIVE,
        )
    )
    torrent = result.scalar_one_or_none()
    if torrent is not None:
        return torrent

    # Fallback: look up info_hash from scrape_log
    log_result = await session.execute(
        select(ScrapeLog)
        .where(
            ScrapeLog.media_item_id == item.id,
            ScrapeLog.scraper == "pipeline",
            ScrapeLog.selected_result.isnot(None),
        )
        .order_by(ScrapeLog.id.desc())
        .limit(1)
    )
    log_entry = log_result.scalar_one_or_none()
    if log_entry is None:
        return None

    try:
        selected = _json.loads(log_entry.selected_result)
        info_hash = selected.get("info_hash")
    except (ValueError, TypeError, AttributeError):
        return None

    if not info_hash:
        return None

    result = await session.execute(
        select(RdTorrent).where(
            RdTorrent.info_hash == info_hash.lower(),
            RdTorrent.status == TorrentStatus.ACTIVE,
        )
    )
    torrent = result.scalar_one_or_none()
    if torrent is not None:
        logger.debug(
            "Torrent fallback: item id=%d found via scrape_log hash=%s (rd_id=%s)",
            item.id, info_hash, torrent.rd_id,
        )
    return torrent


async def _get_absolute_episode_range(tmdb_id: str, target_season: int) -> tuple[int, int] | None:
    """Calculate the absolute episode range for a target season using TMDB data.

    For shows with absolute episode numbering (no season markers), uses
    TMDB episode counts per season to determine which absolute episode
    numbers correspond to the target season.

    Args:
        tmdb_id: TMDB show ID as string.
        target_season: The season number to calculate the range for.

    Returns:
        A (start, end) tuple of absolute episode numbers (inclusive),
        or None if TMDB data is unavailable or the season is not found.
    """
    try:
        tid = int(tmdb_id)
    except (ValueError, TypeError):
        return None

    from src.services.tmdb import tmdb_client
    show = await tmdb_client.get_show_details(tid)
    if not show or not show.seasons:
        logger.debug(
            "_get_absolute_episode_range: TMDB returned no data for tmdb_id=%s", tmdb_id,
        )
        return None

    # Sort seasons by number, skip specials (season 0)
    regular_seasons = sorted(
        [s for s in show.seasons if s.season_number > 0],
        key=lambda s: s.season_number,
    )

    logger.debug(
        "_get_absolute_episode_range: tmdb_id=%s target_season=%d seasons=%s",
        tmdb_id, target_season,
        [(s.season_number, s.episode_count) for s in regular_seasons],
    )

    # Try AniDB episode counts for more accurate anime data (all seasons
    # up to and including target, so the cumulative offset is also correct).
    anidb_counts: dict[int, int] = {}
    if settings.anidb.enabled:
        try:
            from src.services.anidb import anidb_client  # noqa: PLC0415
            async with async_session() as anidb_session:
                anidb_counts = await anidb_client.get_episode_counts_up_to_season(
                    anidb_session, tid, target_season
                )
                await anidb_session.commit()
            if anidb_counts:
                logger.debug(
                    "_get_absolute_episode_range: AniDB episode counts for tmdb_id=%s: %s",
                    tmdb_id, anidb_counts,
                )
        except Exception as exc:
            logger.debug(
                "_get_absolute_episode_range: AniDB lookup failed for tmdb_id=%s: %s",
                tmdb_id, exc,
            )

    cumulative = 0
    for s in regular_seasons:
        # Use AniDB count if available, fall back to TMDB
        ep_count = anidb_counts.get(s.season_number, s.episode_count)
        if s.season_number == target_season:
            start = cumulative + 1
            end = cumulative + ep_count
            logger.info(
                "_get_absolute_episode_range: tmdb_id=%s S%02d -> absolute episodes %d-%d%s",
                tmdb_id, target_season, start, end,
                " (AniDB)" if s.season_number in anidb_counts else "",
            )
            return (start, end)
        cumulative += ep_count

    logger.warning(
        "_get_absolute_episode_range: season %d not found in TMDB data for tmdb_id=%s "
        "(available: %s)",
        target_season, tmdb_id, [s.season_number for s in regular_seasons],
    )
    return None


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

        # --- Stage 0: Recover stuck SCRAPING items ---
        stale_cutoff = datetime.now(timezone.utc) - timedelta(minutes=30)
        result = await session.execute(
            select(MediaItem).where(
                MediaItem.state == QueueState.SCRAPING,
                MediaItem.state_changed_at <= stale_cutoff,
            )
        )
        stale_items = result.scalars().all()
        for item in stale_items:
            logger.warning(
                "Recovering stuck SCRAPING item id=%d title=%s (stale for >30min)",
                item.id, item.title,
            )
            try:
                await queue_manager.transition(session, item.id, QueueState.SLEEPING)
            except Exception:
                logger.exception("Failed to recover stuck item id=%d", item.id)

        # --- Stage 1: WANTED/SCRAPING → pipeline ---
        # Include SCRAPING items: process_queue() transitions SLEEPING→SCRAPING,
        # but Stage 1 previously only queried WANTED, leaving those items stuck
        # until Stage 0 caught them as "stale" 30 min later.
        result = await session.execute(
            select(MediaItem).where(
                MediaItem.state.in_([QueueState.WANTED, QueueState.SCRAPING])
            )
        )
        scrape_items = result.scalars().all()
        logger.info("Items to scrape (WANTED+SCRAPING): %d", len(scrape_items))

        for item in scrape_items:
            # Cache attributes before try block so error handlers don't
            # trigger lazy loads on a potentially poisoned session.
            item_id = item.id
            item_title = item.title
            item_state = item.state
            try:
                if item_state == QueueState.WANTED:
                    # Normal path: transition to SCRAPING before running pipeline.
                    await queue_manager.transition(session, item_id, QueueState.SCRAPING)
                else:
                    # Item already in SCRAPING (set by process_queue from SLEEPING).
                    # Skip the WANTED→SCRAPING transition and run pipeline directly.
                    logger.debug(
                        "Item id=%d title=%s already SCRAPING, running pipeline directly",
                        item_id, item_title,
                    )
                await scrape_pipeline.run(session, item)
            except Exception:
                logger.exception(
                    "Scrape pipeline failed for item id=%d title=%s", item_id, item_title
                )
                try:
                    await session.rollback()
                    await queue_manager.transition(session, item_id, QueueState.SLEEPING)
                except Exception:
                    logger.exception(
                        "Failed to transition item id=%d to SLEEPING after pipeline error",
                        item_id,
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
                torrent = await _find_torrent_for_item(session, item)
                if torrent is None or torrent.rd_id is None:
                    # Timeout: transition to SLEEPING if stuck too long
                    sca = item.state_changed_at
                    if sca is not None and sca.tzinfo is None:
                        sca = sca.replace(tzinfo=timezone.utc)
                    adding_timeout = datetime.now(timezone.utc) - timedelta(
                        minutes=settings.retry.checking_timeout_minutes
                    )
                    if sca and sca <= adding_timeout:
                        logger.warning(
                            "ADDING item id=%d has no active RD torrent and timed out after %d min, transitioning to SLEEPING",
                            item.id, settings.retry.checking_timeout_minutes,
                        )
                        await queue_manager.transition(session, item.id, QueueState.SLEEPING)
                    else:
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
                    rd_filename = rd_info.get("filename")
                    if rd_filename and torrent.filename != rd_filename:
                        torrent.filename = rd_filename
                        logger.info(
                            "ADDING item id=%d: captured RD filename %r",
                            item.id, rd_filename,
                        )
                    await queue_manager.transition(session, item.id, QueueState.CHECKING)
            except RealDebridError as e:
                if e.status_code == 404 or "unknown_ressource" in str(e):
                    logger.warning(
                        "ADDING item id=%d: RD torrent %s no longer exists (404), transitioning to SLEEPING",
                        item.id, torrent.rd_id if torrent else "unknown",
                    )
                    try:
                        await queue_manager.transition(session, item.id, QueueState.SLEEPING)
                    except Exception:
                        logger.exception(
                            "Failed to transition ADDING item id=%d to SLEEPING after 404",
                            item.id,
                        )
                else:
                    logger.exception(
                        "Failed to check RD status for ADDING item id=%d title=%s",
                        item.id, item.title,
                    )
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

        # Collect newly created symlink target paths — triggered AFTER all
        # symlinks are created.  Plex derives the scan directory from each
        # path; Jellyfin maps media_type to library IDs for a safe scan.
        media_scan_queue: list[tuple[str, str]] = []  # (media_type, target_path)

        for item in checking_items:
            try:
                if item.is_season_pack:
                    # Tracks episode offset for absolute-numbered multi-season torrents.
                    # Set to (start_ep - 1) when the absolute episode fallback is used so
                    # that symlink_manager can remap absolute numbers to per-season numbers.
                    _season_pack_episode_offset: int = 0
                    scan_result = None
                    # Season packs: look up ALL episodes (episode=None) and symlink each match
                    _sp_alt_titles = await gather_alt_titles(session, item)
                    matches = await mount_scanner.lookup_multi(
                        session,
                        _sp_alt_titles,
                        season=item.season,
                        episode=None,
                    )
                    if not matches:
                        # Targeted scan: check if the RD torrent directory exists on mount
                        torrent = await _find_torrent_for_item(session, item)
                        if torrent and torrent.filename:
                            scan_result = await mount_scanner.scan_directory(session, torrent.filename)
                            if scan_result.files_indexed > 0:
                                matches = await mount_scanner.lookup(
                                    session,
                                    title=item.title,
                                    season=item.season,
                                    episode=None,
                                )
                            # RD filename refresh: torrent.filename may be stale (e.g.
                            # stored as item title instead of actual RD torrent name).
                            # Re-fetch from RD API and retry scan+lookup if it differs.
                            if not matches and torrent.rd_id:
                                try:
                                    rd_info = await rd_client.get_torrent_info(torrent.rd_id)
                                    rd_filename = rd_info.get("filename")
                                    if rd_filename and rd_filename != torrent.filename:
                                        logger.info(
                                            "CHECKING season pack id=%d: refreshing torrent filename %r → %r",
                                            item.id, torrent.filename, rd_filename,
                                        )
                                        torrent.filename = rd_filename
                                        scan_result = await mount_scanner.scan_directory(session, rd_filename)
                                        if scan_result.files_indexed > 0:
                                            matches = await mount_scanner.lookup(
                                                session,
                                                title=item.title,
                                                season=item.season,
                                                episode=None,
                                            )
                                except Exception:
                                    logger.warning(
                                        "CHECKING season pack id=%d: RD filename refresh failed",
                                        item.id,
                                    )
                            # Path-based fallback: PTN may parse individual filenames
                            # differently from the item title (e.g. disc rips, episode
                            # title filenames), so fall back to directory prefix lookup.
                            # Run whenever matched_dir_path is known, regardless of whether
                            # new files were indexed (files may have been indexed by a prior
                            # full scan).
                            if not matches and scan_result.matched_dir_path:
                                logger.info(
                                    "CHECKING season pack id=%d: title lookup failed, "
                                    "trying path prefix %r",
                                    item.id, scan_result.matched_dir_path,
                                )
                                matches = await mount_scanner.lookup_by_path_prefix(
                                    session,
                                    scan_result.matched_dir_path,
                                    season=item.season,
                                    episode=None,
                                )
                            # Absolute episode fallback: complete collections with flat structure
                            # (no season markers in filenames). Use TMDB episode counts to
                            # calculate which absolute episodes belong to the target season.
                            # Tried BEFORE the relaxed season=None query so TMDB-guided
                            # filtering gets first shot for multi-season torrents.
                            if not matches and scan_result.matched_dir_path and item.tmdb_id and item.season is not None:
                                try:
                                    all_files = await mount_scanner.lookup_by_path_prefix(
                                        session,
                                        scan_result.matched_dir_path,
                                        season=None,
                                        episode=None,
                                    )
                                    if all_files:
                                        abs_range = await _get_absolute_episode_range(item.tmdb_id, item.season)
                                        if abs_range:
                                            start_ep, end_ep = abs_range
                                            ep_values = sorted(set(
                                                f.parsed_episode for f in all_files if f.parsed_episode is not None
                                            ))
                                            logger.info(
                                                "CHECKING season pack id=%d: absolute fallback range=%d-%d, "
                                                "file episodes=%s (sample filenames: %s)",
                                                item.id, start_ep, end_ep,
                                                ep_values[:20] if len(ep_values) <= 20 else f"{ep_values[:10]}...{ep_values[-5:]} ({len(ep_values)} total)",
                                                [f.filename for f in all_files[:3]],
                                            )
                                            matches = [
                                                f for f in all_files
                                                if f.parsed_episode is not None and start_ep <= f.parsed_episode <= end_ep
                                            ]
                                            if matches:
                                                # Record offset so symlink_manager can remap
                                                # absolute episode numbers to per-season numbers.
                                                # e.g. if S02 starts at ep 27, offset=26 maps
                                                # file ep 27 → S02E01, ep 28 → S02E02, etc.
                                                _season_pack_episode_offset = start_ep - 1
                                                logger.info(
                                                    "CHECKING season pack id=%d: absolute episode fallback matched %d files "
                                                    "(absolute range %d-%d for S%02d, offset=%d)",
                                                    item.id, len(matches), start_ep, end_ep, item.season,
                                                    _season_pack_episode_offset,
                                                )
                                except Exception as exc:
                                    logger.warning(
                                        "CHECKING season pack id=%d: absolute episode fallback failed: %s",
                                        item.id, exc,
                                    )
                            # Relax season filter: files may lack season markers
                            # (e.g. "28. Episode Title.mp4" with no S02 prefix).
                            # The path prefix already constrains to the correct
                            # torrent directory, so dropping season is safe.
                            # Guard against multi-season torrents: if files span
                            # multiple distinct season values, re-apply the season
                            # filter so only the requested season's files are used.
                            if not matches and scan_result.matched_dir_path:
                                unfiltered = await mount_scanner.lookup_by_path_prefix(
                                    session,
                                    scan_result.matched_dir_path,
                                    season=None,
                                    episode=None,
                                )
                                if unfiltered and item.season is not None:
                                    distinct_seasons = {
                                        f.parsed_season for f in unfiltered
                                        if f.parsed_season is not None
                                    }
                                    if len(distinct_seasons) > 1:
                                        # Multi-season torrent: re-filter to requested season only.
                                        logger.info(
                                            "CHECKING season pack id=%d: relaxed fallback found multi-season "
                                            "torrent (%s), re-filtering to S%02d",
                                            item.id, sorted(distinct_seasons), item.season,
                                        )
                                        matches = [
                                            f for f in unfiltered
                                            if f.parsed_season == item.season
                                        ]
                                        if not matches:
                                            logger.warning(
                                                "CHECKING season pack id=%d: re-filtered multi-season set is empty "
                                                "for S%02d — leaving matches empty",
                                                item.id, item.season,
                                            )
                                    else:
                                        # Single season or no season markers: trust all files.
                                        matches = unfiltered
                                else:
                                    matches = unfiltered

                    # XEM scene pack fallback: the item's season is the scene
                    # season (e.g. S02) but mount files are stored with TMDB
                    # season (e.g. S01).  When xem_scene_pack metadata is
                    # present and the standard lookup found nothing, retry with
                    # the TMDB anchor season and then filter to the TMDB episode
                    # range.  Set episode_offset so symlinks get scene numbering.
                    if not matches and item.metadata_json:
                        try:
                            _xem_meta = json.loads(item.metadata_json)
                            if _xem_meta.get("xem_scene_pack"):
                                _anchor_season = _xem_meta.get("tmdb_anchor_season")
                                _anchor_ep = _xem_meta.get("tmdb_anchor_episode")
                                _end_ep = _xem_meta.get("tmdb_end_episode")
                                if _anchor_season is not None:
                                    logger.info(
                                        "CHECKING season pack id=%d: XEM scene pack fallback, "
                                        "retrying lookup with TMDB anchor season=%d "
                                        "(item scene_season=%d)",
                                        item.id, int(_anchor_season), item.season or 0,
                                    )
                                    xem_matches = await mount_scanner.lookup(
                                        session,
                                        title=item.title,
                                        season=int(_anchor_season),
                                        episode=None,
                                    )
                                    if not xem_matches and scan_result and scan_result.matched_dir_path:
                                        xem_matches = await mount_scanner.lookup_by_path_prefix(
                                            session,
                                            scan_result.matched_dir_path,
                                            season=int(_anchor_season),
                                            episode=None,
                                        )
                                    if xem_matches and _anchor_ep is not None and _end_ep is not None:
                                        # Filter to only the TMDB episode range for this scene season.
                                        xem_matches = [
                                            f for f in xem_matches
                                            if f.parsed_episode is not None
                                            and int(_anchor_ep) <= f.parsed_episode <= int(_end_ep)
                                        ]
                                        if xem_matches:
                                            # Offset maps TMDB absolute episode → scene episode.
                                            # e.g. anchor_ep=14, offset=13 → E14 becomes E01.
                                            _season_pack_episode_offset = int(_anchor_ep) - 1
                                            matches = xem_matches
                                            logger.info(
                                                "CHECKING season pack id=%d: XEM scene pack matched "
                                                "%d files (TMDB S%02dE%02d-E%02d, offset=%d)",
                                                item.id, len(matches),
                                                int(_anchor_season), int(_anchor_ep), int(_end_ep),
                                                _season_pack_episode_offset,
                                            )
                        except (ValueError, TypeError, AttributeError) as _xem_exc:
                            logger.debug(
                                "CHECKING season pack id=%d: XEM scene pack metadata parse failed: %s",
                                item.id, _xem_exc,
                            )

                    # Auto-correct: season packs must be shows
                    if matches and item.media_type == MediaType.MOVIE:
                        logger.info(
                            "CHECKING season pack id=%d: correcting media_type movie → show",
                            item.id,
                        )
                        item.media_type = MediaType.SHOW

                    # Filter out sample files, special files (NCOP/NCED/OP/ED),
                    # and files with no parsed episode.
                    if matches:
                        filtered = [
                            m for m in matches
                            if m.parsed_episode is not None
                            and not os.path.basename(m.filepath).lower().startswith("sample")
                            and not _SPECIAL_FILENAME_RE.search(os.path.basename(m.filepath))
                        ]
                        if not filtered and matches:
                            # All files lack parsed_episode — trigger a targeted
                            # re-scan so updated parser fallbacks (e.g. 3-digit
                            # S01E001 episodes) can fill in the episode numbers.
                            logger.info(
                                "CHECKING season pack id=%d: %d files found but none "
                                "have parsed_episode, triggering re-scan",
                                item.id, len(matches),
                            )
                            torrent = await _find_torrent_for_item(session, item)
                            if torrent and torrent.filename:
                                rescan = await mount_scanner.scan_directory(session, torrent.filename)
                                if rescan.files_indexed > 0:
                                    matches = await mount_scanner.lookup(
                                        session,
                                        title=item.title,
                                        season=item.season,
                                        episode=None,
                                    )
                                    filtered = [
                                        m for m in matches
                                        if m.parsed_episode is not None
                                        and not os.path.basename(m.filepath).lower().startswith("sample")
                                        and not _SPECIAL_FILENAME_RE.search(os.path.basename(m.filepath))
                                    ]
                            if not filtered:
                                logger.warning(
                                    "CHECKING season pack id=%d: no valid episode files after filtering",
                                    item.id,
                                )
                        matches = filtered

                    if not matches:
                        timeout_threshold = datetime.now(timezone.utc) - timedelta(
                            minutes=settings.retry.checking_timeout_minutes
                        )
                        # Normalize: SQLite-loaded datetimes are naive, in-memory ones are tz-aware
                        sca = item.state_changed_at
                        if sca is not None and sca.tzinfo is None:
                            sca = sca.replace(tzinfo=timezone.utc)
                        if sca and sca <= timeout_threshold:
                            # Store failed hash for loop prevention (before transition)
                            _sp_torrent = await _find_torrent_for_item(session, item)
                            if _sp_torrent and _sp_torrent.info_hash:
                                try:
                                    _sp_meta = json.loads(item.metadata_json) if item.metadata_json else {}
                                except (ValueError, TypeError):
                                    _sp_meta = {}
                                _sp_meta["checking_failed_hash"] = _sp_torrent.info_hash
                                item.metadata_json = json.dumps(_sp_meta)
                                await session.flush()
                            logger.warning(
                                "CHECKING season pack id=%d title=%r timed out after %d min, transitioning to SLEEPING",
                                item.id, item.title, settings.retry.checking_timeout_minutes,
                            )
                            await queue_manager.transition(session, item.id, QueueState.SLEEPING)
                        else:
                            logger.info(
                                "CHECKING season pack id=%d title=%r not found in mount yet, will retry next cycle",
                                item.id, item.title,
                            )
                        continue

                    # Deduplicate: pick one file per episode
                    _RES_RANK = {"2160p": 4, "1080p": 3, "720p": 2, "480p": 1}
                    by_episode: dict[int, list] = {}
                    for m in matches:
                        by_episode.setdefault(m.parsed_episode, []).append(m)

                    best_per_episode = []
                    for ep, ep_matches in sorted(by_episode.items()):
                        def _sort_key(m):
                            res = m.parsed_resolution
                            if item.requested_resolution and res == item.requested_resolution:
                                preferred = 1  # boost exact match
                            else:
                                preferred = 0
                            return (preferred, _RES_RANK.get(res, 0), m.filesize or 0)
                        best = max(ep_matches, key=_sort_key)
                        best_per_episode.append(best)

                    logger.info(
                        "CHECKING season pack id=%d: %d mount matches deduplicated to %d episodes",
                        item.id, len(matches), len(best_per_episode),
                    )
                    created = 0
                    symlink = None
                    stale_paths: list[str] = []
                    for match in best_per_episode:
                        try:
                            symlink = await symlink_manager.create_symlink(
                                session, item, match.filepath,
                                episode_offset=_season_pack_episode_offset,
                            )
                            created += 1
                        except SourceNotFoundError:
                            stale_paths.append(match.filepath)
                        except Exception:
                            logger.warning(
                                "CHECKING season pack id=%d: failed to symlink %s, skipping",
                                item.id, match.filepath,
                            )
                    if stale_paths:
                        from src.models.mount_index import MountIndex

                        await session.execute(
                            delete(MountIndex).where(
                                MountIndex.filepath.in_(stale_paths)
                            )
                        )
                        await session.flush()
                        logger.warning(
                            "CHECKING season pack id=%d: purged %d stale mount index entries",
                            item.id, len(stale_paths),
                        )
                    if created == 0:
                        logger.warning(
                            "CHECKING season pack id=%d: all %d symlinks failed, will retry next cycle",
                            item.id, len(best_per_episode),
                        )
                        continue
                else:
                    # Single episode/movie: use the first (most recent) match
                    torrent = None
                    _se_alt_titles = await gather_alt_titles(session, item)
                    matches = await mount_scanner.lookup_multi(
                        session,
                        _se_alt_titles,
                        season=item.season,
                        episode=item.episode,
                    )
                    scan_result = None
                    if not matches:
                        # Targeted scan: check if the RD torrent directory exists on mount
                        torrent = await _find_torrent_for_item(session, item)
                        if torrent and torrent.filename:
                            scan_result = await mount_scanner.scan_directory(session, torrent.filename)
                            if scan_result.files_indexed > 0:
                                _post_scan_titles = await gather_alt_titles(
                                    session, item,
                                    torrent_filename=torrent.filename,
                                )
                                matches = await mount_scanner.lookup_multi(
                                    session,
                                    _post_scan_titles,
                                    season=item.season,
                                    episode=item.episode,
                                )
                            # Save matched_dir_path before RD refresh may overwrite scan_result
                            first_matched_dir_path = scan_result.matched_dir_path if scan_result else None
                            # RD filename refresh: torrent.filename may be stale (e.g.
                            # stored as item title instead of actual RD torrent name).
                            # Re-fetch from RD API and retry scan+lookup if it differs.
                            if not matches and torrent.rd_id:
                                try:
                                    rd_info = await rd_client.get_torrent_info(torrent.rd_id)
                                    rd_filename = rd_info.get("filename")
                                    if rd_filename and rd_filename != torrent.filename:
                                        logger.info(
                                            "CHECKING item id=%d: refreshing torrent filename %r → %r",
                                            item.id, torrent.filename, rd_filename,
                                        )
                                        torrent.filename = rd_filename
                                        scan_result = await mount_scanner.scan_directory(session, rd_filename)
                                        if scan_result.files_indexed > 0:
                                            matches = await mount_scanner.lookup_multi(
                                                session,
                                                await gather_alt_titles(
                                                    session, item,
                                                    torrent_filename=rd_filename,
                                                ),
                                                season=item.season,
                                                episode=item.episode,
                                            )
                                except Exception:
                                    logger.warning(
                                        "CHECKING item id=%d: RD filename refresh failed",
                                        item.id,
                                    )
                            # Path-based fallback: PTN may parse individual filenames
                            # differently from the item title (e.g. disc rips, episode
                            # title filenames), so fall back to directory prefix lookup.
                            # Run whenever matched_dir_path is known, regardless of whether
                            # new files were indexed (files may have been indexed by a prior
                            # full scan).
                            effective_dir_path = (
                                (scan_result.matched_dir_path if scan_result else None)
                                or first_matched_dir_path
                            )
                            if not matches and effective_dir_path:
                                logger.info(
                                    "CHECKING item id=%d: title lookup failed, "
                                    "trying path prefix %r",
                                    item.id, effective_dir_path,
                                )
                                matches = await mount_scanner.lookup_by_path_prefix(
                                    session,
                                    effective_dir_path,
                                    season=item.season,
                                    episode=item.episode,
                                )
                                # Relax season filter: anime files often lack season
                                # markers (e.g. "[Group] Show - 01.mkv").  The path
                                # prefix already constrains to the exact torrent
                                # directory, so matching by episode alone is safe.
                                if not matches:
                                    matches = await mount_scanner.lookup_by_path_prefix(
                                        session,
                                        effective_dir_path,
                                        season=None,
                                        episode=item.episode,
                                    )
                                # Last resort: single-file torrent directories contain
                                # exactly one file.  Skip episode filter entirely.
                                if not matches:
                                    matches = await mount_scanner.lookup_by_path_prefix(
                                        session,
                                        effective_dir_path,
                                        season=None,
                                        episode=None,
                                    )
                                    if matches:
                                        logger.info(
                                            "CHECKING item id=%d: no-filter fallback found %d file(s) "
                                            "(parsed_season=%s parsed_episode=%s filename=%r)",
                                            item.id, len(matches),
                                            matches[0].parsed_season, matches[0].parsed_episode,
                                            matches[0].filename,
                                        )
                                        # Only trust this for single-file directories
                                        if len(matches) != 1:
                                            matches = []

                    # XEM fallback: files may use scene numbering (e.g. S02E01)
                    # while the item stores TMDB numbering (e.g. S01E29).
                    # Try again with scene-mapped season/episode.
                    if (
                        not matches
                        and settings.xem.enabled
                        and item.season is not None
                        and item.episode is not None
                    ):
                        try:
                            from src.core.xem_mapper import xem_mapper

                            mapping = await xem_mapper.get_scene_numbering_for_item(
                                session, item.tvdb_id, item.tmdb_id,
                                item.season, item.episode,
                            )
                            if mapping is not None:
                                scene_season, scene_episode = mapping
                                logger.info(
                                    "CHECKING item id=%d: XEM remap S%02dE%02d → S%02dE%02d",
                                    item.id, item.season, item.episode,
                                    scene_season, scene_episode,
                                )
                                matches = await mount_scanner.lookup(
                                    session,
                                    title=item.title,
                                    season=scene_season,
                                    episode=scene_episode,
                                )
                                if not matches and scan_result and scan_result.matched_dir_path:
                                    matches = await mount_scanner.lookup_by_path_prefix(
                                        session,
                                        scan_result.matched_dir_path,
                                        season=scene_season,
                                        episode=scene_episode,
                                    )
                        except Exception:
                            logger.debug(
                                "CHECKING item id=%d: XEM lookup failed, using original numbering",
                                item.id,
                            )

                    if not matches:
                        timeout_threshold = datetime.now(timezone.utc) - timedelta(
                            minutes=settings.retry.checking_timeout_minutes
                        )
                        # Normalize: SQLite-loaded datetimes are naive, in-memory ones are tz-aware
                        sca = item.state_changed_at
                        if sca is not None and sca.tzinfo is None:
                            sca = sca.replace(tzinfo=timezone.utc)
                        if sca and sca <= timeout_threshold:
                            # Store failed hash BEFORE transitioning (so both happen or neither)
                            if torrent is None:
                                torrent = await _find_torrent_for_item(session, item)
                            if torrent and torrent.info_hash:
                                try:
                                    meta = json.loads(item.metadata_json) if item.metadata_json else {}
                                except (ValueError, TypeError):
                                    meta = {}
                                meta["checking_failed_hash"] = torrent.info_hash
                                item.metadata_json = json.dumps(meta)
                                await session.flush()
                            logger.warning(
                                "CHECKING item id=%d title=%r timed out after %d min, transitioning to SLEEPING",
                                item.id, item.title, settings.retry.checking_timeout_minutes,
                            )
                            await queue_manager.transition(session, item.id, QueueState.SLEEPING)
                        else:
                            logger.info(
                                "CHECKING item id=%d title=%r not found in mount yet, will retry next cycle",
                                item.id, item.title,
                            )
                        continue

                    # Auto-promote: show item found multiple distinct episodes in mount
                    if (
                        item.media_type == MediaType.SHOW
                        and not item.is_season_pack
                        and len(matches) > 1
                    ):
                        # Only count files with a parsed episode number (exclude bonus/extras)
                        ep_matches = [m for m in matches if m.parsed_episode is not None]
                        distinct_episodes = {m.parsed_episode for m in ep_matches}
                        if len(distinct_episodes) > 1:
                            logger.info(
                                "CHECKING item id=%d: auto-promoting to season pack (%d files, %d distinct episodes)",
                                item.id, len(ep_matches), len(distinct_episodes),
                            )

                            # Deduplicate: pick best file per episode
                            _RES_RANK = {"2160p": 4, "1080p": 3, "720p": 2, "480p": 1}
                            by_episode: dict[int, list] = {}
                            for m in ep_matches:
                                by_episode.setdefault(m.parsed_episode, []).append(m)

                            best_per_episode = []
                            for ep, ep_group in sorted(by_episode.items()):
                                def _sort_key(m):
                                    res = m.parsed_resolution
                                    if item.requested_resolution and res == item.requested_resolution:
                                        preferred = 1
                                    else:
                                        preferred = 0
                                    return (preferred, _RES_RANK.get(res, 0), m.filesize or 0)
                                best = max(ep_group, key=_sort_key)
                                best_per_episode.append(best)

                            created = 0
                            created_symlinks: list[Symlink] = []
                            stale_paths_auto: list[str] = []
                            for match in best_per_episode:
                                try:
                                    symlink = await symlink_manager.create_symlink(session, item, match.filepath)
                                    created += 1
                                    created_symlinks.append(symlink)
                                except SourceNotFoundError:
                                    stale_paths_auto.append(match.filepath)
                                except Exception:
                                    logger.warning(
                                        "CHECKING item id=%d: failed to symlink %s, skipping",
                                        item.id, match.filepath,
                                    )
                            if stale_paths_auto:
                                from src.models.mount_index import MountIndex

                                await session.execute(
                                    delete(MountIndex).where(
                                        MountIndex.filepath.in_(stale_paths_auto)
                                    )
                                )
                                await session.flush()
                                logger.warning(
                                    "CHECKING item id=%d: purged %d stale mount index entries",
                                    item.id, len(stale_paths_auto),
                                )
                            if created == 0:
                                logger.warning(
                                    "CHECKING item id=%d: all %d symlinks failed, will retry next cycle",
                                    item.id, len(best_per_episode),
                                )
                                continue

                            # Persist metadata changes only after symlinks succeed
                            item.is_season_pack = True
                            item.episode = None
                            if item.season is None:
                                file_seasons = {m.parsed_season for m in ep_matches if m.parsed_season is not None}
                                if len(file_seasons) == 1:
                                    item.season = file_seasons.pop()

                            # Clear loop-prevention flag on success (auto-promote path)
                            if item.metadata_json:
                                try:
                                    _meta_ap = json.loads(item.metadata_json)
                                    if _meta_ap.pop("checking_failed_hash", None) is not None:
                                        item.metadata_json = json.dumps(_meta_ap) if _meta_ap else None
                                        await session.flush()
                                except (ValueError, TypeError):
                                    pass

                            await queue_manager.transition(session, item.id, QueueState.COMPLETE)
                            for sl in created_symlinks:
                                media_scan_queue.append((item.media_type, sl.target_path))
                            continue

                    # Filesize verification: prefer matches whose size is close
                    # to the RD torrent's reported size (protects against stale
                    # mount_index entries from Zurg auto-recovery).
                    if len(matches) > 1:
                        if torrent is None:
                            torrent = await _find_torrent_for_item(session, item)
                        if torrent and torrent.filesize:
                            expected = torrent.filesize
                            # Accept files within 15% of expected size
                            lo = expected * 0.85
                            hi = expected * 1.15
                            size_matched = [
                                m for m in matches
                                if m.filesize is not None and lo <= m.filesize <= hi
                            ]
                            if size_matched:
                                logger.info(
                                    "CHECKING item id=%d: filesize filter narrowed %d → %d matches "
                                    "(expected %d bytes)",
                                    item.id,
                                    len(matches),
                                    len(size_matched),
                                    expected,
                                )
                                matches = size_matched

                    source_path = matches[0].filepath
                    logger.info(
                        "CHECKING item id=%d found in mount: %s", item.id, source_path,
                    )
                    try:
                        symlink = await symlink_manager.create_symlink(session, item, source_path)
                    except SourceNotFoundError:
                        from src.models.mount_index import MountIndex

                        await session.execute(
                            delete(MountIndex).where(
                                MountIndex.filepath == source_path
                            )
                        )
                        await session.flush()
                        logger.warning(
                            "CHECKING item id=%d: source not found, purged stale "
                            "mount index entry %s, will retry next cycle",
                            item.id, source_path,
                        )
                        continue

                # Clear loop-prevention flag on success
                if item.metadata_json:
                    try:
                        _meta_complete = json.loads(item.metadata_json)
                        if _meta_complete.pop("checking_failed_hash", None) is not None:
                            item.metadata_json = json.dumps(_meta_complete) if _meta_complete else None
                            await session.flush()
                    except (ValueError, TypeError):
                        pass

                await queue_manager.transition(session, item.id, QueueState.COMPLETE)

                # Queue media file path (triggered after all items are processed)
                if symlink is not None:
                    media_scan_queue.append((item.media_type, symlink.target_path))

            except Exception:
                logger.exception(
                    "Failed to process CHECKING item id=%d title=%s",
                    item.id, item.title,
                )

        # --- Plex scans (batched, deduplicated) ---
        if media_scan_queue:
            try:
                if settings.plex.enabled and settings.plex.scan_after_symlink and settings.plex.token:
                    from src.services.plex import plex_client

                    # Deduplicate: one scan per unique (section_id, scan_dir)
                    seen_scans: set[tuple[int, str]] = set()
                    for media_type, target_path in media_scan_queue:
                        scan_dir = os.path.dirname(target_path)
                        section_ids = (
                            settings.plex.movie_section_ids
                            if media_type == MediaType.MOVIE
                            else settings.plex.show_section_ids
                        )
                        if not section_ids:
                            continue
                        for sid in section_ids:
                            if (sid, scan_dir) in seen_scans:
                                continue
                            seen_scans.add((sid, scan_dir))
                            try:
                                await plex_client.scan_section(sid, path=scan_dir)
                                logger.info(
                                    "Triggered Plex scan for section %d path=%s",
                                    sid, scan_dir,
                                )
                            except Exception:
                                logger.exception(
                                    "Plex scan failed for section %d path=%s (non-fatal)",
                                    sid, scan_dir,
                                )
            except Exception:
                logger.exception("Plex batch scan trigger failed (non-fatal)")

        # --- Jellyfin scans (deduplicated per library ID) ---
        if media_scan_queue and settings.jellyfin.enabled and settings.jellyfin.scan_after_symlink and settings.jellyfin.api_key:
            from src.services.jellyfin import jellyfin_client

            try:
                seen_jf_libs: set[str] = set()
                for media_type, _target_path in media_scan_queue:
                    lib_ids = (
                        settings.jellyfin.movie_library_ids
                        if media_type == MediaType.MOVIE
                        else settings.jellyfin.show_library_ids
                    )
                    for lib_id in lib_ids:
                        if lib_id in seen_jf_libs:
                            continue
                        seen_jf_libs.add(lib_id)
                        try:
                            await jellyfin_client.scan_library(lib_id)
                        except Exception:
                            logger.exception(
                                "Jellyfin scan_library failed for %s (non-fatal)",
                                lib_id,
                            )
            except Exception:
                logger.exception("Jellyfin scan trigger failed (non-fatal)")

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
from src.api.routes.queue import router as queue_router  # noqa: E402
from src.api.routes.search import router as search_router  # noqa: E402
from src.api.routes.settings import router as settings_router  # noqa: E402
from src.api.routes.duplicates import router as duplicates_router  # noqa: E402
from src.api.routes.discover import router as discover_router  # noqa: E402
from src.api.routes.sse import router as sse_router  # noqa: E402
from src.api.routes.tools import router as tools_router  # noqa: E402
from src.api.routes.show import router as show_router  # noqa: E402
from src.api.routes.health import router as health_router  # noqa: E402
from src.api.routes.omdb import router as omdb_router  # noqa: E402
from src.api.routes.movie import router as movie_router  # noqa: E402
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
