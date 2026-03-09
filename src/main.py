"""FastAPI application entrypoint with startup/shutdown hooks and scheduler."""

import logging
import os
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
from sqlalchemy.ext.asyncio import AsyncSession

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

    cumulative = 0
    for s in regular_seasons:
        if s.season_number == target_season:
            start = cumulative + 1
            end = cumulative + s.episode_count
            logger.info(
                "_get_absolute_episode_range: tmdb_id=%s S%02d → absolute episodes %d-%d",
                tmdb_id, target_season, start, end,
            )
            return (start, end)
        cumulative += s.episode_count

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
                await queue_manager.force_transition(session, item.id, QueueState.WANTED)
            except Exception:
                logger.exception("Failed to recover stuck item id=%d", item.id)

        # --- Stage 1: WANTED → SCRAPING → pipeline ---
        result = await session.execute(
            select(MediaItem).where(MediaItem.state == QueueState.WANTED)
        )
        wanted_items = result.scalars().all()
        logger.info("WANTED items to scrape: %d", len(wanted_items))

        for item in wanted_items:
            # Cache attributes before try block so error handlers don't
            # trigger lazy loads on a potentially poisoned session.
            item_id = item.id
            item_title = item.title
            try:
                await queue_manager.transition(session, item_id, QueueState.SCRAPING)
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

        # Collect Plex scan directories — triggered AFTER all symlinks are
        # created so that a single scan per directory picks up every file.
        plex_scan_queue: list[tuple[str, str]] = []  # (media_type, scan_dir)

        for item in checking_items:
            try:
                if item.is_season_pack:
                    # Season packs: look up ALL episodes (episode=None) and symlink each match
                    matches = await mount_scanner.lookup(
                        session,
                        title=item.title,
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
                                                logger.info(
                                                    "CHECKING season pack id=%d: absolute episode fallback matched %d files "
                                                    "(absolute range %d-%d for S%02d)",
                                                    item.id, len(matches), start_ep, end_ep, item.season,
                                                )
                                except Exception as exc:
                                    logger.warning(
                                        "CHECKING season pack id=%d: absolute episode fallback failed: %s",
                                        item.id, exc,
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
                    by_episode: dict[int | None, list] = {}
                    for m in matches:
                        by_episode.setdefault(m.parsed_episode, []).append(m)

                    best_per_episode = []
                    for ep, ep_matches in sorted(by_episode.items(), key=lambda x: (x[0] is None, x[0])):
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
                    for match in best_per_episode:
                        try:
                            symlink = await symlink_manager.create_symlink(session, item, match.filepath)
                            created += 1
                        except Exception:
                            logger.warning(
                                "CHECKING season pack id=%d: failed to symlink %s, skipping",
                                item.id, match.filepath,
                            )
                    if created == 0:
                        logger.warning(
                            "CHECKING season pack id=%d: all %d symlinks failed, will retry next cycle",
                            item.id, len(best_per_episode),
                        )
                        continue
                else:
                    # Single episode/movie: use the first (most recent) match
                    matches = await mount_scanner.lookup(
                        session,
                        title=item.title,
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
                                matches = await mount_scanner.lookup(
                                    session,
                                    title=item.title,
                                    season=item.season,
                                    episode=item.episode,
                                )
                            # Path-based fallback: PTN may parse individual filenames
                            # differently from the item title (e.g. disc rips, episode
                            # title filenames), so fall back to directory prefix lookup.
                            # Run whenever matched_dir_path is known, regardless of whether
                            # new files were indexed (files may have been indexed by a prior
                            # full scan).
                            if not matches and scan_result.matched_dir_path:
                                logger.info(
                                    "CHECKING item id=%d: title lookup failed, "
                                    "trying path prefix %r",
                                    item.id, scan_result.matched_dir_path,
                                )
                                matches = await mount_scanner.lookup_by_path_prefix(
                                    session,
                                    scan_result.matched_dir_path,
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
                                        scan_result.matched_dir_path,
                                        season=None,
                                        episode=item.episode,
                                    )
                                # Last resort: single-file torrent directories contain
                                # exactly one file.  Skip episode filter entirely.
                                if not matches:
                                    matches = await mount_scanner.lookup_by_path_prefix(
                                        session,
                                        scan_result.matched_dir_path,
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

                    source_path = matches[0].filepath
                    logger.info(
                        "CHECKING item id=%d found in mount: %s", item.id, source_path,
                    )
                    symlink = await symlink_manager.create_symlink(session, item, source_path)

                await queue_manager.transition(session, item.id, QueueState.COMPLETE)

                # Queue Plex scan directory (triggered after all items are processed)
                if symlink is not None:
                    scan_dir = os.path.dirname(symlink.target_path)
                    plex_scan_queue.append((item.media_type, scan_dir))

            except Exception:
                logger.exception(
                    "Failed to process CHECKING item id=%d title=%s",
                    item.id, item.title,
                )

        # --- Plex scans (batched, deduplicated) ---
        if plex_scan_queue:
            try:
                if settings.plex.enabled and settings.plex.scan_after_symlink and settings.plex.token:
                    from src.services.plex import plex_client

                    # Deduplicate: one scan per unique (section_id, scan_dir)
                    seen_scans: set[tuple[int, str]] = set()
                    for media_type, scan_dir in plex_scan_queue:
                        section_ids = (
                            settings.plex.movie_section_ids
                            if media_type == "movie"
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
from src.api.routes.discover import router as discover_router  # noqa: E402
from src.api.routes.sse import router as sse_router  # noqa: E402
from src.api.routes.tools import router as tools_router  # noqa: E402
from src.api.routes.show import router as show_router  # noqa: E402

app.include_router(dashboard_router)
app.include_router(queue_router, prefix="/api/queue", tags=["queue"])
app.include_router(search_router, prefix="/api", tags=["search"])
app.include_router(settings_router, prefix="/api/settings", tags=["settings"])
app.include_router(duplicates_router, prefix="/api/duplicates", tags=["duplicates"])
app.include_router(discover_router, prefix="/api/discover", tags=["discover"])
app.include_router(sse_router, prefix="/api", tags=["sse"])
app.include_router(tools_router, tags=["tools"])
app.include_router(show_router, prefix="/api/show", tags=["show"])


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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.main:app",
        host=settings.server.host,
        port=settings.server.port,
        reload=True,
    )
