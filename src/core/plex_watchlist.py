"""Plex watchlist sync: fetch watchlist items and add missing ones to the queue.

Design notes:
- Batch dedup: one SQL query collects all existing tmdb_ids and imdb_ids before
  any inserts, avoiding N+1 database round-trips.
- Per-item IntegrityError handling: a race condition on a single item does not
  abort the rest of the sync.
- TV shows automatically get a MonitoredShow record so future seasons are tracked.
- Caller owns the transaction — this module flushes but does not commit.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.models.media_item import MediaItem, MediaType, QueueState
from src.services.plex import plex_client
from src.services.tmdb import tmdb_client

logger = logging.getLogger(__name__)


async def sync_watchlist(session: AsyncSession) -> dict[str, int]:
    """Fetch Plex watchlist, dedup against queue, and create new WANTED items.

    For each new watchlist item:
    - Movies are created with ``season=None, episode=None, is_season_pack=False``.
    - TV shows are created with ``season=1, episode=None, is_season_pack=True``
      and a MonitoredShow record is created so future seasons are auto-added.

    Missing IDs are resolved via TMDB before insertion.  Items that already exist
    in the queue (matched by tmdb_id or imdb_id) are silently skipped.

    Args:
        session: Async database session.  The caller is responsible for
                 committing or rolling back.

    Returns:
        A dict with keys ``"total"``, ``"added"``, ``"skipped"``, ``"errors"``.
    """
    stats: dict[str, int] = {"total": 0, "added": 0, "skipped": 0, "errors": 0}

    if not settings.plex.token or not settings.plex.watchlist_sync_enabled:
        logger.debug("plex_watchlist.sync: disabled or no token configured")
        return stats

    watchlist = await plex_client.get_watchlist()
    stats["total"] = len(watchlist)

    if not watchlist:
        logger.info("plex_watchlist.sync: watchlist is empty or could not be fetched")
        return stats

    # --- Batch dedup: collect all IDs and query the DB once ---
    all_tmdb_ids = [item.tmdb_id for item in watchlist if item.tmdb_id]
    all_imdb_ids = [item.imdb_id for item in watchlist if item.imdb_id]

    existing_tmdb_ids: set[str] = set()
    existing_imdb_ids: set[str] = set()

    if all_tmdb_ids or all_imdb_ids:
        # Build parameterised placeholders for the IN clauses.
        # SQLite does not support binding a list to a single :param, so we
        # expand each value as a separate named bind parameter.
        parts: list[str] = []
        bind_params: dict[str, str] = {}

        if all_tmdb_ids:
            t_placeholders = ", ".join(f":t{i}" for i in range(len(all_tmdb_ids)))
            for i, v in enumerate(all_tmdb_ids):
                bind_params[f"t{i}"] = v
            parts.append(f"tmdb_id IN ({t_placeholders})")

        if all_imdb_ids:
            m_placeholders = ", ".join(f":m{i}" for i in range(len(all_imdb_ids)))
            for i, v in enumerate(all_imdb_ids):
                bind_params[f"m{i}"] = v
            parts.append(f"imdb_id IN ({m_placeholders})")

        where_clause = " OR ".join(parts)
        rows = await session.execute(
            text(
                f"SELECT tmdb_id, imdb_id FROM media_items WHERE {where_clause}"
            ).bindparams(**bind_params)
        )
        for row_tmdb_id, row_imdb_id in rows:
            if row_tmdb_id:
                existing_tmdb_ids.add(str(row_tmdb_id))
            if row_imdb_id:
                existing_imdb_ids.add(str(row_imdb_id))

    logger.info(
        "plex_watchlist.sync: watchlist=%d existing_tmdb=%d existing_imdb=%d",
        len(watchlist),
        len(existing_tmdb_ids),
        len(existing_imdb_ids),
    )

    # --- Process each watchlist item ---
    for wl_item in watchlist:
        tmdb_id = wl_item.tmdb_id
        imdb_id = wl_item.imdb_id

        # Skip if already in queue by either ID
        if tmdb_id and tmdb_id in existing_tmdb_ids:
            logger.debug(
                "plex_watchlist.sync: skipping %r — tmdb_id=%s already in queue",
                wl_item.title,
                tmdb_id,
            )
            stats["skipped"] += 1
            continue

        if imdb_id and imdb_id in existing_imdb_ids:
            logger.debug(
                "plex_watchlist.sync: skipping %r — imdb_id=%s already in queue",
                wl_item.title,
                imdb_id,
            )
            stats["skipped"] += 1
            continue

        # --- Resolve missing IDs via TMDB ---
        tmdb_media_type = "tv" if wl_item.media_type == "show" else "movie"

        if tmdb_id and not imdb_id:
            # Resolve imdb_id from tmdb_id
            try:
                tmdb_int = int(tmdb_id)
                ext_ids = await tmdb_client.get_external_ids(tmdb_int, tmdb_media_type)
                if ext_ids is not None:
                    imdb_id = ext_ids.imdb_id
            except (ValueError, TypeError) as exc:
                logger.warning(
                    "plex_watchlist.sync: could not parse tmdb_id=%r for %r (%s)",
                    tmdb_id,
                    wl_item.title,
                    exc,
                )

        elif imdb_id and not tmdb_id:
            # No direct TMDB find-by-IMDB-ID endpoint available; the item will
            # be added with only imdb_id.  The scrape pipeline can still process
            # it via Torrentio/Zilean which accept imdb_id directly.
            logger.debug(
                "plex_watchlist.sync: no tmdb_id for %r — will add with imdb_id=%s only",
                wl_item.title,
                imdb_id,
            )

        # --- Guard: require at least one external ID ---
        if not tmdb_id and not imdb_id:
            logger.warning(
                "plex_watchlist.sync: skipping %r — no tmdb_id or imdb_id available",
                wl_item.title,
            )
            stats["skipped"] += 1
            continue

        # --- Mount index check: skip if content already exists in Zurg mount ---
        # This catches items added to RD outside of vibeDebrid (e.g. manually).
        try:
            from src.core.mount_scanner import mount_scanner  # noqa: PLC0415

            mount_match = await mount_scanner.lookup(
                session, wl_item.title, season=None, episode=None
            )
            if mount_match:
                logger.info(
                    "plex_watchlist.sync: skipping %r — already found in mount: %s",
                    wl_item.title,
                    mount_match[0].filepath,
                )
                stats["skipped"] += 1
                continue
        except Exception as exc:
            logger.warning(
                "plex_watchlist.sync: mount lookup failed for %r (%s), proceeding with add",
                wl_item.title,
                exc,
            )

        # --- Build the MediaItem ---
        now = datetime.now(timezone.utc)
        tmdb_id_str = str(tmdb_id) if tmdb_id else None

        if wl_item.media_type == "show":
            item_season: int | None = 1
            item_episode: int | None = None
            item_is_season_pack = True
            db_media_type = MediaType.SHOW
        else:
            item_season = None
            item_episode = None
            item_is_season_pack = False
            db_media_type = MediaType.MOVIE

        # --- Resolve original_language from TMDB ---
        original_language: str | None = None
        if tmdb_id:
            try:
                tmdb_int = int(tmdb_id)
                if wl_item.media_type == "show":
                    show_detail = await tmdb_client.get_show_details(tmdb_int)
                    if show_detail is not None:
                        original_language = show_detail.original_language
                else:
                    movie_detail = await tmdb_client.get_movie_details(tmdb_int)
                    if movie_detail is not None:
                        original_language = movie_detail.original_language
            except (ValueError, TypeError):
                pass
            except Exception as exc:
                logger.warning(
                    "plex_watchlist.sync: could not fetch original_language for %r "
                    "tmdb_id=%s (%s)",
                    wl_item.title,
                    tmdb_id,
                    exc,
                )

        item = MediaItem(
            title=wl_item.title,
            year=wl_item.year,
            media_type=db_media_type,
            tmdb_id=tmdb_id_str,
            imdb_id=imdb_id,
            state=QueueState.WANTED,
            source="plex_watchlist",
            added_at=now,
            state_changed_at=now,
            retry_count=0,
            season=item_season,
            episode=item_episode,
            is_season_pack=item_is_season_pack,
            original_language=original_language,
        )
        try:
            async with session.begin_nested():
                session.add(item)
                await session.flush()
        except IntegrityError:
            logger.info(
                "plex_watchlist.sync: skipping %r — conflict on flush (concurrent insert)",
                wl_item.title,
            )
            stats["skipped"] += 1
            continue
        except Exception as exc:
            logger.error(
                "plex_watchlist.sync: error inserting %r (%s)",
                wl_item.title,
                exc,
            )
            stats["errors"] += 1
            continue

        # Track newly inserted IDs to prevent duplicates within the same batch
        if tmdb_id_str:
            existing_tmdb_ids.add(tmdb_id_str)
        if imdb_id:
            existing_imdb_ids.add(imdb_id)

        logger.info(
            "plex_watchlist.sync: added %r item_id=%d tmdb_id=%s imdb_id=%s media_type=%s",
            wl_item.title,
            item.id,
            tmdb_id_str,
            imdb_id,
            wl_item.media_type,
        )

        # --- Enable monitoring and discover all seasons for TV shows ---
        if wl_item.media_type == "show" and tmdb_id_str:
            try:
                tmdb_int = int(tmdb_id_str)
                from src.core.show_manager import show_manager  # noqa: PLC0415
                from src.models.monitored_show import MonitoredShow  # noqa: PLC0415
                await show_manager.set_subscription(
                    session,
                    tmdb_id=tmdb_int,
                    enabled=True,
                    imdb_id=imdb_id,
                    title=wl_item.title,
                    year=wl_item.year,
                )
                await session.flush()
                # Immediately check for all available seasons so we don't
                # wait for the 6-hour monitored shows cycle.
                result = await session.execute(
                    select(MonitoredShow).where(
                        MonitoredShow.tmdb_id == tmdb_int
                    )
                )
                monitored = result.scalar_one_or_none()
                if monitored is not None:
                    new_from_check = await show_manager._check_single_show(
                        session, monitored
                    )
                    if new_from_check > 0:
                        logger.info(
                            "plex_watchlist.sync: discovered %d additional "
                            "season/episode items for %r",
                            new_from_check,
                            wl_item.title,
                        )
                        stats["added"] += new_from_check
            except (ValueError, TypeError) as exc:
                logger.warning(
                    "plex_watchlist.sync: could not enable monitoring for %r "
                    "tmdb_id=%s (%s)",
                    wl_item.title,
                    tmdb_id_str,
                    exc,
                )
            except Exception as exc:
                logger.warning(
                    "plex_watchlist.sync: monitoring setup failed for %r (%s)",
                    wl_item.title,
                    exc,
                    exc_info=True,
                )

        # --- Publish SSE event ---
        try:
            from src.core.event_bus import event_bus, QueueEvent  # noqa: PLC0415
            event_bus.publish(
                QueueEvent(
                    item_id=item.id,
                    title=wl_item.title,
                    old_state="",
                    new_state=QueueState.WANTED.value,
                    retry_count=0,
                    media_type=db_media_type.value,
                )
            )
        except Exception as exc:
            logger.warning(
                "plex_watchlist.sync: failed to publish SSE event for item_id=%d (%s)",
                item.id,
                exc,
            )

        stats["added"] += 1

    logger.info(
        "plex_watchlist.sync complete: total=%d added=%d skipped=%d errors=%d",
        stats["total"],
        stats["added"],
        stats["skipped"],
        stats["errors"],
    )
    return stats
