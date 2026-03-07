"""XEM scene numbering mapper with SQLite caching."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.models.xem_cache import XemCacheEntry
from src.services.xem import xem_client

logger = logging.getLogger(__name__)


class XemMapper:
    """Maps TVDB episode numbering to scene numbering using XEM with caching.

    The cache stores all mappings for a show keyed by tvdb_id.  On first
    lookup (or when the cache is stale) the full mapping set is fetched from
    XEM and written to SQLite so subsequent lookups within cache_hours are
    free of API calls.
    """

    async def get_scene_numbering(
        self,
        session: AsyncSession,
        tvdb_id: int,
        season: int,
        episode: int,
    ) -> tuple[int, int] | None:
        """Look up scene numbering for a TVDB season/episode.

        Checks SQLite cache first.  If the cache is stale (older than
        settings.xem.cache_hours) or missing, fetches from XEM and
        rebuilds the cache for this show.

        Args:
            session: Async database session.
            tvdb_id: The TVDB show ID.
            season: TVDB season number.
            episode: TVDB episode number.

        Returns:
            (scene_season, scene_episode) if a mapping exists, or None if
            no mapping is needed (numbers are identical or XEM has no data).
        """
        cache_cutoff = datetime.now(timezone.utc) - timedelta(hours=settings.xem.cache_hours)

        # Query all cache entries for this show to check freshness.
        result = await session.execute(
            select(XemCacheEntry).where(XemCacheEntry.tvdb_id == tvdb_id)
        )
        cached_entries = list(result.scalars().all())

        cache_is_fresh = (
            bool(cached_entries)
            and all(
                e.fetched_at.replace(tzinfo=timezone.utc) >= cache_cutoff
                if e.fetched_at.tzinfo is None
                else e.fetched_at >= cache_cutoff
                for e in cached_entries
            )
        )

        if not cache_is_fresh:
            # Fetch from XEM and rebuild cache.
            show_mappings = await xem_client.get_show_mappings(tvdb_id)

            # Delete stale/existing entries for this show.
            await session.execute(
                delete(XemCacheEntry).where(XemCacheEntry.tvdb_id == tvdb_id)
            )

            now = datetime.now(timezone.utc)
            new_entries: list[XemCacheEntry] = []
            for mapping in show_mappings.mappings:
                new_entries.append(
                    XemCacheEntry(
                        tvdb_id=tvdb_id,
                        tvdb_season=mapping.tvdb_season,
                        tvdb_episode=mapping.tvdb_episode,
                        scene_season=mapping.scene_season,
                        scene_episode=mapping.scene_episode,
                        fetched_at=now,
                    )
                )

            if new_entries:
                session.add_all(new_entries)
                await session.flush()
                logger.debug(
                    "xem_mapper: cached %d mappings for tvdb_id=%d",
                    len(new_entries),
                    tvdb_id,
                )
            else:
                logger.debug(
                    "xem_mapper: XEM returned no mappings for tvdb_id=%d", tvdb_id
                )
                # Nothing to cache — no mapping needed.
                return None

            # Use the freshly fetched mappings for the lookup below.
            cached_entries = new_entries

        # Search for the specific season/episode in cached entries.
        for entry in cached_entries:
            if entry.tvdb_season == season and entry.tvdb_episode == episode:
                logger.debug(
                    "xem_mapper: tvdb_id=%d S%02dE%02d → S%02dE%02d",
                    tvdb_id,
                    season,
                    episode,
                    entry.scene_season,
                    entry.scene_episode,
                )
                return entry.scene_season, entry.scene_episode

        # No mapping found — episode numbering is the same in both schemes.
        return None

    async def get_scene_numbering_for_item(
        self,
        session: AsyncSession,
        tvdb_id: int | None,
        tmdb_id: str | None,
        season: int,
        episode: int,
    ) -> tuple[int, int] | None:
        """Convenience wrapper that handles a missing tvdb_id.

        If tvdb_id is not available on the MediaItem, attempts to resolve it
        via the TMDB external_ids endpoint.  If the ID still cannot be
        determined, returns None (no mapping possible).

        Args:
            session: Async database session.
            tvdb_id: TVDB show ID from the MediaItem, or None.
            tmdb_id: TMDB ID string from the MediaItem, or None.
            season: TVDB season number.
            episode: TVDB episode number.

        Returns:
            (scene_season, scene_episode) or None.
        """
        resolved_tvdb_id = tvdb_id

        if resolved_tvdb_id is None and tmdb_id is not None:
            from src.services.tmdb import tmdb_client

            try:
                ext_ids = await tmdb_client.get_external_ids(int(tmdb_id), "tv")
                if ext_ids is not None:
                    resolved_tvdb_id = ext_ids.tvdb_id
            except Exception:
                # Broad catch: TVDB ID resolution is a non-critical fallback path.
                # Failures here just mean we use original numbering.
                logger.debug(
                    "xem_mapper: failed to resolve tvdb_id via TMDB for tmdb_id=%s",
                    tmdb_id,
                )

        if resolved_tvdb_id is None:
            logger.debug(
                "xem_mapper: no tvdb_id available for tmdb_id=%s — skipping XEM lookup",
                tmdb_id,
            )
            return None

        return await self.get_scene_numbering(session, resolved_tvdb_id, season, episode)


# Module-level singleton
xem_mapper = XemMapper()
