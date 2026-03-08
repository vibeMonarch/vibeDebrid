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

    async def _ensure_cached_entries(
        self,
        session: AsyncSession,
        tvdb_id: int,
    ) -> list[XemCacheEntry]:
        """Ensure cache is fresh and return all entries for tvdb_id.

        Checks SQLite cache first.  If stale or missing, fetches from XEM
        and rebuilds the cache.  Returns an empty list when XEM is disabled
        or returns no mappings for the show.

        Args:
            session: Async database session.
            tvdb_id: The TVDB show ID.

        Returns:
            List of XemCacheEntry rows for the show (may be empty).
        """
        if not settings.xem.enabled:
            logger.debug("xem_mapper: XEM disabled, skipping cache check")
            return []

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

        if cache_is_fresh:
            return cached_entries

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
                    tvdb_absolute=mapping.tvdb_absolute,
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

        return new_entries

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
            (scene_season, scene_episode) if a mapping exists and the numbers
            differ, or None if no remapping is needed.
        """
        cached_entries = await self._ensure_cached_entries(session, tvdb_id)

        if not cached_entries:
            return None

        # Search for the specific season/episode in cached entries.
        for entry in cached_entries:
            if entry.tvdb_season == season and entry.tvdb_episode == episode:
                # Identity check: if TVDB and scene numbers are the same,
                # no remapping is needed.
                if entry.scene_season == season and entry.scene_episode == episode:
                    return None
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

    async def get_all_scene_mappings(
        self,
        session: AsyncSession,
        tvdb_id: int,
    ) -> dict[tuple[int, int], tuple[int, int]]:
        """Get all TVDB→scene mappings for a show.

        Returns a dict mapping (tvdb_season, tvdb_episode) → (scene_season,
        scene_episode) for ALL episodes including identity mappings.

        Empty dict is returned when XEM is disabled, the show has no XEM
        mappings, or an error occurs.

        Args:
            session: Async database session.
            tvdb_id: The TVDB show ID.

        Returns:
            Dict of (tvdb_season, tvdb_episode) → (scene_season, scene_episode).
        """
        cached_entries = await self._ensure_cached_entries(session, tvdb_id)

        mapping: dict[tuple[int, int], tuple[int, int]] = {}
        for entry in cached_entries:
            mapping[(entry.tvdb_season, entry.tvdb_episode)] = (
                entry.scene_season,
                entry.scene_episode,
            )

        logger.debug(
            "xem_mapper.get_all_scene_mappings: tvdb_id=%d total_mappings=%d",
            tvdb_id,
            len(mapping),
        )
        return mapping

    async def get_absolute_scene_map(
        self,
        session: AsyncSession,
        tvdb_id: int,
    ) -> dict[int, tuple[int, int]] | None:
        """Map absolute episode numbers to scene (season, episode).

        Returns a dict mapping tvdb_absolute → (scene_season, scene_episode)
        for all episodes that have an absolute number.  This is used by the
        show detail page to bridge TMDB's continuous episode numbering to
        scene season structure.

        Args:
            session: Async database session.
            tvdb_id: The TVDB show ID.

        Returns:
            Dict of absolute → (scene_season, scene_episode), or None if
            XEM is disabled or has no data for this show.
        """
        cached_entries = await self._ensure_cached_entries(session, tvdb_id)

        if not cached_entries:
            return None

        mapping: dict[int, tuple[int, int]] = {}
        for entry in cached_entries:
            if entry.tvdb_absolute is not None:
                mapping[entry.tvdb_absolute] = (
                    entry.scene_season,
                    entry.scene_episode,
                )

        if not mapping:
            return None

        logger.debug(
            "xem_mapper.get_absolute_scene_map: tvdb_id=%d entries=%d",
            tvdb_id,
            len(mapping),
        )
        return mapping

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
