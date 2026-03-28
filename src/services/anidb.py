"""Async client for AniDB title enrichment and episode count lookups.

Uses two offline data sources (title dump + Fribb mapping) and an optional
HTTP API for episode counts. Designed for zero-latency title lookups from
SQLite during scraping.
"""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
import defusedxml.ElementTree as ET
from datetime import datetime, timezone
from typing import Any

import httpx
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.models.anidb import AnidbMapping, AnidbTitle
from src.services.http_client import CircuitOpenError, get_circuit_breaker, get_client

logger = logging.getLogger(__name__)

# Title type priority for ordering results
_TYPE_PRIORITY = {"main": 0, "official": 1, "syn": 2, "short": 3}


class AnidbClient:
    """AniDB title enrichment and episode count client."""

    async def _get_client(self) -> httpx.AsyncClient:
        """Return the pooled httpx.AsyncClient for AniDB HTTP API."""
        cfg = settings.anidb
        return await get_client(
            "anidb",
            cfg.api_base_url.rstrip("/"),
            timeout=cfg.timeout_seconds,
            headers={"User-Agent": "vibeDebrid/0.1"},
            follow_redirects=True,
        )

    async def is_data_fresh(self, session: AsyncSession) -> bool:
        """Check if cached data is within refresh_hours.

        Args:
            session: Async database session.

        Returns:
            True if the oldest AnidbTitle row is within the configured
            refresh window, False if the table is empty or data is stale.
        """
        result = await session.execute(
            select(func.min(AnidbTitle.fetched_at))
        )
        oldest = result.scalar()
        if oldest is None:
            return False
        if oldest.tzinfo is None:
            oldest = oldest.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - oldest).total_seconds() / 3600
        return age_hours < settings.anidb.refresh_hours

    async def refresh_data(self, session: AsyncSession) -> None:
        """Download title dump + Fribb mapping, full-replace both tables.

        Downloads the AniDB anime-titles XML dump and the Fribb anime-lists
        mapping JSON, parses both, then atomically replaces the contents of
        anidb_titles and anidb_mappings tables.

        Args:
            session: Async database session (caller is responsible for commit).
        """
        cfg = settings.anidb
        if not cfg.enabled:
            return

        breaker = get_circuit_breaker("anidb")
        try:
            await breaker.before_request()
        except CircuitOpenError:
            logger.warning("anidb.refresh_data: circuit open, skipping")
            return

        titles_data: list[dict[str, Any]] = []
        mappings_data: list[dict[str, Any]] = []

        # --- Download and parse title dump ---
        try:
            async with httpx.AsyncClient(timeout=cfg.timeout_seconds, follow_redirects=True) as client:
                resp = await client.get(cfg.titles_url, headers={"User-Agent": "vibeDebrid/0.1"})
            if not resp.is_success:
                logger.error("anidb.refresh_data: title dump HTTP %d", resp.status_code)
                await breaker.record_failure()
                return
            xml_bytes = gzip.decompress(resp.content)
            root = ET.fromstring(xml_bytes)
            for anime_el in root.iter("anime"):
                aid_str = anime_el.get("aid")
                if not aid_str:
                    continue
                try:
                    aid = int(aid_str)
                except ValueError:
                    continue
                for title_el in anime_el:
                    if title_el.tag != "title":
                        continue
                    lang = title_el.get("{http://www.w3.org/XML/1998/namespace}lang", "")
                    ttype = title_el.get("type", "")
                    text = (title_el.text or "").strip()
                    if text and lang in cfg.title_languages:
                        titles_data.append({
                            "anidb_id": aid,
                            "title": text,
                            "title_type": ttype,
                            "language": lang,
                        })
            logger.info("anidb.refresh_data: parsed %d titles from dump", len(titles_data))
        except (httpx.TimeoutException, httpx.ConnectError, httpx.RequestError) as exc:
            logger.error("anidb.refresh_data: title dump network error: %s", exc)
            await breaker.record_failure()
            return
        except (gzip.BadGzipFile, ET.ParseError, ValueError) as exc:
            logger.error("anidb.refresh_data: title dump parse error: %s", exc)
            await breaker.record_failure()
            return

        # --- Download and parse Fribb mapping ---
        try:
            async with httpx.AsyncClient(timeout=cfg.timeout_seconds, follow_redirects=True) as client:
                resp = await client.get(cfg.mappings_url, headers={"User-Agent": "vibeDebrid/0.1"})
            if not resp.is_success:
                logger.error("anidb.refresh_data: Fribb mapping HTTP %d", resp.status_code)
                await breaker.record_failure()
                return
            raw_mappings: list[dict[str, Any]] = resp.json()
            for entry in raw_mappings:
                anidb_id = entry.get("anidb_id")
                if not anidb_id:
                    continue
                # Season info is nested: {"tvdb": N, "tmdb": N}
                season_obj = entry.get("season") or {}
                mappings_data.append({
                    "anidb_id": int(anidb_id),
                    "tmdb_id": entry.get("themoviedb_id") or None,
                    "tmdb_season": season_obj.get("tmdb") if season_obj.get("tmdb") is not None else None,
                    "tvdb_id": entry.get("tvdb_id") or None,
                    "imdb_id": entry.get("imdb_id") or None,
                })
            logger.info("anidb.refresh_data: parsed %d mappings from Fribb", len(mappings_data))
        except (httpx.TimeoutException, httpx.ConnectError, httpx.RequestError) as exc:
            logger.error("anidb.refresh_data: Fribb mapping network error: %s", exc)
            await breaker.record_failure()
            return
        except (json.JSONDecodeError, ValueError, KeyError) as exc:
            logger.error("anidb.refresh_data: Fribb mapping parse error: %s", exc)
            await breaker.record_failure()
            return

        await breaker.record_success()

        # --- Deduplicate before insert ---
        # Title dump can have the same (anidb_id, title) under multiple types;
        # keep the first occurrence (highest priority type from parse order).
        seen_titles: set[tuple[int, str]] = set()
        deduped_titles: list[dict[str, Any]] = []
        for row in titles_data:
            key = (row["anidb_id"], row["title"])
            if key not in seen_titles:
                seen_titles.add(key)
                deduped_titles.append(row)

        # --- Full-replace both tables in one transaction ---
        now = datetime.now(timezone.utc)
        await session.execute(delete(AnidbTitle))
        await session.execute(delete(AnidbMapping))

        batch_size = 500
        for i in range(0, len(deduped_titles), batch_size):
            batch = deduped_titles[i:i + batch_size]
            session.add_all([
                AnidbTitle(fetched_at=now, **row) for row in batch
            ])

        for i in range(0, len(mappings_data), batch_size):
            batch = mappings_data[i:i + batch_size]
            session.add_all([
                AnidbMapping(fetched_at=now, **row) for row in batch
            ])

        await session.flush()
        logger.info(
            "anidb.refresh_data: stored %d titles + %d mappings",
            len(titles_data), len(mappings_data),
        )

    async def get_titles_for_tmdb_id(self, session: AsyncSession, tmdb_id: int) -> list[str]:
        """Look up AniDB title variants for a TMDB ID.

        Returns deduplicated titles ordered by type priority (main > official > syn > short).
        Returns [] for non-anime (no mapping = empty result, ~1ms).

        Args:
            session: Async database session.
            tmdb_id: TMDB show identifier.

        Returns:
            Ordered list of unique title strings, or empty list if no AniDB
            mapping exists or the feature is disabled.
        """
        if not settings.anidb.enabled:
            return []

        # Get all anidb_ids for this tmdb_id
        result = await session.execute(
            select(AnidbMapping.anidb_id).where(AnidbMapping.tmdb_id == tmdb_id)
        )
        anidb_ids = [row[0] for row in result.fetchall()]
        if not anidb_ids:
            return []

        # Get all titles for those anidb_ids, filtered by configured languages
        cfg = settings.anidb
        result = await session.execute(
            select(AnidbTitle.title, AnidbTitle.title_type, AnidbTitle.language)
            .where(
                AnidbTitle.anidb_id.in_(anidb_ids),
                AnidbTitle.language.in_(cfg.title_languages),
            )
            .order_by(AnidbTitle.title_type)
        )
        rows = result.fetchall()

        # Deduplicate and sort by type priority
        seen: set[str] = set()
        titles: list[tuple[int, str]] = []
        for title, ttype, _lang in rows:
            lower = title.lower()
            if lower not in seen:
                seen.add(lower)
                priority = _TYPE_PRIORITY.get(ttype, 99)
                titles.append((priority, title))

        titles.sort(key=lambda x: x[0])
        return [t for _, t in titles]

    async def get_episode_count_for_tmdb_season(
        self, session: AsyncSession, tmdb_id: int, season: int
    ) -> int | None:
        """Get the total episode count for a TMDB season from AniDB data.

        Looks up AniDB entries mapped to this TMDB season, sums their episode
        counts. Fetches from AniDB HTTP API lazily if not cached.
        Requires api_enabled + client_name configured.

        Args:
            session: Async database session.
            tmdb_id: TMDB show identifier.
            season: Season number to look up.

        Returns:
            Total episode count across all AniDB entries for this season,
            or None if no AniDB data is available or the API is not configured.
        """
        cfg = settings.anidb
        if not cfg.enabled:
            return None

        # Find AniDB entries for this TMDB season
        result = await session.execute(
            select(AnidbMapping).where(
                AnidbMapping.tmdb_id == tmdb_id,
                AnidbMapping.tmdb_season == season,
            )
        )
        entries = list(result.scalars().all())
        if not entries:
            return None

        total = 0
        any_fetched = False
        for entry in entries:
            if entry.episode_count is not None:
                total += entry.episode_count
                any_fetched = True
                continue

            # Try to fetch from AniDB HTTP API
            if not cfg.api_enabled or not cfg.client_name:
                continue

            count = await self._fetch_episode_count(entry.anidb_id)
            if count is not None:
                entry.episode_count = count
                total += count
                any_fetched = True
                await session.flush()
                # Rate limit: 1 req per 2 seconds per AniDB API rules
                await asyncio.sleep(2)

        return total if any_fetched else None

    async def get_episode_counts_up_to_season(
        self, session: AsyncSession, tmdb_id: int, up_to_season: int
    ) -> dict[int, int]:
        """Get AniDB episode counts for all seasons up to and including a target.

        Returns a dict of {season_number: episode_count} for seasons that have
        AniDB data. Missing seasons (no AniDB mapping or no episode count) are
        omitted — the caller should fall back to TMDB for those.

        Args:
            session: Async database session.
            tmdb_id: TMDB show identifier.
            up_to_season: Include seasons 1 through this number.

        Returns:
            Dict mapping season numbers to episode counts.
        """
        cfg = settings.anidb
        if not cfg.enabled:
            return {}

        # Fetch all mappings for this TMDB show up to the target season
        result = await session.execute(
            select(AnidbMapping).where(
                AnidbMapping.tmdb_id == tmdb_id,
                AnidbMapping.tmdb_season.isnot(None),
                AnidbMapping.tmdb_season >= 1,
                AnidbMapping.tmdb_season <= up_to_season,
            )
        )
        entries = list(result.scalars().all())
        if not entries:
            return {}

        # Group by season, resolve episode counts
        season_entries: dict[int, list[AnidbMapping]] = {}
        for entry in entries:
            season_entries.setdefault(entry.tmdb_season, []).append(entry)

        counts: dict[int, int] = {}
        for season_num, season_mappings in sorted(season_entries.items()):
            total = 0
            any_fetched = False
            for entry in season_mappings:
                if entry.episode_count is not None:
                    total += entry.episode_count
                    any_fetched = True
                    continue

                # Try AniDB HTTP API for missing counts
                if not cfg.api_enabled or not cfg.client_name:
                    continue

                count = await self._fetch_episode_count(entry.anidb_id)
                if count is not None:
                    entry.episode_count = count
                    total += count
                    any_fetched = True
                    await session.flush()
                    await asyncio.sleep(2)

            if any_fetched:
                counts[season_num] = total

        return counts

    async def get_cached_episode_counts(
        self, session: AsyncSession, tmdb_id: int
    ) -> dict[int, int]:
        """Get cached AniDB episode counts for all seasons of a TMDB show.

        Read-only — returns only pre-cached episode_count values, never makes
        API calls. Used by the show detail page for fast enrichment.

        Args:
            session: Async database session.
            tmdb_id: TMDB show identifier.

        Returns:
            Dict mapping season numbers to summed episode counts. Empty if
            disabled or no cached data exists.
        """
        if not settings.anidb.enabled:
            return {}

        result = await session.execute(
            select(AnidbMapping.tmdb_season, func.sum(AnidbMapping.episode_count))
            .where(
                AnidbMapping.tmdb_id == tmdb_id,
                AnidbMapping.tmdb_season.isnot(None),
                AnidbMapping.tmdb_season >= 1,
                AnidbMapping.episode_count.isnot(None),
            )
            .group_by(AnidbMapping.tmdb_season)
        )
        return {row[0]: row[1] for row in result.fetchall()}

    async def _fetch_episode_count(self, anidb_id: int) -> int | None:
        """Fetch episode count from AniDB HTTP API for a single anime entry.

        Args:
            anidb_id: The AniDB anime identifier.

        Returns:
            Episode count integer, or None on any failure.
        """
        cfg = settings.anidb
        breaker = get_circuit_breaker("anidb")
        try:
            await breaker.before_request()
        except CircuitOpenError:
            logger.warning("anidb._fetch_episode_count: circuit open, skipping aid=%d", anidb_id)
            return None

        try:
            client = await self._get_client()
            params = {
                "request": "anime",
                "client": cfg.client_name,
                "clientver": cfg.client_version,
                "protover": 1,
                "aid": anidb_id,
            }
            resp = await client.get("", params=params)
        except httpx.TimeoutException as exc:
            await breaker.record_failure()
            logger.warning("anidb._fetch_episode_count: timeout aid=%d: %s", anidb_id, exc)
            return None
        except httpx.ConnectError as exc:
            await breaker.record_failure()
            logger.warning("anidb._fetch_episode_count: connect error aid=%d: %s", anidb_id, exc)
            return None
        except httpx.RequestError as exc:
            await breaker.record_failure()
            logger.warning("anidb._fetch_episode_count: network error aid=%d: %s", anidb_id, exc)
            return None

        if not resp.is_success:
            if resp.status_code != 429:
                await breaker.record_failure()
            logger.warning("anidb._fetch_episode_count: HTTP %d for aid=%d", resp.status_code, anidb_id)
            return None

        await breaker.record_success()

        try:
            root = ET.fromstring(resp.text)
            ep_count_el = root.find(".//episodecount")
            if ep_count_el is not None and ep_count_el.text:
                count = int(ep_count_el.text)
                logger.debug("anidb._fetch_episode_count: aid=%d -> %d episodes", anidb_id, count)
                return count
        except ET.ParseError as exc:
            logger.warning("anidb._fetch_episode_count: XML parse error aid=%d: %s", anidb_id, exc)
        except ValueError as exc:
            logger.warning("anidb._fetch_episode_count: value error aid=%d: %s", anidb_id, exc)

        return None


# Module-level singleton
anidb_client = AnidbClient()
