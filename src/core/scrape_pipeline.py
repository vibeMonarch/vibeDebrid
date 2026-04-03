"""Scraping orchestrator for vibeDebrid.

This module ties together all existing core modules and services to run the
full acquisition flow for a single media item.  It is the central orchestrator
called by the queue manager when an item enters SCRAPING state.

Priority order (SPEC.md Section 3.5):
1. Check Zurg mount index (instant, no API call)
2. Check RD account torrents (fast DB-only dedup check)
3. Query Zilean (local, fast)
4. Query Torrentio (remote, slower)
5. Combine, filter, and add to Real-Debrid

Design rules:
- NEVER let the pipeline crash — every external call is guarded and always
  returns a PipelineResult even when errors occur.
- The caller (queue manager) owns the commit — this module only calls
  ``session.flush()`` inside helpers that it delegates to.
- All state transitions go through ``queue_manager.transition()`` so the
  state-change audit trail is maintained in one place.
- Scrape history is logged into the ``scrape_log`` table after every
  significant pipeline step for debugging via the UI.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from pydantic import BaseModel
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.core.dedup import dedup_engine
from src.core.filter_engine import FilteredResult, filter_engine
from src.core.mount_scanner import VIDEO_EXTENSIONS, mount_scanner
from src.core.queue_manager import queue_manager
from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.mount_index import MountIndex
from src.models.scrape_result import ScrapeLog
from src.services.nyaa import NyaaResult, nyaa_client
from src.services.real_debrid import RealDebridError, RealDebridRateLimitError, rd_client
from src.services.torrent_parser import parse_episode_from_filename
from src.services.torrentio import TorrentioResult, torrentio_client
from src.services.zilean import ZileanResult, zilean_client

logger = logging.getLogger(__name__)

# Union type for combined scraper results
_AnyResult = TorrentioResult | ZileanResult | NyaaResult


# ---------------------------------------------------------------------------
# Output model
# ---------------------------------------------------------------------------


class PipelineResult(BaseModel):
    """Result of running the scrape pipeline for a single media item.

    Attributes:
        item_id: The MediaItem primary key that was processed.
        action: Short label for what happened, one of:
            ``"mount_hit"`` — file already present in the Zurg mount index,
            ``"dedup_hit"`` — content already tracked in the local RD registry,
            ``"added_to_rd"`` — torrent successfully added to Real-Debrid,
            ``"no_results"`` — no usable results from any scraper / filter,
            ``"error"`` — an unexpected exception interrupted the pipeline.
        message: Human-readable description of the outcome.
        selected_hash: Info hash of the torrent that was selected, or None.
        mount_path: Filepath from a mount index hit, or None.
        rd_torrent_id: Real-Debrid torrent ID returned by add_magnet, or None.
        scrape_results_count: Total raw results returned across all scrapers.
        filtered_results_count: Results that survived the filter engine.
    """

    item_id: int
    action: str
    message: str
    selected_hash: str | None = None
    mount_path: str | None = None
    rd_torrent_id: str | None = None
    scrape_results_count: int = 0
    filtered_results_count: int = 0


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def filter_year_mismatches(
    matches: list[MountIndex],
    item_year: int | None,
) -> list[MountIndex]:
    """Remove mount index entries whose parsed_year diverges from the item's year.

    Tolerance is ±1 year to handle air-year vs. release-year discrepancies
    (e.g. a show that premiered in December is often listed under the following
    calendar year in some databases).

    Entries without a ``parsed_year`` are always kept — the absence of year
    metadata is not evidence of a mismatch.  If ``item_year`` is ``None`` the
    function returns all matches unmodified.

    Args:
        matches: Raw results from ``mount_scanner.lookup_multi``.
        item_year: The canonical release year from the MediaItem, or None.

    Returns:
        Filtered list containing only entries that pass the year check.
    """
    if item_year is None:
        return matches
    return [
        m
        for m in matches
        if m.parsed_year is None or abs(m.parsed_year - item_year) <= 1
    ]


def _is_latin_script(text: str) -> bool:
    """Return True when the majority of non-whitespace characters are Latin.

    Accepts Basic Latin and Latin Extended blocks (U+0000–U+024F).  Characters
    beyond that range — CJK, Hangul, Kana, Arabic, etc. — are non-Latin.  The
    threshold is >50% so lightly romanised strings still pass.

    Args:
        text: Title string to evaluate.

    Returns:
        True when more than half of non-whitespace code points are ≤ U+024F.
    """
    chars = [c for c in text if not c.isspace()]
    if not chars:
        return True
    latin_count = sum(1 for c in chars if ord(c) < 0x0250)
    return latin_count / len(chars) > 0.5


async def collect_alt_titles(
    session: AsyncSession,
    tmdb_id: int | None,
    primary_title: str,
    media_type: MediaType,
    tmdb_original_title: str | None = None,
    *,
    max_titles: int = 3,
) -> list[str]:
    """Collect Latin-script alternative titles for a media item.

    Sources (in priority order):
      1. ``tmdb_original_title`` — already fetched by the pipeline's Step 0.
      2. AniDB SQLite cache — zero-latency lookup for anime shows.
      3. TMDB ``/alternative_titles`` — filled only when more slots remain.

    Non-Latin-script titles (CJK, Arabic, etc.) are excluded because scrapers
    return 0 results for them.  Results are case-insensitively deduplicated
    against ``primary_title`` and against each other.

    Args:
        session: Async database session (for AniDB lookup).
        tmdb_id: TMDB numeric identifier, or None when unknown.
        primary_title: The item's canonical title (used for dedup only).
        media_type: MediaType enum value (determines TMDB API endpoint).
        tmdb_original_title: Optional original-language title from the TMDB
            detail response, which the pipeline already fetched in Step 0.
        max_titles: Maximum number of alternative titles to return.

    Returns:
        Up to ``max_titles`` unique Latin-script alternative titles.
    """
    candidates: list[str] = []
    seen_lower: set[str] = {primary_title.lower()}

    def _add(title: str) -> None:
        if not title or not _is_latin_script(title):
            return
        low = title.lower()
        if low in seen_lower:
            return
        seen_lower.add(low)
        candidates.append(title)

    # Source 1: TMDB original title (already in memory — free)
    if tmdb_original_title:
        _add(tmdb_original_title)

    if tmdb_id is None or len(candidates) >= max_titles:
        return candidates[:max_titles]

    # Source 2: AniDB SQLite — zero-latency, non-anime returns [] immediately
    if settings.anidb.enabled:
        try:
            from src.services.anidb import anidb_client as _anidb_client

            anidb_titles = await _anidb_client.get_titles_for_tmdb_id(session, tmdb_id)
            for t in anidb_titles:
                _add(t)
                if len(candidates) >= max_titles:
                    break
        except Exception as exc:
            logger.warning("collect_alt_titles: AniDB lookup failed tmdb_id=%d: %s", tmdb_id, exc)

    if len(candidates) >= max_titles:
        return candidates[:max_titles]

    # Source 3: TMDB /alternative_titles — only when we still need more
    if settings.tmdb.enabled and settings.tmdb.api_key:
        try:
            from src.services.tmdb import tmdb_client as _tmdb_client

            media_type_str = "movie" if media_type == MediaType.MOVIE else "tv"
            extra = await _tmdb_client.get_alternative_titles(tmdb_id, media_type_str)
            for t in extra:
                _add(t)
                if len(candidates) >= max_titles:
                    break
        except Exception as exc:
            logger.debug(
                "collect_alt_titles: TMDB alt titles fetch failed tmdb_id=%d: %s", tmdb_id, exc
            )

    return candidates[:max_titles]


# ---------------------------------------------------------------------------
# ScrapePipeline
# ---------------------------------------------------------------------------


class ScrapePipeline:
    """Orchestrates the full acquisition flow for a single MediaItem.

    This class is intentionally stateless — it holds no mutable instance
    variables.  The module-level ``scrape_pipeline`` singleton is safe to
    share across concurrent coroutines.

    All public methods accept an ``AsyncSession`` and follow the project
    convention of flushing but not committing — transaction management belongs
    to the caller.
    """

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def run(self, session: AsyncSession, item: MediaItem) -> PipelineResult:
        """Run the full scrape-and-acquire pipeline for *item*.

        Executes the seven-step acquisition flow in order:
          1. Mount check — return immediately on a hit.
          2. Dedup check — return immediately when content already tracked.
          3. XEM scene numbering resolution (once for all scrapers).
          4. Zilean scrape — swallows all errors.
          4.5. Nyaa scrape (anime only, optional) — swallows all errors.
          5. Torrentio scrape — swallows all errors.
          6. Combine, check RD cache, filter.
          7. Add best result to Real-Debrid.

        The method never raises — every code path ends with a PipelineResult.
        State transitions (ADDING / SLEEPING) are applied here so the caller
        only needs to commit the session.

        Args:
            session: Caller-managed async database session.
            item: The MediaItem to process.  Must be in SCRAPING state.

        Returns:
            A PipelineResult describing what happened and what (if anything)
            was added to Real-Debrid.
        """
        logger.info(
            "scrape_pipeline.run: starting for item id=%d title=%r state=%s",
            item.id,
            item.title,
            item.state.value,
        )

        try:
            return await self._run_pipeline(session, item)
        except (httpx.RequestError, RealDebridError, TimeoutError, OSError) as exc:
            # Transient network/API errors — back off and retry later.
            logger.error(
                "scrape_pipeline.run: transient error for item id=%d title=%r: %s",
                item.id,
                item.title,
                exc,
                exc_info=True,
            )
            await self._safe_transition_sleeping(session, item)
            return PipelineResult(
                item_id=item.id,
                action="error",
                message=f"Transient pipeline error: {exc}",
            )
        except Exception as exc:
            # Unexpected / programming errors — log at CRITICAL so they surface
            # in monitoring, but still transition to SLEEPING to avoid leaving
            # the item stuck in SCRAPING state.
            logger.critical(
                "scrape_pipeline.run: unexpected error for item id=%d title=%r: %s",
                item.id,
                item.title,
                exc,
                exc_info=True,
            )
            await self._safe_transition_sleeping(session, item)
            return PipelineResult(
                item_id=item.id,
                action="error",
                message=f"Unexpected pipeline error: {exc}",
            )

    # ------------------------------------------------------------------
    # Internal pipeline steps
    # ------------------------------------------------------------------

    async def _run_pipeline(
        self, session: AsyncSession, item: MediaItem
    ) -> PipelineResult:
        """Inner pipeline logic — may raise; the outer run() catches everything.

        Args:
            session: Caller-managed async database session.
            item: The MediaItem to process.

        Returns:
            A PipelineResult describing the outcome.
        """
        # ------------------------------------------------------------------
        # Extract loop-prevention metadata early (used in hash-based dedup).
        # ------------------------------------------------------------------
        _checking_failed_hash: str | None = None
        try:
            _meta = json.loads(item.metadata_json) if item.metadata_json else {}
            _checking_failed_hash = _meta.get("checking_failed_hash")
        except (ValueError, TypeError):
            pass

        # ------------------------------------------------------------------
        # Step 0 — Fetch TMDB details for original_language backfill and
        # alternative title collection (used by Zilean fallback).
        # Runs whenever tmdb_id is present and TMDB is configured.
        # original_language backfill only writes when prefer_original_language
        # is enabled — avoids mutating the item without the feature on.
        # ------------------------------------------------------------------
        _tmdb_original_title: str | None = None  # populated below when available
        _detail_for_ep_count: Any = None

        if item.tmdb_id and settings.tmdb.enabled and settings.tmdb.api_key:
            try:
                from src.services.tmdb import tmdb_client as _tmdb_client

                tmdb_id_int = int(item.tmdb_id)
                if item.media_type == MediaType.MOVIE:
                    _detail = await _tmdb_client.get_movie_details(tmdb_id_int)
                else:
                    _detail = await _tmdb_client.get_show_details(tmdb_id_int)
                if _detail is not None:
                    _detail_for_ep_count = _detail
                    if (
                        item.original_language is None
                        and settings.filters.prefer_original_language
                        and _detail.original_language
                    ):
                        item.original_language = _detail.original_language
                        logger.debug(
                            "scrape_pipeline: backfilled original_language=%r for item id=%d",
                            item.original_language,
                            item.id,
                        )
                    _tmdb_original_title = _detail.original_title or None
            except Exception as exc:
                logger.debug(
                    "scrape_pipeline: TMDB detail fetch failed for item id=%d: %s",
                    item.id,
                    exc,
                )

        # ------------------------------------------------------------------
        # Step 1 — Mount check
        # ------------------------------------------------------------------
        mount_result = await self._step_mount_check(
            session, item, tmdb_original_title=_tmdb_original_title
        )
        if mount_result is not None:
            return mount_result

        # ------------------------------------------------------------------
        # Step 2 — Dedup check
        # ------------------------------------------------------------------
        dedup_result = await self._step_dedup_check(session, item)
        if dedup_result is not None:
            return dedup_result

        # ------------------------------------------------------------------
        # Step 3 — Resolve scene numbering once for both scrapers
        # ------------------------------------------------------------------
        scene_season: int | None = item.season
        scene_episode: int | None = item.episode

        # XEM scene pack branch: when a season pack carries xem_scene_pack
        # metadata, use the stored TMDB anchor season/episode for scrapers so
        # Torrentio is queried with TMDB coordinates (e.g. S01E14) instead of
        # the scene season number (e.g. S02E01).
        if (
            item.is_season_pack
            and item.episode is None
            and item.media_type == MediaType.SHOW
            and item.metadata_json
        ):
            try:
                _meta = json.loads(item.metadata_json)
                if _meta.get("xem_scene_pack"):
                    _anchor_season = _meta.get("tmdb_anchor_season")
                    _anchor_episode = _meta.get("tmdb_anchor_episode")
                    if _anchor_season is not None and _anchor_episode is not None:
                        scene_season = int(_anchor_season)
                        scene_episode = int(_anchor_episode)
                        logger.info(
                            "scrape_pipeline: XEM scene pack anchor remap for item id=%d "
                            "scene S%02d → TMDB S%02dE%02d",
                            item.id,
                            item.season or 0,
                            scene_season,
                            scene_episode,
                        )
            except (ValueError, TypeError, AttributeError) as exc:
                logger.debug(
                    "scrape_pipeline: XEM scene pack metadata parse failed for item id=%d: %s",
                    item.id, exc,
                )

        elif (
            item.media_type == MediaType.SHOW
            and settings.xem.enabled
            and item.episode is not None
            and item.season is not None
        ):
            from src.core.xem_mapper import xem_mapper

            try:
                mapping = await xem_mapper.get_scene_numbering_for_item(
                    session, item.tvdb_id, item.tmdb_id, item.season, item.episode
                )
                if mapping is not None:
                    scene_season, scene_episode = mapping
                    logger.info(
                        "scrape_pipeline: XEM mapping S%02dE%02d → S%02dE%02d for item id=%d",
                        item.season,
                        item.episode,
                        scene_season,
                        scene_episode,
                        item.id,
                    )
            except Exception as exc:
                logger.debug(
                    "scrape_pipeline: XEM lookup failed for item id=%d, using original numbering: %s",
                    item.id,
                    exc,
                )

        # ------------------------------------------------------------------
        # Step 4 — Zilean scrape
        # Collect alt titles once here so both Zilean and Nyaa can query them
        # concurrently alongside the primary title.
        # ------------------------------------------------------------------
        try:
            _alt_titles = await collect_alt_titles(
                session,
                int(item.tmdb_id) if item.tmdb_id else None,
                item.title,
                item.media_type,
                tmdb_original_title=_tmdb_original_title,
            )
        except Exception as exc:
            logger.warning(
                "scrape_pipeline: collect_alt_titles failed for item id=%d: %s",
                item.id,
                exc,
            )
            _alt_titles = []

        zilean_results, zilean_duration_ms = await self._step_zilean(
            session,
            item,
            scene_season=scene_season,
            scene_episode=scene_episode,
            alt_titles=_alt_titles,
        )

        # ------------------------------------------------------------------
        # Step 4.5 — Nyaa scrape (anime-focused, SHOW-only, optional)
        # ------------------------------------------------------------------
        nyaa_results, nyaa_duration_ms = await self._step_nyaa(
            session,
            item,
            scene_season=scene_season,
            scene_episode=scene_episode,
            alt_titles=_alt_titles,
        )

        # ------------------------------------------------------------------
        # Step 5 — Torrentio scrape
        # ------------------------------------------------------------------
        torrentio_results, torrentio_duration_ms = await self._step_torrentio(
            session, item, scene_season=scene_season, scene_episode=scene_episode
        )

        # ------------------------------------------------------------------
        # Step 6 — Combine, RD cache check, filter
        # ------------------------------------------------------------------
        # Dedup by info_hash across all scrapers before passing to filter so
        # the same torrent from Nyaa and Torrentio doesn't inflate counts.
        _seen_hashes: set[str] = set()
        _combined_deduped: list[_AnyResult] = []
        for _r in zilean_results + nyaa_results + torrentio_results:  # type: ignore[operator]
            _h = getattr(_r, "info_hash", None)
            if _h and _h in _seen_hashes:
                continue
            if _h:
                _seen_hashes.add(_h)
            _combined_deduped.append(_r)
        combined: list[_AnyResult] = _combined_deduped  # type: ignore[assignment]
        total_count = len(combined)

        if not combined:
            logger.info(
                "scrape_pipeline: no results from any scraper for item id=%d, "
                "transitioning to SLEEPING",
                item.id,
            )
            await self._log_scrape(
                session,
                media_item_id=item.id,
                scraper="pipeline",
                query_params=json.dumps({"title": item.title, "step": "combine"}),
                results_count=0,
                results_summary=json.dumps([]),
                selected_result=None,
                duration_ms=zilean_duration_ms + nyaa_duration_ms + torrentio_duration_ms,
            )
            await queue_manager.transition(session, item.id, QueueState.SLEEPING)
            return PipelineResult(
                item_id=item.id,
                action="no_results",
                message="No results returned from any scraper",
                scrape_results_count=0,
                filtered_results_count=0,
            )

        # Resolve original language name for scoring.
        from src.services.tmdb import iso_to_language_name
        orig_lang_name = iso_to_language_name(item.original_language) or item.original_language

        # Build cached_hashes from Torrentio results that have RD cache status.
        cached_hashes: set[str] = {
            r.info_hash for r in combined
            if getattr(r, "cached", False) and getattr(r, "info_hash", None)
        }

        # Build known_titles for title similarity scoring.
        known_titles: list[str] = [item.title]
        if _tmdb_original_title and _tmdb_original_title.lower() != item.title.lower():
            known_titles.append(_tmdb_original_title)
        for alt in _alt_titles:
            if alt.lower() not in {t.lower() for t in known_titles}:
                known_titles.append(alt)

        # Resolve expected episode count for season pack size validation.
        _expected_episode_count: int | None = None
        if item.is_season_pack and item.season is not None:
            # XEM scene packs: use stored TMDB episode list length
            if item.metadata_json:
                try:
                    _meta_ep = json.loads(item.metadata_json) if isinstance(item.metadata_json, str) else item.metadata_json
                    if _meta_ep.get("xem_scene_pack") and _meta_ep.get("tmdb_episodes"):
                        _expected_episode_count = len(_meta_ep["tmdb_episodes"])
                except (ValueError, TypeError):
                    pass

            # TMDB show details (already fetched in Step 0)
            if _expected_episode_count is None and _detail_for_ep_count is not None:
                seasons = getattr(_detail_for_ep_count, "seasons", None)
                if seasons:
                    season_info = next(
                        (s for s in seasons if s.season_number == item.season),
                        None,
                    )
                    if season_info is not None and season_info.episode_count > 0:
                        _expected_episode_count = season_info.episode_count

            # AniDB override for anime (read-only cache, no API calls)
            if _expected_episode_count is None and item.tmdb_id:
                try:
                    from src.services.anidb import anidb_client
                    _anidb_counts = await anidb_client.get_cached_episode_counts(session, int(item.tmdb_id))
                    if _anidb_counts:
                        _anidb_ep = _anidb_counts.get(item.season)
                        if _anidb_ep is not None and _anidb_ep > 0:
                            _expected_episode_count = _anidb_ep
                except (ValueError, TypeError, ImportError) as exc:
                    logger.debug(
                        "scrape_pipeline: AniDB episode count lookup failed for tmdb_id=%s: %s",
                        item.tmdb_id, exc,
                    )

            if _expected_episode_count is not None:
                logger.debug(
                    "scrape_pipeline: expected_episode_count=%d for item id=%d season=%d",
                    _expected_episode_count, item.id, item.season,
                )

        # Filter and rank first, then probe RD cache on the top candidates.
        ranked = filter_engine.filter_and_rank(
            combined,  # type: ignore[arg-type]
            profile_name=item.quality_profile,
            cached_hashes=cached_hashes,
            prefer_season_packs=bool(item.is_season_pack),
            original_language=orig_lang_name,
            requested_season=scene_season if item.media_type == MediaType.SHOW and not item.is_season_pack else None,
            requested_episode=scene_episode if item.media_type == MediaType.SHOW and not item.is_season_pack else None,
            known_titles=known_titles,
            expected_episode_count=_expected_episode_count,
        )

        # Extract XEM scene pack episode range early — needed for both dedup
        # validation and the cache check loop later.
        xem_anchor_episode: int | None = None
        xem_end_episode: int | None = None
        xem_anchor_season: int | None = None
        if item.is_season_pack and item.metadata_json:
            try:
                _xem_meta = json.loads(item.metadata_json) if isinstance(item.metadata_json, str) else item.metadata_json
                if _xem_meta.get("xem_scene_pack"):
                    _raw_anchor = _xem_meta.get("tmdb_anchor_episode")
                    _raw_end = _xem_meta.get("tmdb_end_episode")
                    _raw_season = _xem_meta.get("tmdb_anchor_season")
                    if _raw_anchor is not None:
                        xem_anchor_episode = int(_raw_anchor)
                    if _raw_end is not None:
                        xem_end_episode = int(_raw_end)
                    if _raw_season is not None:
                        xem_anchor_season = int(_raw_season)
            except (ValueError, TypeError, json.JSONDecodeError):
                pass

        # --- Hash-based dedup: skip cache check if top result already registered ---
        if ranked:
            top_hash = ranked[0].result.info_hash
            if top_hash:
                try:
                    existing_torrent = await dedup_engine.check_local_duplicate(session, top_hash)
                except Exception as exc:
                    logger.warning(
                        "scrape_pipeline: hash dedup check failed for item id=%d: %s",
                        item.id, exc,
                    )
                    existing_torrent = None
                if existing_torrent:
                    # XEM validation: if this is a scene pack, verify the existing
                    # torrent's files contain the target episode range before accepting.
                    dedup_valid = True
                    if xem_anchor_episode is not None and xem_end_episode is not None and existing_torrent.rd_id:
                        try:
                            info = await rd_client.get_torrent_info(existing_torrent.rd_id)
                            rd_files = info.get("files", [])
                            if rd_files:
                                valid, matched = self._validate_xem_episode_range(
                                    rd_files, xem_anchor_episode, xem_end_episode,
                                    anchor_season=xem_anchor_season,
                                )
                                if not valid:
                                    logger.warning(
                                        "scrape_pipeline: hash dedup hit for item id=%d rejected "
                                        "— existing torrent rd_id=%s fails XEM validation "
                                        "(target E%02d-E%02d, no matching episodes)",
                                        item.id, existing_torrent.rd_id,
                                        xem_anchor_episode, xem_end_episode,
                                    )
                                    dedup_valid = False
                        except Exception as exc:
                            logger.warning(
                                "scrape_pipeline: XEM validation on dedup torrent failed "
                                "for item id=%d: %s — skipping dedup",
                                item.id, exc,
                            )
                            dedup_valid = False

                    if dedup_valid:
                        # Loop prevention: skip if this hash previously failed CHECKING
                        if _checking_failed_hash and _checking_failed_hash == top_hash:
                            logger.warning(
                                "scrape_pipeline: hash-based dedup for item id=%d skipped — "
                                "hash %s previously failed CHECKING",
                                item.id, top_hash,
                            )
                            # Remove the failed hash from ranked results and clear flag
                            ranked = [r for r in ranked if r.result.info_hash != top_hash]
                            try:
                                meta = json.loads(item.metadata_json) if item.metadata_json else {}
                            except (ValueError, TypeError):
                                meta = {}
                            meta.pop("checking_failed_hash", None)
                            item.metadata_json = json.dumps(meta) if meta else None
                            await session.flush()
                            if not ranked:
                                # No alternatives — transition to SLEEPING
                                await queue_manager.transition(session, item.id, QueueState.SLEEPING)
                                return PipelineResult(
                                    item_id=item.id,
                                    action="checking_loop_skip",
                                    message=f"No alternatives after skipping failed hash {top_hash}",
                                    selected_hash=top_hash,
                                    scrape_results_count=total_count,
                                    filtered_results_count=0,
                                )
                            # Falls through to normal cache-check flow with the next-best result.
                            # The flag was cleared, so if this result also fails CHECKING,
                            # it will get a fresh CHECKING attempt with its own flag.
                        else:
                            logger.info(
                                "scrape_pipeline: hash-based dedup hit for item id=%d — "
                                "hash=%s already registered (rd_id=%s), skipping to CHECKING",
                                item.id, top_hash, existing_torrent.rd_id,
                            )
                            await self._log_scrape(
                                session,
                                media_item_id=item.id,
                                scraper="pipeline",
                                query_params=json.dumps({
                                    "title": item.title,
                                    "step": "hash_dedup",
                                    "info_hash": top_hash,
                                }),
                                results_count=total_count,
                                results_summary=json.dumps(self._summarise_results(combined[:5])),
                                selected_result=json.dumps({
                                    "info_hash": top_hash,
                                    "rd_id": existing_torrent.rd_id,
                                    "action": "hash_dedup_hit",
                                }),
                                duration_ms=zilean_duration_ms + nyaa_duration_ms + torrentio_duration_ms,
                            )
                            await queue_manager.transition(session, item.id, QueueState.CHECKING)
                            return PipelineResult(
                                item_id=item.id,
                                action="dedup_hit",
                                message=f"Torrent already in RD registry (hash={top_hash}, rd_id={existing_torrent.rd_id})",
                                selected_hash=top_hash,
                                rd_torrent_id=existing_torrent.rd_id,
                                scrape_results_count=total_count,
                                filtered_results_count=len(ranked),
                            )

        filtered_count = len(ranked)
        best: FilteredResult | None = ranked[0] if ranked else None

        if best is None:
            # Season pack split: when results exist but all are single episodes,
            # split into individual episode items instead of sleeping.
            if item.is_season_pack and total_count > 0:
                try:
                    created = await self._split_season_pack_to_episodes(session, item)
                except Exception as exc:
                    logger.warning(
                        "scrape_pipeline: season pack split failed for item id=%d: %s",
                        item.id, exc,
                    )
                    created = 0

                if created > 0:
                    logger.info(
                        "scrape_pipeline: no season packs found for item id=%d title=%r, "
                        "split into %d individual episode items",
                        item.id, item.title, created,
                    )
                    await self._log_scrape(
                        session,
                        media_item_id=item.id,
                        scraper="pipeline",
                        query_params=json.dumps({
                            "title": item.title,
                            "step": "season_pack_split",
                            "total": total_count,
                            "episodes_created": created,
                        }),
                        results_count=total_count,
                        results_summary=json.dumps(self._summarise_results(combined[:5])),
                        selected_result=None,
                        duration_ms=zilean_duration_ms + nyaa_duration_ms + torrentio_duration_ms,
                    )
                    await queue_manager.transition(session, item.id, QueueState.COMPLETE)
                    return PipelineResult(
                        item_id=item.id,
                        action="season_pack_split",
                        message=f"No season packs available, created {created} individual episode items",
                        scrape_results_count=total_count,
                        filtered_results_count=0,
                    )

            # Original logic: no results or split failed → SLEEPING
            logger.info(
                "scrape_pipeline: all %d results rejected by filter engine for "
                "item id=%d, transitioning to SLEEPING",
                total_count,
                item.id,
            )
            await self._log_scrape(
                session,
                media_item_id=item.id,
                scraper="pipeline",
                query_params=json.dumps(
                    {
                        "title": item.title,
                        "step": "filter",
                        "total": total_count,
                    }
                ),
                results_count=total_count,
                results_summary=json.dumps(
                    self._summarise_results(combined[:5])
                ),
                selected_result=None,
                duration_ms=zilean_duration_ms + nyaa_duration_ms + torrentio_duration_ms,
            )
            await queue_manager.transition(session, item.id, QueueState.SLEEPING)
            return PipelineResult(
                item_id=item.id,
                action="no_results",
                message=(
                    f"All {total_count} results were rejected by the filter engine"
                ),
                scrape_results_count=total_count,
                filtered_results_count=0,
            )

        # ------------------------------------------------------------------
        # Step 7 — Sequential RD cache check + Add to Real-Debrid
        # ------------------------------------------------------------------

        cache_limit = settings.search.cache_check_limit
        winner, winner_rd_id, winner_is_cached = await self._sequential_cache_check(
            item_id=item.id,
            ranked=ranked,
            cache_limit=cache_limit,
            xem_anchor_episode=xem_anchor_episode,
            xem_end_episode=xem_end_episode,
            xem_anchor_season=xem_anchor_season,
        )

        # Handle case where all candidates were rejected by XEM file validation.
        if winner is None:
            logger.warning(
                "scrape_pipeline.run: all %d candidates failed XEM episode range validation "
                "for item id=%d title=%r — splitting to individual episodes",
                len(ranked),
                item.id,
                item.title,
            )
            if item.is_season_pack:
                try:
                    created = await self._split_season_pack_to_episodes(session, item)
                except Exception as exc:
                    logger.warning(
                        "scrape_pipeline: XEM-rejected season pack split failed for "
                        "item id=%d: %s",
                        item.id, exc,
                    )
                    created = 0

                if created > 0:
                    await self._log_scrape(
                        session,
                        media_item_id=item.id,
                        scraper="pipeline",
                        query_params=json.dumps({
                            "title": item.title,
                            "step": "xem_validation_rejected_split",
                            "total": total_count,
                            "episodes_created": created,
                        }),
                        results_count=total_count,
                        results_summary=json.dumps(self._summarise_results(combined[:5])),
                        selected_result=None,
                        duration_ms=zilean_duration_ms + nyaa_duration_ms + torrentio_duration_ms,
                    )
                    await queue_manager.transition(session, item.id, QueueState.COMPLETE)
                    return PipelineResult(
                        item_id=item.id,
                        action="season_pack_split",
                        message=(
                            f"All candidates failed XEM episode range validation, "
                            f"created {created} individual episode items"
                        ),
                        scrape_results_count=total_count,
                        filtered_results_count=filtered_count,
                    )

            # Split failed or no episodes created — transition to SLEEPING.
            await queue_manager.transition(session, item.id, QueueState.SLEEPING)
            return PipelineResult(
                item_id=item.id,
                action="no_results",
                message="All candidates failed XEM episode range validation",
                scrape_results_count=total_count,
                filtered_results_count=filtered_count,
            )

        return await self._step_add_to_rd(
            session,
            item,
            winner=winner,
            winner_rd_id=winner_rd_id,
            winner_is_cached=winner_is_cached,
            scrape_results_count=total_count,
            filtered_results_count=filtered_count,
            total_duration_ms=zilean_duration_ms + nyaa_duration_ms + torrentio_duration_ms,
        )

    # ------------------------------------------------------------------
    # Step implementations
    # ------------------------------------------------------------------

    async def _step_mount_check(
        self,
        session: AsyncSession,
        item: MediaItem,
        tmdb_original_title: str | None = None,
    ) -> PipelineResult | None:
        """Check the Zurg mount index for an existing file match.

        Args:
            session: Caller-managed async database session.
            item: The MediaItem to check.
            tmdb_original_title: Optional original title from TMDB (e.g. Japanese
                title for anime). Used to build the alt-title list passed to
                ``lookup_multi`` so non-English titles are matched correctly.

        Returns:
            A PipelineResult on a mount hit, or None to continue the pipeline.
        """
        from src.core.mount_scanner import gather_alt_titles

        t0 = time.monotonic()
        try:
            mount_available = await mount_scanner.is_mount_available()
        except Exception as exc:
            logger.warning(
                "scrape_pipeline: mount availability check failed for item id=%d, "
                "skipping mount check: %s",
                item.id,
                exc,
            )
            return None

        if not mount_available:
            logger.warning(
                "scrape_pipeline: mount unavailable, skipping mount check for "
                "item id=%d",
                item.id,
            )
            return None

        try:
            titles = await gather_alt_titles(session, item, tmdb_original_title)
            matches = await mount_scanner.lookup_multi(
                session, titles, item.season, item.episode
            )
        except Exception as exc:
            logger.warning(
                "scrape_pipeline: mount lookup failed for item id=%d: %s",
                item.id,
                exc,
            )
            return None

        # Year-mismatch post-filter: discard entries whose parsed_year diverges
        # from the item's year by more than ±1 (keeps None-year entries).
        original_matches = matches
        pre_filter_count = len(matches)
        matches = filter_year_mismatches(matches, item.year)
        if pre_filter_count > 0 and not matches:
            logger.info(
                "scrape_pipeline: mount matches for item id=%d title=%r filtered out "
                "by year mismatch (item_year=%s, match_years=%s)",
                item.id,
                item.title,
                item.year,
                list({m.parsed_year for m in original_matches if m.parsed_year}),
            )

        duration_ms = int((time.monotonic() - t0) * 1000)

        query_params = json.dumps(
            {
                "title": item.title,
                "season": item.season,
                "episode": item.episode,
            }
        )

        if not matches:
            await self._log_scrape(
                session,
                media_item_id=item.id,
                scraper="mount_scan",
                query_params=query_params,
                results_count=0,
                results_summary=json.dumps([]),
                selected_result=None,
                duration_ms=duration_ms,
            )
            return None

        # Verify the matched file actually exists on disk — mount_index may
        # contain stale entries from a previous scan (e.g. torrent removed from
        # RD, Zurg no longer serves it).
        best_match = matches[0]
        file_exists = await asyncio.to_thread(os.path.exists, best_match.filepath)
        if not file_exists:
            logger.warning(
                "scrape_pipeline: mount index hit for item id=%d title=%r but "
                "file not found on disk, removing stale index entry: %s",
                item.id,
                item.title,
                best_match.filepath,
            )
            await session.execute(
                delete(MountIndex).where(
                    MountIndex.filepath == best_match.filepath
                )
            )
            await session.flush()
            await self._log_scrape(
                session,
                media_item_id=item.id,
                scraper="mount_scan",
                query_params=query_params,
                results_count=0,
                results_summary=json.dumps([]),
                selected_result=None,
                duration_ms=duration_ms,
            )
            return None

        # Mount hit — log and return; caller transitions state
        logger.info(
            "scrape_pipeline: mount hit for item id=%d title=%r -> %s",
            item.id,
            item.title,
            best_match.filepath,
        )
        results_summary = json.dumps(
            [
                {
                    "filepath": m.filepath,
                    "filename": m.filename,
                    "resolution": m.parsed_resolution,
                }
                for m in matches[:5]
            ]
        )
        selected_result = json.dumps(
            {
                "filepath": best_match.filepath,
                "filename": best_match.filename,
                "resolution": best_match.parsed_resolution,
            }
        )
        await self._log_scrape(
            session,
            media_item_id=item.id,
            scraper="mount_scan",
            query_params=query_params,
            results_count=len(matches),
            results_summary=results_summary,
            selected_result=selected_result,
            duration_ms=duration_ms,
        )
        await queue_manager.transition(session, item.id, QueueState.CHECKING)
        return PipelineResult(
            item_id=item.id,
            action="mount_hit",
            message=(
                f"File already present in mount: {best_match.filepath}"
            ),
            mount_path=best_match.filepath,
            scrape_results_count=len(matches),
            filtered_results_count=len(matches),
        )

    async def _step_dedup_check(
        self, session: AsyncSession, item: MediaItem
    ) -> PipelineResult | None:
        """Check the local RD registry for an already-tracked content match.

        Args:
            session: Caller-managed async database session.
            item: The MediaItem to check.

        Returns:
            A PipelineResult on a dedup hit, or None to continue the pipeline.
        """
        t0 = time.monotonic()
        query_params = json.dumps(
            {
                "imdb_id": item.imdb_id,
                "season": item.season,
                "episode": item.episode,
            }
        )

        try:
            # Pass None for resolution to match any — register_torrent stores
            # the actual result resolution, so a resolution-specific check here
            # would miss valid duplicates registered at a different resolution.
            existing = await dedup_engine.check_content_duplicate(
                session,
                item.imdb_id,
                item.season,
                item.episode,
                None,
            )
        except Exception as exc:
            logger.warning(
                "scrape_pipeline: dedup check failed for item id=%d: %s",
                item.id,
                exc,
            )
            return None

        duration_ms = int((time.monotonic() - t0) * 1000)

        if existing is None:
            await self._log_scrape(
                session,
                media_item_id=item.id,
                scraper="rd_check",
                query_params=query_params,
                results_count=0,
                results_summary=json.dumps([]),
                selected_result=None,
                duration_ms=duration_ms,
            )
            return None

        logger.info(
            "scrape_pipeline: dedup hit for item id=%d — existing rd_torrent.id=%d "
            "info_hash=%s",
            item.id,
            existing.id,
            existing.info_hash,
        )

        # Loop prevention: skip if this hash previously failed CHECKING
        if existing.info_hash:
            try:
                meta = json.loads(item.metadata_json) if item.metadata_json else {}
            except (ValueError, TypeError):
                meta = {}
            failed_hash = meta.get("checking_failed_hash")
            if failed_hash and failed_hash == existing.info_hash:
                logger.warning(
                    "scrape_pipeline: dedup hit for item id=%d skipped — hash %s "
                    "previously failed CHECKING, transitioning to SLEEPING",
                    item.id, existing.info_hash,
                )
                # Clear flag so next retry can attempt CHECKING
                meta.pop("checking_failed_hash", None)
                item.metadata_json = json.dumps(meta) if meta else None
                await session.flush()
                await queue_manager.transition(session, item.id, QueueState.SLEEPING)
                return PipelineResult(
                    item_id=item.id,
                    action="checking_loop_skip",
                    message=f"Skipping CHECKING for hash {existing.info_hash} — previously failed",
                    selected_hash=existing.info_hash,
                    scrape_results_count=1,
                    filtered_results_count=1,
                )

        selected = json.dumps(
            {
                "rd_id": existing.rd_id,
                "info_hash": existing.info_hash,
                "resolution": existing.resolution,
                "filename": existing.filename,
            }
        )
        await self._log_scrape(
            session,
            media_item_id=item.id,
            scraper="rd_check",
            query_params=query_params,
            results_count=1,
            results_summary=json.dumps([]),
            selected_result=selected,
            duration_ms=duration_ms,
        )
        await queue_manager.transition(session, item.id, QueueState.CHECKING)
        return PipelineResult(
            item_id=item.id,
            action="dedup_hit",
            message=(
                f"Content already tracked in RD registry "
                f"(rd_id={existing.rd_id}, hash={existing.info_hash})"
            ),
            selected_hash=existing.info_hash,
            scrape_results_count=1,
            filtered_results_count=1,
        )

    async def _step_zilean(
        self,
        session: AsyncSession,
        item: MediaItem,
        *,
        scene_season: int | None = None,
        scene_episode: int | None = None,
        alt_titles: list[str] | None = None,
    ) -> tuple[list[ZileanResult], int]:
        """Query Zilean with primary title and all alt titles concurrently.

        All titles (primary + up to 3 alternatives) are queried simultaneously
        via ``asyncio.gather``.  Results from all queries are merged and
        deduplicated by ``info_hash`` (first occurrence wins, preserving primary
        title precedence).

        Errors from individual queries are caught and logged; the merged result
        from all successful queries is returned.  An empty list is returned when
        all queries fail or yield nothing.

        Args:
            session: Caller-managed async database session.
            item: The MediaItem to search for.
            scene_season: Pre-resolved scene season from XEM (or item.season if
                not provided).
            scene_episode: Pre-resolved scene episode from XEM (or item.episode
                if not provided).
            alt_titles: Pre-computed alternative titles from ``collect_alt_titles``
                (already filtered to Latin-script, deduped against primary).

        Returns:
            2-tuple of (results, duration_ms).
        """
        t0 = time.monotonic()

        # Fall back to item's original numbering when not pre-resolved.
        if scene_season is None:
            scene_season = item.season
        if scene_episode is None:
            scene_episode = item.episode

        titles_to_query: list[str] = [item.title] + (alt_titles or [])

        async def _query_one(query: str) -> list[ZileanResult]:
            try:
                return await zilean_client.search(
                    query=query,
                    season=scene_season,
                    episode=scene_episode,
                    year=item.year,
                    imdb_id=item.imdb_id,
                )
            except Exception as exc:
                logger.warning(
                    "scrape_pipeline: Zilean query failed for item id=%d query=%r: %s",
                    item.id,
                    query,
                    exc,
                )
                return []

        raw_results = await asyncio.gather(
            *[_query_one(t) for t in titles_to_query]
        )

        # Merge and dedup by info_hash — primary title results come first.
        seen_hashes: set[str] = set()
        results: list[ZileanResult] = []
        per_title_counts: dict[str, int] = {}
        for title, res in zip(titles_to_query, raw_results):
            new_count = 0
            for r in res:
                h = r.info_hash
                if h and h not in seen_hashes:
                    seen_hashes.add(h)
                    results.append(r)
                    new_count += 1
            per_title_counts[title] = new_count

        logger.debug(
            "scrape_pipeline: Zilean merged %d unique results for item id=%d "
            "(queried %d titles: %s)",
            len(results),
            item.id,
            len(titles_to_query),
            per_title_counts,
        )

        query_params = json.dumps(
            {
                "query": item.title,
                "alt_titles": alt_titles or [],
                "per_title_counts": per_title_counts,
                "season": item.season,
                "episode": item.episode,
                "scene_season": scene_season,
                "scene_episode": scene_episode,
                "year": item.year,
                "imdb_id": item.imdb_id,
            }
        )

        duration_ms = int((time.monotonic() - t0) * 1000)

        results_summary = json.dumps(self._summarise_results(results[:5]))  # type: ignore[arg-type]
        await self._log_scrape(
            session,
            media_item_id=item.id,
            scraper="zilean",
            query_params=query_params,
            results_count=len(results),
            results_summary=results_summary,
            selected_result=None,
            duration_ms=duration_ms,
        )
        return results, duration_ms

    async def _step_nyaa(
        self,
        session: AsyncSession,
        item: MediaItem,
        *,
        scene_season: int | None = None,
        scene_episode: int | None = None,
        alt_titles: list[str] | None = None,
    ) -> tuple[list[NyaaResult], int]:
        """Query Nyaa.si RSS for anime torrent results.

        Only runs when ``settings.scrapers.nyaa.enabled`` is ``True`` and the
        item is a TV show (``MediaType.SHOW``).  Movies are not indexed on Nyaa
        in a way that benefits the pipeline, so they are skipped.

        For episode items a multi-level query fallback is tried:
          1. ``"{title} S{season:02d}E{episode:02d}"`` — standard SxxExx notation
          2. ``"{title} {episode:02d}"`` — bare anime episode number
          3. ``"{title}"`` — broad title-only search

        For season packs:
          1. ``"{title} S{season:02d} batch"`` — explicit batch search
          2. ``"{title} S{season:02d}"`` — season-only search

        When the primary title returns no results at all levels and ``alt_titles``
        has entries, the first alternative title is retried through the same
        fallback chain.

        Errors are caught and logged; an empty list is returned so the pipeline
        continues to Torrentio.

        Args:
            session: Caller-managed async database session.
            item: The MediaItem to search for.
            scene_season: Pre-resolved scene season from XEM (or item.season if
                not provided).
            scene_episode: Pre-resolved scene episode from XEM (or item.episode
                if not provided).
            alt_titles: Pre-computed alternative titles to try on zero results
                (e.g. original_title from the TMDB detail call in Step 0).

        Returns:
            2-tuple of (results, duration_ms).
        """
        if not settings.scrapers.nyaa.enabled:
            return [], 0

        if item.media_type != MediaType.SHOW:
            return [], 0

        t0 = time.monotonic()

        # Fall back to item's original numbering when not pre-resolved.
        eff_season = scene_season if scene_season is not None else item.season
        eff_episode = scene_episode if scene_episode is not None else item.episode

        title = item.title
        results: list[NyaaResult] = []

        async def _try_queries(search_title: str) -> list[NyaaResult]:
            """Run the fallback query chain for a given title.

            Returns the first non-empty result list encountered, or an empty
            list if all fallback levels return nothing.
            """
            if item.is_season_pack:
                # Season pack query chain
                if eff_season is not None:
                    queries = [
                        f"{search_title} S{eff_season:02d} batch",
                        f"{search_title} S{eff_season:02d}",
                    ]
                else:
                    queries = [search_title]
            elif eff_season is not None and eff_episode is not None:
                # Episode query chain
                queries = [
                    f"{search_title} S{eff_season:02d}E{eff_episode:02d}",
                    f"{search_title} {eff_episode:02d}",
                    search_title,
                ]
            else:
                queries = [search_title]

            for q in queries:
                try:
                    r = await nyaa_client.search(q)
                except Exception as exc:
                    logger.warning(
                        "scrape_pipeline: Nyaa search failed for item id=%d query=%r: %s",
                        item.id,
                        q,
                        exc,
                    )
                    r = []
                if r:
                    logger.debug(
                        "scrape_pipeline: Nyaa returned %d results for item id=%d query=%r",
                        len(r),
                        item.id,
                        q,
                    )
                    return r
            return []

        # Run primary + up to 2 alt title query chains concurrently.
        titles_to_query: list[str] = [title] + (alt_titles or [])[:2]

        raw_results = await asyncio.gather(
            *[_try_queries(t) for t in titles_to_query]
        )

        # Merge and dedup by info_hash — primary title results come first.
        seen_hashes: set[str] = set()
        results: list[NyaaResult] = []
        per_title_counts: dict[str, int] = {}
        for t, res in zip(titles_to_query, raw_results):
            new_count = 0
            for r in res:
                h = r.info_hash
                if h and h not in seen_hashes:
                    seen_hashes.add(h)
                    results.append(r)
                    new_count += 1
            per_title_counts[t] = new_count

        logger.debug(
            "scrape_pipeline: Nyaa merged %d unique results for item id=%d "
            "(queried %d titles: %s)",
            len(results),
            item.id,
            len(titles_to_query),
            per_title_counts,
        )

        duration_ms = int((time.monotonic() - t0) * 1000)

        query_params = json.dumps(
            {
                "query": title,
                "alt_titles": (alt_titles or [])[:2],
                "per_title_counts": per_title_counts,
                "season": item.season,
                "episode": item.episode,
                "scene_season": eff_season,
                "scene_episode": eff_episode,
                "is_season_pack": item.is_season_pack,
            }
        )

        results_summary = json.dumps(self._summarise_results(results[:5]))  # type: ignore[arg-type]
        await self._log_scrape(
            session,
            media_item_id=item.id,
            scraper="nyaa",
            query_params=query_params,
            results_count=len(results),
            results_summary=results_summary,
            selected_result=None,
            duration_ms=duration_ms,
        )
        return results, duration_ms

    async def _step_torrentio(
        self,
        session: AsyncSession,
        item: MediaItem,
        *,
        scene_season: int | None = None,
        scene_episode: int | None = None,
    ) -> tuple[list[TorrentioResult], int]:
        """Query Torrentio for results using the media type to pick the correct call.

        Errors are caught and logged; an empty list is returned so the pipeline
        can proceed with whatever Zilean returned (or to no_results).

        Args:
            session: Caller-managed async database session.
            item: The MediaItem to scrape.
            scene_season: Pre-resolved scene season from XEM (or item.season if
                not provided).
            scene_episode: Pre-resolved scene episode from XEM (or item.episode
                if not provided).

        Returns:
            2-tuple of (results, duration_ms).
        """
        t0 = time.monotonic()
        results: list[TorrentioResult] = []

        if not item.imdb_id:
            logger.debug(
                "scrape_pipeline: skipping Torrentio for item id=%d — no imdb_id",
                item.id,
            )
            return results, 0

        if item.media_type == MediaType.MOVIE:
            query_params = json.dumps(
                {"imdb_id": item.imdb_id, "type": "movie"}
            )
            try:
                results = await torrentio_client.scrape_movie(item.imdb_id)
                logger.debug(
                    "scrape_pipeline: Torrentio returned %d results for movie "
                    "item id=%d",
                    len(results),
                    item.id,
                )
            except Exception as exc:
                logger.warning(
                    "scrape_pipeline: Torrentio scrape_movie failed for item id=%d: %s",
                    item.id,
                    exc,
                )
        else:
            # SHOW — requires at least a season number
            if item.season is None:
                logger.warning(
                    "scrape_pipeline: item id=%d is a SHOW but season=%s episode=%s "
                    "— cannot scrape without both values",
                    item.id,
                    item.season,
                    item.episode,
                )
                return results, 0
            season = item.season
            episode = item.episode if item.episode is not None else 1

            # Use pre-resolved scene numbers; fall back to original when not provided.
            eff_scene_season = scene_season if scene_season is not None else season
            eff_scene_episode = scene_episode if scene_episode is not None else episode

            query_params = json.dumps(
                {
                    "imdb_id": item.imdb_id,
                    "type": "show",
                    "season": season,
                    "episode": episode,
                    "scene_season": eff_scene_season,
                    "scene_episode": eff_scene_episode,
                }
            )
            try:
                results = await torrentio_client.scrape_episode(
                    item.imdb_id, eff_scene_season, eff_scene_episode,
                )
                logger.debug(
                    "scrape_pipeline: Torrentio returned %d results for episode "
                    "item id=%d S%02dE%02d",
                    len(results),
                    item.id,
                    season,
                    episode,
                )
            except Exception as exc:
                logger.warning(
                    "scrape_pipeline: Torrentio scrape_episode failed for item id=%d: %s",
                    item.id,
                    exc,
                )

        duration_ms = int((time.monotonic() - t0) * 1000)

        results_summary = json.dumps(self._summarise_results(results[:5]))  # type: ignore[arg-type]
        await self._log_scrape(
            session,
            media_item_id=item.id,
            scraper="torrentio",
            query_params=query_params,
            results_count=len(results),
            results_summary=results_summary,
            selected_result=None,
            duration_ms=duration_ms,
        )
        return results, duration_ms

    async def _sequential_cache_check(
        self,
        *,
        item_id: int,
        ranked: list[FilteredResult],
        cache_limit: int,
        xem_anchor_episode: int | None = None,
        xem_end_episode: int | None = None,
        xem_anchor_season: int | None = None,
    ) -> tuple[FilteredResult | None, str | None, bool]:
        """Check ranked results one at a time for RD cache status, stopping at the first hit.

        Iterates through up to ``cache_limit`` of the top-ranked filtered results.
        For each candidate:
        - Adds the magnet to RD to obtain an ``rd_id``.
        - Checks the torrent status: if it is cached (``downloaded`` or
          ``waiting_files_selection``), keeps it in RD and returns immediately.
        - If not cached, deletes it from RD and moves to the next candidate.

        When ``xem_anchor_episode`` and ``xem_end_episode`` are both provided,
        cached torrents are additionally validated against the XEM episode range.
        A cached torrent whose files contain no episodes in
        ``[xem_anchor_episode, xem_end_episode]`` is deleted from RD and the
        loop continues to the next candidate.  If *all* candidates fail XEM
        validation, ``(None, None, False)`` is returned to signal the caller
        that a season-pack-to-episodes split should be attempted.

        If no cached result is found after exhausting the limit, the last
        checked torrent is left in RD so it will begin downloading — this
        matches the original behavior where the best uncached result is used.

        On a RD API error that is not a 451 (blocked) response, the loop stops
        early and returns whatever was last successfully added to RD (or the
        top-ranked result with no ``rd_id`` if nothing was added).  On a 451,
        the candidate is skipped and the loop continues.

        Args:
            item_id: The MediaItem primary key (used only for log messages).
            ranked: Filtered results ordered best-first by metadata score.
            cache_limit: Maximum number of candidates to probe.
            xem_anchor_episode: First TMDB episode in the XEM range (inclusive),
                or ``None`` to skip file-level validation entirely.
            xem_end_episode: Last TMDB episode in the XEM range (inclusive), or
                ``None`` to skip file-level validation entirely.

        Returns:
            A 3-tuple of (winner, winner_rd_id, winner_is_cached) where:
            - ``winner``: The chosen FilteredResult, or ``None`` when all
              candidates were rejected by XEM file validation.
            - ``winner_rd_id``: The RD torrent ID if the winner is already in
              RD (either cached or the last-checked uncached one), or ``None``
              if the loop never succeeded or all candidates were rejected.
            - ``winner_is_cached``: ``True`` if the winner is confirmed cached.
        """
        xem_validation_active = (
            xem_anchor_episode is not None and xem_end_episode is not None
        )
        total_attempts = min(len(ranked), cache_limit)

        candidates = ranked[:cache_limit]
        if not candidates:
            return ranked[0], None, False

        last_added: FilteredResult | None = None
        last_rd_id: str | None = None
        xem_rejected_count = 0

        for attempt_idx, candidate in enumerate(candidates):
            info_hash: str = candidate.result.info_hash
            magnet_uri = f"magnet:?xt=urn:btih:{info_hash}"

            # --- Add magnet ---
            try:
                add_resp = await rd_client.add_magnet(magnet_uri)
            except RealDebridError as exc:
                if exc.status_code == 451:
                    logger.warning(
                        "scrape_pipeline: cache check attempt %d/%d: hash=%s "
                        "blocked (451) for item id=%d — skipping",
                        attempt_idx + 1, len(candidates), info_hash, item_id,
                    )
                    continue
                logger.warning(
                    "scrape_pipeline: cache check attempt %d/%d: add_magnet "
                    "failed for item id=%d hash=%s: %s — stopping early",
                    attempt_idx + 1, len(candidates), item_id, info_hash, exc,
                )
                # Return whatever we have so far, or fall back to top candidate.
                if last_added is not None:
                    return last_added, last_rd_id, False
                return ranked[0], None, False
            except (httpx.RequestError, TimeoutError) as exc:
                logger.warning(
                    "scrape_pipeline: cache check attempt %d/%d: network error "
                    "for item id=%d hash=%s: %s — stopping early",
                    attempt_idx + 1, len(candidates), item_id, info_hash, exc,
                )
                if last_added is not None:
                    return last_added, last_rd_id, False
                return ranked[0], None, False
            except Exception as exc:
                logger.warning(
                    "scrape_pipeline: cache check attempt %d/%d: unexpected error "
                    "for item id=%d hash=%s: %s — stopping early",
                    attempt_idx + 1, len(candidates), item_id, info_hash, exc,
                    exc_info=True,
                )
                if last_added is not None:
                    return last_added, last_rd_id, False
                return ranked[0], None, False

            rd_id: str = str(add_resp.get("id", ""))
            if not rd_id:
                logger.warning(
                    "scrape_pipeline: cache check attempt %d/%d: add_magnet "
                    "returned empty id for item id=%d hash=%s — skipping",
                    attempt_idx + 1, len(candidates), item_id, info_hash,
                )
                continue

            # --- Check torrent status ---
            cached = False
            info: dict[str, Any] = {}
            try:
                info = await rd_client.get_torrent_info(rd_id)
                status = info.get("status", "")
                cached = status in {"downloaded", "waiting_files_selection"}
            except Exception as exc:
                logger.warning(
                    "scrape_pipeline: cache check attempt %d/%d: get_torrent_info "
                    "failed for item id=%d hash=%s rd_id=%s: %s — keeping as fallback",
                    attempt_idx + 1, len(candidates), item_id, info_hash, rd_id, exc,
                )
                # Unknown cache status — keep in RD as fallback, don't delete.
                return candidate, rd_id, False

            logger.info(
                "scrape_pipeline: cache check attempt %d/%d: hash=%s cached=%s "
                "for item id=%d",
                attempt_idx + 1, len(candidates), info_hash, cached, item_id,
            )

            if cached:
                # XEM scene pack validation: check file list matches target episode range.
                if xem_validation_active:
                    rd_files = info.get("files", [])
                    if rd_files:
                        valid, matched_eps = self._validate_xem_episode_range(
                            rd_files,
                            xem_anchor_episode,  # type: ignore[arg-type]
                            xem_end_episode,  # type: ignore[arg-type]
                            anchor_season=xem_anchor_season,
                        )
                        if not valid:
                            logger.warning(
                                "scrape_pipeline: cache check attempt %d/%d: hash=%s cached but "
                                "fails XEM validation (target E%02d-E%02d, no matching episodes) "
                                "for item id=%d — deleting and trying next",
                                attempt_idx + 1, total_attempts, info_hash,
                                xem_anchor_episode, xem_end_episode, item_id,
                            )
                            try:
                                await rd_client.delete_torrent(rd_id)
                            except RealDebridRateLimitError:
                                logger.warning(
                                    "scrape_pipeline: 429 during XEM reject delete for rd_id=%s, "
                                    "sleeping 2s",
                                    rd_id,
                                )
                                await asyncio.sleep(2)
                            except Exception as del_exc:
                                logger.warning(
                                    "scrape_pipeline: XEM reject delete failed for rd_id=%s: %s",
                                    rd_id, del_exc,
                                )
                            xem_rejected_count += 1
                            continue  # try next candidate
                        # Files are present and pass validation — fall through to return.
                        logger.debug(
                            "scrape_pipeline: cache check attempt %d/%d: hash=%s passed XEM "
                            "validation (target E%02d-E%02d, matched=%s) for item id=%d",
                            attempt_idx + 1, total_attempts, info_hash,
                            xem_anchor_episode, xem_end_episode, matched_eps, item_id,
                        )
                    # If files list is empty (still in magnet_conversion), skip
                    # validation — cannot check yet, accept the torrent as-is.

                # Found a cached torrent that passes all checks — keep it in RD.
                return candidate, rd_id, True

            is_last = attempt_idx == len(candidates) - 1
            if is_last:
                # Exhausted the limit without finding a cached result.
                # For XEM-validated items, run a final file check if files are available.
                if xem_validation_active and info.get("files"):
                    valid, matched_eps = self._validate_xem_episode_range(
                        info["files"],
                        xem_anchor_episode,  # type: ignore[arg-type]
                        xem_end_episode,  # type: ignore[arg-type]
                        anchor_season=xem_anchor_season,
                    )
                    if not valid:
                        logger.warning(
                            "scrape_pipeline: cache check: last candidate hash=%s (uncached) "
                            "fails XEM validation (target E%02d-E%02d) for item id=%d — "
                            "deleting",
                            info_hash, xem_anchor_episode, xem_end_episode, item_id,
                        )
                        try:
                            await rd_client.delete_torrent(rd_id)
                        except RealDebridRateLimitError:
                            logger.warning(
                                "scrape_pipeline: 429 during XEM reject delete (last) for "
                                "rd_id=%s, sleeping 2s",
                                rd_id,
                            )
                            await asyncio.sleep(2)
                        except Exception as del_exc:
                            logger.warning(
                                "scrape_pipeline: XEM reject delete (last) failed for "
                                "rd_id=%s: %s",
                                rd_id, del_exc,
                            )
                        xem_rejected_count += 1
                        # Fall through — loop ends and we handle xem_rejected_count below.
                        break

                # Keep this torrent in RD so it starts downloading.
                logger.info(
                    "scrape_pipeline: cache check exhausted %d/%d candidates "
                    "without a cache hit for item id=%d — using last result "
                    "(hash=%s rd_id=%s, will download)",
                    len(candidates), cache_limit, item_id, info_hash, rd_id,
                )
                return candidate, rd_id, False

            # Not cached and not last — delete from RD, record as last-seen fallback,
            # then try the next candidate.
            deleted = False
            try:
                await rd_client.delete_torrent(rd_id)
                deleted = True
            except Exception as exc:
                logger.warning(
                    "scrape_pipeline: cache check: failed to delete uncached "
                    "torrent rd_id=%s hash=%s: %s — torrent still in RD",
                    rd_id, info_hash, exc,
                )
            # Record as fallback. If delete failed, keep the rd_id since the
            # torrent is still in RD (avoids orphan + redundant re-add).
            last_added = candidate
            last_rd_id = None if deleted else rd_id

        # All candidates were either 451-blocked or rejected by XEM validation.
        # When XEM validation rejected any candidates, prefer the split fallback
        # over an unvalidated uncached torrent — uncached non-last candidates were
        # never XEM-checked, so their content is unknown and likely wrong.
        if xem_validation_active and xem_rejected_count > 0:
            logger.info(
                "scrape_pipeline: cache check: %d candidates failed XEM "
                "validation for item id=%d — signalling split fallback",
                xem_rejected_count, item_id,
            )
            # Clean up any uncached fallback still in RD.
            if last_added is not None and last_rd_id is not None:
                try:
                    await rd_client.delete_torrent(last_rd_id)
                except Exception as del_exc:
                    logger.debug(
                        "scrape_pipeline: cleanup of uncached fallback rd_id=%s "
                        "failed: %s", last_rd_id, del_exc,
                    )
            return None, None, False

        # Fall back to the top-ranked result with no rd_id (all 451-blocked or
        # last_added holds the best uncached fallback we recorded).
        if last_added is not None:
            return last_added, last_rd_id, False
        logger.info(
            "scrape_pipeline: cache check: all %d candidates were skipped "
            "for item id=%d — no rd_id available",
            len(candidates), item_id,
        )
        return ranked[0], None, False

    async def _step_add_to_rd(
        self,
        session: AsyncSession,
        item: MediaItem,
        *,
        winner: FilteredResult,
        winner_rd_id: str | None,
        winner_is_cached: bool,
        scrape_results_count: int,
        filtered_results_count: int,
        total_duration_ms: int,
    ) -> PipelineResult:
        """Add the sequential-cache-check winner to Real-Debrid and register it locally.

        The ``winner`` was already selected by ``_sequential_cache_check``.  If
        ``winner_rd_id`` is provided, the torrent is already in RD (either
        cached or an uncached torrent that was kept to start downloading) and
        ``add_magnet`` is skipped.  Otherwise ``add_magnet`` is called now.

        When add_magnet returns HTTP 451 (infringing_file), the candidate is
        permanently blocked and the pipeline falls back to SLEEPING.

        On add_magnet failure (non-451): transitions to SLEEPING, returns error result.
        On select_files failure: logs warning but does NOT change state (the
        torrent is already in RD and will be processed by the CHECKING step).

        Args:
            session: Caller-managed async database session.
            item: The MediaItem being acquired.
            winner: The chosen FilteredResult from _sequential_cache_check.
            winner_rd_id: The RD torrent ID if already in RD, or None.
            winner_is_cached: True if the winner is confirmed cached in RD.
            scrape_results_count: Total raw results count (for PipelineResult).
            filtered_results_count: Filtered results count (for PipelineResult).
            total_duration_ms: Cumulative scraper duration (for scrape_log).

        Returns:
            A PipelineResult describing the outcome.
        """
        info_hash: str = winner.result.info_hash
        rd_id: str = winner_rd_id or ""

        if rd_id:
            logger.info(
                "scrape_pipeline: reusing rd_id=%s from cache check for hash=%s "
                "(cached=%s, item id=%d)",
                rd_id, info_hash, winner_is_cached, item.id,
            )
        else:
            # The sequential check did not produce an rd_id (all candidates were
            # 451-blocked or the limit was not reached).  Add the winner now.
            candidate_magnet = f"magnet:?xt=urn:btih:{info_hash}"
            logger.info(
                "scrape_pipeline: adding to RD — item id=%d title=%r hash=%s "
                "score=%.1f",
                item.id, item.title, info_hash, winner.score,
            )
            try:
                add_response = await rd_client.add_magnet(candidate_magnet)
            except RealDebridRateLimitError as exc:
                err_msg = f"RD rate limited during add_magnet: {exc}"
                logger.warning(
                    "scrape_pipeline: %s for item id=%d — transitioning to SLEEPING (5 min)",
                    err_msg, item.id,
                )
                transitioned = await queue_manager.transition(
                    session, item.id, QueueState.SLEEPING
                )
                transitioned.next_retry_at = (
                    datetime.now(UTC) + timedelta(minutes=5)
                )
                await session.flush()
                return PipelineResult(
                    item_id=item.id,
                    action="rate_limited",
                    message=err_msg,
                    selected_hash=info_hash,
                    scrape_results_count=scrape_results_count,
                    filtered_results_count=filtered_results_count,
                )
            except RealDebridError as exc:
                if exc.status_code == 451:
                    err_msg = f"Torrent blocked by RD (HTTP 451 infringing_file): hash={info_hash}"
                else:
                    err_msg = f"RD add_magnet failed: {exc}"
                logger.error(
                    "scrape_pipeline: %s for item id=%d",
                    err_msg, item.id,
                )
                await self._log_scrape(
                    session,
                    media_item_id=item.id,
                    scraper="pipeline",
                    query_params=json.dumps(
                        {"magnet_uri": candidate_magnet, "step": "add_magnet"}
                    ),
                    results_count=scrape_results_count,
                    results_summary=None,
                    selected_result=json.dumps({"info_hash": info_hash, "error": err_msg}),
                    duration_ms=total_duration_ms,
                )
                await queue_manager.transition(session, item.id, QueueState.SLEEPING)
                return PipelineResult(
                    item_id=item.id,
                    action="error",
                    message=err_msg,
                    selected_hash=info_hash,
                    scrape_results_count=scrape_results_count,
                    filtered_results_count=filtered_results_count,
                )
            except Exception as exc:
                err_msg = f"RD add_magnet failed: {exc}"
                logger.error(
                    "scrape_pipeline: unexpected error in add_magnet for item id=%d "
                    "hash=%s: %s",
                    item.id, info_hash, exc, exc_info=True,
                )
                await self._log_scrape(
                    session,
                    media_item_id=item.id,
                    scraper="pipeline",
                    query_params=json.dumps(
                        {"magnet_uri": candidate_magnet, "step": "add_magnet"}
                    ),
                    results_count=scrape_results_count,
                    results_summary=None,
                    selected_result=json.dumps({"info_hash": info_hash, "error": err_msg}),
                    duration_ms=total_duration_ms,
                )
                await queue_manager.transition(session, item.id, QueueState.SLEEPING)
                return PipelineResult(
                    item_id=item.id,
                    action="error",
                    message=err_msg,
                    selected_hash=info_hash,
                    scrape_results_count=scrape_results_count,
                    filtered_results_count=filtered_results_count,
                )

            rd_id = str(add_response.get("id", ""))
            if not rd_id:
                err_msg = "add_magnet returned empty torrent ID"
                logger.error(
                    "scrape_pipeline: %s for item id=%d hash=%s",
                    err_msg, item.id, info_hash,
                )
                await self._log_scrape(
                    session,
                    media_item_id=item.id,
                    scraper="pipeline",
                    query_params=json.dumps(
                        {"magnet_uri": candidate_magnet, "step": "add_magnet"}
                    ),
                    results_count=scrape_results_count,
                    results_summary=None,
                    selected_result=json.dumps({"info_hash": info_hash, "error": err_msg}),
                    duration_ms=total_duration_ms,
                )
                await queue_manager.transition(session, item.id, QueueState.SLEEPING)
                return PipelineResult(
                    item_id=item.id,
                    action="error",
                    message=err_msg,
                    selected_hash=info_hash,
                    scrape_results_count=scrape_results_count,
                    filtered_results_count=filtered_results_count,
                )

        # --- select_files ---
        try:
            await rd_client.select_files(rd_id, "all")
        except RealDebridError as exc:
            logger.warning(
                "scrape_pipeline: select_files failed for item id=%d rd_id=%s "
                "(torrent is already in RD, continuing): %s",
                item.id,
                rd_id,
                exc,
            )
        except Exception as exc:
            logger.warning(
                "scrape_pipeline: unexpected error in select_files for item id=%d "
                "rd_id=%s (continuing): %s",
                item.id,
                rd_id,
                exc,
            )

        magnet_uri = f"magnet:?xt=urn:btih:{info_hash}"

        # --- Register in local dedup registry ---
        try:
            async with session.begin_nested():
                await dedup_engine.register_torrent(
                    session,
                    rd_id=rd_id,
                    info_hash=info_hash,
                    magnet_uri=magnet_uri,
                    media_item_id=item.id,
                    filename=winner.result.title,
                    filesize=winner.result.size_bytes,
                    resolution=winner.result.resolution,
                    cached=winner_is_cached,
                )
        except Exception as exc:
            logger.error(
                "scrape_pipeline: register_torrent failed for item id=%d hash=%s: %s",
                item.id,
                info_hash,
                exc,
            )
            # Savepoint rolled back automatically; outer transaction and all
            # previously-flushed ScrapeLog entries are preserved.

        # --- Transition to ADDING ---
        await queue_manager.transition(session, item.id, QueueState.ADDING)

        # --- Log the pipeline result ---
        selected_result = json.dumps(
            {
                "info_hash": info_hash,
                "rd_id": rd_id,
                "title": winner.result.title,
                "score": winner.score,
                "score_breakdown": winner.score_breakdown,
                "resolution": winner.result.resolution,
                "cached": winner_is_cached,
                "size_bytes": winner.result.size_bytes,
            }
        )
        await self._log_scrape(
            session,
            media_item_id=item.id,
            scraper="pipeline",
            query_params=json.dumps(
                {
                    "title": item.title,
                    "imdb_id": item.imdb_id,
                    "quality_profile": item.quality_profile,
                }
            ),
            results_count=scrape_results_count,
            results_summary=json.dumps(self._summarise_results([])),
            selected_result=selected_result,
            duration_ms=total_duration_ms,
        )

        logger.info(
            "scrape_pipeline: successfully added item id=%d title=%r to RD "
            "(rd_id=%s hash=%s cached=%s)",
            item.id,
            item.title,
            rd_id,
            info_hash,
            winner_is_cached,
        )

        return PipelineResult(
            item_id=item.id,
            action="added_to_rd",
            message=(
                f"Torrent added to RD: {winner.result.title} "
                f"(hash={info_hash}, rd_id={rd_id})"
            ),
            selected_hash=info_hash,
            rd_torrent_id=rd_id,
            scrape_results_count=scrape_results_count,
            filtered_results_count=filtered_results_count,
        )

    # ------------------------------------------------------------------
    # Scrape log helper
    # ------------------------------------------------------------------

    async def _log_scrape(
        self,
        session: AsyncSession,
        *,
        media_item_id: int,
        scraper: str,
        query_params: str | None,
        results_count: int,
        results_summary: str | None,
        selected_result: str | None,
        duration_ms: int,
    ) -> None:
        """Insert a ScrapeLog row for a pipeline step.

        All JSON-serialisable values must be pre-serialised to strings by the
        caller using ``json.dumps`` before passing here.

        Args:
            session: Caller-managed async database session.
            media_item_id: FK to the MediaItem that was scraped.
            scraper: Label for the scraper/step (e.g. ``"zilean"``,
                ``"torrentio"``, ``"mount_scan"``, ``"rd_check"``,
                ``"pipeline"``).
            query_params: JSON string of the parameters used for this query.
            results_count: Number of results returned.
            results_summary: JSON string summarising the top results.
            selected_result: JSON string describing the chosen result, or None.
            duration_ms: Wall-clock time for this step in milliseconds.
        """
        log_entry = ScrapeLog(
            media_item_id=media_item_id,
            scraper=scraper,
            query_params=query_params,
            results_count=results_count,
            results_summary=results_summary,
            selected_result=selected_result,
            duration_ms=duration_ms,
        )
        session.add(log_entry)
        try:
            await session.flush()
        except Exception as exc:
            logger.warning(
                "scrape_pipeline._log_scrape: flush failed for scraper=%r "
                "media_item_id=%d: %s",
                scraper,
                media_item_id,
                exc,
            )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _summarise_results(results: list[_AnyResult]) -> list[dict[str, Any]]:
        """Return a compact list of dicts describing the top results.

        Used to populate the ``results_summary`` column in scrape_log so UI
        debugging views can show what the scrapers returned without storing the
        full result objects.

        Args:
            results: Up to N scrape results to summarise.

        Returns:
            List of dicts with keys: ``title``, ``info_hash``, ``resolution``,
            ``size_bytes``, ``seeders``, ``is_season_pack``.
        """
        return [
            {
                "title": r.title,
                "info_hash": r.info_hash,
                "resolution": r.resolution,
                "size_bytes": r.size_bytes,
                "seeders": r.seeders,
                "is_season_pack": r.is_season_pack,
            }
            for r in results
        ]

    # Regex for extracting season number from filenames (S01E05 → season 1).
    _SEASON_RE = re.compile(r"[Ss](\d{1,2})[Ee]\d{1,3}")

    @staticmethod
    def _validate_xem_episode_range(
        files: list[dict[str, Any]],
        anchor_episode: int,
        end_episode: int,
        anchor_season: int | None = None,
    ) -> tuple[bool, list[int]]:
        """Check whether RD torrent files contain episodes in the target XEM range.

        Iterates over the torrent's file list, filters to video files only, and
        parses each filename to extract an episode number.  Returns True when at
        least one video file has an episode number that falls within the inclusive
        range ``[anchor_episode, end_episode]``.

        When ``anchor_season`` is provided, also verifies that the parsed season
        number matches.  This is required for multi-TMDB-season shows where
        episode numbers alone are ambiguous (e.g. S01E01-E12 vs S02E01-E12).

        Args:
            files: List of file dicts from the RD torrent info response.  Each
                dict is expected to have at minimum a ``"path"`` key whose value
                is a string (e.g. ``"/Show.Title.S01E01.mkv"``).
            anchor_episode: First TMDB episode number in the target range
                (inclusive).
            end_episode: Last TMDB episode number in the target range
                (inclusive).
            anchor_season: Expected TMDB season number.  When set, files whose
                parsed season differs are excluded from matching.

        Returns:
            A 2-tuple ``(valid, matched_episodes)`` where:
            - ``valid`` is ``True`` if at least one video file episode falls
              within ``[anchor_episode, end_episode]`` (and season matches when
              ``anchor_season`` is set).
            - ``matched_episodes`` is the sorted list of matched episode numbers.
              Always empty when ``valid`` is ``False``.
        """
        matched: list[int] = []
        for f in files:
            path: str = f.get("path", "")
            if not path:
                continue
            _, ext = os.path.splitext(path)
            if ext.lower() not in VIDEO_EXTENSIONS:
                continue
            filename = os.path.basename(path)

            # Season check: if anchor_season is set, verify the file's season matches.
            if anchor_season is not None:
                season_match = ScrapePipeline._SEASON_RE.search(filename)
                if season_match and int(season_match.group(1)) != anchor_season:
                    continue  # Wrong season — skip this file.

            ep_num = parse_episode_from_filename(filename)
            if ep_num is not None and anchor_episode <= ep_num <= end_episode:
                matched.append(ep_num)
        matched.sort()
        return (len(matched) > 0, matched)

    async def _split_season_pack_to_episodes(
        self,
        session: AsyncSession,
        item: MediaItem,
    ) -> int:
        """Split a season pack item into individual episode items.

        When no season packs are available on any scraper, creates one queue
        item per episode so that individual episodes can be acquired instead.

        For XEM scene packs (``xem_scene_pack`` in metadata), uses the stored
        TMDB episode list instead of querying TMDB directly — the season number
        on the item is the scene season and may not exist in TMDB.

        For regular season packs, uses TMDB to determine the episode count.

        Existing episode items for the same show+season are skipped to avoid
        duplicates.

        Args:
            session: Caller-managed async database session.
            item: The season pack MediaItem to split.

        Returns:
            Number of new episode items created. Returns 0 on failure (e.g.
            TMDB unavailable, all episodes already exist in queue).
        """
        if not item.tmdb_id or item.season is None:
            logger.warning(
                "scrape_pipeline._split_season_pack_to_episodes: item id=%d "
                "missing tmdb_id or season — cannot split",
                item.id,
            )
            return 0

        # ------------------------------------------------------------------
        # XEM scene pack branch: use stored TMDB episode list
        # ------------------------------------------------------------------
        if item.metadata_json:
            try:
                _meta = json.loads(item.metadata_json)
            except (ValueError, TypeError):
                _meta = {}
        else:
            _meta = {}

        if _meta.get("xem_scene_pack") and _meta.get("tmdb_episodes"):
            tmdb_episodes: list[dict] = _meta["tmdb_episodes"]
            logger.info(
                "scrape_pipeline._split_season_pack_to_episodes: item id=%d is XEM scene pack "
                "scene_season=%s — splitting using stored TMDB episode list (%d episodes)",
                item.id, item.season, len(tmdb_episodes),
            )

            # Find existing episode items keyed by (tmdb_season, tmdb_episode).
            existing_stmt = select(MediaItem.season, MediaItem.episode).where(
                MediaItem.tmdb_id == item.tmdb_id,
                MediaItem.episode.is_not(None),
            )
            existing_result = await session.execute(existing_stmt)
            existing_keys: set[tuple[int, int]] = {
                (row[0], row[1]) for row in existing_result.all()
                if row[0] is not None and row[1] is not None
            }

            now = datetime.now(UTC)
            created = 0
            for ep_entry in tmdb_episodes:
                try:
                    ep_season = int(ep_entry["s"])
                    ep_episode = int(ep_entry["e"])
                except (KeyError, ValueError, TypeError):
                    logger.debug(
                        "scrape_pipeline._split_season_pack_to_episodes: skipping "
                        "malformed ep_entry %r for item id=%d",
                        ep_entry, item.id,
                    )
                    continue

                if (ep_season, ep_episode) in existing_keys:
                    logger.debug(
                        "scrape_pipeline._split_season_pack_to_episodes: S%02dE%02d "
                        "already exists for item id=%d — skipping",
                        ep_season, ep_episode, item.id,
                    )
                    continue

                new_item = MediaItem(
                    title=item.title,
                    year=item.year,
                    media_type=MediaType.SHOW,
                    tmdb_id=item.tmdb_id,
                    imdb_id=item.imdb_id,
                    tvdb_id=item.tvdb_id,
                    state=QueueState.WANTED,
                    source="season_pack_split",
                    added_at=now,
                    state_changed_at=now,
                    retry_count=0,
                    season=ep_season,
                    episode=ep_episode,
                    is_season_pack=False,
                    quality_profile=item.quality_profile,
                    original_language=item.original_language,
                )
                session.add(new_item)
                existing_keys.add((ep_season, ep_episode))
                created += 1

            if created > 0:
                await session.flush()

            logger.info(
                "scrape_pipeline._split_season_pack_to_episodes: XEM scene pack item id=%d "
                "title=%r scene_season=%d created %d TMDB episode items (skipped %d existing)",
                item.id, item.title, item.season, created,
                len(tmdb_episodes) - created,
            )
            return created

        # ------------------------------------------------------------------
        # Standard branch: look up episode count from TMDB
        # ------------------------------------------------------------------
        try:
            tmdb_id_int = int(item.tmdb_id)
        except (ValueError, TypeError):
            logger.warning(
                "scrape_pipeline._split_season_pack_to_episodes: item id=%d "
                "has non-numeric tmdb_id=%r — cannot split",
                item.id, item.tmdb_id,
            )
            return 0

        from src.services.tmdb import tmdb_client

        try:
            show_details = await tmdb_client.get_show_details(tmdb_id_int)
        except Exception as exc:
            logger.warning(
                "scrape_pipeline._split_season_pack_to_episodes: TMDB error for "
                "tmdb_id=%s (item id=%d): %s",
                item.tmdb_id, item.id, exc,
            )
            return 0

        if show_details is None:
            logger.warning(
                "scrape_pipeline._split_season_pack_to_episodes: TMDB returned None "
                "for tmdb_id=%s (item id=%d)",
                item.tmdb_id, item.id,
            )
            return 0

        # Find the matching season and get its episode count.
        season_info = next(
            (s for s in show_details.seasons if s.season_number == item.season),
            None,
        )
        if season_info is None or season_info.episode_count <= 0:
            logger.warning(
                "scrape_pipeline._split_season_pack_to_episodes: season %d not found "
                "in TMDB data for tmdb_id=%s (item id=%d)",
                item.season, item.tmdb_id, item.id,
            )
            return 0

        episode_count = season_info.episode_count
        logger.info(
            "scrape_pipeline._split_season_pack_to_episodes: item id=%d tmdb_id=%s "
            "season=%d has %d episodes per TMDB",
            item.id, item.tmdb_id, item.season, episode_count,
        )

        # Find which episodes already exist in the queue for this show+season.
        existing_stmt = select(MediaItem.episode).where(
            MediaItem.tmdb_id == item.tmdb_id,
            MediaItem.season == item.season,
            MediaItem.episode.is_not(None),
        )
        existing_result = await session.execute(existing_stmt)
        existing_episodes: set[int] = {row[0] for row in existing_result.all()}

        now = datetime.now(UTC)
        created = 0
        for ep_num in range(1, episode_count + 1):
            if ep_num in existing_episodes:
                logger.debug(
                    "scrape_pipeline._split_season_pack_to_episodes: episode %d already "
                    "exists for item id=%d season=%d — skipping",
                    ep_num, item.id, item.season,
                )
                continue

            new_item = MediaItem(
                title=item.title,
                year=item.year,
                media_type=MediaType.SHOW,
                tmdb_id=item.tmdb_id,
                imdb_id=item.imdb_id,
                tvdb_id=item.tvdb_id,
                state=QueueState.WANTED,
                source="season_pack_split",
                added_at=now,
                state_changed_at=now,
                retry_count=0,
                season=item.season,
                episode=ep_num,
                is_season_pack=False,
                quality_profile=item.quality_profile,
                original_language=item.original_language,
            )
            session.add(new_item)
            created += 1

        if created > 0:
            await session.flush()

        logger.info(
            "scrape_pipeline._split_season_pack_to_episodes: created %d new episode "
            "items for item id=%d title=%r season=%d (skipped %d existing)",
            created, item.id, item.title, item.season,
            episode_count - created,
        )
        return created

    async def _safe_transition_sleeping(
        self, session: AsyncSession, item: MediaItem
    ) -> None:
        """Attempt to transition *item* to SLEEPING, swallowing any errors.

        Used in the outermost exception handler to ensure items don't get
        stuck in SCRAPING when an unexpected error occurs.

        Args:
            session: Caller-managed async database session.
            item: The MediaItem to transition.
        """
        try:
            await queue_manager.transition(session, item.id, QueueState.SLEEPING)
        except Exception as exc:
            logger.error(
                "scrape_pipeline._safe_transition_sleeping: transition failed "
                "for item id=%d: %s",
                item.id,
                exc,
            )


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

scrape_pipeline = ScrapePipeline()
