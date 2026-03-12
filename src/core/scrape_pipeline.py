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

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.core.dedup import dedup_engine
from src.core.filter_engine import FilteredResult, filter_engine
from src.core.mount_scanner import mount_scanner
from src.core.queue_manager import queue_manager
from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.scrape_result import ScrapeLog
from src.services.real_debrid import CacheCheckResult, RealDebridError, RealDebridRateLimitError, rd_client
from src.services.torrentio import TorrentioResult, torrentio_client
from src.services.zilean import ZileanResult, zilean_client

logger = logging.getLogger(__name__)

# Union type for combined scraper results
_AnyResult = TorrentioResult | ZileanResult


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
          3. XEM scene numbering resolution (once for both scrapers).
          4. Zilean scrape — swallows all errors.
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
        except Exception as exc:
            logger.error(
                "scrape_pipeline.run: unexpected exception for item id=%d title=%r: %s",
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
        # Step 0 — Backfill original_language from TMDB if missing
        # Only runs when prefer_original_language is enabled AND TMDB is
        # configured — avoids unnecessary API calls when the feature is off.
        # ------------------------------------------------------------------
        if (
            item.original_language is None
            and item.tmdb_id
            and settings.filters.prefer_original_language
            and settings.tmdb.enabled
            and settings.tmdb.api_key
        ):
            try:
                from src.services.tmdb import tmdb_client as _tmdb_client

                tmdb_id_int = int(item.tmdb_id)
                if item.media_type == MediaType.MOVIE:
                    _detail = await _tmdb_client.get_movie_details(tmdb_id_int)
                else:
                    _detail = await _tmdb_client.get_show_details(tmdb_id_int)
                if _detail is not None and _detail.original_language:
                    item.original_language = _detail.original_language
                    logger.debug(
                        "scrape_pipeline: backfilled original_language=%r for item id=%d",
                        item.original_language,
                        item.id,
                    )
            except Exception as exc:
                logger.debug(
                    "original_language backfill failed for item %d: %s", item.id, exc
                )

        # ------------------------------------------------------------------
        # Step 1 — Mount check
        # ------------------------------------------------------------------
        mount_result = await self._step_mount_check(session, item)
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
        if (
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
            except Exception:
                logger.debug(
                    "scrape_pipeline: XEM lookup failed for item id=%d, using original numbering",
                    item.id,
                )

        # ------------------------------------------------------------------
        # Step 4 — Zilean scrape
        # ------------------------------------------------------------------
        zilean_results, zilean_duration_ms = await self._step_zilean(
            session, item, scene_season=scene_season, scene_episode=scene_episode
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
        combined: list[_AnyResult] = zilean_results + torrentio_results  # type: ignore[assignment]
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
                duration_ms=zilean_duration_ms + torrentio_duration_ms,
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

        # Check for force_original_language flag in metadata and clear it.
        force_original = False
        if item.metadata_json:
            try:
                _meta = json.loads(item.metadata_json)
                force_original = bool(_meta.pop("force_original_language", False))
                if force_original:
                    item.metadata_json = json.dumps(_meta)
                    logger.info(
                        "scrape_pipeline: force_original_language flag detected for "
                        "item id=%d original_language=%r",
                        item.id, orig_lang_name,
                    )
            except (ValueError, TypeError):
                pass

        # Filter and rank first, then probe RD cache on the top candidates.
        ranked = filter_engine.filter_and_rank(
            combined,  # type: ignore[arg-type]
            profile_name=item.quality_profile,
            cached_hashes=set(),
            prefer_season_packs=bool(item.is_season_pack),
            original_language=orig_lang_name,
        )

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
                        duration_ms=zilean_duration_ms + torrentio_duration_ms,
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

        # Probe RD cache for the top filtered results, keeping cached
        # torrents in the RD account to avoid a redundant add_magnet later.
        cache_limit = settings.search.cache_check_limit
        top_hashes = [
            fr.result.info_hash for fr in ranked[:cache_limit] if fr.result.info_hash
        ]
        cache_results: dict[str, CacheCheckResult] = {}
        cached_set: set[str] = set()
        if top_hashes:
            try:
                cache_results = await rd_client.check_cached_batch(
                    top_hashes, keep_if_cached=True,
                )
                cached_set = {
                    h for h, cr in cache_results.items() if cr.cached is True
                }
            except Exception as exc:
                logger.warning(
                    "scrape_pipeline: check_cached_batch failed for item id=%d: %s",
                    item.id,
                    exc,
                )

        # Re-rank with actual cached hashes if any were found.
        if cached_set:
            ranked = filter_engine.filter_and_rank(
                combined,  # type: ignore[arg-type]
                profile_name=item.quality_profile,
                cached_hashes=cached_set,
                prefer_season_packs=bool(item.is_season_pack),
                original_language=orig_lang_name,
            )

        # When force_original is set, double the original_language score component
        # to give it stronger weight over quality factors.
        if force_original and orig_lang_name and ranked:
            for fr in ranked:
                ol_score = fr.score_breakdown.get("original_language", 0.0)
                if ol_score != 0.0:
                    extra = ol_score  # Double by adding an equal amount
                    fr.score_breakdown["original_language"] = ol_score + extra
                    fr.score += extra
            ranked.sort(key=lambda fr: fr.score, reverse=True)

        logger.debug(
            "scrape_pipeline: %d/%d top results cached for item id=%d",
            len(cached_set),
            len(top_hashes),
            item.id,
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
                        duration_ms=zilean_duration_ms + torrentio_duration_ms,
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
                duration_ms=zilean_duration_ms + torrentio_duration_ms,
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
        # Step 7 — Add to Real-Debrid
        # ------------------------------------------------------------------
        return await self._step_add_to_rd(
            session,
            item,
            best=best,
            ranked=ranked,
            cached_set=cached_set,
            cache_results=cache_results,
            scrape_results_count=total_count,
            filtered_results_count=filtered_count,
            total_duration_ms=zilean_duration_ms + torrentio_duration_ms,
        )

    # ------------------------------------------------------------------
    # Step implementations
    # ------------------------------------------------------------------

    async def _step_mount_check(
        self, session: AsyncSession, item: MediaItem
    ) -> PipelineResult | None:
        """Check the Zurg mount index for an existing file match.

        Args:
            session: Caller-managed async database session.
            item: The MediaItem to check.

        Returns:
            A PipelineResult on a mount hit, or None to continue the pipeline.
        """
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
            matches = await mount_scanner.lookup(
                session, item.title, item.season, item.episode
            )
        except Exception as exc:
            logger.warning(
                "scrape_pipeline: mount lookup failed for item id=%d: %s",
                item.id,
                exc,
            )
            return None

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

        # Mount hit — log and return; caller transitions state
        best_match = matches[0]
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
        await queue_manager.transition(session, item.id, QueueState.COMPLETE)
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
    ) -> tuple[list[ZileanResult], int]:
        """Query Zilean for results.

        Errors are caught and logged; an empty list is returned so the pipeline
        continues to Torrentio.

        Args:
            session: Caller-managed async database session.
            item: The MediaItem to search for.
            scene_season: Pre-resolved scene season from XEM (or item.season if
                not provided).
            scene_episode: Pre-resolved scene episode from XEM (or item.episode
                if not provided).

        Returns:
            2-tuple of (results, duration_ms).
        """
        t0 = time.monotonic()
        results: list[ZileanResult] = []

        # Fall back to item's original numbering when not pre-resolved.
        if scene_season is None:
            scene_season = item.season
        if scene_episode is None:
            scene_episode = item.episode

        query_params = json.dumps(
            {
                "query": item.title,
                "season": item.season,
                "episode": item.episode,
                "scene_season": scene_season,
                "scene_episode": scene_episode,
                "year": item.year,
                "imdb_id": item.imdb_id,
            }
        )

        try:
            results = await zilean_client.search(
                query=item.title,
                season=scene_season,
                episode=scene_episode,
                year=item.year,
                imdb_id=item.imdb_id,
            )
            logger.debug(
                "scrape_pipeline: Zilean returned %d results for item id=%d",
                len(results),
                item.id,
            )
        except Exception as exc:
            logger.warning(
                "scrape_pipeline: Zilean search failed for item id=%d: %s",
                item.id,
                exc,
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
                results = await torrentio_client.scrape_movie(item.imdb_id, include_debrid_key=False)
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
                    include_debrid_key=False,
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

    async def _step_add_to_rd(
        self,
        session: AsyncSession,
        item: MediaItem,
        *,
        best: FilteredResult,
        ranked: list[FilteredResult],
        cached_set: set[str],
        cache_results: dict[str, CacheCheckResult],
        scrape_results_count: int,
        filtered_results_count: int,
        total_duration_ms: int,
    ) -> PipelineResult:
        """Add the best-scored result to Real-Debrid and register it locally.

        If the cache check already kept the torrent (rd_id in cache_results),
        the add_magnet call is skipped and the existing rd_id is reused.

        When add_magnet returns HTTP 451 (infringing_file), the candidate is
        permanently blocked and the pipeline falls back to the next ranked
        candidate.  Only transient errors (rate limit, network) abort the loop
        and send the item to SLEEPING immediately — burning through all
        candidates would risk exhausting the RD rate limit.

        On add_magnet failure (non-451): transitions to SLEEPING, returns error result.
        On select_files failure: logs warning but does NOT change state (the
        torrent is already in RD and will be processed by the CHECKING step).

        Args:
            session: Caller-managed async database session.
            item: The MediaItem being acquired.
            best: The top-ranked FilteredResult from the filter engine.
            ranked: Full ordered list of FilteredResults (used for fallback).
            cached_set: Info hashes known to be cached in RD.
            cache_results: Results from check_cached_batch (may contain rd_ids).
            scrape_results_count: Total raw results count (for PipelineResult).
            filtered_results_count: Filtered results count (for PipelineResult).
            total_duration_ms: Cumulative scraper duration (for scrape_log).

        Returns:
            A PipelineResult describing the outcome.
        """
        # Build the candidate list from all ranked results, skipping any hashes
        # that were already flagged as blocked (451) during the cache check.
        blocked_hashes: set[str] = {
            h for h, cr in cache_results.items() if cr.blocked
        }
        candidates: list[FilteredResult] = [
            fr for fr in ranked if fr.result.info_hash not in blocked_hashes
        ]

        # If best was blocked during cache check, candidates list will start
        # with a different entry — that's fine, the loop handles it.
        # If the original best is still available it will always be first since
        # ranked is already ordered by score and best == ranked[0].

        rd_id: str = ""
        info_hash: str = ""
        selected_best: FilteredResult = best  # will be updated if we fall back
        transient_error: Exception | None = None

        for candidate in candidates:
            candidate_hash: str = candidate.result.info_hash
            candidate_magnet = f"magnet:?xt=urn:btih:{candidate_hash}"

            # Reuse rd_id from cache check when the torrent was already kept.
            existing_cr = cache_results.get(candidate_hash)
            if existing_cr and existing_cr.rd_id:
                rd_id = existing_cr.rd_id
                info_hash = candidate_hash
                selected_best = candidate
                logger.info(
                    "scrape_pipeline: reusing kept rd_id=%s from cache check "
                    "for hash=%s (item id=%d)",
                    rd_id, info_hash, item.id,
                )
                break

            logger.info(
                "scrape_pipeline: adding to RD — item id=%d title=%r hash=%s "
                "score=%.1f cached=%s",
                item.id,
                item.title,
                candidate_hash,
                candidate.score,
                candidate_hash in cached_set,
            )

            try:
                add_response = await rd_client.add_magnet(candidate_magnet)
            except RealDebridError as exc:
                if exc.status_code == 451:
                    logger.warning(
                        "scrape_pipeline: hash=%s blocked by RD (451 infringing_file) "
                        "for item id=%d — trying next candidate",
                        candidate_hash,
                        item.id,
                    )
                    continue  # Try the next ranked candidate
                # Any other RealDebridError (rate limit, auth, etc.) is transient —
                # stop the loop immediately rather than burning through candidates.
                logger.error(
                    "scrape_pipeline: add_magnet failed for item id=%d hash=%s: %s",
                    item.id,
                    candidate_hash,
                    exc,
                )
                transient_error = exc
                break
            except Exception as exc:
                logger.error(
                    "scrape_pipeline: unexpected error in add_magnet for item id=%d "
                    "hash=%s: %s",
                    item.id,
                    candidate_hash,
                    exc,
                    exc_info=True,
                )
                transient_error = exc
                break

            rd_id = str(add_response.get("id", ""))
            if not rd_id:
                logger.error(
                    "scrape_pipeline: add_magnet returned empty rd_id for item "
                    "id=%d hash=%s",
                    item.id,
                    candidate_hash,
                )
                transient_error = ValueError("add_magnet returned empty torrent ID")
                break

            info_hash = candidate_hash
            selected_best = candidate
            break  # Success

        # --- Handle loop outcomes ---
        if not rd_id:
            # Either all candidates were blocked (451) or a transient error occurred.
            if transient_error is not None:
                err_msg = f"RD add_magnet failed: {transient_error}"
            else:
                err_msg = (
                    f"All {len(candidates)} candidate(s) are blocked by RD "
                    "(HTTP 451 infringing_file)"
                )
            logger.error(
                "scrape_pipeline: %s for item id=%d",
                err_msg,
                item.id,
            )
            failed_hash = info_hash or best.result.info_hash
            await self._log_scrape(
                session,
                media_item_id=item.id,
                scraper="pipeline",
                query_params=json.dumps(
                    {"magnet_uri": f"magnet:?xt=urn:btih:{failed_hash}", "step": "add_magnet"}
                ),
                results_count=scrape_results_count,
                results_summary=None,
                selected_result=json.dumps({"info_hash": failed_hash, "error": err_msg}),
                duration_ms=total_duration_ms,
            )
            await self._cleanup_kept_torrents(cache_results)
            await queue_manager.transition(session, item.id, QueueState.SLEEPING)
            return PipelineResult(
                item_id=item.id,
                action="error",
                message=err_msg,
                selected_hash=failed_hash,
                scrape_results_count=scrape_results_count,
                filtered_results_count=filtered_results_count,
            )

        # Clean up any other kept torrents (not the one we're using)
        await self._cleanup_kept_torrents(cache_results, exclude_hash=info_hash)

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

        is_cached = info_hash in cached_set
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
                    filename=selected_best.result.title,
                    filesize=selected_best.result.size_bytes,
                    resolution=selected_best.result.resolution,
                    cached=is_cached,
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
                "title": selected_best.result.title,
                "score": selected_best.score,
                "score_breakdown": selected_best.score_breakdown,
                "resolution": selected_best.result.resolution,
                "cached": is_cached,
                "size_bytes": selected_best.result.size_bytes,
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
            is_cached,
        )

        return PipelineResult(
            item_id=item.id,
            action="added_to_rd",
            message=(
                f"Torrent added to RD: {selected_best.result.title} "
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
    async def _cleanup_kept_torrents(
        cache_results: dict[str, CacheCheckResult],
        exclude_hash: str | None = None,
    ) -> None:
        """Delete any torrents that were kept during cache checking but not chosen.

        Args:
            cache_results: The cache check results dict.
            exclude_hash: Info hash of the torrent to keep (the chosen one).
        """
        for h, cr in cache_results.items():
            if cr.rd_id and h != exclude_hash:
                try:
                    await rd_client.delete_torrent(cr.rd_id)
                    logger.debug(
                        "scrape_pipeline: cleaned up kept torrent rd_id=%s hash=%s",
                        cr.rd_id, h,
                    )
                except Exception as exc:
                    logger.warning(
                        "scrape_pipeline: failed to clean up kept torrent rd_id=%s: %s",
                        cr.rd_id, exc,
                    )

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

    async def _split_season_pack_to_episodes(
        self,
        session: AsyncSession,
        item: MediaItem,
    ) -> int:
        """Split a season pack item into individual episode items.

        When no season packs are available on any scraper, creates one queue
        item per episode so that individual episodes can be acquired instead.
        Uses TMDB to determine the episode count for the season.

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

        now = datetime.now(timezone.utc)
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
