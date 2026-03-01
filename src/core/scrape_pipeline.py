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
from typing import Any

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

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

        Executes the six-step acquisition flow in order:
          1. Mount check — return immediately on a hit.
          2. Dedup check — return immediately when content already tracked.
          3. Zilean scrape — swallows all errors.
          4. Torrentio scrape — swallows all errors.
          5. Combine, check RD cache, filter.
          6. Add best result to Real-Debrid.

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
        # Step 3 — Zilean scrape
        # ------------------------------------------------------------------
        zilean_results, zilean_duration_ms = await self._step_zilean(session, item)

        # ------------------------------------------------------------------
        # Step 4 — Torrentio scrape
        # ------------------------------------------------------------------
        torrentio_results, torrentio_duration_ms = await self._step_torrentio(
            session, item
        )

        # ------------------------------------------------------------------
        # Step 5 — Combine, RD cache check, filter
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

        # Filter and rank first, then probe RD cache on the top candidates.
        ranked = filter_engine.filter_and_rank(
            combined,  # type: ignore[arg-type]
            profile_name=item.quality_profile,
            cached_hashes=set(),
        )

        # Probe RD cache for the top 10 filtered results, keeping cached
        # torrents in the RD account to avoid a redundant add_magnet later.
        top_hashes = [
            fr.result.info_hash for fr in ranked[:10] if fr.result.info_hash
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
            )

        logger.debug(
            "scrape_pipeline: %d/%d top results cached for item id=%d",
            len(cached_set),
            len(top_hashes),
            item.id,
        )
        filtered_count = len(ranked)
        best: FilteredResult | None = ranked[0] if ranked else None

        if best is None:
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
        # Step 6 — Add to Real-Debrid
        # ------------------------------------------------------------------
        return await self._step_add_to_rd(
            session,
            item,
            best=best,
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
        self, session: AsyncSession, item: MediaItem
    ) -> tuple[list[ZileanResult], int]:
        """Query Zilean for results.

        Errors are caught and logged; an empty list is returned so the pipeline
        continues to Torrentio.

        Args:
            session: Caller-managed async database session.
            item: The MediaItem to search for.

        Returns:
            2-tuple of (results, duration_ms).
        """
        t0 = time.monotonic()
        results: list[ZileanResult] = []

        query_params = json.dumps(
            {
                "query": item.title,
                "season": item.season,
                "episode": item.episode,
                "year": item.year,
                "imdb_id": item.imdb_id,
            }
        )

        try:
            results = await zilean_client.search(
                query=item.title,
                season=item.season,
                episode=item.episode,
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
        self, session: AsyncSession, item: MediaItem
    ) -> tuple[list[TorrentioResult], int]:
        """Query Torrentio for results using the media type to pick the correct call.

        Errors are caught and logged; an empty list is returned so the pipeline
        can proceed with whatever Zilean returned (or to no_results).

        Args:
            session: Caller-managed async database session.
            item: The MediaItem to scrape.

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
            query_params = json.dumps(
                {
                    "imdb_id": item.imdb_id,
                    "type": "show",
                    "season": season,
                    "episode": episode,
                }
            )
            try:
                results = await torrentio_client.scrape_episode(
                    item.imdb_id, season, episode
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
        cached_set: set[str],
        cache_results: dict[str, CacheCheckResult],
        scrape_results_count: int,
        filtered_results_count: int,
        total_duration_ms: int,
    ) -> PipelineResult:
        """Add the best-scored result to Real-Debrid and register it locally.

        If the cache check already kept the torrent (rd_id in cache_results),
        the add_magnet call is skipped and the existing rd_id is reused.

        On add_magnet failure: transitions to SLEEPING, returns error result.
        On select_files failure: logs warning but does NOT change state (the
        torrent is already in RD and will be processed by the CHECKING step).

        Args:
            session: Caller-managed async database session.
            item: The MediaItem being acquired.
            best: The top-ranked FilteredResult from the filter engine.
            cached_set: Info hashes known to be cached in RD.
            cache_results: Results from check_cached_batch (may contain rd_ids).
            scrape_results_count: Total raw results count (for PipelineResult).
            filtered_results_count: Filtered results count (for PipelineResult).
            total_duration_ms: Cumulative scraper duration (for scrape_log).

        Returns:
            A PipelineResult describing the outcome.
        """
        info_hash: str = best.result.info_hash
        magnet_uri = f"magnet:?xt=urn:btih:{info_hash}"
        is_cached = info_hash in cached_set

        logger.info(
            "scrape_pipeline: adding to RD — item id=%d title=%r hash=%s "
            "score=%.1f cached=%s",
            item.id,
            item.title,
            info_hash,
            best.score,
            is_cached,
        )

        # --- Reuse rd_id from cache check if available ---
        existing_cr = cache_results.get(info_hash)
        rd_id: str = ""
        if existing_cr and existing_cr.rd_id:
            rd_id = existing_cr.rd_id
            logger.info(
                "scrape_pipeline: reusing kept rd_id=%s from cache check for hash=%s",
                rd_id, info_hash,
            )
        else:
            # --- add_magnet ---
            try:
                add_response = await rd_client.add_magnet(magnet_uri)
            except RealDebridError as exc:
                logger.error(
                    "scrape_pipeline: add_magnet failed for item id=%d hash=%s: %s",
                    item.id,
                    info_hash,
                    exc,
                )
                await self._log_scrape(
                    session,
                    media_item_id=item.id,
                    scraper="pipeline",
                    query_params=json.dumps(
                        {"magnet_uri": magnet_uri, "step": "add_magnet"}
                    ),
                    results_count=scrape_results_count,
                    results_summary=None,
                    selected_result=json.dumps({"info_hash": info_hash, "error": str(exc)}),
                    duration_ms=total_duration_ms,
                )
                await self._cleanup_kept_torrents(cache_results)
                await queue_manager.transition(session, item.id, QueueState.SLEEPING)
                return PipelineResult(
                    item_id=item.id,
                    action="error",
                    message=f"RD add_magnet failed: {exc}",
                    selected_hash=info_hash,
                    scrape_results_count=scrape_results_count,
                    filtered_results_count=filtered_results_count,
                )
            except Exception as exc:
                logger.error(
                    "scrape_pipeline: unexpected error in add_magnet for item id=%d: %s",
                    item.id,
                    exc,
                    exc_info=True,
                )
                await self._cleanup_kept_torrents(cache_results)
                await queue_manager.transition(session, item.id, QueueState.SLEEPING)
                return PipelineResult(
                    item_id=item.id,
                    action="error",
                    message=f"Unexpected error adding magnet: {exc}",
                    selected_hash=info_hash,
                    scrape_results_count=scrape_results_count,
                    filtered_results_count=filtered_results_count,
                )

            rd_id = str(add_response.get("id", ""))

        if not rd_id:
            logger.error(
                "scrape_pipeline: add_magnet returned empty rd_id for item id=%d hash=%s",
                item.id,
                info_hash,
            )
            await self._cleanup_kept_torrents(cache_results)
            await queue_manager.transition(session, item.id, QueueState.SLEEPING)
            return PipelineResult(
                item_id=item.id,
                action="error",
                message="RD add_magnet returned empty torrent ID",
                selected_hash=info_hash,
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

        # --- Register in local dedup registry ---
        try:
            await dedup_engine.register_torrent(
                session,
                rd_id=rd_id,
                info_hash=info_hash,
                magnet_uri=magnet_uri,
                media_item_id=item.id,
                filename=best.result.title,
                filesize=best.result.size_bytes,
                resolution=best.result.resolution,
                cached=is_cached,
            )
        except Exception as exc:
            logger.error(
                "scrape_pipeline: register_torrent failed for item id=%d hash=%s: %s",
                item.id,
                info_hash,
                exc,
            )
            # Rollback the failed flush so the session is usable for subsequent ops.
            # The torrent is already in RD so we still transition to ADDING.
            await session.rollback()

        # --- Transition to ADDING ---
        await queue_manager.transition(session, item.id, QueueState.ADDING)

        # --- Log the pipeline result ---
        selected_result = json.dumps(
            {
                "info_hash": info_hash,
                "rd_id": rd_id,
                "title": best.result.title,
                "score": best.score,
                "score_breakdown": best.score_breakdown,
                "resolution": best.result.resolution,
                "cached": is_cached,
                "size_bytes": best.result.size_bytes,
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
                f"Torrent added to RD: {best.result.title} "
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
