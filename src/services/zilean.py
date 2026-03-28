"""Async scraper wrapper for the Zilean DMM hashlist search API.

Zilean is a LOCAL service — it indexes DMM (Debrid Media Manager) hashlists and
exposes a simple REST search endpoint.  No authentication is required.

Real-world quirks documented here:
- Zilean may not have recently aired content; DMM hashlists lag behind active
  release groups.  Zero results is expected for fresh content, not an error.
- The response is a flat JSON array, NOT wrapped in a ``{"streams": [...]}``
  envelope like Torrentio.
- Fields use snake_case: ``info_hash``, ``raw_title``, ``seasons`` (int array),
  ``episodes`` (int array).  Parsed metadata (resolution, codec, quality,
  group) lives at the top level.
- ``size`` is returned as a string (bytes).  PTN is only used as a fallback
  when the top-level metadata fields are absent.
- Zilean naturally returns a compact result set (local DB, filtered); no
  ``max_results`` cap is applied here.
- The service is typically very fast (<50ms) because it queries a local SQLite/
  Postgres instance, not a remote third-party API.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx
import PTN
from pydantic import BaseModel

from src.config import settings
from src.services.http_client import CircuitOpenError, get_circuit_breaker, get_client
from src.services.torrent_parser import (
    detect_season_pack,
    parse_episode_fallbacks,
    parse_languages,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Custom exception
# ---------------------------------------------------------------------------


class ZileanError(Exception):
    """Raised when Zilean returns an unexpected API-level error."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


# ---------------------------------------------------------------------------
# Output schema — field names intentionally mirror TorrentioResult so that
# the scrape_pipeline can handle list[TorrentioResult | ZileanResult] uniformly.
# ---------------------------------------------------------------------------


class ZileanResult(BaseModel):
    """A single parsed result from the Zilean DMM API."""

    info_hash: str
    title: str  # raw_title from Zilean
    resolution: str | None = None
    codec: str | None = None
    quality: str | None = None  # WEB-DL, BluRay, WEBRip, HDTV, …
    release_group: str | None = None
    size_bytes: int | None = None
    seeders: int | None = None  # Always None — DMM hashlists have no seeder data
    source_tracker: str | None = None  # Set to "Zilean" during result construction
    season: int | None = None
    episode: int | None = None
    is_season_pack: bool = False
    file_idx: int | None = None  # Always None — not applicable for Zilean
    languages: list[str] = []


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class ZileanClient:
    """Async scraper client for the Zilean DMM hashlist search API.

    This client is intentionally stateless — no persistent HTTP session is kept
    between calls so that config changes (base_url, timeout) are picked up
    without restart.  Each public method opens and closes its own httpx client.

    All public methods swallow network/API failures, log them, and return an
    empty list so that a Zilean outage never crashes the queue manager.
    """

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _get_client(self) -> httpx.AsyncClient:
        """Return the pooled httpx.AsyncClient for Zilean."""
        cfg = settings.scrapers.zilean
        return await get_client(
            "zilean",
            cfg.base_url.rstrip("/"),
            timeout=cfg.timeout_seconds,
            headers={"User-Agent": "vibeDebrid/0.1"},
            follow_redirects=True,
        )

    # ------------------------------------------------------------------
    # Public search method
    # ------------------------------------------------------------------

    async def search(
        self,
        query: str,
        *,
        season: int | None = None,
        episode: int | None = None,
        year: int | None = None,
        imdb_id: str | None = None,
    ) -> list[ZileanResult]:
        """Search Zilean for torrent metadata.

        Sends a GET request to ``/dmm/filtered`` with the provided query
        parameters.  All network and API errors are swallowed and an empty
        list is returned so that the scrape pipeline can continue gracefully
        even when Zilean is unavailable.

        Args:
            query: Title search text sent as the ``Query`` parameter.
            season: Optional season number filter (``Season`` parameter).
            episode: Optional episode number filter (``Episode`` parameter).
            year: Optional release year filter (``Year`` parameter).
            imdb_id: Optional IMDB ID filter (``ImdbId`` parameter).

        Returns:
            Parsed ZileanResult objects, or an empty list on any failure.
        """
        cfg = settings.scrapers.zilean
        if not cfg.enabled:
            logger.debug("zilean.search: Zilean disabled, skipping")
            return []

        params: dict[str, Any] = {"Query": query}
        if season is not None:
            params["Season"] = season
        if episode is not None:
            params["Episode"] = episode
        if year is not None:
            params["Year"] = year
        if imdb_id is not None:
            params["ImdbId"] = imdb_id

        logger.debug(
            "zilean.search: query=%r season=%s episode=%s year=%s imdb_id=%s",
            query,
            season,
            episode,
            year,
            imdb_id,
        )

        breaker = get_circuit_breaker("zilean")
        try:
            await breaker.before_request()
        except CircuitOpenError:
            logger.warning("zilean.search: circuit open, skipping query=%r", query)
            return []

        t0 = time.monotonic()
        try:
            client = await self._get_client()
            response = await client.get("/dmm/filtered", params=params)

            # Fallback: if filtered query returned empty, retry with title
            # only.  Many torrents in Zilean lack IMDB/season/episode metadata
            # even though the title matches.
            has_filters = imdb_id is not None or season is not None or episode is not None
            if has_filters and response.is_success:
                try:
                    first_body = response.json()
                except ValueError:
                    first_body = None
                if first_body == []:
                    logger.info(
                        "zilean.search: 0 results with filters (imdb=%s season=%s episode=%s), "
                        "retrying title-only",
                        imdb_id, season, episode,
                    )
                    response = await client.get(
                        "/dmm/filtered", params={"Query": query},
                    )
        except httpx.ConnectError as exc:
            await breaker.record_failure()
            logger.warning(
                "zilean.search: connection refused — Zilean may be down. "
                "query=%r (%s)",
                query,
                exc,
            )
            return []
        except httpx.TimeoutException as exc:
            await breaker.record_failure()
            logger.warning(
                "zilean.search: request timed out. query=%r (%s)", query, exc
            )
            return []
        except httpx.RequestError as exc:
            await breaker.record_failure()
            logger.warning(
                "zilean.search: network error. query=%r (%s)", query, exc
            )
            return []

        elapsed_ms = int((time.monotonic() - t0) * 1000)

        if response.status_code == 401 or response.status_code == 403:
            await breaker.record_failure()
            logger.error(
                "zilean.search: auth failure %d — check Zilean configuration. "
                "query=%r",
                response.status_code,
                query,
            )
            return []

        if response.status_code == 429:
            # Rate limiting is expected — do not penalise the circuit breaker.
            logger.warning(
                "zilean.search: rate limited (429). query=%r", query
            )
            return []

        if response.status_code >= 500:
            await breaker.record_failure()
            logger.error(
                "zilean.search: server error %d. query=%r body=%s",
                response.status_code,
                query,
                response.text[:200],
            )
            return []

        if not response.is_success:
            await breaker.record_failure()
            logger.error(
                "zilean.search: unexpected status %d. query=%r",
                response.status_code,
                query,
            )
            return []

        await breaker.record_success()

        try:
            raw_list: list[dict[str, Any]] = response.json()
        except ValueError as exc:
            logger.error(
                "zilean.search: malformed JSON. query=%r body=%s (%s)",
                query,
                response.text[:200],
                exc,
            )
            return []

        if not isinstance(raw_list, list):
            logger.error(
                "zilean.search: expected JSON array, got %s. query=%r body=%s",
                type(raw_list).__name__,
                query,
                response.text[:200],
            )
            return []

        logger.debug(
            "zilean.search: query=%r elapsed=%dms raw_count=%d first_keys=%s",
            query,
            elapsed_ms,
            len(raw_list),
            list(raw_list[0].keys())[:10] if raw_list else "[]",
        )

        results: list[ZileanResult] = []
        for entry in raw_list:
            try:
                parsed = self._parse_entry(entry)
            except (ValueError, TypeError, KeyError) as exc:
                logger.debug("zilean: skipping unparseable entry: %s", exc)
                continue
            if parsed is not None:
                results.append(parsed)

        logger.debug(
            "zilean.search: query=%r parsed=%d/%d",
            query,
            len(results),
            len(raw_list),
        )
        return results

    # ------------------------------------------------------------------
    # Internal parsing helpers
    # ------------------------------------------------------------------

    def _parse_entry(self, entry: dict[str, Any]) -> ZileanResult | None:
        """Parse a single entry from the Zilean /dmm/filtered response array.

        Reads top-level snake_case fields (``info_hash``, ``raw_title``,
        ``seasons``, ``episodes``, ``resolution``, etc.).  Falls back to PTN
        for metadata when the top-level fields are absent or empty.

        Args:
            entry: A single element from the Zilean response array.

        Returns:
            A populated ZileanResult, or None if the entry should be skipped.
        """
        # --- Required fields ---
        info_hash = entry.get("info_hash")
        if not info_hash or not isinstance(info_hash, str):
            logger.debug("zilean._parse_entry: skipping entry with missing info_hash")
            return None

        raw_title = entry.get("raw_title", "")
        if not raw_title or not isinstance(raw_title, str):
            logger.debug(
                "zilean._parse_entry: skipping entry hash=%s with missing raw_title",
                info_hash,
            )
            return None

        # --- Size (returned as a string) ---
        size_raw = entry.get("size")
        size_bytes: int | None = None
        if size_raw is not None:
            try:
                size_val = int(size_raw)
                size_bytes = size_val if size_val > 0 else None
            except (ValueError, TypeError):
                pass

        # --- Season / episode (arrays: "seasons": [2], "episodes": [5]) ---
        season: int | None = None
        episode: int | None = None

        seasons_arr = entry.get("seasons")
        if isinstance(seasons_arr, list) and seasons_arr:
            season = int(seasons_arr[0])

        episodes_arr = entry.get("episodes")
        if isinstance(episodes_arr, list) and episodes_arr:
            episode = int(episodes_arr[0])

        # --- Metadata from top-level fields, PTN as fallback ---
        resolution: str | None = entry.get("resolution") or None
        codec: str | None = entry.get("codec") or None
        quality: str | None = entry.get("quality") or None
        release_group: str | None = entry.get("group") or None

        # PTN is always called for season/episode extraction — Zilean's structured
        # metadata fields (resolution, codec, etc.) are populated independently and
        # their presence does not imply that season/episode are also populated.
        # PTN is also used as a fallback for resolution/codec/quality/group when
        # those top-level fields are absent.
        ptn_data: dict[str, Any] = {}
        if not any([resolution, codec, quality, release_group]) or (
            season is None and episode is None
        ):
            try:
                ptn_data = PTN.parse(raw_title) or {}
            except (ValueError, TypeError, KeyError) as exc:
                logger.debug(
                    "zilean._parse_entry: PTN failed for raw_title=%r (%s)",
                    raw_title,
                    exc,
                )

        if not any([resolution, codec, quality, release_group]):
            resolution = resolution or ptn_data.get("resolution")
            codec = codec or ptn_data.get("codec")
            quality = quality or ptn_data.get("quality")
            release_group = release_group or ptn_data.get("group")

        if season is None:
            ptn_season = ptn_data.get("season")
            if isinstance(ptn_season, list):
                ptn_season = ptn_season[0] if ptn_season else None
            season = ptn_season
        if episode is None:
            ptn_episode = ptn_data.get("episode")
            if isinstance(ptn_episode, list):
                ptn_episode = ptn_episode[0] if ptn_episode else None
            episode = ptn_episode

        # Apply anime/non-standard episode fallback chain (shared with Torrentio).
        season, episode, _is_anime_batch = parse_episode_fallbacks(
            raw_title, season, episode
        )

        # --- Season pack detection ---
        # A result is a season pack if:
        #   - episode is null, AND
        #   - the raw title matches the season-only pattern OR a "complete" keyword,
        #     OR we detected an anime batch/range above.
        is_season_pack = detect_season_pack(raw_title, episode, _is_anime_batch)

        # --- Language detection ---
        languages = self._parse_languages(raw_title)

        return ZileanResult(
            info_hash=info_hash.lower(),
            title=raw_title,
            resolution=resolution,
            codec=codec,
            quality=quality,
            release_group=release_group,
            size_bytes=size_bytes,
            seeders=None,
            source_tracker="Zilean",
            season=season,
            episode=episode,
            is_season_pack=is_season_pack,
            file_idx=None,
            languages=languages,
        )

    def _parse_languages(self, raw_title: str) -> list[str]:
        """Extract language tags from a raw torrent title.

        Scans the release name for known language tokens.  English is not
        included — it is assumed when no other language is present.

        Args:
            raw_title: The raw_title string from a Zilean entry.

        Returns:
            A deduplicated list of language names found, e.g. ``["French"]``.
        """
        return parse_languages(raw_title)


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

zilean_client = ZileanClient()
