"""Async scraper wrapper for the Zilean DMM hashlist search API.

Zilean is a LOCAL service — it indexes DMM (Debrid Media Manager) hashlists and
exposes a simple REST search endpoint.  No authentication is required.

Real-world quirks documented here:
- Zilean may not have recently aired content; DMM hashlists lag behind active
  release groups.  Zero results is expected for fresh content, not an error.
- The response is a flat JSON array, NOT wrapped in a ``{"streams": [...]}``
  envelope like Torrentio.
- Pre-parsed metadata is available in the ``parseResponse`` sub-object (resolution,
  codec, quality, group).  PTN is only used as a fallback when ``parseResponse``
  is absent or empty.
- ``size`` is already in bytes at the top level — no emoji parsing needed.
- Zilean naturally returns a compact result set (local DB, filtered); no
  ``max_results`` cap is applied here.
- The service is typically very fast (<50ms) because it queries a local SQLite/
  Postgres instance, not a remote third-party API.
"""

from __future__ import annotations

import logging
import re
import time
from typing import Any

import httpx
import PTN
from pydantic import BaseModel

from src.config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Compiled regexes (shared logic with torrentio.py)
# ---------------------------------------------------------------------------

# Season-only patterns in release names (S02, S2) — no episode part.
# Used for season-pack detection alongside a missing episode field.
_SEASON_ONLY_RE = re.compile(
    r"(?:\b|_)S(\d{1,2})(?!\s*E\d)",  # S02 not followed by E##
    re.IGNORECASE,
)

# Explicitly tagged "complete" season packs
_COMPLETE_RE = re.compile(r"\b(?:complete|season\.?\d+)\b", re.IGNORECASE)

# Known language tokens that appear in torrent names (mirrors torrentio.py set).
_LANGUAGE_TOKENS: dict[str, str] = {
    "FRENCH": "French",
    "TRUEFRENCH": "French",
    "VOSTFR": "French",
    "GERMAN": "German",
    "DEUTSCH": "German",
    "SPANISH": "Spanish",
    "SPANISH.DUBBED": "Spanish",
    "PORTUGUESE": "Portuguese",
    "ITALIAN": "Italian",
    "DUTCH": "Dutch",
    "RUSSIAN": "Russian",
    "JAPANESE": "Japanese",
    "KOREAN": "Korean",
    "CHINESE": "Chinese",
    "MULTI": "Multi",
    "MULTi": "Multi",
}

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
    title: str  # rawTitle from Zilean
    resolution: str | None = None
    codec: str | None = None
    quality: str | None = None  # WEB-DL, BluRay, WEBRip, HDTV, …
    release_group: str | None = None
    size_bytes: int | None = None
    seeders: int | None = None  # Always None — DMM hashlists have no seeder data
    source_tracker: str | None = None  # Always None — not applicable for Zilean
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

    def _build_client(self) -> httpx.AsyncClient:
        """Create a new httpx.AsyncClient pointed at the Zilean API."""
        cfg = settings.scrapers.zilean
        return httpx.AsyncClient(
            base_url=cfg.base_url.rstrip("/"),
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

        t0 = time.monotonic()
        try:
            async with self._build_client() as client:
                response = await client.get("/dmm/filtered", params=params)
        except httpx.ConnectError as exc:
            logger.warning(
                "zilean.search: connection refused — Zilean may be down. "
                "query=%r (%s)",
                query,
                exc,
            )
            return []
        except httpx.TimeoutException as exc:
            logger.warning(
                "zilean.search: request timed out. query=%r (%s)", query, exc
            )
            return []
        except httpx.RequestError as exc:
            logger.warning(
                "zilean.search: network error. query=%r (%s)", query, exc
            )
            return []

        elapsed_ms = int((time.monotonic() - t0) * 1000)

        if response.status_code == 401 or response.status_code == 403:
            logger.error(
                "zilean.search: auth failure %d — check Zilean configuration. "
                "query=%r",
                response.status_code,
                query,
            )
            return []

        if response.status_code == 429:
            logger.warning(
                "zilean.search: rate limited (429). query=%r", query
            )
            return []

        if response.status_code >= 500:
            logger.error(
                "zilean.search: server error %d. query=%r body=%s",
                response.status_code,
                query,
                response.text[:200],
            )
            return []

        if not response.is_success:
            logger.error(
                "zilean.search: unexpected status %d. query=%r",
                response.status_code,
                query,
            )
            return []

        try:
            raw_list: list[dict[str, Any]] = response.json()
        except Exception as exc:
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

        results: list[ZileanResult] = []
        for entry in raw_list:
            parsed = self._parse_entry(entry)
            if parsed is not None:
                results.append(parsed)

        logger.debug(
            "zilean.search: query=%r elapsed=%dms raw=%d parsed=%d",
            query,
            elapsed_ms,
            len(raw_list),
            len(results),
        )
        return results

    # ------------------------------------------------------------------
    # Internal parsing helpers
    # ------------------------------------------------------------------

    def _parse_entry(self, entry: dict[str, Any]) -> ZileanResult | None:
        """Parse a single entry from the Zilean /dmm/filtered response array.

        Prioritises the pre-parsed fields in ``parseResponse`` over PTN for
        resolution, codec, quality, and release group.  PTN is only used as a
        fallback when ``parseResponse`` is absent or empty.

        Args:
            entry: A single element from the Zilean response array.

        Returns:
            A populated ZileanResult, or None if the entry should be skipped.
        """
        # --- Required fields ---
        info_hash = entry.get("infoHash")
        if not info_hash or not isinstance(info_hash, str):
            logger.debug("zilean._parse_entry: skipping entry with missing infoHash")
            return None

        raw_title = entry.get("rawTitle", "")
        if not raw_title or not isinstance(raw_title, str):
            logger.debug(
                "zilean._parse_entry: skipping entry hash=%s with missing rawTitle",
                info_hash,
            )
            return None

        # --- Size (already in bytes) ---
        size_raw = entry.get("size")
        size_bytes: int | None = int(size_raw) if isinstance(size_raw, (int, float)) and size_raw > 0 else None

        # --- Season / episode from top-level fields ---
        season_raw = entry.get("season")
        episode_raw = entry.get("episode")
        season: int | None = int(season_raw) if isinstance(season_raw, int) else None
        episode: int | None = int(episode_raw) if isinstance(episode_raw, int) else None

        # --- Metadata: prefer parseResponse, fall back to PTN ---
        parse_response: dict[str, Any] = entry.get("parseResponse") or {}

        resolution: str | None = None
        codec: str | None = None
        quality: str | None = None
        release_group: str | None = None

        if parse_response:
            resolution = parse_response.get("resolution") or None
            codec = parse_response.get("codec") or None
            quality = parse_response.get("quality") or None
            release_group = parse_response.get("group") or None

        # Fall back to PTN if parseResponse gave us nothing useful
        if not any([resolution, codec, quality, release_group]):
            ptn_data: dict[str, Any] = {}
            try:
                ptn_data = PTN.parse(raw_title) or {}
            except Exception as exc:
                logger.debug(
                    "zilean._parse_entry: PTN failed for rawTitle=%r (%s)",
                    raw_title,
                    exc,
                )
            resolution = resolution or ptn_data.get("resolution")
            codec = codec or ptn_data.get("codec")
            quality = quality or ptn_data.get("quality")
            release_group = release_group or ptn_data.get("group")

            # Also pull season/episode from PTN when Zilean's top-level fields
            # are absent (some entries omit them even though the title contains them).
            if season is None:
                season = ptn_data.get("season")
            if episode is None:
                episode = ptn_data.get("episode")

        # --- Season pack detection ---
        # A result is a season pack if:
        #   - season is set AND episode is null, AND
        #   - the raw title matches the season-only pattern OR a "complete" keyword.
        is_season_pack = False
        if episode is None:
            has_season_marker = bool(_SEASON_ONLY_RE.search(raw_title))
            has_complete_marker = bool(_COMPLETE_RE.search(raw_title))
            if has_season_marker or has_complete_marker:
                is_season_pack = True

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
            source_tracker=None,
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
            raw_title: The ``rawTitle`` string from a Zilean entry.

        Returns:
            A deduplicated list of language names found, e.g. ``["French"]``.
        """
        upper_name = raw_title.upper()
        found: list[str] = []
        seen: set[str] = set()
        for token, lang_name in _LANGUAGE_TOKENS.items():
            if token.upper() in upper_name and lang_name not in seen:
                found.append(lang_name)
                seen.add(lang_name)
        return found


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

zilean_client = ZileanClient()
