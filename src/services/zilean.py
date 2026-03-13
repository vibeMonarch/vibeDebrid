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

# Fallback: anime-style "S2 - 06" or "S02-E06" (PTN doesn't handle dash-separated notation)
_SEASON_DASH_EP_RE = re.compile(
    r"(?:\b|_)S(\d{1,2})\s*-\s*E?(\d{1,3})(?:\b|_|\[)",
    re.IGNORECASE,
)

# Fallback: ordinal season + episode, e.g. "2nd Season - 01", "1st Season - 28"
_ORDINAL_SEASON_EP_RE = re.compile(
    r"(\d+)(?:st|nd|rd|th)\s+Season\s*[-–]\s*(\d{1,3})\b",
    re.IGNORECASE,
)

# Fallback: anime bare dash notation, e.g. "[Group] Title - 29 [1080p]"
_ANIME_BARE_DASH_EP_RE = re.compile(r"\s-\s(\d{1,3})(?:\s|$|\[|\()")

# Dub / Dual Audio detection — used to tag results with "Dubbed" or "Dual Audio".
# _DUB_RE matches "DUB" and "DUBBED" but not "DUBLIN" (word boundary prevents it).
_DUB_RE = re.compile(r"\bDUB(?:BED)?\b", re.IGNORECASE)
# _DUAL_AUDIO_RE requires "AUDIO" after "DUAL" (with optional separator) to avoid
# false positives in show titles like "Dual.Survival.S01E01".
_DUAL_AUDIO_RE = re.compile(r"\bDUAL[\.\s-]?AUDIO\b", re.IGNORECASE)

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

# Cyrillic script detection — any Cyrillic character in the title means Russian.
_CYRILLIC_RE = re.compile(r"[\u0400-\u04FF]")

# Short ISO/scene abbreviations that need word-boundary matching to avoid false
# positives (e.g. "RUS" inside "BRUSH").  Compiled once at module level.
_LANGUAGE_ABBREV_TOKENS: dict[str, str] = {
    "RUS": "Russian",
    "JAP": "Japanese",
    "JPN": "Japanese",
    "KOR": "Korean",
    "CHI": "Chinese",
    "ITA": "Italian",
    "NLD": "Dutch",
    "DEU": "German",
    "SPA": "Spanish",
    "POR": "Portuguese",
    "FRA": "French",
}
_LANGUAGE_ABBREV_RES: dict[str, re.Pattern[str]] = {
    abbrev: re.compile(rf"\b{abbrev}\b", re.IGNORECASE)
    for abbrev in _LANGUAGE_ABBREV_TOKENS
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
    title: str  # raw_title from Zilean
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

        if not any([resolution, codec, quality, release_group]):
            ptn_data: dict[str, Any] = {}
            try:
                ptn_data = PTN.parse(raw_title) or {}
            except (ValueError, TypeError, KeyError) as exc:
                logger.debug(
                    "zilean._parse_entry: PTN failed for raw_title=%r (%s)",
                    raw_title,
                    exc,
                )
            resolution = resolution or ptn_data.get("resolution")
            codec = codec or ptn_data.get("codec")
            quality = quality or ptn_data.get("quality")
            release_group = release_group or ptn_data.get("group")

            if season is None:
                season = ptn_data.get("season")
            if episode is None:
                episode = ptn_data.get("episode")

        # Fallback: PTN doesn't handle dash-separated anime notation like "S2 - 06"
        if episode is None:
            m = _SEASON_DASH_EP_RE.search(raw_title)
            if m:
                season = int(m.group(1))
                episode = int(m.group(2))

        # Fallback: ordinal season notation "2nd Season - 01"
        if episode is None:
            m = _ORDINAL_SEASON_EP_RE.search(raw_title)
            if m:
                season = int(m.group(1))
                episode = int(m.group(2))

        # Fallback: anime bare dash "Title - 29 [1080p]"
        if episode is None:
            m = _ANIME_BARE_DASH_EP_RE.search(raw_title)
            if m:
                episode = int(m.group(1))
                # No season info from this pattern — leave season as-is

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
            raw_title: The raw_title string from a Zilean entry.

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
        # Cyrillic script → Russian (catches titles written in Russian that lack
        # any English-language tag).
        if _CYRILLIC_RE.search(raw_title) and "Russian" not in seen:
            found.append("Russian")
            seen.add("Russian")
        # Short ISO/scene abbreviations (word-boundary matched).
        for abbrev, lang_name in _LANGUAGE_ABBREV_TOKENS.items():
            if lang_name not in seen and _LANGUAGE_ABBREV_RES[abbrev].search(raw_title):
                found.append(lang_name)
                seen.add(lang_name)
        # Dub / Dual Audio detection
        if _DUB_RE.search(raw_title) and "Dubbed" not in seen:
            found.append("Dubbed")
            seen.add("Dubbed")
        if _DUAL_AUDIO_RE.search(raw_title) and "Dual Audio" not in seen:
            found.append("Dual Audio")
            seen.add("Dual Audio")
        return found


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

zilean_client = ZileanClient()
