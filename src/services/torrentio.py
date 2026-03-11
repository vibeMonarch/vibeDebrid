"""Async scraper wrapper for the Torrentio Stremio addon API.

Torrentio is NOT a traditional REST API — it is a Stremio addon that exposes
stream metadata for movies and TV episodes.  No authentication is required.

Real-world quirks documented here:
- Episode-level queries return 0 results for running seasons (e.g. a show that
  is mid-air on streaming services).  The fallback chain (episode → season)
  is the primary bug fix over the previous CLI Debrid implementation.
- Do NOT append ``limit=1`` or ``cachedonly`` to URLs — these were CLI Debrid
  bugs that silently throttled and filtered results.
- The ``opts`` config value is inserted as a path segment between base_url and
  ``/stream``, e.g. ``{base_url}/sort=seeders|qualityfilter=4k/stream/...``.
- ``infoHash`` is always lowercase hex in practice, but we normalise it anyway.
- The ``title`` field contains multiple lines separated by ``\\n``:
    line 0 — the raw release name / torrent name (what PTN should parse)
    line 1 — (optional) the specific filename within the torrent
    line 2 — emoji-encoded metadata: seeders 👤, size 💾, tracker ⚙️
    line 3 — (optional) extra info such as "Multi Audio"
  We split on the first ``\\n`` only, so the emoji regexes search the remainder.
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
# Compiled regexes for metadata extraction from title line 1
# ---------------------------------------------------------------------------

# 💾 4.2 GB  /  💾 850 MB  /  💾 1.2 TB
_SIZE_RE = re.compile(r"\U0001f4be\s*([\d.]+)\s*(GB|MB|TB)", re.IGNORECASE)

# 👤 823  — Torrentio uses U+1F464 (bust in silhouette, single person).
# We also accept U+1F465 (busts in silhouette) for compatibility.
_SEEDERS_RE = re.compile(r"[\U0001f464\U0001f465]\s*(\d+)")

# ⚙️ BIT-HDTV  (everything after the gear to end-of-string)
_SOURCE_RE = re.compile(r"\u2699\ufe0f\s*(.+?)(?:\s*$)", re.MULTILINE)

# Season-only patterns in release names (S02, S2, Season.2) — no episode part.
# Used for season-pack detection together with PTN's absence of an 'episode' key.
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

# Strips the ``realdebrid=<value>`` segment from pipe-separated Torrentio opts
# so that scrape-pipeline queries return all results, not just RD-cached ones.
_DEBRID_OPT_RE = re.compile(r"realdebrid=[^|]*")

# Dub / Dual Audio detection — used to tag results with "Dubbed" or "Dual Audio".
# _DUB_RE matches "DUB" and "DUBBED" but not "DUBLIN" (word boundary prevents it).
_DUB_RE = re.compile(r"\bDUB(?:BED)?\b", re.IGNORECASE)
# _DUAL_AUDIO_RE requires "AUDIO" after "DUAL" (with optional separator) to avoid
# false positives in show titles like "Dual.Survival.S01E01".
_DUAL_AUDIO_RE = re.compile(r"\bDUAL[\.\s-]?AUDIO\b", re.IGNORECASE)

# Cached-in-RD indicator.  When the Torrentio opts URL includes an RD API key,
# cached streams are tagged with ⚡ in the ``name`` field (e.g. "⚡ Torrentio\n1080p")
# or with "[RD+]" / "RD+" in the title/name.  We check both fields.
_CACHED_RE = re.compile(r"\u26a1|RD\+|\[RD\+\]", re.IGNORECASE)

# Known language tokens that appear in torrent names (non-exhaustive but covers
# the common cases we encounter in Torrentio output).
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


class TorrentioError(Exception):
    """Raised when Torrentio returns an unexpected API-level error."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


# ---------------------------------------------------------------------------
# Output schema
# ---------------------------------------------------------------------------


class TorrentioResult(BaseModel):
    """A single parsed result from the Torrentio addon API."""

    info_hash: str
    title: str  # raw release name (first line of the Torrentio title field)
    resolution: str | None = None
    codec: str | None = None
    quality: str | None = None  # BluRay, WEB-DL, WEBRip, HDTV, …
    release_group: str | None = None
    size_bytes: int | None = None
    seeders: int | None = None
    source_tracker: str | None = None
    season: int | None = None
    episode: int | None = None
    is_season_pack: bool = False
    file_idx: int | None = None
    languages: list[str] = []
    cached: bool = False


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class TorrentioClient:
    """Async scraper client for the Torrentio Stremio addon.

    This client is intentionally stateless — no persistent HTTP session is kept
    between calls so that config changes (base_url, opts) are picked up without
    restart.  Each public method opens and closes its own httpx client.

    All public methods swallow network/API failures, log them, and return an
    empty list so that a Torrentio outage never crashes the queue manager.
    """

    # ------------------------------------------------------------------
    # URL construction
    # ------------------------------------------------------------------

    def _build_base_url(self, *, include_debrid_key: bool = True) -> str:
        """Return the Torrentio base URL, injecting opts as a path segment.

        If ``opts`` is set (e.g. ``"sort=seeders|qualityfilter=4k"``), the URL
        becomes ``{base_url}/{opts}``.  Otherwise it is just ``{base_url}``.

        Args:
            include_debrid_key: When ``False``, the ``realdebrid=<key>`` segment
                is stripped from opts before building the URL.  This causes
                Torrentio to return all results rather than only RD-cached ones,
                which is the correct behaviour for the scrape pipeline.
        """
        cfg = settings.scrapers.torrentio
        base = cfg.base_url.rstrip("/")
        opts = cfg.opts.strip("/").strip()
        if opts and not include_debrid_key:
            opts = _DEBRID_OPT_RE.sub("", opts)
            opts = re.sub(r"\|{2,}", "|", opts).strip("|")
        if opts:
            return f"{base}/{opts}"
        return base

    def _build_client(self, *, include_debrid_key: bool = True) -> httpx.AsyncClient:
        """Create a new httpx.AsyncClient pointed at the Torrentio addon.

        Args:
            include_debrid_key: Forwarded to :meth:`_build_base_url`.
        """
        cfg = settings.scrapers.torrentio
        return httpx.AsyncClient(
            base_url=self._build_base_url(include_debrid_key=include_debrid_key),
            timeout=cfg.timeout_seconds,
            headers={"User-Agent": "vibeDebrid/0.1"},
            follow_redirects=True,
        )

    # ------------------------------------------------------------------
    # Public scraping methods
    # ------------------------------------------------------------------

    async def scrape_movie(
        self, imdb_id: str, *, include_debrid_key: bool = True
    ) -> list[TorrentioResult]:
        """Scrape Torrentio for movie results.

        Args:
            imdb_id: The IMDB ID, e.g. ``"tt12345678"``.
            include_debrid_key: When ``False``, strips the ``realdebrid=`` key
                from opts so Torrentio returns all results, not just cached ones.

        Returns:
            Parsed results up to ``max_results``, or an empty list on failure.
        """
        if not settings.scrapers.torrentio.enabled:
            logger.debug("scrape_movie: Torrentio disabled, skipping")
            return []

        path = f"/stream/movie/{imdb_id}.json"
        logger.debug("scrape_movie: imdb_id=%s path=%s", imdb_id, path)
        return await self._query(path, include_debrid_key=include_debrid_key)

    async def scrape_episode(
        self,
        imdb_id: str,
        season: int,
        episode: int,
        *,
        include_debrid_key: bool = True,
    ) -> list[TorrentioResult]:
        """Scrape Torrentio for a specific episode with a two-level fallback.

        The fallback chain fixes the primary bug from CLI Debrid where running
        seasons returned 0 results at the episode level and were silently dropped.

        Fallback order:
          1. Episode query  → ``/stream/series/{imdb_id}:{season}:{episode}.json``
          2. Season query   → ``/stream/series/{imdb_id}:{season}:1.json``
             (episode 1 is used as the season anchor; results are season packs)

        Args:
            imdb_id: The IMDB ID, e.g. ``"tt12345678"``.
            season:  Season number (1-based).
            episode: Episode number (1-based).
            include_debrid_key: When ``False``, strips the ``realdebrid=`` key
                from opts so Torrentio returns all results, not just cached ones.

        Returns:
            Parsed results up to ``max_results``, or an empty list on failure.
        """
        if not settings.scrapers.torrentio.enabled:
            logger.debug("scrape_episode: Torrentio disabled, skipping")
            return []

        # Step 1 — episode-level query
        ep_path = f"/stream/series/{imdb_id}:{season}:{episode}.json"
        logger.debug(
            "scrape_episode: step=1 (episode) imdb_id=%s S%02dE%02d",
            imdb_id,
            season,
            episode,
        )
        results = await self._query(ep_path, include_debrid_key=include_debrid_key)
        if results:
            logger.debug(
                "scrape_episode: step=1 succeeded with %d results S%02dE%02d",
                len(results),
                season,
                episode,
            )
            return results

        # Step 2 — season-level query (episode 1 as anchor to get the season list)
        logger.info(
            "scrape_episode: step=1 returned 0 results for %s S%02dE%02d, "
            "trying season query",
            imdb_id,
            season,
            episode,
        )
        season_path = f"/stream/series/{imdb_id}:{season}:1.json"
        results = await self._query(season_path, include_debrid_key=include_debrid_key)
        if results:
            logger.info(
                "scrape_episode: step=2 (season) succeeded with %d results "
                "for %s S%02d",
                len(results),
                imdb_id,
                season,
            )
        else:
            logger.warning(
                "scrape_episode: both fallback levels returned 0 results "
                "for %s S%02dE%02d — content may not be available yet",
                imdb_id,
                season,
                episode,
            )
        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _query(
        self, path: str, *, include_debrid_key: bool = True
    ) -> list[TorrentioResult]:
        """Execute a single Torrentio addon request and parse the stream list.

        Args:
            path: URL path relative to the (optionally opts-prefixed) base URL,
                  e.g. ``"/stream/movie/tt12345678.json"``.
            include_debrid_key: Forwarded to :meth:`_build_client`.  Pass
                ``False`` from the scrape pipeline to obtain unfiltered results.

        Returns:
            List of parsed TorrentioResult objects, capped at ``max_results``.
            Returns an empty list on any network or API error.
        """
        max_results = settings.scrapers.torrentio.max_results
        t0 = time.monotonic()
        try:
            async with self._build_client(include_debrid_key=include_debrid_key) as client:
                response = await client.get(path)
        except httpx.ConnectError as exc:
            logger.warning(
                "torrentio._query: connection refused for path=%s (%s)", path, exc
            )
            return []
        except httpx.TimeoutException as exc:
            logger.warning(
                "torrentio._query: request timed out for path=%s (%s)", path, exc
            )
            return []
        except httpx.RequestError as exc:
            logger.warning(
                "torrentio._query: network error for path=%s (%s)", path, exc
            )
            return []

        elapsed_ms = int((time.monotonic() - t0) * 1000)

        if response.status_code == 429:
            logger.warning(
                "torrentio._query: rate limited (429) for path=%s", path
            )
            return []

        if response.status_code >= 500:
            logger.error(
                "torrentio._query: server error %d for path=%s body=%s",
                response.status_code,
                path,
                response.text[:200],
            )
            return []

        if not response.is_success:
            logger.error(
                "torrentio._query: unexpected status %d for path=%s",
                response.status_code,
                path,
            )
            return []

        try:
            data: dict[str, Any] = response.json()
        except Exception as exc:
            logger.error(
                "torrentio._query: malformed JSON for path=%s body=%s (%s)",
                path,
                response.text[:200],
                exc,
            )
            return []

        raw_streams = data.get("streams")
        if not isinstance(raw_streams, list):
            # Torrentio returns {"streams": []} for no results — a missing key
            # or wrong type is unexpected and worth logging.
            if raw_streams is not None:
                logger.error(
                    "torrentio._query: unexpected 'streams' type %s for path=%s",
                    type(raw_streams).__name__,
                    path,
                )
            logger.debug(
                "torrentio._query: path=%s elapsed=%dms results=0", path, elapsed_ms
            )
            return []

        results: list[TorrentioResult] = []
        for stream in raw_streams:
            parsed = self._parse_stream(stream)
            if parsed is not None:
                results.append(parsed)
            if len(results) >= max_results:
                break

        logger.debug(
            "torrentio._query: path=%s elapsed=%dms raw=%d parsed=%d",
            path,
            elapsed_ms,
            len(raw_streams),
            len(results),
        )
        return results

    def _parse_stream(self, stream: dict[str, Any]) -> TorrentioResult | None:
        """Parse a single stream entry from the Torrentio response.

        Returns ``None`` if the entry is missing required fields or is otherwise
        unparseable — callers should skip ``None`` values.

        Args:
            stream: A single element from the ``streams`` list in the response.

        Returns:
            A populated TorrentioResult, or None if the entry should be skipped.
        """
        # --- Cached-in-RD detection ---
        # When the opts URL includes an RD API key, Torrentio marks cached
        # streams with ⚡ in the name field or "[RD+]" in the title/name.
        stream_name = stream.get("name", "")
        raw_title_full = stream.get("title", "")
        cached = bool(
            _CACHED_RE.search(stream_name) or _CACHED_RE.search(raw_title_full)
        )

        # --- Required fields ---
        # Primary: standard Stremio protocol field (camelCase).
        # Fallback: some Torrentio forks / proxies use all-lowercase keys.
        info_hash = stream.get("infoHash") or stream.get("infohash")
        if not info_hash or not isinstance(info_hash, str):
            # Last resort: extract hash from behaviorHints.bingeGroup
            # (format "torrentio|<40-hex-char-hash>")
            info_hash = self._extract_hash_from_hints(stream)
        if not info_hash:
            logger.debug("_parse_stream: skipping entry with missing infoHash")
            return None

        raw_title = raw_title_full
        if not raw_title or not isinstance(raw_title, str):
            logger.debug(
                "_parse_stream: skipping entry hash=%s with missing title", info_hash
            )
            return None

        # --- Split release name from metadata line ---
        lines = raw_title.split("\n", 1)
        release_name = lines[0].strip()
        meta_line = lines[1].strip() if len(lines) > 1 else ""

        if not release_name:
            logger.debug(
                "_parse_stream: skipping entry hash=%s with empty release name",
                info_hash,
            )
            return None

        # --- Metadata from emoji-encoded line ---
        size_bytes = self._parse_size(meta_line)
        seeders = self._parse_seeders(meta_line)
        source_tracker = self._parse_source(meta_line)

        # --- Parse release name with PTN ---
        ptn_data: dict[str, Any] = {}
        try:
            ptn_data = PTN.parse(release_name) or {}
        except Exception as exc:
            logger.debug(
                "_parse_stream: PTN failed for release=%r (%s)", release_name, exc
            )

        resolution: str | None = ptn_data.get("resolution")
        codec: str | None = ptn_data.get("codec")
        quality: str | None = ptn_data.get("quality")
        release_group: str | None = ptn_data.get("group")
        ptn_season: int | None = ptn_data.get("season")
        ptn_episode: int | None = ptn_data.get("episode")

        # Fallback: PTN doesn't handle dash-separated anime notation like "S2 - 06"
        if ptn_episode is None:
            m = _SEASON_DASH_EP_RE.search(release_name)
            if m:
                ptn_season = int(m.group(1))
                ptn_episode = int(m.group(2))

        # --- Season pack detection ---
        # PTN does NOT emit a 'season' key when it cannot find an episode number
        # — instead it folds e.g. "S02" into the title string.  A result is a
        # season pack if PTN found no episode, AND either:
        #   (a) the release name matches our season-only pattern, OR
        #   (b) the release name contains "complete" / "season" keywords.
        is_season_pack = False
        if ptn_episode is None:
            has_season_marker = bool(_SEASON_ONLY_RE.search(release_name))
            has_complete_marker = bool(_COMPLETE_RE.search(release_name))
            if has_season_marker or has_complete_marker:
                is_season_pack = True

        # --- Language detection ---
        languages = self._parse_languages(release_name, ptn_data)

        # --- fileIdx ---
        file_idx_raw = stream.get("fileIdx")
        file_idx: int | None = (
            int(file_idx_raw) if isinstance(file_idx_raw, (int, float)) else None
        )

        return TorrentioResult(
            info_hash=info_hash.lower(),
            title=release_name,
            resolution=resolution,
            codec=codec,
            quality=quality,
            release_group=release_group,
            size_bytes=size_bytes,
            seeders=seeders,
            source_tracker=source_tracker,
            season=ptn_season,
            episode=ptn_episode,
            is_season_pack=is_season_pack,
            file_idx=file_idx,
            languages=languages,
            cached=cached,
        )

    # ------------------------------------------------------------------
    # Metadata extraction helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_hash_from_hints(stream: dict[str, Any]) -> str | None:
        """Extract info hash from behaviorHints.bingeGroup as a last resort.

        The bingeGroup format is ``"torrentio|<40-hex-char-hash>"``.
        Returns the lowercase hex hash, or None if not extractable.
        """
        hints = stream.get("behaviorHints")
        if not isinstance(hints, dict):
            return None
        binge = hints.get("bingeGroup")
        if not isinstance(binge, str):
            return None
        parts = binge.split("|")
        for part in parts:
            cleaned = part.strip().lower()
            if len(cleaned) == 40 and all(c in "0123456789abcdef" for c in cleaned):
                return cleaned
        return None

    def _parse_size(self, meta_line: str) -> int | None:
        """Parse the 💾 size annotation from a Torrentio metadata line.

        Args:
            meta_line: The second line of a Torrentio stream title, e.g.
                       ``"👤 823 💾 3.45 GB ⚙️ BIT-HDTV"``.

        Returns:
            Size in bytes, or None if not found or unparseable.
        """
        if not meta_line:
            return None
        match = _SIZE_RE.search(meta_line)
        if not match:
            return None
        try:
            value = float(match.group(1))
            unit = match.group(2).upper()
        except (ValueError, IndexError):
            return None

        if unit == "MB":
            return int(value * 1024 * 1024)
        if unit == "GB":
            return int(value * 1024 * 1024 * 1024)
        if unit == "TB":
            return int(value * 1024 * 1024 * 1024 * 1024)
        return None  # unreachable given the regex, but satisfies type checker

    def _parse_seeders(self, meta_line: str) -> int | None:
        """Parse the 👤 seeder count from a Torrentio metadata line.

        Args:
            meta_line: The second line of a Torrentio stream title.

        Returns:
            Seeder count as an integer, or None if not found.
        """
        if not meta_line:
            return None
        match = _SEEDERS_RE.search(meta_line)
        if not match:
            return None
        try:
            return int(match.group(1))
        except ValueError:
            return None

    def _parse_source(self, meta_line: str) -> str | None:
        """Parse the ⚙️ tracker/source annotation from a Torrentio metadata line.

        Args:
            meta_line: The second line of a Torrentio stream title.

        Returns:
            The tracker/source name, or None if not found.
        """
        if not meta_line:
            return None
        match = _SOURCE_RE.search(meta_line)
        if not match:
            return None
        return match.group(1).strip() or None

    def _parse_languages(
        self, release_name: str, ptn_data: dict[str, Any]
    ) -> list[str]:
        """Extract language tags from a release name and PTN output.

        PTN does not reliably expose a 'language' key — language tokens often
        end up in the PTN 'title', 'excess', or are simply not extracted.  We
        scan the raw release name for known language tokens instead.

        Args:
            release_name: The first line of the Torrentio title field.
            ptn_data:     The dict returned by ``PTN.parse(release_name)``.

        Returns:
            A deduplicated list of language names found, e.g. ``["French"]``.
            English is not included — it is assumed when no other language is
            present.
        """
        upper_name = release_name.upper()
        found: list[str] = []
        seen: set[str] = set()
        for token, lang_name in _LANGUAGE_TOKENS.items():
            if token.upper() in upper_name and lang_name not in seen:
                found.append(lang_name)
                seen.add(lang_name)
        # Cyrillic script → Russian (catches titles written in Russian that lack
        # any English-language tag).
        if _CYRILLIC_RE.search(release_name) and "Russian" not in seen:
            found.append("Russian")
            seen.add("Russian")
        # Short ISO/scene abbreviations (word-boundary matched).
        for abbrev, lang_name in _LANGUAGE_ABBREV_TOKENS.items():
            if lang_name not in seen and _LANGUAGE_ABBREV_RES[abbrev].search(release_name):
                found.append(lang_name)
                seen.add(lang_name)
        # Dub / Dual Audio detection
        if _DUB_RE.search(release_name) and "Dubbed" not in seen:
            found.append("Dubbed")
            seen.add("Dubbed")
        if _DUAL_AUDIO_RE.search(release_name) and "Dual Audio" not in seen:
            found.append("Dual Audio")
            seen.add("Dual Audio")
        return found


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

torrentio_client = TorrentioClient()
