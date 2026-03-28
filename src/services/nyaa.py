"""Async scraper wrapper for the Nyaa.si RSS feed (anime-focused torrent index).

Nyaa.si is a public torrent index primarily used for anime fansubs and raw
content.  No authentication is required.

Real-world quirks documented here:
- The API is an RSS feed, not a JSON endpoint.  Response is XML with a custom
  ``nyaa:`` namespace (URI: ``https://nyaa.si/xmlns/nyaa``).
- Results are sorted by seeders descending when ``s=seeders&o=desc`` is passed.
- Nyaa reports sizes in IEC binary units (KiB, MiB, GiB, TiB) as well as SI
  decimal units (KB, MB, GB, TB).  Both are supported.
- 75 results per page is the practical maximum.
- No IMDB or TMDB ID metadata is provided — all matching is title-based.
- The scraper is anime-optimised: the default category ``1_2`` returns only
  "Anime - English-translated" torrents.  Adjust ``category`` in config to
  broaden the scope.
"""

from __future__ import annotations

import logging
import re
import time
import xml.etree.ElementTree as ET
from typing import Any

import httpx
import PTN
from pydantic import BaseModel

from src.__version__ import __version__
from src.config import settings
from src.services.http_client import CircuitOpenError, get_circuit_breaker, get_client
from src.services.torrent_parser import (
    detect_season_pack,
    parse_episode_fallbacks,
    parse_languages,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Nyaa XML namespace
# ---------------------------------------------------------------------------

_NYAA_NS = "https://nyaa.si/xmlns/nyaa"

# ---------------------------------------------------------------------------
# Size parsing
# ---------------------------------------------------------------------------

_NYAA_SIZE_RE = re.compile(r"([\d.]+)\s*(TiB|GiB|MiB|KiB|TB|GB|MB|KB)", re.IGNORECASE)
_SIZE_MULTIPLIERS: dict[str, int] = {
    "kib": 1024,
    "mib": 1024 ** 2,
    "gib": 1024 ** 3,
    "tib": 1024 ** 4,
    "kb": 1000,
    "mb": 1000 ** 2,
    "gb": 1000 ** 3,
    "tb": 1000 ** 4,
}


def _parse_nyaa_size(size_str: str) -> int | None:
    """Parse a Nyaa size string like ``"1.4 GiB"`` into bytes.

    Supports both IEC binary (KiB/MiB/GiB/TiB) and SI decimal (KB/MB/GB/TB)
    suffixes.

    Args:
        size_str: Raw size string from the ``<nyaa:size>`` element.

    Returns:
        Size in bytes as an integer, or ``None`` if the string cannot be parsed.
    """
    if not size_str:
        return None
    match = _NYAA_SIZE_RE.search(size_str)
    if not match:
        return None
    try:
        value = float(match.group(1))
        unit = match.group(2).lower()
    except (ValueError, IndexError):
        return None
    multiplier = _SIZE_MULTIPLIERS.get(unit)
    if multiplier is None:
        return None
    return int(value * multiplier)


# ---------------------------------------------------------------------------
# Output schema
# ---------------------------------------------------------------------------


class NyaaResult(BaseModel):
    """A single parsed result from the Nyaa.si RSS feed.

    Field names intentionally mirror ``TorrentioResult`` and ``ZileanResult``
    so the scrape pipeline can handle all result types uniformly via the
    ``_AnyResult`` union type.
    """

    info_hash: str
    title: str  # raw torrent title from the RSS <title> element
    resolution: str | None = None
    codec: str | None = None
    quality: str | None = None  # WEB, BluRay, WEBRip, etc.
    release_group: str | None = None
    size_bytes: int | None = None
    seeders: int | None = None
    source_tracker: str | None = None  # Always "Nyaa"
    season: int | None = None
    episode: int | None = None
    is_season_pack: bool = False
    file_idx: int | None = None  # Always None — Nyaa RSS has no file-level index
    languages: list[str] = []


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class NyaaClient:
    """Async scraper client for the Nyaa.si RSS torrent feed.

    Uses the pooled ``httpx.AsyncClient`` from ``http_client.py`` and the
    shared circuit breaker pattern used by all other scrapers.

    All public methods swallow network/API failures, log them, and return an
    empty list so that a Nyaa outage never crashes the queue manager.
    """

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _get_client(self) -> httpx.AsyncClient:
        """Return the pooled httpx.AsyncClient for Nyaa."""
        cfg = settings.scrapers.nyaa
        return await get_client(
            "nyaa",
            cfg.base_url.rstrip("/"),
            timeout=cfg.timeout_seconds,
            headers={"User-Agent": f"vibeDebrid/{__version__}"},
            follow_redirects=True,
        )

    # ------------------------------------------------------------------
    # Public search method
    # ------------------------------------------------------------------

    async def search(
        self,
        query: str,
        *,
        category: str | None = None,
        filter_level: int | None = None,
    ) -> list[NyaaResult]:
        """Search Nyaa.si via RSS for torrents matching *query*.

        Sends a GET request to ``/?page=rss&q=<query>&c=<category>&f=<filter>
        &s=seeders&o=desc``.  XML is parsed with :mod:`xml.etree.ElementTree`.

        Args:
            query: Title search string.
            category: Nyaa category code (e.g. ``"1_2"``).  Falls back to the
                configured ``settings.scrapers.nyaa.category`` when ``None``.
            filter_level: Nyaa filter level (0=all, 1=no remakes, 2=trusted).
                Falls back to ``settings.scrapers.nyaa.filter`` when ``None``.

        Returns:
            Parsed ``NyaaResult`` objects, or an empty list on any failure.
        """
        cfg = settings.scrapers.nyaa
        if not cfg.enabled:
            logger.debug("nyaa.search: Nyaa disabled, skipping")
            return []

        eff_category = category if category is not None else cfg.category
        eff_filter = filter_level if filter_level is not None else cfg.filter

        params: dict[str, Any] = {
            "page": "rss",
            "q": query,
            "c": eff_category,
            "f": str(eff_filter),
            "s": "seeders",
            "o": "desc",
        }

        logger.debug(
            "nyaa.search: query=%r category=%s filter=%s",
            query,
            eff_category,
            eff_filter,
        )

        breaker = get_circuit_breaker("nyaa")
        try:
            await breaker.before_request()
        except CircuitOpenError:
            logger.warning("nyaa.search: circuit open, skipping query=%r", query)
            return []

        t0 = time.monotonic()
        try:
            client = await self._get_client()
            response = await client.get("/", params=params)
        except httpx.ConnectError as exc:
            await breaker.record_failure()
            logger.warning(
                "nyaa.search: connection refused — Nyaa.si may be down. "
                "query=%r (%s)",
                query,
                exc,
            )
            return []
        except httpx.TimeoutException as exc:
            await breaker.record_failure()
            logger.warning(
                "nyaa.search: request timed out. query=%r (%s)", query, exc
            )
            return []
        except httpx.RequestError as exc:
            await breaker.record_failure()
            logger.warning(
                "nyaa.search: network error. query=%r (%s)", query, exc
            )
            return []

        elapsed_ms = int((time.monotonic() - t0) * 1000)

        if response.status_code == 403:
            await breaker.record_failure()
            logger.error(
                "nyaa.search: access forbidden (403) — possible IP block. query=%r",
                query,
            )
            return []

        if response.status_code == 429:
            # Rate limiting — do not penalise the circuit breaker.
            logger.warning(
                "nyaa.search: rate limited (429). query=%r", query
            )
            return []

        if response.status_code >= 500:
            await breaker.record_failure()
            logger.error(
                "nyaa.search: server error %d. query=%r body=%s",
                response.status_code,
                query,
                response.text[:200],
            )
            return []

        if not response.is_success:
            await breaker.record_failure()
            logger.error(
                "nyaa.search: unexpected status %d. query=%r",
                response.status_code,
                query,
            )
            return []

        await breaker.record_success()

        # Size guard: stdlib ET does not resolve external entities, but internal
        # entity expansion is still possible (billion-laughs variant).  Cap at
        # 2 MB — Nyaa RSS pages are well under 1 MB in practice.
        text = response.text
        if len(text) > 2 * 1024 * 1024:
            logger.warning(
                "nyaa.search: RSS response too large (%d bytes), skipping query=%r",
                len(text),
                query,
            )
            return []

        try:
            root = ET.fromstring(text)
        except ET.ParseError as exc:
            logger.error(
                "nyaa.search: XML parse error. query=%r body=%s (%s)",
                query,
                text[:200],
                exc,
            )
            return []

        # The RSS feed wraps items in <channel>.
        channel = root.find("channel")
        if channel is None:
            logger.debug(
                "nyaa.search: no <channel> element in response. query=%r elapsed=%dms",
                query,
                elapsed_ms,
            )
            return []

        items = channel.findall("item")
        max_results = cfg.max_results
        results: list[NyaaResult] = []

        for item_el in items:
            parsed = self._parse_item(item_el)
            if parsed is not None:
                results.append(parsed)
            if len(results) >= max_results:
                break

        logger.debug(
            "nyaa.search: query=%r elapsed=%dms raw=%d parsed=%d",
            query,
            elapsed_ms,
            len(items),
            len(results),
        )
        return results

    # ------------------------------------------------------------------
    # Internal parsing helpers
    # ------------------------------------------------------------------

    def _parse_item(self, item_el: ET.Element) -> NyaaResult | None:
        """Parse a single ``<item>`` element from the Nyaa RSS feed.

        Returns ``None`` if the entry is missing required fields (info_hash or
        title) or is otherwise unparseable.  Individual field failures do not
        discard the whole item — only the affected field is set to ``None``.

        Args:
            item_el: An ``<item>`` XML element from the Nyaa RSS channel.

        Returns:
            A populated ``NyaaResult``, or ``None`` if the item should be skipped.
        """
        try:
            return self._parse_item_inner(item_el)
        except (AttributeError, TypeError, ValueError, KeyError) as exc:
            logger.warning(
                "nyaa: failed to parse RSS item: %s", exc,
            )
            return None

    def _parse_item_inner(self, item_el: ET.Element) -> NyaaResult | None:
        """Inner parse logic — may raise; the outer _parse_item catches all errors.

        Args:
            item_el: An ``<item>`` XML element.

        Returns:
            A populated ``NyaaResult``, or ``None`` if required fields are absent.
        """
        # --- Required: info_hash ---
        info_hash_el = item_el.find(f"{{{_NYAA_NS}}}infoHash")
        info_hash = info_hash_el.text.strip() if info_hash_el is not None and info_hash_el.text else None
        if not info_hash:
            logger.debug("nyaa._parse_item: skipping entry with missing infoHash")
            return None

        # --- Required: title ---
        title_el = item_el.find("title")
        title = title_el.text.strip() if title_el is not None and title_el.text else None
        if not title:
            logger.debug(
                "nyaa._parse_item: skipping entry hash=%s with missing title",
                info_hash,
            )
            return None

        # --- Optional: seeders ---
        seeders: int | None = None
        seeders_el = item_el.find(f"{{{_NYAA_NS}}}seeders")
        if seeders_el is not None and seeders_el.text:
            try:
                seeders = int(seeders_el.text.strip())
            except ValueError:
                pass

        # --- Optional: size ---
        size_bytes: int | None = None
        size_el = item_el.find(f"{{{_NYAA_NS}}}size")
        if size_el is not None and size_el.text:
            size_bytes = _parse_nyaa_size(size_el.text.strip())

        # --- Parse title with PTN ---
        ptn_data: dict[str, Any] = {}
        try:
            ptn_data = PTN.parse(title) or {}
        except (ValueError, TypeError, KeyError) as exc:
            logger.debug(
                "nyaa._parse_item: PTN failed for title=%r (%s)", title, exc
            )

        resolution: str | None = ptn_data.get("resolution")
        codec: str | None = ptn_data.get("codec")
        quality: str | None = ptn_data.get("quality")
        release_group: str | None = ptn_data.get("group")

        ptn_season: int | None = ptn_data.get("season")
        if isinstance(ptn_season, list):
            ptn_season = ptn_season[0] if ptn_season else None

        ptn_episode: int | None = ptn_data.get("episode")
        if isinstance(ptn_episode, list):
            ptn_episode = ptn_episode[0] if ptn_episode else None

        # --- Anime / non-standard episode fallback chain ---
        ptn_season, ptn_episode, _is_anime_batch = parse_episode_fallbacks(
            title, ptn_season, ptn_episode
        )

        # --- Season pack detection ---
        is_season_pack = detect_season_pack(title, ptn_episode, _is_anime_batch)

        # --- Language detection ---
        languages = parse_languages(title)

        return NyaaResult(
            info_hash=info_hash.lower(),
            title=title,
            resolution=resolution,
            codec=codec,
            quality=quality,
            release_group=release_group,
            size_bytes=size_bytes,
            seeders=seeders,
            source_tracker="Nyaa",
            season=ptn_season,
            episode=ptn_episode,
            is_season_pack=is_season_pack,
            file_idx=None,
            languages=languages,
        )


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

nyaa_client = NyaaClient()
