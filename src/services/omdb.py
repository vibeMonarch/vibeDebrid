"""Async client for the Open Movie Database (OMDb) API.

Used to fetch IMDb ratings, Rotten Tomatoes scores, and Metascore for the
Discover and Search pages (issues #6 + #7).

Design notes:
- Module-level singleton ``omdb_client`` is imported by routes and services.
- In-memory cache keyed by IMDB ID with a configurable TTL (default 7 days)
  avoids hammering the free-tier API quota.
- All public methods swallow network/API failures, log them, and return None
  so that an OMDb outage never crashes the queue or routes.
- The OMDb v1 API uses a simple ``?apikey=`` query-param style — no Bearer
  token needed.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx
from pydantic import BaseModel

from src.config import settings
from src.services.http_client import CircuitOpenError, get_circuit_breaker, get_client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Response model
# ---------------------------------------------------------------------------


class OmdbRatings(BaseModel):
    """Parsed ratings extracted from an OMDb API response."""

    imdb_rating: float | None = None
    imdb_votes: str | None = None
    rt_score: int | None = None
    metascore: int | None = None


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class OmdbClient:
    """Async client for the OMDb API with in-memory caching and circuit breaking.

    Args:
        None — configuration is read from the module-level ``settings`` singleton
        at call time so that runtime config changes take effect without a restart.
    """

    def __init__(self) -> None:
        # Cache: imdb_id -> (expires_at_monotonic, OmdbRatings)
        self._cache: dict[str, tuple[float, OmdbRatings]] = {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _is_configured(self) -> bool:
        """Return True when OMDb is enabled and an API key is present."""
        cfg = settings.omdb
        return cfg.enabled and bool(cfg.api_key)

    async def _get_client(self) -> httpx.AsyncClient:
        """Return the pooled httpx client for OMDb."""
        cfg = settings.omdb
        return await get_client(
            "omdb",
            cfg.base_url,
            timeout=float(cfg.timeout_seconds),
        )

    def _cache_get(self, imdb_id: str) -> OmdbRatings | None:
        """Return cached ratings if still within TTL, else None."""
        entry = self._cache.get(imdb_id)
        if entry is None:
            return None
        expires_at, ratings = entry
        if time.monotonic() < expires_at:
            return ratings
        # Expired — remove stale entry
        del self._cache[imdb_id]
        return None

    def _cache_set(self, imdb_id: str, ratings: OmdbRatings) -> None:
        """Store ratings in the cache with the configured TTL."""
        ttl = settings.omdb.cache_hours * 3600
        expires_at = time.monotonic() + ttl
        self._cache[imdb_id] = (expires_at, ratings)

    @staticmethod
    def _parse_ratings(data: dict[str, Any]) -> OmdbRatings:
        """Parse an OMDb API response dict into an OmdbRatings object.

        Args:
            data: The parsed JSON response from OMDb.

        Returns:
            OmdbRatings with any available fields populated; missing or "N/A"
            values are represented as None.
        """
        # IMDb rating — "8.5" or "N/A"
        imdb_rating: float | None = None
        raw_rating = data.get("imdbRating", "N/A")
        if raw_rating and raw_rating != "N/A":
            try:
                imdb_rating = float(raw_rating)
            except ValueError:
                pass

        # IMDb votes — keep as a display string, e.g. "1,234,567"
        imdb_votes: str | None = None
        raw_votes = data.get("imdbVotes", "N/A")
        if raw_votes and raw_votes != "N/A":
            imdb_votes = raw_votes

        # Rotten Tomatoes — search the Ratings array for Source == "Rotten Tomatoes"
        rt_score: int | None = None
        for entry in data.get("Ratings", []):
            if entry.get("Source") == "Rotten Tomatoes":
                value = entry.get("Value", "")
                if value.endswith("%"):
                    try:
                        rt_score = int(value[:-1])
                    except ValueError:
                        pass
                break

        # Metascore — "74" or "N/A"
        metascore: int | None = None
        raw_meta = data.get("Metascore", "N/A")
        if raw_meta and raw_meta != "N/A":
            try:
                metascore = int(raw_meta)
            except ValueError:
                pass

        return OmdbRatings(
            imdb_rating=imdb_rating,
            imdb_votes=imdb_votes,
            rt_score=rt_score,
            metascore=metascore,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_ratings(self, imdb_id: str) -> OmdbRatings | None:
        """Fetch ratings for a title by its IMDb ID.

        Checks the in-memory cache first. On a cache miss, calls the OMDb API
        and stores the result. Returns None (without raising) on any error.

        Args:
            imdb_id: A valid IMDb ID, e.g. ``"tt0133093"``.

        Returns:
            An OmdbRatings instance, or None when OMDb is disabled/unconfigured
            or when a network/API error occurs.
        """
        if not self._is_configured():
            logger.debug("omdb: disabled or not configured, skipping ratings lookup for %s", imdb_id)
            return None

        # Cache hit
        cached = self._cache_get(imdb_id)
        if cached is not None:
            logger.debug("omdb: cache hit for %s", imdb_id)
            return cached

        breaker = get_circuit_breaker("omdb")
        try:
            await breaker.before_request()
        except CircuitOpenError:
            logger.warning("omdb: circuit breaker open, skipping ratings lookup for %s", imdb_id)
            return None

        try:
            client = await self._get_client()
            response = await client.get(
                "/",
                params={"i": imdb_id, "apikey": settings.omdb.api_key},
            )
            response.raise_for_status()
        except httpx.ConnectError as exc:
            logger.warning("omdb: connection error for %s: %s", imdb_id, exc)
            await breaker.record_failure()
            return None
        except httpx.TimeoutException as exc:
            logger.warning("omdb: timeout for %s: %s", imdb_id, exc)
            await breaker.record_failure()
            return None
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if status == 429:
                # Rate limit — do not count as a circuit-breaking failure
                logger.warning("omdb: rate limited (429) for %s", imdb_id)
                return None
            logger.error("omdb: HTTP %d for %s: %s", status, imdb_id, exc.response.text)
            await breaker.record_failure()
            return None
        except httpx.RequestError as exc:
            logger.warning("omdb: request error for %s: %s", imdb_id, exc)
            await breaker.record_failure()
            return None

        try:
            data: dict[str, Any] = response.json()
        except Exception as exc:
            logger.warning("omdb: invalid JSON for %s: %s", imdb_id, exc)
            await breaker.record_failure()
            return None

        # OMDb signals errors in-band with Response=="False"
        if data.get("Response") == "False":
            logger.warning("omdb: API error for %s: %s", imdb_id, data.get("Error", "unknown"))
            await breaker.record_success()  # Not a service failure — valid response
            return None

        await breaker.record_success()
        ratings = self._parse_ratings(data)
        self._cache_set(imdb_id, ratings)
        logger.debug(
            "omdb: ratings for %s — IMDb=%.1f RT=%s Meta=%s",
            imdb_id,
            ratings.imdb_rating or 0.0,
            ratings.rt_score,
            ratings.metascore,
        )
        return ratings

    async def test_connection(self) -> dict[str, Any] | None:
        """Test OMDb connectivity by fetching The Matrix (tt0133093).

        Returns:
            A dict with at least a ``"Title"`` key on success, or None when
            OMDb is disabled/unconfigured or a network error occurs.
        """
        if not self._is_configured():
            logger.info("omdb: test_connection skipped — not configured")
            return None

        breaker = get_circuit_breaker("omdb")
        try:
            await breaker.before_request()
        except CircuitOpenError:
            logger.warning("omdb: circuit breaker open during test_connection")
            return None

        try:
            client = await self._get_client()
            response = await client.get(
                "/",
                params={"i": "tt0133093", "apikey": settings.omdb.api_key},
            )
            response.raise_for_status()
            data: dict[str, Any] = response.json()
        except httpx.ConnectError as exc:
            logger.warning("omdb: test_connection connect error: %s", exc)
            await breaker.record_failure()
            return None
        except httpx.TimeoutException as exc:
            logger.warning("omdb: test_connection timeout: %s", exc)
            await breaker.record_failure()
            return None
        except httpx.HTTPStatusError as exc:
            logger.error("omdb: test_connection HTTP %d: %s", exc.response.status_code, exc.response.text)
            await breaker.record_failure()
            return None
        except httpx.RequestError as exc:
            logger.warning("omdb: test_connection request error: %s", exc)
            await breaker.record_failure()
            return None
        except Exception as exc:
            logger.warning("omdb: test_connection unexpected error: %s", exc)
            await breaker.record_failure()
            return None

        if data.get("Response") == "False":
            logger.warning("omdb: test_connection API error: %s", data.get("Error", "unknown"))
            return None

        await breaker.record_success()
        return data


# Module-level singleton — import ``omdb_client`` from here.
omdb_client = OmdbClient()
