"""Async client for The Movie Database (TMDB) API.

Used by the Discovery feature to browse trending/popular content and resolve
external IDs (IMDb) for items being added to the queue.

Design notes:
- Stateless client: each public method opens and closes its own httpx session
  so that config changes (api_key, base_url, timeout) take effect without a
  restart.
- All public methods swallow network/API failures, log them, and return an
  empty result so that a TMDB outage never crashes the queue or routes.
- The TMDB v3 API uses Bearer token authentication (api_key is actually a
  read-access token for v3 calls when passed as Authorization header).
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
# In-memory TTL cache for TMDB detail responses.
# Keys are (method_name, tmdb_id[, season_number]), values are (expire_time, result).
# ---------------------------------------------------------------------------

_detail_cache: dict[tuple, tuple[float, Any]] = {}
_CACHE_TTL = 7200  # 2 hours
_CACHE_MAX_SIZE = 500


def _cache_get(key: tuple) -> Any | None:
    """Return cached value if present and not expired, else None."""
    entry = _detail_cache.get(key)
    if entry is None:
        return None
    expire_time, value = entry
    if time.monotonic() > expire_time:
        _detail_cache.pop(key, None)
        return None
    return value


def _cache_set(key: tuple, value: Any) -> None:
    """Store a value in the cache with TTL. Evict oldest entry if at capacity.

    Safe without locking: runs in the asyncio event loop with no await points,
    so the min/pop/insert sequence executes atomically.
    """
    if len(_detail_cache) >= _CACHE_MAX_SIZE:
        oldest_key = min(_detail_cache, key=lambda k: _detail_cache[k][0])
        _detail_cache.pop(oldest_key, None)
    _detail_cache[key] = (time.monotonic() + _CACHE_TTL, value)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


ISO_639_1_TO_LANGUAGE: dict[str, str] = {
    "en": "English",
    "ja": "Japanese",
    "ko": "Korean",
    "zh": "Chinese",
    "fr": "French",
    "de": "German",
    "es": "Spanish",
    "pt": "Portuguese",
    "it": "Italian",
    "nl": "Dutch",
    "ru": "Russian",
}


def iso_to_language_name(code: str | None) -> str | None:
    """Convert ISO 639-1 code to the language name used in torrent parsing."""
    if code is None:
        return None
    return ISO_639_1_TO_LANGUAGE.get(code.lower())


class TmdbItem(BaseModel):
    """A single movie or TV show item from TMDB."""

    tmdb_id: int
    title: str
    year: int | None = None
    media_type: str  # "movie" or "tv"
    overview: str = ""
    poster_path: str | None = None
    vote_average: float = 0.0
    imdb_id: str | None = None  # only populated when external IDs are fetched
    original_language: str | None = None
    original_title: str | None = None  # original_title (movie) or original_name (tv)


class TmdbExternalIds(BaseModel):
    """External identifiers for a TMDB item."""

    imdb_id: str | None = None
    tvdb_id: int | None = None


class TmdbSearchResult(BaseModel):
    """Paginated search results from TMDB."""

    items: list[TmdbItem]
    total_results: int = 0
    page: int = 1
    total_pages: int = 1


class TmdbSeasonInfo(BaseModel):
    """Season summary from a TMDB show detail response."""

    season_number: int
    name: str = ""
    episode_count: int = 0
    air_date: str | None = None
    overview: str = ""
    poster_path: str | None = None


class TmdbEpisodeAirInfo(BaseModel):
    season_number: int
    episode_number: int
    air_date: str | None = None


class TmdbCastMember(BaseModel):
    """A single cast member from TMDB credits."""

    name: str
    character: str = ""
    profile_path: str | None = None
    order: int = 999


class TmdbCrewMember(BaseModel):
    """A single crew member from TMDB credits."""

    name: str
    job: str
    department: str = ""
    profile_path: str | None = None


class TmdbCredits(BaseModel):
    """Cast and crew credits from TMDB append_to_response=credits."""

    cast: list[TmdbCastMember] = []
    crew: list[TmdbCrewMember] = []


class TmdbShowDetail(BaseModel):
    """Full show details from TMDB /tv/{id} endpoint."""

    tmdb_id: int
    title: str
    year: int | None = None
    overview: str = ""
    tagline: str = ""
    poster_path: str | None = None
    backdrop_path: str | None = None
    status: str = ""  # "Returning Series", "Ended", "Canceled", etc.
    vote_average: float = 0.0
    number_of_seasons: int = 0
    seasons: list[TmdbSeasonInfo] = []
    imdb_id: str | None = None
    tvdb_id: int | None = None
    genres: list[dict] = []
    next_episode_to_air: TmdbEpisodeAirInfo | None = None
    last_episode_to_air: TmdbEpisodeAirInfo | None = None
    original_language: str | None = None
    original_title: str | None = None  # original_name field from TMDB
    credits: TmdbCredits | None = None


class TmdbEpisodeInfo(BaseModel):
    """Episode info from a TMDB season detail response."""

    episode_number: int
    name: str = ""
    air_date: str | None = None
    overview: str = ""
    still_path: str | None = None  # TMDB episode still image path


class TmdbSeasonDetail(BaseModel):
    """Detailed season info including episodes."""

    season_number: int
    name: str = ""
    episodes: list[TmdbEpisodeInfo] = []


class TmdbMovieDetail(BaseModel):
    """Full movie details from TMDB /movie/{id} endpoint."""

    tmdb_id: int
    title: str
    year: int | None = None
    overview: str = ""
    tagline: str = ""
    poster_path: str | None = None
    backdrop_path: str | None = None
    vote_average: float = 0.0
    runtime: int | None = None  # runtime in minutes
    genres: list[dict] = []
    imdb_id: str | None = None
    original_language: str | None = None
    original_title: str | None = None  # original_title field from TMDB
    credits: TmdbCredits | None = None


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class TmdbClient:
    """Async client for the TMDB v3 REST API.

    This client is intentionally stateless — no persistent HTTP session is kept
    between calls so that config changes are picked up without restart.  Each
    public method opens and closes its own httpx client.

    All public methods swallow network/API failures, log them, and return an
    empty result so that a TMDB outage never crashes the discovery routes.
    """

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _get_client(self) -> httpx.AsyncClient:
        """Return the pooled httpx.AsyncClient for TMDB."""
        cfg = settings.tmdb
        return await get_client(
            "tmdb",
            cfg.base_url.rstrip("/"),
            timeout=cfg.timeout_seconds,
            headers={
                "Authorization": f"Bearer {cfg.api_key}",
                "User-Agent": "vibeDebrid/0.1",
            },
            follow_redirects=True,
        )

    def _parse_item(
        self, raw: dict[str, Any], media_type: str | None = None
    ) -> TmdbItem | None:
        """Parse a single raw TMDB result dict into a TmdbItem.

        Normalises the movie ``title`` vs TV ``name`` field naming, and
        extracts the release year from ``release_date`` (movie) or
        ``first_air_date`` (tv).

        Args:
            raw: A single element from a TMDB results array.
            media_type: Explicit media type when not present in the raw dict
                        (e.g. when calling a typed endpoint like /search/movie).

        Returns:
            A populated TmdbItem, or None if the item should be skipped.
        """
        # Determine media_type — prefer the field in the response dict (present
        # in /search/multi and /trending results) over the caller-supplied value.
        mt = raw.get("media_type") or media_type
        if mt not in ("movie", "tv"):
            return None

        tmdb_id = raw.get("id")
        if not isinstance(tmdb_id, int):
            return None

        # Title field differs between movies and TV shows.
        if mt == "movie":
            title = raw.get("title") or raw.get("name") or ""
            date_field = raw.get("release_date") or ""
            original_title: str | None = raw.get("original_title") or None
        else:
            title = raw.get("name") or raw.get("title") or ""
            date_field = raw.get("first_air_date") or ""
            original_title = raw.get("original_name") or None

        if not title:
            return None

        # Extract 4-digit year from ISO date string.
        year: int | None = None
        if date_field and len(date_field) >= 4:
            try:
                year = int(date_field[:4])
            except ValueError:
                pass

        overview: str = raw.get("overview") or ""
        poster_path: str | None = raw.get("poster_path") or None
        vote_average: float = float(raw.get("vote_average") or 0.0)
        original_language: str | None = raw.get("original_language") or None

        return TmdbItem(
            tmdb_id=tmdb_id,
            title=title,
            year=year,
            media_type=mt,
            overview=overview,
            poster_path=poster_path,
            vote_average=vote_average,
            original_language=original_language,
            original_title=original_title,
        )

    def _handle_error_status(self, response: httpx.Response, context: str) -> bool:
        """Log error status codes and return True when the caller should abort.

        Args:
            response: The httpx response object.
            context: A short description of the call site for log messages.

        Returns:
            True if the caller should abort and return empty, False if the
            response is a success.
        """
        if response.status_code in (401, 403):
            logger.error(
                "tmdb.%s: auth failure %d — check TMDB API key",
                context,
                response.status_code,
            )
            return True

        if response.status_code == 429:
            logger.warning("tmdb.%s: rate limited (429)", context)
            return True

        if response.status_code >= 500:
            logger.error(
                "tmdb.%s: server error %d body=%s",
                context,
                response.status_code,
                response.text[:200],
            )
            return True

        if not response.is_success:
            logger.error(
                "tmdb.%s: unexpected status %d",
                context,
                response.status_code,
            )
            return True

        return False

    def _check_configured(self, context: str) -> bool:
        """Return True when TMDB is enabled and the API key is set.

        Logs a warning if the key is absent and returns False so callers can
        skip the HTTP call gracefully.

        Args:
            context: A short description of the call site for log messages.

        Returns:
            True if configured and ready, False otherwise.
        """
        cfg = settings.tmdb
        if not cfg.enabled:
            logger.debug("tmdb.%s: TMDB disabled, skipping", context)
            return False
        if not cfg.api_key:
            logger.warning(
                "tmdb.%s: TMDB API key not configured — skipping", context
            )
            return False
        return True

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    async def get_trending(
        self, media_type: str, time_window: str = "week"
    ) -> list[TmdbItem]:
        """Fetch trending movies or TV shows from TMDB.

        Args:
            media_type: Either ``"movie"`` or ``"tv"``.
            time_window: Either ``"day"`` or ``"week"`` (default ``"week"``).

        Returns:
            A list of TmdbItem objects, or an empty list on any failure.
        """
        if not self._check_configured("get_trending"):
            return []

        breaker = get_circuit_breaker("tmdb")
        try:
            await breaker.before_request()
        except CircuitOpenError:
            logger.warning("tmdb.get_trending: circuit open, skipping media_type=%s", media_type)
            return []

        try:
            client = await self._get_client()
            response = await client.get(f"/trending/{media_type}/{time_window}")
        except httpx.ConnectError as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.get_trending: connection error media_type=%s (%s)",
                media_type,
                exc,
            )
            return []
        except httpx.TimeoutException as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.get_trending: request timed out media_type=%s (%s)",
                media_type,
                exc,
            )
            return []
        except httpx.RequestError as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.get_trending: network error media_type=%s (%s)",
                media_type,
                exc,
            )
            return []

        if self._handle_error_status(response, "get_trending"):
            if response.status_code != 429:
                await breaker.record_failure()
            return []
        await breaker.record_success()

        try:
            data: dict[str, Any] = response.json()
        except ValueError as exc:
            logger.error("tmdb.get_trending: malformed JSON (%s)", exc)
            return []

        raw_results: list[dict[str, Any]] = data.get("results") or []
        items: list[TmdbItem] = []
        for raw in raw_results:
            parsed = self._parse_item(raw, media_type=media_type)
            if parsed is not None:
                items.append(parsed)

        logger.debug(
            "tmdb.get_trending: media_type=%s window=%s returned=%d parsed=%d",
            media_type,
            time_window,
            len(raw_results),
            len(items),
        )
        return items

    async def search_multi(self, query: str, page: int = 1) -> TmdbSearchResult:
        """Search TMDB for movies and TV shows using the multi-search endpoint.

        Args:
            query: Search query string.
            page: Page number to fetch (1-based).

        Returns:
            A TmdbSearchResult with matching items filtered to movie and tv,
            or an empty TmdbSearchResult on any failure.
        """
        return await self.search(query, media_type="multi", page=page)

    async def search(
        self, query: str, media_type: str = "multi", page: int = 1
    ) -> TmdbSearchResult:
        """Search TMDB with optional media type scoping.

        Routes to ``/search/movie``, ``/search/tv``, or ``/search/multi``
        depending on media_type.  Multi-search results are filtered to
        movie and tv only (people are excluded).

        Args:
            query: Search query string.
            media_type: ``"movie"``, ``"tv"``, or ``"multi"`` (default).
            page: Page number to fetch (1-based).

        Returns:
            A TmdbSearchResult, or an empty result on any failure.
        """
        if not self._check_configured("search"):
            return TmdbSearchResult(items=[])

        if media_type == "movie":
            endpoint = "/search/movie"
            parse_as: str | None = "movie"
        elif media_type == "tv":
            endpoint = "/search/tv"
            parse_as = "tv"
        else:
            endpoint = "/search/multi"
            parse_as = None  # media_type present in each result dict

        params = {"query": query, "page": page}

        breaker = get_circuit_breaker("tmdb")
        try:
            await breaker.before_request()
        except CircuitOpenError:
            logger.warning("tmdb.search: circuit open, skipping query=%r", query)
            return TmdbSearchResult(items=[])

        try:
            client = await self._get_client()
            response = await client.get(endpoint, params=params)
        except httpx.ConnectError as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.search: connection error query=%r (%s)", query, exc
            )
            return TmdbSearchResult(items=[])
        except httpx.TimeoutException as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.search: request timed out query=%r (%s)", query, exc
            )
            return TmdbSearchResult(items=[])
        except httpx.RequestError as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.search: network error query=%r (%s)", query, exc
            )
            return TmdbSearchResult(items=[])

        if self._handle_error_status(response, "search"):
            if response.status_code != 429:
                await breaker.record_failure()
            return TmdbSearchResult(items=[])
        await breaker.record_success()

        try:
            data: dict[str, Any] = response.json()
        except ValueError as exc:
            logger.error("tmdb.search: malformed JSON query=%r (%s)", query, exc)
            return TmdbSearchResult(items=[])

        raw_results: list[dict[str, Any]] = data.get("results") or []
        items: list[TmdbItem] = []
        for raw in raw_results:
            parsed = self._parse_item(raw, media_type=parse_as)
            if parsed is not None:
                items.append(parsed)

        total_results: int = int(data.get("total_results") or 0)
        current_page: int = int(data.get("page") or 1)
        total_pages: int = int(data.get("total_pages") or 1)

        logger.debug(
            "tmdb.search: query=%r media_type=%s page=%d returned=%d parsed=%d total=%d",
            query,
            media_type,
            page,
            len(raw_results),
            len(items),
            total_results,
        )
        return TmdbSearchResult(
            items=items,
            total_results=total_results,
            page=current_page,
            total_pages=total_pages,
        )

    async def get_external_ids(
        self, tmdb_id: int, media_type: str
    ) -> TmdbExternalIds | None:
        """Fetch external identifiers (IMDb, TVDB) for a TMDB item.

        Args:
            tmdb_id: The TMDB numeric identifier.
            media_type: ``"movie"`` or ``"tv"``.

        Returns:
            A TmdbExternalIds object, or None on any failure.
        """
        if not self._check_configured("get_external_ids"):
            return None

        breaker = get_circuit_breaker("tmdb")
        try:
            await breaker.before_request()
        except CircuitOpenError:
            logger.warning("tmdb.get_external_ids: circuit open, skipping tmdb_id=%d", tmdb_id)
            return None

        try:
            client = await self._get_client()
            response = await client.get(f"/{media_type}/{tmdb_id}/external_ids")
        except httpx.ConnectError as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.get_external_ids: connection error tmdb_id=%d (%s)",
                tmdb_id,
                exc,
            )
            return None
        except httpx.TimeoutException as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.get_external_ids: request timed out tmdb_id=%d (%s)",
                tmdb_id,
                exc,
            )
            return None
        except httpx.RequestError as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.get_external_ids: network error tmdb_id=%d (%s)",
                tmdb_id,
                exc,
            )
            return None

        if self._handle_error_status(response, "get_external_ids"):
            if response.status_code != 429:
                await breaker.record_failure()
            return None
        await breaker.record_success()

        try:
            data: dict[str, Any] = response.json()
        except ValueError as exc:
            logger.error(
                "tmdb.get_external_ids: malformed JSON tmdb_id=%d (%s)", tmdb_id, exc
            )
            return None

        imdb_id: str | None = data.get("imdb_id") or None
        tvdb_id_raw = data.get("tvdb_id")
        tvdb_id: int | None = int(tvdb_id_raw) if isinstance(tvdb_id_raw, (int, float)) and tvdb_id_raw else None

        logger.debug(
            "tmdb.get_external_ids: tmdb_id=%d imdb_id=%s tvdb_id=%s",
            tmdb_id,
            imdb_id,
            tvdb_id,
        )
        return TmdbExternalIds(imdb_id=imdb_id, tvdb_id=tvdb_id)

    async def get_top_rated(
        self, media_type: str, page: int = 1
    ) -> list[TmdbItem]:
        """Fetch top-rated movies or TV shows from TMDB.

        Args:
            media_type: Either ``"movie"`` or ``"tv"``.
            page: Page number to fetch (1-based).

        Returns:
            A list of TmdbItem objects, or an empty list on any failure.
        """
        if not self._check_configured("get_top_rated"):
            return []

        breaker = get_circuit_breaker("tmdb")
        try:
            await breaker.before_request()
        except CircuitOpenError:
            logger.warning("tmdb.get_top_rated: circuit open, skipping media_type=%s", media_type)
            return []

        try:
            client = await self._get_client()
            response = await client.get(
                f"/{media_type}/top_rated", params={"page": page}
            )
        except httpx.ConnectError as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.get_top_rated: connection error media_type=%s (%s)",
                media_type,
                exc,
            )
            return []
        except httpx.TimeoutException as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.get_top_rated: request timed out media_type=%s (%s)",
                media_type,
                exc,
            )
            return []
        except httpx.RequestError as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.get_top_rated: network error media_type=%s (%s)",
                media_type,
                exc,
            )
            return []

        if self._handle_error_status(response, "get_top_rated"):
            if response.status_code != 429:
                await breaker.record_failure()
            return []
        await breaker.record_success()

        try:
            data: dict[str, Any] = response.json()
        except ValueError as exc:
            logger.error("tmdb.get_top_rated: malformed JSON (%s)", exc)
            return []

        raw_results: list[dict[str, Any]] = data.get("results") or []
        items: list[TmdbItem] = []
        for raw in raw_results:
            parsed = self._parse_item(raw, media_type=media_type)
            if parsed is not None:
                items.append(parsed)

        logger.debug(
            "tmdb.get_top_rated: media_type=%s page=%d returned=%d parsed=%d",
            media_type,
            page,
            len(raw_results),
            len(items),
        )
        return items

    async def get_genres(self, media_type: str) -> list[dict]:
        """Fetch the list of official genres for movies or TV shows from TMDB.

        Args:
            media_type: Either ``"movie"`` or ``"tv"``.

        Returns:
            A list of genre dicts (e.g. ``[{"id": 28, "name": "Action"}, ...]``),
            or an empty list on any failure.
        """
        if not self._check_configured("get_genres"):
            return []

        breaker = get_circuit_breaker("tmdb")
        try:
            await breaker.before_request()
        except CircuitOpenError:
            logger.warning("tmdb.get_genres: circuit open, skipping media_type=%s", media_type)
            return []

        try:
            client = await self._get_client()
            response = await client.get(f"/genre/{media_type}/list")
        except httpx.ConnectError as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.get_genres: connection error media_type=%s (%s)",
                media_type,
                exc,
            )
            return []
        except httpx.TimeoutException as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.get_genres: request timed out media_type=%s (%s)",
                media_type,
                exc,
            )
            return []
        except httpx.RequestError as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.get_genres: network error media_type=%s (%s)",
                media_type,
                exc,
            )
            return []

        if self._handle_error_status(response, "get_genres"):
            if response.status_code != 429:
                await breaker.record_failure()
            return []
        await breaker.record_success()

        try:
            data: dict[str, Any] = response.json()
        except ValueError as exc:
            logger.error("tmdb.get_genres: malformed JSON (%s)", exc)
            return []

        genres: list[dict] = data.get("genres") or []

        logger.debug(
            "tmdb.get_genres: media_type=%s genres=%d",
            media_type,
            len(genres),
        )
        return genres

    async def discover(
        self,
        media_type: str,
        genre_id: int | None = None,
        sort_by: str = "popularity.desc",
        page: int = 1,
        vote_count_gte: int | None = None,
    ) -> TmdbSearchResult:
        """Discover movies or TV shows via the TMDB discover endpoint.

        Args:
            media_type: Either ``"movie"`` or ``"tv"``.
            genre_id: Restrict results to a specific genre ID, or None for all.
            sort_by: Sort order string (default ``"popularity.desc"``).
            page: Page number to fetch (1-based).
            vote_count_gte: Minimum vote count filter, or None to omit.

        Returns:
            A TmdbSearchResult with matching items, or an empty TmdbSearchResult
            on any failure.
        """
        if not self._check_configured("discover"):
            return TmdbSearchResult(items=[])

        params: dict[str, Any] = {"sort_by": sort_by, "page": page}
        if genre_id is not None:
            params["with_genres"] = genre_id
        if vote_count_gte is not None:
            params["vote_count.gte"] = vote_count_gte

        breaker = get_circuit_breaker("tmdb")
        try:
            await breaker.before_request()
        except CircuitOpenError:
            logger.warning("tmdb.discover: circuit open, skipping media_type=%s", media_type)
            return TmdbSearchResult(items=[])

        try:
            client = await self._get_client()
            response = await client.get(f"/discover/{media_type}", params=params)
        except httpx.ConnectError as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.discover: connection error media_type=%s (%s)",
                media_type,
                exc,
            )
            return TmdbSearchResult(items=[])
        except httpx.TimeoutException as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.discover: request timed out media_type=%s (%s)",
                media_type,
                exc,
            )
            return TmdbSearchResult(items=[])
        except httpx.RequestError as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.discover: network error media_type=%s (%s)",
                media_type,
                exc,
            )
            return TmdbSearchResult(items=[])

        if self._handle_error_status(response, "discover"):
            if response.status_code != 429:
                await breaker.record_failure()
            return TmdbSearchResult(items=[])
        await breaker.record_success()

        try:
            data: dict[str, Any] = response.json()
        except ValueError as exc:
            logger.error("tmdb.discover: malformed JSON (%s)", exc)
            return TmdbSearchResult(items=[])

        raw_results: list[dict[str, Any]] = data.get("results") or []
        items: list[TmdbItem] = []
        for raw in raw_results:
            parsed = self._parse_item(raw, media_type=media_type)
            if parsed is not None:
                items.append(parsed)

        total_results: int = int(data.get("total_results") or 0)
        current_page: int = int(data.get("page") or 1)
        total_pages: int = int(data.get("total_pages") or 1)

        logger.debug(
            "tmdb.discover: media_type=%s genre_id=%s page=%d returned=%d parsed=%d total=%d",
            media_type,
            genre_id,
            page,
            len(raw_results),
            len(items),
            total_results,
        )
        return TmdbSearchResult(
            items=items,
            total_results=total_results,
            page=current_page,
            total_pages=total_pages,
        )

    async def test_connection(self) -> bool:
        """Test the TMDB API connection by fetching the configuration endpoint.

        Returns:
            True if the API is reachable and the key is valid, False otherwise.
        """
        cfg = settings.tmdb
        if not cfg.api_key:
            logger.warning("tmdb.test_connection: API key not configured")
            return False

        # test_connection is a one-off settings check — bypass the circuit breaker
        # and use a fresh client to avoid stale connection pools.
        try:
            async with httpx.AsyncClient(
                base_url=settings.tmdb.base_url.rstrip("/"),
                timeout=settings.tmdb.timeout_seconds,
                headers={
                    "Authorization": f"Bearer {settings.tmdb.api_key}",
                    "User-Agent": "vibeDebrid/0.1",
                },
                follow_redirects=True,
            ) as client:
                response = await client.get("/configuration")
        except httpx.RequestError as exc:
            logger.warning("tmdb.test_connection: network error (%s)", exc)
            return False

        if response.status_code == 200:
            logger.debug("tmdb.test_connection: success")
            return True

        logger.warning(
            "tmdb.test_connection: unexpected status %d", response.status_code
        )
        return False


    async def get_show_details(self, tmdb_id: int) -> TmdbShowDetail | None:
        """Fetch full TV show details including seasons and external IDs.

        Uses append_to_response=external_ids to get IMDB ID in one call.

        Args:
            tmdb_id: The TMDB numeric identifier for the TV show.

        Returns:
            A TmdbShowDetail object, or None on any failure.
        """
        if not self._check_configured("get_show_details"):
            return None

        cache_key = ("show_detail", tmdb_id)
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached

        breaker = get_circuit_breaker("tmdb")
        try:
            await breaker.before_request()
        except CircuitOpenError:
            logger.warning("tmdb.get_show_details: circuit open, skipping tmdb_id=%d", tmdb_id)
            return None

        try:
            client = await self._get_client()
            response = await client.get(
                f"/tv/{tmdb_id}",
                params={"append_to_response": "external_ids,credits"},
            )
        except httpx.ConnectError as exc:
            await breaker.record_failure()
            logger.warning("tmdb.get_show_details: connection error tmdb_id=%d (%s)", tmdb_id, exc)
            return None
        except httpx.TimeoutException as exc:
            await breaker.record_failure()
            logger.warning("tmdb.get_show_details: request timed out tmdb_id=%d (%s)", tmdb_id, exc)
            return None
        except httpx.RequestError as exc:
            await breaker.record_failure()
            logger.warning("tmdb.get_show_details: network error tmdb_id=%d (%s)", tmdb_id, exc)
            return None

        if self._handle_error_status(response, "get_show_details"):
            if response.status_code != 429:
                await breaker.record_failure()
            return None
        await breaker.record_success()

        try:
            data: dict[str, Any] = response.json()
        except ValueError as exc:
            logger.error("tmdb.get_show_details: malformed JSON tmdb_id=%d (%s)", tmdb_id, exc)
            return None

        # Parse title and year
        title = data.get("name") or ""
        first_air = data.get("first_air_date") or ""
        year: int | None = None
        if first_air and len(first_air) >= 4:
            try:
                year = int(first_air[:4])
            except ValueError:
                pass

        # Parse seasons — include all (caller filters season 0 if desired)
        raw_seasons = data.get("seasons") or []
        seasons: list[TmdbSeasonInfo] = []
        for s in raw_seasons:
            seasons.append(TmdbSeasonInfo(
                season_number=s.get("season_number", 0),
                name=s.get("name", ""),
                episode_count=s.get("episode_count", 0),
                air_date=s.get("air_date") or None,
                overview=s.get("overview", ""),
                poster_path=s.get("poster_path") or None,
            ))

        # Extract IMDB ID and TVDB ID from external_ids
        ext_ids = data.get("external_ids") or {}
        imdb_id: str | None = ext_ids.get("imdb_id") or None
        tvdb_id_raw = ext_ids.get("tvdb_id")
        tvdb_id: int | None = int(tvdb_id_raw) if isinstance(tvdb_id_raw, (int, float)) and tvdb_id_raw else None

        # Parse genres
        raw_genres = data.get("genres") or []

        # Parse next/last episode air info
        def _parse_episode_air_info(raw: Any) -> TmdbEpisodeAirInfo | None:
            if not isinstance(raw, dict):
                return None
            season_num = raw.get("season_number")
            episode_num = raw.get("episode_number")
            if not isinstance(season_num, int) or not isinstance(episode_num, int):
                return None
            return TmdbEpisodeAirInfo(
                season_number=season_num,
                episode_number=episode_num,
                air_date=raw.get("air_date") or None,
            )

        next_episode_to_air = _parse_episode_air_info(data.get("next_episode_to_air"))
        last_episode_to_air = _parse_episode_air_info(data.get("last_episode_to_air"))

        # Parse credits from append_to_response block
        raw_credits = data.get("credits") or {}
        credits: TmdbCredits | None = None
        if raw_credits:
            cast: list[TmdbCastMember] = [
                TmdbCastMember(
                    name=m.get("name") or "",
                    character=m.get("character") or "",
                    profile_path=m.get("profile_path") or None,
                    order=m.get("order") if isinstance(m.get("order"), int) else 999,
                )
                for m in (raw_credits.get("cast") or [])
                if m.get("name")
            ]
            crew: list[TmdbCrewMember] = [
                TmdbCrewMember(
                    name=m.get("name") or "",
                    job=m.get("job") or "",
                    department=m.get("department") or "",
                    profile_path=m.get("profile_path") or None,
                )
                for m in (raw_credits.get("crew") or [])
                if m.get("name")
            ]
            credits = TmdbCredits(cast=cast, crew=crew)

        result = TmdbShowDetail(
            tmdb_id=tmdb_id,
            title=title,
            year=year,
            overview=data.get("overview") or "",
            tagline=data.get("tagline") or "",
            poster_path=data.get("poster_path") or None,
            backdrop_path=data.get("backdrop_path") or None,
            status=data.get("status") or "",
            vote_average=float(data.get("vote_average") or 0.0),
            number_of_seasons=int(data.get("number_of_seasons") or 0),
            seasons=seasons,
            imdb_id=imdb_id,
            tvdb_id=tvdb_id,
            genres=raw_genres,
            next_episode_to_air=next_episode_to_air,
            last_episode_to_air=last_episode_to_air,
            original_language=data.get("original_language") or None,
            original_title=data.get("original_name") or None,
            credits=credits,
        )

        logger.debug(
            "tmdb.get_show_details: tmdb_id=%d title=%r seasons=%d imdb_id=%s",
            tmdb_id, title, len(seasons), imdb_id,
        )
        _cache_set(cache_key, result)
        return result

    async def find_by_imdb_id(self, imdb_id: str) -> dict[str, Any] | None:
        """Find a TMDB entry by IMDB ID.

        Uses GET /find/{imdb_id}?external_source=imdb_id.  Checks movie_results
        first, then tv_results.  For TV results, follows up with get_external_ids
        to obtain the tvdb_id (not returned by the /find endpoint itself).

        Args:
            imdb_id: The IMDB ID string (e.g. ``"tt1234567"``).

        Returns:
            A dict with keys ``tmdb_id`` (int), ``tvdb_id`` (int|None), and
            ``media_type`` (``"movie"`` or ``"tv"``), or None if not found or
            on any error.
        """
        if not self._check_configured("find_by_imdb_id"):
            return None

        breaker = get_circuit_breaker("tmdb")
        try:
            await breaker.before_request()
        except CircuitOpenError:
            logger.warning("tmdb.find_by_imdb_id: circuit open, skipping imdb_id=%s", imdb_id)
            return None

        try:
            client = await self._get_client()
            response = await client.get(
                f"/find/{imdb_id}",
                params={"external_source": "imdb_id"},
            )
        except httpx.ConnectError as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.find_by_imdb_id: connection error imdb_id=%s (%s)", imdb_id, exc
            )
            return None
        except httpx.TimeoutException as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.find_by_imdb_id: request timed out imdb_id=%s (%s)", imdb_id, exc
            )
            return None
        except httpx.RequestError as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.find_by_imdb_id: network error imdb_id=%s (%s)", imdb_id, exc
            )
            return None

        if self._handle_error_status(response, "find_by_imdb_id"):
            if response.status_code != 429:
                await breaker.record_failure()
            return None
        await breaker.record_success()

        try:
            data: dict[str, Any] = response.json()
        except ValueError as exc:
            logger.error(
                "tmdb.find_by_imdb_id: malformed JSON imdb_id=%s (%s)", imdb_id, exc
            )
            return None

        # Check movie_results first, then tv_results
        movie_results: list[dict[str, Any]] = data.get("movie_results") or []
        tv_results: list[dict[str, Any]] = data.get("tv_results") or []

        if movie_results:
            raw = movie_results[0]
            tmdb_id_int = raw.get("id")
            if not isinstance(tmdb_id_int, int):
                return None
            logger.debug(
                "tmdb.find_by_imdb_id: imdb_id=%s → movie tmdb_id=%d",
                imdb_id,
                tmdb_id_int,
            )
            return {"tmdb_id": tmdb_id_int, "tvdb_id": None, "media_type": "movie"}

        if tv_results:
            raw = tv_results[0]
            tmdb_id_int = raw.get("id")
            if not isinstance(tmdb_id_int, int):
                return None
            # Fetch tvdb_id via external_ids — not included in /find response
            tvdb_id: int | None = None
            ext = await self.get_external_ids(tmdb_id_int, "tv")
            if ext is not None:
                tvdb_id = ext.tvdb_id
            logger.debug(
                "tmdb.find_by_imdb_id: imdb_id=%s → tv tmdb_id=%d tvdb_id=%s",
                imdb_id,
                tmdb_id_int,
                tvdb_id,
            )
            return {"tmdb_id": tmdb_id_int, "tvdb_id": tvdb_id, "media_type": "tv"}

        logger.debug("tmdb.find_by_imdb_id: no results for imdb_id=%s", imdb_id)
        return None

    async def get_movie_details(self, tmdb_id: int) -> TmdbItem | None:
        """Fetch basic movie details from TMDB /movie/{id} endpoint.

        Args:
            tmdb_id: The TMDB numeric identifier for the movie.

        Returns:
            A TmdbItem with original_language populated, or None on any failure.
        """
        if not self._check_configured("get_movie_details"):
            return None

        breaker = get_circuit_breaker("tmdb")
        try:
            await breaker.before_request()
        except CircuitOpenError:
            logger.warning("tmdb.get_movie_details: circuit open, skipping tmdb_id=%d", tmdb_id)
            return None

        try:
            client = await self._get_client()
            response = await client.get(f"/movie/{tmdb_id}")
        except httpx.ConnectError as exc:
            await breaker.record_failure()
            logger.warning("tmdb.get_movie_details: connection error tmdb_id=%d (%s)", tmdb_id, exc)
            return None
        except httpx.TimeoutException as exc:
            await breaker.record_failure()
            logger.warning("tmdb.get_movie_details: request timed out tmdb_id=%d (%s)", tmdb_id, exc)
            return None
        except httpx.RequestError as exc:
            await breaker.record_failure()
            logger.warning("tmdb.get_movie_details: network error tmdb_id=%d (%s)", tmdb_id, exc)
            return None

        if self._handle_error_status(response, "get_movie_details"):
            if response.status_code != 429:
                await breaker.record_failure()
            return None
        await breaker.record_success()

        try:
            data: dict[str, Any] = response.json()
        except ValueError as exc:
            logger.error("tmdb.get_movie_details: malformed JSON tmdb_id=%d (%s)", tmdb_id, exc)
            return None

        return self._parse_item(data, media_type="movie")

    async def get_alternative_titles(
        self, tmdb_id: int, media_type: str
    ) -> list[str]:
        """Fetch alternative/localized titles for a movie or TV show from TMDB.

        Uses GET /{media_type}/{tmdb_id}/alternative_titles.  For movies the
        response key is ``titles`` with each entry containing a ``title`` field;
        for TV the key is ``results`` with the same ``title`` field per entry.

        Args:
            tmdb_id: The TMDB numeric identifier.
            media_type: ``"movie"`` or ``"tv"``.

        Returns:
            A deduplicated list of alternative title strings, or an empty list
            on any failure or when TMDB is not configured.
        """
        if not self._check_configured("get_alternative_titles"):
            return []

        breaker = get_circuit_breaker("tmdb")
        try:
            await breaker.before_request()
        except CircuitOpenError:
            logger.warning(
                "tmdb.get_alternative_titles: circuit open, skipping "
                "tmdb_id=%d media_type=%s",
                tmdb_id,
                media_type,
            )
            return []

        try:
            client = await self._get_client()
            response = await client.get(
                f"/{media_type}/{tmdb_id}/alternative_titles"
            )
        except httpx.ConnectError as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.get_alternative_titles: connection error "
                "tmdb_id=%d media_type=%s (%s)",
                tmdb_id,
                media_type,
                exc,
            )
            return []
        except httpx.TimeoutException as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.get_alternative_titles: request timed out "
                "tmdb_id=%d media_type=%s (%s)",
                tmdb_id,
                media_type,
                exc,
            )
            return []
        except httpx.RequestError as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.get_alternative_titles: network error "
                "tmdb_id=%d media_type=%s (%s)",
                tmdb_id,
                media_type,
                exc,
            )
            return []

        if self._handle_error_status(response, "get_alternative_titles"):
            if response.status_code != 429:
                await breaker.record_failure()
            return []
        await breaker.record_success()

        try:
            data: dict[str, Any] = response.json()
        except ValueError as exc:
            logger.error(
                "tmdb.get_alternative_titles: malformed JSON "
                "tmdb_id=%d media_type=%s (%s)",
                tmdb_id,
                media_type,
                exc,
            )
            return []

        # Movies use "titles" key; TV uses "results" key.
        if media_type == "movie":
            raw_list: list[dict[str, Any]] = data.get("titles") or []
        else:
            raw_list = data.get("results") or []

        seen: set[str] = set()
        titles: list[str] = []
        for entry in raw_list:
            t = entry.get("title") or ""
            if t and t not in seen:
                seen.add(t)
                titles.append(t)

        logger.debug(
            "tmdb.get_alternative_titles: tmdb_id=%d media_type=%s found=%d",
            tmdb_id,
            media_type,
            len(titles),
        )
        return titles

    async def get_season_details(self, tmdb_id: int, season_number: int) -> TmdbSeasonDetail | None:
        """Fetch detailed season info including episode air dates.

        Args:
            tmdb_id: The TMDB numeric identifier for the TV show.
            season_number: The season number to fetch.

        Returns:
            A TmdbSeasonDetail object, or None on any failure.
        """
        if not self._check_configured("get_season_details"):
            return None

        cache_key = ("season_detail", tmdb_id, season_number)
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached

        breaker = get_circuit_breaker("tmdb")
        try:
            await breaker.before_request()
        except CircuitOpenError:
            logger.warning(
                "tmdb.get_season_details: circuit open, skipping tmdb_id=%d s=%d",
                tmdb_id, season_number,
            )
            return None

        try:
            client = await self._get_client()
            response = await client.get(f"/tv/{tmdb_id}/season/{season_number}")
        except httpx.ConnectError as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.get_season_details: connection error tmdb_id=%d s=%d (%s)",
                tmdb_id, season_number, exc,
            )
            return None
        except httpx.TimeoutException as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.get_season_details: request timed out tmdb_id=%d s=%d (%s)",
                tmdb_id, season_number, exc,
            )
            return None
        except httpx.RequestError as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.get_season_details: network error tmdb_id=%d s=%d (%s)",
                tmdb_id, season_number, exc,
            )
            return None

        if self._handle_error_status(response, "get_season_details"):
            if response.status_code != 429:
                await breaker.record_failure()
            return None
        await breaker.record_success()

        try:
            data: dict[str, Any] = response.json()
        except ValueError as exc:
            logger.error(
                "tmdb.get_season_details: malformed JSON tmdb_id=%d s=%d (%s)",
                tmdb_id, season_number, exc,
            )
            return None

        raw_episodes = data.get("episodes") or []
        episodes: list[TmdbEpisodeInfo] = []
        for ep in raw_episodes:
            episodes.append(TmdbEpisodeInfo(
                episode_number=ep.get("episode_number", 0),
                name=ep.get("name", ""),
                air_date=ep.get("air_date") or None,
                overview=ep.get("overview", ""),
                still_path=ep.get("still_path") or None,
            ))

        result = TmdbSeasonDetail(
            season_number=int(data.get("season_number", season_number)),
            name=data.get("name") or "",
            episodes=episodes,
        )

        logger.debug(
            "tmdb.get_season_details: tmdb_id=%d season=%d episodes=%d",
            tmdb_id, season_number, len(episodes),
        )
        _cache_set(cache_key, result)
        return result


    async def get_movie_details_full(self, tmdb_id: int) -> TmdbMovieDetail | None:
        """Fetch full movie details including external IDs (IMDB) in one call.

        Uses ``GET /movie/{id}?append_to_response=external_ids`` so that the
        IMDB ID is available without a second round-trip.

        Args:
            tmdb_id: The TMDB numeric identifier for the movie.

        Returns:
            A TmdbMovieDetail with all fields populated, or None on any failure.
        """
        if not self._check_configured("get_movie_details_full"):
            return None

        cache_key = ("movie_detail", tmdb_id)
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached

        breaker = get_circuit_breaker("tmdb")
        try:
            await breaker.before_request()
        except CircuitOpenError:
            logger.warning(
                "tmdb.get_movie_details_full: circuit open, skipping tmdb_id=%d", tmdb_id
            )
            return None

        try:
            client = await self._get_client()
            response = await client.get(
                f"/movie/{tmdb_id}",
                params={"append_to_response": "external_ids,credits"},
            )
        except httpx.ConnectError as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.get_movie_details_full: connection error tmdb_id=%d (%s)", tmdb_id, exc
            )
            return None
        except httpx.TimeoutException as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.get_movie_details_full: request timed out tmdb_id=%d (%s)", tmdb_id, exc
            )
            return None
        except httpx.RequestError as exc:
            await breaker.record_failure()
            logger.warning(
                "tmdb.get_movie_details_full: network error tmdb_id=%d (%s)", tmdb_id, exc
            )
            return None

        if self._handle_error_status(response, "get_movie_details_full"):
            if response.status_code == 404:
                logger.debug("tmdb.get_movie_details_full: not found tmdb_id=%d", tmdb_id)
            elif response.status_code != 429:
                await breaker.record_failure()
            return None
        await breaker.record_success()

        try:
            data: dict[str, Any] = response.json()
        except ValueError as exc:
            logger.error(
                "tmdb.get_movie_details_full: malformed JSON tmdb_id=%d (%s)", tmdb_id, exc
            )
            return None

        # Parse title and year
        title: str = data.get("title") or data.get("name") or ""
        if not title:
            logger.warning("tmdb.get_movie_details_full: empty title for tmdb_id=%d", tmdb_id)
            return None

        release_date: str = data.get("release_date") or ""
        year: int | None = None
        if release_date and len(release_date) >= 4:
            try:
                year = int(release_date[:4])
            except ValueError:
                pass

        # Extract IMDB ID from appended external_ids block
        ext_ids: dict[str, Any] = data.get("external_ids") or {}
        imdb_id: str | None = ext_ids.get("imdb_id") or None

        # Parse genres
        raw_genres: list[dict] = data.get("genres") or []

        # Runtime may be None for unreleased films
        runtime_raw = data.get("runtime")
        runtime: int | None = int(runtime_raw) if isinstance(runtime_raw, int) and runtime_raw > 0 else None

        # Parse credits from append_to_response block
        raw_credits = data.get("credits") or {}
        credits: TmdbCredits | None = None
        if raw_credits:
            cast: list[TmdbCastMember] = [
                TmdbCastMember(
                    name=m.get("name") or "",
                    character=m.get("character") or "",
                    profile_path=m.get("profile_path") or None,
                    order=m.get("order") if isinstance(m.get("order"), int) else 999,
                )
                for m in (raw_credits.get("cast") or [])
                if m.get("name")
            ]
            crew: list[TmdbCrewMember] = [
                TmdbCrewMember(
                    name=m.get("name") or "",
                    job=m.get("job") or "",
                    department=m.get("department") or "",
                    profile_path=m.get("profile_path") or None,
                )
                for m in (raw_credits.get("crew") or [])
                if m.get("name")
            ]
            credits = TmdbCredits(cast=cast, crew=crew)

        result = TmdbMovieDetail(
            tmdb_id=tmdb_id,
            title=title,
            year=year,
            overview=data.get("overview") or "",
            tagline=data.get("tagline") or "",
            poster_path=data.get("poster_path") or None,
            backdrop_path=data.get("backdrop_path") or None,
            vote_average=float(data.get("vote_average") or 0.0),
            runtime=runtime,
            genres=raw_genres,
            imdb_id=imdb_id,
            original_language=data.get("original_language") or None,
            original_title=data.get("original_title") or None,
            credits=credits,
        )

        logger.debug(
            "tmdb.get_movie_details_full: tmdb_id=%d title=%r imdb_id=%s runtime=%s",
            tmdb_id, title, imdb_id, runtime,
        )
        _cache_set(cache_key, result)
        return result


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

tmdb_client = TmdbClient()
