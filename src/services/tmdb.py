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
from typing import Any

import httpx
from pydantic import BaseModel

from src.config import settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


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

    def _build_client(self) -> httpx.AsyncClient:
        """Create a new httpx.AsyncClient pointed at the TMDB API."""
        cfg = settings.tmdb
        return httpx.AsyncClient(
            base_url=cfg.base_url.rstrip("/"),
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
        else:
            title = raw.get("name") or raw.get("title") or ""
            date_field = raw.get("first_air_date") or ""

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

        return TmdbItem(
            tmdb_id=tmdb_id,
            title=title,
            year=year,
            media_type=mt,
            overview=overview,
            poster_path=poster_path,
            vote_average=vote_average,
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

        try:
            async with self._build_client() as client:
                response = await client.get(f"/trending/{media_type}/{time_window}")
        except httpx.ConnectError as exc:
            logger.warning(
                "tmdb.get_trending: connection error media_type=%s (%s)",
                media_type,
                exc,
            )
            return []
        except httpx.TimeoutException as exc:
            logger.warning(
                "tmdb.get_trending: request timed out media_type=%s (%s)",
                media_type,
                exc,
            )
            return []
        except httpx.RequestError as exc:
            logger.warning(
                "tmdb.get_trending: network error media_type=%s (%s)",
                media_type,
                exc,
            )
            return []

        if self._handle_error_status(response, "get_trending"):
            return []

        try:
            data: dict[str, Any] = response.json()
        except Exception as exc:
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

        try:
            async with self._build_client() as client:
                response = await client.get(endpoint, params=params)
        except httpx.ConnectError as exc:
            logger.warning(
                "tmdb.search: connection error query=%r (%s)", query, exc
            )
            return TmdbSearchResult(items=[])
        except httpx.TimeoutException as exc:
            logger.warning(
                "tmdb.search: request timed out query=%r (%s)", query, exc
            )
            return TmdbSearchResult(items=[])
        except httpx.RequestError as exc:
            logger.warning(
                "tmdb.search: network error query=%r (%s)", query, exc
            )
            return TmdbSearchResult(items=[])

        if self._handle_error_status(response, "search"):
            return TmdbSearchResult(items=[])

        try:
            data: dict[str, Any] = response.json()
        except Exception as exc:
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

        try:
            async with self._build_client() as client:
                response = await client.get(f"/{media_type}/{tmdb_id}/external_ids")
        except httpx.ConnectError as exc:
            logger.warning(
                "tmdb.get_external_ids: connection error tmdb_id=%d (%s)",
                tmdb_id,
                exc,
            )
            return None
        except httpx.TimeoutException as exc:
            logger.warning(
                "tmdb.get_external_ids: request timed out tmdb_id=%d (%s)",
                tmdb_id,
                exc,
            )
            return None
        except httpx.RequestError as exc:
            logger.warning(
                "tmdb.get_external_ids: network error tmdb_id=%d (%s)",
                tmdb_id,
                exc,
            )
            return None

        if self._handle_error_status(response, "get_external_ids"):
            return None

        try:
            data: dict[str, Any] = response.json()
        except Exception as exc:
            logger.error(
                "tmdb.get_external_ids: malformed JSON tmdb_id=%d (%s)", tmdb_id, exc
            )
            return None

        imdb_id: str | None = data.get("imdb_id") or None
        tvdb_id_raw = data.get("tvdb_id")
        tvdb_id: int | None = int(tvdb_id_raw) if isinstance(tvdb_id_raw, int) else None

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

        try:
            async with self._build_client() as client:
                response = await client.get(
                    f"/{media_type}/top_rated", params={"page": page}
                )
        except httpx.ConnectError as exc:
            logger.warning(
                "tmdb.get_top_rated: connection error media_type=%s (%s)",
                media_type,
                exc,
            )
            return []
        except httpx.TimeoutException as exc:
            logger.warning(
                "tmdb.get_top_rated: request timed out media_type=%s (%s)",
                media_type,
                exc,
            )
            return []
        except httpx.RequestError as exc:
            logger.warning(
                "tmdb.get_top_rated: network error media_type=%s (%s)",
                media_type,
                exc,
            )
            return []

        if self._handle_error_status(response, "get_top_rated"):
            return []

        try:
            data: dict[str, Any] = response.json()
        except Exception as exc:
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

        try:
            async with self._build_client() as client:
                response = await client.get(f"/genre/{media_type}/list")
        except httpx.ConnectError as exc:
            logger.warning(
                "tmdb.get_genres: connection error media_type=%s (%s)",
                media_type,
                exc,
            )
            return []
        except httpx.TimeoutException as exc:
            logger.warning(
                "tmdb.get_genres: request timed out media_type=%s (%s)",
                media_type,
                exc,
            )
            return []
        except httpx.RequestError as exc:
            logger.warning(
                "tmdb.get_genres: network error media_type=%s (%s)",
                media_type,
                exc,
            )
            return []

        if self._handle_error_status(response, "get_genres"):
            return []

        try:
            data: dict[str, Any] = response.json()
        except Exception as exc:
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

        try:
            async with self._build_client() as client:
                response = await client.get(f"/discover/{media_type}", params=params)
        except httpx.ConnectError as exc:
            logger.warning(
                "tmdb.discover: connection error media_type=%s (%s)",
                media_type,
                exc,
            )
            return TmdbSearchResult(items=[])
        except httpx.TimeoutException as exc:
            logger.warning(
                "tmdb.discover: request timed out media_type=%s (%s)",
                media_type,
                exc,
            )
            return TmdbSearchResult(items=[])
        except httpx.RequestError as exc:
            logger.warning(
                "tmdb.discover: network error media_type=%s (%s)",
                media_type,
                exc,
            )
            return TmdbSearchResult(items=[])

        if self._handle_error_status(response, "discover"):
            return TmdbSearchResult(items=[])

        try:
            data: dict[str, Any] = response.json()
        except Exception as exc:
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

        try:
            async with self._build_client() as client:
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


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

tmdb_client = TmdbClient()
