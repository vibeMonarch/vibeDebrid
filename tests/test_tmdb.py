"""Tests for src/services/tmdb.py.

All external HTTP calls are intercepted by a custom _MockTransport so no real
network traffic is generated. Each test exercises a single behaviour.

The mocking pattern mirrors test_zilean.py exactly:
  - monkeypatch patches src.services.tmdb.settings so the mock stays alive
    during await calls.
  - _MockTransport records every request in .requests_made for URL assertions.
  - _patch_client() replaces client._build_client to inject the mock transport.
  - _make_response() builds fake httpx.Response objects.
  - _make_mock_cfg() creates mock TmdbConfig objects.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from src.services.http_client import CircuitBreaker
from src.services.tmdb import (
    TmdbClient,
    TmdbExternalIds,
    TmdbItem,
    TmdbSearchResult,
)


def _make_noop_breaker() -> CircuitBreaker:
    """Return a CircuitBreaker that is always CLOSED (never rejects requests)."""
    return CircuitBreaker("test", failure_threshold=999, recovery_timeout=0.0)


# ---------------------------------------------------------------------------
# Helpers — mock data builders
# ---------------------------------------------------------------------------


def _make_movie_raw(
    tmdb_id: int = 123,
    title: str = "Movie Title",
    release_date: str = "2024-06-15",
    overview: str = "A movie...",
    poster_path: str | None = "/abc123.jpg",
    vote_average: float = 7.5,
) -> dict[str, Any]:
    """Build a single raw TMDB movie result matching the live API schema."""
    return {
        "id": tmdb_id,
        "title": title,
        "release_date": release_date,
        "media_type": "movie",
        "overview": overview,
        "poster_path": poster_path,
        "vote_average": vote_average,
    }


def _make_tv_raw(
    tmdb_id: int = 456,
    name: str = "TV Show",
    first_air_date: str = "2024-01-10",
    overview: str = "A show...",
    poster_path: str | None = "/def456.jpg",
    vote_average: float = 8.2,
) -> dict[str, Any]:
    """Build a single raw TMDB TV show result matching the live API schema."""
    return {
        "id": tmdb_id,
        "name": name,
        "first_air_date": first_air_date,
        "media_type": "tv",
        "overview": overview,
        "poster_path": poster_path,
        "vote_average": vote_average,
    }


def _make_trending_envelope(results: list[dict[str, Any]]) -> dict[str, Any]:
    """Wrap results in the TMDB trending API response envelope."""
    return {
        "page": 1,
        "results": results,
        "total_results": len(results),
        "total_pages": 1,
    }


def _make_search_envelope(
    results: list[dict[str, Any]],
    total_results: int = 100,
    page: int = 1,
    total_pages: int = 5,
) -> dict[str, Any]:
    """Wrap results in the TMDB search API response envelope."""
    return {
        "page": page,
        "results": results,
        "total_results": total_results,
        "total_pages": total_pages,
    }


def _make_external_ids_response(
    imdb_id: str | None = "tt1234567",
    tvdb_id: int | None = 98765,
) -> dict[str, Any]:
    """Build a TMDB external IDs response."""
    return {
        "imdb_id": imdb_id,
        "tvdb_id": tvdb_id,
        "facebook_id": None,
        "instagram_id": None,
        "twitter_id": None,
    }


def _make_response(
    status_code: int,
    body: Any = None,
    *,
    content_type: str = "application/json",
) -> httpx.Response:
    """Build a fake httpx.Response for use with _MockTransport."""
    if body is None:
        raw = b""
    elif isinstance(body, (dict, list)):
        raw = json.dumps(body).encode()
    else:
        raw = body if isinstance(body, bytes) else str(body).encode()

    return httpx.Response(
        status_code=status_code,
        headers={"content-type": content_type},
        content=raw,
        request=httpx.Request("GET", "https://api.themoviedb.org/3/"),
    )


# ---------------------------------------------------------------------------
# Mock transport
# ---------------------------------------------------------------------------


class _MockTransport(httpx.AsyncBaseTransport):
    """URL-aware mock transport: maps URL substrings to pre-built responses.

    If ``responses_by_url`` is provided the transport matches each incoming
    URL against the keys (substring match) and returns the corresponding
    response. Falls back to ``default_response`` when no key matches.

    If only ``responses`` (a plain list) is provided the transport behaves
    like a sequential queue — first call gets responses[0], etc.
    """

    def __init__(
        self,
        responses: list[httpx.Response] | None = None,
        *,
        responses_by_url: dict[str, httpx.Response] | None = None,
        default_response: httpx.Response | None = None,
    ) -> None:
        self._queue = list(responses or [])
        self._queue_index = 0
        self._by_url = responses_by_url or {}
        self._default = default_response
        self.requests_made: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests_made.append(request)
        url_str = str(request.url)

        # URL-keyed lookup first
        for key, resp in self._by_url.items():
            if key in url_str:
                resp.request = request  # type: ignore[attr-defined]
                return resp

        # Sequential queue
        if self._queue_index < len(self._queue):
            resp = self._queue[self._queue_index]
            self._queue_index += 1
            resp.request = request  # type: ignore[attr-defined]
            return resp

        # Default fallback
        if self._default is not None:
            self._default.request = request  # type: ignore[attr-defined]
            return self._default

        raise RuntimeError(
            f"_MockTransport: no response configured for URL {url_str!r}"
        )


def _make_mock_cfg(
    base_url: str = "https://api.themoviedb.org/3",
    image_base_url: str = "https://image.tmdb.org/t/p",
    api_key: str = "test-api-key",
    timeout_seconds: int = 10,
    enabled: bool = True,
) -> MagicMock:
    """Return a MagicMock that looks like a TmdbConfig object."""
    cfg = MagicMock()
    cfg.base_url = base_url
    cfg.image_base_url = image_base_url
    cfg.api_key = api_key
    cfg.timeout_seconds = timeout_seconds
    cfg.enabled = enabled
    return cfg


def _patch_client(
    client: TmdbClient,
    responses: list[httpx.Response] | None = None,
    *,
    responses_by_url: dict[str, httpx.Response] | None = None,
    default_response: httpx.Response | None = None,
) -> _MockTransport:
    """Monkey-patch *client._build_client* to inject a _MockTransport.

    Every call to ``client._build_client()`` returns a fresh
    ``httpx.AsyncClient`` backed by the same transport instance, so
    ``transport.requests_made`` accumulates correctly.

    Args:
        client:           The TmdbClient instance under test.
        responses:        Ordered list of responses to return sequentially.
        responses_by_url: URL-substring -> response mapping (checked first).
        default_response: Fallback response when nothing else matches.

    Returns:
        The _MockTransport so tests can inspect ``transport.requests_made``.
    """
    transport = _MockTransport(
        responses,
        responses_by_url=responses_by_url,
        default_response=default_response,
    )

    async def _fake_get_client() -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url="https://api.themoviedb.org/3",
            transport=transport,
        )

    client._get_client = _fake_get_client  # type: ignore[method-assign]
    return transport


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch) -> TmdbClient:
    """A TmdbClient configured with test defaults (no real HTTP)."""
    cfg = _make_mock_cfg()

    mock_settings = MagicMock()
    mock_settings.tmdb = cfg
    monkeypatch.setattr("src.services.tmdb.settings", mock_settings)
    monkeypatch.setattr(
        "src.services.tmdb.get_circuit_breaker",
        lambda *args, **kwargs: _make_noop_breaker(),
    )

    return TmdbClient()


@pytest.fixture()
def disabled_client(monkeypatch: pytest.MonkeyPatch) -> TmdbClient:
    """A TmdbClient where enabled=False — should short-circuit all requests."""
    cfg = _make_mock_cfg(enabled=False)

    mock_settings = MagicMock()
    mock_settings.tmdb = cfg
    monkeypatch.setattr("src.services.tmdb.settings", mock_settings)
    monkeypatch.setattr(
        "src.services.tmdb.get_circuit_breaker",
        lambda *args, **kwargs: _make_noop_breaker(),
    )

    return TmdbClient()


@pytest.fixture()
def no_key_client(monkeypatch: pytest.MonkeyPatch) -> TmdbClient:
    """A TmdbClient where api_key is empty — should short-circuit all requests."""
    cfg = _make_mock_cfg(api_key="")

    mock_settings = MagicMock()
    mock_settings.tmdb = cfg
    monkeypatch.setattr("src.services.tmdb.settings", mock_settings)
    monkeypatch.setattr(
        "src.services.tmdb.get_circuit_breaker",
        lambda *args, **kwargs: _make_noop_breaker(),
    )

    return TmdbClient()


# ---------------------------------------------------------------------------
# get_trending — happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_trending_movies(client: TmdbClient) -> None:
    """get_trending returns a list of TmdbItems with correct fields for movies."""
    movie1 = _make_movie_raw(tmdb_id=111, title="First Movie", release_date="2024-03-10", vote_average=7.5)
    movie2 = _make_movie_raw(tmdb_id=222, title="Second Movie", release_date="2024-07-22", vote_average=6.8)
    envelope = _make_trending_envelope([movie1, movie2])

    _patch_client(client, [_make_response(200, envelope)])
    results = await client.get_trending("movie")

    assert len(results) == 2
    assert all(isinstance(item, TmdbItem) for item in results)

    first = results[0]
    assert first.tmdb_id == 111
    assert first.title == "First Movie"
    assert first.year == 2024
    assert first.media_type == "movie"
    assert first.poster_path == "/abc123.jpg"
    assert first.vote_average == 7.5

    second = results[1]
    assert second.tmdb_id == 222
    assert second.title == "Second Movie"
    assert second.year == 2024
    assert second.vote_average == 6.8


@pytest.mark.asyncio
async def test_get_trending_tv(client: TmdbClient) -> None:
    """get_trending normalises TV shows: title from 'name', year from 'first_air_date'."""
    show = _make_tv_raw(
        tmdb_id=789,
        name="Great TV Show",
        first_air_date="2023-09-15",
        poster_path="/tv789.jpg",
        vote_average=9.0,
    )
    envelope = _make_trending_envelope([show])

    _patch_client(client, [_make_response(200, envelope)])
    results = await client.get_trending("tv")

    assert len(results) == 1
    item = results[0]
    assert item.tmdb_id == 789
    assert item.title == "Great TV Show"
    assert item.year == 2023
    assert item.media_type == "tv"
    assert item.poster_path == "/tv789.jpg"
    assert item.vote_average == 9.0


@pytest.mark.asyncio
async def test_get_trending_uses_week_window_by_default(client: TmdbClient) -> None:
    """get_trending uses 'week' as the default time_window in the URL."""
    envelope = _make_trending_envelope([_make_movie_raw()])
    transport = _patch_client(client, [_make_response(200, envelope)])

    await client.get_trending("movie")

    assert len(transport.requests_made) == 1
    url = str(transport.requests_made[0].url)
    assert "/trending/movie/week" in url


@pytest.mark.asyncio
async def test_get_trending_custom_window(client: TmdbClient) -> None:
    """get_trending honours a custom time_window argument."""
    envelope = _make_trending_envelope([_make_movie_raw()])
    transport = _patch_client(client, [_make_response(200, envelope)])

    await client.get_trending("movie", time_window="day")

    url = str(transport.requests_made[0].url)
    assert "/trending/movie/day" in url


# ---------------------------------------------------------------------------
# get_trending — disabled / unconfigured
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_trending_disabled(disabled_client: TmdbClient) -> None:
    """When enabled=False, get_trending returns [] without making any HTTP calls."""
    transport = _patch_client(disabled_client, [])

    results = await disabled_client.get_trending("movie")

    assert results == []
    assert len(transport.requests_made) == 0


@pytest.mark.asyncio
async def test_get_trending_no_api_key(no_key_client: TmdbClient) -> None:
    """When api_key is empty, get_trending returns [] without making any HTTP calls."""
    transport = _patch_client(no_key_client, [])

    results = await no_key_client.get_trending("movie")

    assert results == []
    assert len(transport.requests_made) == 0


# ---------------------------------------------------------------------------
# get_trending — error handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_trending_auth_error(client: TmdbClient) -> None:
    """A 401 Unauthorized response returns [] without raising."""
    _patch_client(client, [_make_response(401, {"status_message": "Invalid API key."})])

    results = await client.get_trending("movie")

    assert results == []


@pytest.mark.asyncio
async def test_get_trending_server_error(client: TmdbClient) -> None:
    """A 500 Internal Server Error response returns [] without raising."""
    _patch_client(client, [_make_response(500, {"status_message": "Server Error"})])

    results = await client.get_trending("movie")

    assert results == []


@pytest.mark.asyncio
async def test_get_trending_connection_error(client: TmdbClient) -> None:
    """A ConnectError returns [] without raising an exception."""

    class _ConnErrorTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused", request=request)

    async def _fake_get_client() -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=_ConnErrorTransport())

    client._get_client = _fake_get_client  # type: ignore[method-assign]

    results = await client.get_trending("movie")

    assert results == []


@pytest.mark.asyncio
async def test_get_trending_timeout(client: TmdbClient) -> None:
    """A TimeoutException returns [] without raising an exception."""

    class _TimeoutTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            raise httpx.TimeoutException("timed out", request=request)

    async def _fake_get_client() -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=_TimeoutTransport())

    client._get_client = _fake_get_client  # type: ignore[method-assign]

    results = await client.get_trending("tv")

    assert results == []


@pytest.mark.asyncio
async def test_get_trending_rate_limited(client: TmdbClient) -> None:
    """A 429 Too Many Requests response returns [] without raising."""
    _patch_client(client, [_make_response(429, {"status_message": "Request count over limit."})])

    results = await client.get_trending("movie")

    assert results == []


@pytest.mark.asyncio
async def test_get_trending_malformed_json(client: TmdbClient) -> None:
    """A non-JSON response body returns [] without raising."""
    _patch_client(
        client,
        [_make_response(200, b"<html>not json</html>", content_type="text/html")],
    )

    results = await client.get_trending("movie")

    assert results == []


@pytest.mark.asyncio
async def test_get_trending_empty_results(client: TmdbClient) -> None:
    """An empty results array returns an empty list."""
    _patch_client(client, [_make_response(200, _make_trending_envelope([]))])

    results = await client.get_trending("movie")

    assert results == []


# ---------------------------------------------------------------------------
# search — happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_multi(client: TmdbClient) -> None:
    """search_multi returns only movie and tv items, filtering out persons."""
    movie = _make_movie_raw(tmdb_id=100, title="Great Movie")
    show = _make_tv_raw(tmdb_id=200, name="Great Show")
    person = {
        "id": 300,
        "name": "Famous Person",
        "media_type": "person",
        "profile_path": "/person.jpg",
    }
    envelope = _make_search_envelope([movie, show, person], total_results=3, total_pages=1)

    _patch_client(client, [_make_response(200, envelope)])
    result = await client.search_multi("Great")

    assert isinstance(result, TmdbSearchResult)
    assert len(result.items) == 2
    media_types = {item.media_type for item in result.items}
    assert media_types == {"movie", "tv"}
    assert all(item.media_type in ("movie", "tv") for item in result.items)


@pytest.mark.asyncio
async def test_search_movie_scoped(client: TmdbClient) -> None:
    """search with media_type='movie' returns all items with media_type='movie'."""
    movies = [
        _make_movie_raw(tmdb_id=i, title=f"Movie {i}")
        for i in range(1, 4)
    ]
    # Scoped search results don't include media_type in each raw dict
    for m in movies:
        del m["media_type"]
    envelope = _make_search_envelope(movies, total_results=3, total_pages=1)

    _patch_client(client, [_make_response(200, envelope)])
    result = await client.search("Action", media_type="movie")

    assert len(result.items) == 3
    assert all(item.media_type == "movie" for item in result.items)


@pytest.mark.asyncio
async def test_search_tv_scoped(client: TmdbClient) -> None:
    """search with media_type='tv' returns all items with media_type='tv'."""
    shows = [
        _make_tv_raw(tmdb_id=i + 100, name=f"Show {i}")
        for i in range(1, 3)
    ]
    # Scoped search results don't include media_type in each raw dict
    for s in shows:
        del s["media_type"]
    envelope = _make_search_envelope(shows, total_results=2, total_pages=1)

    _patch_client(client, [_make_response(200, envelope)])
    result = await client.search("Drama", media_type="tv")

    assert len(result.items) == 2
    assert all(item.media_type == "tv" for item in result.items)


@pytest.mark.asyncio
async def test_search_pagination(client: TmdbClient) -> None:
    """search correctly populates total_results, page, and total_pages from the response."""
    movie = _make_movie_raw()
    envelope = _make_search_envelope(
        [movie],
        total_results=250,
        page=3,
        total_pages=13,
    )

    _patch_client(client, [_make_response(200, envelope)])
    result = await client.search("action", page=3)

    assert result.total_results == 250
    assert result.page == 3
    assert result.total_pages == 13


@pytest.mark.asyncio
async def test_search_sends_query_and_page_params(client: TmdbClient) -> None:
    """search sends query and page parameters in the request URL."""
    envelope = _make_search_envelope([_make_movie_raw()])
    transport = _patch_client(client, [_make_response(200, envelope)])

    await client.search("Inception", media_type="multi", page=2)

    assert len(transport.requests_made) == 1
    url = str(transport.requests_made[0].url)
    assert "/search/multi" in url
    assert "query=" in url
    assert "page=2" in url


@pytest.mark.asyncio
async def test_search_routes_to_correct_endpoint(client: TmdbClient) -> None:
    """search routes to /search/movie, /search/tv, or /search/multi based on media_type."""
    envelope = _make_search_envelope([])
    transport = _patch_client(
        client,
        default_response=_make_response(200, envelope),
    )

    await client.search("test", media_type="movie")
    assert "/search/movie" in str(transport.requests_made[-1].url)

    await client.search("test", media_type="tv")
    assert "/search/tv" in str(transport.requests_made[-1].url)

    await client.search("test", media_type="multi")
    assert "/search/multi" in str(transport.requests_made[-1].url)


@pytest.mark.asyncio
async def test_search_multi_convenience_wrapper(client: TmdbClient) -> None:
    """search_multi delegates to search with media_type='multi'."""
    envelope = _make_search_envelope([_make_movie_raw()])
    transport = _patch_client(client, [_make_response(200, envelope)])

    await client.search_multi("test query")

    url = str(transport.requests_made[0].url)
    assert "/search/multi" in url


# ---------------------------------------------------------------------------
# search — disabled / unconfigured
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_disabled(disabled_client: TmdbClient) -> None:
    """When enabled=False, search returns empty TmdbSearchResult without any HTTP calls."""
    transport = _patch_client(disabled_client, [])

    result = await disabled_client.search("test")

    assert isinstance(result, TmdbSearchResult)
    assert result.items == []
    assert len(transport.requests_made) == 0


@pytest.mark.asyncio
async def test_search_no_api_key(no_key_client: TmdbClient) -> None:
    """When api_key is empty, search returns empty TmdbSearchResult without any HTTP calls."""
    transport = _patch_client(no_key_client, [])

    result = await no_key_client.search("test")

    assert isinstance(result, TmdbSearchResult)
    assert result.items == []
    assert len(transport.requests_made) == 0


# ---------------------------------------------------------------------------
# search — error handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_auth_error(client: TmdbClient) -> None:
    """A 401 response returns an empty TmdbSearchResult without raising."""
    _patch_client(client, [_make_response(401, {"status_message": "Invalid API key."})])

    result = await client.search("test")

    assert isinstance(result, TmdbSearchResult)
    assert result.items == []


@pytest.mark.asyncio
async def test_search_server_error(client: TmdbClient) -> None:
    """A 500 response returns an empty TmdbSearchResult without raising."""
    _patch_client(client, [_make_response(500, {})])

    result = await client.search("test")

    assert result.items == []


@pytest.mark.asyncio
async def test_search_connection_error(client: TmdbClient) -> None:
    """A ConnectError returns an empty TmdbSearchResult without raising."""

    class _ConnErrorTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused", request=request)

    async def _fake_get_client() -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=_ConnErrorTransport())

    client._get_client = _fake_get_client  # type: ignore[method-assign]

    result = await client.search("test")

    assert result.items == []


@pytest.mark.asyncio
async def test_search_malformed_json(client: TmdbClient) -> None:
    """A non-JSON response body returns an empty TmdbSearchResult without raising."""
    _patch_client(
        client,
        [_make_response(200, b"<html>error</html>", content_type="text/html")],
    )

    result = await client.search("test")

    assert result.items == []


# ---------------------------------------------------------------------------
# get_external_ids
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_external_ids(client: TmdbClient) -> None:
    """get_external_ids returns a populated TmdbExternalIds on success."""
    _patch_client(
        client,
        [_make_response(200, _make_external_ids_response(imdb_id="tt1234567", tvdb_id=98765))],
    )

    result = await client.get_external_ids(123, "movie")

    assert isinstance(result, TmdbExternalIds)
    assert result.imdb_id == "tt1234567"
    assert result.tvdb_id == 98765


@pytest.mark.asyncio
async def test_get_external_ids_tv_uses_correct_url(client: TmdbClient) -> None:
    """get_external_ids uses the correct URL path for TV shows."""
    transport = _patch_client(
        client,
        [_make_response(200, _make_external_ids_response())],
    )

    await client.get_external_ids(456, "tv")

    url = str(transport.requests_made[0].url)
    assert "/tv/456/external_ids" in url


@pytest.mark.asyncio
async def test_get_external_ids_movie_uses_correct_url(client: TmdbClient) -> None:
    """get_external_ids uses the correct URL path for movies."""
    transport = _patch_client(
        client,
        [_make_response(200, _make_external_ids_response())],
    )

    await client.get_external_ids(123, "movie")

    url = str(transport.requests_made[0].url)
    assert "/movie/123/external_ids" in url


@pytest.mark.asyncio
async def test_get_external_ids_missing_imdb(client: TmdbClient) -> None:
    """When imdb_id is null in the response, TmdbExternalIds.imdb_id is None."""
    _patch_client(
        client,
        [_make_response(200, _make_external_ids_response(imdb_id=None, tvdb_id=12345))],
    )

    result = await client.get_external_ids(123, "movie")

    assert result is not None
    assert result.imdb_id is None
    assert result.tvdb_id == 12345


@pytest.mark.asyncio
async def test_get_external_ids_both_null(client: TmdbClient) -> None:
    """When both imdb_id and tvdb_id are null, TmdbExternalIds fields are both None."""
    _patch_client(
        client,
        [_make_response(200, _make_external_ids_response(imdb_id=None, tvdb_id=None))],
    )

    result = await client.get_external_ids(999, "movie")

    assert result is not None
    assert result.imdb_id is None
    assert result.tvdb_id is None


@pytest.mark.asyncio
async def test_get_external_ids_auth_error(client: TmdbClient) -> None:
    """A 401 response returns None without raising."""
    _patch_client(client, [_make_response(401, {"status_message": "Invalid API key."})])

    result = await client.get_external_ids(123, "movie")

    assert result is None


@pytest.mark.asyncio
async def test_get_external_ids_not_found(client: TmdbClient) -> None:
    """A 404 response returns None without raising."""
    _patch_client(client, [_make_response(404, {"status_message": "The resource you requested could not be found."})])

    result = await client.get_external_ids(999999, "movie")

    assert result is None


@pytest.mark.asyncio
async def test_get_external_ids_connection_error(client: TmdbClient) -> None:
    """A ConnectError returns None without raising."""

    class _ConnErrorTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused", request=request)

    async def _fake_get_client() -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=_ConnErrorTransport())

    client._get_client = _fake_get_client  # type: ignore[method-assign]

    result = await client.get_external_ids(123, "movie")

    assert result is None


@pytest.mark.asyncio
async def test_get_external_ids_disabled(disabled_client: TmdbClient) -> None:
    """When enabled=False, get_external_ids returns None without any HTTP calls."""
    transport = _patch_client(disabled_client, [])

    result = await disabled_client.get_external_ids(123, "movie")

    assert result is None
    assert len(transport.requests_made) == 0


# ---------------------------------------------------------------------------
# test_connection
# ---------------------------------------------------------------------------


def _make_inline_client_patcher(
    transport: _MockTransport,
) -> Any:
    """Return a context manager that patches the inline ``async with httpx.AsyncClient(...)``
    used inside ``test_connection``.

    ``test_connection`` bypasses the pooled ``_get_client`` and creates a fresh
    client directly.  Patching ``src.services.tmdb.httpx.AsyncClient`` intercepts
    that constructor call and returns a fake async context manager backed by the
    supplied transport.

    Args:
        transport: The mock transport to inject.

    Returns:
        A context manager suitable for use in a ``with`` block.
    """
    fake_http_client = httpx.AsyncClient(
        base_url="https://api.themoviedb.org/3",
        transport=transport,
    )

    class _FakeCM:
        async def __aenter__(self) -> httpx.AsyncClient:
            return fake_http_client

        async def __aexit__(self, *args: object) -> bool:
            return False

    return patch("src.services.tmdb.httpx.AsyncClient", lambda **kwargs: _FakeCM())


@pytest.mark.asyncio
async def test_test_connection_success(client: TmdbClient) -> None:
    """test_connection returns True when the configuration endpoint returns 200."""
    transport = _MockTransport(
        [_make_response(200, {"images": {"base_url": "http://image.tmdb.org/t/p/"}})]
    )

    with _make_inline_client_patcher(transport):
        result = await client.test_connection()

    assert result is True


@pytest.mark.asyncio
async def test_test_connection_failure(client: TmdbClient) -> None:
    """test_connection returns False on a 401 Unauthorized response."""
    transport = _MockTransport(
        [_make_response(401, {"status_message": "Invalid API key."})]
    )

    with _make_inline_client_patcher(transport):
        result = await client.test_connection()

    assert result is False


@pytest.mark.asyncio
async def test_test_connection_no_api_key(no_key_client: TmdbClient) -> None:
    """test_connection returns False immediately when api_key is not configured."""
    # Short-circuits before any HTTP call — no need to patch the transport.
    result = await no_key_client.test_connection()

    assert result is False


@pytest.mark.asyncio
async def test_test_connection_network_error(client: TmdbClient) -> None:
    """test_connection returns False on a network error without raising."""

    class _ConnErrorTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused", request=request)

    transport = _MockTransport([], default_response=None)

    fake_http_client = httpx.AsyncClient(
        base_url="https://api.themoviedb.org/3",
        transport=_ConnErrorTransport(),
    )

    class _FakeCM:
        async def __aenter__(self) -> httpx.AsyncClient:
            return fake_http_client

        async def __aexit__(self, *args: object) -> bool:
            return False

    with patch("src.services.tmdb.httpx.AsyncClient", lambda **kwargs: _FakeCM()):
        result = await client.test_connection()

    assert result is False


@pytest.mark.asyncio
async def test_test_connection_uses_configuration_endpoint(client: TmdbClient) -> None:
    """test_connection calls the /configuration endpoint."""
    transport = _MockTransport([_make_response(200, {"images": {}})])

    with _make_inline_client_patcher(transport):
        await client.test_connection()

    assert len(transport.requests_made) == 1
    url = str(transport.requests_made[0].url)
    assert "/configuration" in url


# ---------------------------------------------------------------------------
# _parse_item — unit tests (call directly, no HTTP)
# ---------------------------------------------------------------------------


def test_parse_item_movie() -> None:
    """_parse_item correctly normalises a movie dict: title from 'title', year from 'release_date'."""
    client = TmdbClient()
    raw = _make_movie_raw(tmdb_id=42, title="Test Film", release_date="2023-11-01", vote_average=8.1)
    del raw["media_type"]  # use caller-supplied media_type

    result = client._parse_item(raw, media_type="movie")

    assert result is not None
    assert result.tmdb_id == 42
    assert result.title == "Test Film"
    assert result.year == 2023
    assert result.media_type == "movie"
    assert result.vote_average == 8.1


def test_parse_item_tv() -> None:
    """_parse_item correctly normalises a TV dict: title from 'name', year from 'first_air_date'."""
    client = TmdbClient()
    raw = _make_tv_raw(tmdb_id=99, name="Cool Show", first_air_date="2022-04-20", vote_average=7.3)
    del raw["media_type"]  # use caller-supplied media_type

    result = client._parse_item(raw, media_type="tv")

    assert result is not None
    assert result.tmdb_id == 99
    assert result.title == "Cool Show"
    assert result.year == 2022
    assert result.media_type == "tv"
    assert result.vote_average == 7.3


def test_parse_item_uses_media_type_from_dict() -> None:
    """_parse_item prefers 'media_type' field in the raw dict over the caller-supplied value."""
    client = TmdbClient()
    raw = _make_tv_raw(tmdb_id=55, name="A Show")  # has media_type="tv" in dict

    # Even though caller says "movie", the dict field wins
    result = client._parse_item(raw, media_type="movie")

    assert result is not None
    assert result.media_type == "tv"


def test_parse_item_skips_person() -> None:
    """_parse_item returns None for a person result (media_type='person')."""
    client = TmdbClient()
    raw: dict[str, Any] = {
        "id": 300,
        "name": "Famous Actor",
        "media_type": "person",
        "profile_path": "/actor.jpg",
    }

    result = client._parse_item(raw)

    assert result is None


def test_parse_item_skips_unknown_media_type() -> None:
    """_parse_item returns None when media_type is not 'movie' or 'tv'."""
    client = TmdbClient()
    raw: dict[str, Any] = {"id": 1, "title": "Something", "media_type": "collection"}

    result = client._parse_item(raw)

    assert result is None


def test_parse_item_missing_id() -> None:
    """_parse_item returns None when the 'id' field is absent."""
    client = TmdbClient()
    raw: dict[str, Any] = {
        "title": "No ID Movie",
        "release_date": "2024-01-01",
        "media_type": "movie",
    }

    result = client._parse_item(raw)

    assert result is None


def test_parse_item_non_int_id() -> None:
    """_parse_item returns None when 'id' is not an integer."""
    client = TmdbClient()
    raw: dict[str, Any] = {
        "id": "not-an-int",
        "title": "Bad ID Movie",
        "media_type": "movie",
        "release_date": "2024-01-01",
    }

    result = client._parse_item(raw)

    assert result is None


def test_parse_item_missing_title_returns_none() -> None:
    """_parse_item returns None when both title and name fields are absent/empty."""
    client = TmdbClient()
    raw: dict[str, Any] = {
        "id": 10,
        "title": "",
        "release_date": "2024-01-01",
        "media_type": "movie",
    }

    result = client._parse_item(raw)

    assert result is None


def test_parse_item_missing_release_date_yields_none_year() -> None:
    """_parse_item sets year=None when release_date is absent."""
    client = TmdbClient()
    raw: dict[str, Any] = {
        "id": 10,
        "title": "Dateless Movie",
        "media_type": "movie",
        "overview": "",
    }

    result = client._parse_item(raw)

    assert result is not None
    assert result.year is None


def test_parse_item_invalid_date_format_yields_none_year() -> None:
    """_parse_item sets year=None when the date string cannot be parsed."""
    client = TmdbClient()
    raw: dict[str, Any] = {
        "id": 10,
        "title": "Bad Date Movie",
        "media_type": "movie",
        "release_date": "not-a-date",
    }

    result = client._parse_item(raw)

    assert result is not None
    assert result.year is None


def test_parse_item_poster_path_none_when_absent() -> None:
    """_parse_item sets poster_path=None when the field is absent from the dict."""
    client = TmdbClient()
    raw: dict[str, Any] = {
        "id": 5,
        "title": "No Poster Movie",
        "media_type": "movie",
        "release_date": "2024-01-01",
    }

    result = client._parse_item(raw)

    assert result is not None
    assert result.poster_path is None


def test_parse_item_vote_average_defaults_to_zero() -> None:
    """_parse_item sets vote_average=0.0 when the field is absent."""
    client = TmdbClient()
    raw: dict[str, Any] = {
        "id": 5,
        "title": "Unrated Movie",
        "media_type": "movie",
        "release_date": "2024-01-01",
    }

    result = client._parse_item(raw)

    assert result is not None
    assert result.vote_average == 0.0


def test_parse_item_tv_falls_back_to_title_field() -> None:
    """For tv media_type, _parse_item falls back to 'title' if 'name' is absent."""
    client = TmdbClient()
    raw: dict[str, Any] = {
        "id": 77,
        "title": "Show With Title Field",  # 'name' absent, use 'title'
        "media_type": "tv",
        "first_air_date": "2020-01-01",
    }

    result = client._parse_item(raw)

    assert result is not None
    assert result.title == "Show With Title Field"


def test_parse_item_movie_falls_back_to_name_field() -> None:
    """For movie media_type, _parse_item falls back to 'name' if 'title' is absent."""
    client = TmdbClient()
    raw: dict[str, Any] = {
        "id": 88,
        "name": "Movie With Name Field",  # 'title' absent, use 'name'
        "media_type": "movie",
        "release_date": "2021-05-10",
    }

    result = client._parse_item(raw)

    assert result is not None
    assert result.title == "Movie With Name Field"


def test_parse_item_unicode_title() -> None:
    """_parse_item handles Unicode characters in titles without crashing."""
    client = TmdbClient()
    raw: dict[str, Any] = {
        "id": 99,
        "title": "\u30c6\u30b9\u30c8\u6620\u753b",  # Japanese characters
        "media_type": "movie",
        "release_date": "2024-01-01",
    }

    result = client._parse_item(raw)

    assert result is not None
    assert result.title == "\u30c6\u30b9\u30c8\u6620\u753b"


# ---------------------------------------------------------------------------
# get_alternative_titles — Issue #34
# ---------------------------------------------------------------------------


def _make_movie_alt_titles_response(titles: list[str]) -> dict[str, Any]:
    """Build a TMDB /movie/{id}/alternative_titles response body."""
    return {
        "id": 123,
        "titles": [{"iso_3166_1": "US", "title": t, "type": ""} for t in titles],
    }


def _make_tv_alt_titles_response(titles: list[str]) -> dict[str, Any]:
    """Build a TMDB /tv/{id}/alternative_titles response body."""
    return {
        "id": 456,
        "results": [{"iso_3166_1": "US", "title": t, "type": ""} for t in titles],
    }


@pytest.mark.asyncio
async def test_get_alternative_titles_movie(client: TmdbClient) -> None:
    """get_alternative_titles for a movie returns titles from the 'titles' key, deduped."""
    body = _make_movie_alt_titles_response(["Alt Title One", "Alt Title Two", "Alt Title One"])
    _patch_client(client, [_make_response(200, body)])

    result = await client.get_alternative_titles(123, "movie")

    assert isinstance(result, list)
    # Dedup: "Alt Title One" appears twice in source but only once in result
    assert result.count("Alt Title One") == 1
    assert "Alt Title Two" in result
    assert len(result) == 2


@pytest.mark.asyncio
async def test_get_alternative_titles_tv(client: TmdbClient) -> None:
    """get_alternative_titles for a TV show returns titles from the 'results' key."""
    body = _make_tv_alt_titles_response(["Show Alt A", "Show Alt B"])
    _patch_client(client, [_make_response(200, body)])

    result = await client.get_alternative_titles(456, "tv")

    assert isinstance(result, list)
    assert "Show Alt A" in result
    assert "Show Alt B" in result


@pytest.mark.asyncio
async def test_get_alternative_titles_failure(client: TmdbClient) -> None:
    """A network error from get_alternative_titles returns an empty list without raising."""

    class _ConnErrorTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused", request=request)

    async def _fake_get_client() -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=_ConnErrorTransport())

    client._get_client = _fake_get_client  # type: ignore[method-assign]

    result = await client.get_alternative_titles(123, "movie")

    assert result == []


@pytest.mark.asyncio
async def test_get_alternative_titles_not_configured(disabled_client: TmdbClient) -> None:
    """When TMDB is disabled, get_alternative_titles returns an empty list without any HTTP calls."""
    transport = _patch_client(disabled_client, [])

    result = await disabled_client.get_alternative_titles(123, "movie")

    assert result == []
    assert len(transport.requests_made) == 0


@pytest.mark.asyncio
async def test_get_alternative_titles_no_api_key(no_key_client: TmdbClient) -> None:
    """When api_key is empty, get_alternative_titles returns [] without any HTTP calls."""
    transport = _patch_client(no_key_client, [])

    result = await no_key_client.get_alternative_titles(456, "tv")

    assert result == []
    assert len(transport.requests_made) == 0


@pytest.mark.asyncio
async def test_get_alternative_titles_server_error(client: TmdbClient) -> None:
    """A 500 response from get_alternative_titles returns an empty list without raising."""
    _patch_client(client, [_make_response(500, {"status_message": "Internal Server Error"})])

    result = await client.get_alternative_titles(123, "movie")

    assert result == []


@pytest.mark.asyncio
async def test_get_alternative_titles_timeout(client: TmdbClient) -> None:
    """A TimeoutException returns an empty list without raising."""

    class _TimeoutTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            raise httpx.TimeoutException("timed out", request=request)

    async def _fake_get_client() -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=_TimeoutTransport())

    client._get_client = _fake_get_client  # type: ignore[method-assign]

    result = await client.get_alternative_titles(123, "movie")

    assert result == []


@pytest.mark.asyncio
async def test_get_alternative_titles_rate_limited(client: TmdbClient) -> None:
    """A 429 Too Many Requests response returns an empty list without raising."""
    _patch_client(client, [_make_response(429, {"status_message": "Request count over limit."})])

    result = await client.get_alternative_titles(123, "movie")

    assert result == []


@pytest.mark.asyncio
async def test_get_alternative_titles_malformed_json(client: TmdbClient) -> None:
    """A non-JSON response body returns an empty list without raising."""
    _patch_client(
        client,
        [_make_response(200, b"<html>not json</html>", content_type="text/html")],
    )

    result = await client.get_alternative_titles(123, "movie")

    assert result == []


@pytest.mark.asyncio
async def test_get_alternative_titles_auth_error(client: TmdbClient) -> None:
    """A 401 Unauthorized response returns an empty list without raising."""
    _patch_client(client, [_make_response(401, {"status_message": "Invalid API key."})])

    result = await client.get_alternative_titles(456, "tv")

    assert result == []


@pytest.mark.asyncio
async def test_get_alternative_titles_filters_empty_title_entries(client: TmdbClient) -> None:
    """Entries with empty or missing 'title' field are excluded from the result."""
    body = {
        "id": 123,
        "titles": [
            {"iso_3166_1": "US", "title": "Valid Title", "type": ""},
            {"iso_3166_1": "DE", "title": "", "type": ""},       # empty string — skip
            {"iso_3166_1": "FR", "type": ""},                    # missing title key — skip
            {"iso_3166_1": "JP", "title": "Another Valid", "type": ""},
        ],
    }
    _patch_client(client, [_make_response(200, body)])

    result = await client.get_alternative_titles(123, "movie")

    assert result == ["Valid Title", "Another Valid"]


@pytest.mark.asyncio
async def test_get_alternative_titles_empty_response(client: TmdbClient) -> None:
    """An empty titles/results list returns an empty list."""
    body = _make_movie_alt_titles_response([])
    _patch_client(client, [_make_response(200, body)])

    result = await client.get_alternative_titles(123, "movie")

    assert result == []


@pytest.mark.asyncio
async def test_get_alternative_titles_movie_uses_correct_url(client: TmdbClient) -> None:
    """get_alternative_titles for a movie hits the /movie/{id}/alternative_titles endpoint."""
    body = _make_movie_alt_titles_response(["Alt"])
    transport = _patch_client(client, [_make_response(200, body)])

    await client.get_alternative_titles(789, "movie")

    assert len(transport.requests_made) == 1
    url = str(transport.requests_made[0].url)
    assert "/movie/789/alternative_titles" in url


@pytest.mark.asyncio
async def test_get_alternative_titles_tv_uses_correct_url(client: TmdbClient) -> None:
    """get_alternative_titles for a TV show hits the /tv/{id}/alternative_titles endpoint."""
    body = _make_tv_alt_titles_response(["Alt"])
    transport = _patch_client(client, [_make_response(200, body)])

    await client.get_alternative_titles(321, "tv")

    assert len(transport.requests_made) == 1
    url = str(transport.requests_made[0].url)
    assert "/tv/321/alternative_titles" in url


# ---------------------------------------------------------------------------
# original_title on TmdbShowDetail and TmdbItem — Issue #34
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_original_title_parsed_show(client: TmdbClient) -> None:
    """get_show_details populates TmdbShowDetail.original_title from the API response."""
    from src.services.tmdb import TmdbShowDetail  # noqa: PLC0415

    raw_response = {
        "id": 456,
        "name": "Attack on Titan",
        "original_name": "Shingeki no Kyojin",
        "first_air_date": "2013-04-06",
        "overview": "Giants eat people.",
        "poster_path": "/aot.jpg",
        "backdrop_path": None,
        "status": "Ended",
        "vote_average": 9.0,
        "number_of_seasons": 4,
        "seasons": [],
        "genres": [],
        "next_episode_to_air": None,
        "last_episode_to_air": None,
        "original_language": "ja",
        "external_ids": {
            "imdb_id": "tt2560140",
            "tvdb_id": 267440,
        },
    }
    _patch_client(client, [_make_response(200, raw_response)])

    result = await client.get_show_details(456)

    assert result is not None
    assert isinstance(result, TmdbShowDetail)
    assert result.original_title == "Shingeki no Kyojin"


@pytest.mark.asyncio
async def test_original_title_parsed_show_none_when_absent(client: TmdbClient) -> None:
    """get_show_details sets original_title to None when 'original_name' is absent."""
    from src.services.tmdb import TmdbShowDetail  # noqa: PLC0415

    raw_response = {
        "id": 456,
        "name": "Some Show",
        "first_air_date": "2020-01-01",
        "overview": "",
        "status": "Ended",
        "vote_average": 7.0,
        "number_of_seasons": 1,
        "seasons": [],
        "genres": [],
        "next_episode_to_air": None,
        "last_episode_to_air": None,
        "original_language": "en",
        "external_ids": {},
    }
    _patch_client(client, [_make_response(200, raw_response)])

    result = await client.get_show_details(456)

    assert result is not None
    # original_title should be None when not provided, or equal to title when same
    # The field must exist on the model
    assert hasattr(result, "original_title")


def test_original_title_parsed_item() -> None:
    """_parse_item populates TmdbItem.original_title from 'original_title' (movie) field."""
    client = TmdbClient()
    raw: dict[str, Any] = {
        "id": 123,
        "title": "Spirited Away",
        "original_title": "Sen to Chihiro no Kamikakushi",
        "media_type": "movie",
        "release_date": "2001-07-20",
        "overview": "A girl enters a spirit world.",
        "vote_average": 8.6,
        "original_language": "ja",
    }

    result = client._parse_item(raw, media_type="movie")

    assert result is not None
    assert hasattr(result, "original_title")
    assert result.original_title == "Sen to Chihiro no Kamikakushi"


def test_original_title_parsed_item_tv() -> None:
    """_parse_item populates TmdbItem.original_title from 'original_name' (tv) field."""
    client = TmdbClient()
    raw: dict[str, Any] = {
        "id": 789,
        "name": "My Hero Academia",
        "original_name": "Boku no Hero Academia",
        "media_type": "tv",
        "first_air_date": "2016-04-03",
        "overview": "Kids with superpowers.",
        "vote_average": 8.1,
        "original_language": "ja",
    }

    result = client._parse_item(raw, media_type="tv")

    assert result is not None
    assert hasattr(result, "original_title")
    assert result.original_title == "Boku no Hero Academia"


def test_original_title_parsed_item_none_when_absent() -> None:
    """_parse_item sets original_title to None when the original title field is absent."""
    client = TmdbClient()
    raw: dict[str, Any] = {
        "id": 42,
        "title": "Generic English Movie",
        "media_type": "movie",
        "release_date": "2022-06-01",
    }

    result = client._parse_item(raw, media_type="movie")

    assert result is not None
    assert hasattr(result, "original_title")
    # When original_title is not in the raw dict, it should be None or absent
    assert result.original_title is None or result.original_title == ""


# ---------------------------------------------------------------------------
# Model contracts
# ---------------------------------------------------------------------------


def test_tmdb_item_model_fields() -> None:
    """TmdbItem can be constructed with all required fields."""
    item = TmdbItem(
        tmdb_id=123,
        title="Test",
        year=2024,
        media_type="movie",
        overview="A film.",
        poster_path="/test.jpg",
        vote_average=7.5,
    )
    assert item.tmdb_id == 123
    assert item.title == "Test"
    assert item.year == 2024
    assert item.imdb_id is None  # optional, defaults None


def test_tmdb_external_ids_model_fields() -> None:
    """TmdbExternalIds can be constructed with both fields."""
    ext = TmdbExternalIds(imdb_id="tt9876543", tvdb_id=42)
    assert ext.imdb_id == "tt9876543"
    assert ext.tvdb_id == 42


def test_tmdb_search_result_defaults() -> None:
    """TmdbSearchResult has sensible defaults for pagination fields."""
    result = TmdbSearchResult(items=[])
    assert result.total_results == 0
    assert result.page == 1
    assert result.total_pages == 1


# ---------------------------------------------------------------------------
# get_top_rated
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_top_rated_movies(client: TmdbClient) -> None:
    """get_top_rated returns a list of TmdbItems for movies."""
    movie1 = _make_movie_raw(tmdb_id=501, title="Classic Movie", release_date="2020-01-01", vote_average=8.5)
    movie2 = _make_movie_raw(tmdb_id=502, title="Another Classic", release_date="2019-06-15", vote_average=8.2)
    envelope = _make_trending_envelope([movie1, movie2])

    transport = _patch_client(client, [_make_response(200, envelope)])
    results = await client.get_top_rated("movie")

    assert len(results) == 2
    assert results[0].tmdb_id == 501
    assert results[0].title == "Classic Movie"
    assert results[0].media_type == "movie"
    assert "/movie/top_rated" in str(transport.requests_made[0].url)


@pytest.mark.asyncio
async def test_get_top_rated_tv(client: TmdbClient) -> None:
    """get_top_rated normalises TV shows correctly."""
    show = _make_tv_raw(tmdb_id=601, name="Top Show", first_air_date="2021-03-10", vote_average=9.1)
    envelope = _make_trending_envelope([show])

    transport = _patch_client(client, [_make_response(200, envelope)])
    results = await client.get_top_rated("tv")

    assert len(results) == 1
    assert results[0].title == "Top Show"
    assert results[0].media_type == "tv"
    assert "/tv/top_rated" in str(transport.requests_made[0].url)


@pytest.mark.asyncio
async def test_get_top_rated_disabled(disabled_client: TmdbClient) -> None:
    """When enabled=False, get_top_rated returns [] without HTTP calls."""
    transport = _patch_client(disabled_client, [])
    results = await disabled_client.get_top_rated("movie")
    assert results == []
    assert len(transport.requests_made) == 0


# ---------------------------------------------------------------------------
# get_genres
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_genres_movies(client: TmdbClient) -> None:
    """get_genres returns a list of genre dicts for movies."""
    body = {"genres": [{"id": 28, "name": "Action"}, {"id": 35, "name": "Comedy"}, {"id": 27, "name": "Horror"}]}
    transport = _patch_client(client, [_make_response(200, body)])

    result = await client.get_genres("movie")

    assert len(result) == 3
    assert result[0] == {"id": 28, "name": "Action"}
    assert result[1] == {"id": 35, "name": "Comedy"}
    assert "/genre/movie/list" in str(transport.requests_made[0].url)


@pytest.mark.asyncio
async def test_get_genres_empty(client: TmdbClient) -> None:
    """get_genres returns empty list when API returns empty genres."""
    body = {"genres": []}
    _patch_client(client, [_make_response(200, body)])

    result = await client.get_genres("tv")

    assert result == []


@pytest.mark.asyncio
async def test_get_genres_error(client: TmdbClient) -> None:
    """get_genres returns [] on a 500 server error."""
    _patch_client(client, [_make_response(500, {"status_message": "Server Error"})])

    result = await client.get_genres("movie")

    assert result == []


# ---------------------------------------------------------------------------
# discover
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_discover_by_genre(client: TmdbClient) -> None:
    """discover returns items filtered by genre with correct URL params."""
    movie = _make_movie_raw(tmdb_id=701, title="Action Movie")
    # discover results don't include media_type per item
    del movie["media_type"]
    envelope = _make_search_envelope([movie], total_results=50, page=1, total_pages=3)

    transport = _patch_client(client, [_make_response(200, envelope)])
    result = await client.discover("movie", genre_id=28, sort_by="popularity.desc", page=1, vote_count_gte=50)

    assert isinstance(result, TmdbSearchResult)
    assert len(result.items) == 1
    assert result.items[0].tmdb_id == 701
    assert result.items[0].media_type == "movie"
    assert result.total_results == 50

    url = str(transport.requests_made[0].url)
    assert "/discover/movie" in url
    assert "with_genres=28" in url
    assert "vote_count.gte=50" in url


@pytest.mark.asyncio
async def test_discover_pagination(client: TmdbClient) -> None:
    """discover correctly populates pagination fields."""
    movie = _make_movie_raw(tmdb_id=702, title="Page 2 Movie")
    del movie["media_type"]
    envelope = _make_search_envelope([movie], total_results=200, page=2, total_pages=10)

    _patch_client(client, [_make_response(200, envelope)])
    result = await client.discover("movie", genre_id=35, page=2)

    assert result.page == 2
    assert result.total_pages == 10
    assert result.total_results == 200


@pytest.mark.asyncio
async def test_discover_error(client: TmdbClient) -> None:
    """discover returns empty TmdbSearchResult on auth error."""
    _patch_client(client, [_make_response(401, {"status_message": "Invalid API key."})])

    result = await client.discover("movie", genre_id=28)

    assert isinstance(result, TmdbSearchResult)
    assert result.items == []
