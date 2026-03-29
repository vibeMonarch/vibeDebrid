"""Tests for src/services/omdb.py and GET /api/omdb/{imdb_id}.

All external HTTP calls are intercepted via a _MockTransport so no real
network traffic is generated.  Each test exercises a single behaviour.

Mocking pattern mirrors test_tmdb.py exactly:
  - monkeypatch replaces src.services.omdb.settings with a MagicMock.
  - _MockTransport intercepts httpx requests and returns canned responses.
  - _patch_client() replaces client._get_client to inject the mock transport.
  - _make_response() builds fake httpx.Response objects.
  - _make_mock_cfg() creates a MagicMock that looks like OmdbConfig.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from httpx import ASGITransport
from httpx import AsyncClient as HttpxTestClient

from src.services.http_client import CircuitBreaker
from src.services.omdb import OmdbClient, OmdbRatings

# ---------------------------------------------------------------------------
# Helpers — mock infrastructure
# ---------------------------------------------------------------------------


def _make_noop_breaker() -> CircuitBreaker:
    """Return a CircuitBreaker that is always CLOSED (never rejects requests)."""
    return CircuitBreaker("test", failure_threshold=999, recovery_timeout=0.0)


def _make_mock_cfg(
    enabled: bool = True,
    api_key: str = "test-omdb-key",
    base_url: str = "http://www.omdbapi.com",
    cache_hours: int = 168,
    timeout_seconds: int = 10,
) -> MagicMock:
    """Return a MagicMock that looks like an OmdbConfig object."""
    cfg = MagicMock()
    cfg.enabled = enabled
    cfg.api_key = api_key
    cfg.base_url = base_url
    cfg.cache_hours = cache_hours
    cfg.timeout_seconds = timeout_seconds
    return cfg


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
        request=httpx.Request("GET", "http://www.omdbapi.com/"),
    )


class _MockTransport(httpx.AsyncBaseTransport):
    """Sequential mock transport that returns canned httpx.Response objects."""

    def __init__(self, responses: list[httpx.Response]) -> None:
        self._responses = list(responses)
        self._index = 0
        self.requests_made: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests_made.append(request)
        if self._index >= len(self._responses):
            raise RuntimeError(
                f"_MockTransport: no response configured for request #{self._index + 1}"
            )
        resp = self._responses[self._index]
        self._index += 1
        resp.request = request  # type: ignore[attr-defined]
        return resp


def _patch_client(
    client: OmdbClient,
    responses: list[httpx.Response],
) -> _MockTransport:
    """Monkey-patch ``client._get_client`` to inject a _MockTransport.

    Args:
        client:    The OmdbClient under test.
        responses: Ordered list of responses to return.

    Returns:
        The _MockTransport so tests can inspect ``transport.requests_made``.
    """
    transport = _MockTransport(responses)

    async def _fake_get_client() -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url="http://www.omdbapi.com",
            transport=transport,
        )

    client._get_client = _fake_get_client  # type: ignore[method-assign]
    return transport


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_full_omdb_response(
    imdb_id: str = "tt0133093",
    title: str = "The Matrix",
    imdb_rating: str = "8.7",
    imdb_votes: str = "2,000,000",
    rt_score: str = "88%",
    metascore: str = "73",
) -> dict[str, Any]:
    """Build a representative OMDb API response dict."""
    return {
        "Title": title,
        "imdbID": imdb_id,
        "imdbRating": imdb_rating,
        "imdbVotes": imdb_votes,
        "Ratings": [
            {"Source": "Internet Movie Database", "Value": f"{imdb_rating}/10"},
            {"Source": "Rotten Tomatoes", "Value": rt_score},
            {"Source": "Metacritic", "Value": f"{metascore}/100"},
        ],
        "Metascore": metascore,
        "Response": "True",
    }


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch) -> OmdbClient:
    """An OmdbClient with test defaults (no real HTTP, empty cache)."""
    cfg = _make_mock_cfg()
    mock_settings = MagicMock()
    mock_settings.omdb = cfg
    monkeypatch.setattr("src.services.omdb.settings", mock_settings)
    monkeypatch.setattr(
        "src.services.omdb.get_circuit_breaker",
        lambda *args, **kwargs: _make_noop_breaker(),
    )
    return OmdbClient()


@pytest.fixture()
def disabled_client(monkeypatch: pytest.MonkeyPatch) -> OmdbClient:
    """An OmdbClient where enabled=False — all calls should short-circuit."""
    cfg = _make_mock_cfg(enabled=False)
    mock_settings = MagicMock()
    mock_settings.omdb = cfg
    monkeypatch.setattr("src.services.omdb.settings", mock_settings)
    return OmdbClient()


@pytest.fixture()
def no_key_client(monkeypatch: pytest.MonkeyPatch) -> OmdbClient:
    """An OmdbClient where api_key is empty — all calls should short-circuit."""
    cfg = _make_mock_cfg(api_key="")
    mock_settings = MagicMock()
    mock_settings.omdb = cfg
    monkeypatch.setattr("src.services.omdb.settings", mock_settings)
    return OmdbClient()


# ---------------------------------------------------------------------------
# get_ratings — happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_ratings_success(client: OmdbClient) -> None:
    """get_ratings parses all fields from a full OMDb response."""
    body = _make_full_omdb_response(
        imdb_rating="8.7",
        imdb_votes="2,000,000",
        rt_score="88%",
        metascore="73",
    )
    _patch_client(client, [_make_response(200, body)])

    result = await client.get_ratings("tt0133093")

    assert result is not None
    assert isinstance(result, OmdbRatings)
    assert result.imdb_rating == 8.7
    assert result.imdb_votes == "2,000,000"
    assert result.rt_score == 88
    assert result.metascore == 73


@pytest.mark.asyncio
async def test_get_ratings_partial(client: OmdbClient) -> None:
    """get_ratings returns None for missing RT/Metascore fields."""
    body = {
        "Title": "Obscure Film",
        "imdbRating": "6.2",
        "imdbVotes": "500",
        "Ratings": [],   # no Rotten Tomatoes entry
        "Metascore": "N/A",
        "Response": "True",
    }
    _patch_client(client, [_make_response(200, body)])

    result = await client.get_ratings("tt9999999")

    assert result is not None
    assert result.imdb_rating == 6.2
    assert result.imdb_votes == "500"
    assert result.rt_score is None
    assert result.metascore is None


@pytest.mark.asyncio
async def test_get_ratings_na_values(client: OmdbClient) -> None:
    """'N/A' in imdbRating and Metascore fields should produce None."""
    body = {
        "Title": "Unrated Film",
        "imdbRating": "N/A",
        "imdbVotes": "N/A",
        "Ratings": [],
        "Metascore": "N/A",
        "Response": "True",
    }
    _patch_client(client, [_make_response(200, body)])

    result = await client.get_ratings("tt1111111")

    assert result is not None
    assert result.imdb_rating is None
    assert result.imdb_votes is None
    assert result.rt_score is None
    assert result.metascore is None


# ---------------------------------------------------------------------------
# get_ratings — cache behaviour
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_ratings_cache_hit(client: OmdbClient) -> None:
    """A second call for the same IMDB ID returns cached data without HTTP."""
    body = _make_full_omdb_response()
    transport = _patch_client(client, [_make_response(200, body)])

    first = await client.get_ratings("tt0133093")
    second = await client.get_ratings("tt0133093")

    assert first == second
    # Only one HTTP request despite two calls
    assert len(transport.requests_made) == 1


@pytest.mark.asyncio
async def test_get_ratings_cache_expired(client: OmdbClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """After TTL expires the client fetches fresh data and makes a new HTTP call."""
    import time as _time

    body = _make_full_omdb_response()
    transport = _patch_client(client, [_make_response(200, body), _make_response(200, body)])

    # Seed the cache with a TTL of 0 (already expired)
    ratings = OmdbRatings(imdb_rating=7.0)
    client._cache["tt0133093"] = (_time.monotonic() - 1, ratings)

    result = await client.get_ratings("tt0133093")

    assert result is not None
    # Should have hit the network because the cache entry was stale
    assert len(transport.requests_made) == 1


# ---------------------------------------------------------------------------
# get_ratings — disabled / unconfigured
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_ratings_disabled(disabled_client: OmdbClient) -> None:
    """When enabled=False, get_ratings returns None without any HTTP call."""
    # No transport patched — any HTTP attempt would raise RuntimeError
    result = await disabled_client.get_ratings("tt0133093")
    assert result is None


@pytest.mark.asyncio
async def test_get_ratings_no_api_key(no_key_client: OmdbClient) -> None:
    """When api_key is empty, get_ratings returns None without any HTTP call."""
    result = await no_key_client.get_ratings("tt0133093")
    assert result is None


# ---------------------------------------------------------------------------
# get_ratings — error handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_ratings_network_error(client: OmdbClient) -> None:
    """ConnectError is caught, logged, and returns None."""
    async def _raise() -> httpx.AsyncClient:
        raise httpx.ConnectError("refused")

    client._get_client = _raise  # type: ignore[method-assign]

    result = await client.get_ratings("tt0133093")
    assert result is None


@pytest.mark.asyncio
async def test_get_ratings_timeout(client: OmdbClient) -> None:
    """TimeoutException is caught, logged, and returns None."""
    async def _raise() -> httpx.AsyncClient:
        raise httpx.TimeoutException("timed out")

    client._get_client = _raise  # type: ignore[method-assign]

    result = await client.get_ratings("tt0133093")
    assert result is None


@pytest.mark.asyncio
async def test_get_ratings_circuit_open(client: OmdbClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """When the circuit breaker is OPEN, get_ratings returns None immediately."""
    open_breaker = CircuitBreaker("omdb_test", failure_threshold=1, recovery_timeout=9999.0)
    # Force it open by recording a failure
    await open_breaker.record_failure()

    monkeypatch.setattr(
        "src.services.omdb.get_circuit_breaker",
        lambda *args, **kwargs: open_breaker,
    )

    # No transport patched — any HTTP attempt would raise RuntimeError
    result = await client.get_ratings("tt0133093")
    assert result is None


@pytest.mark.asyncio
async def test_get_ratings_invalid_json(client: OmdbClient) -> None:
    """A malformed (non-JSON) response body returns None."""
    _patch_client(client, [_make_response(200, b"not-json", content_type="text/plain")])

    result = await client.get_ratings("tt0133093")
    assert result is None


@pytest.mark.asyncio
async def test_get_ratings_api_error_response(client: OmdbClient) -> None:
    """OMDb in-band error (Response=False) returns None and does not crash."""
    body = {"Response": "False", "Error": "Movie not found!"}
    _patch_client(client, [_make_response(200, body)])

    result = await client.get_ratings("tt0000000")
    assert result is None


# ---------------------------------------------------------------------------
# test_connection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_connection_success(client: OmdbClient) -> None:
    """test_connection returns a dict with 'Title' on success."""
    body = _make_full_omdb_response(title="The Matrix")
    _patch_client(client, [_make_response(200, body)])

    result = await client.test_connection()

    assert result is not None
    assert isinstance(result, dict)
    assert result.get("Title") == "The Matrix"


@pytest.mark.asyncio
async def test_connection_disabled(disabled_client: OmdbClient) -> None:
    """test_connection returns None when OMDb is disabled."""
    result = await disabled_client.test_connection()
    assert result is None


@pytest.mark.asyncio
async def test_connection_no_api_key(no_key_client: OmdbClient) -> None:
    """test_connection returns None when api_key is empty."""
    result = await no_key_client.test_connection()
    assert result is None


# ---------------------------------------------------------------------------
# API route tests — GET /api/omdb/{imdb_id}
# ---------------------------------------------------------------------------


@pytest.fixture
async def http_client() -> HttpxTestClient:
    """Async HTTP test client backed by the FastAPI app."""
    from src.main import app
    transport = ASGITransport(app=app)
    async with HttpxTestClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_omdb_route_success(http_client: HttpxTestClient) -> None:
    """GET /api/omdb/{imdb_id} returns ratings when omdb_client succeeds."""
    ratings = OmdbRatings(imdb_rating=8.7, imdb_votes="2,000,000", rt_score=88, metascore=73)

    with patch("src.api.routes.omdb.omdb_client") as mock_client:
        mock_client.get_ratings = AsyncMock(return_value=ratings)
        response = await http_client.get("/api/omdb/tt0133093")

    assert response.status_code == 200
    data = response.json()
    assert data["imdb_rating"] == 8.7
    assert data["rt_score"] == 88
    assert data["metascore"] == 73
    assert data["imdb_votes"] == "2,000,000"


@pytest.mark.asyncio
async def test_omdb_route_disabled(http_client: HttpxTestClient) -> None:
    """GET /api/omdb/{imdb_id} returns empty dict when omdb_client returns None."""
    with patch("src.api.routes.omdb.omdb_client") as mock_client:
        mock_client.get_ratings = AsyncMock(return_value=None)
        response = await http_client.get("/api/omdb/tt0133093")

    assert response.status_code == 200
    assert response.json() == {}


@pytest.mark.asyncio
async def test_omdb_route_invalid_id(http_client: HttpxTestClient) -> None:
    """GET /api/omdb/{id} with a non-tt ID returns empty dict without calling the service."""
    with patch("src.api.routes.omdb.omdb_client") as mock_client:
        mock_client.get_ratings = AsyncMock(return_value=None)
        response = await http_client.get("/api/omdb/notanimdbid")

    assert response.status_code == 200
    assert response.json() == {}
    # The client should not have been called
    mock_client.get_ratings.assert_not_called()
