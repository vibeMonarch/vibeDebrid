"""Tests for src/services/jellyfin.py.

All external HTTP calls are intercepted by a custom _MockTransport so no real
network traffic is generated.  Each test exercises a single behaviour.

Mocking strategy:
  - monkeypatch patches src.services.jellyfin.settings so the mock config
    stays alive during await calls.
  - _MockTransport records every request in .requests_made for URL assertions.
  - _patch_get_client() replaces src.services.jellyfin.get_client (async) to
    inject the mock transport for get_libraries / scan_library.
  - test_connection uses an inline httpx.AsyncClient — those tests patch
    src.services.jellyfin.httpx.AsyncClient directly.
  - _make_response() builds fake httpx.Response objects.
  - _make_mock_cfg() creates a MagicMock that looks like JellyfinConfig.
  - _make_noop_breaker() produces a CircuitBreaker that never trips.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src.services.jellyfin import JellyfinClient, JellyfinLibrary
from src.services.http_client import CircuitBreaker


# ---------------------------------------------------------------------------
# Helpers — mock data builders
# ---------------------------------------------------------------------------


def _make_virtual_folders_response(
    folders: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Build a Jellyfin /Library/VirtualFolders JSON response."""
    if folders is None:
        folders = [
            {"ItemId": "aa-111", "Name": "Movies", "CollectionType": "movies"},
            {"ItemId": "bb-222", "Name": "TV Shows", "CollectionType": "tvshows"},
            {"ItemId": "cc-333", "Name": "Music", "CollectionType": "music"},
        ]
    return folders


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
        request=httpx.Request("GET", "http://jellyfin.local:8096/"),
    )


# ---------------------------------------------------------------------------
# Mock transport
# ---------------------------------------------------------------------------


class _MockTransport(httpx.AsyncBaseTransport):
    """Sequential-queue mock transport that records every request made.

    Responses are popped from the front of the queue in order.  When the queue
    is exhausted the transport raises RuntimeError so tests fail loudly rather
    than silently returning stale data.
    """

    def __init__(self, responses: list[httpx.Response]) -> None:
        self._queue = list(responses)
        self._queue_index = 0
        self.requests_made: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests_made.append(request)

        if self._queue_index >= len(self._queue):
            raise RuntimeError(
                f"_MockTransport: no response configured for URL {str(request.url)!r}"
            )

        resp = self._queue[self._queue_index]
        self._queue_index += 1
        resp.request = request  # type: ignore[attr-defined]
        return resp


def _make_mock_cfg(
    url: str = "http://jellyfin.local:8096",
    api_key: str = "test-jellyfin-key",
    movie_library_ids: list[str] | None = None,
    show_library_ids: list[str] | None = None,
) -> MagicMock:
    """Return a MagicMock that looks like a JellyfinConfig object."""
    cfg = MagicMock()
    cfg.url = url
    cfg.api_key = api_key
    cfg.movie_library_ids = movie_library_ids or []
    cfg.show_library_ids = show_library_ids or []
    cfg.scan_after_symlink = True
    cfg.enabled = True
    return cfg


def _make_noop_breaker() -> CircuitBreaker:
    """Return a CircuitBreaker that never opens (infinite threshold)."""
    return CircuitBreaker("jellyfin_test", failure_threshold=10_000)


def _patch_get_client(
    monkeypatch: pytest.MonkeyPatch,
    responses: list[httpx.Response],
) -> _MockTransport:
    """Monkey-patch src.services.jellyfin.get_client (async) to inject a _MockTransport.

    Used for get_libraries and scan_library which use the pooled client.

    Args:
        monkeypatch: pytest MonkeyPatch fixture.
        responses:   Ordered responses for the Jellyfin server client.

    Returns:
        The _MockTransport so callers can inspect ``requests_made``.
    """
    local_transport = _MockTransport(responses)

    async def _fake_get_client(
        service_name: str,
        base_url: str,
        *,
        headers: dict[str, str] | None = None,
        timeout: float = 10.0,
        **kwargs: Any,
    ) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=base_url,
            transport=local_transport,
        )

    monkeypatch.setattr("src.services.jellyfin.get_client", _fake_get_client)
    return local_transport


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch) -> JellyfinClient:
    """A JellyfinClient with a patched settings singleton and noop circuit breaker."""
    cfg = _make_mock_cfg()
    mock_settings = MagicMock()
    mock_settings.jellyfin = cfg
    monkeypatch.setattr("src.services.jellyfin.settings", mock_settings)
    monkeypatch.setattr(
        "src.services.jellyfin.get_circuit_breaker",
        lambda *a, **kw: _make_noop_breaker(),
    )
    return JellyfinClient()


@pytest.fixture()
def no_key_client(monkeypatch: pytest.MonkeyPatch) -> JellyfinClient:
    """A JellyfinClient whose api_key is empty — simulates unconfigured state."""
    cfg = _make_mock_cfg(api_key="")
    mock_settings = MagicMock()
    mock_settings.jellyfin = cfg
    monkeypatch.setattr("src.services.jellyfin.settings", mock_settings)
    monkeypatch.setattr(
        "src.services.jellyfin.get_circuit_breaker",
        lambda *a, **kw: _make_noop_breaker(),
    )
    return JellyfinClient()


# ---------------------------------------------------------------------------
# test_connection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_test_connection_success(
    client: JellyfinClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """test_connection returns True when the server responds with HTTP 200."""
    mock_response = _make_response(200, {"ServerName": "Test", "Version": "10.9.0"})
    mock_transport = _MockTransport([mock_response])
    fake_client = httpx.AsyncClient(
        transport=mock_transport,
    )

    class _FakeAsyncClientCM:
        async def __aenter__(self) -> httpx.AsyncClient:
            return fake_client

        async def __aexit__(self, *args: object) -> bool:
            return False

    monkeypatch.setattr(
        "src.services.jellyfin.httpx.AsyncClient",
        lambda **kwargs: _FakeAsyncClientCM(),
    )

    result = await client.test_connection()

    assert result is True


@pytest.mark.asyncio
async def test_test_connection_auth_failure(
    client: JellyfinClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """test_connection returns False when the server responds with HTTP 401."""
    mock_response = _make_response(401)
    mock_transport = _MockTransport([mock_response])
    fake_client = httpx.AsyncClient(
        transport=mock_transport,
    )

    class _FakeAsyncClientCM:
        async def __aenter__(self) -> httpx.AsyncClient:
            return fake_client

        async def __aexit__(self, *args: object) -> bool:
            return False

    monkeypatch.setattr(
        "src.services.jellyfin.httpx.AsyncClient",
        lambda **kwargs: _FakeAsyncClientCM(),
    )

    result = await client.test_connection()

    assert result is False


@pytest.mark.asyncio
async def test_test_connection_unreachable(
    client: JellyfinClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """test_connection returns False when a ConnectError is raised."""

    class _ErrorTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("Connection refused")

    error_client = httpx.AsyncClient(transport=_ErrorTransport())

    class _FakeAsyncClientCM:
        async def __aenter__(self) -> httpx.AsyncClient:
            return error_client

        async def __aexit__(self, *args: object) -> bool:
            return False

    monkeypatch.setattr(
        "src.services.jellyfin.httpx.AsyncClient",
        lambda **kwargs: _FakeAsyncClientCM(),
    )

    result = await client.test_connection()

    assert result is False


# ---------------------------------------------------------------------------
# get_libraries
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_libraries_success_filters_types(
    client: JellyfinClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """get_libraries returns only movies and tvshows, filtering out music."""
    body = _make_virtual_folders_response()  # includes movies, tvshows, and music
    _patch_get_client(monkeypatch, [_make_response(200, body)])

    libraries = await client.get_libraries()

    # Only movies and tvshows should be returned (music filtered out)
    assert len(libraries) == 2
    assert all(isinstance(lib, JellyfinLibrary) for lib in libraries)

    movies = libraries[0]
    assert movies.library_id == "aa-111"
    assert movies.name == "Movies"
    assert movies.collection_type == "movies"

    tv = libraries[1]
    assert tv.library_id == "bb-222"
    assert tv.name == "TV Shows"
    assert tv.collection_type == "tvshows"


@pytest.mark.asyncio
async def test_get_libraries_empty_response(
    client: JellyfinClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """get_libraries returns [] when the server returns an empty list."""
    _patch_get_client(monkeypatch, [_make_response(200, [])])

    libraries = await client.get_libraries()

    assert libraries == []


@pytest.mark.asyncio
async def test_get_libraries_not_configured(
    no_key_client: JellyfinClient,
) -> None:
    """get_libraries returns [] immediately when api_key is empty."""
    # No HTTP call should be made — short-circuits on missing credentials.
    libraries = await no_key_client.get_libraries()

    assert libraries == []


# ---------------------------------------------------------------------------
# scan_library
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scan_library_success_204(
    client: JellyfinClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """scan_library returns True when the server responds with HTTP 204."""
    _patch_get_client(monkeypatch, [_make_response(204)])

    result = await client.scan_library("aa-111")

    assert result is True


@pytest.mark.asyncio
async def test_scan_library_success_200(
    client: JellyfinClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """scan_library returns True when the server responds with HTTP 200."""
    _patch_get_client(monkeypatch, [_make_response(200)])

    result = await client.scan_library("aa-111")

    assert result is True


@pytest.mark.asyncio
async def test_scan_library_failure_500(
    client: JellyfinClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """scan_library returns False when the server returns HTTP 500."""
    _patch_get_client(monkeypatch, [_make_response(500)])

    result = await client.scan_library("aa-111")

    assert result is False
