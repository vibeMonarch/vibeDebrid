"""Tests for src/services/plex.py.

All external HTTP calls are intercepted by a custom _MockTransport so no real
network traffic is generated. Each test exercises a single behaviour.

The mocking pattern mirrors test_tmdb.py exactly:
  - monkeypatch patches src.services.plex.settings so the mock config stays
    alive during await calls.
  - _MockTransport records every request in .requests_made for URL assertions.
  - _patch_client() replaces client._build_client (and optionally
    client._build_plex_tv_client) to inject the mock transport.
  - _make_response() builds fake httpx.Response objects.
  - _make_mock_cfg() creates a mock PlexConfig-like MagicMock.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest

from src.services.plex import PlexClient, PlexLibrarySection, PlexPinResponse


# ---------------------------------------------------------------------------
# Helpers — mock data builders
# ---------------------------------------------------------------------------


def _make_libraries_response(
    directories: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a Plex /library/sections JSON response."""
    if directories is None:
        directories = [
            {"key": "1", "title": "Movies", "type": "movie"},
            {"key": "2", "title": "TV Shows", "type": "show"},
        ]
    return {"MediaContainer": {"Directory": directories}}


def _make_pin_response(
    pin_id: int = 12345,
    code: str = "abcd1234",
) -> dict[str, Any]:
    """Build a Plex /api/v2/pins POST response."""
    return {"id": pin_id, "code": code}


def _make_check_pin_response(
    pin_id: int = 12345,
    auth_token: str = "",
) -> dict[str, Any]:
    """Build a Plex /api/v2/pins/{pin_id} GET response."""
    return {"id": pin_id, "authToken": auth_token}


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
        request=httpx.Request("GET", "http://plex.local:32400/"),
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
    url: str = "http://plex.local:32400",
    token: str = "test-plex-token",
) -> MagicMock:
    """Return a MagicMock that looks like a PlexConfig object."""
    cfg = MagicMock()
    cfg.url = url
    cfg.token = token
    return cfg


def _patch_client(
    client: PlexClient,
    responses: list[httpx.Response],
    *,
    plex_tv_responses: list[httpx.Response] | None = None,
) -> tuple[_MockTransport, _MockTransport | None]:
    """Monkey-patch *client._build_client* (and optionally *_build_plex_tv_client*)
    to inject a _MockTransport.

    Args:
        client:             The PlexClient instance under test.
        responses:          Ordered responses for the local Plex server client.
        plex_tv_responses:  Ordered responses for the plex.tv client.  When
                            None, _build_plex_tv_client is not patched.

    Returns:
        A tuple of (local_transport, plex_tv_transport).  plex_tv_transport is
        None when plex_tv_responses was not provided.
    """
    local_transport = _MockTransport(responses)

    def _fake_build_client(
        base_url: str | None = None, timeout: int = 10
    ) -> httpx.AsyncClient:
        resolved = (base_url or "http://plex.local:32400").rstrip("/")
        return httpx.AsyncClient(
            base_url=resolved,
            transport=local_transport,
        )

    client._build_client = _fake_build_client  # type: ignore[method-assign]

    plex_tv_transport: _MockTransport | None = None
    if plex_tv_responses is not None:
        plex_tv_transport = _MockTransport(plex_tv_responses)

        def _fake_build_plex_tv_client(timeout: int = 10) -> httpx.AsyncClient:
            return httpx.AsyncClient(
                base_url="https://plex.tv",
                transport=plex_tv_transport,
            )

        client._build_plex_tv_client = _fake_build_plex_tv_client  # type: ignore[method-assign]

    return local_transport, plex_tv_transport


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch) -> PlexClient:
    """A PlexClient with a patched settings singleton."""
    cfg = _make_mock_cfg()
    mock_settings = MagicMock()
    mock_settings.plex = cfg
    monkeypatch.setattr("src.services.plex.settings", mock_settings)
    return PlexClient()


@pytest.fixture()
def no_token_client(monkeypatch: pytest.MonkeyPatch) -> PlexClient:
    """A PlexClient whose token is empty — simulates unconfigured state."""
    cfg = _make_mock_cfg(token="")
    mock_settings = MagicMock()
    mock_settings.plex = cfg
    monkeypatch.setattr("src.services.plex.settings", mock_settings)
    return PlexClient()


# ---------------------------------------------------------------------------
# test_connection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_test_connection_success(client: PlexClient) -> None:
    """test_connection returns True when the server responds with HTTP 200."""
    _patch_client(client, [_make_response(200, {"MediaContainer": {}})])

    result = await client.test_connection()

    assert result is True


@pytest.mark.asyncio
async def test_test_connection_failure(client: PlexClient) -> None:
    """test_connection returns False when the server responds with HTTP 401."""
    _patch_client(client, [_make_response(401)])

    result = await client.test_connection()

    assert result is False


@pytest.mark.asyncio
async def test_test_connection_network_error(client: PlexClient) -> None:
    """test_connection returns False when a ConnectError is raised."""

    class _ErrorTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("Connection refused")

    def _fake_build_client(
        base_url: str | None = None, timeout: int = 10
    ) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url="http://plex.local:32400",
            transport=_ErrorTransport(),
        )

    client._build_client = _fake_build_client  # type: ignore[method-assign]

    result = await client.test_connection()

    assert result is False


# ---------------------------------------------------------------------------
# get_libraries
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_libraries_success(client: PlexClient) -> None:
    """get_libraries parses MediaContainer.Directory into a list of PlexLibrarySection."""
    body = _make_libraries_response()
    _patch_client(client, [_make_response(200, body)])

    sections = await client.get_libraries()

    assert len(sections) == 2
    assert all(isinstance(s, PlexLibrarySection) for s in sections)

    movies = sections[0]
    assert movies.section_id == 1
    assert movies.title == "Movies"
    assert movies.type == "movie"

    tv = sections[1]
    assert tv.section_id == 2
    assert tv.title == "TV Shows"
    assert tv.type == "show"


@pytest.mark.asyncio
async def test_get_libraries_empty(client: PlexClient) -> None:
    """get_libraries returns [] when the Directory array is empty."""
    body = _make_libraries_response(directories=[])
    _patch_client(client, [_make_response(200, body)])

    sections = await client.get_libraries()

    assert sections == []


@pytest.mark.asyncio
async def test_get_libraries_not_configured(no_token_client: PlexClient) -> None:
    """get_libraries returns [] when the token is empty (auth failure short-circuits)."""
    # The server rejects the request with 401 because the token is absent.
    _patch_client(no_token_client, [_make_response(401)])

    sections = await no_token_client.get_libraries()

    assert sections == []


# ---------------------------------------------------------------------------
# scan_section
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scan_section_success(client: PlexClient) -> None:
    """scan_section returns True when the server accepts the scan (HTTP 200)."""
    _patch_client(client, [_make_response(200)])

    result = await client.scan_section(section_id=1)

    assert result is True


@pytest.mark.asyncio
async def test_scan_section_failure(client: PlexClient) -> None:
    """scan_section returns False when the server returns HTTP 500."""
    _patch_client(client, [_make_response(500)])

    result = await client.scan_section(section_id=1)

    assert result is False


@pytest.mark.asyncio
async def test_scan_section_with_path(client: PlexClient) -> None:
    """scan_section passes the path as a query parameter when provided."""
    local_transport, _ = _patch_client(client, [_make_response(200)])

    await client.scan_section(section_id=3, path="/media/movies/Dune")

    assert len(local_transport.requests_made) == 1
    req = local_transport.requests_made[0]
    assert "path=" in str(req.url)
    assert "Dune" in str(req.url)
    assert "/library/sections/3/refresh" in str(req.url)


# ---------------------------------------------------------------------------
# create_pin
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_pin_success(client: PlexClient) -> None:
    """create_pin returns a PlexPinResponse with correct pin_id, code, and auth_url."""
    body = _make_pin_response(pin_id=12345, code="abcd1234")
    _, _ = _patch_client(client, [], plex_tv_responses=[_make_response(201, body)])

    result = await client.create_pin()

    assert result is not None
    assert isinstance(result, PlexPinResponse)
    assert result.pin_id == 12345
    assert result.code == "abcd1234"
    # auth_url must point to plex.tv/auth and embed both clientID and the code
    assert "app.plex.tv/auth" in result.auth_url
    assert "abcd1234" in result.auth_url
    assert "vibeDebrid" in result.auth_url


# ---------------------------------------------------------------------------
# check_pin
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_pin_pending(client: PlexClient) -> None:
    """check_pin returns None when authToken is an empty string (user not yet authed)."""
    body = _make_check_pin_response(pin_id=12345, auth_token="")
    _, _ = _patch_client(client, [], plex_tv_responses=[_make_response(200, body)])

    result = await client.check_pin(pin_id=12345)

    assert result is None


@pytest.mark.asyncio
async def test_check_pin_complete(client: PlexClient) -> None:
    """check_pin returns the token string when authToken is populated."""
    body = _make_check_pin_response(pin_id=12345, auth_token="my-plex-token-123")
    _, _ = _patch_client(client, [], plex_tv_responses=[_make_response(200, body)])

    result = await client.check_pin(pin_id=12345)

    assert result == "my-plex-token-123"
