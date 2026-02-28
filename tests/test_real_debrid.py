"""Tests for src/services/real_debrid.py.

All external HTTP calls are intercepted by httpx.MockTransport so no real
network traffic is generated. Each test exercises a single method and covers
both the happy path and the primary error paths.
"""

import json
from typing import Any
from unittest.mock import patch

import httpx
import pytest

from src.services.real_debrid import (
    RealDebridAuthError,
    RealDebridClient,
    RealDebridError,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_response(
    status_code: int,
    body: Any = None,
    *,
    content_type: str = "application/json",
) -> httpx.Response:
    """Build a fake httpx.Response for use with MockTransport."""
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
        request=httpx.Request("GET", "https://api.real-debrid.com/rest/1.0/"),
    )


class _MockTransport(httpx.AsyncBaseTransport):
    """Single-shot mock transport that returns a pre-built response."""

    def __init__(self, responses: list[httpx.Response]) -> None:
        self._responses = list(responses)
        self._index = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        resp = self._responses[self._index]
        self._index += 1
        # Attach the real request so raise_for_status works properly
        resp.request = request  # type: ignore[attr-defined]
        return resp


def _patch_client(client: RealDebridClient, responses: list[httpx.Response]):
    """Patch RealDebridClient._build_client to inject a mock transport."""
    original_build = client._build_client

    def _patched_build(timeout=15.0):
        real_client = original_build(timeout)
        real_client._transport = _MockTransport(responses)
        real_client._async_transport = _MockTransport(responses)
        return real_client

    return patch.object(client, "_build_client", side_effect=_patched_build)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def client() -> RealDebridClient:
    """A RealDebridClient with a dummy API key (no real HTTP calls)."""
    c = RealDebridClient()
    c._api_key = "test_api_key"
    return c


# ---------------------------------------------------------------------------
# get_user
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_user_success(client: RealDebridClient) -> None:
    """get_user returns the parsed dict on a successful 200 response."""
    payload = {
        "id": 12345,
        "username": "testuser",
        "email": "test@example.com",
        "points": 1000,
        "locale": "en",
        "avatar": "",
        "type": "premium",
        "premium": 365,
        "expiration": "2027-01-01T00:00:00.000Z",
    }
    responses = [_make_response(200, payload)]

    with _patch_client(client, responses):
        result = await client.get_user()

    assert result["username"] == "testuser"
    assert result["type"] == "premium"


@pytest.mark.asyncio
async def test_get_user_auth_error(client: RealDebridClient) -> None:
    """get_user raises RealDebridAuthError on HTTP 401."""
    responses = [_make_response(401, {"error": "Bad token", "error_code": 8})]

    with _patch_client(client, responses):
        with pytest.raises(RealDebridAuthError):
            await client.get_user()


# ---------------------------------------------------------------------------
# add_magnet
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_add_magnet_success(client: RealDebridClient) -> None:
    """add_magnet returns a dict with 'id' and 'uri' on success."""
    payload = {"id": "TORRENTID123", "uri": "https://api.real-debrid.com/rest/1.0/torrents/info/TORRENTID123"}
    responses = [_make_response(201, payload)]

    with _patch_client(client, responses):
        result = await client.add_magnet("magnet:?xt=urn:btih:abc123")

    assert result["id"] == "TORRENTID123"
    assert "uri" in result


@pytest.mark.asyncio
async def test_add_magnet_server_error(client: RealDebridClient) -> None:
    """add_magnet raises RealDebridError on a 5xx response."""
    responses = [_make_response(503, {"error": "Service Unavailable"})]

    with _patch_client(client, responses):
        with pytest.raises(RealDebridError) as exc_info:
            await client.add_magnet("magnet:?xt=urn:btih:abc123")

    assert exc_info.value.status_code == 503


# ---------------------------------------------------------------------------
# check_instant_availability (DEPRECATED — returns empty dict)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_instant_availability_deprecated_returns_empty(client: RealDebridClient) -> None:
    """check_instant_availability always returns an empty dict (endpoint deprecated)."""
    result = await client.check_instant_availability(["a" * 40, "b" * 40])
    assert result == {}


@pytest.mark.asyncio
async def test_check_instant_availability_empty_input(client: RealDebridClient) -> None:
    """check_instant_availability returns an empty dict for empty input."""
    result = await client.check_instant_availability([])
    assert result == {}


# ---------------------------------------------------------------------------
# list_torrents
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_torrents_success(client: RealDebridClient) -> None:
    """list_torrents returns a list of torrent dicts."""
    payload = [
        {"id": "T1", "filename": "Movie.A.mkv", "hash": "a" * 40, "status": "downloaded"},
        {"id": "T2", "filename": "Movie.B.mkv", "hash": "b" * 40, "status": "downloaded"},
    ]
    responses = [_make_response(200, payload)]

    with _patch_client(client, responses):
        result = await client.list_torrents()

    assert len(result) == 2
    assert result[0]["id"] == "T1"


@pytest.mark.asyncio
async def test_list_torrents_pagination_params(client: RealDebridClient) -> None:
    """list_torrents passes limit and offset to the API."""
    captured_requests: list[httpx.Request] = []

    class _CapturingTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            captured_requests.append(request)
            resp = _make_response(200, [])
            resp.request = request  # type: ignore[attr-defined]
            return resp

    original_build = client._build_client

    def _patched_build(timeout=15.0):
        c = original_build(timeout)
        c._transport = _CapturingTransport()
        c._async_transport = _CapturingTransport()
        return c

    with patch.object(client, "_build_client", side_effect=_patched_build):
        await client.list_torrents(limit=25, offset=50)

    assert len(captured_requests) == 1
    url = captured_requests[0].url
    assert url.params["limit"] == "25"
    assert url.params["offset"] == "50"


# ---------------------------------------------------------------------------
# get_torrent_info
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_torrent_info_success(client: RealDebridClient) -> None:
    """get_torrent_info returns the full info dict including files and links."""
    payload = {
        "id": "TORRENT1",
        "filename": "Great.Movie.2024.1080p.mkv",
        "original_filename": "Great.Movie.2024.1080p.mkv",
        "hash": "c" * 40,
        "bytes": 4_000_000_000,
        "progress": 100.0,
        "status": "downloaded",
        "files": [{"id": 1, "path": "/Great.Movie.2024.1080p.mkv", "bytes": 4_000_000_000, "selected": 1}],
        "links": ["https://real-debrid.com/d/LINK1"],
    }
    responses = [_make_response(200, payload)]

    with _patch_client(client, responses):
        result = await client.get_torrent_info("TORRENT1")

    assert result["id"] == "TORRENT1"
    assert result["status"] == "downloaded"
    assert len(result["files"]) == 1
    assert len(result["links"]) == 1


@pytest.mark.asyncio
async def test_get_torrent_info_not_found(client: RealDebridClient) -> None:
    """get_torrent_info raises RealDebridError on a 404 response."""
    responses = [_make_response(404, {"error": "Unknown ressource", "error_code": 6})]

    with _patch_client(client, responses):
        with pytest.raises(RealDebridError) as exc_info:
            await client.get_torrent_info("BADID")

    assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# select_files
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_select_files_all(client: RealDebridClient) -> None:
    """select_files with default 'all' completes without raising on 204."""
    responses = [_make_response(204, b"")]

    with _patch_client(client, responses):
        result = await client.select_files("TORRENT1")

    assert result is None


@pytest.mark.asyncio
async def test_select_files_specific_ids(client: RealDebridClient) -> None:
    """select_files passes comma-separated IDs in the form body."""
    captured_requests: list[httpx.Request] = []

    class _CapturingTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            captured_requests.append(request)
            resp = _make_response(204, b"")
            resp.request = request  # type: ignore[attr-defined]
            return resp

    original_build = client._build_client

    def _patched_build(timeout=15.0):
        c = original_build(timeout)
        c._transport = _CapturingTransport()
        c._async_transport = _CapturingTransport()
        return c

    with patch.object(client, "_build_client", side_effect=_patched_build):
        await client.select_files("TORRENT1", file_ids=[1, 3, 7])

    assert len(captured_requests) == 1
    body = captured_requests[0].content.decode()
    assert "files=1%2C3%2C7" in body or "files=1,3,7" in body


@pytest.mark.asyncio
async def test_select_files_error(client: RealDebridClient) -> None:
    """select_files raises RealDebridError on a non-success non-204 response."""
    responses = [_make_response(500, {"error": "Internal Server Error"})]

    with _patch_client(client, responses):
        with pytest.raises(RealDebridError):
            await client.select_files("TORRENT1")


# ---------------------------------------------------------------------------
# delete_torrent
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_torrent_success(client: RealDebridClient) -> None:
    """delete_torrent completes silently on a 204 response."""
    responses = [_make_response(204, b"")]

    with _patch_client(client, responses):
        result = await client.delete_torrent("TORRENT1")

    assert result is None


@pytest.mark.asyncio
async def test_delete_torrent_already_deleted(client: RealDebridClient) -> None:
    """delete_torrent does not raise on 404 — treats delete as idempotent."""
    responses = [_make_response(404, {"error": "Unknown ressource", "error_code": 6})]

    with _patch_client(client, responses):
        result = await client.delete_torrent("ALREADY_GONE")

    assert result is None


@pytest.mark.asyncio
async def test_delete_torrent_auth_error(client: RealDebridClient) -> None:
    """delete_torrent raises RealDebridAuthError on HTTP 401."""
    responses = [_make_response(401, {"error": "Bad token", "error_code": 8})]

    with _patch_client(client, responses):
        with pytest.raises(RealDebridAuthError):
            await client.delete_torrent("TORRENT1")


# ---------------------------------------------------------------------------
# Error response parsing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_error_code_preserved(client: RealDebridClient) -> None:
    """RealDebridError carries both status_code and error_code from the response."""
    responses = [_make_response(403, {"error": "Permission denied", "error_code": 9})]

    with _patch_client(client, responses):
        with pytest.raises(RealDebridError) as exc_info:
            await client.get_user()

    assert exc_info.value.status_code == 403
    assert exc_info.value.error_code == 9


@pytest.mark.asyncio
async def test_malformed_json_error_body(client: RealDebridClient) -> None:
    """A non-JSON error body still triggers RealDebridError without crashing."""
    responses = [_make_response(502, b"<html>Bad Gateway</html>", content_type="text/html")]

    with _patch_client(client, responses):
        with pytest.raises(RealDebridError) as exc_info:
            await client.get_user()

    assert exc_info.value.status_code == 502
