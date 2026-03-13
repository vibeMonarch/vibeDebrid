"""Tests for src/services/real_debrid.py.

All external HTTP calls are intercepted by httpx.MockTransport so no real
network traffic is generated. Each test exercises a single method and covers
both the happy path and the primary error paths.
"""

import json
from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from src.services.http_client import CircuitBreaker
from src.services.real_debrid import (
    CacheCheckResult,
    RealDebridAuthError,
    RealDebridClient,
    RealDebridError,
    RealDebridRateLimitError,
)


def _make_noop_breaker() -> CircuitBreaker:
    """Return a CircuitBreaker that is always CLOSED (never rejects requests)."""
    return CircuitBreaker("test", failure_threshold=999, recovery_timeout=0.0)


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
    """Patch RealDebridClient._get_client and the inline httpx.AsyncClient.

    Covers two cases:
    - Pooled methods (add_magnet, list_torrents, etc.): patch _get_client.
    - get_user which uses inline ``async with httpx.AsyncClient(...) as client``:
      patched via ``patch("src.services.real_debrid.httpx.AsyncClient")``.

    Both use the same _MockTransport so that the response queue is shared.
    Also patches the circuit breaker to a no-op so tests are not affected by
    circuit state.
    """
    transport = _MockTransport(responses)
    mock_http_client = httpx.AsyncClient(
        base_url="https://api.real-debrid.com/rest/1.0",
        transport=transport,
    )

    async def _fake_get_client(timeout=15.0):
        return mock_http_client

    # Build a mock class for httpx.AsyncClient that behaves as an async context
    # manager and delegates requests to our mock transport.
    import contextlib

    class _FakeAsyncClientCM:
        """Async context manager that wraps the mock transport client."""

        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return mock_http_client

        async def __aexit__(self, *args):
            pass

    class _MultiPatch:
        """Context manager that stacks all required patches."""

        def __enter__(self):
            self._p1 = patch.object(client, "_get_client", side_effect=_fake_get_client)
            self._p2 = patch("src.services.real_debrid.httpx.AsyncClient", new=_FakeAsyncClientCM)
            self._p3 = patch(
                "src.services.real_debrid.get_circuit_breaker",
                side_effect=lambda *a, **k: _make_noop_breaker(),
            )
            self._p1.__enter__()
            self._p2.__enter__()
            self._p3.__enter__()
            return self

        def __exit__(self, *args):
            self._p3.__exit__(*args)
            self._p2.__exit__(*args)
            self._p1.__exit__(*args)

    return _MultiPatch()


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
    """list_torrents passes limit and page to the API."""
    captured_requests: list[httpx.Request] = []

    class _CapturingTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            captured_requests.append(request)
            resp = _make_response(200, [])
            resp.request = request  # type: ignore[attr-defined]
            return resp

    capturing_client = httpx.AsyncClient(
        base_url="https://api.real-debrid.com/rest/1.0",
        transport=_CapturingTransport(),
    )

    async def _fake_get_client(timeout: float = 15.0) -> httpx.AsyncClient:
        return capturing_client

    with (
        patch.object(client, "_get_client", side_effect=_fake_get_client),
        patch("src.services.real_debrid.get_circuit_breaker", side_effect=lambda *a, **k: _make_noop_breaker()),
    ):
        await client.list_torrents(limit=25, page=3)

    assert len(captured_requests) == 1
    url = captured_requests[0].url
    assert url.params["limit"] == "25"
    assert url.params["page"] == "3"


@pytest.mark.asyncio
async def test_list_torrents_204_returns_empty(client: RealDebridClient) -> None:
    """list_torrents returns empty list on HTTP 204 No Content."""
    responses = [_make_response(204, None)]

    with _patch_client(client, responses):
        result = await client.list_torrents()

    assert result == []


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

    capturing_client = httpx.AsyncClient(
        base_url="https://api.real-debrid.com/rest/1.0",
        transport=_CapturingTransport(),
    )

    async def _fake_get_client(timeout: float = 15.0) -> httpx.AsyncClient:
        return capturing_client

    with (
        patch.object(client, "_get_client", side_effect=_fake_get_client),
        patch("src.services.real_debrid.get_circuit_breaker", side_effect=lambda *a, **k: _make_noop_breaker()),
    ):
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


# ---------------------------------------------------------------------------
# Shared stateful mock transport for multi-call methods
# ---------------------------------------------------------------------------


class _SharedTransport(httpx.AsyncBaseTransport):
    """Transport that returns pre-built responses in order across all clients."""

    def __init__(self, responses: list[httpx.Response]) -> None:
        self._responses = list(responses)
        self._index = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        resp = self._responses[self._index]
        self._index += 1
        resp.request = request
        return resp


def _patch_client_shared(client: RealDebridClient, transport: _SharedTransport):
    """Patch so every _get_client call shares the same transport.

    Returns a context manager that applies two patches:
    - client._get_client → async function returning a shared-transport client
    - src.services.real_debrid.get_circuit_breaker → noop breaker factory
    """
    shared_http_client = httpx.AsyncClient(
        base_url="https://api.real-debrid.com/rest/1.0",
        transport=transport,
    )

    async def _fake_get_client(timeout: float = 15.0) -> httpx.AsyncClient:
        return shared_http_client

    class _MultiPatch:
        def __enter__(self):
            self._p1 = patch.object(client, "_get_client", side_effect=_fake_get_client)
            self._p2 = patch(
                "src.services.real_debrid.get_circuit_breaker",
                side_effect=lambda *a, **k: _make_noop_breaker(),
            )
            self._p1.__enter__()
            self._p2.__enter__()
            return self

        def __exit__(self, *args):
            self._p2.__exit__(*args)
            self._p1.__exit__(*args)

    return _MultiPatch()


# ---------------------------------------------------------------------------
# check_cached
# ---------------------------------------------------------------------------

_SAMPLE_HASH = "a" * 40


@pytest.mark.asyncio
async def test_check_cached_downloaded(client: RealDebridClient) -> None:
    """check_cached returns cached=True and rd_id=None when torrent status is 'downloaded'.

    With default keep_if_cached=False the torrent is deleted after probing, so
    rd_id is None in the result.
    """
    transport = _SharedTransport([
        _make_response(201, {"id": "RD1", "uri": "https://rd.example/RD1"}),
        _make_response(200, {"id": "RD1", "status": "downloaded", "hash": _SAMPLE_HASH}),
        _make_response(204, b""),  # delete
    ])
    with _patch_client_shared(client, transport):
        result = await client.check_cached(_SAMPLE_HASH)
    assert result.cached is True
    assert result.rd_id is None


@pytest.mark.asyncio
async def test_check_cached_waiting_files_selection(client: RealDebridClient) -> None:
    """check_cached returns cached=True and rd_id=None for 'waiting_files_selection'.

    With default keep_if_cached=False the torrent is deleted after probing.
    """
    transport = _SharedTransport([
        _make_response(201, {"id": "RD2", "uri": "https://rd.example/RD2"}),
        _make_response(200, {"id": "RD2", "status": "waiting_files_selection", "hash": _SAMPLE_HASH}),
        _make_response(204, b""),
    ])
    with _patch_client_shared(client, transport):
        result = await client.check_cached(_SAMPLE_HASH)
    assert result.cached is True
    assert result.rd_id is None


@pytest.mark.asyncio
async def test_check_cached_not_cached(client: RealDebridClient) -> None:
    """check_cached returns cached=False when torrent status is 'magnet_conversion'."""
    transport = _SharedTransport([
        _make_response(201, {"id": "RD3", "uri": "https://rd.example/RD3"}),
        _make_response(200, {"id": "RD3", "status": "magnet_conversion", "hash": _SAMPLE_HASH}),
        _make_response(204, b""),
    ])
    with _patch_client_shared(client, transport):
        result = await client.check_cached(_SAMPLE_HASH)
    assert result.cached is False


@pytest.mark.asyncio
async def test_check_cached_add_magnet_fails(client: RealDebridClient) -> None:
    """check_cached returns cached=None when add_magnet raises an error."""
    transport = _SharedTransport([
        _make_response(503, {"error": "Service Unavailable"}),
    ])
    with _patch_client_shared(client, transport):
        result = await client.check_cached(_SAMPLE_HASH)
    assert result.cached is None


@pytest.mark.asyncio
async def test_check_cached_get_info_fails(client: RealDebridClient) -> None:
    """check_cached returns cached=None when get_torrent_info fails, still cleans up."""
    transport = _SharedTransport([
        _make_response(201, {"id": "RD4", "uri": "https://rd.example/RD4"}),
        _make_response(500, {"error": "Internal error"}),
        _make_response(204, b""),  # delete should still be called
    ])
    with _patch_client_shared(client, transport):
        result = await client.check_cached(_SAMPLE_HASH)
    assert result.cached is None


@pytest.mark.asyncio
async def test_check_cached_empty_id(client: RealDebridClient) -> None:
    """check_cached returns cached=None when add_magnet returns an empty id."""
    transport = _SharedTransport([
        _make_response(201, {"id": "", "uri": ""}),
    ])
    with _patch_client_shared(client, transport):
        result = await client.check_cached(_SAMPLE_HASH)
    assert result.cached is None


# ---------------------------------------------------------------------------
# check_cached_batch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_cached_batch_returns_cached(client: RealDebridClient) -> None:
    """check_cached_batch returns a dict mapping hashes to CacheCheckResult."""
    hash_a = "a" * 40
    hash_b = "b" * 40
    hash_c = "c" * 40

    transport = _SharedTransport([
        # hash_a: cached
        _make_response(201, {"id": "RD_A", "uri": ""}),
        _make_response(200, {"id": "RD_A", "status": "downloaded"}),
        _make_response(204, b""),
        # hash_b: not cached
        _make_response(201, {"id": "RD_B", "uri": ""}),
        _make_response(200, {"id": "RD_B", "status": "magnet_conversion"}),
        _make_response(204, b""),
        # hash_c: cached
        _make_response(201, {"id": "RD_C", "uri": ""}),
        _make_response(200, {"id": "RD_C", "status": "waiting_files_selection"}),
        _make_response(204, b""),
    ])
    with _patch_client_shared(client, transport):
        result = await client.check_cached_batch([hash_a, hash_b, hash_c])
    assert result[hash_a].cached is True
    assert result[hash_c].cached is True
    assert result[hash_b].cached is False


@pytest.mark.asyncio
async def test_check_cached_batch_max_checks(client: RealDebridClient) -> None:
    """check_cached_batch respects max_checks limit."""
    hashes = [f"{chr(ord('a') + i)}" * 40 for i in range(5)]

    transport = _SharedTransport([
        # Only 2 checks — the rest should not be attempted
        _make_response(201, {"id": "RD1", "uri": ""}),
        _make_response(200, {"id": "RD1", "status": "downloaded"}),
        _make_response(204, b""),
        _make_response(201, {"id": "RD2", "uri": ""}),
        _make_response(200, {"id": "RD2", "status": "downloaded"}),
        _make_response(204, b""),
    ])
    with _patch_client_shared(client, transport):
        result = await client.check_cached_batch(hashes, max_checks=2)
    assert len(result) == 2


@pytest.mark.asyncio
async def test_check_cached_batch_empty_input(client: RealDebridClient) -> None:
    """check_cached_batch returns empty dict for empty input."""
    result = await client.check_cached_batch([])
    assert result == {}


@pytest.mark.asyncio
async def test_check_cached_batch_rate_limit_stops_early(client: RealDebridClient) -> None:
    """check_cached_batch stops early when check_cached raises RealDebridRateLimitError.

    Simulates the case where the inner check_cached call propagates a rate-limit
    error to the batch loop (e.g. when the caller has disabled internal retries).
    The batch catches the exception and stops without recording hash_b.
    """
    hash_a = "a" * 40
    hash_b = "b" * 40

    cached_result = CacheCheckResult(info_hash=hash_a, cached=True)

    call_count = 0

    async def _side_effect(h: str, *, keep_if_cached: bool = False) -> CacheCheckResult:
        nonlocal call_count
        call_count += 1
        if h == hash_a:
            return cached_result
        raise RealDebridRateLimitError("Too many requests", status_code=429)

    with patch.object(client, "check_cached", side_effect=_side_effect):
        result = await client.check_cached_batch([hash_a, hash_b])

    assert hash_a in result
    assert result[hash_a].cached is True
    assert hash_b not in result


# ---------------------------------------------------------------------------
# check_cached — keep_if_cached and error/video checks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_cached_keep_if_cached_true_keeps_torrent(client: RealDebridClient) -> None:
    """check_cached with keep_if_cached=True does not delete a cached torrent.

    Only 2 HTTP calls are made (add + get_info). No delete response is provided,
    confirming the torrent is left in the RD account.
    """
    transport = _SharedTransport([
        _make_response(201, {"id": "RD_KEEP", "uri": "https://rd.example/RD_KEEP"}),
        _make_response(200, {
            "id": "RD_KEEP",
            "status": "downloaded",
            "hash": _SAMPLE_HASH,
            "files": [{"id": 1, "path": "/Movie.mkv", "bytes": 4_000_000_000, "selected": 1}],
        }),
        # No delete response — keeping the torrent means no DELETE call is issued.
    ])
    with _patch_client_shared(client, transport):
        result = await client.check_cached(_SAMPLE_HASH, keep_if_cached=True)
    assert result.cached is True
    assert result.rd_id == "RD_KEEP"
    assert result.has_video_files is True


@pytest.mark.asyncio
async def test_check_cached_keep_if_cached_true_deletes_uncached(client: RealDebridClient) -> None:
    """check_cached with keep_if_cached=True still deletes an uncached torrent."""
    transport = _SharedTransport([
        _make_response(201, {"id": "RD_UC", "uri": "https://rd.example/RD_UC"}),
        _make_response(200, {"id": "RD_UC", "status": "magnet_conversion", "hash": _SAMPLE_HASH}),
        _make_response(204, b""),  # delete
    ])
    with _patch_client_shared(client, transport):
        result = await client.check_cached(_SAMPLE_HASH, keep_if_cached=True)
    assert result.cached is False
    assert result.rd_id is None


@pytest.mark.asyncio
async def test_check_cached_error_status_magnet_error(client: RealDebridClient) -> None:
    """check_cached returns cached=None for the 'magnet_error' error status."""
    transport = _SharedTransport([
        _make_response(201, {"id": "RD_ERR", "uri": "https://rd.example/RD_ERR"}),
        _make_response(200, {
            "id": "RD_ERR",
            "status": "magnet_error",
            "hash": _SAMPLE_HASH,
        }),
        _make_response(204, b""),  # delete
    ])
    with _patch_client_shared(client, transport):
        result = await client.check_cached(_SAMPLE_HASH)
    assert result.cached is None
    assert result.has_video_files is False


@pytest.mark.asyncio
async def test_check_cached_error_status_virus(client: RealDebridClient) -> None:
    """check_cached returns cached=None for the 'virus' error status."""
    transport = _SharedTransport([
        _make_response(201, {"id": "RD_VIR", "uri": "https://rd.example/RD_VIR"}),
        _make_response(200, {
            "id": "RD_VIR",
            "status": "virus",
            "hash": _SAMPLE_HASH,
        }),
        _make_response(204, b""),  # delete
    ])
    with _patch_client_shared(client, transport):
        result = await client.check_cached(_SAMPLE_HASH)
    assert result.cached is None
    assert result.has_video_files is False


@pytest.mark.asyncio
async def test_check_cached_no_video_files(client: RealDebridClient) -> None:
    """check_cached returns cached=None when the torrent has no video files."""
    transport = _SharedTransport([
        _make_response(201, {"id": "RD_NV", "uri": "https://rd.example/RD_NV"}),
        _make_response(200, {
            "id": "RD_NV",
            "status": "downloaded",
            "hash": _SAMPLE_HASH,
            "files": [{"id": 1, "path": "/readme.txt", "bytes": 1_000, "selected": 1}],
        }),
        _make_response(204, b""),  # delete
    ])
    with _patch_client_shared(client, transport):
        result = await client.check_cached(_SAMPLE_HASH)
    assert result.cached is None
    assert result.has_video_files is False


@pytest.mark.asyncio
async def test_check_cached_with_video_files(client: RealDebridClient) -> None:
    """check_cached returns cached=True when the torrent contains a video file."""
    transport = _SharedTransport([
        _make_response(201, {"id": "RD_VF", "uri": "https://rd.example/RD_VF"}),
        _make_response(200, {
            "id": "RD_VF",
            "status": "downloaded",
            "hash": _SAMPLE_HASH,
            "files": [{"id": 1, "path": "/Movie.2024.1080p.mkv", "bytes": 4_000_000_000, "selected": 1}],
        }),
        _make_response(204, b""),  # delete (keep_if_cached=False)
    ])
    with _patch_client_shared(client, transport):
        result = await client.check_cached(_SAMPLE_HASH)
    assert result.cached is True
    assert result.has_video_files is True


@pytest.mark.asyncio
async def test_check_cached_retry_on_rate_limit(client: RealDebridClient) -> None:
    """check_cached retries add_magnet on a 429 and succeeds on the next attempt."""
    transport = _SharedTransport([
        _make_response(429, {"error": "Too many requests"}),  # first attempt → rate limited
        _make_response(201, {"id": "RD_RT", "uri": "https://rd.example/RD_RT"}),  # retry succeeds
        _make_response(200, {
            "id": "RD_RT",
            "status": "downloaded",
            "hash": _SAMPLE_HASH,
        }),
        _make_response(204, b""),  # delete
    ])
    with patch("src.services.real_debrid.asyncio.sleep", new_callable=AsyncMock):
        with _patch_client_shared(client, transport):
            result = await client.check_cached(_SAMPLE_HASH)
    assert result.cached is True


@pytest.mark.asyncio
async def test_check_cached_batch_keep_if_cached_true(client: RealDebridClient) -> None:
    """check_cached_batch passes keep_if_cached=True through to each check_cached call.

    hash_a is cached and kept (no delete, rd_id returned).
    hash_b is not cached and deleted (rd_id is None).
    """
    hash_a = "a" * 40
    hash_b = "b" * 40

    transport = _SharedTransport([
        # hash_a: cached and kept — no delete response provided
        _make_response(201, {"id": "RD_A", "uri": ""}),
        _make_response(200, {
            "id": "RD_A",
            "status": "downloaded",
            "hash": hash_a,
        }),
        # hash_b: not cached — deleted
        _make_response(201, {"id": "RD_B", "uri": ""}),
        _make_response(200, {"id": "RD_B", "status": "magnet_conversion"}),
        _make_response(204, b""),  # delete
    ])
    with _patch_client_shared(client, transport):
        result = await client.check_cached_batch([hash_a, hash_b], keep_if_cached=True)
    assert result[hash_a].cached is True
    assert result[hash_a].rd_id == "RD_A"
    assert result[hash_b].cached is False
    assert result[hash_b].rd_id is None
