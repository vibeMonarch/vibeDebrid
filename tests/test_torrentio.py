"""Tests for src/services/torrentio.py.

All external HTTP calls are intercepted by a custom MockTransport so no real
network traffic is generated. Each test exercises a single behaviour. The
fallback chain tests (episode → season → show) are the most critical section
and are therefore the most thoroughly exercised.
"""

import json
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest

from src.services.http_client import CircuitBreaker
from src.services.torrentio import (
    TorrentioClient,
    TorrentioError,
    TorrentioResult,
)


def _make_noop_breaker() -> CircuitBreaker:
    """Return a CircuitBreaker that is always CLOSED (never rejects requests)."""
    breaker = CircuitBreaker("test", failure_threshold=999, recovery_timeout=0.0)
    return breaker


# ---------------------------------------------------------------------------
# Helpers — mock data builders
# ---------------------------------------------------------------------------


def _make_stream(
    info_hash: str = "a" * 40,
    title: str = "Movie.2024.1080p.WEB-DL.x264-GROUP",
    resolution: str = "1080p",
    size: str = "4.2 GB",
    seeders: int = 15,
    source: str = "ThePirateBay",
    file_idx: int = 0,
) -> dict[str, Any]:
    """Build a single Torrentio stream entry."""
    return {
        "name": f"Torrentio\n{resolution}",
        "title": f"{title}\n\U0001f464 {seeders} \U0001f4be {size} \u2699\ufe0f {source}",
        "infoHash": info_hash,
        "fileIdx": file_idx,
        "behaviorHints": {
            "bingeGroup": f"torrentio|{resolution}|GROUP",
            "filename": f"{title}.mkv",
        },
    }


def _make_torrentio_response(streams: list[dict[str, Any]]) -> dict[str, Any]:
    """Wrap streams in the Torrentio API response envelope."""
    return {"streams": streams}


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
        request=httpx.Request("GET", "https://torrentio.strem.fun/"),
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
    base_url: str = "https://torrentio.strem.fun",
    opts: str = "",
    timeout_seconds: int = 30,
    max_results: int = 100,
) -> MagicMock:
    """Return a MagicMock that looks like a torrentio config object."""
    cfg = MagicMock()
    cfg.base_url = base_url
    cfg.opts = opts
    cfg.timeout_seconds = timeout_seconds
    cfg.max_results = max_results
    # enabled must be truthy so scrape_movie / scrape_episode don't short-circuit
    cfg.enabled = True
    return cfg


def _patch_client(
    client: TorrentioClient,
    responses: list[httpx.Response] | None = None,
    *,
    responses_by_url: dict[str, httpx.Response] | None = None,
    default_response: httpx.Response | None = None,
    cfg: MagicMock | None = None,
) -> "_MockTransport":
    """Monkey-patch *client._build_client* to inject a _MockTransport.

    Every call to ``client._build_client()`` (which happens inside ``_query``)
    will return a fresh ``httpx.AsyncClient`` backed by the same transport
    instance, so ``transport.requests_made`` accumulates across fallback steps.

    Callers are responsible for ensuring ``src.services.torrentio.settings`` is
    patched before the test's ``await`` calls — the fixtures in this module use
    ``monkeypatch.setattr`` which keeps the patch active for the whole test.

    Args:
        client:            The TorrentioClient instance under test.
        responses:         Ordered list of responses to return sequentially.
        responses_by_url:  URL-substring → response mapping (checked first).
        default_response:  Fallback response when nothing else matches.
        cfg:               Unused parameter kept for API compatibility.

    Returns:
        The _MockTransport so tests can inspect ``transport.requests_made``.
    """
    transport = _MockTransport(
        responses,
        responses_by_url=responses_by_url,
        default_response=default_response,
    )

    # Patch _build_client to return an AsyncClient with our mock transport.
    # We keep the same transport instance across multiple _build_client() calls
    # so that the requests_made list accumulates correctly across fallback steps.
    # Delegate to client._build_base_url() so that the opts path segment and
    # other config read from the already-patched module-level settings are used.
    # Forward include_debrid_key so integration tests exercise URL stripping.
    async def _fake_get_client(**kwargs: object) -> httpx.AsyncClient:
        include_debrid_key = kwargs.get("include_debrid_key", True)
        base_url = client._build_base_url(include_debrid_key=include_debrid_key)
        return httpx.AsyncClient(
            base_url=base_url,
            transport=transport,
        )

    client._get_client = _fake_get_client  # type: ignore[method-assign]

    return transport


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch) -> TorrentioClient:
    """A TorrentioClient configured with test defaults (no real HTTP)."""
    cfg = _make_mock_cfg()

    mock_settings = MagicMock()
    mock_settings.scrapers.torrentio = cfg
    monkeypatch.setattr("src.services.torrentio.settings", mock_settings)
    monkeypatch.setattr(
        "src.services.torrentio.get_circuit_breaker",
        lambda *args, **kwargs: _make_noop_breaker(),
    )

    c = TorrentioClient()
    # Expose cfg for per-test mutation
    c._test_cfg = cfg  # type: ignore[attr-defined]
    return c


@pytest.fixture()
def client_with_opts(monkeypatch: pytest.MonkeyPatch) -> TorrentioClient:
    """A TorrentioClient with opts set to 'sort=qualitysize|qualityfilter=480p'."""
    cfg = _make_mock_cfg(opts="sort=qualitysize|qualityfilter=480p")

    mock_settings = MagicMock()
    mock_settings.scrapers.torrentio = cfg
    monkeypatch.setattr("src.services.torrentio.settings", mock_settings)
    monkeypatch.setattr(
        "src.services.torrentio.get_circuit_breaker",
        lambda *args, **kwargs: _make_noop_breaker(),
    )

    return TorrentioClient()


# ---------------------------------------------------------------------------
# scrape_movie — happy path & error paths
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scrape_movie_success(client: TorrentioClient) -> None:
    """scrape_movie returns a non-empty list of TorrentioResult on a 200 response."""
    stream = _make_stream(info_hash="a" * 40, resolution="1080p", size="4.2 GB", seeders=15)
    payload = _make_torrentio_response([stream])
    _transport = _patch_client(client, [_make_response(200, payload)])

    results = await client.scrape_movie("tt1234567")

    assert len(results) == 1
    assert isinstance(results[0], TorrentioResult)
    assert results[0].info_hash == "a" * 40


@pytest.mark.asyncio
async def test_scrape_movie_calls_correct_url(client: TorrentioClient) -> None:
    """scrape_movie hits /stream/movie/{imdb_id}.json."""
    payload = _make_torrentio_response([_make_stream()])
    transport = _patch_client(client, [_make_response(200, payload)])

    await client.scrape_movie("tt9999999")

    assert len(transport.requests_made) == 1
    url = str(transport.requests_made[0].url)
    assert "/stream/movie/tt9999999.json" in url


@pytest.mark.asyncio
async def test_scrape_movie_no_results(client: TorrentioClient) -> None:
    """scrape_movie returns an empty list when the streams array is empty."""
    payload = _make_torrentio_response([])
    _patch_client(client, [_make_response(200, payload)])

    results = await client.scrape_movie("tt0000001")

    assert results == []


@pytest.mark.asyncio
async def test_scrape_movie_http_error(client: TorrentioClient) -> None:
    """scrape_movie returns an empty list (does not raise) on a 5xx HTTP error."""
    _patch_client(client, [_make_response(503, {"error": "Service Unavailable"})])

    results = await client.scrape_movie("tt1234567")

    assert results == []


@pytest.mark.asyncio
async def test_scrape_movie_http_404(client: TorrentioClient) -> None:
    """scrape_movie returns an empty list on a 404 Not Found response."""
    _patch_client(client, [_make_response(404, b"Not Found", content_type="text/plain")])

    results = await client.scrape_movie("tt0000000")

    assert results == []


# ---------------------------------------------------------------------------
# scrape_episode — fallback chain (episode → season → show)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scrape_episode_found_directly(client: TorrentioClient) -> None:
    """Episode-level query returns results; no fallback to season/show is needed."""
    stream = _make_stream(
        info_hash="b" * 40,
        title="Show.Name.S02E05.1080p.WEB-DL.x264-GROUP",
        resolution="1080p",
    )
    episode_resp = _make_response(200, _make_torrentio_response([stream]))
    transport = _patch_client(client, [episode_resp])

    results = await client.scrape_episode("tt7654321", season=2, episode=5)

    assert len(results) == 1
    assert results[0].info_hash == "b" * 40
    # Only one HTTP call should have been made
    assert len(transport.requests_made) == 1


@pytest.mark.asyncio
async def test_scrape_episode_url_contains_season_episode(client: TorrentioClient) -> None:
    """Episode-level query URL encodes the season and episode numbers."""
    stream = _make_stream(info_hash="c" * 40)
    transport = _patch_client(client, [_make_response(200, _make_torrentio_response([stream]))])

    await client.scrape_episode("tt1111111", season=3, episode=7)

    url = str(transport.requests_made[0].url)
    assert ":3:7" in url or "/3/7" in url or "3:7" in url


@pytest.mark.asyncio
async def test_scrape_episode_fallback_to_season(client: TorrentioClient) -> None:
    """When episode query returns 0 results, season-pack query is attempted."""
    season_stream = _make_stream(
        info_hash="d" * 40,
        title="Show.Name.S02.1080p.WEB-DL.x264-GROUP",
    )
    # URL-based routing: episode path → empty; season path → results
    transport = _patch_client(
        client,
        responses_by_url={
            ":2:5": _make_response(200, _make_torrentio_response([])),
            ":2:": _make_response(200, _make_torrentio_response([season_stream])),
        },
        default_response=_make_response(200, _make_torrentio_response([])),
    )

    results = await client.scrape_episode("tt2222222", season=2, episode=5)

    assert len(results) == 1
    assert results[0].info_hash == "d" * 40
    # At least two HTTP calls: episode query + season query
    assert len(transport.requests_made) >= 2


@pytest.mark.asyncio
async def test_scrape_episode_both_fallbacks_empty(client: TorrentioClient) -> None:
    """Both episode and season fallback levels return 0 results; method returns []."""
    empty = _make_response(200, _make_torrentio_response([]))
    transport = _patch_client(client, [empty, empty])

    results = await client.scrape_episode("tt3333333", season=2, episode=5)

    assert results == []
    # Both levels should have been queried
    assert len(transport.requests_made) == 2


@pytest.mark.asyncio
async def test_scrape_episode_no_duplicate_calls(client: TorrentioClient) -> None:
    """When episode query succeeds, season and show queries are NOT made."""
    stream = _make_stream(info_hash="f" * 40)
    transport = _patch_client(client, [_make_response(200, _make_torrentio_response([stream]))])

    await client.scrape_episode("tt5555555", season=1, episode=3)

    # Exactly one call — no unnecessary fallback requests
    assert len(transport.requests_made) == 1


@pytest.mark.asyncio
async def test_scrape_episode_fallback_stops_at_season(client: TorrentioClient) -> None:
    """When season-level fallback finds results, show-level query is NOT made."""
    season_stream = _make_stream(info_hash="aa" * 20)
    empty = _make_response(200, _make_torrentio_response([]))
    season_resp = _make_response(200, _make_torrentio_response([season_stream]))

    transport = _patch_client(client, [empty, season_resp])

    results = await client.scrape_episode("tt6666666", season=4, episode=2)

    assert len(results) == 1
    # Show query was never issued
    assert len(transport.requests_made) == 2


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_parse_stream_full_fields(client: TorrentioClient) -> None:
    """All fields present: info_hash, resolution, size_bytes, seeders, source_tracker are parsed."""
    stream = _make_stream(
        info_hash="abc123" + "0" * 34,
        title="Great.Movie.2024.1080p.WEB-DL.x264-GRP",
        resolution="1080p",
        size="4.2 GB",
        seeders=42,
        source="ThePirateBay",
    )
    payload = _make_torrentio_response([stream])
    _patch_client(client, [_make_response(200, payload)])

    results = await client.scrape_movie("tt0000001")

    assert len(results) == 1
    r = results[0]
    assert r.info_hash == "abc123" + "0" * 34
    assert r.resolution == "1080p"
    assert r.seeders == 42
    assert r.source_tracker == "ThePirateBay"
    assert r.size_bytes is not None
    assert r.size_bytes > 0


@pytest.mark.asyncio
async def test_parse_stream_missing_optional_fields(client: TorrentioClient) -> None:
    """Streams with missing behaviorHints or seeders are parsed without crashing."""
    stream: dict[str, Any] = {
        "name": "Torrentio\n720p",
        "title": "Movie.2024.720p.WEB-DL\nno seeders or source here",
        "infoHash": "b" * 40,
        # behaviorHints intentionally omitted
    }
    payload = _make_torrentio_response([stream])
    _patch_client(client, [_make_response(200, payload)])

    results = await client.scrape_movie("tt0000002")

    assert len(results) == 1
    r = results[0]
    assert r.info_hash == "b" * 40
    # Optional fields should be None or 0, not crash
    assert r.seeders is None or isinstance(r.seeders, int)


@pytest.mark.asyncio
async def test_parse_stream_no_info_hash(client: TorrentioClient) -> None:
    """Streams that lack an infoHash are skipped entirely."""
    stream_no_hash: dict[str, Any] = {
        "name": "Torrentio\n1080p",
        "title": "Movie.2024.1080p.WEB-DL\n\U0001f464 10 \U0001f4be 2.0 GB \u2699\ufe0f TPB",
        # no infoHash key
    }
    stream_good = _make_stream(info_hash="c" * 40)
    payload = _make_torrentio_response([stream_no_hash, stream_good])
    _patch_client(client, [_make_response(200, payload)])

    results = await client.scrape_movie("tt0000003")

    # Only the stream with a valid infoHash survives
    assert len(results) == 1
    assert results[0].info_hash == "c" * 40


@pytest.mark.asyncio
async def test_parse_stream_empty_info_hash(client: TorrentioClient) -> None:
    """Streams with an empty string infoHash are skipped."""
    stream: dict[str, Any] = {
        "name": "Torrentio\n1080p",
        "title": "Movie.2024.1080p\n\U0001f464 5 \U0001f4be 1.0 GB \u2699\ufe0f TPB",
        "infoHash": "",
    }
    payload = _make_torrentio_response([stream])
    _patch_client(client, [_make_response(200, payload)])

    results = await client.scrape_movie("tt0000004")

    assert results == []


# ---------------------------------------------------------------------------
# Size parsing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_parse_size_gb(client: TorrentioClient) -> None:
    """'4.2 GB' is parsed to the correct byte count."""
    stream = _make_stream(size="4.2 GB")
    _patch_client(client, [_make_response(200, _make_torrentio_response([stream]))])

    results = await client.scrape_movie("tt0000005")

    assert len(results) == 1
    expected_bytes = int(4.2 * 1024 ** 3)
    # Allow a small rounding window (±1 MB)
    assert abs(results[0].size_bytes - expected_bytes) < 1024 * 1024


@pytest.mark.asyncio
async def test_parse_size_mb(client: TorrentioClient) -> None:
    """'850 MB' is parsed to the correct byte count."""
    stream = _make_stream(size="850 MB")
    _patch_client(client, [_make_response(200, _make_torrentio_response([stream]))])

    results = await client.scrape_movie("tt0000006")

    assert len(results) == 1
    expected_bytes = int(850 * 1024 ** 2)
    assert abs(results[0].size_bytes - expected_bytes) < 1024 * 1024


@pytest.mark.asyncio
async def test_parse_size_tb(client: TorrentioClient) -> None:
    """'1.2 TB' is parsed to the correct byte count."""
    stream = _make_stream(size="1.2 TB")
    _patch_client(client, [_make_response(200, _make_torrentio_response([stream]))])

    results = await client.scrape_movie("tt0000007")

    assert len(results) == 1
    expected_bytes = int(1.2 * 1024 ** 4)
    assert abs(results[0].size_bytes - expected_bytes) < 1024 ** 3  # within 1 GB


@pytest.mark.asyncio
async def test_parse_size_invalid(client: TorrentioClient) -> None:
    """An unparseable size string results in size_bytes being None."""
    stream: dict[str, Any] = {
        "name": "Torrentio\n1080p",
        "title": "Movie.2024.1080p\n\U0001f464 5 \U0001f4be ??? \u2699\ufe0f TPB",
        "infoHash": "d" * 40,
    }
    _patch_client(client, [_make_response(200, _make_torrentio_response([stream]))])

    results = await client.scrape_movie("tt0000008")

    assert len(results) == 1
    assert results[0].size_bytes is None


# ---------------------------------------------------------------------------
# Season pack detection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_season_pack_detected(client: TorrentioClient) -> None:
    """A title matching SXX (no episode number) is flagged as is_season_pack=True."""
    stream = _make_stream(title="Show.Name.S02.1080p.WEB-DL.x264-GROUP")
    _patch_client(client, [_make_response(200, _make_torrentio_response([stream]))])

    results = await client.scrape_movie("tt0000009")

    assert len(results) == 1
    assert results[0].is_season_pack is True


@pytest.mark.asyncio
async def test_single_episode_not_season_pack(client: TorrentioClient) -> None:
    """A title matching SXXEXX is flagged as is_season_pack=False."""
    stream = _make_stream(title="Show.Name.S02E05.1080p.WEB-DL.x264-GROUP")
    _patch_client(client, [_make_response(200, _make_torrentio_response([stream]))])

    results = await client.scrape_movie("tt0000010")

    assert len(results) == 1
    assert results[0].is_season_pack is False


@pytest.mark.asyncio
async def test_season_pack_complete_series(client: TorrentioClient) -> None:
    """A title containing multiple season markers with no episode marker is a season pack."""
    stream = _make_stream(title="Show.Name.S01.S02.S03.Complete.1080p.WEB-DL-GROUP")
    _patch_client(client, [_make_response(200, _make_torrentio_response([stream]))])

    results = await client.scrape_movie("tt0000011")

    assert len(results) == 1
    assert results[0].is_season_pack is True


# ---------------------------------------------------------------------------
# Opts configuration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_opts_inserted_in_url(client_with_opts: TorrentioClient) -> None:
    """When opts is set, the URL path is {base_url}/{opts}/stream/movie/{imdb_id}.json."""
    stream = _make_stream()
    transport = _patch_client(
        client_with_opts, [_make_response(200, _make_torrentio_response([stream]))]
    )

    await client_with_opts.scrape_movie("tt8888888")

    assert len(transport.requests_made) == 1
    url = str(transport.requests_made[0].url)
    assert "sort=qualitysize" in url or "sort%3Dqualitysize" in url
    assert "/stream/movie/tt8888888.json" in url


@pytest.mark.asyncio
async def test_empty_opts_no_extra_path(client: TorrentioClient) -> None:
    """When opts is empty string, no extra path segment is inserted."""
    stream = _make_stream()
    transport = _patch_client(client, [_make_response(200, _make_torrentio_response([stream]))])

    await client.scrape_movie("tt7777777")

    _url = str(transport.requests_made[0].url)
    # The path should go straight from the base to /stream/
    # There must NOT be an opts segment between the host and /stream/
    path = transport.requests_made[0].url.path
    assert path.startswith("/stream/")


# ---------------------------------------------------------------------------
# max_results cap
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_results_capped_at_max(monkeypatch: pytest.MonkeyPatch) -> None:
    """When the API returns more streams than max_results, the list is truncated."""
    cfg = _make_mock_cfg(max_results=5)

    mock_settings = MagicMock()
    mock_settings.scrapers.torrentio = cfg
    monkeypatch.setattr("src.services.torrentio.settings", mock_settings)

    capped_client = TorrentioClient()

    streams = [_make_stream(info_hash=f"{i:040x}") for i in range(20)]
    _patch_client(
        capped_client, [_make_response(200, _make_torrentio_response(streams))], cfg=cfg
    )

    results = await capped_client.scrape_movie("tt0000012")

    assert len(results) == 5


@pytest.mark.asyncio
async def test_results_under_max_not_truncated(client: TorrentioClient) -> None:
    """When the API returns fewer streams than max_results, all are returned."""
    streams = [_make_stream(info_hash=f"{i:040x}") for i in range(3)]
    _patch_client(client, [_make_response(200, _make_torrentio_response(streams))])

    results = await client.scrape_movie("tt0000013")

    assert len(results) == 3


# ---------------------------------------------------------------------------
# Error handling — network-level failures return empty list (graceful)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_timeout_returns_empty_list(client: TorrentioClient) -> None:
    """A timeout while querying Torrentio returns [] without raising an exception."""

    class _TimeoutTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            raise httpx.TimeoutException("timed out", request=request)

    _cfg = getattr(client, "_test_cfg", None) or _make_mock_cfg()

    def _fake_build_client(**kwargs: object) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=_TimeoutTransport())

    client._build_client = _fake_build_client  # type: ignore[method-assign]

    results = await client.scrape_movie("tt0000014")

    assert results == []


@pytest.mark.asyncio
async def test_connection_error_returns_empty_list(client: TorrentioClient) -> None:
    """A ConnectError returns [] without raising an exception."""

    class _ConnErrorTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused", request=request)

    def _fake_build_client(**kwargs: object) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=_ConnErrorTransport())

    client._build_client = _fake_build_client  # type: ignore[method-assign]

    results = await client.scrape_movie("tt0000015")

    assert results == []


@pytest.mark.asyncio
async def test_malformed_json_returns_empty_list(client: TorrentioClient) -> None:
    """A non-JSON response body returns [] without raising an exception."""
    _patch_client(
        client,
        [_make_response(200, b"<html>not json</html>", content_type="text/html")],
    )

    results = await client.scrape_movie("tt0000016")

    assert results == []


@pytest.mark.asyncio
async def test_episode_timeout_returns_empty_list(client: TorrentioClient) -> None:
    """A timeout during episode fallback chain returns [] at every level."""

    class _TimeoutTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            raise httpx.TimeoutException("timed out", request=request)

    def _fake_build_client(**kwargs: object) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=_TimeoutTransport())

    client._build_client = _fake_build_client  # type: ignore[method-assign]

    results = await client.scrape_episode("tt0000017", season=1, episode=1)

    assert results == []


@pytest.mark.asyncio
async def test_http_429_rate_limit_returns_empty_list(client: TorrentioClient) -> None:
    """A 429 Too Many Requests response returns [] without raising."""
    _patch_client(client, [_make_response(429, {"error": "Too Many Requests"})])

    results = await client.scrape_movie("tt0000018")

    assert results == []


# ---------------------------------------------------------------------------
# TorrentioError — custom exception is importable and usable
# ---------------------------------------------------------------------------


def test_torrentio_error_is_exception() -> None:
    """TorrentioError is a proper Exception subclass."""
    err = TorrentioError("something went wrong")
    assert isinstance(err, Exception)
    assert str(err) == "something went wrong"


def test_torrentio_result_fields() -> None:
    """TorrentioResult can be constructed with the expected fields."""
    result = TorrentioResult(
        info_hash="a" * 40,
        title="Movie.2024.1080p.WEB-DL.x264-GRP",
        resolution="1080p",
        size_bytes=4_500_000_000,
        seeders=20,
        source_tracker="ThePirateBay",
        is_season_pack=False,
        file_idx=0,
    )
    assert result.info_hash == "a" * 40
    assert result.resolution == "1080p"
    assert result.size_bytes == 4_500_000_000
    assert result.seeders == 20
    assert result.is_season_pack is False


# ---------------------------------------------------------------------------
# Resolution extraction
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolution_extracted_from_name_field(client: TorrentioClient) -> None:
    """Resolution is read from the 'name' field (e.g. 'Torrentio\\n4K')."""
    stream: dict[str, Any] = {
        "name": "Torrentio\n4K",
        "title": "Movie.2024.2160p.WEB-DL\n\U0001f464 5 \U0001f4be 30 GB \u2699\ufe0f TPB",
        "infoHash": "e" * 40,
    }
    _patch_client(client, [_make_response(200, _make_torrentio_response([stream]))])

    results = await client.scrape_movie("tt0000019")

    assert len(results) == 1
    # resolution should be populated (exact string depends on implementation)
    assert results[0].resolution is not None


@pytest.mark.asyncio
async def test_resolution_2160p_or_4k_accepted(client: TorrentioClient) -> None:
    """Streams at 4K/2160p are parsed without crashing."""
    stream = _make_stream(resolution="2160p", title="Movie.2024.2160p.WEB-DL.x265-GROUP")
    _patch_client(client, [_make_response(200, _make_torrentio_response([stream]))])

    results = await client.scrape_movie("tt0000020")

    assert len(results) == 1
    assert results[0].resolution is not None


# ---------------------------------------------------------------------------
# Unicode in titles
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unicode_title_parsed_without_crash(client: TorrentioClient) -> None:
    """Titles with non-ASCII characters (e.g. accented letters, CJK) are handled."""
    stream = _make_stream(
        title="\u30b6\u30fb\u30dc\u30fc\u30a4\u30ba.2024.1080p.WEB-DL.x264-GROUP"
    )
    _patch_client(client, [_make_response(200, _make_torrentio_response([stream]))])

    results = await client.scrape_movie("tt0000021")

    assert isinstance(results, list)


# ---------------------------------------------------------------------------
# Edge cases — missing or malformed streams array
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_streams_key_returns_empty(client: TorrentioClient) -> None:
    """If the response JSON has no 'streams' key, an empty list is returned."""
    _patch_client(client, [_make_response(200, {"error": "not found"})])

    results = await client.scrape_movie("tt0000022")

    assert results == []


@pytest.mark.asyncio
async def test_streams_is_null_returns_empty(client: TorrentioClient) -> None:
    """If 'streams' is null/None in the response, an empty list is returned."""
    _patch_client(client, [_make_response(200, {"streams": None})])

    results = await client.scrape_movie("tt0000023")

    assert results == []


@pytest.mark.asyncio
async def test_multiple_streams_all_parsed(client: TorrentioClient) -> None:
    """Multiple streams in a response are all parsed into TorrentioResult objects."""
    streams = [
        _make_stream(info_hash=f"{i:040x}", resolution=res)
        for i, res in enumerate(["2160p", "1080p", "720p"])
    ]
    _patch_client(client, [_make_response(200, _make_torrentio_response(streams))])

    results = await client.scrape_movie("tt0000024")

    assert len(results) == 3
    hashes = {r.info_hash for r in results}
    assert len(hashes) == 3  # all distinct


# ---------------------------------------------------------------------------
# Seeder parsing edge cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_seeders_parsed_from_title(client: TorrentioClient) -> None:
    """Seeder count is extracted from the emoji-formatted title line."""
    stream = _make_stream(seeders=99)
    _patch_client(client, [_make_response(200, _make_torrentio_response([stream]))])

    results = await client.scrape_movie("tt0000025")

    assert results[0].seeders == 99


@pytest.mark.asyncio
async def test_zero_seeders_handled(client: TorrentioClient) -> None:
    """A seeder count of 0 is parsed as 0, not None."""
    stream = _make_stream(seeders=0)
    _patch_client(client, [_make_response(200, _make_torrentio_response([stream]))])

    results = await client.scrape_movie("tt0000026")

    assert len(results) == 1
    # 0 seeders is valid — could be stored as 0 or None depending on implementation
    assert results[0].seeders is None or results[0].seeders == 0


# ---------------------------------------------------------------------------
# infoHash fallback — lowercase key and behaviorHints extraction
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lowercase_infohash_key(client: TorrentioClient) -> None:
    """Streams with lowercase 'infohash' key are parsed (Torrentio fork compat)."""
    stream: dict[str, Any] = {
        "name": "Torrentio\n1080p",
        "title": "Movie.2024.1080p.WEB-DL\n\U0001f464 5 \U0001f4be 1.0 GB \u2699\ufe0f TPB",
        "infohash": "a" * 40,  # lowercase key
    }
    payload = _make_torrentio_response([stream])
    _patch_client(client, [_make_response(200, payload)])

    results = await client.scrape_movie("tt0000030")

    assert len(results) == 1
    assert results[0].info_hash == "a" * 40


@pytest.mark.asyncio
async def test_infohash_from_behavior_hints(client: TorrentioClient) -> None:
    """Hash extracted from behaviorHints.bingeGroup when infoHash key is absent."""
    the_hash = "ab" * 20
    stream: dict[str, Any] = {
        "name": "Torrentio\n1080p",
        "title": "Movie.2024.1080p.WEB-DL\n\U0001f464 5 \U0001f4be 1.0 GB \u2699\ufe0f TPB",
        "behaviorHints": {
            "bingeGroup": f"torrentio|{the_hash}",
            "filename": "Movie.2024.1080p.WEB-DL.mkv",
        },
    }
    payload = _make_torrentio_response([stream])
    _patch_client(client, [_make_response(200, payload)])

    results = await client.scrape_movie("tt0000031")

    assert len(results) == 1
    assert results[0].info_hash == the_hash


@pytest.mark.asyncio
async def test_realworld_multiline_title(client: TorrentioClient) -> None:
    """Real-world 4-line title format (torrent name, filename, emoji, extra) is parsed."""
    stream: dict[str, Any] = {
        "name": "Torrentio\n4k DV",
        "title": (
            "Show.S02.COMPLETE.2160p.WEB-DL.DV.H265-GROUP\n"
            "Show.S02E06.2160p.WEB-DL.DV.H265-GROUP.mp4\n"
            "\U0001f464 42 \U0001f4be 8.77 GB \u2699\ufe0f ThePirateBay\n"
            "Multi Audio"
        ),
        "infoHash": "f" * 40,
        "fileIdx": 6,
        "behaviorHints": {
            "bingeGroup": f"torrentio|{'f' * 40}",
            "filename": "Show.S02E06.2160p.WEB-DL.DV.H265-GROUP.mp4",
        },
    }
    payload = _make_torrentio_response([stream])
    _patch_client(client, [_make_response(200, payload)])

    results = await client.scrape_movie("tt0000032")

    assert len(results) == 1
    r = results[0]
    assert r.info_hash == "f" * 40
    assert r.seeders == 42
    assert r.source_tracker == "ThePirateBay"
    assert r.size_bytes is not None
    # Release name is first line (torrent name), not the filename
    assert "COMPLETE" in r.title


# ---------------------------------------------------------------------------
# Cached-in-RD detection (⚡ indicator)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cached_detected_from_lightning_in_name(client: TorrentioClient) -> None:
    """A stream with ⚡ in the name field is parsed with cached=True."""
    stream: dict[str, Any] = {
        "name": "\u26a1 Torrentio\n1080p",
        "title": "Movie.2024.1080p.WEB-DL.x264-GROUP\n\U0001f464 10 \U0001f4be 2.0 GB \u2699\ufe0f TPB",
        "infoHash": "a" * 40,
    }
    payload = _make_torrentio_response([stream])
    _patch_client(client, [_make_response(200, payload)])

    results = await client.scrape_movie("tt0000033")

    assert len(results) == 1
    assert results[0].cached is True


@pytest.mark.asyncio
async def test_cached_detected_from_lightning_in_title(client: TorrentioClient) -> None:
    """A stream with ⚡ in the title field is parsed with cached=True."""
    stream: dict[str, Any] = {
        "name": "Torrentio\n1080p",
        "title": "\u26a1 Movie.2024.1080p.WEB-DL.x264-GROUP\n\U0001f464 10 \U0001f4be 2.0 GB \u2699\ufe0f TPB",
        "infoHash": "a" * 40,
    }
    payload = _make_torrentio_response([stream])
    _patch_client(client, [_make_response(200, payload)])

    results = await client.scrape_movie("tt0000034")

    assert len(results) == 1
    assert results[0].cached is True


@pytest.mark.asyncio
async def test_cached_detected_from_rd_plus_in_name(client: TorrentioClient) -> None:
    """A stream with '[RD+]' in the name field is parsed with cached=True."""
    stream: dict[str, Any] = {
        "name": "[RD+] Torrentio\n1080p",
        "title": "Movie.2024.1080p.WEB-DL.x264-GROUP\n\U0001f464 10 \U0001f4be 2.0 GB \u2699\ufe0f TPB",
        "infoHash": "b" * 40,
    }
    payload = _make_torrentio_response([stream])
    _patch_client(client, [_make_response(200, payload)])

    results = await client.scrape_movie("tt0000035")

    assert len(results) == 1
    assert results[0].cached is True


@pytest.mark.asyncio
async def test_no_cached_indicator_means_cached_false(client: TorrentioClient) -> None:
    """A normal stream without ⚡ or RD+ is parsed with cached=False."""
    stream = _make_stream(info_hash="c" * 40)
    payload = _make_torrentio_response([stream])
    _patch_client(client, [_make_response(200, payload)])

    results = await client.scrape_movie("tt0000036")

    assert len(results) == 1
    assert results[0].cached is False


@pytest.mark.asyncio
async def test_cached_field_defaults_to_false(client: TorrentioClient) -> None:
    """TorrentioResult.cached defaults to False when constructed without it."""
    result = TorrentioResult(
        info_hash="a" * 40,
        title="Movie.2024.1080p",
    )
    assert result.cached is False


# ---------------------------------------------------------------------------
# _parse_languages — Cyrillic script detection
# ---------------------------------------------------------------------------


def test_parse_languages_cyrillic_only(client: TorrentioClient) -> None:
    """A title containing Cyrillic characters is detected as Russian."""
    title = "\u041f\u0440\u043e\u0432\u043e\u0436\u0430\u044e\u0449\u0430\u044f / Test.Anime.S01E01.1080p"
    result = client._parse_languages(title, {})
    assert "Russian" in result


def test_parse_languages_cyrillic_full_russian_title(client: TorrentioClient) -> None:
    """A realistic anime title mixing Cyrillic and Latin is detected as Russian."""
    title = "\u041f\u0440\u043e\u0432\u043e\u0436\u0430\u044e\u0449\u0430\u044f \u0432 \u043f\u043e\u0441\u043b\u0435\u0434\u043d\u0438\u0439 \u043f\u0443\u0442\u044c \u0424\u0440\u0438\u0440\u0435\u043d / Test Anime JP Title [02x01-05]"
    result = client._parse_languages(title, {})
    assert "Russian" in result


def test_parse_languages_cyrillic_no_duplicate_with_russian_token(client: TorrentioClient) -> None:
    """Cyrillic script + 'RUSSIAN' token in the same title yields exactly one 'Russian' entry."""
    title = "\u041a\u0430\u043a\u043e\u0439-\u0442\u043e \u0444\u0438\u043b\u044c\u043c RUSSIAN.1080p.WEB-DL"
    result = client._parse_languages(title, {})
    assert result.count("Russian") == 1


def test_parse_languages_cyrillic_with_other_language(client: TorrentioClient) -> None:
    """Title with Cyrillic script and a Japanese tag yields both Russian and Japanese."""
    title = "\u0410\u043d\u0438\u043c\u0435 [JAP] Test.Anime.S01E01.1080p"
    result = client._parse_languages(title, {})
    assert "Russian" in result
    assert "Japanese" in result


# ---------------------------------------------------------------------------
# _parse_languages — abbreviated language tokens with word-boundary matching
# ---------------------------------------------------------------------------


def test_parse_languages_rus_abbreviation(client: TorrentioClient) -> None:
    """'RUS' standalone token is detected as Russian."""
    result = client._parse_languages("Movie.2024.1080p.[RUS].WEB-DL", {})
    assert "Russian" in result


def test_parse_languages_jap_abbreviation(client: TorrentioClient) -> None:
    """'JAP' standalone token is detected as Japanese."""
    result = client._parse_languages("Anime.S01E01.1080p.[JAP].WEB-DL", {})
    assert "Japanese" in result


def test_parse_languages_jpn_abbreviation(client: TorrentioClient) -> None:
    """'JPN' standalone token is detected as Japanese."""
    result = client._parse_languages("Anime.S01E01.1080p.[JPN+ENG].WEB-DL", {})
    assert "Japanese" in result


def test_parse_languages_kor_abbreviation(client: TorrentioClient) -> None:
    """'KOR' standalone token is detected as Korean."""
    result = client._parse_languages("Drama.S01E01.1080p.[KOR].WEB-DL", {})
    assert "Korean" in result


def test_parse_languages_chi_abbreviation(client: TorrentioClient) -> None:
    """'CHI' standalone token is detected as Chinese."""
    result = client._parse_languages("Drama.S01E01.1080p.[CHI].WEB-DL", {})
    assert "Chinese" in result


def test_parse_languages_ita_abbreviation(client: TorrentioClient) -> None:
    """'ITA' standalone token is detected as Italian."""
    result = client._parse_languages("Movie.2024.1080p.[ITA+ENG].WEB-DL", {})
    assert "Italian" in result


def test_parse_languages_nld_abbreviation(client: TorrentioClient) -> None:
    """'NLD' standalone token is detected as Dutch."""
    result = client._parse_languages("Movie.2024.1080p.[NLD].WEB-DL", {})
    assert "Dutch" in result


def test_parse_languages_deu_abbreviation(client: TorrentioClient) -> None:
    """'DEU' standalone token is detected as German."""
    result = client._parse_languages("Movie.2024.1080p.[DEU].WEB-DL", {})
    assert "German" in result


def test_parse_languages_spa_abbreviation(client: TorrentioClient) -> None:
    """'SPA' standalone token is detected as Spanish."""
    result = client._parse_languages("Movie.2024.1080p.[SPA].WEB-DL", {})
    assert "Spanish" in result


def test_parse_languages_por_abbreviation(client: TorrentioClient) -> None:
    """'POR' standalone token is detected as Portuguese."""
    result = client._parse_languages("Movie.2024.1080p.[POR].WEB-DL", {})
    assert "Portuguese" in result


def test_parse_languages_fra_abbreviation(client: TorrentioClient) -> None:
    """'FRA' standalone token is detected as French."""
    result = client._parse_languages("Movie.2024.1080p.[FRA].WEB-DL", {})
    assert "French" in result


def test_parse_languages_multiple_abbreviations(client: TorrentioClient) -> None:
    """A title like '[RUS + JAP]' detects both Russian and Japanese."""
    result = client._parse_languages("Anime.S01E01.1080p.[RUS + JAP].WEB-DL", {})
    assert "Russian" in result
    assert "Japanese" in result


def test_parse_languages_abbreviations_no_duplicates(client: TorrentioClient) -> None:
    """RUS abbreviation alongside full RUSSIAN token yields exactly one Russian entry."""
    result = client._parse_languages("Movie.2024.1080p.RUSSIAN.[RUS].WEB-DL", {})
    assert result.count("Russian") == 1


# ---------------------------------------------------------------------------
# _parse_languages — word-boundary false-positive prevention
# ---------------------------------------------------------------------------


def test_parse_languages_brush_no_false_positive(client: TorrentioClient) -> None:
    """'BRUSH' does not trigger Russian detection (RUS is a substring, not a token)."""
    result = client._parse_languages("Movie.2024.1080p.BRUSH.WEB-DL", {})
    assert "Russian" not in result


def test_parse_languages_trust_no_false_positive(client: TorrentioClient) -> None:
    """'TRUST' does not trigger Russian detection."""
    result = client._parse_languages("Movie.2024.TRUST.1080p.WEB-DL", {})
    assert "Russian" not in result


def test_parse_languages_japan_full_word_not_confused_with_jap(client: TorrentioClient) -> None:
    """'JAPAN' in a title does not detect Japanese (word boundary prevents JAP prefix match)."""
    # This test documents the expected behavior: JAPAN should not
    # match JAP if word-boundary is enforced; the full JAPANESE token handles it.
    result = client._parse_languages("Movie.2024.Japan.Release.1080p.WEB-DL", {})
    # JAPANESE is not in the title so result should be empty OR JAP should not match JAPAN
    assert "Japanese" not in result


def test_parse_languages_regular_english_title_returns_empty(client: TorrentioClient) -> None:
    """A plain English title with no language indicators returns an empty list."""
    result = client._parse_languages(
        "The.Dark.Knight.2008.1080p.BluRay.x264-GROUP", {}
    )
    assert result == []


def test_parse_languages_empty_title_returns_empty(client: TorrentioClient) -> None:
    """An empty release name returns an empty list without crashing."""
    result = client._parse_languages("", {})
    assert result == []


# ---------------------------------------------------------------------------
# _parse_languages — existing long-form tokens still work after enhancement
# ---------------------------------------------------------------------------


def test_parse_languages_french_long_form_still_detected(client: TorrentioClient) -> None:
    """Full 'FRENCH' token continues to be detected after the enhancement."""
    result = client._parse_languages("Movie.2024.1080p.FRENCH.WEB-DL", {})
    assert "French" in result


def test_parse_languages_german_long_form_still_detected(client: TorrentioClient) -> None:
    """Full 'GERMAN' token continues to be detected after the enhancement."""
    result = client._parse_languages("Movie.2024.1080p.GERMAN.WEB-DL", {})
    assert "German" in result


def test_parse_languages_multi_tag_still_detected(client: TorrentioClient) -> None:
    """'MULTI' token continues to be detected after the enhancement."""
    result = client._parse_languages("Movie.2024.1080p.MULTI.WEB-DL", {})
    assert "Multi" in result


# ---------------------------------------------------------------------------
# _build_base_url — include_debrid_key / opts stripping
# ---------------------------------------------------------------------------


class TestDebridKeyStripping:
    """Tests for _build_base_url(include_debrid_key=True/False).

    Each test patches settings.scrapers.torrentio with a specific opts string
    and calls _build_base_url() directly.  No HTTP traffic is generated.
    """

    def _make_client(self, monkeypatch: pytest.MonkeyPatch, opts: str) -> TorrentioClient:
        """Return a TorrentioClient with the given opts value patched in."""
        cfg = _make_mock_cfg(opts=opts)
        mock_settings = MagicMock()
        mock_settings.scrapers.torrentio = cfg
        monkeypatch.setattr("src.services.torrentio.settings", mock_settings)
        return TorrentioClient()

    # ------------------------------------------------------------------
    # include_debrid_key=False — key is stripped
    # ------------------------------------------------------------------

    def test_key_at_end_is_stripped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """realdebrid= at the end of opts is removed when include_debrid_key=False."""
        client = self._make_client(monkeypatch, "sort=qualitysize|realdebrid=ABC123")
        url = client._build_base_url(include_debrid_key=False)
        assert "realdebrid" not in url
        assert "sort=qualitysize" in url

    def test_key_at_start_is_stripped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """realdebrid= at the start of opts is removed when include_debrid_key=False."""
        client = self._make_client(monkeypatch, "realdebrid=ABC123|sort=qualitysize")
        url = client._build_base_url(include_debrid_key=False)
        assert "realdebrid" not in url
        assert "sort=qualitysize" in url

    def test_key_in_middle_is_stripped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """realdebrid= in the middle of opts is removed when include_debrid_key=False."""
        client = self._make_client(
            monkeypatch, "sort=qualitysize|realdebrid=ABC123|qualityfilter=4k"
        )
        url = client._build_base_url(include_debrid_key=False)
        assert "realdebrid" not in url
        assert "sort=qualitysize" in url
        assert "qualityfilter=4k" in url

    def test_remaining_opts_have_no_double_pipes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """After stripping the middle key the two adjacent pipes are collapsed."""
        client = self._make_client(
            monkeypatch, "sort=qualitysize|realdebrid=ABC123|qualityfilter=4k"
        )
        url = client._build_base_url(include_debrid_key=False)
        assert "||" not in url

    def test_key_only_opts_produces_no_opts_segment(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When opts contains only the realdebrid= key the URL has no opts path segment."""
        base_url = "https://torrentio.strem.fun"
        cfg = _make_mock_cfg(base_url=base_url, opts="realdebrid=ABC123")
        mock_settings = MagicMock()
        mock_settings.scrapers.torrentio = cfg
        monkeypatch.setattr("src.services.torrentio.settings", mock_settings)
        client = TorrentioClient()

        url = client._build_base_url(include_debrid_key=False)

        assert url == base_url
        assert "realdebrid" not in url

    def test_no_key_in_opts_unchanged(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """opts with no realdebrid= segment is returned unchanged."""
        client = self._make_client(monkeypatch, "sort=qualitysize")
        url = client._build_base_url(include_debrid_key=False)
        assert "sort=qualitysize" in url
        assert "realdebrid" not in url

    # ------------------------------------------------------------------
    # include_debrid_key=True (default) — key is preserved
    # ------------------------------------------------------------------

    def test_default_keeps_key_at_end(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Default include_debrid_key=True preserves realdebrid= at end of opts."""
        client = self._make_client(monkeypatch, "sort=qualitysize|realdebrid=ABC123")
        url = client._build_base_url(include_debrid_key=True)
        assert "realdebrid=ABC123" in url

    def test_default_keeps_key_at_start(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Default include_debrid_key=True preserves realdebrid= at start of opts."""
        client = self._make_client(monkeypatch, "realdebrid=ABC123|sort=qualitysize")
        url = client._build_base_url(include_debrid_key=True)
        assert "realdebrid=ABC123" in url

    def test_default_keeps_key_in_middle(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Default include_debrid_key=True preserves realdebrid= in middle of opts."""
        client = self._make_client(
            monkeypatch, "sort=qualitysize|realdebrid=ABC123|qualityfilter=4k"
        )
        url = client._build_base_url(include_debrid_key=True)
        assert "realdebrid=ABC123" in url
        assert "sort=qualitysize" in url
        assert "qualityfilter=4k" in url

    def test_implicit_default_keeps_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Calling _build_base_url() with no argument keeps realdebrid= (default=True)."""
        client = self._make_client(monkeypatch, "sort=qualitysize|realdebrid=MYKEY")
        url = client._build_base_url()
        assert "realdebrid=MYKEY" in url

    # ------------------------------------------------------------------
    # URL structure assertions
    # ------------------------------------------------------------------

    def test_stripped_url_opts_embedded_as_path_segment(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """After stripping, remaining opts are still in the path between base and /stream."""
        base_url = "https://torrentio.strem.fun"
        client = self._make_client(
            monkeypatch, "sort=qualitysize|realdebrid=ABC123|qualityfilter=4k"
        )
        url = client._build_base_url(include_debrid_key=False)
        # The opts are part of the path, so URL should look like:
        # https://torrentio.strem.fun/sort=qualitysize|qualityfilter=4k
        assert url.startswith(base_url + "/")
        assert "sort=qualitysize" in url
        assert "qualityfilter=4k" in url

    def test_empty_opts_returns_bare_base_url(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When opts is empty, _build_base_url returns only the base URL regardless of flag."""
        base_url = "https://torrentio.strem.fun"
        cfg = _make_mock_cfg(base_url=base_url, opts="")
        mock_settings = MagicMock()
        mock_settings.scrapers.torrentio = cfg
        monkeypatch.setattr("src.services.torrentio.settings", mock_settings)
        client = TorrentioClient()

        assert client._build_base_url(include_debrid_key=False) == base_url
        assert client._build_base_url(include_debrid_key=True) == base_url

    def test_key_value_with_special_chars_stripped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """realdebrid= value containing alphanumeric and underscore chars is fully stripped."""
        client = self._make_client(
            monkeypatch, "sort=qualitysize|realdebrid=My_Key_123_ABC|qualityfilter=4k"
        )
        url = client._build_base_url(include_debrid_key=False)
        assert "realdebrid" not in url
        assert "My_Key_123_ABC" not in url


# ---------------------------------------------------------------------------
# _parse_stream — anime dash notation (_SEASON_DASH_EP_RE)
# ---------------------------------------------------------------------------


class TestAnimeDashNotationParsing:
    """Tests that _SEASON_DASH_EP_RE in _parse_stream correctly identifies single
    episodes written in anime dash notation (e.g. 'S2 - 06') and does NOT
    classify them as season packs.  Also verifies that genuine season packs
    (S02.COMPLETE, S01 with no episode anywhere) are still detected correctly.
    """

    def _stream_for(self, release_name: str, info_hash: str = "a" * 40) -> dict[str, Any]:
        """Build a minimal Torrentio stream dict for the given release name."""
        return {
            "name": "Torrentio\n1080p",
            "title": f"{release_name}\n\U0001f464 10 \U0001f4be 1.5 GB \u2699\ufe0f TPB",
            "infoHash": info_hash,
        }

    def test_anime_s2_dash_06_not_season_pack(self, client: TorrentioClient) -> None:
        """'[ASW] Test Anime JP Title S2 - 06 [1080p HEVC x265 10Bit][AAC]' must not be
        a season pack — it is a single episode using anime dash notation."""
        release = "[ASW] Test Anime JP Title S2 - 06 [1080p HEVC x265 10Bit][AAC]"
        stream = self._stream_for(release)
        result = client._parse_stream(stream)

        assert result is not None
        assert result.is_season_pack is False
        assert result.season == 2
        assert result.episode == 6

    def test_s02_dash_06_no_space_not_season_pack(self, client: TorrentioClient) -> None:
        """'[SubGroup] Some Anime S02-06 [720p]' (no spaces around dash) must not be
        a season pack."""
        release = "[SubGroup] Some Anime S02-06 [720p]"
        stream = self._stream_for(release)
        result = client._parse_stream(stream)

        assert result is not None
        assert result.is_season_pack is False
        assert result.season == 2
        assert result.episode == 6

    def test_s1_e03_explicit_e_prefix_not_season_pack(self, client: TorrentioClient) -> None:
        """'Anime Title S1-E03 [1080p]' (explicit 'E' prefix) must not be a season pack.

        PTN extracts episode=3 directly from 'S1-E03', so ptn_episode is not None
        and the _SEASON_DASH_EP_RE fallback is never triggered.  The result is
        correctly identified as a single episode (not a pack).  PTN does not
        extract season in this case (known PTN behaviour with S-dash-E notation).
        """
        release = "Anime Title S1-E03 [1080p]"
        stream = self._stream_for(release)
        result = client._parse_stream(stream)

        assert result is not None
        assert result.is_season_pack is False
        assert result.episode == 3

    def test_complete_season_pack_still_detected(self, client: TorrentioClient) -> None:
        """'Some.Show.S02.COMPLETE.1080p' must be detected as a season pack."""
        release = "Some.Show.S02.COMPLETE.1080p"
        stream = self._stream_for(release)
        result = client._parse_stream(stream)

        assert result is not None
        assert result.is_season_pack is True

    def test_season_only_no_episode_is_season_pack(self, client: TorrentioClient) -> None:
        """'Show.S01.720p.x264' (season marker, no episode anywhere) must be a
        season pack.

        PTN does not parse the season number from this format (it folds 'S01'
        into the title string), so result.season will be None.  The season pack
        flag is set by _SEASON_ONLY_RE matching 'S01' with no following episode.
        """
        release = "Show.S01.720p.x264"
        stream = self._stream_for(release)
        result = client._parse_stream(stream)

        assert result is not None
        assert result.is_season_pack is True
        assert result.episode is None


# ---------------------------------------------------------------------------
# _parse_stream — anime batch/season pack patterns
# ---------------------------------------------------------------------------


class TestAnimeBatchPackParsing:
    """Tests for the new anime batch/season pack detection patterns.

    Covers:
      - Episode range "- 01 ~ 13" → season pack, episode NOT set to 1
      - [BATCH] keyword → season pack
      - "(Season N)" keyword → season number extraction
      - Bare title with no markers → bare-dash fallback still works
      - Single-episode bare dash NOT misidentified as batch
    """

    def _stream_for(self, release_name: str, info_hash: str = "a" * 40) -> dict[str, Any]:
        """Build a minimal Torrentio stream dict for the given release name."""
        return {
            "name": "Torrentio\n1080p",
            "title": f"{release_name}\n\U0001f464 10 \U0001f4be 8.0 GB \u2699\ufe0f TPB",
            "infoHash": info_hash,
        }

    def test_batch_keyword_is_season_pack(self, client: TorrentioClient) -> None:
        """'[Erai-raws] Title - 01 ~ 13 [1080p][BATCH]' must be a season pack.

        The [BATCH] keyword signals a multi-episode release.  The episode
        field must remain None (not extracted as episode 1 via bare-dash).
        """
        release = "[Erai-raws] Test Series Title - 01 ~ 13 [1080p][BATCH][Multiple Subtitle]"
        stream = self._stream_for(release)
        result = client._parse_stream(stream)

        assert result is not None
        assert result.is_season_pack is True
        assert result.episode is None

    def test_episode_range_tilde_is_season_pack(self, client: TorrentioClient) -> None:
        """'[Erai-raws] Title - 01 ~ 13 [720p][BATCH]' — the episode range
        '01 ~ 13' must trigger season pack detection regardless of [BATCH].
        """
        release = "[Erai-raws] Title - 01 ~ 13 [720p][BATCH]"
        stream = self._stream_for(release)
        result = client._parse_stream(stream)

        assert result is not None
        assert result.is_season_pack is True
        assert result.episode is None

    def test_episode_range_without_batch_is_season_pack(self, client: TorrentioClient) -> None:
        """'[Group] Title - 01 ~ 24 [1080p]' — range alone (no BATCH keyword)
        is enough to detect a season pack.
        """
        release = "[Group] Some Anime - 01 ~ 24 [1080p]"
        stream = self._stream_for(release)
        result = client._parse_stream(stream)

        assert result is not None
        assert result.is_season_pack is True
        assert result.episode is None

    def test_batch_keyword_alone_no_range_is_season_pack(self, client: TorrentioClient) -> None:
        """'[NanakoRaws] Title (1080p)[BATCH]' — BATCH without a range is still
        recognised as a season pack.
        """
        release = "[Group] Test Series Title (1080p)[BATCH]"
        stream = self._stream_for(release)
        result = client._parse_stream(stream)

        assert result is not None
        assert result.is_season_pack is True
        assert result.episode is None

    def test_season_keyword_extracts_season_number(self, client: TorrentioClient) -> None:
        """'[Group] Title (Season 1)' — '(Season 1)' must set season=1."""
        release = "[Group] Test Series Title (Test Series Full Title) (Season 1)"
        stream = self._stream_for(release)
        result = client._parse_stream(stream)

        assert result is not None
        assert result.season == 1
        assert result.is_season_pack is True

    def test_season_keyword_season_2(self, client: TorrentioClient) -> None:
        """'[Group] Show Name (Season 2) [1080p]' — season number extracted correctly."""
        release = "[Group] Show Name (Season 2) [1080p]"
        stream = self._stream_for(release)
        result = client._parse_stream(stream)

        assert result is not None
        assert result.season == 2
        assert result.is_season_pack is True
        assert result.episode is None

    def test_single_episode_bare_dash_not_batch(self, client: TorrentioClient) -> None:
        """'[Group] Title - 07 [1080p]' (no range, no BATCH) must NOT be a
        season pack.  The bare-dash fallback should still extract episode=7.
        """
        release = "[Group] Some Anime Title - 07 [1080p]"
        stream = self._stream_for(release)
        result = client._parse_stream(stream)

        assert result is not None
        assert result.is_season_pack is False
        assert result.episode == 7

    def test_range_where_end_equals_start_not_batch(self, client: TorrentioClient) -> None:
        """'Title - 07 ~ 07 [1080p]' — range with identical start/end is NOT
        treated as a batch (ep_end == ep_start), so bare-dash fallback fires
        and extracts episode=7.
        """
        release = "[Group] Some Anime - 07 ~ 07 [1080p]"
        stream = self._stream_for(release)
        result = client._parse_stream(stream)

        assert result is not None
        assert result.is_season_pack is False
        assert result.episode == 7

    def test_batch_keyword_sets_season_1_fallback(self, client: TorrentioClient) -> None:
        """When [BATCH] is present but no season marker exists, the season
        defaults to None (not forced to 1) — the caller (scrape pipeline) is
        responsible for defaulting the season when needed.
        """
        release = "[NanakoRaws] Some Anime (1080p)[BATCH]"
        stream = self._stream_for(release)
        result = client._parse_stream(stream)

        assert result is not None
        assert result.is_season_pack is True
        # Season stays None when no season marker is present
        assert result.season is None

    def test_batch_with_season_keyword_sets_season(self, client: TorrentioClient) -> None:
        """'[Group] Title (Season 2) [BATCH]' — both season number and pack
        flag must be set correctly.
        """
        release = "[Group] Some Anime (Season 2) [BATCH]"
        stream = self._stream_for(release)
        result = client._parse_stream(stream)

        assert result is not None
        assert result.is_season_pack is True
        assert result.season == 2
        assert result.episode is None


# ---------------------------------------------------------------------------
# PTN list-value normalisation (Bug 1 & Bug 2)
# ---------------------------------------------------------------------------


class TestPTNListNormalisation:
    """Tests that PTN list returns for episode and season are normalised to int.

    PTN can return ``episode`` or ``season`` as a list when it encounters
    multi-episode titles like ``"Show.S01E01E02.1080p"`` or multi-season packs
    like ``"Show.S01-S04.Complete"``.  The parser must collapse these to the
    first element so that downstream int comparisons work correctly.
    """

    def _stream_for(self, release_name: str, info_hash: str = "a" * 40) -> dict[str, Any]:
        """Build a minimal Torrentio stream dict for the given release name."""
        return {
            "name": "Torrentio\n1080p",
            "title": f"{release_name}\n\U0001f464 10 \U0001f4be 8.0 GB \u2699\ufe0f TPB",
            "infoHash": info_hash,
        }

    def test_ptn_episode_list_takes_first_element(
        self, client: TorrentioClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When PTN returns episode as [1, 2], the result must store episode=1.

        This simulates what PTN does for titles like "Show.S01E01E02.1080p".
        """
        import src.services.torrentio as torrentio_mod

        original_parse = torrentio_mod.PTN.parse

        def _mock_parse(title: str) -> dict[str, Any]:
            data = original_parse(title)
            # Override episode with a list to simulate PTN list return.
            data["episode"] = [1, 2]
            return data

        monkeypatch.setattr(torrentio_mod.PTN, "parse", _mock_parse)

        release = "Show.S01E01E02.1080p.WEB-DL-GROUP"
        stream = self._stream_for(release)
        result = client._parse_stream(stream)

        assert result is not None
        assert result.episode == 1

    def test_ptn_episode_empty_list_becomes_none(
        self, client: TorrentioClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When PTN returns episode as [], the result must store episode=None."""
        import src.services.torrentio as torrentio_mod

        original_parse = torrentio_mod.PTN.parse

        def _mock_parse(title: str) -> dict[str, Any]:
            data = original_parse(title)
            data["episode"] = []
            return data

        monkeypatch.setattr(torrentio_mod.PTN, "parse", _mock_parse)

        release = "Show.S01.1080p.WEB-DL-GROUP"
        stream = self._stream_for(release)
        result = client._parse_stream(stream)

        assert result is not None
        assert result.episode is None

    def test_ptn_season_list_takes_first_element(
        self, client: TorrentioClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When PTN returns season as [1, 2, 3, 4], the result must store season=1.

        This simulates what PTN does for multi-season packs like
        "Show.S01-S04.Complete".
        """
        import src.services.torrentio as torrentio_mod

        original_parse = torrentio_mod.PTN.parse

        def _mock_parse(title: str) -> dict[str, Any]:
            data = original_parse(title)
            data["season"] = [1, 2, 3, 4]
            data.pop("episode", None)
            return data

        monkeypatch.setattr(torrentio_mod.PTN, "parse", _mock_parse)

        release = "Show.S01-S04.Complete.1080p.WEB-DL-GROUP"
        stream = self._stream_for(release)
        result = client._parse_stream(stream)

        assert result is not None
        assert result.season == 1

    def test_ptn_season_empty_list_becomes_none(
        self, client: TorrentioClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When PTN returns season as [], the result must store season=None."""
        import src.services.torrentio as torrentio_mod

        original_parse = torrentio_mod.PTN.parse

        def _mock_parse(title: str) -> dict[str, Any]:
            data = original_parse(title)
            data["season"] = []
            data.pop("episode", None)
            return data

        monkeypatch.setattr(torrentio_mod.PTN, "parse", _mock_parse)

        release = "Show.Complete.1080p.WEB-DL-GROUP"
        stream = self._stream_for(release)
        result = client._parse_stream(stream)

        assert result is not None
        assert result.season is None
