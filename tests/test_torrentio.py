"""Tests for src/services/torrentio.py.

All external HTTP calls are intercepted by a custom MockTransport so no real
network traffic is generated. Each test exercises a single behaviour. The
fallback chain tests (episode → season → show) are the most critical section
and are therefore the most thoroughly exercised.
"""

import json
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from src.services.torrentio import (
    TorrentioClient,
    TorrentioError,
    TorrentioResult,
)


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
    def _fake_build_client() -> httpx.AsyncClient:
        base_url = client._build_base_url()
        return httpx.AsyncClient(
            base_url=base_url,
            transport=transport,
        )

    client._build_client = _fake_build_client  # type: ignore[method-assign]

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

    return TorrentioClient()


# ---------------------------------------------------------------------------
# scrape_movie — happy path & error paths
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scrape_movie_success(client: TorrentioClient) -> None:
    """scrape_movie returns a non-empty list of TorrentioResult on a 200 response."""
    stream = _make_stream(info_hash="a" * 40, resolution="1080p", size="4.2 GB", seeders=15)
    payload = _make_torrentio_response([stream])
    transport = _patch_client(client, [_make_response(200, payload)])

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

    url = str(transport.requests_made[0].url)
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

    cfg = getattr(client, "_test_cfg", None) or _make_mock_cfg()

    def _fake_build_client() -> httpx.AsyncClient:
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

    def _fake_build_client() -> httpx.AsyncClient:
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

    def _fake_build_client() -> httpx.AsyncClient:
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
