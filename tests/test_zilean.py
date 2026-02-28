"""Tests for src/services/zilean.py.

All external HTTP calls are intercepted by a custom _MockTransport so no real
network traffic is generated. Each test exercises a single behaviour.

The mocking pattern mirrors test_torrentio.py exactly:
  - monkeypatch patches src.services.zilean.settings so the mock stays alive
    during await calls (unlike with patch(...) which goes out of scope).
  - _MockTransport records every request in .requests_made for URL assertions.
  - _patch_client() replaces client._build_client to return an AsyncClient
    backed by the mock transport.
  - _make_response() builds fake httpx.Response objects.
"""

import json
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest

from src.services.zilean import (
    ZileanClient,
    ZileanError,
    ZileanResult,
)


# ---------------------------------------------------------------------------
# Helpers — mock data builders
# ---------------------------------------------------------------------------


def _make_zilean_entry(
    info_hash: str = "a" * 40,
    raw_title: str = "Movie.2024.1080p.WEB-DL.x264-GROUP",
    size: str = "4500000000",
    category: str = "movies",
    imdb_id: str = "tt1234567",
    seasons: list[int] | None = None,
    episodes: list[int] | None = None,
    year: int = 2024,
    resolution: str = "1080p",
    codec: str = "x264",
    quality: str = "WEB-DL",
    group: str | None = "GROUP",
) -> dict[str, Any]:
    """Build a single Zilean DMM result entry matching the live API schema."""
    return {
        "info_hash": info_hash,
        "raw_title": raw_title,
        "parsed_title": "Movie",
        "normalized_title": "movie",
        "size": size,
        "category": category,
        "imdb_id": imdb_id,
        "seasons": seasons or [],
        "episodes": episodes or [],
        "year": year,
        "resolution": resolution,
        "codec": codec,
        "quality": quality,
        "group": group,
        "languages": [],
        "complete": False,
        "trash": False,
        "adult": False,
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
        request=httpx.Request("GET", "http://localhost:8182/"),
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
    base_url: str = "http://localhost:8182",
    timeout_seconds: int = 10,
    enabled: bool = True,
) -> MagicMock:
    """Return a MagicMock that looks like a ZileanConfig object."""
    cfg = MagicMock()
    cfg.base_url = base_url
    cfg.timeout_seconds = timeout_seconds
    cfg.enabled = enabled
    return cfg


def _patch_client(
    client: ZileanClient,
    responses: list[httpx.Response] | None = None,
    *,
    responses_by_url: dict[str, httpx.Response] | None = None,
    default_response: httpx.Response | None = None,
) -> _MockTransport:
    """Monkey-patch *client._build_client* to inject a _MockTransport.

    Every call to ``client._build_client()`` (which happens inside the search
    method) will return a fresh ``httpx.AsyncClient`` backed by the same
    transport instance, so ``transport.requests_made`` accumulates correctly.

    Callers must ensure ``src.services.zilean.settings`` is patched via
    monkeypatch before awaiting — the fixtures in this module handle that.

    Args:
        client:           The ZileanClient instance under test.
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

    def _fake_build_client() -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url="http://localhost:8182",
            transport=transport,
        )

    client._build_client = _fake_build_client  # type: ignore[method-assign]
    return transport


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch) -> ZileanClient:
    """A ZileanClient configured with test defaults (no real HTTP)."""
    cfg = _make_mock_cfg()

    mock_settings = MagicMock()
    mock_settings.scrapers.zilean = cfg
    monkeypatch.setattr("src.services.zilean.settings", mock_settings)

    c = ZileanClient()
    c._test_cfg = cfg  # type: ignore[attr-defined]
    return c


@pytest.fixture()
def disabled_client(monkeypatch: pytest.MonkeyPatch) -> ZileanClient:
    """A ZileanClient where enabled=False — should short-circuit all requests."""
    cfg = _make_mock_cfg(enabled=False)

    mock_settings = MagicMock()
    mock_settings.scrapers.zilean = cfg
    monkeypatch.setattr("src.services.zilean.settings", mock_settings)

    return ZileanClient()


# ---------------------------------------------------------------------------
# Basic search — happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_success(client: ZileanClient) -> None:
    """search returns a non-empty list of ZileanResult on a valid 200 response."""
    entry = _make_zilean_entry(info_hash="a" * 40, resolution="1080p")
    transport = _patch_client(client, [_make_response(200, [entry])])

    results = await client.search("Movie 2024")

    assert len(results) == 1
    assert isinstance(results[0], ZileanResult)
    assert results[0].info_hash == "a" * 40


@pytest.mark.asyncio
async def test_search_sends_correct_query_params(client: ZileanClient) -> None:
    """search sends the Query param to /dmm/filtered."""
    entry = _make_zilean_entry()
    transport = _patch_client(client, [_make_response(200, [entry])])

    await client.search("Test Movie")

    assert len(transport.requests_made) == 1
    url = str(transport.requests_made[0].url)
    assert "/dmm/filtered" in url
    # Query param should appear in the URL (possibly percent-encoded)
    assert "Query=" in url or "query=" in url.lower()
    assert "Test" in url or "Test%20Movie" in url or "Test+Movie" in url


@pytest.mark.asyncio
async def test_search_with_season_episode(client: ZileanClient) -> None:
    """search includes Season and Episode query params when provided."""
    entry = _make_zilean_entry(seasons=[2], episodes=[5])
    transport = _patch_client(client, [_make_response(200, [entry])])

    await client.search("Show Name", season=2, episode=5)

    assert len(transport.requests_made) == 1
    url = str(transport.requests_made[0].url)
    assert "Season=2" in url or "season=2" in url.lower()
    assert "Episode=5" in url or "episode=5" in url.lower()


@pytest.mark.asyncio
async def test_search_with_year(client: ZileanClient) -> None:
    """search includes Year query param when provided."""
    entry = _make_zilean_entry(year=2024)
    transport = _patch_client(client, [_make_response(200, [entry])])

    await client.search("Movie", year=2024)

    assert len(transport.requests_made) == 1
    url = str(transport.requests_made[0].url)
    assert "Year=2024" in url or "year=2024" in url.lower()


@pytest.mark.asyncio
async def test_search_with_imdb_id(client: ZileanClient) -> None:
    """search includes ImdbId query param when provided."""
    entry = _make_zilean_entry(imdb_id="tt9876543")
    transport = _patch_client(client, [_make_response(200, [entry])])

    await client.search("Movie", imdb_id="tt9876543")

    assert len(transport.requests_made) == 1
    url = str(transport.requests_made[0].url)
    assert "tt9876543" in url


@pytest.mark.asyncio
async def test_search_no_results(client: ZileanClient) -> None:
    """search returns an empty list when the JSON array is empty."""
    _patch_client(client, [_make_response(200, [])])

    results = await client.search("Nonexistent Movie")

    assert results == []


@pytest.mark.asyncio
async def test_search_disabled(disabled_client: ZileanClient) -> None:
    """When enabled=False, search returns [] immediately without making any HTTP calls."""
    transport = _patch_client(disabled_client, [])

    results = await disabled_client.search("Any Movie")

    assert results == []
    assert len(transport.requests_made) == 0


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_parse_result_full_fields(client: ZileanClient) -> None:
    """All top-level metadata fields are mapped correctly to ZileanResult."""
    entry = _make_zilean_entry(
        info_hash="b" * 40,
        raw_title="Great.Movie.2024.1080p.WEB-DL.x264-GRP",
        size="3800000000",
        resolution="1080p",
        codec="x264",
        quality="WEB-DL",
        group="GRP",
    )
    _patch_client(client, [_make_response(200, [entry])])

    results = await client.search("Great Movie")

    assert len(results) == 1
    r = results[0]
    assert r.resolution == "1080p"
    assert r.codec == "x264"
    assert r.quality == "WEB-DL"
    assert r.release_group == "GRP"


@pytest.mark.asyncio
async def test_parse_result_size_bytes(client: ZileanClient) -> None:
    """The 'size' string from the response is parsed to size_bytes int."""
    entry = _make_zilean_entry(size="4500000000")
    _patch_client(client, [_make_response(200, [entry])])

    results = await client.search("Movie")

    assert len(results) == 1
    assert results[0].size_bytes == 4_500_000_000


@pytest.mark.asyncio
async def test_parse_result_size_large(client: ZileanClient) -> None:
    """Large size strings (>4GB) parse correctly."""
    entry = _make_zilean_entry(size="8232397607")
    _patch_client(client, [_make_response(200, [entry])])

    results = await client.search("Movie")

    assert len(results) == 1
    assert results[0].size_bytes == 8232397607


@pytest.mark.asyncio
async def test_parse_result_season_episode(client: ZileanClient) -> None:
    """seasons/episodes arrays are extracted to scalar season/episode."""
    entry = _make_zilean_entry(seasons=[3], episodes=[7])
    _patch_client(client, [_make_response(200, [entry])])

    results = await client.search("Show Name S03E07")

    assert len(results) == 1
    r = results[0]
    assert r.season == 3
    assert r.episode == 7


@pytest.mark.asyncio
async def test_parse_result_empty_seasons_episodes(client: ZileanClient) -> None:
    """Empty seasons/episodes arrays yield None for both fields."""
    entry = _make_zilean_entry(seasons=[], episodes=[])
    _patch_client(client, [_make_response(200, [entry])])

    results = await client.search("Movie")

    assert len(results) == 1
    assert results[0].season is None
    assert results[0].episode is None


@pytest.mark.asyncio
async def test_parse_result_missing_metadata_falls_back_to_ptn(client: ZileanClient) -> None:
    """When top-level metadata fields are empty, PTN parses the raw_title."""
    entry: dict[str, Any] = {
        "info_hash": "c" * 40,
        "raw_title": "Movie.2024.1080p.WEB-DL.x264-GROUP",
        "size": "4000000000",
        "category": "movies",
        "seasons": [],
        "episodes": [],
        "year": 2024,
        "resolution": None,
        "codec": None,
        "quality": None,
        "group": None,
    }
    _patch_client(client, [_make_response(200, [entry])])

    results = await client.search("Movie")

    assert len(results) == 1
    assert results[0].info_hash == "c" * 40


@pytest.mark.asyncio
async def test_parse_result_no_info_hash(client: ZileanClient) -> None:
    """Entries that are missing info_hash are skipped entirely."""
    entry_no_hash: dict[str, Any] = {
        # info_hash intentionally absent
        "raw_title": "Movie.2024.1080p.WEB-DL.x264-GROUP",
        "size": "4000000000",
        "category": "movies",
        "seasons": [],
        "episodes": [],
        "year": 2024,
        "resolution": "1080p",
        "codec": "x264",
        "quality": "WEB-DL",
        "group": "GROUP",
    }
    entry_good = _make_zilean_entry(info_hash="d" * 40)
    _patch_client(client, [_make_response(200, [entry_no_hash, entry_good])])

    results = await client.search("Movie")

    # Only the valid entry survives
    assert len(results) == 1
    assert results[0].info_hash == "d" * 40


@pytest.mark.asyncio
async def test_parse_result_empty_raw_title(client: ZileanClient) -> None:
    """Entries with an empty raw_title are skipped."""
    entry: dict[str, Any] = {
        "info_hash": "e" * 40,
        "raw_title": "",  # empty
        "size": "4000000000",
        "category": "movies",
        "seasons": [],
        "episodes": [],
        "year": 2024,
        "resolution": "1080p",
        "codec": "x264",
        "quality": "WEB-DL",
        "group": "GROUP",
    }
    _patch_client(client, [_make_response(200, [entry])])

    results = await client.search("Movie")

    assert results == []


@pytest.mark.asyncio
async def test_parse_result_info_hash_normalized(client: ZileanClient) -> None:
    """info_hash is stored as lowercase regardless of the casing in the response."""
    entry = _make_zilean_entry(info_hash="A1B2C3" + "0" * 34)
    _patch_client(client, [_make_response(200, [entry])])

    results = await client.search("Movie")

    assert len(results) == 1
    assert results[0].info_hash == ("a1b2c3" + "0" * 34)
    assert results[0].info_hash == results[0].info_hash.lower()


# ---------------------------------------------------------------------------
# Season pack detection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_season_pack_detected(client: ZileanClient) -> None:
    """When seasons is set and episodes is empty, is_season_pack is True."""
    entry = _make_zilean_entry(
        raw_title="Show.Name.S02.1080p.WEB-DL.x264-GROUP",
        seasons=[2],
        episodes=[],
    )
    _patch_client(client, [_make_response(200, [entry])])

    results = await client.search("Show Name S02")

    assert len(results) == 1
    assert results[0].is_season_pack is True
    assert results[0].season == 2
    assert results[0].episode is None


@pytest.mark.asyncio
async def test_single_episode_not_season_pack(client: ZileanClient) -> None:
    """When both seasons and episodes are set, is_season_pack is False."""
    entry = _make_zilean_entry(
        raw_title="Show.Name.S02E05.1080p.WEB-DL.x264-GROUP",
        seasons=[2],
        episodes=[5],
    )
    _patch_client(client, [_make_response(200, [entry])])

    results = await client.search("Show Name S02E05")

    assert len(results) == 1
    assert results[0].is_season_pack is False


# ---------------------------------------------------------------------------
# Error handling — all return empty list, no exceptions raised
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_timeout_returns_empty_list(client: ZileanClient) -> None:
    """A timeout while querying Zilean returns [] without raising an exception."""

    class _TimeoutTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            raise httpx.TimeoutException("timed out", request=request)

    def _fake_build_client() -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=_TimeoutTransport())

    client._build_client = _fake_build_client  # type: ignore[method-assign]

    results = await client.search("Movie")

    assert results == []


@pytest.mark.asyncio
async def test_connection_error_returns_empty_list(client: ZileanClient) -> None:
    """A ConnectError (Zilean is down) returns [] without raising an exception."""

    class _ConnErrorTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused", request=request)

    def _fake_build_client() -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=_ConnErrorTransport())

    client._build_client = _fake_build_client  # type: ignore[method-assign]

    results = await client.search("Movie")

    assert results == []


@pytest.mark.asyncio
async def test_http_500_returns_empty_list(client: ZileanClient) -> None:
    """An HTTP 500 server error returns [] without raising an exception."""
    _patch_client(client, [_make_response(500, {"error": "Internal Server Error"})])

    results = await client.search("Movie")

    assert results == []


@pytest.mark.asyncio
async def test_malformed_json_returns_empty_list(client: ZileanClient) -> None:
    """A non-JSON response body returns [] without raising an exception."""
    _patch_client(
        client,
        [_make_response(200, b"<html>not json</html>", content_type="text/html")],
    )

    results = await client.search("Movie")

    assert results == []


@pytest.mark.asyncio
async def test_http_404_returns_empty_list(client: ZileanClient) -> None:
    """An HTTP 404 Not Found response returns [] without raising an exception."""
    _patch_client(
        client,
        [_make_response(404, b"Not Found", content_type="text/plain")],
    )

    results = await client.search("Movie")

    assert results == []


# ---------------------------------------------------------------------------
# Data model — ZileanError and ZileanResult contracts
# ---------------------------------------------------------------------------


def test_zilean_error_is_exception() -> None:
    """ZileanError is a proper Exception subclass and carries its message."""
    err = ZileanError("something went wrong")
    assert isinstance(err, Exception)
    assert str(err) == "something went wrong"


def test_zilean_result_compatible_with_torrentio() -> None:
    """ZileanResult exposes the same field names as TorrentioResult for pipeline compatibility."""
    from src.services.torrentio import TorrentioResult

    zilean_fields = set(ZileanResult.model_fields.keys())
    torrentio_fields = set(TorrentioResult.model_fields.keys())

    expected_shared = {
        "info_hash",
        "title",
        "resolution",
        "codec",
        "quality",
        "release_group",
        "size_bytes",
        "seeders",
        "source_tracker",
        "season",
        "episode",
        "is_season_pack",
        "file_idx",
        "languages",
    }
    assert expected_shared.issubset(zilean_fields), (
        f"ZileanResult is missing fields: {expected_shared - zilean_fields}"
    )
    assert expected_shared.issubset(torrentio_fields), (
        f"TorrentioResult is missing fields: {expected_shared - torrentio_fields}"
    )


def test_zilean_result_seeders_always_none() -> None:
    """ZileanResult.seeders defaults to None (Zilean does not provide seeder counts)."""
    result = ZileanResult(
        info_hash="a" * 40,
        title="Movie.2024.1080p.WEB-DL.x264-GROUP",
    )
    assert result.seeders is None


def test_zilean_result_file_idx_always_none() -> None:
    """ZileanResult.file_idx defaults to None (Zilean does not provide file indices)."""
    result = ZileanResult(
        info_hash="a" * 40,
        title="Movie.2024.1080p.WEB-DL.x264-GROUP",
    )
    assert result.file_idx is None


def test_zilean_result_source_tracker_always_none() -> None:
    """ZileanResult.source_tracker defaults to None (Zilean does not provide tracker info)."""
    result = ZileanResult(
        info_hash="a" * 40,
        title="Movie.2024.1080p.WEB-DL.x264-GROUP",
    )
    assert result.source_tracker is None


def test_zilean_result_languages_default_empty() -> None:
    """ZileanResult.languages defaults to an empty list."""
    result = ZileanResult(
        info_hash="a" * 40,
        title="Movie.2024.1080p.WEB-DL.x264-GROUP",
    )
    assert result.languages == []


def test_zilean_result_is_season_pack_default_false() -> None:
    """ZileanResult.is_season_pack defaults to False."""
    result = ZileanResult(
        info_hash="a" * 40,
        title="Movie.2024.1080p.WEB-DL.x264-GROUP",
    )
    assert result.is_season_pack is False


def test_zilean_result_full_construction() -> None:
    """ZileanResult can be constructed with all fields explicitly."""
    result = ZileanResult(
        info_hash="a" * 40,
        title="Movie.2024.1080p.WEB-DL.x264-GROUP",
        resolution="1080p",
        codec="x264",
        quality="WEB-DL",
        release_group="GROUP",
        size_bytes=4_500_000_000,
        seeders=None,
        source_tracker=None,
        season=None,
        episode=None,
        is_season_pack=False,
        file_idx=None,
        languages=[],
    )
    assert result.info_hash == "a" * 40
    assert result.resolution == "1080p"
    assert result.size_bytes == 4_500_000_000
    assert result.seeders is None
    assert result.file_idx is None
    assert result.source_tracker is None


# ---------------------------------------------------------------------------
# Multiple results
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_multiple_results_all_parsed(client: ZileanClient) -> None:
    """A response with 3 items returns 3 ZileanResult objects, all distinct."""
    entries = [
        _make_zilean_entry(
            info_hash=f"{i:040x}",
            raw_title=f"Movie.Part.{i}.2024.1080p.WEB-DL.x264-GROUP",
        )
        for i in range(1, 4)
    ]
    _patch_client(client, [_make_response(200, entries)])

    results = await client.search("Movie Part")

    assert len(results) == 3
    hashes = {r.info_hash for r in results}
    assert len(hashes) == 3  # all distinct


@pytest.mark.asyncio
async def test_mixed_valid_invalid_results(client: ZileanClient) -> None:
    """Invalid entries (missing info_hash) are skipped; valid ones are preserved."""
    entry_missing_hash: dict[str, Any] = {
        # info_hash intentionally absent
        "raw_title": "Bad.Movie.2024.1080p.WEB-DL.x264-GROUP",
        "size": "1000000000",
        "category": "movies",
        "seasons": [],
        "episodes": [],
        "year": 2024,
        "resolution": "1080p",
        "codec": "x264",
        "quality": "WEB-DL",
        "group": "GROUP",
    }
    entry_valid_1 = _make_zilean_entry(info_hash="f" * 40, raw_title="Good.Movie.A.2024.1080p.WEB-DL.x264-GROUP")
    entry_valid_2 = _make_zilean_entry(info_hash="1" * 40, raw_title="Good.Movie.B.2024.1080p.WEB-DL.x264-GROUP")

    _patch_client(
        client,
        [_make_response(200, [entry_missing_hash, entry_valid_1, entry_valid_2])],
    )

    results = await client.search("Good Movie")

    assert len(results) == 2
    result_hashes = {r.info_hash for r in results}
    assert "f" * 40 in result_hashes
    assert "1" * 40 in result_hashes


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unicode_title_parsed_without_crash(client: ZileanClient) -> None:
    """Titles with non-ASCII characters are handled without crashing."""
    entry = _make_zilean_entry(
        raw_title="\u30b6\u30fb\u30dc\u30fc\u30a4\u30ba.2024.1080p.WEB-DL.x264-GROUP"
    )
    _patch_client(client, [_make_response(200, [entry])])

    results = await client.search("Japanese Movie")

    # Must not raise; result may or may not be populated depending on PTN
    assert isinstance(results, list)


@pytest.mark.asyncio
async def test_search_without_optional_params(client: ZileanClient) -> None:
    """search works with only a query string and no season/episode/year/imdb_id."""
    entry = _make_zilean_entry()
    transport = _patch_client(client, [_make_response(200, [entry])])

    results = await client.search("Movie")

    assert len(transport.requests_made) == 1
    assert len(results) == 1


@pytest.mark.asyncio
async def test_null_info_hash_entry_skipped(client: ZileanClient) -> None:
    """Entries with info_hash explicitly set to null/None are skipped."""
    entry: dict[str, Any] = {
        "info_hash": None,
        "raw_title": "Movie.2024.1080p.WEB-DL.x264-GROUP",
        "size": "4000000000",
        "category": "movies",
        "seasons": [],
        "episodes": [],
        "year": 2024,
        "resolution": "1080p",
        "codec": "x264",
        "quality": "WEB-DL",
        "group": "GROUP",
    }
    _patch_client(client, [_make_response(200, [entry])])

    results = await client.search("Movie")

    assert results == []


@pytest.mark.asyncio
async def test_http_503_returns_empty_list(client: ZileanClient) -> None:
    """An HTTP 503 Service Unavailable response returns [] without raising."""
    _patch_client(client, [_make_response(503, {"error": "Service Unavailable"})])

    results = await client.search("Movie")

    assert results == []


@pytest.mark.asyncio
async def test_empty_info_hash_string_skipped(client: ZileanClient) -> None:
    """Entries with an empty string info_hash are skipped."""
    entry = _make_zilean_entry(info_hash="")
    _patch_client(client, [_make_response(200, [entry])])

    results = await client.search("Movie")

    assert results == []


@pytest.mark.asyncio
async def test_season_none_episode_none_not_season_pack(client: ZileanClient) -> None:
    """A movie entry (empty seasons and episodes) is not flagged as a season pack."""
    entry = _make_zilean_entry(
        raw_title="Movie.2024.1080p.WEB-DL.x264-GROUP",
        seasons=[],
        episodes=[],
    )
    _patch_client(client, [_make_response(200, [entry])])

    results = await client.search("Movie")

    assert len(results) == 1
    assert results[0].is_season_pack is False


@pytest.mark.asyncio
async def test_parse_result_zero_size(client: ZileanClient) -> None:
    """An entry with size="0" is parsed without crashing; size_bytes is None."""
    entry = _make_zilean_entry(size="0")
    _patch_client(client, [_make_response(200, [entry])])

    results = await client.search("Movie")

    # Should not crash
    assert len(results) == 1
    assert results[0].size_bytes is None


@pytest.mark.asyncio
async def test_large_result_set_all_returned(client: ZileanClient) -> None:
    """All results from a large batch (50 items) are returned without truncation."""
    entries = [
        _make_zilean_entry(info_hash=f"{i:040x}")
        for i in range(50)
    ]
    _patch_client(client, [_make_response(200, entries)])

    results = await client.search("Popular Movie")

    assert len(results) == 50


@pytest.mark.asyncio
async def test_http_request_error_returns_empty_list(client: ZileanClient) -> None:
    """A generic httpx.RequestError returns [] without raising."""

    class _RequestErrorTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            raise httpx.RequestError("generic request error", request=request)

    def _fake_build_client() -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=_RequestErrorTransport())

    client._build_client = _fake_build_client  # type: ignore[method-assign]

    results = await client.search("Movie")

    assert results == []


@pytest.mark.asyncio
async def test_response_is_json_object_not_array(client: ZileanClient) -> None:
    """If the response is a JSON object (not an array), returns [] gracefully."""
    _patch_client(client, [_make_response(200, {"error": "unexpected format"})])

    results = await client.search("Movie")

    assert results == []


@pytest.mark.asyncio
async def test_response_is_null_returns_empty(client: ZileanClient) -> None:
    """If the response body is JSON null, returns [] without crashing."""
    _patch_client(
        client,
        [_make_response(200, b"null", content_type="application/json")],
    )

    results = await client.search("Movie")

    assert results == []
