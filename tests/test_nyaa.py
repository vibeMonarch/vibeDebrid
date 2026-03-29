"""Tests for src/services/nyaa.py and the _step_nyaa integration in ScrapePipeline.

All external HTTP calls are intercepted by a custom _MockTransport so no real
network traffic is generated.  The mocking pattern mirrors test_zilean.py exactly:

- monkeypatch patches ``src.services.nyaa.settings`` so the mock stays alive
  during ``await`` calls.
- ``_patch_client()`` replaces ``client._get_client`` to return an AsyncClient
  backed by the mock transport.
- ``_make_response()`` builds fake ``httpx.Response`` objects.

Test groups
-----------
Group 1 — Size parsing (_parse_nyaa_size)
Group 2 — RSS XML parsing (_parse_item / NyaaClient.search with mock transport)
Group 3 — NyaaClient error handling (timeout, 5xx, 429, 403, disabled)
Group 4 — _step_nyaa pipeline integration (disabled, movie skip)
"""

from __future__ import annotations

import textwrap
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.media_item import MediaItem, MediaType, QueueState
from src.services.http_client import CircuitBreaker
from src.services.nyaa import NyaaClient, NyaaResult, _parse_nyaa_size

# ---------------------------------------------------------------------------
# Helper — noop circuit breaker
# ---------------------------------------------------------------------------


def _make_noop_breaker() -> CircuitBreaker:
    """Return a CircuitBreaker that is always CLOSED (never rejects requests)."""
    return CircuitBreaker("test", failure_threshold=999, recovery_timeout=0.0)


# ---------------------------------------------------------------------------
# Helper — build fake httpx.Response
# ---------------------------------------------------------------------------


def _make_response(
    status_code: int,
    body: Any = None,
    *,
    content_type: str = "application/xml",
) -> httpx.Response:
    """Build a fake httpx.Response for use with _MockTransport.

    Args:
        status_code: HTTP status code.
        body:        Response body.  ``bytes`` passed as-is; ``str`` encoded as
                     UTF-8; ``dict``/``list`` JSON-serialised (for error bodies).
        content_type: Content-type header value.

    Returns:
        A pre-built ``httpx.Response`` with no live connection.
    """
    import json as _json

    if body is None:
        raw = b""
    elif isinstance(body, bytes):
        raw = body
    elif isinstance(body, (dict, list)):
        raw = _json.dumps(body).encode()
    else:
        raw = str(body).encode("utf-8")

    return httpx.Response(
        status_code=status_code,
        headers={"content-type": content_type},
        content=raw,
        request=httpx.Request("GET", "https://nyaa.si/"),
    )


# ---------------------------------------------------------------------------
# Mock transport
# ---------------------------------------------------------------------------


class _MockTransport(httpx.AsyncBaseTransport):
    """Sequential-queue mock transport.

    Each call to ``handle_async_request`` returns the next response from
    ``responses``.  All requests are recorded in ``requests_made`` for
    URL-level assertions.
    """

    def __init__(self, responses: list[httpx.Response]) -> None:
        self._queue = list(responses)
        self._index = 0
        self.requests_made: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests_made.append(request)
        if self._index >= len(self._queue):
            raise RuntimeError(
                f"_MockTransport: no more responses (got {len(self._queue)}, "
                f"called {self._index + 1} times) for URL {request.url!s}"
            )
        resp = self._queue[self._index]
        self._index += 1
        resp.request = request  # type: ignore[attr-defined]
        return resp


# ---------------------------------------------------------------------------
# Helper — patch _get_client on a NyaaClient instance
# ---------------------------------------------------------------------------


def _patch_client(
    client: NyaaClient,
    responses: list[httpx.Response],
) -> _MockTransport:
    """Replace ``client._get_client`` with a coroutine backed by _MockTransport.

    Args:
        client:    The NyaaClient instance under test.
        responses: Ordered list of responses to return sequentially.

    Returns:
        The _MockTransport so tests can inspect ``transport.requests_made``.
    """
    transport = _MockTransport(responses)

    async def _fake_get_client() -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url="https://nyaa.si",
            transport=transport,
        )

    client._get_client = _fake_get_client  # type: ignore[method-assign]
    return transport


# ---------------------------------------------------------------------------
# Helper — build a mock NyaaConfig
# ---------------------------------------------------------------------------


def _make_mock_cfg(
    *,
    enabled: bool = True,
    base_url: str = "https://nyaa.si",
    category: str = "1_2",
    filter_level: int = 1,
    timeout_seconds: int = 15,
    max_results: int = 75,
) -> MagicMock:
    """Return a MagicMock that looks like a NyaaConfig object."""
    cfg = MagicMock()
    cfg.enabled = enabled
    cfg.base_url = base_url
    cfg.category = category
    cfg.filter = filter_level
    cfg.timeout_seconds = timeout_seconds
    cfg.max_results = max_results
    return cfg


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch) -> NyaaClient:
    """A NyaaClient configured with test defaults (no real HTTP)."""
    cfg = _make_mock_cfg()
    mock_settings = MagicMock()
    mock_settings.scrapers.nyaa = cfg
    monkeypatch.setattr("src.services.nyaa.settings", mock_settings)
    monkeypatch.setattr(
        "src.services.nyaa.get_circuit_breaker",
        lambda *args, **kwargs: _make_noop_breaker(),
    )
    c = NyaaClient()
    c._test_cfg = cfg  # type: ignore[attr-defined]
    return c


@pytest.fixture()
def disabled_client(monkeypatch: pytest.MonkeyPatch) -> NyaaClient:
    """A NyaaClient where enabled=False — should short-circuit all requests."""
    cfg = _make_mock_cfg(enabled=False)
    mock_settings = MagicMock()
    mock_settings.scrapers.nyaa = cfg
    monkeypatch.setattr("src.services.nyaa.settings", mock_settings)
    monkeypatch.setattr(
        "src.services.nyaa.get_circuit_breaker",
        lambda *args, **kwargs: _make_noop_breaker(),
    )
    return NyaaClient()


# ---------------------------------------------------------------------------
# RSS XML builder helpers
# ---------------------------------------------------------------------------

_NYAA_NS = "https://nyaa.si/xmlns/nyaa"


def _rss_wrap(items_xml: str) -> str:
    """Wrap item XML fragments inside a complete Nyaa RSS document.

    The XML declaration must be at column 0 (no leading whitespace) or
    xml.etree.ElementTree raises ParseError.
    """
    return (
        '<?xml version="1.0" encoding="utf-8"?>\n'
        f'<rss xmlns:atom="http://www.w3.org/2005/Atom"'
        f' xmlns:nyaa="{_NYAA_NS}" version="2.0">\n'
        "<channel>\n"
        f"{items_xml}\n"
        "</channel>\n"
        "</rss>\n"
    )


def _make_rss_item(
    *,
    title: str = "[SubsPlease] Test Anime - 05 (1080p) [ABCD1234].mkv",
    link: str = "https://nyaa.si/download/1234567.torrent",
    info_hash: str = "a" * 40,
    seeders: str = "150",
    leechers: str = "10",
    size: str = "1.4 GiB",
    category_id: str = "1_2",
    trusted: str = "Yes",
    remake: str = "No",
) -> str:
    """Build a single Nyaa RSS <item> XML string."""
    return textwrap.dedent(f"""\
        <item>
          <title>{title}</title>
          <link>{link}</link>
          <nyaa:infoHash xmlns:nyaa="{_NYAA_NS}">{info_hash}</nyaa:infoHash>
          <nyaa:seeders xmlns:nyaa="{_NYAA_NS}">{seeders}</nyaa:seeders>
          <nyaa:leechers xmlns:nyaa="{_NYAA_NS}">{leechers}</nyaa:leechers>
          <nyaa:size xmlns:nyaa="{_NYAA_NS}">{size}</nyaa:size>
          <nyaa:categoryId xmlns:nyaa="{_NYAA_NS}">{category_id}</nyaa:categoryId>
          <nyaa:trusted xmlns:nyaa="{_NYAA_NS}">{trusted}</nyaa:trusted>
          <nyaa:remake xmlns:nyaa="{_NYAA_NS}">{remake}</nyaa:remake>
        </item>
    """)


# ===========================================================================
# Group 1: Size parsing — _parse_nyaa_size
# ===========================================================================


class TestParseSizes:
    """Unit tests for the _parse_nyaa_size helper function."""

    def test_parse_size_gib(self) -> None:
        """'1.4 GiB' → 1503238553 bytes (1.4 * 1024^3)."""
        result = _parse_nyaa_size("1.4 GiB")
        assert result == int(1.4 * 1024**3)

    def test_parse_size_mib(self) -> None:
        """'936.4 MiB' → int(936.4 * 1024^2) bytes."""
        result = _parse_nyaa_size("936.4 MiB")
        assert result == int(936.4 * 1024**2)

    def test_parse_size_tib(self) -> None:
        """'2.3 TiB' → approximately 2528609173913 bytes (2.3 * 1024^4)."""
        result = _parse_nyaa_size("2.3 TiB")
        assert result == int(2.3 * 1024**4)

    def test_parse_size_kib(self) -> None:
        """'500 KiB' → 512000 bytes (500 * 1024)."""
        result = _parse_nyaa_size("500 KiB")
        assert result == 512000

    def test_parse_size_gb(self) -> None:
        """'1.5 GB' → 1500000000 bytes (SI decimal: 1.5 * 1000^3)."""
        result = _parse_nyaa_size("1.5 GB")
        assert result == int(1.5 * 1000**3)

    def test_parse_size_mb(self) -> None:
        """'250 MB' → 250000000 bytes (SI decimal: 250 * 1000^2)."""
        result = _parse_nyaa_size("250 MB")
        assert result == 250 * 1000**2

    def test_parse_size_invalid(self) -> None:
        """Unrecognised string 'unknown' → None."""
        result = _parse_nyaa_size("unknown")
        assert result is None

    def test_parse_size_empty(self) -> None:
        """Empty string '' → None."""
        result = _parse_nyaa_size("")
        assert result is None

    def test_parse_size_case_insensitive(self) -> None:
        """'1.0 gib' (lowercase) should also parse correctly."""
        result = _parse_nyaa_size("1.0 gib")
        assert result == 1024**3

    def test_parse_size_no_space(self) -> None:
        """'500KiB' (no space before unit) → 512000 bytes."""
        result = _parse_nyaa_size("500KiB")
        assert result == 512000


# ===========================================================================
# Group 2: RSS XML item parsing via NyaaClient._parse_item
# ===========================================================================


class TestRssItemParsing:
    """Tests for NyaaClient._parse_item and _parse_item_inner logic."""

    def setup_method(self) -> None:
        """Create a NyaaClient for item-parsing tests (no network needed)."""
        self.client = NyaaClient()

    def _parse_xml_item(self, item_xml: str) -> NyaaResult | None:
        """Parse a single <item> XML string using the client's parser."""
        import xml.etree.ElementTree as ET

        full = _rss_wrap(item_xml)
        root = ET.fromstring(full)
        channel = root.find("channel")
        assert channel is not None
        items = channel.findall("item")
        assert len(items) == 1
        return self.client._parse_item(items[0])

    def test_parse_rss_valid_fields(self) -> None:
        """A well-formed item returns a NyaaResult with all expected fields."""
        item_xml = _make_rss_item(
            title="[SubsPlease] Test Anime - 05 (1080p) [ABCD1234].mkv",
            info_hash="a" * 40,
            seeders="150",
            size="1.4 GiB",
        )
        result = self._parse_xml_item(item_xml)

        assert result is not None
        assert result.info_hash == "a" * 40
        assert result.title == "[SubsPlease] Test Anime - 05 (1080p) [ABCD1234].mkv"
        assert result.seeders == 150
        assert result.size_bytes == int(1.4 * 1024**3)

    def test_parse_item_missing_hash_skipped(self) -> None:
        """Item without <nyaa:infoHash> → _parse_item returns None."""
        import xml.etree.ElementTree as ET

        item_xml = textwrap.dedent(f"""\
            <item>
              <title>[SubsPlease] Test Anime - 05 (1080p)</title>
              <nyaa:seeders xmlns:nyaa="{_NYAA_NS}">50</nyaa:seeders>
            </item>
        """)
        full = _rss_wrap(item_xml)
        root = ET.fromstring(full)
        channel = root.find("channel")
        assert channel is not None
        items = channel.findall("item")
        result = self.client._parse_item(items[0])
        assert result is None

    def test_parse_item_missing_title_skipped(self) -> None:
        """Item with empty <title> → _parse_item returns None."""
        import xml.etree.ElementTree as ET

        item_xml = textwrap.dedent(f"""\
            <item>
              <title></title>
              <nyaa:infoHash xmlns:nyaa="{_NYAA_NS}">{"b" * 40}</nyaa:infoHash>
            </item>
        """)
        full = _rss_wrap(item_xml)
        root = ET.fromstring(full)
        channel = root.find("channel")
        assert channel is not None
        items = channel.findall("item")
        result = self.client._parse_item(items[0])
        assert result is None

    def test_parse_item_extracts_resolution(self) -> None:
        """'[Group] Test Anime - 05 (1080p)' → resolution='1080p'."""
        item_xml = _make_rss_item(
            title="[GroupName] Test Anime - 05 (1080p) [HASH1234].mkv",
            info_hash="c" * 40,
        )
        result = self._parse_xml_item(item_xml)

        assert result is not None
        assert result.resolution == "1080p"

    def test_parse_item_detects_season_pack(self) -> None:
        """'[Group] Sample Series S02 Batch [1080p]' → is_season_pack=True."""
        item_xml = _make_rss_item(
            title="[GroupName] Sample Series S02 Batch [1080p] [HASH1234]",
            info_hash="d" * 40,
        )
        result = self._parse_xml_item(item_xml)

        assert result is not None
        assert result.is_season_pack is True

    def test_parse_item_bare_episode(self) -> None:
        """'[Group] Test Show - 05 [1080p]' → episode=5 (via bare dash fallback)."""
        item_xml = _make_rss_item(
            title="[SubsGroup] Test Show - 05 [1080p] [ABCDEF12].mkv",
            info_hash="e" * 40,
        )
        result = self._parse_xml_item(item_xml)

        assert result is not None
        assert result.episode == 5

    def test_source_tracker_always_nyaa(self) -> None:
        """Every successfully parsed result has source_tracker='Nyaa'."""
        for i in range(3):
            item_xml = _make_rss_item(
                title=f"[SubsPlease] Test Anime - 0{i + 1} (720p) [XYZ{i:04d}].mkv",
                info_hash=hex(0xABCDEF + i)[2:].zfill(40),
            )
            result = self._parse_xml_item(item_xml)
            assert result is not None
            assert result.source_tracker == "Nyaa"

    def test_parse_item_info_hash_lowercased(self) -> None:
        """info_hash is stored in lower case regardless of source casing."""
        item_xml = _make_rss_item(
            title="[Group] Test Anime - 01 [720p]",
            info_hash="ABCDEF1234" * 4,  # 40 uppercase hex chars
        )
        result = self._parse_xml_item(item_xml)

        assert result is not None
        assert result.info_hash == ("abcdef1234" * 4)

    def test_parse_item_zero_seeders(self) -> None:
        """Item with 0 seeders → seeders=0, not None."""
        item_xml = _make_rss_item(
            title="[SubsGroup] Test Show - 12 [480p]",
            info_hash="f" * 40,
            seeders="0",
        )
        result = self._parse_xml_item(item_xml)

        assert result is not None
        assert result.seeders == 0

    def test_parse_item_bad_seeders_ignored(self) -> None:
        """Non-numeric seeders value → seeders=None, item still returned."""
        item_xml = _make_rss_item(
            title="[SubsGroup] Test Show - 12 [480p]",
            info_hash="1" * 40,
            seeders="N/A",
        )
        result = self._parse_xml_item(item_xml)

        assert result is not None
        assert result.seeders is None

    def test_parse_item_standard_sxxexx(self) -> None:
        """'[Group] Test Show S01E07 [1080p]' → season=1, episode=7."""
        item_xml = _make_rss_item(
            title="[SubsGroup] Test Show S01E07 [1080p] [HASH0001].mkv",
            info_hash="2" * 40,
        )
        result = self._parse_xml_item(item_xml)

        assert result is not None
        assert result.season == 1
        assert result.episode == 7
        assert result.is_season_pack is False

    def test_parse_item_file_idx_always_none(self) -> None:
        """file_idx is always None for Nyaa results (no file-level index)."""
        item_xml = _make_rss_item(
            title="[Group] Test Anime - 03 (720p) [HASH0002].mkv",
            info_hash="3" * 40,
        )
        result = self._parse_xml_item(item_xml)

        assert result is not None
        assert result.file_idx is None


# ===========================================================================
# Group 3: Full RSS document parsing via NyaaClient.search (with mock HTTP)
# ===========================================================================


class TestRssDocumentParsing:
    """Tests for full RSS document parsing through NyaaClient.search."""

    @pytest.mark.asyncio
    async def test_parse_rss_valid_two_items(self, client: NyaaClient) -> None:
        """RSS with 2 valid items → search returns 2 NyaaResult objects."""
        rss = _rss_wrap(
            _make_rss_item(
                title="[SubsPlease] Test Anime - 01 (1080p) [HASH0001].mkv",
                info_hash="a" * 40,
            )
            + _make_rss_item(
                title="[SubsPlease] Test Anime - 02 (1080p) [HASH0002].mkv",
                info_hash="b" * 40,
            )
        )
        _patch_client(client, [_make_response(200, rss.encode())])

        results = await client.search("Test Anime")

        assert len(results) == 2
        assert all(isinstance(r, NyaaResult) for r in results)
        assert results[0].info_hash == "a" * 40
        assert results[1].info_hash == "b" * 40

    @pytest.mark.asyncio
    async def test_parse_rss_empty(self, client: NyaaClient) -> None:
        """RSS with 0 items → search returns an empty list."""
        rss = _rss_wrap("")
        _patch_client(client, [_make_response(200, rss.encode())])

        results = await client.search("Test Anime")

        assert results == []

    @pytest.mark.asyncio
    async def test_parse_rss_skips_missing_hash(self, client: NyaaClient) -> None:
        """Item without <nyaa:infoHash> is skipped; other items are returned."""
        no_hash_item = textwrap.dedent(f"""\
            <item>
              <title>[SubsPlease] Test Anime - 03 (1080p) [MISS0001].mkv</title>
              <nyaa:seeders xmlns:nyaa="{_NYAA_NS}">50</nyaa:seeders>
            </item>
        """)
        good_item = _make_rss_item(
            title="[SubsPlease] Test Anime - 04 (1080p) [GOOD0001].mkv",
            info_hash="c" * 40,
        )
        rss = _rss_wrap(no_hash_item + good_item)
        _patch_client(client, [_make_response(200, rss.encode())])

        results = await client.search("Test Anime")

        assert len(results) == 1
        assert results[0].info_hash == "c" * 40

    @pytest.mark.asyncio
    async def test_parse_rss_skips_empty_title(self, client: NyaaClient) -> None:
        """Item with empty <title> is skipped; other items are returned."""
        empty_title_item = textwrap.dedent(f"""\
            <item>
              <title></title>
              <nyaa:infoHash xmlns:nyaa="{_NYAA_NS}">{"d" * 40}</nyaa:infoHash>
            </item>
        """)
        good_item = _make_rss_item(
            title="[SubsPlease] Test Anime - 05 (720p) [GOOD0002].mkv",
            info_hash="e" * 40,
        )
        rss = _rss_wrap(empty_title_item + good_item)
        _patch_client(client, [_make_response(200, rss.encode())])

        results = await client.search("Test Anime")

        assert len(results) == 1
        assert results[0].info_hash == "e" * 40

    @pytest.mark.asyncio
    async def test_parse_rss_malformed_xml(self, client: NyaaClient) -> None:
        """Malformed XML response → returns empty list, does not crash."""
        _patch_client(client, [_make_response(200, b"not xml at all <!!>")])

        results = await client.search("Test Anime")

        assert results == []


# ===========================================================================
# Group 4: NyaaClient network / HTTP error handling
# ===========================================================================


class TestClientErrorHandling:
    """Tests for NyaaClient.search error handling paths."""

    @pytest.mark.asyncio
    async def test_search_disabled(self, disabled_client: NyaaClient) -> None:
        """nyaa.enabled=False → search returns [] without making any HTTP call."""
        transport = _patch_client(disabled_client, [])

        results = await disabled_client.search("Test Anime")

        assert results == []
        assert len(transport.requests_made) == 0

    @pytest.mark.asyncio
    async def test_search_success(self, client: NyaaClient) -> None:
        """200 response with valid RSS → returns parsed NyaaResult objects."""
        rss = _rss_wrap(
            _make_rss_item(
                title="[SubsPlease] Test Anime - 07 (1080p) [HASH0007].mkv",
                info_hash="f" * 40,
            )
        )
        _patch_client(client, [_make_response(200, rss.encode())])

        results = await client.search("Test Anime")

        assert len(results) == 1
        assert isinstance(results[0], NyaaResult)

    @pytest.mark.asyncio
    async def test_search_timeout(self, client: NyaaClient) -> None:
        """TimeoutException → search returns [] without raising."""

        class _TimeoutTransport(httpx.AsyncBaseTransport):
            async def handle_async_request(
                self, request: httpx.Request
            ) -> httpx.Response:
                raise httpx.TimeoutException("timed out", request=request)

        async def _fake_get_client() -> httpx.AsyncClient:
            return httpx.AsyncClient(transport=_TimeoutTransport())

        client._get_client = _fake_get_client  # type: ignore[method-assign]

        results = await client.search("Test Anime")

        assert results == []

    @pytest.mark.asyncio
    async def test_search_connect_error(self, client: NyaaClient) -> None:
        """ConnectError (server down) → search returns [] without raising."""

        class _ConnErrorTransport(httpx.AsyncBaseTransport):
            async def handle_async_request(
                self, request: httpx.Request
            ) -> httpx.Response:
                raise httpx.ConnectError("connection refused", request=request)

        async def _fake_get_client() -> httpx.AsyncClient:
            return httpx.AsyncClient(transport=_ConnErrorTransport())

        client._get_client = _fake_get_client  # type: ignore[method-assign]

        results = await client.search("Test Anime")

        assert results == []

    @pytest.mark.asyncio
    async def test_search_server_error_500(self, client: NyaaClient) -> None:
        """500 response → search returns [] without raising."""
        _patch_client(
            client,
            [_make_response(500, b"Internal Server Error", content_type="text/plain")],
        )

        results = await client.search("Test Anime")

        assert results == []

    @pytest.mark.asyncio
    async def test_search_rate_limit_429(self, client: NyaaClient) -> None:
        """429 response → search returns [] without circuit-breaker penalty."""
        # The circuit breaker is a noop in these tests, but we verify the
        # method returns [] gracefully regardless.
        _patch_client(client, [_make_response(429, b"Too Many Requests")])

        results = await client.search("Test Anime")

        assert results == []

    @pytest.mark.asyncio
    async def test_search_cloudflare_403(self, client: NyaaClient) -> None:
        """403 response (CloudFlare block) → search returns [] without raising."""
        _patch_client(
            client,
            [_make_response(403, b"Forbidden", content_type="text/html")],
        )

        results = await client.search("Test Anime")

        assert results == []

    @pytest.mark.asyncio
    async def test_search_max_results_cap(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Results are capped at cfg.max_results (e.g. 3 out of 5 items)."""
        cfg = _make_mock_cfg(max_results=3)
        mock_settings = MagicMock()
        mock_settings.scrapers.nyaa = cfg
        monkeypatch.setattr("src.services.nyaa.settings", mock_settings)
        monkeypatch.setattr(
            "src.services.nyaa.get_circuit_breaker",
            lambda *args, **kwargs: _make_noop_breaker(),
        )
        capped_client = NyaaClient()

        # Build RSS with 5 valid items
        items_xml = "".join(
            _make_rss_item(
                title=f"[SubsPlease] Test Anime - 0{i} (1080p) [HASH{i:04d}].mkv",
                info_hash=hex(0xAAAAAAAA + i)[2:].zfill(40),
            )
            for i in range(1, 6)
        )
        rss = _rss_wrap(items_xml)
        _patch_client(capped_client, [_make_response(200, rss.encode())])

        results = await capped_client.search("Test Anime")

        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_search_sends_correct_params(self, client: NyaaClient) -> None:
        """search sends page=rss, q=<query>, s=seeders, o=desc query params."""
        rss = _rss_wrap("")
        transport = _patch_client(client, [_make_response(200, rss.encode())])

        await client.search("Test Show Query")

        assert len(transport.requests_made) == 1
        url = str(transport.requests_made[0].url)
        assert "page=rss" in url
        assert "q=Test+Show+Query" in url or "q=Test%20Show%20Query" in url or "Test+Show+Query" in url
        assert "s=seeders" in url
        assert "o=desc" in url

    @pytest.mark.asyncio
    async def test_search_custom_category(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Explicit category parameter overrides the config default."""
        cfg = _make_mock_cfg(category="1_0")
        mock_settings = MagicMock()
        mock_settings.scrapers.nyaa = cfg
        monkeypatch.setattr("src.services.nyaa.settings", mock_settings)
        monkeypatch.setattr(
            "src.services.nyaa.get_circuit_breaker",
            lambda *args, **kwargs: _make_noop_breaker(),
        )
        c = NyaaClient()

        rss = _rss_wrap("")
        transport = _patch_client(c, [_make_response(200, rss.encode())])

        await c.search("Test Anime", category="3_5")

        url = str(transport.requests_made[0].url)
        assert "c=3_5" in url

    @pytest.mark.asyncio
    async def test_search_circuit_open_returns_empty(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When the circuit breaker is open, search returns [] immediately."""
        from src.services.http_client import CircuitBreaker

        # A breaker with failure_threshold=1; record one failure to open it.
        open_breaker = CircuitBreaker("test-open", failure_threshold=1, recovery_timeout=9999)
        await open_breaker.record_failure()  # now OPEN

        cfg = _make_mock_cfg()
        mock_settings = MagicMock()
        mock_settings.scrapers.nyaa = cfg
        monkeypatch.setattr("src.services.nyaa.settings", mock_settings)
        monkeypatch.setattr(
            "src.services.nyaa.get_circuit_breaker",
            lambda *args, **kwargs: open_breaker,
        )
        c = NyaaClient()
        transport = _patch_client(c, [])

        results = await c.search("Test Anime")

        assert results == []
        assert len(transport.requests_made) == 0


# ===========================================================================
# Group 5: _step_nyaa pipeline integration
# ===========================================================================


def _make_show_item(
    *,
    title: str = "Test Anime",
    season: int | None = 1,
    episode: int | None = 5,
    is_season_pack: bool = False,
) -> MediaItem:
    """Build an unsaved MediaItem of type SHOW for pipeline step tests."""
    item = MediaItem(
        id=1,
        imdb_id="tt0000001",
        title=title,
        year=2024,
        media_type=MediaType.SHOW,
        state=QueueState.WANTED,
        state_changed_at=datetime.now(UTC),
        retry_count=0,
        season=season,
        episode=episode,
        is_season_pack=is_season_pack,
    )
    return item


def _make_movie_item(*, title: str = "Test Movie") -> MediaItem:
    """Build an unsaved MediaItem of type MOVIE for pipeline step tests."""
    item = MediaItem(
        id=2,
        imdb_id="tt0000002",
        title=title,
        year=2024,
        media_type=MediaType.MOVIE,
        state=QueueState.WANTED,
        state_changed_at=datetime.now(UTC),
        retry_count=0,
    )
    return item


class TestStepNyaa:
    """Unit tests for ScrapePipeline._step_nyaa method directly."""

    def _import_pipeline(self):
        """Import ScrapePipeline (deferred to keep collection fast)."""
        from src.core.scrape_pipeline import ScrapePipeline  # noqa: PLC0415
        return ScrapePipeline

    @pytest.mark.asyncio
    async def test_step_nyaa_disabled_skipped(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """_step_nyaa returns ([], 0) when nyaa.enabled=False."""
        ScrapePipeline = self._import_pipeline()
        mock_settings = MagicMock()
        mock_settings.scrapers.nyaa.enabled = False
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        pipeline = ScrapePipeline()
        item = _make_show_item()

        results, duration_ms = await pipeline._step_nyaa(session, item)

        assert results == []
        assert duration_ms == 0

    @pytest.mark.asyncio
    async def test_step_nyaa_movie_skipped(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """_step_nyaa returns ([], 0) for MOVIE items (Nyaa is show-only)."""
        ScrapePipeline = self._import_pipeline()
        mock_settings = MagicMock()
        mock_settings.scrapers.nyaa.enabled = True
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        pipeline = ScrapePipeline()
        item = _make_movie_item()

        results, duration_ms = await pipeline._step_nyaa(session, item)

        assert results == []
        assert duration_ms == 0

    @pytest.mark.asyncio
    async def test_step_nyaa_returns_results(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """_step_nyaa returns NyaaResult list when nyaa_client.search succeeds."""
        ScrapePipeline = self._import_pipeline()
        mock_settings = MagicMock()
        mock_settings.scrapers.nyaa.enabled = True
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        fake_result = NyaaResult(
            info_hash="a" * 40,
            title="[SubsPlease] Test Anime - 05 (1080p) [HASH0005].mkv",
            resolution="1080p",
            seeders=100,
            source_tracker="Nyaa",
            season=1,
            episode=5,
        )

        pipeline = ScrapePipeline()
        item = _make_show_item(season=1, episode=5)

        # Patch nyaa_client.search inside the scrape_pipeline module
        with patch(
            "src.core.scrape_pipeline.nyaa_client.search",
            new_callable=AsyncMock,
            return_value=[fake_result],
        ):
            # Also patch _log_scrape so we don't need a real DB
            pipeline._log_scrape = AsyncMock()  # type: ignore[method-assign]
            results, duration_ms = await pipeline._step_nyaa(session, item)

        assert len(results) == 1
        assert results[0].info_hash == "a" * 40
        assert results[0].source_tracker == "Nyaa"
        assert duration_ms >= 0

    @pytest.mark.asyncio
    async def test_step_nyaa_search_exception_returns_empty(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """_step_nyaa returns ([], duration) when nyaa_client.search raises."""
        ScrapePipeline = self._import_pipeline()
        mock_settings = MagicMock()
        mock_settings.scrapers.nyaa.enabled = True
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        pipeline = ScrapePipeline()
        item = _make_show_item(season=1, episode=5)

        with patch(
            "src.core.scrape_pipeline.nyaa_client.search",
            new_callable=AsyncMock,
            side_effect=RuntimeError("unexpected error"),
        ):
            pipeline._log_scrape = AsyncMock()  # type: ignore[method-assign]
            results, duration_ms = await pipeline._step_nyaa(session, item)

        # The pipeline should survive the error and return empty
        assert results == []
        assert duration_ms >= 0

    @pytest.mark.asyncio
    async def test_step_nyaa_season_pack_queries_batch(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Season pack items use 'S{season} batch' as the first query."""
        ScrapePipeline = self._import_pipeline()
        mock_settings = MagicMock()
        mock_settings.scrapers.nyaa.enabled = True
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        pipeline = ScrapePipeline()
        item = _make_show_item(title="Sample Series", season=2, episode=None, is_season_pack=True)

        queries_made: list[str] = []

        async def _fake_search(query: str, **kwargs: object) -> list[NyaaResult]:
            queries_made.append(query)
            return []

        with patch("src.core.scrape_pipeline.nyaa_client.search", side_effect=_fake_search):
            pipeline._log_scrape = AsyncMock()  # type: ignore[method-assign]
            await pipeline._step_nyaa(session, item)

        # The first query for a season pack with known season must include batch
        assert any("batch" in q.lower() for q in queries_made), (
            f"Expected a 'batch' query; got: {queries_made}"
        )

    @pytest.mark.asyncio
    async def test_step_nyaa_episode_queries_sxxexx_first(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Episode items try 'S{s:02d}E{e:02d}' notation as the first query."""
        ScrapePipeline = self._import_pipeline()
        mock_settings = MagicMock()
        mock_settings.scrapers.nyaa.enabled = True
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        pipeline = ScrapePipeline()
        item = _make_show_item(title="Sample Series", season=1, episode=7)

        queries_made: list[str] = []

        async def _fake_search(query: str, **kwargs: object) -> list[NyaaResult]:
            queries_made.append(query)
            return []

        with patch("src.core.scrape_pipeline.nyaa_client.search", side_effect=_fake_search):
            pipeline._log_scrape = AsyncMock()  # type: ignore[method-assign]
            await pipeline._step_nyaa(session, item)

        assert any("S01E07" in q for q in queries_made), (
            f"Expected SxxExx query; got: {queries_made}"
        )

    @pytest.mark.asyncio
    async def test_step_nyaa_alt_title_fallback(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When primary title returns nothing, the first alt_title is tried."""
        ScrapePipeline = self._import_pipeline()
        mock_settings = MagicMock()
        mock_settings.scrapers.nyaa.enabled = True
        monkeypatch.setattr("src.core.scrape_pipeline.settings", mock_settings)

        pipeline = ScrapePipeline()
        item = _make_show_item(title="Sample Series", season=1, episode=3)

        alt_result = NyaaResult(
            info_hash="b" * 40,
            title="[SubsPlease] Alt Title - 03 (1080p) [HASH0003].mkv",
            source_tracker="Nyaa",
        )

        call_count = [0]

        async def _fake_search(query: str, **kwargs: object) -> list[NyaaResult]:
            call_count[0] += 1
            # Primary title returns nothing, alt title returns a result
            if "Alt Title" in query:
                return [alt_result]
            return []

        with patch("src.core.scrape_pipeline.nyaa_client.search", side_effect=_fake_search):
            pipeline._log_scrape = AsyncMock()  # type: ignore[method-assign]
            results, _ = await pipeline._step_nyaa(
                session, item, alt_titles=["Alt Title"]
            )

        assert len(results) == 1
        assert results[0].info_hash == "b" * 40
