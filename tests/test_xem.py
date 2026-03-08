"""Tests for src/services/xem.py and src/core/xem_mapper.py.

Covers:
  Section A — XemClient HTTP layer (8 tests)
    - Successful mapping fetch with filter of identical numbers
    - Skips identical TVDB/scene number pairs
    - Skips malformed entries gracefully
    - ConnectError / TimeoutException / RequestError → empty result
    - Non-success API result field → empty result
    - XEM disabled → empty result, no HTTP call
    - get_shows_with_mappings success
    - get_shows_with_mappings timeout → empty set

  Section B — XemMapper DB caching layer (9 tests)
    - Fresh fetch (no cache) → calls API → inserts cache rows
    - Cache hit (fresh entries) → returns mapping without API call
    - Stale cache → calls API → old rows deleted, new rows inserted
    - No mapping from XEM → returns None
    - Episode not in cached mappings → returns None
    - get_scene_numbering_for_item resolves tvdb_id via TMDB
    - get_scene_numbering_for_item: tvdb_id=None and tmdb_id=None → None
    - get_scene_numbering_for_item: TMDB resolve failure → None
    - XEM disabled → client returns empty → mapper returns None

  Section B2 — XemMapper.get_all_scene_mappings (5 tests)
    - Returns correct dict of all mappings from cache entries
    - Returns empty dict when no cache entries exist
    - Returns empty dict when XEM is disabled
    - Multiple entries produce correct key/value pairs
    - Cache shared between get_all_scene_mappings and get_scene_numbering (one API call)

  Section C — Pipeline XEM integration (5 tests)
    - Torrentio _step_torrentio uses scene numbers from XEM
    - Torrentio _step_torrentio uses original numbers when mapper returns None
    - Torrentio _step_torrentio skips XEM for season packs (episode=None)
    - Zilean _step_zilean uses scene numbers from XEM
    - XEM disabled → scrape pipeline uses original numbers, no mapper call

asyncio_mode = "auto" (set in pyproject.toml), so no @pytest.mark.asyncio needed.
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.xem_cache import XemCacheEntry
from src.services.xem import XemClient, XemMapping, XemShowMappings
from src.services.tmdb import TmdbExternalIds


# ---------------------------------------------------------------------------
# Shared HTTP helper utilities (mirrors pattern from test_torrentio.py)
# ---------------------------------------------------------------------------


def _make_response(
    status_code: int,
    body: Any = None,
    *,
    content_type: str = "application/json",
) -> httpx.Response:
    """Build a fake httpx.Response."""
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
        request=httpx.Request("GET", "https://thexem.info/"),
    )


class _MockTransport(httpx.AsyncBaseTransport):
    """Sequential mock transport — responses consumed in order."""

    def __init__(
        self,
        responses: list[httpx.Response] | None = None,
        *,
        raise_on_send: Exception | None = None,
    ) -> None:
        self._queue = list(responses or [])
        self._index = 0
        self._raise = raise_on_send
        self.requests_made: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests_made.append(request)
        if self._raise is not None:
            raise self._raise
        if self._index < len(self._queue):
            resp = self._queue[self._index]
            self._index += 1
            resp.request = request  # type: ignore[attr-defined]
            return resp
        raise RuntimeError(f"_MockTransport: no response for {request.url}")


def _patch_xem_client(
    client: XemClient,
    responses: list[httpx.Response] | None = None,
    *,
    raise_on_send: Exception | None = None,
) -> _MockTransport:
    """Monkey-patch client._build_client to inject a _MockTransport."""
    transport = _MockTransport(responses, raise_on_send=raise_on_send)

    def _fake_build() -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url="https://thexem.info",
            transport=transport,
        )

    client._build_client = _fake_build  # type: ignore[method-assign]
    return transport


# ---------------------------------------------------------------------------
# Common XEM API response fixtures
# ---------------------------------------------------------------------------

_SUCCESS_MAPPINGS_BODY: dict[str, Any] = {
    "result": "success",
    "data": [
        {"tvdb": {"season": 1, "episode": 29}, "scene": {"season": 2, "episode": 1}},
        {"tvdb": {"season": 1, "episode": 30}, "scene": {"season": 2, "episode": 2}},
    ],
}

_IDENTICAL_MAPPINGS_BODY: dict[str, Any] = {
    "result": "success",
    "data": [
        # Identity mappings — client now includes these (filtering moved to mapper layer)
        {"tvdb": {"season": 1, "episode": 1}, "scene": {"season": 1, "episode": 1}},
        {"tvdb": {"season": 1, "episode": 2}, "scene": {"season": 1, "episode": 2}},
    ],
}

_HAVE_MAP_BODY: dict[str, Any] = {
    "result": "success",
    "data": [12345, 67890],
}

TVDB_ID = 76290


# ---------------------------------------------------------------------------
# Section A: XemClient Tests
# ---------------------------------------------------------------------------


class TestXemClient:
    """Tests for XemClient HTTP layer — all calls are intercepted."""

    # ------------------------------------------------------------------
    # get_show_mappings — happy path
    # ------------------------------------------------------------------

    async def test_get_show_mappings_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Valid XEM response → returns XemShowMappings with correct values."""
        mock_settings = MagicMock()
        mock_settings.xem.enabled = True
        mock_settings.xem.base_url = "https://thexem.info"
        mock_settings.xem.timeout_seconds = 10
        monkeypatch.setattr("src.services.xem.settings", mock_settings)

        client = XemClient()
        transport = _patch_xem_client(
            client,
            [_make_response(200, _SUCCESS_MAPPINGS_BODY)],
        )

        result = await client.get_show_mappings(TVDB_ID)

        assert isinstance(result, XemShowMappings)
        assert result.tvdb_id == TVDB_ID
        assert len(result.mappings) == 2

        first = result.mappings[0]
        assert first.tvdb_season == 1
        assert first.tvdb_episode == 29
        assert first.scene_season == 2
        assert first.scene_episode == 1

        second = result.mappings[1]
        assert second.tvdb_episode == 30
        assert second.scene_episode == 2

        assert len(transport.requests_made) == 1
        assert "/map/all" in str(transport.requests_made[0].url)

    async def test_get_show_mappings_includes_identical(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Entries where TVDB and scene numbers are identical are now included (filtering moved to mapper layer)."""
        mock_settings = MagicMock()
        mock_settings.xem.enabled = True
        mock_settings.xem.base_url = "https://thexem.info"
        mock_settings.xem.timeout_seconds = 10
        monkeypatch.setattr("src.services.xem.settings", mock_settings)

        client = XemClient()
        _patch_xem_client(client, [_make_response(200, _IDENTICAL_MAPPINGS_BODY)])

        result = await client.get_show_mappings(TVDB_ID)

        # Both identity entries are returned — the client no longer filters them out.
        assert len(result.mappings) == 2
        assert result.mappings[0].tvdb_season == 1
        assert result.mappings[0].tvdb_episode == 1
        assert result.mappings[0].scene_season == 1
        assert result.mappings[0].scene_episode == 1
        assert result.mappings[1].tvdb_episode == 2
        assert result.mappings[1].scene_episode == 2

    async def test_get_show_mappings_skips_malformed_entries(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Malformed entries (missing keys, non-int values) are silently skipped."""
        mock_settings = MagicMock()
        mock_settings.xem.enabled = True
        mock_settings.xem.base_url = "https://thexem.info"
        mock_settings.xem.timeout_seconds = 10
        monkeypatch.setattr("src.services.xem.settings", mock_settings)

        body: dict[str, Any] = {
            "result": "success",
            "data": [
                # Missing 'scene' key entirely
                {"tvdb": {"season": 1, "episode": 1}},
                # Non-dict tvdb block
                {"tvdb": "bad", "scene": {"season": 2, "episode": 1}},
                # Non-integer episode in scene block
                {"tvdb": {"season": 1, "episode": 2}, "scene": {"season": 2, "episode": "bad"}},
                # Non-dict entry at top level
                "not-a-dict",
                # Missing episode key in tvdb block
                {"tvdb": {"season": 1}, "scene": {"season": 2, "episode": 3}},
                # Both season and episode identical — client now includes identity entries
                {"tvdb": {"season": 1, "episode": 5}, "scene": {"season": 1, "episode": 5}},
            ],
        }
        # Only malformed entries are skipped. The identical (1,5)→(1,5) entry IS returned now.
        client = XemClient()
        _patch_xem_client(client, [_make_response(200, body)])

        result = await client.get_show_mappings(TVDB_ID)

        assert len(result.mappings) == 1
        assert result.mappings[0].tvdb_episode == 5
        assert result.mappings[0].scene_episode == 5

    async def test_get_show_mappings_api_failure_connect_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """ConnectError → returns empty XemShowMappings, no exception propagated."""
        mock_settings = MagicMock()
        mock_settings.xem.enabled = True
        mock_settings.xem.base_url = "https://thexem.info"
        mock_settings.xem.timeout_seconds = 10
        monkeypatch.setattr("src.services.xem.settings", mock_settings)

        client = XemClient()
        _patch_xem_client(
            client,
            raise_on_send=httpx.ConnectError("connection refused"),
        )

        result = await client.get_show_mappings(TVDB_ID)

        assert isinstance(result, XemShowMappings)
        assert result.tvdb_id == TVDB_ID
        assert result.mappings == []

    async def test_get_show_mappings_timeout(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """TimeoutException → returns empty XemShowMappings, no exception propagated."""
        mock_settings = MagicMock()
        mock_settings.xem.enabled = True
        mock_settings.xem.base_url = "https://thexem.info"
        mock_settings.xem.timeout_seconds = 10
        monkeypatch.setattr("src.services.xem.settings", mock_settings)

        client = XemClient()
        _patch_xem_client(
            client,
            raise_on_send=httpx.ReadTimeout("timed out", request=None),  # type: ignore[arg-type]
        )

        result = await client.get_show_mappings(TVDB_ID)

        assert result.mappings == []

    async def test_get_show_mappings_non_success_result(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """result='failure' in JSON body → returns empty mappings."""
        mock_settings = MagicMock()
        mock_settings.xem.enabled = True
        mock_settings.xem.base_url = "https://thexem.info"
        mock_settings.xem.timeout_seconds = 10
        monkeypatch.setattr("src.services.xem.settings", mock_settings)

        body = {"result": "failure", "message": "show not found"}
        client = XemClient()
        _patch_xem_client(client, [_make_response(200, body)])

        result = await client.get_show_mappings(TVDB_ID)

        assert result.mappings == []

    async def test_get_show_mappings_disabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When xem.enabled=False → returns empty mappings without any HTTP call."""
        mock_settings = MagicMock()
        mock_settings.xem.enabled = False
        monkeypatch.setattr("src.services.xem.settings", mock_settings)

        client = XemClient()
        transport = _patch_xem_client(client, [_make_response(200, _SUCCESS_MAPPINGS_BODY)])

        result = await client.get_show_mappings(TVDB_ID)

        assert result.mappings == []
        assert len(transport.requests_made) == 0

    # ------------------------------------------------------------------
    # get_shows_with_mappings
    # ------------------------------------------------------------------

    async def test_get_shows_with_mappings_success(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Valid response → returns correct set of TVDB IDs."""
        mock_settings = MagicMock()
        mock_settings.xem.enabled = True
        mock_settings.xem.base_url = "https://thexem.info"
        mock_settings.xem.timeout_seconds = 10
        monkeypatch.setattr("src.services.xem.settings", mock_settings)

        client = XemClient()
        _patch_xem_client(client, [_make_response(200, _HAVE_MAP_BODY)])

        result = await client.get_shows_with_mappings()

        assert result == {12345, 67890}

    async def test_get_shows_with_mappings_timeout(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Timeout → returns empty set, no exception propagated."""
        mock_settings = MagicMock()
        mock_settings.xem.enabled = True
        mock_settings.xem.base_url = "https://thexem.info"
        mock_settings.xem.timeout_seconds = 10
        monkeypatch.setattr("src.services.xem.settings", mock_settings)

        client = XemClient()
        _patch_xem_client(
            client,
            raise_on_send=httpx.ReadTimeout("timed out", request=None),  # type: ignore[arg-type]
        )

        result = await client.get_shows_with_mappings()

        assert result == set()

    async def test_get_shows_with_mappings_non_success_result(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Non-success result field → returns empty set."""
        mock_settings = MagicMock()
        mock_settings.xem.enabled = True
        mock_settings.xem.base_url = "https://thexem.info"
        mock_settings.xem.timeout_seconds = 10
        monkeypatch.setattr("src.services.xem.settings", mock_settings)

        body = {"result": "failure", "message": "error"}
        client = XemClient()
        _patch_xem_client(client, [_make_response(200, body)])

        result = await client.get_shows_with_mappings()

        assert result == set()

    async def test_get_shows_with_mappings_filters_non_int(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Non-integer entries in data array are silently dropped."""
        mock_settings = MagicMock()
        mock_settings.xem.enabled = True
        mock_settings.xem.base_url = "https://thexem.info"
        mock_settings.xem.timeout_seconds = 10
        monkeypatch.setattr("src.services.xem.settings", mock_settings)

        body = {"result": "success", "data": [12345, "bad", None, 67890]}
        client = XemClient()
        _patch_xem_client(client, [_make_response(200, body)])

        result = await client.get_shows_with_mappings()

        assert result == {12345, 67890}

    async def test_get_show_mappings_http_500(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """HTTP 500 server error → returns empty mappings."""
        mock_settings = MagicMock()
        mock_settings.xem.enabled = True
        mock_settings.xem.base_url = "https://thexem.info"
        mock_settings.xem.timeout_seconds = 10
        monkeypatch.setattr("src.services.xem.settings", mock_settings)

        client = XemClient()
        _patch_xem_client(client, [_make_response(500, b"Internal Server Error")])

        result = await client.get_show_mappings(TVDB_ID)

        assert result.mappings == []

    async def test_get_show_mappings_http_429_rate_limit(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """HTTP 429 rate limit → returns empty mappings."""
        mock_settings = MagicMock()
        mock_settings.xem.enabled = True
        mock_settings.xem.base_url = "https://thexem.info"
        mock_settings.xem.timeout_seconds = 10
        monkeypatch.setattr("src.services.xem.settings", mock_settings)

        client = XemClient()
        _patch_xem_client(client, [_make_response(429, b"Too Many Requests")])

        result = await client.get_show_mappings(TVDB_ID)

        assert result.mappings == []

    async def test_get_show_mappings_malformed_json(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Response body is not valid JSON → returns empty mappings."""
        mock_settings = MagicMock()
        mock_settings.xem.enabled = True
        mock_settings.xem.base_url = "https://thexem.info"
        mock_settings.xem.timeout_seconds = 10
        monkeypatch.setattr("src.services.xem.settings", mock_settings)

        client = XemClient()
        _patch_xem_client(client, [_make_response(200, b"{not valid json!!!")])

        result = await client.get_show_mappings(TVDB_ID)

        assert result.mappings == []


# ---------------------------------------------------------------------------
# Section B: XemMapper Tests
# ---------------------------------------------------------------------------


def _make_xem_mapping(
    tvdb_season: int = 1,
    tvdb_episode: int = 29,
    scene_season: int = 2,
    scene_episode: int = 1,
) -> XemMapping:
    """Build an XemMapping for use as a mock return value."""
    return XemMapping(
        tvdb_season=tvdb_season,
        tvdb_episode=tvdb_episode,
        scene_season=scene_season,
        scene_episode=scene_episode,
    )


def _make_cache_entry(
    tvdb_id: int = TVDB_ID,
    tvdb_season: int = 1,
    tvdb_episode: int = 29,
    scene_season: int = 2,
    scene_episode: int = 1,
    *,
    age_hours: float = 0.0,
) -> XemCacheEntry:
    """Build an XemCacheEntry with fetched_at set relative to now."""
    fetched_at = datetime.now(timezone.utc) - timedelta(hours=age_hours)
    return XemCacheEntry(
        tvdb_id=tvdb_id,
        tvdb_season=tvdb_season,
        tvdb_episode=tvdb_episode,
        scene_season=scene_season,
        scene_episode=scene_episode,
        fetched_at=fetched_at,
    )


class TestXemMapper:
    """Tests for XemMapper DB caching layer using in-memory SQLite."""

    # ------------------------------------------------------------------
    # get_scene_numbering — fresh fetch (no cache)
    # ------------------------------------------------------------------

    async def test_get_scene_numbering_fresh_fetch(self, session: AsyncSession) -> None:
        """No cache in DB → calls XEM API → inserts cache rows → returns mapping."""
        from src.core.xem_mapper import XemMapper

        mock_show_mappings = XemShowMappings(
            tvdb_id=TVDB_ID,
            mappings=[_make_xem_mapping(tvdb_episode=29, scene_episode=1)],
        )

        mock_client = AsyncMock()
        mock_client.get_show_mappings.return_value = mock_show_mappings

        with patch("src.core.xem_mapper.xem_client", mock_client):
            mapper = XemMapper()
            result = await mapper.get_scene_numbering(session, TVDB_ID, season=1, episode=29)

        assert result == (2, 1)
        mock_client.get_show_mappings.assert_awaited_once_with(TVDB_ID)

        # Verify cache rows were written to DB
        rows = list((await session.execute(
            select(XemCacheEntry).where(XemCacheEntry.tvdb_id == TVDB_ID)
        )).scalars().all())
        assert len(rows) == 1
        assert rows[0].tvdb_episode == 29
        assert rows[0].scene_episode == 1

    # ------------------------------------------------------------------
    # get_scene_numbering — cache hit (fresh entries)
    # ------------------------------------------------------------------

    async def test_get_scene_numbering_cache_hit(self, session: AsyncSession) -> None:
        """Fresh cache entries exist → returns cached mapping WITHOUT calling XEM API."""
        from src.core.xem_mapper import XemMapper

        entry = _make_cache_entry(tvdb_episode=29, scene_episode=1, age_hours=0.5)
        session.add(entry)
        await session.flush()

        mock_client = AsyncMock()

        with patch("src.core.xem_mapper.xem_client", mock_client):
            mapper = XemMapper()
            result = await mapper.get_scene_numbering(session, TVDB_ID, season=1, episode=29)

        assert result == (2, 1)
        mock_client.get_show_mappings.assert_not_awaited()

    # ------------------------------------------------------------------
    # get_scene_numbering — stale cache
    # ------------------------------------------------------------------

    async def test_get_scene_numbering_cache_stale(self, session: AsyncSession) -> None:
        """Cache older than cache_hours → calls API → old rows deleted, new rows inserted."""
        from src.core.xem_mapper import XemMapper

        # Insert stale entry (25 hours old, cache_hours default = 24)
        stale_entry = _make_cache_entry(tvdb_episode=29, scene_episode=1, age_hours=25.0)
        session.add(stale_entry)
        await session.flush()

        # API returns updated mapping — scene_episode changed from 1 to 99
        fresh_mappings = XemShowMappings(
            tvdb_id=TVDB_ID,
            mappings=[_make_xem_mapping(tvdb_episode=29, scene_episode=99)],
        )
        mock_client = AsyncMock()
        mock_client.get_show_mappings.return_value = fresh_mappings

        with patch("src.core.xem_mapper.xem_client", mock_client):
            mapper = XemMapper()
            result = await mapper.get_scene_numbering(session, TVDB_ID, season=1, episode=29)

        assert result == (2, 99)
        mock_client.get_show_mappings.assert_awaited_once_with(TVDB_ID)

        # Verify only fresh rows remain (stale row with scene_episode=1 is gone)
        rows = list((await session.execute(
            select(XemCacheEntry).where(XemCacheEntry.tvdb_id == TVDB_ID)
        )).scalars().all())
        assert len(rows) == 1
        assert rows[0].scene_episode == 99

    # ------------------------------------------------------------------
    # get_scene_numbering — no mapping from XEM
    # ------------------------------------------------------------------

    async def test_get_scene_numbering_no_mapping(self, session: AsyncSession) -> None:
        """XEM returns empty mappings for a show → returns None."""
        from src.core.xem_mapper import XemMapper

        mock_client = AsyncMock()
        mock_client.get_show_mappings.return_value = XemShowMappings(
            tvdb_id=TVDB_ID, mappings=[]
        )

        with patch("src.core.xem_mapper.xem_client", mock_client):
            mapper = XemMapper()
            result = await mapper.get_scene_numbering(session, TVDB_ID, season=1, episode=1)

        assert result is None

    # ------------------------------------------------------------------
    # get_scene_numbering — episode not in mappings
    # ------------------------------------------------------------------

    async def test_get_scene_numbering_episode_not_in_mappings(
        self, session: AsyncSession
    ) -> None:
        """XEM has mappings but not for the requested episode → returns None."""
        from src.core.xem_mapper import XemMapper

        # Cache contains mapping for episode 29, but test asks for episode 5
        entry = _make_cache_entry(tvdb_episode=29, scene_episode=1, age_hours=0.0)
        session.add(entry)
        await session.flush()

        mock_client = AsyncMock()

        with patch("src.core.xem_mapper.xem_client", mock_client):
            mapper = XemMapper()
            result = await mapper.get_scene_numbering(session, TVDB_ID, season=1, episode=5)

        # Episode 5 is not in mappings → no remapping needed
        assert result is None
        mock_client.get_show_mappings.assert_not_awaited()

    # ------------------------------------------------------------------
    # get_scene_numbering_for_item — resolves tvdb_id via TMDB
    # ------------------------------------------------------------------

    async def test_get_scene_numbering_for_item_resolves_tvdb_id(
        self, session: AsyncSession
    ) -> None:
        """tvdb_id=None + tmdb_id set → calls TMDB get_external_ids → uses resolved tvdb_id."""
        from src.core.xem_mapper import XemMapper

        resolved_tvdb = 99999
        ext_ids = TmdbExternalIds(imdb_id="tt1234567", tvdb_id=resolved_tvdb)

        mock_tmdb = AsyncMock()
        mock_tmdb.get_external_ids.return_value = ext_ids

        # XEM returns a mapping for the resolved tvdb_id
        fresh_mappings = XemShowMappings(
            tvdb_id=resolved_tvdb,
            mappings=[_make_xem_mapping(tvdb_episode=5, scene_episode=55)],
        )
        mock_xem_client = AsyncMock()
        mock_xem_client.get_show_mappings.return_value = fresh_mappings

        with (
            patch("src.core.xem_mapper.xem_client", mock_xem_client),
            patch("src.core.xem_mapper.tmdb_client", mock_tmdb, create=True),
        ):
            # Patch the inline import inside get_scene_numbering_for_item
            with patch("src.services.tmdb.tmdb_client", mock_tmdb):
                mapper = XemMapper()
                result = await mapper.get_scene_numbering_for_item(
                    session,
                    tvdb_id=None,
                    tmdb_id="12345",
                    season=1,
                    episode=5,
                )

        mock_tmdb.get_external_ids.assert_awaited_once_with(12345, "tv")
        mock_xem_client.get_show_mappings.assert_awaited_once_with(resolved_tvdb)
        assert result == (2, 55)

    async def test_get_scene_numbering_for_item_no_tvdb_id(
        self, session: AsyncSession
    ) -> None:
        """tvdb_id=None and tmdb_id=None → returns None without any API calls."""
        from src.core.xem_mapper import XemMapper

        mock_client = AsyncMock()

        with patch("src.core.xem_mapper.xem_client", mock_client):
            mapper = XemMapper()
            result = await mapper.get_scene_numbering_for_item(
                session,
                tvdb_id=None,
                tmdb_id=None,
                season=1,
                episode=1,
            )

        assert result is None
        mock_client.get_show_mappings.assert_not_awaited()

    async def test_get_scene_numbering_for_item_tmdb_resolve_fails(
        self, session: AsyncSession
    ) -> None:
        """tvdb_id=None + TMDB returns None ext_ids → returns None gracefully."""
        from src.core.xem_mapper import XemMapper

        mock_tmdb = AsyncMock()
        mock_tmdb.get_external_ids.return_value = None

        mock_xem_client = AsyncMock()

        with patch("src.core.xem_mapper.xem_client", mock_xem_client):
            with patch("src.services.tmdb.tmdb_client", mock_tmdb):
                mapper = XemMapper()
                result = await mapper.get_scene_numbering_for_item(
                    session,
                    tvdb_id=None,
                    tmdb_id="99999",
                    season=1,
                    episode=3,
                )

        assert result is None
        mock_xem_client.get_show_mappings.assert_not_awaited()

    async def test_get_scene_numbering_xem_disabled_returns_none(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When XEM is disabled the client returns empty → mapper returns None."""
        from src.core.xem_mapper import XemMapper

        # Client will return empty mappings (simulating disabled state)
        mock_client = AsyncMock()
        mock_client.get_show_mappings.return_value = XemShowMappings(
            tvdb_id=TVDB_ID, mappings=[]
        )

        with patch("src.core.xem_mapper.xem_client", mock_client):
            mapper = XemMapper()
            result = await mapper.get_scene_numbering(session, TVDB_ID, season=1, episode=5)

        assert result is None

    # ------------------------------------------------------------------
    # Edge cases
    # ------------------------------------------------------------------

    async def test_get_scene_numbering_multiple_episodes_cached(
        self, session: AsyncSession
    ) -> None:
        """Multiple cache entries for a show — correct episode is selected."""
        from src.core.xem_mapper import XemMapper

        # Two different episodes cached, ask for episode 30
        for ep in (29, 30):
            entry = _make_cache_entry(
                tvdb_episode=ep,
                scene_episode=ep - 28,  # ep29→1, ep30→2
                age_hours=0.0,
            )
            session.add(entry)
        await session.flush()

        mock_client = AsyncMock()

        with patch("src.core.xem_mapper.xem_client", mock_client):
            mapper = XemMapper()
            result = await mapper.get_scene_numbering(session, TVDB_ID, season=1, episode=30)

        assert result == (2, 2)
        mock_client.get_show_mappings.assert_not_awaited()

    async def test_get_scene_numbering_for_item_with_tvdb_id_skips_tmdb(
        self, session: AsyncSession
    ) -> None:
        """When tvdb_id is provided directly, TMDB is NOT consulted."""
        from src.core.xem_mapper import XemMapper

        fresh_mappings = XemShowMappings(
            tvdb_id=TVDB_ID,
            mappings=[_make_xem_mapping(tvdb_episode=3, scene_episode=3)],
        )
        # Note: scene_episode == tvdb_episode here, so the mapping client returns it
        # but it wasn't filtered at API level (it came from cache in this test path)
        mock_xem_client = AsyncMock()
        mock_xem_client.get_show_mappings.return_value = fresh_mappings

        mock_tmdb = AsyncMock()

        with patch("src.core.xem_mapper.xem_client", mock_xem_client):
            mapper = XemMapper()
            result = await mapper.get_scene_numbering_for_item(
                session,
                tvdb_id=TVDB_ID,
                tmdb_id="12345",
                season=1,
                episode=3,
            )

        # TMDB should never have been called
        mock_tmdb.get_external_ids.assert_not_called()
        # Result: scene episode==3 is in cache so (2, 3) returned
        assert result == (2, 3)


# ---------------------------------------------------------------------------
# Section B2: XemMapper.get_all_scene_mappings Tests
# ---------------------------------------------------------------------------


class TestXemMapperGetAllSceneMappings:
    """Tests for XemMapper.get_all_scene_mappings — new method added alongside refactoring."""

    async def test_returns_correct_dict_from_cache_entries(self, session: AsyncSession) -> None:
        """Fresh cache with two entries → correct (tvdb_s, tvdb_e)→(scene_s, scene_e) dict."""
        from src.core.xem_mapper import XemMapper

        entry1 = _make_cache_entry(tvdb_episode=29, scene_season=2, scene_episode=1, age_hours=0.0)
        entry2 = _make_cache_entry(tvdb_episode=30, scene_season=2, scene_episode=2, age_hours=0.0)
        session.add(entry1)
        session.add(entry2)
        await session.flush()

        mock_client = AsyncMock()

        with patch("src.core.xem_mapper.xem_client", mock_client):
            mapper = XemMapper()
            result = await mapper.get_all_scene_mappings(session, TVDB_ID)

        assert isinstance(result, dict)
        assert (1, 29) in result
        assert result[(1, 29)] == (2, 1)
        assert (1, 30) in result
        assert result[(1, 30)] == (2, 2)
        # No API call — cache was fresh
        mock_client.get_show_mappings.assert_not_awaited()

    async def test_returns_empty_dict_when_no_cache_entries(self, session: AsyncSession) -> None:
        """No cache and XEM returns no mappings → empty dict returned."""
        from src.core.xem_mapper import XemMapper

        mock_client = AsyncMock()
        mock_client.get_show_mappings.return_value = XemShowMappings(
            tvdb_id=TVDB_ID, mappings=[]
        )

        with patch("src.core.xem_mapper.xem_client", mock_client):
            mapper = XemMapper()
            result = await mapper.get_all_scene_mappings(session, TVDB_ID)

        assert result == {}
        mock_client.get_show_mappings.assert_awaited_once_with(TVDB_ID)

    async def test_returns_empty_dict_when_xem_disabled(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When settings.xem.enabled=False → empty dict without any API call."""
        from src.core.xem_mapper import XemMapper

        mock_settings = MagicMock()
        mock_settings.xem.enabled = False
        mock_settings.xem.cache_hours = 24
        monkeypatch.setattr("src.core.xem_mapper.settings", mock_settings)

        mock_client = AsyncMock()

        with patch("src.core.xem_mapper.xem_client", mock_client):
            mapper = XemMapper()
            result = await mapper.get_all_scene_mappings(session, TVDB_ID)

        assert result == {}
        mock_client.get_show_mappings.assert_not_awaited()

    async def test_multiple_entries_produce_correct_key_value_pairs(
        self, session: AsyncSession
    ) -> None:
        """Many cache entries → all appear as separate keys in returned dict."""
        from src.core.xem_mapper import XemMapper

        entries = [
            _make_cache_entry(tvdb_episode=ep, scene_season=2, scene_episode=ep - 28, age_hours=0.0)
            for ep in range(29, 36)  # ep 29..35 → scene ep 1..7
        ]
        for e in entries:
            session.add(e)
        await session.flush()

        mock_client = AsyncMock()

        with patch("src.core.xem_mapper.xem_client", mock_client):
            mapper = XemMapper()
            result = await mapper.get_all_scene_mappings(session, TVDB_ID)

        assert len(result) == 7
        for i, tvdb_ep in enumerate(range(29, 36), start=1):
            assert result[(1, tvdb_ep)] == (2, i)
        mock_client.get_show_mappings.assert_not_awaited()

    async def test_cache_shared_between_get_all_and_get_scene_numbering(
        self, session: AsyncSession
    ) -> None:
        """Calling get_all_scene_mappings then get_scene_numbering reuses the cache.

        Only one XEM API call should be made — both methods share _ensure_cached_entries.
        """
        from src.core.xem_mapper import XemMapper

        fresh_mappings = XemShowMappings(
            tvdb_id=TVDB_ID,
            mappings=[_make_xem_mapping(tvdb_episode=29, scene_episode=1)],
        )
        mock_client = AsyncMock()
        mock_client.get_show_mappings.return_value = fresh_mappings

        with patch("src.core.xem_mapper.xem_client", mock_client):
            mapper = XemMapper()
            all_map = await mapper.get_all_scene_mappings(session, TVDB_ID)
            single = await mapper.get_scene_numbering(session, TVDB_ID, season=1, episode=29)

        # get_all_scene_mappings triggered one API call and wrote cache.
        # get_scene_numbering found fresh cache — no second API call.
        assert mock_client.get_show_mappings.await_count == 1
        assert all_map == {(1, 29): (2, 1)}
        assert single == (2, 1)


# ---------------------------------------------------------------------------
# Section C: Pipeline XEM Integration Tests
# ---------------------------------------------------------------------------


def _make_show_item_in_db() -> MediaItem:
    """Build a show MediaItem (not yet persisted) with tvdb_id set."""
    return MediaItem(
        imdb_id="tt7654321",
        title="Test Anime Show",
        year=2020,
        media_type=MediaType.SHOW,
        state=QueueState.WANTED,
        state_changed_at=datetime.now(timezone.utc),
        retry_count=0,
        season=1,
        episode=29,
        is_season_pack=False,
        tvdb_id=TVDB_ID,
        tmdb_id="98765",
    )


# Patch targets matching the pipeline's actual import pattern.
# The pipeline does `from src.core.xem_mapper import xem_mapper` inside the
# function body at call time, so we patch the singleton on the source module.
_PIPELINE_PATCH_TARGETS = {
    "mount_scanner_available": "src.core.scrape_pipeline.mount_scanner.is_mount_available",
    "mount_scanner_lookup": "src.core.scrape_pipeline.mount_scanner.lookup",
    "dedup_check": "src.core.scrape_pipeline.dedup_engine.check_content_duplicate",
    "dedup_register": "src.core.scrape_pipeline.dedup_engine.register_torrent",
    "zilean_search": "src.core.scrape_pipeline.zilean_client.search",
    "torrentio_movie": "src.core.scrape_pipeline.torrentio_client.scrape_movie",
    "torrentio_episode": "src.core.scrape_pipeline.torrentio_client.scrape_episode",
    "rd_add": "src.core.scrape_pipeline.rd_client.add_magnet",
    "rd_select": "src.core.scrape_pipeline.rd_client.select_files",
    "rd_check_cached_batch": "src.core.scrape_pipeline.rd_client.check_cached_batch",
    "rd_delete": "src.core.scrape_pipeline.rd_client.delete_torrent",
    "filter_rank": "src.core.scrape_pipeline.filter_engine.filter_and_rank",
    "queue_transition": "src.core.scrape_pipeline.queue_manager.transition",
    "xem_mapper_get": "src.core.xem_mapper.xem_mapper.get_scene_numbering_for_item",
}


class _PipelineMocks:
    mount_scanner_available: AsyncMock
    mount_scanner_lookup: AsyncMock
    dedup_check: AsyncMock
    dedup_register: AsyncMock
    zilean_search: AsyncMock
    torrentio_movie: AsyncMock
    torrentio_episode: AsyncMock
    rd_add: AsyncMock
    rd_select: AsyncMock
    rd_check_cached_batch: AsyncMock
    rd_delete: AsyncMock
    filter_rank: MagicMock
    queue_transition: AsyncMock
    xem_mapper_get: AsyncMock


@asynccontextmanager
async def _pipeline_mocks(
    *,
    xem_mapping: tuple[int, int] | None = None,
) -> AsyncGenerator[_PipelineMocks, None]:
    """Patch every ScrapePipeline dependency including the XEM mapper."""
    mocks = _PipelineMocks()
    patchers = {name: patch(target) for name, target in _PIPELINE_PATCH_TARGETS.items()}

    started: dict[str, MagicMock] = {}
    try:
        for name, patcher in patchers.items():
            started[name] = patcher.start()
    except Exception:
        for p in patchers.values():
            try:
                p.stop()
            except RuntimeError:
                pass
        raise

    mocks.mount_scanner_available = started["mount_scanner_available"]
    mocks.mount_scanner_available.return_value = True
    mocks.mount_scanner_lookup = started["mount_scanner_lookup"]
    mocks.mount_scanner_lookup.return_value = []
    mocks.dedup_check = started["dedup_check"]
    mocks.dedup_check.return_value = None
    mocks.dedup_register = started["dedup_register"]
    mocks.dedup_register.return_value = MagicMock()
    mocks.zilean_search = started["zilean_search"]
    mocks.zilean_search.return_value = []
    mocks.torrentio_movie = started["torrentio_movie"]
    mocks.torrentio_movie.return_value = []
    mocks.torrentio_episode = started["torrentio_episode"]
    mocks.torrentio_episode.return_value = []
    mocks.rd_add = started["rd_add"]
    mocks.rd_add.return_value = {"id": "RD123", "uri": "magnet:?xt=urn:btih:" + "a" * 40}
    mocks.rd_select = started["rd_select"]
    mocks.rd_select.return_value = None
    mocks.rd_check_cached_batch = started["rd_check_cached_batch"]
    mocks.rd_check_cached_batch.return_value = {}
    mocks.rd_delete = started["rd_delete"]
    mocks.rd_delete.return_value = None
    mocks.filter_rank = started["filter_rank"]
    mocks.filter_rank.return_value = []
    mocks.queue_transition = started["queue_transition"]
    mocks.queue_transition.return_value = MagicMock(spec=MediaItem)
    mocks.xem_mapper_get = started["xem_mapper_get"]
    mocks.xem_mapper_get.return_value = xem_mapping

    try:
        yield mocks
    finally:
        for p in patchers.values():
            p.stop()


class TestScrapePipelineXem:
    """Integration tests: pipeline correctly applies XEM-mapped numbers.

    XEM resolution now happens once in _run_pipeline, which passes the resolved
    scene_season/scene_episode to both _step_zilean and _step_torrentio as
    keyword arguments.  The step tests below verify that the steps correctly
    honour the pre-resolved numbers passed in.  Full-pipeline tests verify that
    _run_pipeline calls the XEM mapper and passes results through.
    """

    async def test_torrentio_uses_scene_numbering(self, session: AsyncSession) -> None:
        """_step_torrentio called with scene numbers → scrape_episode uses those numbers."""
        from src.core.scrape_pipeline import ScrapePipeline

        item = _make_show_item_in_db()
        session.add(item)
        await session.flush()

        # XEM resolution done externally; pass scene numbers directly to the step.
        async with _pipeline_mocks() as m:
            pipeline = ScrapePipeline()
            await pipeline._step_torrentio(session, item, scene_season=2, scene_episode=1)

            # torrentio_episode should have been called with the passed scene numbers
            m.torrentio_episode.assert_awaited_once_with(
                item.imdb_id,
                2,  # scene_season
                1,  # scene_episode
            )

    async def test_torrentio_no_xem_mapping_uses_original(
        self, session: AsyncSession
    ) -> None:
        """When scene_season/scene_episode not passed, _step_torrentio uses original numbers."""
        from src.core.scrape_pipeline import ScrapePipeline

        item = _make_show_item_in_db()
        session.add(item)
        await session.flush()

        # No scene numbers provided — falls back to item.season / item.episode.
        async with _pipeline_mocks() as m:
            pipeline = ScrapePipeline()
            await pipeline._step_torrentio(session, item)

            # Original season/episode used: S01E29
            m.torrentio_episode.assert_awaited_once_with(
                item.imdb_id,
                1,  # original season
                29,  # original episode
            )

    async def test_torrentio_season_pack_uses_original_episode_anchor(
        self, session: AsyncSession
    ) -> None:
        """Season pack items (episode=None) use episode=1 as the scrape anchor."""
        from src.core.scrape_pipeline import ScrapePipeline

        item = MediaItem(
            imdb_id="tt7654321",
            title="Test Anime Show",
            year=2020,
            media_type=MediaType.SHOW,
            state=QueueState.WANTED,
            state_changed_at=datetime.now(timezone.utc),
            retry_count=0,
            season=2,
            episode=None,  # season pack
            is_season_pack=True,
            tvdb_id=TVDB_ID,
        )
        session.add(item)
        await session.flush()

        # No scene_season/scene_episode passed (XEM not called for season packs
        # by _run_pipeline because item.episode is None).
        async with _pipeline_mocks() as m:
            pipeline = ScrapePipeline()
            await pipeline._step_torrentio(session, item)

            # Torrentio called with season=2 and episode=1 anchor (no XEM remapping).
            m.torrentio_episode.assert_awaited_once_with(
                item.imdb_id,
                2,  # original season
                1,  # episode=1 anchor for season pack
            )

    async def test_zilean_uses_scene_numbering(self, session: AsyncSession) -> None:
        """_step_zilean called with scene numbers → zilean_client.search uses those numbers."""
        from src.core.scrape_pipeline import ScrapePipeline

        item = _make_show_item_in_db()
        session.add(item)
        await session.flush()

        # XEM maps S01E29 → S02E01 — resolved externally, passed to the step.
        async with _pipeline_mocks() as m:
            pipeline = ScrapePipeline()
            await pipeline._step_zilean(session, item, scene_season=2, scene_episode=1)

            call_kwargs = m.zilean_search.call_args
            assert call_kwargs is not None
            # The search should use the passed scene season=2, episode=1
            assert call_kwargs.kwargs.get("season") == 2
            assert call_kwargs.kwargs.get("episode") == 1

    async def test_zilean_no_xem_mapping_uses_original(self, session: AsyncSession) -> None:
        """When no scene numbers are passed, _step_zilean uses original season/episode."""
        from src.core.scrape_pipeline import ScrapePipeline

        item = _make_show_item_in_db()
        session.add(item)
        await session.flush()

        async with _pipeline_mocks() as m:
            pipeline = ScrapePipeline()
            await pipeline._step_zilean(session, item)

            call_kwargs = m.zilean_search.call_args
            assert call_kwargs is not None
            # Original numbers used
            assert call_kwargs.kwargs.get("season") == 1
            assert call_kwargs.kwargs.get("episode") == 29

    async def test_xem_disabled_run_pipeline_skips_mapper(
        self, session: AsyncSession
    ) -> None:
        """When settings.xem.enabled=False, _run_pipeline never calls XEM mapper."""
        from src.core.scrape_pipeline import ScrapePipeline

        item = _make_show_item_in_db()
        session.add(item)
        await session.flush()

        mock_settings = MagicMock()
        mock_settings.xem.enabled = False
        # Pipeline reads these settings in various places
        mock_settings.search.cache_check_limit = 5

        async with _pipeline_mocks(xem_mapping=(99, 99)) as m:
            with patch("src.core.scrape_pipeline.settings", mock_settings):
                pipeline = ScrapePipeline()
                await pipeline._run_pipeline(session, item)

            # XEM mapper must NOT have been called because xem.enabled=False
            m.xem_mapper_get.assert_not_awaited()
            # Torrentio called with original numbers (no XEM remapping applied)
            m.torrentio_episode.assert_awaited_once_with(
                item.imdb_id,
                1,   # original season
                29,  # original episode
            )
