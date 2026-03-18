"""Tests for AniDB title enrichment and episode count lookups.

Covers:
  Section A — is_data_fresh (3 tests)
    - Empty DB → returns False
    - Recent fetched_at → returns True
    - Stale fetched_at → returns False

  Section B — get_titles_for_tmdb_id (6 tests)
    - No mapping rows → returns []
    - With titles → returned in priority order, deduplicated
    - Multiple anidb_ids for same tmdb_id → all titles collected
    - Disabled → returns []
    - Language filter: non-configured language excluded
    - Exact case-insensitive deduplication

  Section C — refresh_data (4 tests)
    - Success: titles and mappings inserted; Korean title filtered
    - Disabled → returns early, no DB changes
    - Title dump HTTP failure → returns without crash
    - Old data replaced by new data on success

  Section D — get_episode_count_for_tmdb_season (5 tests)
    - Cached episode_count → returns without API call
    - No mapping → returns None
    - api_enabled=False → returns None
    - api_enabled=True → fetches from HTTP, caches in DB
    - Multiple mappings same season → sums episode counts
"""

from __future__ import annotations

import gzip
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.anidb import AnidbMapping, AnidbTitle
from src.services.anidb import AnidbClient
from src.services.http_client import CircuitBreaker


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_noop_breaker() -> CircuitBreaker:
    """Return a CircuitBreaker that is always CLOSED (never rejects requests)."""
    return CircuitBreaker("test", failure_threshold=999, recovery_timeout=0.0)


def _make_anidb_cfg(
    *,
    enabled: bool = True,
    api_enabled: bool = False,
    client_name: str = "",
    client_version: int = 1,
    titles_url: str = "https://anidb.net/api/anime-titles.xml.gz",
    mappings_url: str = "https://fribb.example.com/anime-list.json",
    api_base_url: str = "http://api.anidb.net:9001/httpapi",
    refresh_hours: int = 168,
    timeout_seconds: int = 30,
    title_languages: list[str] | None = None,
) -> MagicMock:
    """Return a MagicMock that looks like AnidbConfig."""
    cfg = MagicMock()
    cfg.enabled = enabled
    cfg.api_enabled = api_enabled
    cfg.client_name = client_name
    cfg.client_version = client_version
    cfg.titles_url = titles_url
    cfg.mappings_url = mappings_url
    cfg.api_base_url = api_base_url
    cfg.refresh_hours = refresh_hours
    cfg.timeout_seconds = timeout_seconds
    cfg.title_languages = title_languages if title_languages is not None else ["x-jat", "en", "ja"]
    return cfg


def _make_mock_settings(cfg: MagicMock) -> MagicMock:
    """Return a MagicMock for settings with .anidb set to cfg."""
    s = MagicMock()
    s.anidb = cfg
    return s


def _make_response(
    status_code: int,
    body: bytes | dict | list | None = None,
) -> httpx.Response:
    """Build a minimal fake httpx.Response."""
    if body is None:
        raw = b""
    elif isinstance(body, (dict, list)):
        raw = json.dumps(body).encode()
    elif isinstance(body, bytes):
        raw = body
    else:
        raw = str(body).encode()

    return httpx.Response(
        status_code=status_code,
        headers={"content-type": "application/json"},
        content=raw,
        request=httpx.Request("GET", "http://test.local/"),
    )


def _make_gzip_xml(xml_bytes: bytes) -> bytes:
    """Gzip-compress raw XML bytes."""
    return gzip.compress(xml_bytes)


def _minimal_titles_xml() -> bytes:
    """Return a minimal AniDB anime-titles XML document (not gzipped)."""
    return b"""<?xml version="1.0" encoding="UTF-8"?>
<animetitles>
  <anime aid="1">
    <title xml:lang="x-jat" type="main">Test Anime Romaji</title>
    <title xml:lang="en" type="official">Test Anime English</title>
    <title xml:lang="ja" type="official">Test Anime Japanese</title>
    <title xml:lang="x-jat" type="syn">Test Anime Synonym</title>
    <title xml:lang="ko" type="official">Test Anime Korean</title>
  </anime>
  <anime aid="2">
    <title xml:lang="x-jat" type="main">Second Anime</title>
    <title xml:lang="en" type="official">Second Anime EN</title>
  </anime>
</animetitles>"""


def _minimal_mappings_json() -> list[dict]:
    """Return a minimal Fribb anime-list mapping payload."""
    return [
        {
            "anidb_id": 1,
            "themoviedb_id": 100,
            "themoviedb_season": 1,
            "thetvdb_id": 200,
            "imdb_id": "tt0000001",
        },
        {
            "anidb_id": 2,
            "themoviedb_id": 100,
            "themoviedb_season": 2,
            "thetvdb_id": 200,
            "imdb_id": "tt0000001",
        },
        {
            "anidb_id": 3,
            "themoviedb_id": 300,
            "themoviedb_season": 1,
            "thetvdb_id": None,
            "imdb_id": None,
        },
    ]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def client() -> AnidbClient:
    """Fresh AniDB client for each test."""
    return AnidbClient()


# ---------------------------------------------------------------------------
# Section A — is_data_fresh
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_is_data_fresh_empty_db(client: AnidbClient, session: AsyncSession) -> None:
    """Empty DB returns False — no titles have been fetched yet."""
    cfg = _make_anidb_cfg()
    mock_settings = _make_mock_settings(cfg)

    with patch("src.services.anidb.settings", mock_settings):
        result = await client.is_data_fresh(session)

    assert result is False


@pytest.mark.asyncio
async def test_is_data_fresh_recent(client: AnidbClient, session: AsyncSession) -> None:
    """A title fetched just now is within the refresh window → True."""
    now = datetime.now(timezone.utc)
    session.add(AnidbTitle(
        anidb_id=1,
        title="Test Anime",
        title_type="main",
        language="en",
        fetched_at=now,
    ))
    await session.flush()

    cfg = _make_anidb_cfg(refresh_hours=168)
    mock_settings = _make_mock_settings(cfg)

    with patch("src.services.anidb.settings", mock_settings):
        result = await client.is_data_fresh(session)

    assert result is True


@pytest.mark.asyncio
async def test_is_data_fresh_stale(client: AnidbClient, session: AsyncSession) -> None:
    """A title fetched 200 hours ago is outside the 168h refresh window → False."""
    stale_time = datetime.now(timezone.utc) - timedelta(hours=200)
    session.add(AnidbTitle(
        anidb_id=1,
        title="Test Anime",
        title_type="main",
        language="en",
        fetched_at=stale_time,
    ))
    await session.flush()

    cfg = _make_anidb_cfg(refresh_hours=168)
    mock_settings = _make_mock_settings(cfg)

    with patch("src.services.anidb.settings", mock_settings):
        result = await client.is_data_fresh(session)

    assert result is False


# ---------------------------------------------------------------------------
# Section B — get_titles_for_tmdb_id
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_titles_for_tmdb_id_no_mapping(
    client: AnidbClient, session: AsyncSession
) -> None:
    """When no AnidbMapping rows exist for the given tmdb_id, returns []."""
    cfg = _make_anidb_cfg()
    mock_settings = _make_mock_settings(cfg)

    with patch("src.services.anidb.settings", mock_settings):
        result = await client.get_titles_for_tmdb_id(session, tmdb_id=999)

    assert result == []


@pytest.mark.asyncio
async def test_get_titles_for_tmdb_id_with_titles(
    client: AnidbClient, session: AsyncSession
) -> None:
    """Titles returned in priority order: main=0 < official=1 < syn=2."""
    now = datetime.now(timezone.utc)

    session.add(AnidbMapping(anidb_id=10, tmdb_id=100, tmdb_season=1, fetched_at=now))
    session.add_all([
        AnidbTitle(anidb_id=10, title="Show Title Romaji", title_type="main", language="x-jat", fetched_at=now),
        AnidbTitle(anidb_id=10, title="Show Title English", title_type="official", language="en", fetched_at=now),
        AnidbTitle(anidb_id=10, title="Show Title Synonym", title_type="syn", language="x-jat", fetched_at=now),
    ])
    await session.flush()

    cfg = _make_anidb_cfg()
    mock_settings = _make_mock_settings(cfg)

    with patch("src.services.anidb.settings", mock_settings):
        result = await client.get_titles_for_tmdb_id(session, tmdb_id=100)

    assert len(result) == 3
    # main must come before official, official before syn
    assert result[0] == "Show Title Romaji"
    assert result[1] == "Show Title English"
    assert result[2] == "Show Title Synonym"


@pytest.mark.asyncio
async def test_get_titles_for_tmdb_id_deduplication(
    client: AnidbClient, session: AsyncSession
) -> None:
    """Same title in different cases is deduplicated; the first (higher priority) wins."""
    now = datetime.now(timezone.utc)

    session.add(AnidbMapping(anidb_id=20, tmdb_id=200, tmdb_season=1, fetched_at=now))
    session.add_all([
        AnidbTitle(anidb_id=20, title="Duplicate Title", title_type="main", language="x-jat", fetched_at=now),
        # Same title, different casing — should be deduplicated
        AnidbTitle(anidb_id=20, title="duplicate title", title_type="official", language="en", fetched_at=now),
    ])
    await session.flush()

    cfg = _make_anidb_cfg()
    mock_settings = _make_mock_settings(cfg)

    with patch("src.services.anidb.settings", mock_settings):
        result = await client.get_titles_for_tmdb_id(session, tmdb_id=200)

    assert len(result) == 1
    assert result[0] == "Duplicate Title"


@pytest.mark.asyncio
async def test_get_titles_for_tmdb_id_multiple_anidb(
    client: AnidbClient, session: AsyncSession
) -> None:
    """Two anidb_ids mapped to the same tmdb_id → all their titles are collected."""
    now = datetime.now(timezone.utc)

    session.add_all([
        AnidbMapping(anidb_id=30, tmdb_id=300, tmdb_season=1, fetched_at=now),
        AnidbMapping(anidb_id=31, tmdb_id=300, tmdb_season=2, fetched_at=now),
    ])
    session.add_all([
        AnidbTitle(anidb_id=30, title="Show Part One", title_type="main", language="x-jat", fetched_at=now),
        AnidbTitle(anidb_id=31, title="Show Part Two", title_type="main", language="x-jat", fetched_at=now),
    ])
    await session.flush()

    cfg = _make_anidb_cfg()
    mock_settings = _make_mock_settings(cfg)

    with patch("src.services.anidb.settings", mock_settings):
        result = await client.get_titles_for_tmdb_id(session, tmdb_id=300)

    assert "Show Part One" in result
    assert "Show Part Two" in result
    assert len(result) == 2


@pytest.mark.asyncio
async def test_get_titles_for_tmdb_id_disabled(
    client: AnidbClient, session: AsyncSession
) -> None:
    """When anidb.enabled=False, returns [] immediately without querying the DB."""
    now = datetime.now(timezone.utc)
    session.add(AnidbMapping(anidb_id=40, tmdb_id=400, tmdb_season=1, fetched_at=now))
    session.add(AnidbTitle(anidb_id=40, title="Ignored Anime", title_type="main", language="x-jat", fetched_at=now))
    await session.flush()

    cfg = _make_anidb_cfg(enabled=False)
    mock_settings = _make_mock_settings(cfg)

    with patch("src.services.anidb.settings", mock_settings):
        result = await client.get_titles_for_tmdb_id(session, tmdb_id=400)

    assert result == []


@pytest.mark.asyncio
async def test_get_titles_for_tmdb_id_language_filter(
    client: AnidbClient, session: AsyncSession
) -> None:
    """A title with language 'ko' (Korean) is excluded when not in title_languages."""
    now = datetime.now(timezone.utc)

    session.add(AnidbMapping(anidb_id=50, tmdb_id=500, tmdb_season=1, fetched_at=now))
    session.add_all([
        AnidbTitle(anidb_id=50, title="Show Title EN", title_type="official", language="en", fetched_at=now),
        # Korean — not in default ["x-jat", "en", "ja"]
        AnidbTitle(anidb_id=50, title="Show Title KO", title_type="official", language="ko", fetched_at=now),
    ])
    await session.flush()

    # Default title_languages = ["x-jat", "en", "ja"] — no "ko"
    cfg = _make_anidb_cfg(title_languages=["x-jat", "en", "ja"])
    mock_settings = _make_mock_settings(cfg)

    with patch("src.services.anidb.settings", mock_settings):
        result = await client.get_titles_for_tmdb_id(session, tmdb_id=500)

    assert "Show Title EN" in result
    assert "Show Title KO" not in result


# ---------------------------------------------------------------------------
# Section C — refresh_data
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_refresh_data_success(client: AnidbClient, session: AsyncSession) -> None:
    """Successful refresh populates anidb_titles and anidb_mappings tables.

    Korean titles are excluded because 'ko' is not in title_languages.
    """
    xml_gz = _make_gzip_xml(_minimal_titles_xml())
    mappings_json = _minimal_mappings_json()

    title_resp = _make_response(200, xml_gz)
    title_resp.headers = httpx.Headers({"content-type": "application/octet-stream"})  # type: ignore[assignment]
    # Re-create with correct content
    title_resp = httpx.Response(
        status_code=200,
        content=xml_gz,
        request=httpx.Request("GET", "https://anidb.net/api/anime-titles.xml.gz"),
    )
    mapping_resp = httpx.Response(
        status_code=200,
        content=json.dumps(mappings_json).encode(),
        headers={"content-type": "application/json"},
        request=httpx.Request("GET", "https://fribb.example.com/anime-list.json"),
    )

    cfg = _make_anidb_cfg(title_languages=["x-jat", "en", "ja"])
    mock_settings = _make_mock_settings(cfg)

    # Patch the module-level httpx.AsyncClient used in refresh_data
    # refresh_data uses two separate `async with httpx.AsyncClient(...) as client:` calls
    # We set up a side_effect sequence so first call returns title dump, second returns mappings
    mock_context_title = AsyncMock()
    mock_context_title.__aenter__ = AsyncMock(return_value=AsyncMock(
        get=AsyncMock(return_value=title_resp),
        is_success=True,
    ))
    mock_context_title.__aexit__ = AsyncMock(return_value=False)

    mock_context_mapping = AsyncMock()
    mock_context_mapping.__aenter__ = AsyncMock(return_value=AsyncMock(
        get=AsyncMock(return_value=mapping_resp),
        is_success=True,
    ))
    mock_context_mapping.__aexit__ = AsyncMock(return_value=False)

    call_count = 0

    def _mock_client_factory(*args, **kwargs) -> AsyncMock:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return mock_context_title
        return mock_context_mapping

    noop_breaker = _make_noop_breaker()

    with patch("src.services.anidb.settings", mock_settings), \
         patch("src.services.anidb.get_circuit_breaker", return_value=noop_breaker), \
         patch("src.services.anidb.httpx.AsyncClient", side_effect=_mock_client_factory):
        await client.refresh_data(session)
        await session.flush()

    from sqlalchemy import select, func
    title_count = (await session.execute(select(func.count()).select_from(AnidbTitle))).scalar()
    mapping_count = (await session.execute(select(func.count()).select_from(AnidbMapping))).scalar()

    # Titles expected: from aid=1: x-jat/main, en/official, ja/official, x-jat/syn → 4
    # Korean (ko) is excluded. From aid=2: x-jat/main, en/official → 2. Total = 6
    assert title_count == 6
    # Mappings: 3 from _minimal_mappings_json
    assert mapping_count == 3


@pytest.mark.asyncio
async def test_refresh_data_disabled(client: AnidbClient, session: AsyncSession) -> None:
    """When anidb.enabled=False, refresh_data returns early with no DB changes."""
    cfg = _make_anidb_cfg(enabled=False)
    mock_settings = _make_mock_settings(cfg)

    http_called = False

    def _should_not_call(*args, **kwargs):
        nonlocal http_called
        http_called = True
        return MagicMock()

    with patch("src.services.anidb.settings", mock_settings), \
         patch("src.services.anidb.httpx.AsyncClient", side_effect=_should_not_call):
        await client.refresh_data(session)

    assert not http_called

    from sqlalchemy import select, func
    title_count = (await session.execute(select(func.count()).select_from(AnidbTitle))).scalar()
    assert title_count == 0


@pytest.mark.asyncio
async def test_refresh_data_title_dump_failure(
    client: AnidbClient, session: AsyncSession
) -> None:
    """An HTTP 500 on the title dump returns without crashing or touching the DB."""
    fail_resp = httpx.Response(
        status_code=500,
        content=b"Internal Server Error",
        request=httpx.Request("GET", "https://anidb.net/api/anime-titles.xml.gz"),
    )

    mock_http_client = AsyncMock()
    mock_http_client.__aenter__ = AsyncMock(return_value=AsyncMock(
        get=AsyncMock(return_value=fail_resp),
    ))
    mock_http_client.__aexit__ = AsyncMock(return_value=False)

    cfg = _make_anidb_cfg()
    mock_settings = _make_mock_settings(cfg)
    noop_breaker = _make_noop_breaker()

    with patch("src.services.anidb.settings", mock_settings), \
         patch("src.services.anidb.get_circuit_breaker", return_value=noop_breaker), \
         patch("src.services.anidb.httpx.AsyncClient", return_value=mock_http_client):
        await client.refresh_data(session)  # Must not raise

    from sqlalchemy import select, func
    title_count = (await session.execute(select(func.count()).select_from(AnidbTitle))).scalar()
    assert title_count == 0


@pytest.mark.asyncio
async def test_refresh_data_replaces_old_data(
    client: AnidbClient, session: AsyncSession
) -> None:
    """Calling refresh_data a second time deletes old rows and inserts fresh ones."""
    now = datetime.now(timezone.utc)
    # Pre-populate with stale data
    session.add_all([
        AnidbTitle(anidb_id=99, title="Old Title", title_type="main", language="en", fetched_at=now),
        AnidbMapping(anidb_id=99, tmdb_id=999, tmdb_season=1, fetched_at=now),
    ])
    await session.flush()

    xml_gz = _make_gzip_xml(_minimal_titles_xml())
    mappings_json = _minimal_mappings_json()

    title_resp = httpx.Response(
        status_code=200,
        content=xml_gz,
        request=httpx.Request("GET", "https://anidb.net/api/anime-titles.xml.gz"),
    )
    mapping_resp = httpx.Response(
        status_code=200,
        content=json.dumps(mappings_json).encode(),
        headers={"content-type": "application/json"},
        request=httpx.Request("GET", "https://fribb.example.com/anime-list.json"),
    )

    cfg = _make_anidb_cfg(title_languages=["x-jat", "en", "ja"])
    mock_settings = _make_mock_settings(cfg)

    call_count = 0

    def _mock_client_factory(*args, **kwargs) -> AsyncMock:
        nonlocal call_count
        call_count += 1
        mock_ctx = AsyncMock()
        resp = title_resp if call_count == 1 else mapping_resp
        mock_ctx.__aenter__ = AsyncMock(return_value=AsyncMock(
            get=AsyncMock(return_value=resp),
        ))
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        return mock_ctx

    noop_breaker = _make_noop_breaker()

    with patch("src.services.anidb.settings", mock_settings), \
         patch("src.services.anidb.get_circuit_breaker", return_value=noop_breaker), \
         patch("src.services.anidb.httpx.AsyncClient", side_effect=_mock_client_factory):
        await client.refresh_data(session)
        await session.flush()

    from sqlalchemy import select
    # The old title (anidb_id=99) must be gone
    old_titles = (await session.execute(
        select(AnidbTitle).where(AnidbTitle.anidb_id == 99)
    )).scalars().all()
    assert old_titles == []

    # New titles from the XML must be present (anidb_id 1 and 2)
    new_titles = (await session.execute(
        select(AnidbTitle).where(AnidbTitle.anidb_id == 1)
    )).scalars().all()
    assert len(new_titles) > 0

    # Old mapping must be gone
    old_mappings = (await session.execute(
        select(AnidbMapping).where(AnidbMapping.anidb_id == 99)
    )).scalars().all()
    assert old_mappings == []


# ---------------------------------------------------------------------------
# Section D — get_episode_count_for_tmdb_season
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_episode_count_for_tmdb_season_cached(
    client: AnidbClient, session: AsyncSession
) -> None:
    """When episode_count is already populated, returns it without any HTTP call."""
    now = datetime.now(timezone.utc)
    session.add(AnidbMapping(
        anidb_id=60,
        tmdb_id=600,
        tmdb_season=1,
        episode_count=24,
        fetched_at=now,
    ))
    await session.flush()

    cfg = _make_anidb_cfg(api_enabled=True, client_name="testclient")
    mock_settings = _make_mock_settings(cfg)

    http_called = False

    async def _should_not_be_called(anidb_id: int) -> int | None:
        nonlocal http_called
        http_called = True
        return None

    with patch("src.services.anidb.settings", mock_settings):
        client._fetch_episode_count = _should_not_be_called  # type: ignore[method-assign]
        result = await client.get_episode_count_for_tmdb_season(session, tmdb_id=600, season=1)

    assert result == 24
    assert not http_called


@pytest.mark.asyncio
async def test_get_episode_count_for_tmdb_season_no_mapping(
    client: AnidbClient, session: AsyncSession
) -> None:
    """No mapping for the given tmdb_id/season returns None."""
    cfg = _make_anidb_cfg()
    mock_settings = _make_mock_settings(cfg)

    with patch("src.services.anidb.settings", mock_settings):
        result = await client.get_episode_count_for_tmdb_season(session, tmdb_id=700, season=1)

    assert result is None


@pytest.mark.asyncio
async def test_get_episode_count_for_tmdb_season_api_disabled(
    client: AnidbClient, session: AsyncSession
) -> None:
    """When api_enabled=False, returns None (cannot fetch uncached data)."""
    now = datetime.now(timezone.utc)
    session.add(AnidbMapping(
        anidb_id=70,
        tmdb_id=700,
        tmdb_season=1,
        episode_count=None,  # not cached
        fetched_at=now,
    ))
    await session.flush()

    cfg = _make_anidb_cfg(api_enabled=False, client_name="")
    mock_settings = _make_mock_settings(cfg)

    with patch("src.services.anidb.settings", mock_settings):
        result = await client.get_episode_count_for_tmdb_season(session, tmdb_id=700, season=1)

    assert result is None


@pytest.mark.asyncio
async def test_get_episode_count_for_tmdb_season_api_fetch(
    client: AnidbClient, session: AsyncSession
) -> None:
    """When api_enabled=True and episode_count is None, fetches from AniDB HTTP API and caches."""
    now = datetime.now(timezone.utc)
    session.add(AnidbMapping(
        anidb_id=80,
        tmdb_id=800,
        tmdb_season=1,
        episode_count=None,
        fetched_at=now,
    ))
    await session.flush()

    cfg = _make_anidb_cfg(api_enabled=True, client_name="testclient")
    mock_settings = _make_mock_settings(cfg)

    api_xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<anime id="80">
  <episodecount>24</episodecount>
</anime>"""
    api_resp = httpx.Response(
        status_code=200,
        content=api_xml,
        headers={"content-type": "text/xml"},
        request=httpx.Request("GET", "http://api.anidb.net:9001/httpapi"),
    )

    # Patch _fetch_episode_count to avoid the pooled client and asyncio.sleep
    async def _mock_fetch(anidb_id: int) -> int | None:
        assert anidb_id == 80
        return 24

    noop_breaker = _make_noop_breaker()

    with patch("src.services.anidb.settings", mock_settings), \
         patch("src.services.anidb.get_circuit_breaker", return_value=noop_breaker):
        client._fetch_episode_count = _mock_fetch  # type: ignore[method-assign]
        result = await client.get_episode_count_for_tmdb_season(session, tmdb_id=800, season=1)

    assert result == 24

    # Verify the mapping was updated in the DB
    from sqlalchemy import select
    updated = (await session.execute(
        select(AnidbMapping).where(AnidbMapping.anidb_id == 80)
    )).scalar_one()
    assert updated.episode_count == 24


@pytest.mark.asyncio
async def test_get_episode_count_multiple_entries_sum(
    client: AnidbClient, session: AsyncSession
) -> None:
    """Two mappings for the same tmdb_id/season have their counts summed."""
    now = datetime.now(timezone.utc)
    session.add_all([
        AnidbMapping(anidb_id=90, tmdb_id=900, tmdb_season=1, episode_count=12, fetched_at=now),
        AnidbMapping(anidb_id=91, tmdb_id=900, tmdb_season=1, episode_count=13, fetched_at=now),
    ])
    await session.flush()

    cfg = _make_anidb_cfg(api_enabled=False)
    mock_settings = _make_mock_settings(cfg)

    with patch("src.services.anidb.settings", mock_settings):
        result = await client.get_episode_count_for_tmdb_season(session, tmdb_id=900, season=1)

    assert result == 25


# ---------------------------------------------------------------------------
# Additional edge cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_episode_count_disabled(
    client: AnidbClient, session: AsyncSession
) -> None:
    """When anidb.enabled=False, get_episode_count_for_tmdb_season returns None."""
    now = datetime.now(timezone.utc)
    session.add(AnidbMapping(
        anidb_id=95,
        tmdb_id=950,
        tmdb_season=1,
        episode_count=12,
        fetched_at=now,
    ))
    await session.flush()

    cfg = _make_anidb_cfg(enabled=False)
    mock_settings = _make_mock_settings(cfg)

    with patch("src.services.anidb.settings", mock_settings):
        result = await client.get_episode_count_for_tmdb_season(session, tmdb_id=950, season=1)

    assert result is None


@pytest.mark.asyncio
async def test_refresh_data_circuit_open_skips(
    client: AnidbClient, session: AsyncSession
) -> None:
    """When the circuit breaker is OPEN, refresh_data returns without making HTTP calls."""
    from src.services.http_client import CircuitBreaker

    open_breaker = CircuitBreaker("anidb_test_open", failure_threshold=1, recovery_timeout=9999.0)
    await open_breaker.record_failure()  # Opens the circuit

    cfg = _make_anidb_cfg()
    mock_settings = _make_mock_settings(cfg)

    http_called = False

    def _should_not_call(*args, **kwargs):
        nonlocal http_called
        http_called = True
        return MagicMock()

    with patch("src.services.anidb.settings", mock_settings), \
         patch("src.services.anidb.get_circuit_breaker", return_value=open_breaker), \
         patch("src.services.anidb.httpx.AsyncClient", side_effect=_should_not_call):
        await client.refresh_data(session)

    assert not http_called


@pytest.mark.asyncio
async def test_refresh_data_mappings_failure(
    client: AnidbClient, session: AsyncSession
) -> None:
    """When mappings URL returns 500, refresh_data returns without writing any rows."""
    xml_gz = _make_gzip_xml(_minimal_titles_xml())

    title_resp = httpx.Response(
        status_code=200,
        content=xml_gz,
        request=httpx.Request("GET", "https://anidb.net/api/anime-titles.xml.gz"),
    )
    fail_resp = httpx.Response(
        status_code=500,
        content=b"error",
        request=httpx.Request("GET", "https://fribb.example.com/anime-list.json"),
    )

    cfg = _make_anidb_cfg(title_languages=["x-jat", "en", "ja"])
    mock_settings = _make_mock_settings(cfg)
    noop_breaker = _make_noop_breaker()

    call_count = 0

    def _mock_client_factory(*args, **kwargs) -> AsyncMock:
        nonlocal call_count
        call_count += 1
        mock_ctx = AsyncMock()
        resp = title_resp if call_count == 1 else fail_resp
        mock_ctx.__aenter__ = AsyncMock(return_value=AsyncMock(
            get=AsyncMock(return_value=resp),
        ))
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        return mock_ctx

    with patch("src.services.anidb.settings", mock_settings), \
         patch("src.services.anidb.get_circuit_breaker", return_value=noop_breaker), \
         patch("src.services.anidb.httpx.AsyncClient", side_effect=_mock_client_factory):
        await client.refresh_data(session)

    # Neither table should have data since mappings failed
    from sqlalchemy import select, func
    title_count = (await session.execute(select(func.count()).select_from(AnidbTitle))).scalar()
    mapping_count = (await session.execute(select(func.count()).select_from(AnidbMapping))).scalar()

    # Titles are parsed before mappings fail, but the whole function returns early on
    # mappings failure before executing DELETE + INSERT
    assert title_count == 0
    assert mapping_count == 0


@pytest.mark.asyncio
async def test_is_data_fresh_naive_datetime(
    client: AnidbClient, session: AsyncSession
) -> None:
    """A naive (no-tzinfo) fetched_at is treated as UTC, not as a timezone error."""
    # SQLite returns naive datetimes; the service must handle them
    now_naive = datetime.now()  # no tzinfo
    session.add(AnidbTitle(
        anidb_id=1,
        title="Test Anime",
        title_type="main",
        language="en",
        fetched_at=now_naive,
    ))
    await session.flush()

    cfg = _make_anidb_cfg(refresh_hours=168)
    mock_settings = _make_mock_settings(cfg)

    with patch("src.services.anidb.settings", mock_settings):
        # Should not raise; just return True (within window)
        result = await client.is_data_fresh(session)

    assert result is True


@pytest.mark.asyncio
async def test_get_titles_priority_order_all_types(
    client: AnidbClient, session: AsyncSession
) -> None:
    """All four title types are sorted correctly: main(0) < official(1) < syn(2) < short(3)."""
    now = datetime.now(timezone.utc)

    session.add(AnidbMapping(anidb_id=100, tmdb_id=1000, tmdb_season=1, fetched_at=now))
    session.add_all([
        AnidbTitle(anidb_id=100, title="Short Abbrev", title_type="short", language="en", fetched_at=now),
        AnidbTitle(anidb_id=100, title="Synonym Form", title_type="syn", language="en", fetched_at=now),
        AnidbTitle(anidb_id=100, title="Official EN", title_type="official", language="en", fetched_at=now),
        AnidbTitle(anidb_id=100, title="Main Romaji", title_type="main", language="x-jat", fetched_at=now),
    ])
    await session.flush()

    cfg = _make_anidb_cfg()
    mock_settings = _make_mock_settings(cfg)

    with patch("src.services.anidb.settings", mock_settings):
        result = await client.get_titles_for_tmdb_id(session, tmdb_id=1000)

    assert len(result) == 4
    assert result[0] == "Main Romaji"
    assert result[1] == "Official EN"
    assert result[2] == "Synonym Form"
    assert result[3] == "Short Abbrev"


@pytest.mark.asyncio
async def test_get_episode_count_only_none_counts_no_api(
    client: AnidbClient, session: AsyncSession
) -> None:
    """When all episode_counts are None and api_enabled=False, returns None (not 0)."""
    now = datetime.now(timezone.utc)
    session.add(AnidbMapping(
        anidb_id=110,
        tmdb_id=1100,
        tmdb_season=1,
        episode_count=None,
        fetched_at=now,
    ))
    await session.flush()

    cfg = _make_anidb_cfg(api_enabled=False)
    mock_settings = _make_mock_settings(cfg)

    with patch("src.services.anidb.settings", mock_settings):
        result = await client.get_episode_count_for_tmdb_season(session, tmdb_id=1100, season=1)

    assert result is None


# ---------------------------------------------------------------------------
# Section F — get_episode_counts_up_to_season
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_episode_counts_up_to_season_multi(
    client: AnidbClient, session: AsyncSession
) -> None:
    """Returns counts for all seasons up to the target, keyed by season number."""
    now = datetime.now(timezone.utc)
    session.add_all([
        AnidbMapping(anidb_id=200, tmdb_id=2000, tmdb_season=1, episode_count=12, fetched_at=now),
        AnidbMapping(anidb_id=201, tmdb_id=2000, tmdb_season=2, episode_count=24, fetched_at=now),
        AnidbMapping(anidb_id=202, tmdb_id=2000, tmdb_season=3, episode_count=13, fetched_at=now),
    ])
    await session.flush()

    cfg = _make_anidb_cfg()
    mock_settings = _make_mock_settings(cfg)

    with patch("src.services.anidb.settings", mock_settings):
        result = await client.get_episode_counts_up_to_season(session, tmdb_id=2000, up_to_season=2)

    # Should include seasons 1 and 2, but NOT season 3
    assert result == {1: 12, 2: 24}


@pytest.mark.asyncio
async def test_get_episode_counts_up_to_season_partial(
    client: AnidbClient, session: AsyncSession
) -> None:
    """Seasons without cached episode_count and no API return partial dict."""
    now = datetime.now(timezone.utc)
    session.add_all([
        AnidbMapping(anidb_id=210, tmdb_id=2100, tmdb_season=1, episode_count=12, fetched_at=now),
        AnidbMapping(anidb_id=211, tmdb_id=2100, tmdb_season=2, episode_count=None, fetched_at=now),
    ])
    await session.flush()

    cfg = _make_anidb_cfg(api_enabled=False)
    mock_settings = _make_mock_settings(cfg)

    with patch("src.services.anidb.settings", mock_settings):
        result = await client.get_episode_counts_up_to_season(session, tmdb_id=2100, up_to_season=2)

    # Season 1 has a cached count; season 2 has None and API is disabled → omitted
    assert result == {1: 12}


@pytest.mark.asyncio
async def test_get_episode_counts_up_to_season_no_mappings(
    client: AnidbClient, session: AsyncSession
) -> None:
    """No AniDB mappings for the TMDB ID returns empty dict."""
    cfg = _make_anidb_cfg()
    mock_settings = _make_mock_settings(cfg)

    with patch("src.services.anidb.settings", mock_settings):
        result = await client.get_episode_counts_up_to_season(session, tmdb_id=9999, up_to_season=3)

    assert result == {}


@pytest.mark.asyncio
async def test_get_episode_counts_up_to_season_sums_parts(
    client: AnidbClient, session: AsyncSession
) -> None:
    """Multiple AniDB entries for the same season are summed (e.g. Part 1 + Part 2)."""
    now = datetime.now(timezone.utc)
    session.add_all([
        AnidbMapping(anidb_id=220, tmdb_id=2200, tmdb_season=1, episode_count=12, fetched_at=now),
        AnidbMapping(anidb_id=221, tmdb_id=2200, tmdb_season=1, episode_count=13, fetched_at=now),
    ])
    await session.flush()

    cfg = _make_anidb_cfg()
    mock_settings = _make_mock_settings(cfg)

    with patch("src.services.anidb.settings", mock_settings):
        result = await client.get_episode_counts_up_to_season(session, tmdb_id=2200, up_to_season=1)

    assert result == {1: 25}
