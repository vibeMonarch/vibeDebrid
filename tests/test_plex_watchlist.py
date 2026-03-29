"""Tests for src/core/plex_watchlist.py and src/services/plex.py (get_watchlist).

Coverage:
  Part 1 — PlexClient.get_watchlist() parsing (HTTP mock via patch("httpx.AsyncClient"))
    - Movies parsed with tmdb:// and imdb:// GUIDs
    - TV shows parsed correctly
    - Items without GUIDs are included (tmdb_id/imdb_id=None)
    - Pagination: two pages fetched and concatenated
    - Empty watchlist (totalSize=0, no Metadata)
    - 401 response returns empty list, no exception
    - Timeout returns empty list, no exception
    - 500 response returns empty list, no exception
    - Unknown/unsupported media type is silently skipped

  Part 2 — sync_watchlist() business logic (plex_client mocked via AsyncMock)
    - Disabled (watchlist_sync_enabled=False) returns zeroed stats immediately
    - No token returns zeroed stats immediately
    - Empty watchlist returns total=0 stats
    - New movie added with correct field values
    - New TV show added with correct field values (season=1, is_season_pack=True)
    - TV show triggers set_subscription with correct arguments
    - Skip movie already in queue by tmdb_id
    - Skip movie already in queue by imdb_id only (tmdb_id differs)
    - Item in every possible queue state is skipped (not re-added)
    - Missing imdb_id resolved via TMDB when tmdb_id is present
    - TMDB resolution failure does not block item insertion
    - IntegrityError on flush: item skipped, remainder continues
    - Multiple items: mix of new and existing, counts verified
    - Within-batch dedup: second item with same tmdb_id is skipped
    - SSE event published for each successfully added item
    - show with no tmdb_id: monitoring not attempted (no crash)

asyncio_mode = "auto" (configured in pyproject.toml).

Mocking strategy
----------------
get_watchlist() opens its own inline httpx.AsyncClient (not via _build_client),
so all Part 1 tests inject mocks via patch("httpx.AsyncClient").
sync_watchlist() mocks are applied via patch() at the module import path.
"""

from __future__ import annotations

from datetime import UTC
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.media_item import MediaItem, MediaType, QueueState
from src.services.plex import PlexClient, WatchlistItem

# ---------------------------------------------------------------------------
# Part 1 helpers — get_watchlist() HTTP-level tests
# ---------------------------------------------------------------------------


def _make_watchlist_page(
    metadata: list[dict[str, Any]],
    total_size: int | None = None,
) -> dict[str, Any]:
    """Build a Plex watchlist API response envelope."""
    if total_size is None:
        total_size = len(metadata)
    return {
        "MediaContainer": {
            "totalSize": total_size,
            "Metadata": metadata,
        }
    }


def _movie_entry(
    title: str = "Test Movie",
    year: int = 2024,
    tmdb_id: str | None = "123456",
    imdb_id: str | None = "tt1234567",
) -> dict[str, Any]:
    """Build a Plex watchlist metadata entry for a movie."""
    guids: list[dict[str, str]] = []
    if tmdb_id:
        guids.append({"id": f"tmdb://{tmdb_id}"})
    if imdb_id:
        guids.append({"id": f"imdb://{imdb_id}"})
    return {"title": title, "year": year, "type": "movie", "Guid": guids}


def _show_entry(
    title: str = "Test Show",
    year: int = 2022,
    tmdb_id: str | None = "789",
    imdb_id: str | None = "tt9876543",
) -> dict[str, Any]:
    """Build a Plex watchlist metadata entry for a TV show."""
    guids: list[dict[str, str]] = []
    if tmdb_id:
        guids.append({"id": f"tmdb://{tmdb_id}"})
    if imdb_id:
        guids.append({"id": f"imdb://{imdb_id}"})
    return {"title": title, "year": year, "type": "show", "Guid": guids}


# ---------------------------------------------------------------------------
# Part 1 — get_watchlist() tests
# ---------------------------------------------------------------------------


async def test_get_watchlist_parses_movie(monkeypatch: pytest.MonkeyPatch) -> None:
    """get_watchlist correctly parses a movie entry with tmdb:// and imdb:// GUIDs."""
    entry = _movie_entry(title="Dune: Part Two", year=2024, tmdb_id="693134", imdb_id="tt15239678")
    page = _make_watchlist_page([entry])

    mock_settings = MagicMock()
    mock_settings.plex.token = "tok"
    monkeypatch.setattr("src.services.plex.settings", mock_settings)

    with patch("httpx.AsyncClient") as mock_cls:
        mock_client_instance = AsyncMock()
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=False)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.is_success = True
        mock_resp.json.return_value = page
        mock_client_instance.get = AsyncMock(return_value=mock_resp)
        mock_cls.return_value = mock_client_instance

        client = PlexClient()
        result = await client.get_watchlist()

    assert len(result) == 1
    item = result[0]
    assert item.title == "Dune: Part Two"
    assert item.year == 2024
    assert item.media_type == "movie"
    assert item.tmdb_id == "693134"
    assert item.imdb_id == "tt15239678"


async def test_get_watchlist_parses_show(monkeypatch: pytest.MonkeyPatch) -> None:
    """get_watchlist correctly maps type='show' entries to media_type='show'."""
    entry = _show_entry(title="The Bear", year=2022, tmdb_id="136315", imdb_id="tt14452776")
    page = _make_watchlist_page([entry])

    mock_settings = MagicMock()
    mock_settings.plex.token = "tok"
    monkeypatch.setattr("src.services.plex.settings", mock_settings)

    with patch("httpx.AsyncClient") as mock_cls:
        mock_client_instance = AsyncMock()
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=False)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.is_success = True
        mock_resp.json.return_value = page
        mock_client_instance.get = AsyncMock(return_value=mock_resp)
        mock_cls.return_value = mock_client_instance

        client = PlexClient()
        result = await client.get_watchlist()

    assert len(result) == 1
    item = result[0]
    assert item.title == "The Bear"
    assert item.year == 2022
    assert item.media_type == "show"
    assert item.tmdb_id == "136315"
    assert item.imdb_id == "tt14452776"


async def test_get_watchlist_item_without_guids(monkeypatch: pytest.MonkeyPatch) -> None:
    """Items with an empty Guid array are included with tmdb_id=None, imdb_id=None."""
    entry: dict[str, Any] = {"title": "Mystery Movie", "year": 2023, "type": "movie", "Guid": []}
    page = _make_watchlist_page([entry])

    mock_settings = MagicMock()
    mock_settings.plex.token = "tok"
    monkeypatch.setattr("src.services.plex.settings", mock_settings)

    with patch("httpx.AsyncClient") as mock_cls:
        mock_client_instance = AsyncMock()
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=False)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.is_success = True
        mock_resp.json.return_value = page
        mock_client_instance.get = AsyncMock(return_value=mock_resp)
        mock_cls.return_value = mock_client_instance

        client = PlexClient()
        result = await client.get_watchlist()

    assert len(result) == 1
    item = result[0]
    assert item.title == "Mystery Movie"
    assert item.media_type == "movie"
    assert item.tmdb_id is None
    assert item.imdb_id is None


async def test_get_watchlist_empty_response(monkeypatch: pytest.MonkeyPatch) -> None:
    """get_watchlist returns [] when totalSize=0 and Metadata is empty."""
    page = _make_watchlist_page([], total_size=0)

    mock_settings = MagicMock()
    mock_settings.plex.token = "tok"
    monkeypatch.setattr("src.services.plex.settings", mock_settings)

    with patch("httpx.AsyncClient") as mock_cls:
        mock_client_instance = AsyncMock()
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=False)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.is_success = True
        mock_resp.json.return_value = page
        mock_client_instance.get = AsyncMock(return_value=mock_resp)
        mock_cls.return_value = mock_client_instance

        client = PlexClient()
        result = await client.get_watchlist()

    assert result == []


async def test_get_watchlist_401_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """get_watchlist returns [] on 401 auth failure without raising an exception."""
    mock_settings = MagicMock()
    mock_settings.plex.token = "expired-token"
    monkeypatch.setattr("src.services.plex.settings", mock_settings)

    with patch("httpx.AsyncClient") as mock_cls:
        mock_client_instance = AsyncMock()
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=False)

        mock_resp = MagicMock()
        mock_resp.status_code = 401
        mock_resp.is_success = False
        mock_client_instance.get = AsyncMock(return_value=mock_resp)
        mock_cls.return_value = mock_client_instance

        client = PlexClient()
        result = await client.get_watchlist()

    assert result == []


async def test_get_watchlist_500_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """get_watchlist returns [] on 500 server error without raising an exception."""
    mock_settings = MagicMock()
    mock_settings.plex.token = "tok"
    monkeypatch.setattr("src.services.plex.settings", mock_settings)

    with patch("httpx.AsyncClient") as mock_cls:
        mock_client_instance = AsyncMock()
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=False)

        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.is_success = False
        mock_resp.text = "Internal Server Error"
        mock_client_instance.get = AsyncMock(return_value=mock_resp)
        mock_cls.return_value = mock_client_instance

        client = PlexClient()
        result = await client.get_watchlist()

    assert result == []


async def test_get_watchlist_timeout_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """get_watchlist returns [] when a TimeoutException is raised."""
    mock_settings = MagicMock()
    mock_settings.plex.token = "tok"
    monkeypatch.setattr("src.services.plex.settings", mock_settings)

    with patch("httpx.AsyncClient") as mock_cls:
        mock_client_instance = AsyncMock()
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=False)
        mock_client_instance.get = AsyncMock(
            side_effect=httpx.TimeoutException("timed out", request=None)
        )
        mock_cls.return_value = mock_client_instance

        client = PlexClient()
        result = await client.get_watchlist()

    assert result == []


async def test_get_watchlist_skips_unknown_media_type(monkeypatch: pytest.MonkeyPatch) -> None:
    """Items with a type other than 'movie' or 'show' are silently skipped."""
    # 'track' is a music type — should be skipped
    entries: list[dict[str, Any]] = [
        {"title": "Some Album Track", "year": 2024, "type": "track", "Guid": [{"id": "tmdb://999"}]},
        _movie_entry(title="A Real Movie"),
    ]
    page = _make_watchlist_page(entries, total_size=2)

    mock_settings = MagicMock()
    mock_settings.plex.token = "tok"
    monkeypatch.setattr("src.services.plex.settings", mock_settings)

    with patch("httpx.AsyncClient") as mock_cls:
        mock_client_instance = AsyncMock()
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=False)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.is_success = True
        mock_resp.json.return_value = page
        mock_client_instance.get = AsyncMock(return_value=mock_resp)
        mock_cls.return_value = mock_client_instance

        client = PlexClient()
        result = await client.get_watchlist()

    # Only the movie should be in the result
    assert len(result) == 1
    assert result[0].title == "A Real Movie"
    assert result[0].media_type == "movie"


async def test_get_watchlist_pagination(monkeypatch: pytest.MonkeyPatch) -> None:
    """get_watchlist fetches all pages when totalSize > page_size."""
    # Page 1: 2 entries, total reported as 3
    page1_entries = [_movie_entry(title="Movie 1"), _movie_entry(title="Movie 2")]
    page1 = _make_watchlist_page(page1_entries, total_size=3)

    # Page 2: 1 entry
    page2_entries = [_show_entry(title="Show 1")]
    page2 = _make_watchlist_page(page2_entries, total_size=3)

    mock_settings = MagicMock()
    mock_settings.plex.token = "tok"
    monkeypatch.setattr("src.services.plex.settings", mock_settings)

    call_count = 0
    pages = [page1, page2]

    with patch("httpx.AsyncClient") as mock_cls:
        mock_client_instance = AsyncMock()
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=False)

        async def _mock_get(url: str, params: dict | None = None) -> MagicMock:
            nonlocal call_count
            resp = MagicMock()
            resp.status_code = 200
            resp.is_success = True
            resp.json.return_value = pages[call_count]
            call_count += 1
            return resp

        mock_client_instance.get = _mock_get
        mock_cls.return_value = mock_client_instance

        client = PlexClient()
        result = await client.get_watchlist()

    assert len(result) == 3
    titles = [item.title for item in result]
    assert "Movie 1" in titles
    assert "Movie 2" in titles
    assert "Show 1" in titles
    assert call_count == 2


# ---------------------------------------------------------------------------
# Part 2 helpers — sync_watchlist() tests
# ---------------------------------------------------------------------------


def _make_plex_settings(
    token: str = "valid-token",
    watchlist_sync_enabled: bool = True,
) -> MagicMock:
    """Build a mock settings object for sync_watchlist tests."""
    mock_settings = MagicMock()
    mock_settings.plex.token = token
    mock_settings.plex.watchlist_sync_enabled = watchlist_sync_enabled
    return mock_settings


def _make_watchlist_item(
    title: str = "Test Movie",
    year: int = 2024,
    media_type: str = "movie",
    tmdb_id: str | None = "111111",
    imdb_id: str | None = "tt1111111",
) -> WatchlistItem:
    """Build a WatchlistItem with sensible defaults."""
    return WatchlistItem(
        title=title,
        year=year,
        media_type=media_type,
        tmdb_id=tmdb_id,
        imdb_id=imdb_id,
    )


# ---------------------------------------------------------------------------
# Part 2 — sync_watchlist() tests
# ---------------------------------------------------------------------------


async def test_sync_disabled_returns_zeroed_stats(session: AsyncSession) -> None:
    """sync_watchlist returns all-zero stats immediately when sync is disabled."""
    with patch("src.core.plex_watchlist.settings") as mock_settings:
        mock_settings.plex.token = "valid-token"
        mock_settings.plex.watchlist_sync_enabled = False

        from src.core.plex_watchlist import sync_watchlist
        stats = await sync_watchlist(session)

    assert stats == {"total": 0, "added": 0, "skipped": 0, "errors": 0}


async def test_sync_no_token_returns_zeroed_stats(session: AsyncSession) -> None:
    """sync_watchlist returns all-zero stats immediately when token is empty."""
    with patch("src.core.plex_watchlist.settings") as mock_settings:
        mock_settings.plex.token = ""
        mock_settings.plex.watchlist_sync_enabled = True

        from src.core.plex_watchlist import sync_watchlist
        stats = await sync_watchlist(session)

    assert stats == {"total": 0, "added": 0, "skipped": 0, "errors": 0}


async def test_sync_empty_watchlist(session: AsyncSession) -> None:
    """sync_watchlist returns total=0 when get_watchlist returns an empty list."""
    with (
        patch("src.core.plex_watchlist.settings") as mock_settings,
        patch("src.core.plex_watchlist.plex_client") as mock_plex,
    ):
        mock_settings.plex.token = "tok"
        mock_settings.plex.watchlist_sync_enabled = True
        mock_plex.get_watchlist = AsyncMock(return_value=[])

        from src.core.plex_watchlist import sync_watchlist
        stats = await sync_watchlist(session)

    assert stats["total"] == 0
    assert stats["added"] == 0
    assert stats["skipped"] == 0
    assert stats["errors"] == 0


async def test_sync_adds_new_movie(session: AsyncSession) -> None:
    """sync_watchlist creates a MediaItem with correct fields for a new movie."""
    movie = _make_watchlist_item(
        title="Dune: Part Two",
        year=2024,
        media_type="movie",
        tmdb_id="693134",
        imdb_id="tt15239678",
    )

    with (
        patch("src.core.plex_watchlist.settings") as mock_settings,
        patch("src.core.plex_watchlist.plex_client") as mock_plex,
        patch("src.core.plex_watchlist.tmdb_client") as mock_tmdb,
        patch("src.core.plex_watchlist.event_bus", create=True),
    ):
        mock_settings.plex.token = "tok"
        mock_settings.plex.watchlist_sync_enabled = True
        mock_plex.get_watchlist = AsyncMock(return_value=[movie])
        # movie already has both IDs so TMDB resolution is not needed
        mock_tmdb.get_external_ids = AsyncMock(return_value=None)

        # Stub out lazy imports inside sync_watchlist
        with (
            patch("src.core.event_bus.event_bus") as _eb,
            patch("src.core.event_bus.QueueEvent") as _qe,
        ):
            _eb.publish = MagicMock()
            from src.core.plex_watchlist import sync_watchlist
            stats = await sync_watchlist(session)

    assert stats["added"] == 1
    assert stats["skipped"] == 0
    assert stats["errors"] == 0
    assert stats["total"] == 1

    # Verify the item was written to the DB
    result = await session.execute(select(MediaItem).where(MediaItem.tmdb_id == "693134"))
    item = result.scalar_one_or_none()
    assert item is not None
    assert item.title == "Dune: Part Two"
    assert item.year == 2024
    assert item.media_type == MediaType.MOVIE
    assert item.state == QueueState.WANTED
    assert item.source == "plex_watchlist"
    assert item.imdb_id == "tt15239678"
    assert item.season is None
    assert item.episode is None
    assert item.is_season_pack is False
    assert item.retry_count == 0


async def test_sync_adds_new_show(session: AsyncSession) -> None:
    """sync_watchlist creates a MediaItem with season=1, is_season_pack=True for a show."""
    show = _make_watchlist_item(
        title="Severance",
        year=2022,
        media_type="show",
        tmdb_id="95396",
        imdb_id="tt11280740",
    )

    with (
        patch("src.core.plex_watchlist.settings") as mock_settings,
        patch("src.core.plex_watchlist.plex_client") as mock_plex,
        patch("src.core.plex_watchlist.tmdb_client") as mock_tmdb,
        patch("src.core.show_manager.show_manager") as mock_sm,
        patch("src.core.event_bus.event_bus"),
        patch("src.core.event_bus.QueueEvent"),
    ):
        mock_settings.plex.token = "tok"
        mock_settings.plex.watchlist_sync_enabled = True
        mock_plex.get_watchlist = AsyncMock(return_value=[show])
        mock_tmdb.get_external_ids = AsyncMock(return_value=None)
        mock_sm.set_subscription = AsyncMock()

        from src.core.plex_watchlist import sync_watchlist
        stats = await sync_watchlist(session)

    assert stats["added"] == 1

    result = await session.execute(select(MediaItem).where(MediaItem.tmdb_id == "95396"))
    item = result.scalar_one_or_none()
    assert item is not None
    assert item.media_type == MediaType.SHOW
    assert item.season == 1
    assert item.episode is None
    assert item.is_season_pack is True
    assert item.source == "plex_watchlist"
    assert item.state == QueueState.WANTED


async def test_sync_show_enables_monitoring(session: AsyncSession) -> None:
    """sync_watchlist calls show_manager.set_subscription for a new TV show."""
    show = _make_watchlist_item(
        title="The Bear",
        year=2022,
        media_type="show",
        tmdb_id="136315",
        imdb_id="tt14452776",
    )

    mock_set_subscription = AsyncMock()

    with (
        patch("src.core.plex_watchlist.settings") as mock_settings,
        patch("src.core.plex_watchlist.plex_client") as mock_plex,
        patch("src.core.plex_watchlist.tmdb_client") as mock_tmdb,
        patch("src.core.event_bus.event_bus"),
        patch("src.core.event_bus.QueueEvent"),
    ):
        mock_settings.plex.token = "tok"
        mock_settings.plex.watchlist_sync_enabled = True
        mock_plex.get_watchlist = AsyncMock(return_value=[show])
        mock_tmdb.get_external_ids = AsyncMock(return_value=None)

        # Patch the lazy import inside sync_watchlist
        with patch("src.core.show_manager.show_manager") as mock_sm:
            mock_sm.set_subscription = mock_set_subscription
            from src.core.plex_watchlist import sync_watchlist
            await sync_watchlist(session)

    mock_set_subscription.assert_awaited_once()
    call_kwargs = mock_set_subscription.call_args
    # set_subscription(session, tmdb_id=int, enabled=True, imdb_id=..., title=..., year=...)
    assert call_kwargs.kwargs.get("tmdb_id") == 136315
    assert call_kwargs.kwargs.get("enabled") is True
    assert call_kwargs.kwargs.get("imdb_id") == "tt14452776"
    assert call_kwargs.kwargs.get("title") == "The Bear"
    assert call_kwargs.kwargs.get("year") == 2022


async def test_sync_skips_item_existing_by_tmdb_id(session: AsyncSession) -> None:
    """sync_watchlist skips a watchlist item when its tmdb_id is already in the DB."""
    # Pre-insert an item with the same tmdb_id
    existing = MediaItem(
        title="Existing Movie",
        year=2020,
        media_type=MediaType.MOVIE,
        tmdb_id="111111",
        imdb_id="tt0000001",
        state=QueueState.COMPLETE,
        state_changed_at=__import__("datetime").datetime.now(__import__("datetime").timezone.utc),
        retry_count=0,
    )
    session.add(existing)
    await session.flush()

    watchlist_item = _make_watchlist_item(
        title="Same Movie Different Title",
        tmdb_id="111111",
        imdb_id="tt0000002",
    )

    with (
        patch("src.core.plex_watchlist.settings") as mock_settings,
        patch("src.core.plex_watchlist.plex_client") as mock_plex,
    ):
        mock_settings.plex.token = "tok"
        mock_settings.plex.watchlist_sync_enabled = True
        mock_plex.get_watchlist = AsyncMock(return_value=[watchlist_item])

        from src.core.plex_watchlist import sync_watchlist
        stats = await sync_watchlist(session)

    assert stats["added"] == 0
    assert stats["skipped"] == 1


async def test_sync_skips_item_existing_by_imdb_id(session: AsyncSession) -> None:
    """sync_watchlist skips a watchlist item matched by imdb_id when tmdb_id differs."""
    existing = MediaItem(
        title="Existing Movie",
        year=2020,
        media_type=MediaType.MOVIE,
        tmdb_id="999999",
        imdb_id="tt7777777",
        state=QueueState.WANTED,
        state_changed_at=__import__("datetime").datetime.now(__import__("datetime").timezone.utc),
        retry_count=0,
    )
    session.add(existing)
    await session.flush()

    # Same imdb_id, different tmdb_id
    watchlist_item = _make_watchlist_item(
        title="Same Movie",
        tmdb_id="888888",
        imdb_id="tt7777777",
    )

    with (
        patch("src.core.plex_watchlist.settings") as mock_settings,
        patch("src.core.plex_watchlist.plex_client") as mock_plex,
    ):
        mock_settings.plex.token = "tok"
        mock_settings.plex.watchlist_sync_enabled = True
        mock_plex.get_watchlist = AsyncMock(return_value=[watchlist_item])

        from src.core.plex_watchlist import sync_watchlist
        stats = await sync_watchlist(session)

    assert stats["added"] == 0
    assert stats["skipped"] == 1


@pytest.mark.parametrize(
    "state",
    [
        QueueState.WANTED,
        QueueState.SCRAPING,
        QueueState.ADDING,
        QueueState.CHECKING,
        QueueState.SLEEPING,
        QueueState.DORMANT,
        QueueState.COMPLETE,
        QueueState.DONE,
    ],
)
async def test_sync_skips_item_in_any_state(
    session: AsyncSession, state: QueueState
) -> None:
    """sync_watchlist skips watchlist items that already exist regardless of state."""
    from datetime import datetime

    existing = MediaItem(
        title="Already Queued",
        year=2023,
        media_type=MediaType.MOVIE,
        tmdb_id="200200",
        imdb_id=f"tt{state.value[:6].ljust(7, '0')}",
        state=state,
        state_changed_at=datetime.now(UTC),
        retry_count=0,
    )
    session.add(existing)
    await session.flush()

    watchlist_item = _make_watchlist_item(
        title="Already Queued",
        tmdb_id="200200",
        imdb_id=f"tt{state.value[:6].ljust(7, '0')}",
    )

    with (
        patch("src.core.plex_watchlist.settings") as mock_settings,
        patch("src.core.plex_watchlist.plex_client") as mock_plex,
    ):
        mock_settings.plex.token = "tok"
        mock_settings.plex.watchlist_sync_enabled = True
        mock_plex.get_watchlist = AsyncMock(return_value=[watchlist_item])

        from src.core.plex_watchlist import sync_watchlist
        stats = await sync_watchlist(session)

    assert stats["added"] == 0
    assert stats["skipped"] == 1


async def test_sync_resolves_missing_imdb_id_via_tmdb(session: AsyncSession) -> None:
    """When imdb_id is absent, sync_watchlist calls TMDB to resolve it."""
    from src.services.tmdb import TmdbExternalIds

    movie = _make_watchlist_item(
        title="Inception",
        year=2010,
        media_type="movie",
        tmdb_id="27205",
        imdb_id=None,  # no imdb_id from Plex
    )

    mock_ext_ids = TmdbExternalIds(imdb_id="tt1375666", tvdb_id=None)

    with (
        patch("src.core.plex_watchlist.settings") as mock_settings,
        patch("src.core.plex_watchlist.plex_client") as mock_plex,
        patch("src.core.plex_watchlist.tmdb_client") as mock_tmdb,
        patch("src.core.event_bus.event_bus"),
        patch("src.core.event_bus.QueueEvent"),
    ):
        mock_settings.plex.token = "tok"
        mock_settings.plex.watchlist_sync_enabled = True
        mock_plex.get_watchlist = AsyncMock(return_value=[movie])
        mock_tmdb.get_external_ids = AsyncMock(return_value=mock_ext_ids)

        from src.core.plex_watchlist import sync_watchlist
        stats = await sync_watchlist(session)

    # TMDB must have been called with the correct tmdb_id and type
    mock_tmdb.get_external_ids.assert_awaited_once_with(27205, "movie")

    assert stats["added"] == 1

    # The resolved imdb_id should be persisted
    result = await session.execute(select(MediaItem).where(MediaItem.tmdb_id == "27205"))
    item = result.scalar_one_or_none()
    assert item is not None
    assert item.imdb_id == "tt1375666"


async def test_sync_handles_tmdb_resolution_failure_gracefully(session: AsyncSession) -> None:
    """When TMDB fails to resolve imdb_id, the item is still inserted without it."""
    movie = _make_watchlist_item(
        title="Orphaned Movie",
        year=2015,
        media_type="movie",
        tmdb_id="555555",
        imdb_id=None,
    )

    with (
        patch("src.core.plex_watchlist.settings") as mock_settings,
        patch("src.core.plex_watchlist.plex_client") as mock_plex,
        patch("src.core.plex_watchlist.tmdb_client") as mock_tmdb,
        patch("src.core.event_bus.event_bus"),
        patch("src.core.event_bus.QueueEvent"),
    ):
        mock_settings.plex.token = "tok"
        mock_settings.plex.watchlist_sync_enabled = True
        mock_plex.get_watchlist = AsyncMock(return_value=[movie])
        mock_tmdb.get_external_ids = AsyncMock(return_value=None)  # TMDB returns nothing

        from src.core.plex_watchlist import sync_watchlist
        stats = await sync_watchlist(session)

    # Item should still be added despite no imdb_id
    assert stats["added"] == 1
    assert stats["errors"] == 0

    result = await session.execute(select(MediaItem).where(MediaItem.tmdb_id == "555555"))
    item = result.scalar_one_or_none()
    assert item is not None
    assert item.imdb_id is None


async def test_sync_integrity_error_skips_item_continues(session: AsyncSession) -> None:
    """IntegrityError on flush is caught; the offending item is skipped and sync continues."""
    movie1 = _make_watchlist_item(title="Movie One", tmdb_id="111001", imdb_id="tt1110001")
    movie2 = _make_watchlist_item(title="Movie Two", tmdb_id="111002", imdb_id="tt1110002")

    flush_call_count = 0
    original_flush = AsyncSession.flush

    async def _patched_flush(self: AsyncSession, objects: Any = None) -> None:
        nonlocal flush_call_count
        flush_call_count += 1
        if flush_call_count == 1:
            raise IntegrityError("UNIQUE constraint failed", None, None)
        await original_flush(self, objects)

    with (
        patch("src.core.plex_watchlist.settings") as mock_settings,
        patch("src.core.plex_watchlist.plex_client") as mock_plex,
        patch("src.core.plex_watchlist.tmdb_client") as mock_tmdb,
        patch("src.core.event_bus.event_bus"),
        patch("src.core.event_bus.QueueEvent"),
        patch.object(AsyncSession, "flush", _patched_flush),
    ):
        mock_settings.plex.token = "tok"
        mock_settings.plex.watchlist_sync_enabled = True
        mock_plex.get_watchlist = AsyncMock(return_value=[movie1, movie2])
        mock_tmdb.get_external_ids = AsyncMock(return_value=None)

        from src.core.plex_watchlist import sync_watchlist
        stats = await sync_watchlist(session)

    # movie1 failed → skipped; movie2 succeeded → added
    assert stats["skipped"] == 1
    assert stats["added"] == 1
    assert stats["errors"] == 0
    assert stats["total"] == 2


async def test_sync_multiple_items_mixed(session: AsyncSession) -> None:
    """sync_watchlist processes multiple items, correctly counting new vs existing."""
    from datetime import datetime

    # Pre-insert one movie
    existing = MediaItem(
        title="Already Here",
        year=2020,
        media_type=MediaType.MOVIE,
        tmdb_id="300300",
        imdb_id="tt3003001",
        state=QueueState.COMPLETE,
        state_changed_at=datetime.now(UTC),
        retry_count=0,
    )
    session.add(existing)
    await session.flush()

    watchlist = [
        _make_watchlist_item(title="Already Here", tmdb_id="300300", imdb_id="tt3003001"),
        _make_watchlist_item(title="Brand New Movie", tmdb_id="400400", imdb_id="tt4004001"),
        _make_watchlist_item(title="Another New Movie", tmdb_id="500500", imdb_id="tt5005001"),
    ]

    with (
        patch("src.core.plex_watchlist.settings") as mock_settings,
        patch("src.core.plex_watchlist.plex_client") as mock_plex,
        patch("src.core.plex_watchlist.tmdb_client") as mock_tmdb,
        patch("src.core.event_bus.event_bus"),
        patch("src.core.event_bus.QueueEvent"),
    ):
        mock_settings.plex.token = "tok"
        mock_settings.plex.watchlist_sync_enabled = True
        mock_plex.get_watchlist = AsyncMock(return_value=watchlist)
        mock_tmdb.get_external_ids = AsyncMock(return_value=None)

        from src.core.plex_watchlist import sync_watchlist
        stats = await sync_watchlist(session)

    assert stats["total"] == 3
    assert stats["added"] == 2
    assert stats["skipped"] == 1
    assert stats["errors"] == 0


async def test_sync_within_batch_dedup(session: AsyncSession) -> None:
    """Two watchlist items with the same tmdb_id: second is skipped within the same batch."""
    item_a = _make_watchlist_item(title="Movie Alpha", tmdb_id="777777", imdb_id="tt7770001")
    item_b = _make_watchlist_item(title="Movie Alpha Dupe", tmdb_id="777777", imdb_id="tt7770002")

    with (
        patch("src.core.plex_watchlist.settings") as mock_settings,
        patch("src.core.plex_watchlist.plex_client") as mock_plex,
        patch("src.core.plex_watchlist.tmdb_client") as mock_tmdb,
        patch("src.core.event_bus.event_bus"),
        patch("src.core.event_bus.QueueEvent"),
    ):
        mock_settings.plex.token = "tok"
        mock_settings.plex.watchlist_sync_enabled = True
        mock_plex.get_watchlist = AsyncMock(return_value=[item_a, item_b])
        mock_tmdb.get_external_ids = AsyncMock(return_value=None)

        from src.core.plex_watchlist import sync_watchlist
        stats = await sync_watchlist(session)

    assert stats["added"] == 1
    assert stats["skipped"] == 1

    # Only one row should be in the DB with this tmdb_id
    result = await session.execute(
        select(MediaItem).where(MediaItem.tmdb_id == "777777")
    )
    rows = result.scalars().all()
    assert len(rows) == 1
    assert rows[0].title == "Movie Alpha"


async def test_sync_publishes_sse_event_per_added_item(session: AsyncSession) -> None:
    """sync_watchlist publishes an SSE QueueEvent for every successfully added item."""
    items = [
        _make_watchlist_item(title="Movie A", tmdb_id="810001", imdb_id="tt8100011"),
        _make_watchlist_item(title="Movie B", tmdb_id="810002", imdb_id="tt8100012"),
    ]

    publish_calls: list[Any] = []

    with (
        patch("src.core.plex_watchlist.settings") as mock_settings,
        patch("src.core.plex_watchlist.plex_client") as mock_plex,
        patch("src.core.plex_watchlist.tmdb_client") as mock_tmdb,
    ):
        mock_settings.plex.token = "tok"
        mock_settings.plex.watchlist_sync_enabled = True
        mock_plex.get_watchlist = AsyncMock(return_value=items)
        mock_tmdb.get_external_ids = AsyncMock(return_value=None)

        # Capture the lazy-imported event_bus.publish calls
        mock_event_bus = MagicMock()
        mock_event_bus.publish = MagicMock(side_effect=lambda ev: publish_calls.append(ev))
        mock_queue_event = MagicMock(side_effect=lambda **kwargs: kwargs)

        with (
            patch("src.core.event_bus.event_bus", mock_event_bus),
            patch("src.core.event_bus.QueueEvent", mock_queue_event),
        ):
            from src.core.plex_watchlist import sync_watchlist
            stats = await sync_watchlist(session)

    assert stats["added"] == 2
    assert len(publish_calls) == 2


async def test_sync_show_without_tmdb_id_skips_monitoring(session: AsyncSession) -> None:
    """A show with no tmdb_id is added to the queue but monitoring is not attempted."""
    show = _make_watchlist_item(
        title="Obscure Show",
        year=2019,
        media_type="show",
        tmdb_id=None,      # no tmdb_id — cannot set up monitoring
        imdb_id="tt9190001",
    )

    mock_set_subscription = AsyncMock()

    with (
        patch("src.core.plex_watchlist.settings") as mock_settings,
        patch("src.core.plex_watchlist.plex_client") as mock_plex,
        patch("src.core.plex_watchlist.tmdb_client") as mock_tmdb,
        patch("src.core.event_bus.event_bus"),
        patch("src.core.event_bus.QueueEvent"),
    ):
        mock_settings.plex.token = "tok"
        mock_settings.plex.watchlist_sync_enabled = True
        mock_plex.get_watchlist = AsyncMock(return_value=[show])
        mock_tmdb.get_external_ids = AsyncMock(return_value=None)

        with patch("src.core.show_manager.show_manager") as mock_sm:
            mock_sm.set_subscription = mock_set_subscription

            from src.core.plex_watchlist import sync_watchlist
            stats = await sync_watchlist(session)

    # Item was added; monitoring was NOT attempted because tmdb_id is None
    assert stats["added"] == 1
    mock_set_subscription.assert_not_awaited()

    result = await session.execute(
        select(MediaItem).where(MediaItem.imdb_id == "tt9190001")
    )
    item = result.scalar_one_or_none()
    assert item is not None
    assert item.media_type == MediaType.SHOW
    assert item.tmdb_id is None


async def test_sync_show_monitoring_failure_does_not_abort(session: AsyncSession) -> None:
    """If set_subscription raises, the item is still counted as added."""
    show = _make_watchlist_item(
        title="Buggy Show",
        year=2021,
        media_type="show",
        tmdb_id="920001",
        imdb_id="tt9200010",
    )

    with (
        patch("src.core.plex_watchlist.settings") as mock_settings,
        patch("src.core.plex_watchlist.plex_client") as mock_plex,
        patch("src.core.plex_watchlist.tmdb_client") as mock_tmdb,
        patch("src.core.event_bus.event_bus"),
        patch("src.core.event_bus.QueueEvent"),
    ):
        mock_settings.plex.token = "tok"
        mock_settings.plex.watchlist_sync_enabled = True
        mock_plex.get_watchlist = AsyncMock(return_value=[show])
        mock_tmdb.get_external_ids = AsyncMock(return_value=None)

        with patch("src.core.show_manager.show_manager") as mock_sm:
            mock_sm.set_subscription = AsyncMock(side_effect=RuntimeError("DB locked"))

            from src.core.plex_watchlist import sync_watchlist
            stats = await sync_watchlist(session)

    # Despite the monitoring error, the show itself was added
    assert stats["added"] == 1
    assert stats["errors"] == 0


async def test_sync_sse_failure_does_not_abort(session: AsyncSession) -> None:
    """If event_bus.publish raises, the item is still counted as added."""
    movie = _make_watchlist_item(title="SSE Bomb", tmdb_id="730001", imdb_id="tt7300010")

    with (
        patch("src.core.plex_watchlist.settings") as mock_settings,
        patch("src.core.plex_watchlist.plex_client") as mock_plex,
        patch("src.core.plex_watchlist.tmdb_client") as mock_tmdb,
    ):
        mock_settings.plex.token = "tok"
        mock_settings.plex.watchlist_sync_enabled = True
        mock_plex.get_watchlist = AsyncMock(return_value=[movie])
        mock_tmdb.get_external_ids = AsyncMock(return_value=None)

        mock_event_bus = MagicMock()
        mock_event_bus.publish = MagicMock(side_effect=RuntimeError("queue full"))

        with (
            patch("src.core.event_bus.event_bus", mock_event_bus),
            patch("src.core.event_bus.QueueEvent", MagicMock()),
        ):
            from src.core.plex_watchlist import sync_watchlist
            stats = await sync_watchlist(session)

    assert stats["added"] == 1
    assert stats["errors"] == 0


async def test_sync_tmdb_resolution_for_tv_show(session: AsyncSession) -> None:
    """For a TV show, TMDB resolution uses media_type='tv' (not 'movie')."""
    from src.services.tmdb import TmdbExternalIds

    show = _make_watchlist_item(
        title="Dark",
        year=2017,
        media_type="show",
        tmdb_id="70523",
        imdb_id=None,
    )

    mock_ext = TmdbExternalIds(imdb_id="tt5753856", tvdb_id=318819)

    with (
        patch("src.core.plex_watchlist.settings") as mock_settings,
        patch("src.core.plex_watchlist.plex_client") as mock_plex,
        patch("src.core.plex_watchlist.tmdb_client") as mock_tmdb,
        patch("src.core.event_bus.event_bus"),
        patch("src.core.event_bus.QueueEvent"),
    ):
        mock_settings.plex.token = "tok"
        mock_settings.plex.watchlist_sync_enabled = True
        mock_plex.get_watchlist = AsyncMock(return_value=[show])
        mock_tmdb.get_external_ids = AsyncMock(return_value=mock_ext)

        with patch("src.core.show_manager.show_manager") as mock_sm:
            mock_sm.set_subscription = AsyncMock()

            from src.core.plex_watchlist import sync_watchlist
            stats = await sync_watchlist(session)

    # get_external_ids must be called with "tv", not "movie"
    mock_tmdb.get_external_ids.assert_awaited_once_with(70523, "tv")
    assert stats["added"] == 1

    result = await session.execute(select(MediaItem).where(MediaItem.tmdb_id == "70523"))
    item = result.scalar_one_or_none()
    assert item is not None
    assert item.imdb_id == "tt5753856"


async def test_sync_item_with_imdb_only_no_tmdb_resolution(session: AsyncSession) -> None:
    """When item has only imdb_id (no tmdb_id), TMDB resolution is not attempted."""
    movie = _make_watchlist_item(
        title="IMDB Only Movie",
        year=2018,
        media_type="movie",
        tmdb_id=None,
        imdb_id="tt6543210",
    )

    with (
        patch("src.core.plex_watchlist.settings") as mock_settings,
        patch("src.core.plex_watchlist.plex_client") as mock_plex,
        patch("src.core.plex_watchlist.tmdb_client") as mock_tmdb,
        patch("src.core.event_bus.event_bus"),
        patch("src.core.event_bus.QueueEvent"),
    ):
        mock_settings.plex.token = "tok"
        mock_settings.plex.watchlist_sync_enabled = True
        mock_plex.get_watchlist = AsyncMock(return_value=[movie])
        mock_tmdb.get_external_ids = AsyncMock(return_value=None)

        from src.core.plex_watchlist import sync_watchlist
        stats = await sync_watchlist(session)

    # TMDB should NOT be called when there is no tmdb_id to resolve from
    mock_tmdb.get_external_ids.assert_not_awaited()
    assert stats["added"] == 1

    result = await session.execute(
        select(MediaItem).where(MediaItem.imdb_id == "tt6543210")
    )
    item = result.scalar_one_or_none()
    assert item is not None
    assert item.tmdb_id is None


async def test_sync_returns_correct_total_count(session: AsyncSession) -> None:
    """stats['total'] equals the number of watchlist items returned by get_watchlist."""
    watchlist = [
        _make_watchlist_item(title=f"Movie {i}", tmdb_id=f"90000{i}", imdb_id=f"tt900000{i}")
        for i in range(5)
    ]

    with (
        patch("src.core.plex_watchlist.settings") as mock_settings,
        patch("src.core.plex_watchlist.plex_client") as mock_plex,
        patch("src.core.plex_watchlist.tmdb_client") as mock_tmdb,
        patch("src.core.event_bus.event_bus"),
        patch("src.core.event_bus.QueueEvent"),
    ):
        mock_settings.plex.token = "tok"
        mock_settings.plex.watchlist_sync_enabled = True
        mock_plex.get_watchlist = AsyncMock(return_value=watchlist)
        mock_tmdb.get_external_ids = AsyncMock(return_value=None)

        from src.core.plex_watchlist import sync_watchlist
        stats = await sync_watchlist(session)

    assert stats["total"] == 5
    assert stats["added"] == 5
    assert stats["skipped"] == 0


async def test_sync_movie_added_at_and_state_changed_at_set(session: AsyncSession) -> None:
    """MediaItems created by sync_watchlist have both added_at and state_changed_at populated."""

    movie = _make_watchlist_item(title="Timestamped Movie", tmdb_id="606060", imdb_id="tt6060601")

    with (
        patch("src.core.plex_watchlist.settings") as mock_settings,
        patch("src.core.plex_watchlist.plex_client") as mock_plex,
        patch("src.core.plex_watchlist.tmdb_client") as mock_tmdb,
        patch("src.core.event_bus.event_bus"),
        patch("src.core.event_bus.QueueEvent"),
    ):
        mock_settings.plex.token = "tok"
        mock_settings.plex.watchlist_sync_enabled = True
        mock_plex.get_watchlist = AsyncMock(return_value=[movie])
        mock_tmdb.get_external_ids = AsyncMock(return_value=None)

        from src.core.plex_watchlist import sync_watchlist
        await sync_watchlist(session)

    result = await session.execute(select(MediaItem).where(MediaItem.tmdb_id == "606060"))
    item = result.scalar_one_or_none()
    assert item is not None
    assert item.added_at is not None
    assert item.state_changed_at is not None
    assert item.retry_count == 0
