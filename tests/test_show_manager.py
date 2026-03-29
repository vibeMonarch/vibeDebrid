"""Tests for src/core/show_manager.py.

Covers:
  - get_show_detail: TMDB None, season statuses (IN_LIBRARY, IN_QUEUE,
    AVAILABLE, UPCOMING), season 0 filtering, subscription flag,
    quality profiles
  - add_seasons: creates MediaItems, skips existing, creates subscription,
    correct field values
  - set_subscription: create, update, unchanged
  - check_monitored_shows: no shows, new season pack, new episodes,
    dedup by existing_keys, future episode skip, last_checked_at update,
    TMDB failure handling
  - XEM-aware: _derive_scene_seasons (XEM-style split, no mappings, disabled,
    season detail fetch failure), get_show_detail with XEM (scene seasons,
    xem_mapped flag, item bucketing), add_seasons with XEM (complete pack,
    airing individual items, dedup, auto-subscribe, fallthrough)
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.show_manager import (
    AddSeasonsRequest,
    AddSeasonsResult,
    SeasonStatus,
    ShowManager,
)
from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.monitored_show import MonitoredShow
from src.services.tmdb import (
    TmdbEpisodeAirInfo,
    TmdbEpisodeInfo,
    TmdbSeasonDetail,
    TmdbSeasonInfo,
    TmdbShowDetail,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TMDB_ID = 12345
TMDB_ID_STR = "12345"


def _make_show_detail(
    *,
    tmdb_id: int = TMDB_ID,
    title: str = "Test Show",
    seasons: list[TmdbSeasonInfo] | None = None,
    imdb_id: str | None = "tt9999001",
    genres: list[dict] | None = None,
    next_episode_to_air: TmdbEpisodeAirInfo | None = None,
    last_episode_to_air: TmdbEpisodeAirInfo | None = None,
) -> TmdbShowDetail:
    """Build a TmdbShowDetail for use as a mock return value."""
    if seasons is None:
        seasons = [
            TmdbSeasonInfo(
                season_number=1,
                name="Season 1",
                episode_count=10,
                air_date="2020-01-01",
            )
        ]
    return TmdbShowDetail(
        tmdb_id=tmdb_id,
        title=title,
        year=2020,
        overview="A great show.",
        poster_path="/poster.jpg",
        backdrop_path="/backdrop.jpg",
        status="Ended",
        vote_average=8.5,
        number_of_seasons=len(seasons),
        seasons=seasons,
        imdb_id=imdb_id,
        genres=genres or [{"id": 18, "name": "Drama"}],
        next_episode_to_air=next_episode_to_air,
        last_episode_to_air=last_episode_to_air,
    )


async def _make_show_item(
    session: AsyncSession,
    *,
    tmdb_id: str = TMDB_ID_STR,
    season: int = 1,
    episode: int | None = None,
    state: QueueState = QueueState.WANTED,
    title: str = "Test Show",
    is_season_pack: bool = True,
) -> MediaItem:
    """Persist a show MediaItem and return it."""
    now = datetime.now(UTC)
    item = MediaItem(
        title=title,
        year=2020,
        media_type=MediaType.SHOW,
        tmdb_id=tmdb_id,
        imdb_id="tt9999001",
        state=state,
        source="show_detail",
        added_at=now,
        state_changed_at=now,
        retry_count=0,
        season=season,
        episode=episode,
        is_season_pack=is_season_pack,
    )
    session.add(item)
    await session.flush()
    return item


async def _make_monitored_show(
    session: AsyncSession,
    *,
    tmdb_id: int = TMDB_ID,
    enabled: bool = True,
    last_season: int | None = None,
    last_episode: int | None = None,
    title: str = "Test Show",
) -> MonitoredShow:
    """Persist a MonitoredShow and return it."""
    now = datetime.now(UTC)
    show = MonitoredShow(
        tmdb_id=tmdb_id,
        imdb_id="tt9999001",
        title=title,
        year=2020,
        quality_profile="high",
        enabled=enabled,
        last_season=last_season,
        last_episode=last_episode,
        created_at=now,
        updated_at=now,
    )
    session.add(show)
    await session.flush()
    return show


# ---------------------------------------------------------------------------
# Tests: get_show_detail
# ---------------------------------------------------------------------------


class TestGetShowDetail:
    """Tests for ShowManager.get_show_detail."""

    async def test_returns_none_when_tmdb_returns_none(self, session: AsyncSession) -> None:
        """get_show_detail returns None when TMDB finds no show."""
        sm = ShowManager()
        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=None,
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is None

    async def test_returns_show_detail_with_correct_basic_fields(
        self, session: AsyncSession
    ) -> None:
        """get_show_detail returns ShowDetail with correct title, year, overview."""
        sm = ShowManager()
        mock_show = _make_show_detail(title="Breaking Bad", imdb_id="tt0903747")

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        assert result.tmdb_id == TMDB_ID
        assert result.title == "Breaking Bad"
        assert result.imdb_id == "tt0903747"
        assert result.year == 2020
        assert result.overview == "A great show."
        assert result.vote_average == 8.5
        assert result.show_status == "Ended"

    async def test_genres_populated_from_show(self, session: AsyncSession) -> None:
        """get_show_detail extracts genre names from the genres list."""
        sm = ShowManager()
        mock_show = _make_show_detail(
            genres=[{"id": 18, "name": "Drama"}, {"id": 35, "name": "Comedy"}]
        )

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        assert "Drama" in result.genres
        assert "Comedy" in result.genres

    async def test_poster_and_backdrop_urls_built(self, session: AsyncSession) -> None:
        """get_show_detail builds full poster and backdrop URLs from paths."""
        sm = ShowManager()
        mock_show = _make_show_detail()

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        assert result.poster_url is not None
        assert "/w500/poster.jpg" in result.poster_url
        assert result.backdrop_url is not None
        assert "/w1280/backdrop.jpg" in result.backdrop_url

    async def test_null_poster_path_gives_null_url(self, session: AsyncSession) -> None:
        """get_show_detail sets poster_url=None when poster_path is None."""
        sm = ShowManager()
        mock_show = _make_show_detail()
        mock_show.poster_path = None
        mock_show.backdrop_path = None

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        assert result.poster_url is None
        assert result.backdrop_url is None

    async def test_season_with_complete_item_shows_in_library(
        self, session: AsyncSession
    ) -> None:
        """A season with a COMPLETE item gets IN_LIBRARY status."""
        await _make_show_item(session, season=1, state=QueueState.COMPLETE)

        sm = ShowManager()
        mock_show = _make_show_detail(
            seasons=[TmdbSeasonInfo(season_number=1, name="S1", episode_count=10, air_date="2020-01-01")]
        )

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        assert len(result.seasons) == 1
        season = result.seasons[0]
        assert season.status.value == "in_library"
        assert season.queue_item_ids != []

    async def test_season_with_done_item_shows_in_library(
        self, session: AsyncSession
    ) -> None:
        """A season with a DONE item also gets IN_LIBRARY status."""
        await _make_show_item(session, season=1, state=QueueState.DONE)

        sm = ShowManager()
        mock_show = _make_show_detail(
            seasons=[TmdbSeasonInfo(season_number=1, name="S1", episode_count=10, air_date="2020-01-01")]
        )

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        assert result.seasons[0].status.value == "in_library"

    async def test_season_with_wanted_item_shows_in_queue(
        self, session: AsyncSession
    ) -> None:
        """A season with a WANTED item (not in library state) gets IN_QUEUE status."""
        await _make_show_item(session, season=1, state=QueueState.WANTED)

        sm = ShowManager()
        mock_show = _make_show_detail(
            seasons=[TmdbSeasonInfo(season_number=1, name="S1", episode_count=10, air_date="2020-01-01")]
        )

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        assert result.seasons[0].status.value == "in_queue"

    async def test_season_with_scraping_item_shows_in_queue(
        self, session: AsyncSession
    ) -> None:
        """A season with a SCRAPING item gets IN_QUEUE status (not library)."""
        await _make_show_item(session, season=2, state=QueueState.SCRAPING)

        sm = ShowManager()
        mock_show = _make_show_detail(
            seasons=[
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=10, air_date="2020-01-01"),
                TmdbSeasonInfo(season_number=2, name="S2", episode_count=8, air_date="2021-01-01"),
            ]
        )

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        s2 = next(s for s in result.seasons if s.season_number == 2)
        assert s2.status.value == "in_queue"

    async def test_past_season_with_no_items_shows_available(
        self, session: AsyncSession
    ) -> None:
        """A past season with no queue items gets AVAILABLE status."""
        sm = ShowManager()
        mock_show = _make_show_detail(
            seasons=[TmdbSeasonInfo(season_number=1, name="S1", episode_count=10, air_date="2020-01-01")]
        )

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        assert result.seasons[0].status.value == "available"
        assert result.seasons[0].queue_item_ids == []

    async def test_future_air_date_shows_upcoming(self, session: AsyncSession) -> None:
        """A season with a future air_date gets UPCOMING status."""
        sm = ShowManager()
        future_date = "2099-12-31"
        mock_show = _make_show_detail(
            seasons=[TmdbSeasonInfo(season_number=3, name="S3", episode_count=8, air_date=future_date)]
        )

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        assert result.seasons[0].status.value == "upcoming"
        assert result.seasons[0].aired_episodes == 0

    async def test_zero_episodes_shows_upcoming(self, session: AsyncSession) -> None:
        """A season with episode_count=0 gets UPCOMING status regardless of air_date."""
        sm = ShowManager()
        mock_show = _make_show_detail(
            seasons=[TmdbSeasonInfo(season_number=4, name="S4", episode_count=0, air_date="2020-01-01")]
        )

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        assert result.seasons[0].status.value == "upcoming"

    async def test_null_air_date_shows_upcoming(self, session: AsyncSession) -> None:
        """A season with no air_date and no queue items gets UPCOMING status."""
        sm = ShowManager()
        mock_show = _make_show_detail(
            seasons=[TmdbSeasonInfo(season_number=2, name="S2", episode_count=6, air_date=None)]
        )

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        assert result.seasons[0].status.value == "upcoming"

    async def test_season_zero_is_filtered_out(self, session: AsyncSession) -> None:
        """Season 0 (Specials) is excluded from the returned seasons list."""
        sm = ShowManager()
        mock_show = _make_show_detail(
            seasons=[
                TmdbSeasonInfo(season_number=0, name="Specials", episode_count=3, air_date="2020-01-01"),
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=10, air_date="2020-01-01"),
            ]
        )

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        season_numbers = [s.season_number for s in result.seasons]
        assert 0 not in season_numbers
        assert 1 in season_numbers
        assert len(result.seasons) == 1

    async def test_is_subscribed_false_when_no_monitored_show(
        self, session: AsyncSession
    ) -> None:
        """is_subscribed is False when no MonitoredShow record exists."""
        sm = ShowManager()
        mock_show = _make_show_detail()

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        assert result.is_subscribed is False

    async def test_is_subscribed_true_when_enabled_record_exists(
        self, session: AsyncSession
    ) -> None:
        """is_subscribed is True when an enabled MonitoredShow record exists."""
        await _make_monitored_show(session, tmdb_id=TMDB_ID, enabled=True)

        sm = ShowManager()
        mock_show = _make_show_detail()

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        assert result.is_subscribed is True

    async def test_is_subscribed_false_when_disabled_record_exists(
        self, session: AsyncSession
    ) -> None:
        """is_subscribed is False when MonitoredShow record is disabled."""
        await _make_monitored_show(session, tmdb_id=TMDB_ID, enabled=False)

        sm = ShowManager()
        mock_show = _make_show_detail()

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        assert result.is_subscribed is False

    async def test_quality_profiles_populated_from_settings(
        self, session: AsyncSession
    ) -> None:
        """quality_profiles comes from settings.quality.profiles keys."""
        sm = ShowManager()
        mock_show = _make_show_detail()

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        # default config has "high" and "standard" profiles
        assert "high" in result.quality_profiles
        assert len(result.quality_profiles) >= 1

    async def test_multiple_seasons_all_returned(self, session: AsyncSession) -> None:
        """All non-zero seasons from TMDB are included in the result."""
        sm = ShowManager()
        mock_show = _make_show_detail(
            seasons=[
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=10, air_date="2020-01-01"),
                TmdbSeasonInfo(season_number=2, name="S2", episode_count=8, air_date="2021-01-01"),
                TmdbSeasonInfo(season_number=3, name="S3", episode_count=6, air_date="2099-06-01"),
            ]
        )

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        assert len(result.seasons) == 3
        assert [s.season_number for s in result.seasons] == [1, 2, 3]

    async def test_queue_item_ids_populated_for_in_queue_season(
        self, session: AsyncSession
    ) -> None:
        """queue_item_ids contains the database IDs of matching items for the season."""
        item1 = await _make_show_item(session, season=1, state=QueueState.WANTED)
        item2 = await _make_show_item(session, season=1, state=QueueState.SLEEPING, is_season_pack=False, episode=2)

        sm = ShowManager()
        mock_show = _make_show_detail(
            seasons=[TmdbSeasonInfo(season_number=1, name="S1", episode_count=10, air_date="2020-01-01")]
        )

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        season = result.seasons[0]
        assert item1.id in season.queue_item_ids
        assert item2.id in season.queue_item_ids

    async def test_upcoming_season_aired_episodes_is_zero(
        self, session: AsyncSession
    ) -> None:
        """aired_episodes is 0 for UPCOMING seasons."""
        sm = ShowManager()
        mock_show = _make_show_detail(
            seasons=[TmdbSeasonInfo(season_number=5, name="S5", episode_count=10, air_date="2099-01-01")]
        )

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        s = result.seasons[0]
        assert s.status.value == "upcoming"
        assert s.aired_episodes == 0

    async def test_available_season_aired_episodes_equals_episode_count(
        self, session: AsyncSession
    ) -> None:
        """aired_episodes equals episode_count for past (AVAILABLE) seasons."""
        sm = ShowManager()
        mock_show = _make_show_detail(
            seasons=[TmdbSeasonInfo(season_number=1, name="S1", episode_count=13, air_date="2020-01-01")]
        )

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        assert result.seasons[0].aired_episodes == 13


# ---------------------------------------------------------------------------
# Tests: add_seasons
# ---------------------------------------------------------------------------


class TestAddSeasons:
    """Tests for ShowManager.add_seasons."""

    def _make_request(
        self,
        seasons: list[int],
        subscribe: bool = False,
        quality_profile: str | None = None,
    ) -> AddSeasonsRequest:
        return AddSeasonsRequest(
            tmdb_id=TMDB_ID,
            imdb_id="tt9999001",
            title="Test Show",
            year=2020,
            seasons=seasons,
            quality_profile=quality_profile,
            subscribe=subscribe,
        )

    async def test_creates_mediaitem_for_each_season(self, session: AsyncSession) -> None:
        """add_seasons creates a WANTED season-pack MediaItem for each requested season."""
        sm = ShowManager()
        req = self._make_request(seasons=[1, 2, 3])

        result = await sm.add_seasons(session, req)

        assert result.created_items == 3
        assert result.skipped_seasons == []

    async def test_created_items_have_correct_fields(self, session: AsyncSession) -> None:
        """Created MediaItems have WANTED state, source='show_detail', is_season_pack=True."""
        from sqlalchemy import select

        sm = ShowManager()
        req = self._make_request(seasons=[1])

        await sm.add_seasons(session, req)

        rows = list((await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == TMDB_ID_STR)
        )).scalars().all())
        assert len(rows) == 1
        item = rows[0]
        assert item.state == QueueState.WANTED
        assert item.source == "show_detail"
        assert item.is_season_pack is True
        assert item.episode is None
        assert item.season == 1
        assert item.media_type == MediaType.SHOW
        assert item.title == "Test Show"
        assert item.year == 2020
        assert item.imdb_id == "tt9999001"

    async def test_skips_existing_season(self, session: AsyncSession) -> None:
        """add_seasons skips a season that already has a MediaItem in the DB."""
        await _make_show_item(session, season=1, state=QueueState.COMPLETE)

        sm = ShowManager()
        req = self._make_request(seasons=[1, 2])

        result = await sm.add_seasons(session, req)

        assert result.created_items == 1
        assert result.skipped_seasons == [1]

    async def test_all_seasons_skipped_when_all_exist(self, session: AsyncSession) -> None:
        """add_seasons returns 0 created when all requested seasons already exist."""
        await _make_show_item(session, season=1)
        await _make_show_item(session, season=2)

        sm = ShowManager()
        req = self._make_request(seasons=[1, 2])

        result = await sm.add_seasons(session, req)

        assert result.created_items == 0
        assert set(result.skipped_seasons) == {1, 2}

    async def test_empty_seasons_list_creates_nothing(self, session: AsyncSession) -> None:
        """add_seasons with an empty seasons list returns zero created."""
        sm = ShowManager()
        req = self._make_request(seasons=[])

        result = await sm.add_seasons(session, req)

        assert result.created_items == 0
        assert result.skipped_seasons == []

    async def test_creates_monitored_show_when_subscribe_true(
        self, session: AsyncSession
    ) -> None:
        """add_seasons creates a MonitoredShow record when subscribe=True."""
        from sqlalchemy import select

        sm = ShowManager()
        req = self._make_request(seasons=[1], subscribe=True)

        result = await sm.add_seasons(session, req)

        assert result.subscription_status == "created"
        row = (await session.execute(
            select(MonitoredShow).where(MonitoredShow.tmdb_id == TMDB_ID)
        )).scalar_one_or_none()
        assert row is not None
        assert row.enabled is True

    async def test_subscription_status_none_when_subscribe_false(
        self, session: AsyncSession
    ) -> None:
        """add_seasons returns subscription_status='none' when subscribe=False."""
        sm = ShowManager()
        req = self._make_request(seasons=[1], subscribe=False)

        result = await sm.add_seasons(session, req)

        assert result.subscription_status == "none"

    async def test_quality_profile_applied_to_created_items(
        self, session: AsyncSession
    ) -> None:
        """add_seasons stores quality_profile on created MediaItems."""
        from sqlalchemy import select

        sm = ShowManager()
        req = self._make_request(seasons=[1], quality_profile="standard")

        await sm.add_seasons(session, req)

        rows = list((await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == TMDB_ID_STR)
        )).scalars().all())
        assert rows[0].quality_profile == "standard"

    async def test_subscribe_true_with_no_seasons(self, session: AsyncSession) -> None:
        """add_seasons with subscribe=True and empty seasons list still creates subscription."""
        from sqlalchemy import select

        sm = ShowManager()
        req = self._make_request(seasons=[], subscribe=True)

        result = await sm.add_seasons(session, req)

        assert result.created_items == 0
        assert result.subscription_status == "created"
        row = (await session.execute(
            select(MonitoredShow).where(MonitoredShow.tmdb_id == TMDB_ID)
        )).scalar_one_or_none()
        assert row is not None

    async def test_existing_episode_item_does_not_block_season_pack(
        self, session: AsyncSession
    ) -> None:
        """An existing episode-level item for season 1 still blocks a season 1 add.

        The dedup check is on season number only, so any existing item for
        that season prevents creating a new one.
        """
        await _make_show_item(session, season=1, episode=3, is_season_pack=False)

        sm = ShowManager()
        req = self._make_request(seasons=[1])

        result = await sm.add_seasons(session, req)

        assert result.created_items == 0
        assert result.skipped_seasons == [1]


# ---------------------------------------------------------------------------
# Tests: set_subscription
# ---------------------------------------------------------------------------


class TestSetSubscription:
    """Tests for ShowManager.set_subscription (the public wrapper)."""

    async def test_creates_new_record_when_enabled_true_and_none_exists(
        self, session: AsyncSession
    ) -> None:
        """set_subscription creates a MonitoredShow when enabled=True and no record exists."""
        from sqlalchemy import select

        sm = ShowManager()
        status = await sm.set_subscription(
            session, TMDB_ID, True, imdb_id="tt9999001", title="Test Show", year=2020
        )

        assert status == "created"
        row = (await session.execute(
            select(MonitoredShow).where(MonitoredShow.tmdb_id == TMDB_ID)
        )).scalar_one_or_none()
        assert row is not None
        assert row.enabled is True
        assert row.title == "Test Show"

    async def test_returns_unchanged_when_disabled_and_no_record(
        self, session: AsyncSession
    ) -> None:
        """set_subscription returns 'unchanged' when enabled=False and no record exists."""
        sm = ShowManager()
        status = await sm.set_subscription(session, TMDB_ID, False)

        assert status == "unchanged"

    async def test_updates_existing_record_when_toggling(
        self, session: AsyncSession
    ) -> None:
        """set_subscription updates enabled flag when it differs from current value."""
        await _make_monitored_show(session, tmdb_id=TMDB_ID, enabled=True)

        sm = ShowManager()
        status = await sm.set_subscription(session, TMDB_ID, False)

        assert status == "updated"

    async def test_returns_unchanged_when_no_change_needed(
        self, session: AsyncSession
    ) -> None:
        """set_subscription returns 'unchanged' when enabled matches current record."""
        await _make_monitored_show(session, tmdb_id=TMDB_ID, enabled=True)

        sm = ShowManager()
        status = await sm.set_subscription(session, TMDB_ID, True)

        assert status == "unchanged"

    async def test_quality_profile_updated_on_toggle(self, session: AsyncSession) -> None:
        """set_subscription updates quality_profile when provided and toggling."""
        existing = await _make_monitored_show(session, tmdb_id=TMDB_ID, enabled=True)
        assert existing.quality_profile == "high"

        sm = ShowManager()
        status = await sm.set_subscription(
            session, TMDB_ID, False, quality_profile="standard"
        )

        assert status == "updated"
        # Refresh
        from sqlalchemy import select
        row = (await session.execute(
            select(MonitoredShow).where(MonitoredShow.tmdb_id == TMDB_ID)
        )).scalar_one_or_none()
        assert row is not None
        assert row.quality_profile == "standard"

    async def test_enable_disabled_record_returns_updated(
        self, session: AsyncSession
    ) -> None:
        """set_subscription enables a previously disabled record and returns 'updated'."""
        await _make_monitored_show(session, tmdb_id=TMDB_ID, enabled=False)

        sm = ShowManager()
        status = await sm.set_subscription(session, TMDB_ID, True)

        assert status == "updated"

    async def test_disabled_unchanged_when_already_disabled(
        self, session: AsyncSession
    ) -> None:
        """set_subscription returns 'unchanged' when disabling an already-disabled record."""
        await _make_monitored_show(session, tmdb_id=TMDB_ID, enabled=False)

        sm = ShowManager()
        status = await sm.set_subscription(session, TMDB_ID, False)

        assert status == "unchanged"


# ---------------------------------------------------------------------------
# Tests: check_monitored_shows
# ---------------------------------------------------------------------------


def _make_season_detail(
    season_number: int,
    episodes: list[tuple[int, str | None]],  # (episode_number, air_date)
) -> TmdbSeasonDetail:
    """Build a TmdbSeasonDetail with the given episodes."""
    eps = [
        TmdbEpisodeInfo(episode_number=ep_num, name=f"Episode {ep_num}", air_date=air_date)
        for ep_num, air_date in episodes
    ]
    return TmdbSeasonDetail(season_number=season_number, name=f"Season {season_number}", episodes=eps)


class TestCheckMonitoredShows:
    """Tests for ShowManager.check_monitored_shows."""

    async def test_returns_zeros_when_no_monitored_shows(
        self, session: AsyncSession
    ) -> None:
        """check_monitored_shows returns zeros when no enabled shows exist."""
        sm = ShowManager()
        result = await sm.check_monitored_shows(session)

        assert result["checked"] == 0
        assert result["new_items"] == 0

    async def test_ignores_disabled_shows(self, session: AsyncSession) -> None:
        """check_monitored_shows skips shows where enabled=False."""
        await _make_monitored_show(session, enabled=False)

        sm = ShowManager()
        result = await sm.check_monitored_shows(session)

        assert result["checked"] == 0

    async def test_creates_season_pack_for_new_season(
        self, session: AsyncSession
    ) -> None:
        """check_monitored_shows creates a season-pack item for a new aired season."""
        from sqlalchemy import select

        await _make_monitored_show(session, last_season=None, last_episode=None)

        mock_show = _make_show_detail(
            seasons=[
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=5, air_date="2020-01-01")
            ]
        )
        mock_season = _make_season_detail(1, [(1, "2020-01-01"), (2, "2020-01-08")])

        sm = ShowManager()
        with (
            patch(
                "src.core.show_manager.tmdb_client.get_show_details",
                new_callable=AsyncMock,
                return_value=mock_show,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                new_callable=AsyncMock,
                return_value=mock_season,
            ),
        ):
            result = await sm.check_monitored_shows(session)

        assert result["checked"] == 1
        assert result["new_items"] == 1

        rows = list((await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == TMDB_ID_STR)
        )).scalars().all())
        assert len(rows) == 1
        item = rows[0]
        assert item.is_season_pack is True
        assert item.season == 1
        assert item.episode is None
        assert item.source == "monitor"
        assert item.state == QueueState.WANTED

    async def test_creates_episode_items_for_current_airing_season(
        self, session: AsyncSession
    ) -> None:
        """check_monitored_shows adds individual episode items for new eps in the tracked season."""
        from sqlalchemy import select

        _show = await _make_monitored_show(session, last_season=1, last_episode=2)

        mock_show = _make_show_detail(
            seasons=[
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=5, air_date="2020-01-01")
            ]
        )
        # Episodes 1-4 have aired; 5 is future. last_episode=2, so ep 3 and 4 should be new.
        mock_season = _make_season_detail(1, [
            (1, "2020-01-01"),
            (2, "2020-01-08"),
            (3, "2020-01-15"),
            (4, "2020-01-22"),
            (5, "2099-12-31"),  # future — skip
        ])

        sm = ShowManager()
        with (
            patch(
                "src.core.show_manager.tmdb_client.get_show_details",
                new_callable=AsyncMock,
                return_value=mock_show,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                new_callable=AsyncMock,
                return_value=mock_season,
            ),
        ):
            result = await sm.check_monitored_shows(session)

        assert result["new_items"] == 2

        rows = list((await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == TMDB_ID_STR)
        )).scalars().all())
        episode_numbers = {r.episode for r in rows}
        assert 3 in episode_numbers
        assert 4 in episode_numbers
        assert 5 not in episode_numbers  # future episode must not be added
        for row in rows:
            assert row.is_season_pack is False

    async def test_skips_seasons_already_in_queue(self, session: AsyncSession) -> None:
        """check_monitored_shows does not create a season pack if one already exists."""
        from sqlalchemy import select

        await _make_monitored_show(session, last_season=None)
        # Pre-existing season pack for S1
        await _make_show_item(session, season=1, is_season_pack=True)

        mock_show = _make_show_detail(
            seasons=[
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=10, air_date="2020-01-01")
            ]
        )

        sm = ShowManager()
        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            result = await sm.check_monitored_shows(session)

        assert result["new_items"] == 0
        rows = list((await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == TMDB_ID_STR)
        )).scalars().all())
        # Only the pre-existing item, no new one created
        assert len(rows) == 1

    async def test_skips_future_season_entirely(self, session: AsyncSession) -> None:
        """check_monitored_shows skips seasons whose air_date is in the future."""
        from sqlalchemy import select

        await _make_monitored_show(session)

        mock_show = _make_show_detail(
            seasons=[
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=8, air_date="2099-06-01")
            ]
        )

        sm = ShowManager()
        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            result = await sm.check_monitored_shows(session)

        assert result["new_items"] == 0
        rows = list((await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == TMDB_ID_STR)
        )).scalars().all())
        assert len(rows) == 0

    async def test_updates_last_checked_at(self, session: AsyncSession) -> None:
        """check_monitored_shows updates last_checked_at on the MonitoredShow record."""
        from sqlalchemy import select

        show = await _make_monitored_show(session)
        assert show.last_checked_at is None

        mock_show = _make_show_detail(seasons=[
            TmdbSeasonInfo(season_number=1, name="S1", episode_count=5, air_date="2020-01-01")
        ])
        mock_season = _make_season_detail(1, [(1, "2020-01-01")])

        sm = ShowManager()
        with (
            patch(
                "src.core.show_manager.tmdb_client.get_show_details",
                new_callable=AsyncMock,
                return_value=mock_show,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                new_callable=AsyncMock,
                return_value=mock_season,
            ),
        ):
            await sm.check_monitored_shows(session)

        # Re-fetch from session
        row = (await session.execute(
            select(MonitoredShow).where(MonitoredShow.tmdb_id == TMDB_ID)
        )).scalar_one()
        assert row.last_checked_at is not None

    async def test_updates_last_season_after_new_season_pack(
        self, session: AsyncSession
    ) -> None:
        """check_monitored_shows advances last_season when a new season pack is created."""
        from sqlalchemy import select

        await _make_monitored_show(session, last_season=None)

        mock_show = _make_show_detail(
            seasons=[TmdbSeasonInfo(season_number=1, name="S1", episode_count=5, air_date="2020-01-01")]
        )
        mock_season = _make_season_detail(1, [(1, "2020-01-01"), (2, "2020-01-08")])

        sm = ShowManager()
        with (
            patch(
                "src.core.show_manager.tmdb_client.get_show_details",
                new_callable=AsyncMock,
                return_value=mock_show,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                new_callable=AsyncMock,
                return_value=mock_season,
            ),
        ):
            await sm.check_monitored_shows(session)

        row = (await session.execute(
            select(MonitoredShow).where(MonitoredShow.tmdb_id == TMDB_ID)
        )).scalar_one()
        assert row.last_season == 1

    async def test_updates_last_episode_to_highest_aired(
        self, session: AsyncSession
    ) -> None:
        """check_monitored_shows sets last_episode to the max aired episode number."""
        from sqlalchemy import select

        await _make_monitored_show(session, last_season=1, last_episode=1)

        mock_show = _make_show_detail(
            seasons=[TmdbSeasonInfo(season_number=1, name="S1", episode_count=5, air_date="2020-01-01")]
        )
        mock_season = _make_season_detail(1, [
            (1, "2020-01-01"),
            (2, "2020-01-08"),
            (3, "2020-01-15"),
            (4, "2099-12-31"),  # future — not counted for last_episode
        ])

        sm = ShowManager()
        with (
            patch(
                "src.core.show_manager.tmdb_client.get_show_details",
                new_callable=AsyncMock,
                return_value=mock_show,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                new_callable=AsyncMock,
                return_value=mock_season,
            ),
        ):
            await sm.check_monitored_shows(session)

        row = (await session.execute(
            select(MonitoredShow).where(MonitoredShow.tmdb_id == TMDB_ID)
        )).scalar_one()
        assert row.last_episode == 3  # ep 4 is in the future

    async def test_handles_tmdb_failure_gracefully(self, session: AsyncSession) -> None:
        """check_monitored_shows continues processing when TMDB returns None for a show.

        A None return from get_show_details is handled inside _check_single_show
        (returns 0, no exception raised) so the outer loop still counts both shows
        as checked. No new items are created for the failed show.
        """
        await _make_monitored_show(session, tmdb_id=TMDB_ID)
        await _make_monitored_show(session, tmdb_id=99999, title="Second Show")

        call_count = 0

        async def _get_details(tmdb_id: int) -> TmdbShowDetail | None:
            nonlocal call_count
            call_count += 1
            if tmdb_id == TMDB_ID:
                return None  # TMDB lookup failure for first show
            return _make_show_detail(tmdb_id=99999, title="Second Show", seasons=[])

        sm = ShowManager()
        with patch("src.core.show_manager.tmdb_client.get_show_details", side_effect=_get_details):
            result = await sm.check_monitored_shows(session)

        # Both shows are "checked" (None return doesn't raise, so outer loop counts both)
        assert result["checked"] == 2
        # No new items created for the show that returned None
        assert result["new_items"] == 0
        assert call_count == 2

    async def test_exception_in_show_check_does_not_crash_loop(
        self, session: AsyncSession
    ) -> None:
        """An unexpected exception during _check_single_show is caught; other shows continue."""
        await _make_monitored_show(session, tmdb_id=TMDB_ID, title="Bad Show")
        await _make_monitored_show(session, tmdb_id=99998, title="Good Show")

        call_count = 0

        async def _get_details(tmdb_id: int) -> TmdbShowDetail | None:
            nonlocal call_count
            call_count += 1
            if tmdb_id == TMDB_ID:
                raise RuntimeError("simulated network error")
            return _make_show_detail(tmdb_id=99998, title="Good Show", seasons=[])

        sm = ShowManager()
        with patch("src.core.show_manager.tmdb_client.get_show_details", side_effect=_get_details):
            result = await sm.check_monitored_shows(session)

        # Good show still checked; bad show exception swallowed
        assert result["checked"] == 1
        assert call_count == 2

    async def test_skips_season_zero_during_monitoring(self, session: AsyncSession) -> None:
        """check_monitored_shows does not create items for season 0 (Specials)."""
        from sqlalchemy import select

        await _make_monitored_show(session)

        mock_show = _make_show_detail(
            seasons=[
                TmdbSeasonInfo(season_number=0, name="Specials", episode_count=3, air_date="2020-01-01"),
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=5, air_date="2020-01-01"),
            ]
        )
        mock_season = _make_season_detail(1, [(1, "2020-01-01")])

        sm = ShowManager()
        with (
            patch(
                "src.core.show_manager.tmdb_client.get_show_details",
                new_callable=AsyncMock,
                return_value=mock_show,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                new_callable=AsyncMock,
                return_value=mock_season,
            ),
        ):
            _result = await sm.check_monitored_shows(session)

        rows = list((await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == TMDB_ID_STR)
        )).scalars().all())
        for row in rows:
            assert row.season != 0

    async def test_multiple_shows_all_checked(self, session: AsyncSession) -> None:
        """check_monitored_shows processes all enabled monitored shows."""
        await _make_monitored_show(session, tmdb_id=11111, title="Show A")
        await _make_monitored_show(session, tmdb_id=22222, title="Show B")

        mock_show_a = _make_show_detail(tmdb_id=11111, title="Show A", seasons=[])
        mock_show_b = _make_show_detail(tmdb_id=22222, title="Show B", seasons=[])

        async def _get_details(tmdb_id: int) -> TmdbShowDetail | None:
            if tmdb_id == 11111:
                return mock_show_a
            if tmdb_id == 22222:
                return mock_show_b
            return None

        sm = ShowManager()
        with patch("src.core.show_manager.tmdb_client.get_show_details", side_effect=_get_details):
            result = await sm.check_monitored_shows(session)

        assert result["checked"] == 2

    async def test_dedup_does_not_double_create_same_episode(
        self, session: AsyncSession
    ) -> None:
        """Existing (season, episode) keys prevent duplicate episode item creation."""
        from sqlalchemy import select

        _show = await _make_monitored_show(session, last_season=1, last_episode=0)
        # Pre-create episode 1 directly
        await _make_show_item(session, season=1, episode=1, is_season_pack=False)

        mock_show = _make_show_detail(
            seasons=[TmdbSeasonInfo(season_number=1, name="S1", episode_count=2, air_date="2020-01-01")]
        )
        mock_season = _make_season_detail(1, [
            (1, "2020-01-01"),
            (2, "2020-01-08"),
        ])

        sm = ShowManager()
        with (
            patch(
                "src.core.show_manager.tmdb_client.get_show_details",
                new_callable=AsyncMock,
                return_value=mock_show,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                new_callable=AsyncMock,
                return_value=mock_season,
            ),
        ):
            result = await sm.check_monitored_shows(session)

        # Only ep 2 is new; ep 1 was already in existing_keys
        assert result["new_items"] == 1
        rows = list((await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == TMDB_ID_STR)
        )).scalars().all())
        episode_numbers = {r.episode for r in rows if r.episode is not None}
        assert episode_numbers == {1, 2}


# ---------------------------------------------------------------------------
# Tests: TmdbEpisodeAirInfo and TmdbShowDetail new fields
# ---------------------------------------------------------------------------


class TestTmdbAirInfoModels:
    """Tests for TmdbEpisodeAirInfo and the new TmdbShowDetail fields."""

    def test_episode_air_info_instantiation(self) -> None:
        """TmdbEpisodeAirInfo can be created with valid data."""
        info = TmdbEpisodeAirInfo(
            season_number=2,
            episode_number=5,
            air_date="2026-06-15",
        )
        assert info.season_number == 2
        assert info.episode_number == 5
        assert info.air_date == "2026-06-15"

    def test_episode_air_info_null_air_date(self) -> None:
        """TmdbEpisodeAirInfo allows air_date=None."""
        info = TmdbEpisodeAirInfo(season_number=1, episode_number=1, air_date=None)
        assert info.air_date is None

    def test_show_detail_with_next_and_last_episode(self) -> None:
        """TmdbShowDetail populates next_episode_to_air and last_episode_to_air."""
        next_ep = TmdbEpisodeAirInfo(season_number=2, episode_number=3, air_date="2026-07-01")
        last_ep = TmdbEpisodeAirInfo(season_number=2, episode_number=2, air_date="2026-06-24")
        detail = _make_show_detail(
            next_episode_to_air=next_ep,
            last_episode_to_air=last_ep,
        )
        assert detail.next_episode_to_air is not None
        assert detail.next_episode_to_air.season_number == 2
        assert detail.next_episode_to_air.episode_number == 3
        assert detail.last_episode_to_air is not None
        assert detail.last_episode_to_air.season_number == 2
        assert detail.last_episode_to_air.episode_number == 2

    def test_show_detail_backward_compat_both_none(self) -> None:
        """TmdbShowDetail with both air info fields as None is valid (ended show)."""
        detail = _make_show_detail(
            next_episode_to_air=None,
            last_episode_to_air=None,
        )
        assert detail.next_episode_to_air is None
        assert detail.last_episode_to_air is None


# ---------------------------------------------------------------------------
# Tests: get_show_detail — AIRING status detection
# ---------------------------------------------------------------------------


class TestGetShowDetailAiringStatus:
    """Tests for AIRING season detection in get_show_detail."""

    async def test_airing_season_detected(self, session: AsyncSession) -> None:
        """Season matching next_episode_to_air.season_number gets AIRING status."""
        sm = ShowManager()
        mock_show = _make_show_detail(
            seasons=[
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=10, air_date="2020-01-01"),
                TmdbSeasonInfo(season_number=2, name="S2", episode_count=10, air_date="2026-01-01"),
            ],
            next_episode_to_air=TmdbEpisodeAirInfo(
                season_number=2, episode_number=6, air_date="2026-07-01"
            ),
        )
        mock_season = _make_season_detail(2, [
            (1, "2026-01-07"),
            (2, "2026-01-14"),
            (3, "2026-01-21"),
            (4, "2026-01-28"),
            (5, "2026-02-04"),
            (6, "2099-07-01"),  # future
        ])

        with (
            patch(
                "src.core.show_manager.tmdb_client.get_show_details",
                new_callable=AsyncMock,
                return_value=mock_show,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                new_callable=AsyncMock,
                return_value=mock_season,
            ),
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        s1 = next(s for s in result.seasons if s.season_number == 1)
        s2 = next(s for s in result.seasons if s.season_number == 2)
        assert s1.status.value == "available"
        assert s2.status.value == "airing"

    async def test_airing_season_aired_episodes_count(self, session: AsyncSession) -> None:
        """AIRING season's aired_episodes reflects only past episodes, not total."""
        sm = ShowManager()
        mock_show = _make_show_detail(
            seasons=[
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=10, air_date="2026-01-01"),
            ],
            next_episode_to_air=TmdbEpisodeAirInfo(
                season_number=1, episode_number=8, air_date="2099-06-01"
            ),
        )
        # 7 past episodes, 3 future
        mock_season = _make_season_detail(1, [
            (1, "2026-01-07"),
            (2, "2026-01-14"),
            (3, "2026-01-21"),
            (4, "2026-01-28"),
            (5, "2026-02-04"),
            (6, "2026-02-11"),
            (7, "2026-02-18"),
            (8, "2099-06-01"),
            (9, "2099-06-08"),
            (10, "2099-06-15"),
        ])

        with (
            patch(
                "src.core.show_manager.tmdb_client.get_show_details",
                new_callable=AsyncMock,
                return_value=mock_show,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                new_callable=AsyncMock,
                return_value=mock_season,
            ),
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        s = result.seasons[0]
        assert s.status.value == "airing"
        assert s.aired_episodes == 7
        assert s.episode_count == 10  # total count unchanged

    async def test_no_airing_when_no_next_episode(self, session: AsyncSession) -> None:
        """When next_episode_to_air is None, no season gets AIRING status."""
        sm = ShowManager()
        mock_show = _make_show_detail(
            seasons=[
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=10, air_date="2020-01-01"),
                TmdbSeasonInfo(season_number=2, name="S2", episode_count=8, air_date="2021-01-01"),
            ],
            next_episode_to_air=None,
        )

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        for s in result.seasons:
            assert s.status.value != "airing"
        assert all(s.status.value == "available" for s in result.seasons)

    async def test_airing_season_with_items_still_shows_airing(
        self, session: AsyncSession
    ) -> None:
        """AIRING takes priority over IN_QUEUE: an airing season with existing queue items
        is still shown as AIRING so the user can select it to add newly released episodes."""
        await _make_show_item(session, season=2, state=QueueState.WANTED)

        sm = ShowManager()
        mock_show = _make_show_detail(
            seasons=[
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=10, air_date="2020-01-01"),
                TmdbSeasonInfo(season_number=2, name="S2", episode_count=10, air_date="2026-01-01"),
            ],
            next_episode_to_air=TmdbEpisodeAirInfo(
                season_number=2, episode_number=6, air_date="2099-06-01"
            ),
        )
        mock_season = _make_season_detail(2, [
            (1, "2026-01-07"), (2, "2026-01-14"), (3, "2026-01-21"),
            (4, "2026-01-28"), (5, "2026-02-04"), (6, "2099-06-01"),
            (7, "2099-06-08"), (8, "2099-06-15"), (9, "2099-06-22"), (10, "2099-06-29"),
        ])

        with (
            patch(
                "src.core.show_manager.tmdb_client.get_show_details",
                new_callable=AsyncMock,
                return_value=mock_show,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                new_callable=AsyncMock,
                return_value=mock_season,
            ),
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        s2 = next(s for s in result.seasons if s.season_number == 2)
        assert s2.status.value == "airing"

    async def test_airing_season_with_library_items_still_shows_airing(
        self, session: AsyncSession
    ) -> None:
        """AIRING takes priority over IN_LIBRARY: an airing season with a completed
        season pack (covering old episodes) is still shown as AIRING so the user
        can select it to add new continuation episodes."""
        await _make_show_item(session, season=2, state=QueueState.COMPLETE)

        sm = ShowManager()
        mock_show = _make_show_detail(
            seasons=[
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=10, air_date="2020-01-01"),
                TmdbSeasonInfo(season_number=2, name="S2", episode_count=10, air_date="2026-01-01"),
            ],
            next_episode_to_air=TmdbEpisodeAirInfo(
                season_number=2, episode_number=6, air_date="2099-06-01"
            ),
        )
        mock_season = _make_season_detail(2, [
            (1, "2026-01-07"), (2, "2026-01-14"), (3, "2026-01-21"),
            (4, "2026-01-28"), (5, "2026-02-04"), (6, "2099-06-01"),
            (7, "2099-06-08"), (8, "2099-06-15"), (9, "2099-06-22"), (10, "2099-06-29"),
        ])

        with (
            patch(
                "src.core.show_manager.tmdb_client.get_show_details",
                new_callable=AsyncMock,
                return_value=mock_show,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                new_callable=AsyncMock,
                return_value=mock_season,
            ),
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        s2 = next(s for s in result.seasons if s.season_number == 2)
        assert s2.status.value == "airing"


# ---------------------------------------------------------------------------
# Tests: add_seasons — per-episode creation for airing seasons
# ---------------------------------------------------------------------------


class TestAddSeasonsAiring:
    """Tests for airing-season per-episode creation in add_seasons."""

    def _make_request(
        self,
        seasons: list[int],
        subscribe: bool = False,
        quality_profile: str | None = None,
    ) -> AddSeasonsRequest:
        return AddSeasonsRequest(
            tmdb_id=TMDB_ID,
            imdb_id="tt9999001",
            title="Test Show",
            year=2020,
            seasons=seasons,
            quality_profile=quality_profile,
            subscribe=subscribe,
        )

    async def test_add_airing_season_creates_episodes(self, session: AsyncSession) -> None:
        """Adding an airing season creates individual episode items, not a season pack.

        With 7 aired and 3 future episodes:
        - 7 WANTED non-season-pack items created
        - 3 UNRELEASED non-season-pack items created
        - No season pack item created
        - result counts are accurate
        """
        from sqlalchemy import select

        mock_show = _make_show_detail(
            seasons=[
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=10, air_date="2026-01-01"),
            ],
            next_episode_to_air=TmdbEpisodeAirInfo(
                season_number=1, episode_number=8, air_date="2099-06-01"
            ),
        )
        mock_season = _make_season_detail(1, [
            (1, "2026-01-07"),
            (2, "2026-01-14"),
            (3, "2026-01-21"),
            (4, "2026-01-28"),
            (5, "2026-02-04"),
            (6, "2026-02-11"),
            (7, "2026-02-18"),
            (8, "2099-06-01"),
            (9, "2099-06-08"),
            (10, "2099-06-15"),
        ])

        sm = ShowManager()
        req = self._make_request(seasons=[1])

        with (
            patch(
                "src.core.show_manager.tmdb_client.get_show_details",
                new_callable=AsyncMock,
                return_value=mock_show,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                new_callable=AsyncMock,
                return_value=mock_season,
            ),
        ):
            result = await sm.add_seasons(session, req)

        assert result.created_episodes == 7
        assert result.created_unreleased == 3
        assert result.created_items == 10

        rows = list((await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == TMDB_ID_STR)
        )).scalars().all())
        # No season pack
        assert all(not row.is_season_pack for row in rows)
        # Correct episode numbers
        ep_nums = {row.episode for row in rows}
        assert ep_nums == set(range(1, 11))
        # WANTED episodes
        wanted = [row for row in rows if row.state == QueueState.WANTED]
        assert len(wanted) == 7
        assert all(row.is_season_pack is False for row in wanted)
        # UNRELEASED episodes
        unreleased = [row for row in rows if row.state == QueueState.UNRELEASED]
        assert len(unreleased) == 3
        assert all(row.air_date is not None for row in unreleased)

    async def test_add_completed_season_creates_pack(self, session: AsyncSession) -> None:
        """Adding a non-airing season still creates a season pack (regression guard)."""
        from sqlalchemy import select

        mock_show = _make_show_detail(
            seasons=[
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=10, air_date="2020-01-01"),
            ],
            next_episode_to_air=None,
        )

        sm = ShowManager()
        req = self._make_request(seasons=[1])

        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            result = await sm.add_seasons(session, req)

        assert result.created_items == 1
        rows = list((await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == TMDB_ID_STR)
        )).scalars().all())
        assert len(rows) == 1
        assert rows[0].is_season_pack is True
        assert rows[0].episode is None
        assert rows[0].state == QueueState.WANTED

    async def test_add_airing_season_dedupes_existing_episodes(
        self, session: AsyncSession
    ) -> None:
        """add_seasons for an airing season skips episodes already in the DB but
        creates new episodes not yet present.

        Airing seasons are NOT skipped wholesale even when existing items are
        found — _add_airing_season handles per-episode dedup via existing_keys.
        Episodes 2-5 are new (ep 1 already exists); ep 1 must not be duplicated.
        """
        from sqlalchemy import select

        # Pre-create episode 1 for season 1 (already in DB)
        await _make_show_item(session, season=1, episode=1, is_season_pack=False)

        mock_show = _make_show_detail(
            seasons=[
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=5, air_date="2026-01-01"),
            ],
            next_episode_to_air=TmdbEpisodeAirInfo(
                season_number=1, episode_number=4, air_date="2099-05-01"
            ),
        )
        # Eps 1-3 aired; ep 1 already exists.  4-5 are future.
        mock_season = _make_season_detail(1, [
            (1, "2026-01-07"),
            (2, "2026-01-14"),
            (3, "2026-01-21"),
            (4, "2099-05-01"),
            (5, "2099-05-08"),
        ])

        sm = ShowManager()
        req = self._make_request(seasons=[1])

        with (
            patch(
                "src.core.show_manager.tmdb_client.get_show_details",
                new_callable=AsyncMock,
                return_value=mock_show,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                new_callable=AsyncMock,
                return_value=mock_season,
            ),
        ):
            result = await sm.add_seasons(session, req)

        # Airing season is NOT skipped; 4 new items created (eps 2, 3 WANTED + eps 4, 5 UNRELEASED)
        assert result.skipped_seasons == []
        assert result.created_items == 4
        assert result.created_episodes == 2   # eps 2 and 3 aired
        assert result.created_unreleased == 2  # eps 4 and 5 future
        rows = list((await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == TMDB_ID_STR)
        )).scalars().all())
        # 1 pre-existing + 4 new = 5 total
        assert len(rows) == 5
        ep_nums = {row.episode for row in rows}
        assert ep_nums == {1, 2, 3, 4, 5}

    async def test_add_airing_season_skips_no_air_date_episodes(
        self, session: AsyncSession
    ) -> None:
        """Episodes with no announced air_date are entirely skipped."""
        from sqlalchemy import select

        mock_show = _make_show_detail(
            seasons=[
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=5, air_date="2026-01-01"),
            ],
            next_episode_to_air=TmdbEpisodeAirInfo(
                season_number=1, episode_number=3, air_date="2099-05-01"
            ),
        )
        # Episodes 3, 4, 5 have no air_date
        mock_season = _make_season_detail(1, [
            (1, "2026-01-07"),
            (2, "2026-01-14"),
            (3, None),
            (4, None),
            (5, None),
        ])

        sm = ShowManager()
        req = self._make_request(seasons=[1])

        with (
            patch(
                "src.core.show_manager.tmdb_client.get_show_details",
                new_callable=AsyncMock,
                return_value=mock_show,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                new_callable=AsyncMock,
                return_value=mock_season,
            ),
        ):
            result = await sm.add_seasons(session, req)

        # Only eps 1 and 2 have air dates
        assert result.created_items == 2
        rows = list((await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == TMDB_ID_STR)
        )).scalars().all())
        assert len(rows) == 2
        ep_nums = {row.episode for row in rows}
        assert ep_nums == {1, 2}

    async def test_add_airing_season_auto_subscribes(self, session: AsyncSession) -> None:
        """Adding an airing season auto-enables monitoring even when subscribe=False."""
        from sqlalchemy import select

        mock_show = _make_show_detail(
            seasons=[
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=5, air_date="2026-01-01"),
            ],
            next_episode_to_air=TmdbEpisodeAirInfo(
                season_number=1, episode_number=3, air_date="2099-04-01"
            ),
        )
        mock_season = _make_season_detail(1, [
            (1, "2026-01-07"),
            (2, "2026-01-14"),
            (3, "2099-04-01"),
        ])

        sm = ShowManager()
        req = self._make_request(seasons=[1], subscribe=False)

        with (
            patch(
                "src.core.show_manager.tmdb_client.get_show_details",
                new_callable=AsyncMock,
                return_value=mock_show,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                new_callable=AsyncMock,
                return_value=mock_season,
            ),
        ):
            result = await sm.add_seasons(session, req)

        # subscription should have been auto-created even though subscribe=False
        assert result.subscription_status == "created"
        row = (await session.execute(
            select(MonitoredShow).where(MonitoredShow.tmdb_id == TMDB_ID)
        )).scalar_one_or_none()
        assert row is not None
        assert row.enabled is True

    async def test_add_mixed_completed_and_airing(self, session: AsyncSession) -> None:
        """S1 (completed) creates a season pack; S2 (airing) creates per-episode items."""
        from sqlalchemy import select

        mock_show = _make_show_detail(
            seasons=[
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=10, air_date="2020-01-01"),
                TmdbSeasonInfo(season_number=2, name="S2", episode_count=6, air_date="2026-01-01"),
            ],
            next_episode_to_air=TmdbEpisodeAirInfo(
                season_number=2, episode_number=4, air_date="2099-05-01"
            ),
        )
        # get_season_details only called for the airing season (S2)
        mock_season_s2 = _make_season_detail(2, [
            (1, "2026-01-07"),
            (2, "2026-01-14"),
            (3, "2026-01-21"),
            (4, "2099-05-01"),
            (5, "2099-05-08"),
            (6, "2099-05-15"),
        ])

        sm = ShowManager()
        req = self._make_request(seasons=[1, 2])

        with (
            patch(
                "src.core.show_manager.tmdb_client.get_show_details",
                new_callable=AsyncMock,
                return_value=mock_show,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                new_callable=AsyncMock,
                return_value=mock_season_s2,
            ),
        ):
            result = await sm.add_seasons(session, req)

        # 1 season pack for S1 + 6 episodes for S2
        assert result.created_items == 7
        assert result.created_episodes == 3
        assert result.created_unreleased == 3

        rows = list((await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == TMDB_ID_STR)
        )).scalars().all())
        assert len(rows) == 7

        season_pack_rows = [r for r in rows if r.is_season_pack]
        episode_rows = [r for r in rows if not r.is_season_pack]
        assert len(season_pack_rows) == 1
        assert season_pack_rows[0].season == 1
        assert len(episode_rows) == 6
        season2_nums = {r.episode for r in episode_rows}
        assert season2_nums == {1, 2, 3, 4, 5, 6}

    async def test_add_airing_season_tmdb_failure_fallback(
        self, session: AsyncSession
    ) -> None:
        """If get_season_details returns None for airing season, 0 items are created.

        The _add_airing_season method logs a warning and returns (0, 0), so
        the season is not added as a pack either — it simply produces no items.
        This prevents adding an incomplete or incorrect season pack as a fallback.
        """
        from sqlalchemy import select

        mock_show = _make_show_detail(
            seasons=[
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=10, air_date="2026-01-01"),
            ],
            next_episode_to_air=TmdbEpisodeAirInfo(
                season_number=1, episode_number=6, air_date="2099-05-01"
            ),
        )

        sm = ShowManager()
        req = self._make_request(seasons=[1])

        with (
            patch(
                "src.core.show_manager.tmdb_client.get_show_details",
                new_callable=AsyncMock,
                return_value=mock_show,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            result = await sm.add_seasons(session, req)

        assert result.created_items == 0
        assert result.created_episodes == 0
        assert result.created_unreleased == 0
        rows = list((await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == TMDB_ID_STR)
        )).scalars().all())
        assert len(rows) == 0


# ---------------------------------------------------------------------------
# Tests: pack cutoff logic for airing seasons (Fix 3)
# ---------------------------------------------------------------------------


class TestAddAiringSeasonPackCutoff:
    """Tests for completed-season-pack cutoff in _add_airing_season.

    Covers the XEM-style scenario: S01 episodes 1-28 covered by a
    COMPLETE season pack; user adds the airing season to get episodes 29+.
    """

    def _make_request(
        self,
        seasons: list[int],
        subscribe: bool = False,
    ) -> AddSeasonsRequest:
        return AddSeasonsRequest(
            tmdb_id=TMDB_ID,
            imdb_id="tt9999001",
            title="Test Anime",
            year=2023,
            seasons=seasons,
            subscribe=subscribe,
        )

    async def test_pack_cutoff_skips_episodes_before_cutoff(
        self, session: AsyncSession
    ) -> None:
        """When a COMPLETE season pack exists, episodes on/before pack completion are skipped.

        Scenario: pack completed on 2024-03-22 (covering eps 1-28).
        Episode 29 aired 2026-01-16 (after cutoff) → WANTED.
        Episode 30 is future → UNRELEASED.
        """
        from sqlalchemy import select

        # Season pack COMPLETE, completed 2024-03-22
        pack_completed_at = datetime(2024, 3, 22, 12, 0, 0, tzinfo=UTC)
        _now = datetime.now(UTC)
        pack = MediaItem(
            title="Test Anime",
            year=2023,
            media_type=MediaType.SHOW,
            tmdb_id=TMDB_ID_STR,
            imdb_id="tt9999001",
            state=QueueState.COMPLETE,
            source="show_detail",
            added_at=pack_completed_at,
            state_changed_at=pack_completed_at,
            retry_count=0,
            season=1,
            episode=None,
            is_season_pack=True,
        )
        session.add(pack)
        await session.flush()

        mock_show = _make_show_detail(
            title="Test Anime",
            seasons=[
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=30, air_date="2023-09-29"),
            ],
            next_episode_to_air=TmdbEpisodeAirInfo(
                season_number=1, episode_number=30, air_date="2099-01-23"
            ),
        )
        # Episodes 1-28 aired before cutoff; 29 after cutoff; 30 future
        mock_season = _make_season_detail(1, [
            *[(ep, f"2023-{9 + (ep - 1) // 4:02d}-{29 + ((ep - 1) % 4) * 7:02d}") for ep in range(1, 29)],
            (29, "2026-01-16"),
            (30, "2099-01-23"),
        ])

        sm = ShowManager()
        req = self._make_request(seasons=[1])

        with (
            patch(
                "src.core.show_manager.tmdb_client.get_show_details",
                new_callable=AsyncMock,
                return_value=mock_show,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                new_callable=AsyncMock,
                return_value=mock_season,
            ),
        ):
            result = await sm.add_seasons(session, req)

        # Only ep 29 (WANTED) and ep 30 (UNRELEASED) created; eps 1-28 skipped
        assert result.created_episodes == 1   # ep 29
        assert result.created_unreleased == 1  # ep 30
        assert result.created_items == 2

        rows = list((await session.execute(
            select(MediaItem).where(
                MediaItem.tmdb_id == TMDB_ID_STR,
                MediaItem.is_season_pack.is_(False),
            )
        )).scalars().all())
        ep_nums = {row.episode for row in rows}
        assert ep_nums == {29, 30}
        assert 1 not in ep_nums
        assert 28 not in ep_nums

    async def test_pack_in_queue_no_cutoff_applied(
        self, session: AsyncSession
    ) -> None:
        """A season pack that is still WANTED (not complete) does NOT set a cutoff.

        All episodes without existing individual-episode keys are eligible for creation.
        """

        # Season pack WANTED (not yet complete)
        now = datetime.now(UTC)
        pack = MediaItem(
            title="Test Anime",
            year=2023,
            media_type=MediaType.SHOW,
            tmdb_id=TMDB_ID_STR,
            imdb_id="tt9999001",
            state=QueueState.WANTED,
            source="show_detail",
            added_at=now,
            state_changed_at=now,
            retry_count=0,
            season=1,
            episode=None,
            is_season_pack=True,
        )
        session.add(pack)
        await session.flush()

        mock_show = _make_show_detail(
            title="Test Anime",
            seasons=[
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=5, air_date="2026-01-01"),
            ],
            next_episode_to_air=TmdbEpisodeAirInfo(
                season_number=1, episode_number=4, air_date="2099-04-01"
            ),
        )
        mock_season = _make_season_detail(1, [
            (1, "2026-01-07"),
            (2, "2026-01-14"),
            (3, "2026-01-21"),
            (4, "2099-04-01"),
            (5, "2099-04-08"),
        ])

        sm = ShowManager()
        req = self._make_request(seasons=[1])

        with (
            patch(
                "src.core.show_manager.tmdb_client.get_show_details",
                new_callable=AsyncMock,
                return_value=mock_show,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                new_callable=AsyncMock,
                return_value=mock_season,
            ),
        ):
            result = await sm.add_seasons(session, req)

        # No cutoff applied — all 5 episodes created (existing_keys has (1, None) from pack,
        # but individual episode keys are not in existing_keys)
        assert result.created_episodes == 3  # eps 1-3 aired
        assert result.created_unreleased == 2  # eps 4-5 future
        assert result.created_items == 5

    async def test_pack_cutoff_on_cutoff_day_is_inclusive(
        self, session: AsyncSession
    ) -> None:
        """Episodes whose air_date equals the pack cutoff date are skipped (inclusive boundary)."""
        from sqlalchemy import select

        cutoff = datetime(2024, 3, 22, 0, 0, 0, tzinfo=UTC)
        pack = MediaItem(
            title="Test Show",
            year=2020,
            media_type=MediaType.SHOW,
            tmdb_id=TMDB_ID_STR,
            imdb_id="tt9999001",
            state=QueueState.COMPLETE,
            source="show_detail",
            added_at=cutoff,
            state_changed_at=cutoff,
            retry_count=0,
            season=1,
            episode=None,
            is_season_pack=True,
        )
        session.add(pack)
        await session.flush()

        mock_show = _make_show_detail(
            seasons=[
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=3, air_date="2024-03-01"),
            ],
            next_episode_to_air=TmdbEpisodeAirInfo(
                season_number=1, episode_number=3, air_date="2099-04-01"
            ),
        )
        # Ep 1 aired before cutoff, ep 2 aired ON cutoff day, ep 3 after cutoff
        mock_season = _make_season_detail(1, [
            (1, "2024-03-15"),
            (2, "2024-03-22"),  # exactly on cutoff — must be skipped
            (3, "2099-04-01"),  # future and after cutoff — UNRELEASED
        ])

        sm = ShowManager()
        req = AddSeasonsRequest(
            tmdb_id=TMDB_ID, imdb_id="tt9999001", title="Test Show",
            year=2020, seasons=[1],
        )

        with (
            patch(
                "src.core.show_manager.tmdb_client.get_show_details",
                new_callable=AsyncMock,
                return_value=mock_show,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                new_callable=AsyncMock,
                return_value=mock_season,
            ),
        ):
            result = await sm.add_seasons(session, req)

        # Only ep 3 created; eps 1 and 2 skipped by cutoff
        assert result.created_items == 1
        rows = list((await session.execute(
            select(MediaItem).where(
                MediaItem.tmdb_id == TMDB_ID_STR,
                MediaItem.is_season_pack.is_(False),
            )
        )).scalars().all())
        assert len(rows) == 1
        assert rows[0].episode == 3


# ---------------------------------------------------------------------------
# Tests: AddSeasonsResult schema
# ---------------------------------------------------------------------------


class TestAddSeasonsResultSchema:
    """Tests for the AddSeasonsResult Pydantic model."""

    def test_result_has_episode_fields(self) -> None:
        """AddSeasonsResult has created_episodes and created_unreleased, both default to 0."""
        result = AddSeasonsResult()
        assert hasattr(result, "created_episodes")
        assert hasattr(result, "created_unreleased")
        assert result.created_episodes == 0
        assert result.created_unreleased == 0

    def test_result_fields_are_set_correctly(self) -> None:
        """AddSeasonsResult stores provided values for all fields."""
        result = AddSeasonsResult(
            created_items=10,
            created_episodes=7,
            created_unreleased=3,
            skipped_seasons=[2, 4],
            subscription_status="created",
        )
        assert result.created_items == 10
        assert result.created_episodes == 7
        assert result.created_unreleased == 3
        assert result.skipped_seasons == [2, 4]
        assert result.subscription_status == "created"

    def test_result_created_items_independent_of_episode_counts(self) -> None:
        """created_items is not constrained to equal created_episodes + created_unreleased."""
        # Season packs count toward created_items but not episode fields
        result = AddSeasonsResult(
            created_items=5,
            created_episodes=0,
            created_unreleased=0,
        )
        assert result.created_items == 5
        assert result.created_episodes == 0


# ---------------------------------------------------------------------------
# Regression tests: monitoring duplicate creation (Critical Fix 1)
# ---------------------------------------------------------------------------


class TestMonitoringNoDuplicates:
    """Regression tests for the duplicate-creation bug when add_seasons auto-subscribes.

    Bug: add_seasons() created episode items for an airing season and also
    auto-created a MonitoredShow with last_season=None/last_episode=None.
    When check_monitored_shows() ran next, it saw last_season is None, entered
    the 'else' branch, and created a season pack for the same season.

    Fix: after auto-subscribing, add_seasons() stamps last_season and
    last_episode on the MonitoredShow to reflect the episodes just created.
    check_monitored_shows() also has a belt-and-suspenders guard that skips
    season pack creation when individual episode items already exist.
    """

    def _make_request(
        self,
        seasons: list[int],
        subscribe: bool = False,
    ) -> AddSeasonsRequest:
        return AddSeasonsRequest(
            tmdb_id=TMDB_ID,
            imdb_id="tt9999001",
            title="Test Show",
            year=2020,
            seasons=seasons,
            subscribe=subscribe,
        )

    async def test_add_seasons_stamps_last_season_and_episode_on_monitored_show(
        self, session: AsyncSession
    ) -> None:
        """After add_seasons for an airing season, MonitoredShow has last_season and last_episode set."""
        from sqlalchemy import select

        mock_show = _make_show_detail(
            seasons=[
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=5, air_date="2026-01-01"),
            ],
            next_episode_to_air=TmdbEpisodeAirInfo(
                season_number=1, episode_number=4, air_date="2099-04-01"
            ),
        )
        # Episodes 1-3 have aired; 4 and 5 are future.
        mock_season = _make_season_detail(1, [
            (1, "2026-01-07"),
            (2, "2026-01-14"),
            (3, "2026-01-21"),
            (4, "2099-04-01"),
            (5, "2099-04-08"),
        ])

        sm = ShowManager()
        req = self._make_request(seasons=[1])

        with (
            patch(
                "src.core.show_manager.tmdb_client.get_show_details",
                new_callable=AsyncMock,
                return_value=mock_show,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                new_callable=AsyncMock,
                return_value=mock_season,
            ),
        ):
            result = await sm.add_seasons(session, req)

        assert result.created_episodes == 3
        assert result.created_unreleased == 2
        assert result.subscription_status == "created"

        row = (await session.execute(
            select(MonitoredShow).where(MonitoredShow.tmdb_id == TMDB_ID)
        )).scalar_one_or_none()
        assert row is not None
        assert row.last_season == 1
        assert row.last_episode == 3  # highest aired episode number

    async def test_check_monitored_shows_does_not_create_season_pack_after_add_seasons(
        self, session: AsyncSession
    ) -> None:
        """check_monitored_shows does not create a season pack for a season that was
        already added as individual episodes by add_seasons.

        This tests the Critical Fix 1 end-to-end: after add_seasons creates episode
        items and stamps last_season/last_episode, check_monitored_shows must not
        create a duplicate season pack in the same session.
        """
        from sqlalchemy import select

        mock_show = _make_show_detail(
            seasons=[
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=5, air_date="2026-01-01"),
            ],
            next_episode_to_air=TmdbEpisodeAirInfo(
                season_number=1, episode_number=4, air_date="2099-04-01"
            ),
        )
        mock_season = _make_season_detail(1, [
            (1, "2026-01-07"),
            (2, "2026-01-14"),
            (3, "2026-01-21"),
            (4, "2099-04-01"),
            (5, "2099-04-08"),
        ])

        sm = ShowManager()
        req = self._make_request(seasons=[1])

        # Step 1: add_seasons creates episode items and auto-subscribes.
        with (
            patch(
                "src.core.show_manager.tmdb_client.get_show_details",
                new_callable=AsyncMock,
                return_value=mock_show,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                new_callable=AsyncMock,
                return_value=mock_season,
            ),
        ):
            await sm.add_seasons(session, req)

        # Step 2: check_monitored_shows runs — should not add a season pack.
        with (
            patch(
                "src.core.show_manager.tmdb_client.get_show_details",
                new_callable=AsyncMock,
                return_value=mock_show,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                new_callable=AsyncMock,
                return_value=mock_season,
            ),
        ):
            check_result = await sm.check_monitored_shows(session)

        # No new items should be created — all episodes already exist.
        assert check_result["new_items"] == 0

        rows = list((await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == TMDB_ID_STR)
        )).scalars().all())
        # Only episode items, no season pack created.
        season_packs = [r for r in rows if r.is_season_pack]
        assert len(season_packs) == 0

    async def test_check_monitored_shows_belt_and_suspenders_skips_season_pack_when_episodes_exist(
        self, session: AsyncSession
    ) -> None:
        """check_monitored_shows skips season pack creation if episode items already exist,
        even when last_season is None (belt-and-suspenders guard for Fix 3).

        Simulates the scenario where last_season/last_episode were never stamped
        but episodes already exist in the DB.
        """
        from sqlalchemy import select

        # MonitoredShow with last_season=None (as if the stamp was missed).
        await _make_monitored_show(session, last_season=None, last_episode=None)
        # Pre-existing individual episode items for season 1.
        await _make_show_item(session, season=1, episode=1, is_season_pack=False)
        await _make_show_item(session, season=1, episode=2, is_season_pack=False)

        mock_show = _make_show_detail(
            seasons=[
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=5, air_date="2020-01-01"),
            ],
        )

        sm = ShowManager()
        with patch(
            "src.core.show_manager.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show,
        ):
            result = await sm.check_monitored_shows(session)

        # No season pack should be created since episodes already exist.
        assert result["new_items"] == 0
        rows = list((await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == TMDB_ID_STR)
        )).scalars().all())
        season_packs = [r for r in rows if r.is_season_pack]
        assert len(season_packs) == 0

    async def test_add_seasons_stamps_none_episode_when_only_unreleased_created(
        self, session: AsyncSession
    ) -> None:
        """When all created episodes are UNRELEASED (none have aired yet),
        last_episode on MonitoredShow remains None — we don't stamp a future ep number.
        """
        from sqlalchemy import select

        mock_show = _make_show_detail(
            seasons=[
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=3, air_date="2099-01-01"),
            ],
            next_episode_to_air=TmdbEpisodeAirInfo(
                season_number=1, episode_number=1, air_date="2099-02-01"
            ),
        )
        # All episodes are in the future (no aired ones).
        mock_season = _make_season_detail(1, [
            (1, "2099-02-01"),
            (2, "2099-02-08"),
            (3, "2099-02-15"),
        ])

        sm = ShowManager()
        req = self._make_request(seasons=[1])

        with (
            patch(
                "src.core.show_manager.tmdb_client.get_show_details",
                new_callable=AsyncMock,
                return_value=mock_show,
            ),
            patch(
                "src.core.show_manager.tmdb_client.get_season_details",
                new_callable=AsyncMock,
                return_value=mock_season,
            ),
        ):
            result = await sm.add_seasons(session, req)

        assert result.created_episodes == 0
        assert result.created_unreleased == 3
        assert result.subscription_status == "created"

        row = (await session.execute(
            select(MonitoredShow).where(MonitoredShow.tmdb_id == TMDB_ID)
        )).scalar_one_or_none()
        assert row is not None
        assert row.last_season == 1
        assert row.last_episode is None  # no aired episodes to stamp


# ---------------------------------------------------------------------------
# XEM Helpers
# ---------------------------------------------------------------------------

TVDB_ID = 76290


def _make_xem_show_detail(
    *,
    tmdb_id: int = TMDB_ID,
    tvdb_id: int = TVDB_ID,
    seasons: list[TmdbSeasonInfo] | None = None,
    next_episode_to_air: TmdbEpisodeAirInfo | None = None,
) -> TmdbShowDetail:
    """Build a TmdbShowDetail with a tvdb_id set (required for XEM path)."""
    if seasons is None:
        seasons = [
            TmdbSeasonInfo(
                season_number=1,
                name="Season 1",
                episode_count=35,
                air_date="2023-09-29",
            )
        ]
    return TmdbShowDetail(
        tmdb_id=tmdb_id,
        title="Test Anime: The Journey",
        year=2023,
        overview="A long-running anime series.",
        poster_path="/test_anime.jpg",
        backdrop_path="/test_anime_bg.jpg",
        status="Ended",
        vote_average=9.0,
        number_of_seasons=len(seasons),
        seasons=seasons,
        imdb_id="tt0000001",
        tvdb_id=tvdb_id,
        genres=[{"id": 16, "name": "Animation"}],
        next_episode_to_air=next_episode_to_air,
    )


def _make_test_anime_season_detail() -> TmdbSeasonDetail:
    """Build a 35-episode season detail for XEM test data (single TMDB season split by scene).

    Episodes 1-28 have past air dates (aired).
    Episodes 29-32 have past air dates (aired, belong to scene S02).
    Episodes 33-35 have future air dates (not yet aired, belong to scene S02).
    """
    _today_str = "2024-01-15"  # fixed reference point inside tests
    _past_dates = {
        ep: f"2023-{9 + (ep - 1) // 4:02d}-{29 + ((ep - 1) % 4) * 7:02d}"
        for ep in range(1, 33)
    }
    # Clamp dates to valid calendar range — just use fixed dates
    episode_dates: dict[int, str] = {}
    for ep in range(1, 29):
        episode_dates[ep] = "2023-10-01"  # all aired
    for ep in range(29, 33):
        episode_dates[ep] = "2024-01-05"  # aired (scene S02 episodes 1-4)
    for ep in range(33, 36):
        episode_dates[ep] = "2099-06-01"  # future (scene S02 episodes 5-7)

    episodes = [
        TmdbEpisodeInfo(
            episode_number=ep,
            name=f"Episode {ep}",
            air_date=episode_dates[ep],
        )
        for ep in range(1, 36)
    ]
    return TmdbSeasonDetail(
        season_number=1,
        name="Season 1",
        episodes=episodes,
    )


def _make_test_anime_xem_map() -> dict[int, tuple[int, int]]:
    """Absolute episode → (scene_season, scene_episode) map for XEM test data.

    TMDB S01 has 35 episodes (one season). Absolute positions are 1-35.
    Episodes 1-28 stay in scene S01; episodes 29-35 remap to scene S02.
    """
    result: dict[int, tuple[int, int]] = {}
    for ep in range(1, 29):
        result[ep] = (1, ep)       # absolute 1-28 → scene S01E01-E28
    for ep in range(29, 36):
        result[ep] = (2, ep - 28)  # absolute 29-35 → scene S02E01-E07
    return result


# ---------------------------------------------------------------------------
# Tests: ShowManager._derive_scene_seasons
# ---------------------------------------------------------------------------


class TestDeriveSceneSeasons:
    """Tests for ShowManager._derive_scene_seasons (internal method)."""

    async def test_xem_one_tmdb_season_yields_two_scene_seasons(
        self, session: AsyncSession
    ) -> None:
        """XEM-mapped show: TMDB S01 with XEM remapping → 2 SceneSeasonGroups."""
        sm = ShowManager()
        test_anime_seasons = [
            TmdbSeasonInfo(season_number=1, name="Season 1", episode_count=35, air_date="2023-09-29")
        ]

        xem_map = _make_test_anime_xem_map()
        season_detail = _make_test_anime_season_detail()

        with (
            patch("src.core.show_manager.xem_mapper.get_absolute_scene_map", new_callable=AsyncMock, return_value=xem_map),
            patch("src.core.show_manager.tmdb_client.get_season_details", new_callable=AsyncMock, return_value=season_detail),
        ):
            groups = await sm._derive_scene_seasons(session, TMDB_ID, TVDB_ID, test_anime_seasons)

        assert groups is not None
        assert len(groups) == 2

        # Scene S01: episodes 1-28 all aired
        s1 = next(g for g in groups if g.scene_season == 1)
        assert s1.total_episodes == 28
        assert s1.aired_episodes == 28
        assert s1.is_complete is True

        # Scene S02: episodes 29-35 remapped, only 4 aired (29-32), 3 future (33-35)
        s2 = next(g for g in groups if g.scene_season == 2)
        assert s2.total_episodes == 7
        assert s2.aired_episodes == 4  # episodes 29-32 (dates in past)
        assert s2.is_complete is False

    async def test_no_xem_mappings_returns_none(self, session: AsyncSession) -> None:
        """When XEM returns no mappings for the show, _derive_scene_seasons returns None."""
        sm = ShowManager()
        seasons = [TmdbSeasonInfo(season_number=1, name="S1", episode_count=13, air_date="2020-01-01")]

        with patch("src.core.show_manager.xem_mapper.get_absolute_scene_map", new_callable=AsyncMock, return_value=None):
            result = await sm._derive_scene_seasons(session, TMDB_ID, TVDB_ID, seasons)

        assert result is None

    async def test_xem_disabled_returns_none(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When settings.xem.enabled=False, _derive_scene_seasons returns None immediately."""
        mock_settings = MagicMock()
        mock_settings.xem.enabled = False
        monkeypatch.setattr("src.core.show_manager.settings", mock_settings)

        sm = ShowManager()
        seasons = [TmdbSeasonInfo(season_number=1, name="S1", episode_count=13, air_date="2020-01-01")]

        mock_xem = AsyncMock(return_value={1: (2, 1)})
        with patch("src.core.show_manager.xem_mapper.get_absolute_scene_map", mock_xem):
            result = await sm._derive_scene_seasons(session, TMDB_ID, TVDB_ID, seasons)

        assert result is None
        # XEM mapper must not have been consulted (settings guard fires first)
        mock_xem.assert_not_awaited()

    async def test_season_detail_fetch_failure_skips_season_gracefully(
        self, session: AsyncSession
    ) -> None:
        """When get_season_details returns None for a season, no crash occurs."""
        sm = ShowManager()
        # Two seasons: S1 fetch will fail, S2 will succeed
        seasons = [
            TmdbSeasonInfo(season_number=1, name="S1", episode_count=10, air_date="2020-01-01"),
            TmdbSeasonInfo(season_number=2, name="S2", episode_count=5, air_date="2021-01-01"),
        ]
        # XEM absolute map: S1 fetch fails so offset stays 0; S2E1 gets absolute=1 → scene S3E1.
        # This ensures the abs_map is non-empty so _derive_scene_seasons enters the XEM path.
        xem_map = {1: (3, 1)}

        s2_detail = TmdbSeasonDetail(
            season_number=2,
            name="Season 2",
            episodes=[
                TmdbEpisodeInfo(episode_number=1, air_date="2021-01-01"),
                TmdbEpisodeInfo(episode_number=2, air_date="2021-01-08"),
            ],
        )

        def _get_season(tmdb_id: int, season_num: int) -> TmdbSeasonDetail | None:
            if season_num == 1:
                return None  # S1 fetch fails
            return s2_detail

        with (
            patch("src.core.show_manager.xem_mapper.get_absolute_scene_map", new_callable=AsyncMock, return_value=xem_map),
            patch("src.core.show_manager.tmdb_client.get_season_details", side_effect=_get_season),
        ):
            result = await sm._derive_scene_seasons(session, TMDB_ID, TVDB_ID, seasons)

        # S1 was skipped (fetch failed), S2 was processed
        assert result is not None
        assert len(result) >= 1
        scene_seasons = {g.scene_season for g in result}
        # Absolute 1 (S2E1) maps to scene S3; absolute 2 (S2E2) has no XEM entry → scene S2
        assert 3 in scene_seasons or 2 in scene_seasons

    async def test_season_zero_skipped_in_xem_path(self, session: AsyncSession) -> None:
        """Season 0 (Specials) is skipped in the XEM path just like the normal path."""
        sm = ShowManager()
        seasons = [
            TmdbSeasonInfo(season_number=0, name="Specials", episode_count=3, air_date="2020-01-01"),
            TmdbSeasonInfo(season_number=1, name="S1", episode_count=10, air_date="2020-01-01"),
        ]
        # S0 is skipped → absolute_offset stays 0 when processing S1.
        # S1E5 → absolute 5 → scene S2E1. Other S1 episodes fall back to identity.
        xem_map = {5: (2, 1)}

        s1_detail = TmdbSeasonDetail(
            season_number=1,
            name="Season 1",
            episodes=[
                TmdbEpisodeInfo(episode_number=i, air_date="2020-01-01")
                for i in range(1, 11)
            ],
        )

        with (
            patch("src.core.show_manager.xem_mapper.get_absolute_scene_map", new_callable=AsyncMock, return_value=xem_map),
            patch("src.core.show_manager.tmdb_client.get_season_details", new_callable=AsyncMock, return_value=s1_detail),
        ):
            result = await sm._derive_scene_seasons(session, TMDB_ID, TVDB_ID, seasons)

        # Season 0 skipped; S1 processed (at least 1 group returned)
        assert result is not None
        for group in result:
            for ep_info in group.episodes:
                # No episode from season 0 should appear
                assert ep_info.tmdb_season != 0


# ---------------------------------------------------------------------------
# Tests: ShowManager.get_show_detail with XEM
# ---------------------------------------------------------------------------


class TestGetShowDetailWithXem:
    """Tests for get_show_detail when XEM mappings are present."""

    async def test_xem_show_returns_scene_seasons_with_xem_mapped_flag(
        self, session: AsyncSession
    ) -> None:
        """XEM-mapped show: seasons list contains SceneSeasonGroups with xem_mapped=True."""
        sm = ShowManager()
        mock_show = _make_xem_show_detail()
        season_detail = _make_test_anime_season_detail()
        xem_map = _make_test_anime_xem_map()

        with (
            patch("src.core.show_manager.tmdb_client.get_show_details", new_callable=AsyncMock, return_value=mock_show),
            patch("src.core.show_manager.xem_mapper.get_absolute_scene_map", new_callable=AsyncMock, return_value=xem_map),
            patch("src.core.show_manager.tmdb_client.get_season_details", new_callable=AsyncMock, return_value=season_detail),
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        assert len(result.seasons) == 2
        for s in result.seasons:
            assert s.xem_mapped is True

    async def test_xem_show_season_numbers_are_scene_numbers(
        self, session: AsyncSession
    ) -> None:
        """Season numbers in ShowDetail reflect scene season numbers (1 and 2), not TMDB."""
        sm = ShowManager()
        mock_show = _make_xem_show_detail()
        season_detail = _make_test_anime_season_detail()
        xem_map = _make_test_anime_xem_map()

        with (
            patch("src.core.show_manager.tmdb_client.get_show_details", new_callable=AsyncMock, return_value=mock_show),
            patch("src.core.show_manager.xem_mapper.get_absolute_scene_map", new_callable=AsyncMock, return_value=xem_map),
            patch("src.core.show_manager.tmdb_client.get_season_details", new_callable=AsyncMock, return_value=season_detail),
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        scene_nums = {s.season_number for s in result.seasons}
        assert 1 in scene_nums
        assert 2 in scene_nums

    async def test_xem_complete_scene_season_gets_available_status(
        self, session: AsyncSession
    ) -> None:
        """Scene S01 (all 28 eps aired, no queue items) gets AVAILABLE status."""
        sm = ShowManager()
        mock_show = _make_xem_show_detail()
        season_detail = _make_test_anime_season_detail()
        xem_map = _make_test_anime_xem_map()

        with (
            patch("src.core.show_manager.tmdb_client.get_show_details", new_callable=AsyncMock, return_value=mock_show),
            patch("src.core.show_manager.xem_mapper.get_absolute_scene_map", new_callable=AsyncMock, return_value=xem_map),
            patch("src.core.show_manager.tmdb_client.get_season_details", new_callable=AsyncMock, return_value=season_detail),
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        s1 = next(s for s in result.seasons if s.season_number == 1)
        assert s1.status == SeasonStatus.AVAILABLE

    async def test_xem_airing_scene_season_gets_airing_status(
        self, session: AsyncSession
    ) -> None:
        """Scene S02 (some eps aired, some future) gets AIRING status."""
        sm = ShowManager()
        mock_show = _make_xem_show_detail()
        season_detail = _make_test_anime_season_detail()
        xem_map = _make_test_anime_xem_map()

        with (
            patch("src.core.show_manager.tmdb_client.get_show_details", new_callable=AsyncMock, return_value=mock_show),
            patch("src.core.show_manager.xem_mapper.get_absolute_scene_map", new_callable=AsyncMock, return_value=xem_map),
            patch("src.core.show_manager.tmdb_client.get_season_details", new_callable=AsyncMock, return_value=season_detail),
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        s2 = next(s for s in result.seasons if s.season_number == 2)
        assert s2.status == SeasonStatus.AIRING

    async def test_non_xem_show_unchanged_behavior(self, session: AsyncSession) -> None:
        """Show without tvdb_id → XEM path not triggered, seasons match TMDB exactly."""
        sm = ShowManager()
        # tvdb_id=None → no XEM lookup
        mock_show = _make_show_detail(
            seasons=[
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=10, air_date="2020-01-01"),
                TmdbSeasonInfo(season_number=2, name="S2", episode_count=8, air_date="2021-01-01"),
            ]
        )
        # _make_show_detail builds a TmdbShowDetail without tvdb_id (defaults to None)

        xem_mock = AsyncMock()
        with (
            patch("src.core.show_manager.tmdb_client.get_show_details", new_callable=AsyncMock, return_value=mock_show),
            patch("src.core.show_manager.xem_mapper.get_absolute_scene_map", xem_mock),
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        assert len(result.seasons) == 2
        assert [s.season_number for s in result.seasons] == [1, 2]
        # XEM was never consulted because tvdb_id is None
        xem_mock.assert_not_awaited()
        for s in result.seasons:
            assert s.xem_mapped is False

    async def test_existing_items_bucketed_to_correct_scene_season(
        self, session: AsyncSession
    ) -> None:
        """Queue items with TMDB numbering (S01E29) appear in scene S02's queue_item_ids."""
        # S01E29 maps to scene S02E01 via XEM
        item = await _make_show_item(
            session,
            season=1,
            episode=29,
            is_season_pack=False,
            state=QueueState.WANTED,
        )

        sm = ShowManager()
        mock_show = _make_xem_show_detail()
        season_detail = _make_test_anime_season_detail()
        xem_map = _make_test_anime_xem_map()

        with (
            patch("src.core.show_manager.tmdb_client.get_show_details", new_callable=AsyncMock, return_value=mock_show),
            patch("src.core.show_manager.xem_mapper.get_absolute_scene_map", new_callable=AsyncMock, return_value=xem_map),
            patch("src.core.show_manager.tmdb_client.get_season_details", new_callable=AsyncMock, return_value=season_detail),
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        s2 = next(s for s in result.seasons if s.season_number == 2)
        assert item.id in s2.queue_item_ids
        # The item should NOT appear in scene S01
        s1 = next(s for s in result.seasons if s.season_number == 1)
        assert item.id not in s1.queue_item_ids

    async def test_scene_season_pack_bucketed_by_scene_season_number(
        self, session: AsyncSession
    ) -> None:
        """A season pack with season=1 (scene S01) appears in scene S01's queue_item_ids."""
        pack = await _make_show_item(
            session,
            season=1,
            episode=None,
            is_season_pack=True,
            state=QueueState.WANTED,
        )

        sm = ShowManager()
        mock_show = _make_xem_show_detail()
        season_detail = _make_test_anime_season_detail()
        xem_map = _make_test_anime_xem_map()

        with (
            patch("src.core.show_manager.tmdb_client.get_show_details", new_callable=AsyncMock, return_value=mock_show),
            patch("src.core.show_manager.xem_mapper.get_absolute_scene_map", new_callable=AsyncMock, return_value=xem_map),
            patch("src.core.show_manager.tmdb_client.get_season_details", new_callable=AsyncMock, return_value=season_detail),
        ):
            result = await sm.get_show_detail(session, TMDB_ID)

        assert result is not None
        s1 = next(s for s in result.seasons if s.season_number == 1)
        assert pack.id in s1.queue_item_ids


# ---------------------------------------------------------------------------
# Tests: ShowManager.add_seasons with XEM
# ---------------------------------------------------------------------------


class TestAddSeasonsWithXem:
    """Tests for add_seasons when XEM mappings restructure TMDB seasons into scene seasons."""

    def _make_xem_request(
        self,
        seasons: list[int],
        subscribe: bool = False,
        quality_profile: str | None = None,
    ) -> AddSeasonsRequest:
        return AddSeasonsRequest(
            tmdb_id=TMDB_ID,
            imdb_id="tt0000001",
            title="Test Anime: The Journey",
            year=2023,
            seasons=seasons,
            quality_profile=quality_profile,
            subscribe=subscribe,
        )

    async def test_complete_scene_season_creates_season_pack(
        self, session: AsyncSession
    ) -> None:
        """Requesting complete scene S01 → 1 season pack with scene season number."""
        sm = ShowManager()
        req = self._make_xem_request(seasons=[1])

        mock_show = _make_xem_show_detail()
        season_detail = _make_test_anime_season_detail()
        xem_map = _make_test_anime_xem_map()

        with (
            patch("src.core.show_manager.tmdb_client.get_show_details", new_callable=AsyncMock, return_value=mock_show),
            patch("src.core.show_manager.xem_mapper.get_absolute_scene_map", new_callable=AsyncMock, return_value=xem_map),
            patch("src.core.show_manager.tmdb_client.get_season_details", new_callable=AsyncMock, return_value=season_detail),
        ):
            result = await sm.add_seasons(session, req)

        assert result.created_items == 1
        assert result.skipped_seasons == []

        rows = list((await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == TMDB_ID_STR)
        )).scalars().all())
        assert len(rows) == 1
        pack = rows[0]
        assert pack.is_season_pack is True
        assert pack.season == 1   # scene season 1
        assert pack.episode is None
        assert pack.state == QueueState.WANTED

    async def test_airing_scene_season_creates_individual_items_with_tmdb_numbers(
        self, session: AsyncSession
    ) -> None:
        """Requesting airing scene S02 → individual items stored with TMDB numbering."""
        sm = ShowManager()
        req = self._make_xem_request(seasons=[2])

        mock_show = _make_xem_show_detail()
        season_detail = _make_test_anime_season_detail()
        xem_map = _make_test_anime_xem_map()

        with (
            patch("src.core.show_manager.tmdb_client.get_show_details", new_callable=AsyncMock, return_value=mock_show),
            patch("src.core.show_manager.xem_mapper.get_absolute_scene_map", new_callable=AsyncMock, return_value=xem_map),
            patch("src.core.show_manager.tmdb_client.get_season_details", new_callable=AsyncMock, return_value=season_detail),
        ):
            result = await sm.add_seasons(session, req)

        # Scene S02 has 7 episodes (4 aired WANTED + 3 future UNRELEASED)
        # Items with no air_date are skipped; season_detail has air_dates for all
        assert result.created_episodes == 4   # eps 29-32 (aired)
        assert result.created_unreleased == 3  # eps 33-35 (future)
        assert result.created_items == 7

        rows = list((await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == TMDB_ID_STR)
        )).scalars().all())
        # All items use TMDB numbering (season=1, episode=29..35)
        for row in rows:
            assert row.season == 1  # TMDB season 1
            assert row.episode in range(29, 36)
            assert row.is_season_pack is False

    async def test_both_complete_and_airing_scene_seasons(
        self, session: AsyncSession
    ) -> None:
        """Requesting seasons=[1, 2]: S1 pack + S2 individual items."""
        sm = ShowManager()
        req = self._make_xem_request(seasons=[1, 2])

        mock_show = _make_xem_show_detail()
        season_detail = _make_test_anime_season_detail()
        xem_map = _make_test_anime_xem_map()

        with (
            patch("src.core.show_manager.tmdb_client.get_show_details", new_callable=AsyncMock, return_value=mock_show),
            patch("src.core.show_manager.xem_mapper.get_absolute_scene_map", new_callable=AsyncMock, return_value=xem_map),
            patch("src.core.show_manager.tmdb_client.get_season_details", new_callable=AsyncMock, return_value=season_detail),
        ):
            result = await sm.add_seasons(session, req)

        # 1 season pack for S01 + 7 individual for S02
        assert result.created_items == 8
        assert result.skipped_seasons == []

        rows = list((await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == TMDB_ID_STR)
        )).scalars().all())
        packs = [r for r in rows if r.is_season_pack]
        episodes = [r for r in rows if not r.is_season_pack]
        assert len(packs) == 1
        assert packs[0].season == 1
        assert len(episodes) == 7

    async def test_dedup_existing_scene_season_pack_skipped(
        self, session: AsyncSession
    ) -> None:
        """If scene S01 pack already exists, requesting seasons=[1] skips it."""
        # Add a pack with scene season number 1
        await _make_show_item(session, season=1, episode=None, is_season_pack=True)

        sm = ShowManager()
        req = self._make_xem_request(seasons=[1])

        mock_show = _make_xem_show_detail()
        season_detail = _make_test_anime_season_detail()
        xem_map = _make_test_anime_xem_map()

        with (
            patch("src.core.show_manager.tmdb_client.get_show_details", new_callable=AsyncMock, return_value=mock_show),
            patch("src.core.show_manager.xem_mapper.get_absolute_scene_map", new_callable=AsyncMock, return_value=xem_map),
            patch("src.core.show_manager.tmdb_client.get_season_details", new_callable=AsyncMock, return_value=season_detail),
        ):
            result = await sm.add_seasons(session, req)

        assert result.created_items == 0
        assert 1 in result.skipped_seasons

    async def test_dedup_existing_episode_items_skipped_for_complete_scene_season(
        self, session: AsyncSession
    ) -> None:
        """If individual episode items already exist for scene S01, pack is skipped."""
        # Add an episode that belongs to scene S01 (TMDB S01E05)
        await _make_show_item(session, season=1, episode=5, is_season_pack=False)

        sm = ShowManager()
        req = self._make_xem_request(seasons=[1])

        mock_show = _make_xem_show_detail()
        season_detail = _make_test_anime_season_detail()
        xem_map = _make_test_anime_xem_map()

        with (
            patch("src.core.show_manager.tmdb_client.get_show_details", new_callable=AsyncMock, return_value=mock_show),
            patch("src.core.show_manager.xem_mapper.get_absolute_scene_map", new_callable=AsyncMock, return_value=xem_map),
            patch("src.core.show_manager.tmdb_client.get_season_details", new_callable=AsyncMock, return_value=season_detail),
        ):
            result = await sm.add_seasons(session, req)

        # Existing episode item blocks the scene season pack
        assert result.created_items == 0
        assert 1 in result.skipped_seasons

    async def test_dedup_existing_episodes_skip_in_airing_scene_season(
        self, session: AsyncSession
    ) -> None:
        """Some S02 episodes already exist → only new ones created."""
        # Add 2 of the 4 aired episodes (TMDB S01E29 and E30)
        await _make_show_item(session, season=1, episode=29, is_season_pack=False)
        await _make_show_item(session, season=1, episode=30, is_season_pack=False)

        sm = ShowManager()
        req = self._make_xem_request(seasons=[2])

        mock_show = _make_xem_show_detail()
        season_detail = _make_test_anime_season_detail()
        xem_map = _make_test_anime_xem_map()

        with (
            patch("src.core.show_manager.tmdb_client.get_show_details", new_callable=AsyncMock, return_value=mock_show),
            patch("src.core.show_manager.xem_mapper.get_absolute_scene_map", new_callable=AsyncMock, return_value=xem_map),
            patch("src.core.show_manager.tmdb_client.get_season_details", new_callable=AsyncMock, return_value=season_detail),
        ):
            result = await sm.add_seasons(session, req)

        # 4 aired - 2 existing = 2 new WANTED + 3 UNRELEASED
        assert result.created_episodes == 2
        assert result.created_unreleased == 3
        assert result.skipped_seasons == []

    async def test_airing_scene_season_auto_subscribes(
        self, session: AsyncSession
    ) -> None:
        """Adding an airing scene season (even with subscribe=False) creates a subscription."""
        sm = ShowManager()
        req = self._make_xem_request(seasons=[2], subscribe=False)

        mock_show = _make_xem_show_detail()
        season_detail = _make_test_anime_season_detail()
        xem_map = _make_test_anime_xem_map()

        with (
            patch("src.core.show_manager.tmdb_client.get_show_details", new_callable=AsyncMock, return_value=mock_show),
            patch("src.core.show_manager.xem_mapper.get_absolute_scene_map", new_callable=AsyncMock, return_value=xem_map),
            patch("src.core.show_manager.tmdb_client.get_season_details", new_callable=AsyncMock, return_value=season_detail),
        ):
            result = await sm.add_seasons(session, req)

        assert result.subscription_status == "created"
        row = (await session.execute(
            select(MonitoredShow).where(MonitoredShow.tmdb_id == TMDB_ID)
        )).scalar_one_or_none()
        assert row is not None
        assert row.enabled is True

    async def test_complete_scene_season_no_auto_subscribe(
        self, session: AsyncSession
    ) -> None:
        """Adding only a complete scene season (not airing) with subscribe=False → no subscription."""
        sm = ShowManager()
        req = self._make_xem_request(seasons=[1], subscribe=False)

        mock_show = _make_xem_show_detail()
        season_detail = _make_test_anime_season_detail()
        xem_map = _make_test_anime_xem_map()

        with (
            patch("src.core.show_manager.tmdb_client.get_show_details", new_callable=AsyncMock, return_value=mock_show),
            patch("src.core.show_manager.xem_mapper.get_absolute_scene_map", new_callable=AsyncMock, return_value=xem_map),
            patch("src.core.show_manager.tmdb_client.get_season_details", new_callable=AsyncMock, return_value=season_detail),
        ):
            result = await sm.add_seasons(session, req)

        assert result.subscription_status == "none"

    async def test_no_xem_mappings_falls_through_to_standard_logic(
        self, session: AsyncSession
    ) -> None:
        """When get_absolute_scene_map returns None, add_seasons uses standard TMDB logic."""
        sm = ShowManager()
        # Use a show WITH tvdb_id but XEM returns no absolute mappings
        mock_show = _make_xem_show_detail(
            seasons=[
                TmdbSeasonInfo(season_number=1, name="S1", episode_count=10, air_date="2020-01-01")
            ]
        )
        req = self._make_xem_request(seasons=[1])

        with (
            patch("src.core.show_manager.tmdb_client.get_show_details", new_callable=AsyncMock, return_value=mock_show),
            patch("src.core.show_manager.xem_mapper.get_absolute_scene_map", new_callable=AsyncMock, return_value=None),
        ):
            result = await sm.add_seasons(session, req)

        # Falls through to standard path → season pack for S01
        assert result.created_items == 1

        rows = list((await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == TMDB_ID_STR)
        )).scalars().all())
        assert len(rows) == 1
        assert rows[0].is_season_pack is True
        assert rows[0].season == 1

    async def test_scene_season_not_found_in_groups_is_skipped(
        self, session: AsyncSession
    ) -> None:
        """Requesting a scene season that doesn't exist in XEM groups → skipped."""
        sm = ShowManager()
        # Only scene S1 and S2 exist; request S99
        req = self._make_xem_request(seasons=[99])

        mock_show = _make_xem_show_detail()
        season_detail = _make_test_anime_season_detail()
        xem_map = _make_test_anime_xem_map()

        with (
            patch("src.core.show_manager.tmdb_client.get_show_details", new_callable=AsyncMock, return_value=mock_show),
            patch("src.core.show_manager.xem_mapper.get_absolute_scene_map", new_callable=AsyncMock, return_value=xem_map),
            patch("src.core.show_manager.tmdb_client.get_season_details", new_callable=AsyncMock, return_value=season_detail),
        ):
            result = await sm.add_seasons(session, req)

        assert result.created_items == 0
        assert 99 in result.skipped_seasons

    async def test_airing_scene_season_items_use_tmdb_season_field(
        self, session: AsyncSession
    ) -> None:
        """Items created for scene S02 store season=1 (TMDB season), not season=2."""
        sm = ShowManager()
        req = self._make_xem_request(seasons=[2])

        mock_show = _make_xem_show_detail()
        season_detail = _make_test_anime_season_detail()
        xem_map = _make_test_anime_xem_map()

        with (
            patch("src.core.show_manager.tmdb_client.get_show_details", new_callable=AsyncMock, return_value=mock_show),
            patch("src.core.show_manager.xem_mapper.get_absolute_scene_map", new_callable=AsyncMock, return_value=xem_map),
            patch("src.core.show_manager.tmdb_client.get_season_details", new_callable=AsyncMock, return_value=season_detail),
        ):
            await sm.add_seasons(session, req)

        rows = list((await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == TMDB_ID_STR)
        )).scalars().all())
        # All items belong to TMDB S01; XEM remapping happens at scrape time
        for row in rows:
            assert row.season == 1, f"Expected TMDB season=1, got season={row.season}"
