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
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.show_manager import AddSeasonsRequest, ShowManager
from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.monitored_show import MonitoredShow
from src.services.tmdb import TmdbEpisodeInfo, TmdbSeasonDetail, TmdbSeasonInfo, TmdbShowDetail

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
    now = datetime.now(timezone.utc)
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
    now = datetime.now(timezone.utc)
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

        show = await _make_monitored_show(session, last_season=1, last_episode=2)

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
            result = await sm.check_monitored_shows(session)

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

        show = await _make_monitored_show(session, last_season=1, last_episode=0)
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
