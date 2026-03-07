"""Show detail page business logic and monitoring scheduler."""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from enum import Enum

from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.monitored_show import MonitoredShow
from src.services.tmdb import tmdb_client

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


class SeasonStatus(str, Enum):
    AVAILABLE = "available"
    IN_QUEUE = "in_queue"
    IN_LIBRARY = "in_library"
    UPCOMING = "upcoming"


class SeasonInfo(BaseModel):
    """Season info enriched with queue status."""

    season_number: int
    name: str = ""
    episode_count: int = 0
    aired_episodes: int = 0
    air_date: str | None = None
    status: SeasonStatus = SeasonStatus.AVAILABLE
    queue_item_ids: list[int] = []


class ShowDetail(BaseModel):
    """Full show detail for the show page."""

    tmdb_id: int
    imdb_id: str | None = None
    title: str
    year: int | None = None
    overview: str = ""
    poster_url: str | None = None
    backdrop_url: str | None = None
    show_status: str = ""
    vote_average: float = 0.0
    genres: list[str] = []
    seasons: list[SeasonInfo] = []
    is_subscribed: bool = False
    quality_profiles: list[str] = []
    default_profile: str = ""


class AddSeasonsRequest(BaseModel):
    """Request to add seasons to queue."""

    tmdb_id: int
    imdb_id: str | None = None
    title: str
    year: int | None = None
    seasons: list[int]
    quality_profile: str | None = None
    subscribe: bool = False


class AddSeasonsResult(BaseModel):
    """Result of adding seasons."""

    created_items: int = 0
    skipped_seasons: list[int] = []
    subscription_status: str = "none"  # "created", "updated", "unchanged", "none"


# ---------------------------------------------------------------------------
# ShowManager
# ---------------------------------------------------------------------------

_LIBRARY_STATES = frozenset({QueueState.COMPLETE, QueueState.DONE})


def _parse_air_date(air_date_str: str | None) -> date | None:
    """Parse an ISO air date string, returning None on failure."""
    if not air_date_str:
        return None
    try:
        return date.fromisoformat(air_date_str)
    except ValueError:
        return None


class ShowManager:
    """Stateless service for show detail operations and monitoring."""

    async def get_show_detail(self, session: AsyncSession, tmdb_id: int) -> ShowDetail | None:
        """Fetch show details from TMDB and cross-reference with queue.

        Args:
            session: Async database session.
            tmdb_id: The TMDB numeric ID of the TV show.

        Returns:
            A ShowDetail object with enriched season statuses, or None if
            TMDB returned no data.
        """
        show = await tmdb_client.get_show_details(tmdb_id)
        if show is None:
            return None

        image_base = settings.tmdb.image_base_url.rstrip("/")
        poster_url = f"{image_base}/w500{show.poster_path}" if show.poster_path else None
        backdrop_url = f"{image_base}/w1280{show.backdrop_path}" if show.backdrop_path else None

        # Get existing queue items for this show (by tmdb_id)
        tmdb_id_str = str(tmdb_id)
        result = await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == tmdb_id_str)
        )
        existing_items = list(result.scalars().all())

        # Build season → item mapping keyed by season number
        season_items: dict[int, list[MediaItem]] = {}
        for item in existing_items:
            if item.season is not None:
                season_items.setdefault(item.season, []).append(item)

        # Check subscription status
        sub_result = await session.execute(
            select(MonitoredShow).where(MonitoredShow.tmdb_id == tmdb_id)
        )
        monitored = sub_result.scalar_one_or_none()
        is_subscribed = monitored is not None and monitored.enabled

        today = datetime.now(timezone.utc).date()

        seasons: list[SeasonInfo] = []
        for s in show.seasons:
            if s.season_number == 0:
                continue  # Skip Specials

            items_for_season = season_items.get(s.season_number, [])
            queue_item_ids = [item.id for item in items_for_season]

            if any(item.state in _LIBRARY_STATES for item in items_for_season):
                status = SeasonStatus.IN_LIBRARY
            elif items_for_season:
                status = SeasonStatus.IN_QUEUE
            elif s.air_date is None or s.episode_count == 0:
                status = SeasonStatus.UPCOMING
            else:
                try:
                    air_date = date.fromisoformat(s.air_date)
                    if air_date > today:
                        status = SeasonStatus.UPCOMING
                    else:
                        status = SeasonStatus.AVAILABLE
                except ValueError:
                    status = SeasonStatus.AVAILABLE

            # For past seasons use episode_count directly; upcoming seasons have 0 aired
            aired_episodes = s.episode_count if status != SeasonStatus.UPCOMING else 0

            seasons.append(SeasonInfo(
                season_number=s.season_number,
                name=s.name,
                episode_count=s.episode_count,
                aired_episodes=aired_episodes,
                air_date=s.air_date,
                status=status,
                queue_item_ids=queue_item_ids,
            ))

        profile_names = list(settings.quality.profiles.keys())
        genres = [g.get("name", "") for g in show.genres if g.get("name")]

        return ShowDetail(
            tmdb_id=tmdb_id,
            imdb_id=show.imdb_id,
            title=show.title,
            year=show.year,
            overview=show.overview,
            poster_url=poster_url,
            backdrop_url=backdrop_url,
            show_status=show.status,
            vote_average=show.vote_average,
            genres=genres,
            seasons=seasons,
            is_subscribed=is_subscribed,
            quality_profiles=profile_names,
            default_profile=settings.quality.default_profile,
        )

    async def add_seasons(
        self, session: AsyncSession, request: AddSeasonsRequest
    ) -> AddSeasonsResult:
        """Add selected seasons to queue as season pack items.

        Skips seasons that already have an existing MediaItem. Optionally
        creates or updates a MonitoredShow subscription if request.subscribe
        is True.

        Args:
            session: Async database session (caller owns the transaction).
            request: AddSeasonsRequest with seasons to add and subscription flag.

        Returns:
            AddSeasonsResult with counts of created items and skipped seasons.
        """
        tmdb_id_str = str(request.tmdb_id)
        now = datetime.now(timezone.utc)
        created = 0
        skipped: list[int] = []

        for season_num in request.seasons:
            # Check for existing item for this season
            result = await session.execute(
                select(MediaItem).where(
                    MediaItem.tmdb_id == tmdb_id_str,
                    MediaItem.season == season_num,
                )
            )
            existing = result.scalars().first()
            if existing is not None:
                skipped.append(season_num)
                logger.info(
                    "show_manager.add_seasons: skipping season %d for tmdb_id=%s (already exists item_id=%d)",
                    season_num, tmdb_id_str, existing.id,
                )
                continue

            item = MediaItem(
                title=request.title,
                year=request.year,
                media_type=MediaType.SHOW,
                tmdb_id=tmdb_id_str,
                imdb_id=request.imdb_id,
                state=QueueState.WANTED,
                source="show_detail",
                added_at=now,
                state_changed_at=now,
                retry_count=0,
                season=season_num,
                episode=None,
                is_season_pack=True,
                quality_profile=request.quality_profile,
            )
            session.add(item)
            created += 1
            logger.info(
                "show_manager.add_seasons: created item for %s S%02d (tmdb_id=%s)",
                request.title, season_num, tmdb_id_str,
            )

        # Handle subscription
        sub_status = "none"
        if request.subscribe:
            sub_status = await self._set_subscription(
                session, request.tmdb_id, request.imdb_id,
                request.title, request.year, request.quality_profile, True,
            )

        await session.flush()

        return AddSeasonsResult(
            created_items=created,
            skipped_seasons=skipped,
            subscription_status=sub_status,
        )

    async def set_subscription(
        self,
        session: AsyncSession,
        tmdb_id: int,
        enabled: bool,
        imdb_id: str | None = None,
        title: str = "",
        year: int | None = None,
        quality_profile: str | None = None,
    ) -> str:
        """Toggle subscription on/off for a monitored show.

        Args:
            session: Async database session (caller owns the transaction).
            tmdb_id: The TMDB numeric ID of the show.
            enabled: Whether to enable or disable monitoring.
            imdb_id: Optional IMDB ID for the show.
            title: Show title (used when creating a new record).
            year: Release year (used when creating a new record).
            quality_profile: Quality profile name to apply when monitoring.

        Returns:
            Status string: "created", "updated", or "unchanged".
        """
        return await self._set_subscription(
            session, tmdb_id, imdb_id, title, year, quality_profile, enabled,
        )

    async def _set_subscription(
        self,
        session: AsyncSession,
        tmdb_id: int,
        imdb_id: str | None,
        title: str,
        year: int | None,
        quality_profile: str | None,
        enabled: bool,
    ) -> str:
        """Internal: create or update a MonitoredShow record.

        Returns:
            "created", "updated", or "unchanged".
        """
        result = await session.execute(
            select(MonitoredShow).where(MonitoredShow.tmdb_id == tmdb_id)
        )
        existing = result.scalar_one_or_none()

        if existing is not None:
            if existing.enabled == enabled:
                return "unchanged"
            existing.enabled = enabled
            existing.updated_at = datetime.now(timezone.utc)
            if quality_profile:
                existing.quality_profile = quality_profile
            logger.info(
                "show_manager: updated subscription tmdb_id=%d enabled=%s",
                tmdb_id, enabled,
            )
            return "updated"

        if not enabled:
            return "unchanged"

        monitored = MonitoredShow(
            tmdb_id=tmdb_id,
            imdb_id=imdb_id,
            title=title,
            year=year,
            quality_profile=quality_profile,
            enabled=True,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        session.add(monitored)
        logger.info(
            "show_manager: created subscription tmdb_id=%d title=%r",
            tmdb_id, title,
        )
        return "created"

    async def check_monitored_shows(self, session: AsyncSession) -> dict[str, int]:
        """Scheduler job: check all monitored shows for new episodes.

        For each enabled MonitoredShow:
        1. Fetch show details from TMDB.
        2. For new complete seasons not yet tracked → create season pack item.
        3. For the current airing season with new episodes → create per-episode items.
        4. Update last_checked_at, last_season, last_episode on each show record.

        Args:
            session: Async database session (caller owns the transaction).

        Returns:
            A dict with "checked" and "new_items" counts.
        """
        result = await session.execute(
            select(MonitoredShow).where(MonitoredShow.enabled.is_(True))
        )
        monitored_shows = list(result.scalars().all())

        if not monitored_shows:
            logger.debug("check_monitored_shows: no enabled monitored shows")
            return {"checked": 0, "new_items": 0}

        total_new = 0
        checked = 0

        for show in monitored_shows:
            try:
                new_items = await self._check_single_show(session, show)
                total_new += new_items
                checked += 1
            except Exception:
                logger.exception(
                    "check_monitored_shows: failed for tmdb_id=%d title=%r",
                    show.tmdb_id, show.title,
                )

        await session.flush()

        logger.info(
            "check_monitored_shows: checked=%d new_items=%d",
            checked, total_new,
        )
        return {"checked": checked, "new_items": total_new}

    async def _check_single_show(
        self, session: AsyncSession, show: MonitoredShow
    ) -> int:
        """Check a single monitored show for new content.

        Args:
            session: Async database session.
            show: The MonitoredShow ORM record to check.

        Returns:
            Count of new MediaItems created.
        """
        tmdb_show = await tmdb_client.get_show_details(show.tmdb_id)
        if tmdb_show is None:
            logger.warning(
                "check_monitored_shows: TMDB returned None for tmdb_id=%d",
                show.tmdb_id,
            )
            return 0

        now = datetime.now(timezone.utc)
        today = datetime.now(timezone.utc).date()
        tmdb_id_str = str(show.tmdb_id)
        new_items = 0

        # Get all existing queue items for this show
        result = await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == tmdb_id_str)
        )
        existing_items = list(result.scalars().all())

        # Build set of existing (season, episode) pairs for dedup
        existing_keys: set[tuple[int | None, int | None]] = set()
        for item in existing_items:
            existing_keys.add((item.season, item.episode))

        # Process each season (skip Specials)
        for season_info in tmdb_show.seasons:
            if season_info.season_number == 0:
                continue
            if season_info.episode_count == 0:
                continue

            season_num = season_info.season_number

            # Skip if we already have a season pack for this season
            if (season_num, None) in existing_keys:
                continue

            # Skip seasons that haven't started airing yet
            season_air_date_str = season_info.air_date
            if season_air_date_str is None:
                continue

            try:
                season_air_date = date.fromisoformat(season_air_date_str)
            except ValueError:
                continue

            if season_air_date > today:
                continue

            if show.last_season is not None and season_num <= show.last_season:
                # We've seen this season before — only process per-episode updates
                # for the most recently tracked season
                if season_num < show.last_season:
                    continue

                # This is the current tracked season — check for new episodes
                season_detail = await tmdb_client.get_season_details(
                    show.tmdb_id, season_num
                )
                if season_detail is None:
                    continue

                for ep in season_detail.episodes:
                    if ep.air_date is None:
                        continue
                    try:
                        ep_air_date = date.fromisoformat(ep.air_date)
                    except ValueError:
                        continue
                    if ep_air_date > today:
                        continue

                    ep_num = ep.episode_number
                    if show.last_episode is not None and ep_num <= show.last_episode:
                        continue
                    if (season_num, ep_num) in existing_keys:
                        continue

                    item = MediaItem(
                        title=show.title,
                        year=show.year,
                        media_type=MediaType.SHOW,
                        tmdb_id=tmdb_id_str,
                        imdb_id=show.imdb_id,
                        state=QueueState.WANTED,
                        source="monitor",
                        added_at=now,
                        state_changed_at=now,
                        retry_count=0,
                        season=season_num,
                        episode=ep_num,
                        is_season_pack=False,
                        quality_profile=show.quality_profile,
                    )
                    session.add(item)
                    new_items += 1
                    existing_keys.add((season_num, ep_num))
                    logger.info(
                        "monitor: new episode %s S%02dE%02d (tmdb_id=%d)",
                        show.title, season_num, ep_num, show.tmdb_id,
                    )

                # Advance last_episode to the highest aired episode number
                aired_episodes = [
                    ep.episode_number
                    for ep in season_detail.episodes
                    if (ad := _parse_air_date(ep.air_date)) is not None and ad <= today
                ]
                if aired_episodes:
                    show.last_episode = max(aired_episodes)
            else:
                # New season we haven't tracked yet — add as season pack
                item = MediaItem(
                    title=show.title,
                    year=show.year,
                    media_type=MediaType.SHOW,
                    tmdb_id=tmdb_id_str,
                    imdb_id=show.imdb_id,
                    state=QueueState.WANTED,
                    source="monitor",
                    added_at=now,
                    state_changed_at=now,
                    retry_count=0,
                    season=season_num,
                    episode=None,
                    is_season_pack=True,
                    quality_profile=show.quality_profile,
                )
                session.add(item)
                new_items += 1
                existing_keys.add((season_num, None))
                logger.info(
                    "monitor: new season pack %s S%02d (tmdb_id=%d)",
                    show.title, season_num, show.tmdb_id,
                )

                show.last_season = season_num

                # Also fetch episode detail to initialise last_episode tracking
                season_detail = await tmdb_client.get_season_details(
                    show.tmdb_id, season_num
                )
                if season_detail:
                    aired_episodes = [
                        ep.episode_number
                        for ep in season_detail.episodes
                        if _parse_air_date(ep.air_date) is not None and _parse_air_date(ep.air_date) <= today
                    ]
                    if aired_episodes:
                        show.last_episode = max(aired_episodes)

        # Update tracking timestamps
        show.last_checked_at = now
        if show.last_season is None and tmdb_show.seasons:
            real_seasons = [
                s for s in tmdb_show.seasons
                if s.season_number > 0 and s.episode_count > 0
            ]
            if real_seasons:
                show.last_season = max(s.season_number for s in real_seasons)

        return new_items


# Module-level singleton
show_manager = ShowManager()
