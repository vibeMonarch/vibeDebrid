"""Show detail page business logic and monitoring scheduler."""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from enum import Enum

import httpx
from pydantic import BaseModel
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.core.xem_mapper import xem_mapper
from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.monitored_show import MonitoredShow
from src.services.tmdb import TmdbSeasonInfo, tmdb_client

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


class SeasonStatus(str, Enum):
    AVAILABLE = "available"
    AIRING = "airing"
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
    xem_mapped: bool = False


class SceneEpisodeInfo(BaseModel):
    """Episode info within a scene season group."""

    tmdb_season: int
    tmdb_episode: int
    scene_episode: int
    air_date: date | None = None
    has_aired: bool = False


class SceneSeasonGroup(BaseModel):
    """A group of episodes that belong to the same scene season."""

    scene_season: int
    episodes: list[SceneEpisodeInfo] = []
    total_episodes: int = 0
    aired_episodes: int = 0
    first_air_date: str | None = None
    is_complete: bool = False


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
    original_language: str | None = None


class AddSeasonsResult(BaseModel):
    """Result of adding seasons."""

    created_items: int = 0
    created_episodes: int = 0
    created_unreleased: int = 0
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

    async def _derive_scene_seasons(
        self,
        session: AsyncSession,
        tmdb_id: int,
        tvdb_id: int,
        tmdb_seasons: list[TmdbSeasonInfo],
    ) -> list[SceneSeasonGroup] | None:
        """Derive scene season groupings using XEM mappings.

        Fetches XEM mappings and TMDB episode details for each season, then
        re-groups all episodes by their scene season number.  Episodes not
        present in the XEM map retain their TMDB season/episode numbers
        (identity mapping).

        Args:
            session: Async database session.
            tmdb_id: TMDB numeric ID for the show.
            tvdb_id: TVDB numeric ID for the show (required for XEM lookup).
            tmdb_seasons: List of TmdbSeasonInfo from get_show_details().

        Returns:
            Sorted list of SceneSeasonGroup when XEM mappings exist, or None
            if XEM is disabled, down, or has no data for this show.
        """
        if not settings.xem.enabled:
            return None

        try:
            abs_map = await xem_mapper.get_absolute_scene_map(session, tvdb_id)
        except (httpx.RequestError, ValueError, TimeoutError, KeyError) as exc:
            logger.warning(
                "show_manager._derive_scene_seasons: XEM lookup failed for tvdb_id=%d, "
                "falling back to TMDB seasons: %s",
                tvdb_id,
                exc,
                exc_info=True,
            )
            return None
        if not abs_map:
            logger.debug(
                "show_manager._derive_scene_seasons: no XEM absolute mappings for "
                "tvdb_id=%d tmdb_id=%d",
                tvdb_id,
                tmdb_id,
            )
            return None

        # XEM absolute map bridges TMDB continuous numbering to scene seasons.
        # For anime with one TMDB season (S01E01-E38) but two scene seasons,
        # absolute 1→scene S01E01, absolute 29→scene S02E01, etc.
        today = datetime.now(timezone.utc).date()
        groups: dict[int, list[SceneEpisodeInfo]] = {}

        # Compute a running absolute offset across TMDB seasons so that
        # multi-season TMDB shows also map correctly.
        absolute_offset = 0

        for s in tmdb_seasons:
            if s.season_number == 0:
                continue  # Skip specials

            season_detail = await tmdb_client.get_season_details(tmdb_id, s.season_number)
            if season_detail is None:
                logger.warning(
                    "show_manager._derive_scene_seasons: could not fetch season detail "
                    "tmdb_id=%d season=%d",
                    tmdb_id,
                    s.season_number,
                )
                continue

            for ep in season_detail.episodes:
                absolute = absolute_offset + ep.episode_number
                if absolute in abs_map:
                    scene_season, scene_ep = abs_map[absolute]
                else:
                    # No XEM entry for this absolute — keep TMDB numbering.
                    scene_season, scene_ep = s.season_number, ep.episode_number

                ep_air_date = _parse_air_date(ep.air_date)
                has_aired = ep_air_date is not None and ep_air_date <= today

                ep_info = SceneEpisodeInfo(
                    tmdb_season=s.season_number,
                    tmdb_episode=ep.episode_number,
                    scene_episode=scene_ep,
                    air_date=ep_air_date,
                    has_aired=has_aired,
                )
                groups.setdefault(scene_season, []).append(ep_info)

            absolute_offset += len(season_detail.episodes)

        if not groups:
            logger.debug(
                "show_manager._derive_scene_seasons: no episode data built for tmdb_id=%d",
                tmdb_id,
            )
            return None

        result: list[SceneSeasonGroup] = []
        for scene_season_num, episodes in sorted(groups.items()):
            total = len(episodes)
            aired = sum(1 for e in episodes if e.has_aired)
            is_complete = total > 0 and aired == total

            # Earliest air date among episodes that have one.
            dated = [e.air_date for e in episodes if e.air_date is not None]
            first_air_date_str: str | None = min(dated).isoformat() if dated else None

            result.append(SceneSeasonGroup(
                scene_season=scene_season_num,
                episodes=episodes,
                total_episodes=total,
                aired_episodes=aired,
                first_air_date=first_air_date_str,
                is_complete=is_complete,
            ))

        logger.info(
            "show_manager._derive_scene_seasons: tmdb_id=%d tvdb_id=%d → %d scene seasons",
            tmdb_id,
            tvdb_id,
            len(result),
        )
        return result

    async def get_show_detail(self, session: AsyncSession, tmdb_id: int) -> ShowDetail | None:
        """Fetch show details from TMDB and cross-reference with queue.

        When XEM mappings exist for the show, seasons are restructured into
        scene seasons (torrent-site numbering) instead of TMDB seasons.  Falls
        back to TMDB-native season listing for shows without XEM data.

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

        # Determine the currently airing season number (if any).
        # next_episode_to_air being set means the show is still running and
        # a future episode has been announced for that season.
        airing_season_num: int | None = None
        if show.next_episode_to_air is not None:
            airing_season_num = show.next_episode_to_air.season_number

        # -----------------------------------------------------------------------
        # XEM-aware path: try to derive scene seasons when tvdb_id is available.
        # -----------------------------------------------------------------------
        seasons: list[SeasonInfo] = []
        scene_groups: list[SceneSeasonGroup] | None = None

        if show.tvdb_id is not None:
            scene_groups = await self._derive_scene_seasons(
                session, tmdb_id, show.tvdb_id, show.seasons
            )

        if scene_groups is not None:
            # Build a fast lookup: tmdb (season, episode) → scene_season for
            # matching existing queue items to their scene season bucket.
            tmdb_ep_to_scene_season: dict[tuple[int, int], int] = {}
            for group in scene_groups:
                for ep in group.episodes:
                    tmdb_ep_to_scene_season[(ep.tmdb_season, ep.tmdb_episode)] = group.scene_season

            for group in scene_groups:
                # Collect queue items that belong to this scene season.
                # Season packs: item.season == scene_season and is_season_pack.
                # Individual episodes: map their TMDB (season, ep) → scene season.
                scene_queue_items: list[MediaItem] = []
                for item in existing_items:
                    if item.season is None:
                        continue
                    if item.episode is None and item.is_season_pack:
                        # Season pack — keyed by scene season number.
                        if item.season == group.scene_season:
                            scene_queue_items.append(item)
                    elif item.episode is not None:
                        # Individual episode — map to scene season via XEM.
                        ep_key = (item.season, item.episode)
                        if tmdb_ep_to_scene_season.get(ep_key) == group.scene_season:
                            scene_queue_items.append(item)

                queue_item_ids = [item.id for item in scene_queue_items]

                # Determine status for this scene season group.
                # AIRING takes priority: a currently airing scene season may
                # already have some items in the library but the user still
                # needs to be able to add newly released episodes.
                if not group.is_complete and group.aired_episodes > 0:
                    # Group has aired episodes but isn't fully complete — it's airing.
                    status = SeasonStatus.AIRING
                elif any(item.state in _LIBRARY_STATES for item in scene_queue_items):
                    status = SeasonStatus.IN_LIBRARY
                elif scene_queue_items:
                    status = SeasonStatus.IN_QUEUE
                elif group.aired_episodes == 0:
                    status = SeasonStatus.UPCOMING
                else:
                    status = SeasonStatus.AVAILABLE

                seasons.append(SeasonInfo(
                    season_number=group.scene_season,
                    name=f"Season {group.scene_season}",
                    episode_count=group.total_episodes,
                    aired_episodes=group.aired_episodes,
                    air_date=group.first_air_date,
                    status=status,
                    queue_item_ids=queue_item_ids,
                    xem_mapped=True,
                ))

        else:
            # -----------------------------------------------------------------------
            # Fallback: standard TMDB season listing (unchanged from original logic).
            # -----------------------------------------------------------------------
            for s in show.seasons:
                if s.season_number == 0:
                    continue  # Skip Specials

                season_num = s.season_number
                items_for_season = season_items.get(season_num, [])
                queue_item_ids = [item.id for item in items_for_season]

                # AIRING takes priority over all queue states.
                if season_num == airing_season_num:
                    status = SeasonStatus.AIRING
                elif any(item.state in _LIBRARY_STATES for item in items_for_season):
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

                # For airing seasons, count only episodes that have actually aired.
                if status == SeasonStatus.UPCOMING:
                    aired_episodes = 0
                elif status == SeasonStatus.AIRING:
                    season_detail = await tmdb_client.get_season_details(tmdb_id, season_num)
                    if season_detail:
                        aired_episodes = sum(
                            1 for ep in season_detail.episodes
                            if (ad := _parse_air_date(ep.air_date)) is not None and ad <= today
                        )
                    else:
                        aired_episodes = s.episode_count
                else:
                    aired_episodes = s.episode_count

                seasons.append(SeasonInfo(
                    season_number=season_num,
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

    async def _add_airing_season(
        self,
        session: AsyncSession,
        request: AddSeasonsRequest,
        season_num: int,
        existing_keys: set[tuple[int | None, int | None]],
        existing_items: list[MediaItem],
        tvdb_id: int | None = None,
    ) -> tuple[int, int, int | None]:
        """Add individual episode items for a currently airing season.

        For aired episodes (air_date <= today): creates WANTED items.
        For future episodes with a known air date: creates UNRELEASED items
        with air_date set so the scheduler can promote them when ready.
        Episodes with no announced air date are skipped.

        When a completed season pack for this season already exists, episodes
        whose air date falls on or before the pack's completion date are
        considered covered by the pack and skipped.  Only episodes that aired
        after the pack was completed (new continuation episodes) are created.

        Args:
            session: Async database session (caller owns the transaction).
            request: AddSeasonsRequest with show metadata and quality profile.
            season_num: The season number that is currently airing.
            existing_keys: Set of (season, episode) tuples already in the DB,
                updated in place as new items are inserted.
            existing_items: Full list of existing MediaItems for this show,
                used to locate any completed season pack and derive a cutoff date.

        Returns:
            Tuple of (created_episodes, created_unreleased, max_aired_episode).
            max_aired_episode is the highest episode number created with state
            WANTED, or None if no WANTED episodes were created.
        """
        season_detail = await tmdb_client.get_season_details(request.tmdb_id, season_num)
        if season_detail is None:
            logger.warning(
                "show_manager._add_airing_season: could not fetch season detail "
                "tmdb_id=%d season=%d",
                request.tmdb_id, season_num,
            )
            return 0, 0, None

        tmdb_id_str = str(request.tmdb_id)
        now = datetime.now(timezone.utc)
        today = now.date()
        created_episodes = 0
        created_unreleased = 0
        max_aired_episode: int | None = None

        # Determine whether a completed season pack covers earlier episodes.
        # If so, skip episodes that aired on or before the pack completion date
        # to avoid re-downloading content already in the library.
        pack_cutoff_date: date | None = None
        for existing_item in existing_items:
            if (
                existing_item.season == season_num
                and existing_item.episode is None
                and existing_item.is_season_pack
                and existing_item.state in _LIBRARY_STATES
                and existing_item.state_changed_at is not None
            ):
                cutoff_dt = existing_item.state_changed_at
                if cutoff_dt.tzinfo is None:
                    cutoff_dt = cutoff_dt.replace(tzinfo=timezone.utc)
                pack_cutoff_date = cutoff_dt.date()
                logger.info(
                    "show_manager._add_airing_season: completed season pack for S%02d "
                    "found (completed %s), skipping episodes on or before cutoff",
                    season_num, pack_cutoff_date,
                )
                break

        for ep in season_detail.episodes:
            ep_num = ep.episode_number
            key = (season_num, ep_num)
            if key in existing_keys:
                continue

            ep_air_date = _parse_air_date(ep.air_date)
            if ep_air_date is None:
                # TMDB hasn't announced an air date — skip for now.
                continue

            # Skip episodes covered by a completed season pack.
            if pack_cutoff_date is not None and ep_air_date <= pack_cutoff_date:
                logger.debug(
                    "show_manager._add_airing_season: skipping S%02dE%02d "
                    "(aired %s, covered by pack completed %s)",
                    season_num, ep_num, ep_air_date, pack_cutoff_date,
                )
                continue

            if ep_air_date <= today:
                state = QueueState.WANTED
                air_date_value: date | None = None
            else:
                state = QueueState.UNRELEASED
                air_date_value = ep_air_date

            item = MediaItem(
                title=request.title,
                year=request.year,
                media_type=MediaType.SHOW,
                tmdb_id=tmdb_id_str,
                imdb_id=request.imdb_id,
                tvdb_id=tvdb_id,
                state=state,
                source="show_detail",
                added_at=now,
                state_changed_at=now,
                retry_count=0,
                season=season_num,
                episode=ep_num,
                is_season_pack=False,
                quality_profile=request.quality_profile,
                air_date=air_date_value,
                original_language=request.original_language,
            )
            session.add(item)
            existing_keys.add(key)

            if state == QueueState.WANTED:
                created_episodes += 1
                if max_aired_episode is None or ep_num > max_aired_episode:
                    max_aired_episode = ep_num
                logger.info(
                    "show_manager._add_airing_season: WANTED %s S%02dE%02d (tmdb_id=%s)",
                    request.title, season_num, ep_num, tmdb_id_str,
                )
            else:
                created_unreleased += 1
                logger.info(
                    "show_manager._add_airing_season: UNRELEASED %s S%02dE%02d air_date=%s (tmdb_id=%s)",
                    request.title, season_num, ep_num, ep_air_date, tmdb_id_str,
                )

        return created_episodes, created_unreleased, max_aired_episode

    async def _add_xem_airing_season(
        self,
        session: AsyncSession,
        request: AddSeasonsRequest,
        group: SceneSeasonGroup,
        existing_keys: set[tuple[int | None, int | None]],
        existing_items: list[MediaItem],
        tvdb_id: int | None,
    ) -> tuple[int, int, int | None]:
        """Add individual episode items for an XEM-remapped airing scene season.

        Similar to _add_airing_season but operates on a pre-computed
        SceneSeasonGroup.  Items are stored with TMDB season/episode numbers so
        the scrape pipeline's existing XEM resolution remaps them to scene
        numbers at scrape time.

        Args:
            session: Async database session (caller owns the transaction).
            request: AddSeasonsRequest with show metadata and quality profile.
            group: The SceneSeasonGroup representing the airing scene season.
            existing_keys: Set of (season, episode) tuples already in the DB,
                updated in place as new items are inserted.
            existing_items: Full list of existing MediaItems for this show,
                used to detect completed season packs.

        Returns:
            Tuple of (created_episodes, created_unreleased, max_aired_episode).
            max_aired_episode is the highest TMDB episode number created with
            state WANTED, or None if no WANTED episodes were created.
        """
        tmdb_id_str = str(request.tmdb_id)
        now = datetime.now(timezone.utc)
        created_episodes = 0
        created_unreleased = 0
        max_aired_episode: int | None = None

        # Check if a completed season pack for the SCENE season covers earlier episodes.
        pack_cutoff_date: date | None = None
        for existing_item in existing_items:
            if (
                existing_item.season == group.scene_season
                and existing_item.episode is None
                and existing_item.is_season_pack
                and existing_item.state in _LIBRARY_STATES
                and existing_item.state_changed_at is not None
            ):
                cutoff_dt = existing_item.state_changed_at
                if cutoff_dt.tzinfo is None:
                    cutoff_dt = cutoff_dt.replace(tzinfo=timezone.utc)
                pack_cutoff_date = cutoff_dt.date()
                logger.info(
                    "show_manager._add_xem_airing_season: completed scene season pack for S%02d "
                    "found (completed %s), skipping episodes on or before cutoff",
                    group.scene_season, pack_cutoff_date,
                )
                break

        for ep_info in group.episodes:
            # Items are stored with TMDB numbering; pipeline XEM-remaps at scrape time.
            key = (ep_info.tmdb_season, ep_info.tmdb_episode)
            if key in existing_keys:
                continue

            if ep_info.air_date is None:
                # No air date announced — skip for now.
                continue

            # Skip episodes covered by a completed season pack.
            if pack_cutoff_date is not None and ep_info.air_date <= pack_cutoff_date:
                logger.debug(
                    "show_manager._add_xem_airing_season: skipping tmdb S%02dE%02d "
                    "(aired %s, covered by pack completed %s)",
                    ep_info.tmdb_season, ep_info.tmdb_episode,
                    ep_info.air_date, pack_cutoff_date,
                )
                continue

            if ep_info.has_aired:
                state = QueueState.WANTED
                air_date_value: date | None = None
            else:
                state = QueueState.UNRELEASED
                air_date_value = ep_info.air_date

            item = MediaItem(
                title=request.title,
                year=request.year,
                media_type=MediaType.SHOW,
                tmdb_id=tmdb_id_str,
                imdb_id=request.imdb_id,
                tvdb_id=tvdb_id,
                state=state,
                source="show_detail",
                added_at=now,
                state_changed_at=now,
                retry_count=0,
                season=ep_info.tmdb_season,
                episode=ep_info.tmdb_episode,
                is_season_pack=False,
                quality_profile=request.quality_profile,
                air_date=air_date_value,
                original_language=request.original_language,
            )
            session.add(item)
            existing_keys.add(key)

            if state == QueueState.WANTED:
                created_episodes += 1
                if max_aired_episode is None or ep_info.tmdb_episode > max_aired_episode:
                    max_aired_episode = ep_info.tmdb_episode
                logger.info(
                    "show_manager._add_xem_airing_season: WANTED %s tmdb S%02dE%02d "
                    "(scene S%02dE%02d) (tmdb_id=%s)",
                    request.title,
                    ep_info.tmdb_season, ep_info.tmdb_episode,
                    group.scene_season, ep_info.scene_episode,
                    tmdb_id_str,
                )
            else:
                created_unreleased += 1
                logger.info(
                    "show_manager._add_xem_airing_season: UNRELEASED %s tmdb S%02dE%02d "
                    "air_date=%s (tmdb_id=%s)",
                    request.title,
                    ep_info.tmdb_season, ep_info.tmdb_episode,
                    ep_info.air_date, tmdb_id_str,
                )

        return created_episodes, created_unreleased, max_aired_episode

    async def add_seasons(
        self, session: AsyncSession, request: AddSeasonsRequest
    ) -> AddSeasonsResult:
        """Add selected seasons to queue as season pack or per-episode items.

        When XEM mappings exist for the show, seasons in the request are
        interpreted as scene season numbers.  Complete scene seasons are added
        as season packs (keyed by scene season number).  Airing scene seasons
        are expanded into per-episode items stored with TMDB numbering (the
        scrape pipeline handles XEM remapping at scrape time).

        For shows without XEM mappings, falls back to standard TMDB-native
        season handling: season packs for complete seasons, per-episode items
        for the currently airing season.

        Skips seasons that already have any existing MediaItem.  Optionally
        creates or updates a MonitoredShow subscription.  Automatically enables
        subscription when an airing season is added.

        Args:
            session: Async database session (caller owns the transaction).
            request: AddSeasonsRequest with seasons to add and subscription flag.

        Returns:
            AddSeasonsResult with counts of created items and skipped seasons.
        """
        tmdb_id_str = str(request.tmdb_id)
        now = datetime.now(timezone.utc)
        created = 0
        created_episodes = 0
        created_unreleased = 0
        skipped: list[int] = []
        airing_max_episode: int | None = None

        # Fetch show detail once to detect the currently airing season and tvdb_id.
        show_detail = await tmdb_client.get_show_details(request.tmdb_id)
        airing_season_num: int | None = None
        tvdb_id: int | None = show_detail.tvdb_id if show_detail else None
        if show_detail and show_detail.next_episode_to_air:
            airing_season_num = show_detail.next_episode_to_air.season_number

        # Backfill original_language from TMDB when not provided in request.
        if request.original_language is None and show_detail is not None and show_detail.original_language:
            request = request.model_copy(update={"original_language": show_detail.original_language})

        # Build existing_keys from DB for duplicate episode checks.
        result = await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == tmdb_id_str)
        )
        all_existing = list(result.scalars().all())
        existing_keys: set[tuple[int | None, int | None]] = {
            (item.season, item.episode) for item in all_existing
        }

        # -----------------------------------------------------------------------
        # XEM-aware path: derive scene seasons when tvdb_id is available.
        # -----------------------------------------------------------------------
        scene_groups: list[SceneSeasonGroup] | None = None
        if tvdb_id is not None and show_detail is not None:
            scene_groups = await self._derive_scene_seasons(
                session, request.tmdb_id, tvdb_id, show_detail.seasons
            )

        has_airing_scene = False  # tracks whether any airing scene season was added

        if scene_groups is not None:
            # Build fast lookup: scene_season → SceneSeasonGroup.
            scene_group_map: dict[int, SceneSeasonGroup] = {
                g.scene_season: g for g in scene_groups
            }

            for season_num in request.seasons:
                group = scene_group_map.get(season_num)
                if group is None:
                    logger.warning(
                        "show_manager.add_seasons: scene season %d not found for tmdb_id=%s",
                        season_num, tmdb_id_str,
                    )
                    skipped.append(season_num)
                    continue

                if group.is_complete:
                    # Complete scene season — add as season pack.
                    # Season pack uses scene season number; scraper searches S{num:02d}.
                    pack_key = (season_num, None)
                    if pack_key in existing_keys:
                        skipped.append(season_num)
                        logger.info(
                            "show_manager.add_seasons: skipping complete scene season %d "
                            "for tmdb_id=%s (already exists)",
                            season_num, tmdb_id_str,
                        )
                        continue

                    # Also check if individual episode items already exist for this
                    # scene season to avoid creating a pack alongside episodes.
                    tmdb_ep_keys_in_group = {
                        (ep.tmdb_season, ep.tmdb_episode) for ep in group.episodes
                    }
                    if any(k in existing_keys for k in tmdb_ep_keys_in_group):
                        skipped.append(season_num)
                        logger.info(
                            "show_manager.add_seasons: skipping complete scene season %d "
                            "for tmdb_id=%s (individual episodes already exist)",
                            season_num, tmdb_id_str,
                        )
                        continue

                    item = MediaItem(
                        title=request.title,
                        year=request.year,
                        media_type=MediaType.SHOW,
                        tmdb_id=tmdb_id_str,
                        imdb_id=request.imdb_id,
                        tvdb_id=tvdb_id,
                        state=QueueState.WANTED,
                        source="show_detail",
                        added_at=now,
                        state_changed_at=now,
                        retry_count=0,
                        season=season_num,
                        episode=None,
                        is_season_pack=True,
                        quality_profile=request.quality_profile,
                        original_language=request.original_language,
                    )
                    session.add(item)
                    existing_keys.add(pack_key)
                    created += 1
                    logger.info(
                        "show_manager.add_seasons: created XEM scene season pack %s S%02d "
                        "(tmdb_id=%s)",
                        request.title, season_num, tmdb_id_str,
                    )

                else:
                    # Airing or upcoming scene season — add per-episode items.
                    # Episodes stored with TMDB numbering; pipeline XEM-remaps at scrape time.
                    eps, unreleased, max_ep = await self._add_xem_airing_season(
                        session, request, group, existing_keys, all_existing, tvdb_id
                    )
                    created_episodes += eps
                    created_unreleased += unreleased
                    created += eps + unreleased
                    if max_ep is not None:
                        airing_max_episode = max(airing_max_episode or 0, max_ep)
                    has_airing_scene = True
                    logger.info(
                        "show_manager.add_seasons: XEM airing scene season %d for %s — "
                        "%d WANTED + %d UNRELEASED episodes (tmdb_id=%s)",
                        season_num, request.title, eps, unreleased, tmdb_id_str,
                    )

        else:
            # -----------------------------------------------------------------------
            # Fallback: standard TMDB-native season handling (unchanged logic).
            # -----------------------------------------------------------------------
            for season_num in request.seasons:
                has_any = any(s == season_num for (s, _) in existing_keys)
                if has_any and season_num != airing_season_num:
                    skipped.append(season_num)
                    logger.info(
                        "show_manager.add_seasons: skipping season %d for tmdb_id=%s (already exists)",
                        season_num, tmdb_id_str,
                    )
                    continue

                if season_num == airing_season_num:
                    eps, unreleased, max_ep = await self._add_airing_season(
                        session, request, season_num, existing_keys, all_existing, tvdb_id
                    )
                    created_episodes += eps
                    created_unreleased += unreleased
                    created += eps + unreleased
                    if max_ep is not None:
                        airing_max_episode = max_ep
                    logger.info(
                        "show_manager.add_seasons: airing season %d for %s — "
                        "%d WANTED + %d UNRELEASED episodes (tmdb_id=%s)",
                        season_num, request.title, eps, unreleased, tmdb_id_str,
                    )
                else:
                    item = MediaItem(
                        title=request.title,
                        year=request.year,
                        media_type=MediaType.SHOW,
                        tmdb_id=tmdb_id_str,
                        imdb_id=request.imdb_id,
                        tvdb_id=tvdb_id,
                        state=QueueState.WANTED,
                        source="show_detail",
                        added_at=now,
                        state_changed_at=now,
                        retry_count=0,
                        season=season_num,
                        episode=None,
                        is_season_pack=True,
                        quality_profile=request.quality_profile,
                        original_language=request.original_language,
                    )
                    session.add(item)
                    existing_keys.add((season_num, None))
                    created += 1
                    logger.info(
                        "show_manager.add_seasons: created season pack %s S%02d (tmdb_id=%s)",
                        request.title, season_num, tmdb_id_str,
                    )

        # Auto-subscribe when the user adds an airing season — they clearly
        # want ongoing episode tracking.
        has_airing_tmdb = airing_season_num is not None and airing_season_num in request.seasons
        should_subscribe = request.subscribe or has_airing_tmdb or has_airing_scene

        sub_status = "none"
        if should_subscribe:
            sub_status = await self._set_subscription(
                session, request.tmdb_id, request.imdb_id,
                request.title, request.year, request.quality_profile, True,
            )

            # Stamp the MonitoredShow with the episode position we just created so
            # check_monitored_shows doesn't re-process the same episodes on next run.
            if has_airing_tmdb or has_airing_scene:
                sub_result = await session.execute(
                    select(MonitoredShow).where(MonitoredShow.tmdb_id == request.tmdb_id)
                )
                monitored = sub_result.scalar_one_or_none()
                if monitored is not None:
                    if has_airing_tmdb and airing_season_num is not None:
                        monitored.last_season = airing_season_num
                    elif has_airing_scene and airing_max_episode is not None:
                        # XEM airing path: stamp with the TMDB season number
                        # of the created episodes so check_monitored_shows
                        # doesn't re-create them.  All XEM airing episodes
                        # share the same TMDB season (the original continuous one).
                        if scene_groups is not None:
                            tmdb_seasons_used = {
                                ep.tmdb_season
                                for g in scene_groups
                                for ep in g.episodes
                                if not g.is_complete
                            }
                            if tmdb_seasons_used:
                                monitored.last_season = max(tmdb_seasons_used)
                    if airing_max_episode is not None:
                        monitored.last_episode = airing_max_episode
                    logger.info(
                        "show_manager.add_seasons: stamped monitored show tmdb_id=%d "
                        "last_season=%s last_episode=%s",
                        request.tmdb_id,
                        monitored.last_season,
                        airing_max_episode,
                    )

        await session.flush()

        return AddSeasonsResult(
            created_items=created,
            created_episodes=created_episodes,
            created_unreleased=created_unreleased,
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
            except Exception as exc:
                logger.error(
                    "check_monitored_shows: failed for tmdb_id=%d title=%r: %s",
                    show.tmdb_id, show.title, exc, exc_info=True,
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
        tvdb_id: int | None = tmdb_show.tvdb_id
        new_items = 0

        # Get all existing queue items for this show.
        # Also match by imdb_id when tmdb_id is absent — covers migrated items
        # that may not yet have been backfilled with a tmdb_id.
        conditions = [MediaItem.tmdb_id == tmdb_id_str]
        if tmdb_show.imdb_id:
            conditions.append(
                (MediaItem.imdb_id == tmdb_show.imdb_id) & (MediaItem.tmdb_id.is_(None))
            )
        result = await session.execute(
            select(MediaItem).where(or_(*conditions))
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
                        # Check for stuck UNRELEASED items with NULL air_date.
                        # TMDB may not have had the air date when the item was
                        # created; now it does and the episode has aired — advance.
                        existing_item = next(
                            (i for i in existing_items
                             if i.season == season_num and i.episode == ep_num
                             and i.state == QueueState.UNRELEASED),
                            None,
                        )
                        if existing_item is not None and existing_item.air_date is None:
                            existing_item.air_date = ep_air_date
                            existing_item.state = QueueState.WANTED
                            existing_item.state_changed_at = now
                            new_items += 1
                            logger.info(
                                "monitor: advanced stuck UNRELEASED %s S%02dE%02d to WANTED "
                                "(air_date was NULL, now %s, tmdb_id=%d)",
                                show.title, season_num, ep_num, ep_air_date, show.tmdb_id,
                            )
                        continue

                    _orig_lang = tmdb_show.original_language if isinstance(tmdb_show.original_language, str) else None
                    item = MediaItem(
                        title=show.title,
                        year=show.year,
                        media_type=MediaType.SHOW,
                        tmdb_id=tmdb_id_str,
                        imdb_id=show.imdb_id,
                        tvdb_id=tvdb_id,
                        state=QueueState.WANTED,
                        source="monitor",
                        added_at=now,
                        state_changed_at=now,
                        retry_count=0,
                        season=season_num,
                        episode=ep_num,
                        is_season_pack=False,
                        quality_profile=show.quality_profile,
                        original_language=_orig_lang,
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
                # Belt-and-suspenders: if individual episode items already exist
                # for this season (e.g. created by add_seasons for an airing
                # season), skip creating a season pack to avoid duplicates.
                has_episodes = any(
                    e is not None for (s, e) in existing_keys if s == season_num
                )
                if has_episodes:
                    continue

                # New season we haven't tracked yet — add as season pack
                _orig_lang = tmdb_show.original_language if isinstance(tmdb_show.original_language, str) else None
                item = MediaItem(
                    title=show.title,
                    year=show.year,
                    media_type=MediaType.SHOW,
                    tmdb_id=tmdb_id_str,
                    imdb_id=show.imdb_id,
                    tvdb_id=tvdb_id,
                    state=QueueState.WANTED,
                    source="monitor",
                    added_at=now,
                    state_changed_at=now,
                    retry_count=0,
                    season=season_num,
                    episode=None,
                    is_season_pack=True,
                    quality_profile=show.quality_profile,
                    original_language=_orig_lang,
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
                        if (ad := _parse_air_date(ep.air_date)) is not None and ad <= today
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
