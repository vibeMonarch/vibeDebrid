"""Discovery endpoints — browse trending/popular TMDB content and add to queue."""

from __future__ import annotations

import enum
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.deps import get_db
from src.config import settings
from src.models.media_item import MediaItem, MediaType, QueueState
from src.services.tmdb import TmdbItem, tmdb_client

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


class QueueStatusBadge(str, enum.Enum):
    IN_LIBRARY = "in_library"  # COMPLETE or DONE state
    IN_QUEUE = "in_queue"      # Any other active state
    AVAILABLE = "available"    # Not in queue at all


class DiscoverItem(BaseModel):
    """A single TMDB item enriched with queue status for the discovery UI."""

    tmdb_id: int
    title: str
    year: int | None = None
    media_type: str
    overview: str = ""
    poster_url: str | None = None  # Full URL with size prefix
    vote_average: float = 0.0
    queue_status: QueueStatusBadge = QueueStatusBadge.AVAILABLE
    queue_item_id: int | None = None
    imdb_id: str | None = None


class TrendingResponse(BaseModel):
    """Response for GET /api/discover/trending/{media_type}."""

    items: list[DiscoverItem]
    media_type: str
    time_window: str


class SearchDiscoverResponse(BaseModel):
    """Response for GET /api/discover/search."""

    items: list[DiscoverItem]
    query: str
    total_results: int
    page: int
    total_pages: int


class Genre(BaseModel):
    """A single TMDB genre."""

    id: int
    name: str


class GenreListResponse(BaseModel):
    """Response for GET /api/discover/genres/{media_type}."""

    genres: list[Genre]


class AddToQueueRequest(BaseModel):
    """Request body for POST /api/discover/add."""

    tmdb_id: int
    media_type: str  # "movie" or "tv"
    title: str
    year: int | None = None


class AddToQueueResponse(BaseModel):
    """Response for POST /api/discover/add."""

    status: str  # "created" or "exists"
    item_id: int
    imdb_id: str | None = None
    message: str


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

_LIBRARY_STATES = {QueueState.COMPLETE, QueueState.DONE}


async def _enrich_with_queue_status(
    items: list[TmdbItem], session: AsyncSession
) -> list[DiscoverItem]:
    """Convert TmdbItems to DiscoverItems, annotating each with queue status.

    Performs a single batch SELECT against media_items for all tmdb_ids so
    that even large trending lists only hit the database once.

    Args:
        items: Raw TMDB items to enrich.
        session: Active async database session.

    Returns:
        A list of DiscoverItems with queue_status and queue_item_id populated.
    """
    if not items:
        return []

    # Build a mapping of str(tmdb_id) → (item_id, state) from the database.
    tmdb_ids_str = [str(item.tmdb_id) for item in items]

    # Use a parameterised IN clause via SQLAlchemy core text with bindparams.
    # We build the placeholders manually because SQLite does not support
    # array binding via a single :param placeholder.
    placeholders = ", ".join(f":p{i}" for i in range(len(tmdb_ids_str)))
    bind_params = {f"p{i}": v for i, v in enumerate(tmdb_ids_str)}

    rows = await session.execute(
        text(
            f"SELECT id, tmdb_id, state FROM media_items WHERE tmdb_id IN ({placeholders})"
        ).bindparams(**bind_params)
    )

    # Map tmdb_id string → (db_id, state string); keep latest by db id.
    db_map: dict[str, tuple[int, str]] = {}
    for row in rows:
        row_id, row_tmdb_id, row_state = row
        if row_tmdb_id and (
            row_tmdb_id not in db_map or row_id > db_map[row_tmdb_id][0]
        ):
            db_map[row_tmdb_id] = (int(row_id), str(row_state))

    image_base = settings.tmdb.image_base_url.rstrip("/")

    enriched: list[DiscoverItem] = []
    for item in items:
        tmdb_id_str = str(item.tmdb_id)

        poster_url: str | None = None
        if item.poster_path:
            poster_url = f"{image_base}/w342{item.poster_path}"

        if tmdb_id_str in db_map:
            db_id, state_str = db_map[tmdb_id_str]
            try:
                # SQLAlchemy stores Enum by name (uppercase) in SQLite, so use
                # name-based lookup QueueState[name] rather than value-based
                # QueueState(value) to avoid always falling back to WANTED.
                state_enum = QueueState[state_str]
            except KeyError:
                state_enum = QueueState.WANTED

            if state_enum in _LIBRARY_STATES:
                badge = QueueStatusBadge.IN_LIBRARY
            else:
                badge = QueueStatusBadge.IN_QUEUE

            enriched.append(
                DiscoverItem(
                    tmdb_id=item.tmdb_id,
                    title=item.title,
                    year=item.year,
                    media_type=item.media_type,
                    overview=item.overview,
                    poster_url=poster_url,
                    vote_average=item.vote_average,
                    queue_status=badge,
                    queue_item_id=db_id,
                )
            )
        else:
            enriched.append(
                DiscoverItem(
                    tmdb_id=item.tmdb_id,
                    title=item.title,
                    year=item.year,
                    media_type=item.media_type,
                    overview=item.overview,
                    poster_url=poster_url,
                    vote_average=item.vote_average,
                    queue_status=QueueStatusBadge.AVAILABLE,
                    queue_item_id=None,
                )
            )

    return enriched


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/trending/{media_type}")
async def get_trending(
    media_type: str,
    session: AsyncSession = Depends(get_db),
) -> TrendingResponse:
    """Return trending movies or TV shows from TMDB, annotated with queue status.

    Args:
        media_type: Must be ``"movie"`` or ``"tv"``.
        session: Injected async database session.

    Returns:
        TrendingResponse with enriched items.

    Raises:
        HTTPException 400: When media_type is not ``"movie"`` or ``"tv"``.
        HTTPException 503: When the TMDB API key is not configured.
    """
    if media_type not in ("movie", "tv"):
        raise HTTPException(
            status_code=400,
            detail="media_type must be 'movie' or 'tv'",
        )

    if not settings.tmdb.api_key:
        raise HTTPException(
            status_code=503,
            detail="TMDB API key is not configured",
        )

    time_window = "week"
    tmdb_items = await tmdb_client.get_trending(media_type, time_window)

    logger.info(
        "discover.get_trending: media_type=%s window=%s items=%d",
        media_type,
        time_window,
        len(tmdb_items),
    )

    enriched = await _enrich_with_queue_status(tmdb_items, session)
    return TrendingResponse(
        items=enriched,
        media_type=media_type,
        time_window=time_window,
    )


@router.get("/search")
async def search_discover(
    q: str,
    media_type: str = "multi",
    page: int = 1,
    session: AsyncSession = Depends(get_db),
) -> SearchDiscoverResponse:
    """Search TMDB for movies and TV shows, annotated with queue status.

    Args:
        q: Search query string.
        media_type: ``"movie"``, ``"tv"``, or ``"multi"`` (default).
        page: Page number (1-based).
        session: Injected async database session.

    Returns:
        SearchDiscoverResponse with enriched items and pagination metadata.

    Raises:
        HTTPException 503: When the TMDB API key is not configured.
    """
    if not settings.tmdb.api_key:
        raise HTTPException(
            status_code=503,
            detail="TMDB API key is not configured",
        )

    if media_type not in ("movie", "tv", "multi"):
        raise HTTPException(
            status_code=400,
            detail="media_type must be 'movie', 'tv', or 'multi'",
        )

    search_result = await tmdb_client.search(q, media_type, page)

    logger.info(
        "discover.search: q=%r media_type=%s page=%d items=%d total=%d",
        q,
        media_type,
        page,
        len(search_result.items),
        search_result.total_results,
    )

    enriched = await _enrich_with_queue_status(search_result.items, session)
    return SearchDiscoverResponse(
        items=enriched,
        query=q,
        total_results=search_result.total_results,
        page=search_result.page,
        total_pages=search_result.total_pages,
    )


@router.get("/top_rated/{media_type}")
async def get_top_rated(
    media_type: str,
    session: AsyncSession = Depends(get_db),
) -> TrendingResponse:
    """Return top-rated movies or TV shows from TMDB, annotated with queue status.

    Args:
        media_type: Must be ``"movie"`` or ``"tv"``.
        session: Injected async database session.

    Returns:
        TrendingResponse with enriched items.

    Raises:
        HTTPException 400: When media_type is not ``"movie"`` or ``"tv"``.
        HTTPException 503: When the TMDB API key is not configured.
    """
    if media_type not in ("movie", "tv"):
        raise HTTPException(
            status_code=400,
            detail="media_type must be 'movie' or 'tv'",
        )

    if not settings.tmdb.api_key:
        raise HTTPException(
            status_code=503,
            detail="TMDB API key is not configured",
        )

    tmdb_items = await tmdb_client.get_top_rated(media_type)

    logger.info(
        "discover.get_top_rated: media_type=%s items=%d",
        media_type,
        len(tmdb_items),
    )

    enriched = await _enrich_with_queue_status(tmdb_items, session)
    return TrendingResponse(
        items=enriched,
        media_type=media_type,
        time_window="top_rated",
    )


@router.get("/genres/{media_type}")
async def get_genres(
    media_type: str,
) -> GenreListResponse:
    """Return the official genre list for movies or TV shows from TMDB.

    Args:
        media_type: Must be ``"movie"`` or ``"tv"``.

    Returns:
        GenreListResponse containing all genres for the given media type.

    Raises:
        HTTPException 400: When media_type is not ``"movie"`` or ``"tv"``.
        HTTPException 503: When the TMDB API key is not configured.
    """
    if media_type not in ("movie", "tv"):
        raise HTTPException(
            status_code=400,
            detail="media_type must be 'movie' or 'tv'",
        )

    if not settings.tmdb.api_key:
        raise HTTPException(
            status_code=503,
            detail="TMDB API key is not configured",
        )

    genres_raw = await tmdb_client.get_genres(media_type)
    genres: list[Genre] = []
    for g in genres_raw:
        if isinstance(g, dict) and "id" in g and "name" in g:
            genres.append(Genre(id=g["id"], name=g["name"]))
    return GenreListResponse(genres=genres)


@router.get("/by-genre/{media_type}")
async def get_by_genre(
    media_type: str,
    genre_id: int,
    page: int = 1,
    session: AsyncSession = Depends(get_db),
) -> SearchDiscoverResponse:
    """Return movies or TV shows filtered by genre from TMDB, annotated with queue status.

    Args:
        media_type: Must be ``"movie"`` or ``"tv"``.
        genre_id: The TMDB genre ID to filter by.
        page: Page number (1-based).
        session: Injected async database session.

    Returns:
        SearchDiscoverResponse with enriched items and pagination metadata.

    Raises:
        HTTPException 400: When media_type is not ``"movie"`` or ``"tv"``.
        HTTPException 503: When the TMDB API key is not configured.
    """
    if media_type not in ("movie", "tv"):
        raise HTTPException(
            status_code=400,
            detail="media_type must be 'movie' or 'tv'",
        )

    if not settings.tmdb.api_key:
        raise HTTPException(
            status_code=503,
            detail="TMDB API key is not configured",
        )

    result = await tmdb_client.discover(
        media_type,
        genre_id=genre_id,
        sort_by="popularity.desc",
        page=page,
        vote_count_gte=50,
    )

    logger.info(
        "discover.get_by_genre: media_type=%s genre_id=%d page=%d items=%d",
        media_type,
        genre_id,
        page,
        len(result.items),
    )

    enriched = await _enrich_with_queue_status(result.items, session)
    return SearchDiscoverResponse(
        items=enriched,
        query=f"genre:{genre_id}",
        total_results=result.total_results,
        page=result.page,
        total_pages=result.total_pages,
    )


@router.post("/add")
async def add_to_queue(
    body: AddToQueueRequest,
    session: AsyncSession = Depends(get_db),
) -> AddToQueueResponse:
    """Add a TMDB item to the media queue.

    Resolves the IMDb ID from TMDB external IDs before creating the queue
    entry.  If an item with the same tmdb_id already exists in the database,
    returns the existing entry without creating a duplicate.

    Args:
        body: The TMDB item to add.
        session: Injected async database session.

    Returns:
        AddToQueueResponse with status ``"created"`` or ``"exists"``.

    Raises:
        HTTPException 400: When media_type is not ``"movie"`` or ``"tv"``.
    """
    # Map TMDB media_type to internal MediaType enum.
    if body.media_type == "movie":
        db_media_type = MediaType.MOVIE
    elif body.media_type == "tv":
        db_media_type = MediaType.SHOW
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid media_type '{body.media_type}': must be 'movie' or 'tv'",
        )

    # Check for existing item by tmdb_id.
    tmdb_id_str = str(body.tmdb_id)
    existing_result = await session.execute(
        select(MediaItem).where(MediaItem.tmdb_id == tmdb_id_str)
    )
    existing = existing_result.scalar_one_or_none()
    if existing is not None:
        logger.info(
            "discover.add_to_queue: tmdb_id=%s already in queue item_id=%d state=%s",
            tmdb_id_str,
            existing.id,
            existing.state.value,
        )
        return AddToQueueResponse(
            status="exists",
            item_id=existing.id,
            imdb_id=existing.imdb_id,
            message="Already in queue",
        )

    # Resolve IMDb ID from TMDB.
    imdb_id: str | None = None
    ext_ids = await tmdb_client.get_external_ids(body.tmdb_id, body.media_type)
    if ext_ids is not None:
        imdb_id = ext_ids.imdb_id

    logger.info(
        "discover.add_to_queue: creating item tmdb_id=%s title=%r imdb_id=%s media_type=%s",
        tmdb_id_str,
        body.title,
        imdb_id,
        body.media_type,
    )

    now = datetime.now(timezone.utc)
    # TV shows default to season 1 as a season pack so the scraper starts from
    # the right place. Movies carry no season/episode context.
    if db_media_type == MediaType.SHOW:
        item_season: int | None = 1
        item_episode: int | None = None
        item_is_season_pack: bool = True
    else:
        item_season = None
        item_episode = None
        item_is_season_pack = False

    item = MediaItem(
        title=body.title,
        year=body.year,
        media_type=db_media_type,
        tmdb_id=tmdb_id_str,
        imdb_id=imdb_id,
        state=QueueState.WANTED,
        source="discover",
        added_at=now,
        state_changed_at=now,
        retry_count=0,
        season=item_season,
        episode=item_episode,
        is_season_pack=item_is_season_pack,
    )
    session.add(item)
    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        # Race condition: another request inserted the same tmdb_id concurrently.
        existing_result = await session.execute(
            select(MediaItem).where(MediaItem.tmdb_id == tmdb_id_str)
        )
        existing = existing_result.scalar_one_or_none()
        if existing is not None:
            return AddToQueueResponse(
                status="exists",
                item_id=existing.id,
                imdb_id=existing.imdb_id,
                message="Already in queue",
            )
        raise
    await session.refresh(item)

    logger.info(
        "discover.add_to_queue: created item_id=%d title=%r tmdb_id=%s imdb_id=%s",
        item.id,
        item.title,
        tmdb_id_str,
        imdb_id,
    )

    return AddToQueueResponse(
        status="created",
        item_id=item.id,
        imdb_id=imdb_id,
        message=f"Added '{body.title}' to queue",
    )


class ResolveResponse(BaseModel):
    """Response for GET /api/discover/resolve/{media_type}/{tmdb_id}."""

    imdb_id: str | None = None


@router.get("/resolve/{media_type}/{tmdb_id}")
async def resolve_imdb(
    media_type: str,
    tmdb_id: int,
) -> ResolveResponse:
    """Resolve a TMDB ID to an IMDB ID via TMDB external IDs.

    Args:
        media_type: Must be ``"movie"`` or ``"tv"``.
        tmdb_id: The TMDB ID to resolve.

    Returns:
        ResolveResponse with the IMDB ID if found.

    Raises:
        HTTPException 400: When media_type is not ``"movie"`` or ``"tv"``.
        HTTPException 503: When the TMDB API key is not configured.
    """
    if media_type not in ("movie", "tv"):
        raise HTTPException(
            status_code=400,
            detail="media_type must be 'movie' or 'tv'",
        )

    if not settings.tmdb.api_key:
        raise HTTPException(
            status_code=503,
            detail="TMDB API key is not configured",
        )

    ext_ids = await tmdb_client.get_external_ids(tmdb_id, media_type)
    imdb_id = ext_ids.imdb_id if ext_ids is not None else None

    return ResolveResponse(imdb_id=imdb_id)
