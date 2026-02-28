"""Manual search endpoints."""

import logging
import re
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.deps import get_db
from src.core.dedup import dedup_engine
from src.core.filter_engine import filter_engine
from src.models.media_item import MediaItem, MediaType, QueueState
from src.services.real_debrid import RealDebridError, rd_client
from src.services.torrentio import torrentio_client
from src.services.zilean import zilean_client

logger = logging.getLogger(__name__)

router = APIRouter()

_HASH_RE = re.compile(r"^[0-9a-fA-F]{40}$")


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


class SearchRequest(BaseModel):
    """Request body for POST /api/search."""

    query: str
    imdb_id: str | None = None
    media_type: str | None = None
    season: int | None = None
    episode: int | None = None
    quality_profile: str | None = None


class SearchResultItem(BaseModel):
    """A single ranked search result returned to the caller."""

    info_hash: str
    title: str
    resolution: str | None = None
    codec: str | None = None
    quality: str | None = None
    size_bytes: int | None = None
    seeders: int | None = None
    is_season_pack: bool = False
    cached: bool = False
    score: float = 0.0
    score_breakdown: dict[str, float] = {}


class SearchResponse(BaseModel):
    """Response body for POST /api/search."""

    results: list[SearchResultItem]
    total_raw: int
    total_filtered: int


class AddRequest(BaseModel):
    """Request body for POST /api/add."""

    magnet_or_hash: str
    title: str
    imdb_id: str
    media_type: str = "movie"
    year: int | None = None
    season: int | None = None
    episode: int | None = None
    quality_profile: str | None = None


class AddResponse(BaseModel):
    """Response body for POST /api/add."""

    status: str
    item_id: int
    rd_id: str | None = None
    message: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/search")
async def search(body: SearchRequest) -> SearchResponse:
    """Search scrapers, filter and rank results with RD cache status.

    Queries Torrentio (when imdb_id is provided) and Zilean in parallel,
    combines the results, checks Real-Debrid instant availability, then
    applies the filter engine to produce a ranked list.

    Args:
        body: Search parameters including query, optional IMDB ID, media type,
              season/episode numbers, and quality profile name.

    Returns:
        Ranked search results with cache status and scoring breakdown.
    """
    torrentio_results = []
    zilean_results = []

    # Query Torrentio — only when an IMDB ID is available because Torrentio's
    # Stremio addon URLs require a valid IMDB ID to route the request.
    if body.imdb_id:
        try:
            if body.media_type == "movie":
                torrentio_results = await torrentio_client.scrape_movie(body.imdb_id)
                logger.debug(
                    "search: torrentio returned %d movie results for imdb_id=%s",
                    len(torrentio_results),
                    body.imdb_id,
                )
            elif (
                body.media_type == "show"
                and body.season is not None
                and body.episode is not None
            ):
                torrentio_results = await torrentio_client.scrape_episode(
                    body.imdb_id, body.season, body.episode
                )
                logger.debug(
                    "search: torrentio returned %d episode results for imdb_id=%s S%02dE%02d",
                    len(torrentio_results),
                    body.imdb_id,
                    body.season,
                    body.episode,
                )
            else:
                logger.debug(
                    "search: skipping torrentio — media_type=%s requires season+episode for shows",
                    body.media_type,
                )
        except Exception as exc:
            logger.warning("search: torrentio scrape failed: %s", exc)

    # Query Zilean — works with or without an IMDB ID via keyword search.
    try:
        zilean_results = await zilean_client.search(
            query=body.query,
            season=body.season,
            episode=body.episode,
            imdb_id=body.imdb_id,
        )
        logger.debug(
            "search: zilean returned %d results for query=%r",
            len(zilean_results),
            body.query,
        )
    except Exception as exc:
        logger.warning("search: zilean search failed: %s", exc)

    combined = torrentio_results + zilean_results  # type: ignore[operator]
    total_raw = len(combined)

    if not combined:
        logger.info(
            "search: no results from any scraper for query=%r imdb_id=%s",
            body.query,
            body.imdb_id,
        )
        return SearchResponse(results=[], total_raw=0, total_filtered=0)

    # Derive cached status from Torrentio's ⚡ indicator.  When the Torrentio
    # opts URL includes an RD API key, Torrentio marks cached streams in the
    # stream name/title — no extra RD API call needed.
    cached_set: set[str] = {
        r.info_hash
        for r in combined
        if r.info_hash and getattr(r, "cached", False)
    }
    logger.debug(
        "search: %d/%d results marked as cached by scrapers",
        len(cached_set),
        len(combined),
    )

    # Filter and rank via the three-tier filter engine.
    ranked = filter_engine.filter_and_rank(
        combined,  # type: ignore[arg-type]
        profile_name=body.quality_profile,
        cached_hashes=cached_set,
    )
    total_filtered = len(ranked)

    logger.info(
        "search: query=%r raw=%d filtered=%d cached=%d",
        body.query,
        total_raw,
        total_filtered,
        len(cached_set),
    )

    results = [
        SearchResultItem(
            info_hash=fr.result.info_hash,
            title=fr.result.title,
            resolution=fr.result.resolution,
            codec=fr.result.codec,
            quality=fr.result.quality,
            size_bytes=fr.result.size_bytes,
            seeders=fr.result.seeders,
            is_season_pack=fr.result.is_season_pack,
            cached=fr.result.info_hash in cached_set,
            score=fr.score,
            score_breakdown=fr.score_breakdown,
        )
        for fr in ranked
    ]

    return SearchResponse(
        results=results,
        total_raw=total_raw,
        total_filtered=total_filtered,
    )


@router.post("/add")
async def add_torrent(
    body: AddRequest,
    session: AsyncSession = Depends(get_db),
) -> AddResponse:
    """Add a torrent to Real-Debrid and create a queue item.

    Accepts either a 40-character hex info hash or a full magnet URI.
    A MediaItem is always created regardless of whether the RD add succeeds:
    on RD failure the item is left in WANTED state so the scrape pipeline
    can retry it later.

    Args:
        body: Add parameters including the magnet/hash, title metadata,
              and optional quality profile.
        session: Injected async database session.

    Returns:
        The created AddResponse with item_id, rd_id, and status.

    Raises:
        HTTPException 400: When the input is not a valid hash or magnet URI,
                           or when media_type is not a recognised enum value.
    """
    input_val = body.magnet_or_hash.strip()

    if _HASH_RE.match(input_val):
        # Bare 40-char hex hash — wrap in a minimal magnet URI.
        info_hash: str | None = input_val.lower()
        magnet_uri = f"magnet:?xt=urn:btih:{info_hash}"
        logger.debug("add_torrent: input is bare hash=%s, constructed magnet", info_hash)
    elif input_val.startswith("magnet:"):
        magnet_uri = input_val
        hash_match = re.search(r"btih:([0-9a-fA-F]{40})", magnet_uri, re.IGNORECASE)
        info_hash = hash_match.group(1).lower() if hash_match else None
        logger.debug(
            "add_torrent: input is magnet URI, extracted info_hash=%s", info_hash
        )
    else:
        raise HTTPException(
            status_code=400,
            detail="Invalid input: must be a 40-character hex info hash or a magnet URI",
        )

    try:
        media_type = MediaType(body.media_type)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid media_type '{body.media_type}': must be 'movie' or 'show'",
        )

    # Always create the MediaItem first in ADDING state.  If the RD call
    # fails we fall back to WANTED so the pipeline can pick it up later.
    item = MediaItem(
        imdb_id=body.imdb_id,
        title=body.title,
        year=body.year,
        media_type=media_type,
        season=body.season,
        episode=body.episode,
        state=QueueState.ADDING,
        state_changed_at=datetime.now(UTC),
        quality_profile=body.quality_profile,
        retry_count=0,
    )
    session.add(item)
    # Flush to obtain item.id before the RD calls so dedup can reference it.
    await session.flush()
    logger.info(
        "add_torrent: created MediaItem id=%d title=%r state=ADDING",
        item.id,
        item.title,
    )

    rd_id: str | None = None
    try:
        add_response = await rd_client.add_magnet(magnet_uri)
        rd_id = str(add_response.get("id", "")) or None

        if rd_id:
            logger.info(
                "add_torrent: RD add_magnet succeeded rd_id=%s item_id=%d",
                rd_id,
                item.id,
            )
            try:
                await rd_client.select_files(rd_id, "all")
                logger.info(
                    "add_torrent: select_files succeeded rd_id=%s", rd_id
                )
            except RealDebridError as exc:
                # select_files failure is non-fatal — the torrent is already
                # registered with RD; the queue poller will detect its status.
                logger.warning(
                    "add_torrent: select_files failed for rd_id=%s: %s",
                    rd_id,
                    exc,
                )

            await dedup_engine.register_torrent(
                session,
                rd_id=rd_id,
                info_hash=info_hash,
                magnet_uri=magnet_uri,
                media_item_id=item.id,
                filename=body.title,
                filesize=None,
                resolution=None,
                cached=None,
            )
            logger.debug(
                "add_torrent: registered in dedup rd_id=%s info_hash=%s",
                rd_id,
                info_hash,
            )

            item.state = QueueState.CHECKING
        else:
            logger.warning(
                "add_torrent: add_magnet returned empty id for item_id=%d", item.id
            )
            item.state = QueueState.WANTED

    except RealDebridError as exc:
        logger.error(
            "add_torrent: RD add_magnet failed for item_id=%d: %s", item.id, exc
        )
        item.state = QueueState.WANTED

    await session.commit()

    if rd_id:
        status = "added"
        message = f"Torrent added to Real-Debrid (rd_id={rd_id})"
    else:
        status = "queued"
        message = "Failed to add torrent to Real-Debrid; item queued for retry"

    logger.info(
        "add_torrent: done item_id=%d rd_id=%s state=%s",
        item.id,
        rd_id,
        item.state.value,
    )

    return AddResponse(
        status=status,
        item_id=item.id,
        rd_id=rd_id,
        message=message,
    )
