"""Movie detail API routes."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.config import settings
from src.services.tmdb import tmdb_client

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Response schema
# ---------------------------------------------------------------------------


class MovieDetailResponse(BaseModel):
    """Movie detail response returned by GET /api/movie/{tmdb_id}."""

    tmdb_id: int
    title: str
    year: int | None = None
    overview: str = ""
    poster_url: str | None = None   # full URL with TMDB image size prefix
    backdrop_url: str | None = None
    vote_average: float = 0.0
    runtime: int | None = None      # runtime in minutes; None when not yet known
    genres: list[dict] = []
    imdb_id: str | None = None
    original_language: str | None = None
    original_title: str | None = None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/{tmdb_id}")
async def get_movie_detail(tmdb_id: int) -> MovieDetailResponse:
    """Fetch movie details from TMDB for the movie detail page.

    Uses ``append_to_response=external_ids`` to obtain the IMDB ID in a single
    API call.  The poster and backdrop paths returned by TMDB are expanded to
    full URLs using the configured image base URL.

    Args:
        tmdb_id: The TMDB numeric identifier for the movie.

    Returns:
        MovieDetailResponse with all available metadata fields.

    Raises:
        HTTPException 503: When the TMDB API key is not configured.
        HTTPException 404: When TMDB has no entry for the given tmdb_id.
    """
    if not settings.tmdb.api_key:
        raise HTTPException(status_code=503, detail="TMDB API key is not configured")

    detail = await tmdb_client.get_movie_details_full(tmdb_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Movie not found on TMDB")

    image_base = settings.tmdb.image_base_url.rstrip("/")

    poster_url: str | None = None
    if detail.poster_path:
        poster_url = f"{image_base}/w342{detail.poster_path}"

    backdrop_url: str | None = None
    if detail.backdrop_path:
        backdrop_url = f"{image_base}/w1280{detail.backdrop_path}"

    logger.info(
        "movie.get_movie_detail: tmdb_id=%d title=%r imdb_id=%s",
        tmdb_id, detail.title, detail.imdb_id,
    )

    return MovieDetailResponse(
        tmdb_id=detail.tmdb_id,
        title=detail.title,
        year=detail.year,
        overview=detail.overview,
        poster_url=poster_url,
        backdrop_url=backdrop_url,
        vote_average=detail.vote_average,
        runtime=detail.runtime,
        genres=detail.genres,
        imdb_id=detail.imdb_id,
        original_language=detail.original_language,
        original_title=detail.original_title,
    )
