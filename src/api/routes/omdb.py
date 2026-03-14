"""OMDb ratings API endpoints."""

from __future__ import annotations

import logging
import re

from fastapi import APIRouter

from src.services.omdb import OmdbRatings, omdb_client

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/{imdb_id}")
async def get_omdb_ratings(imdb_id: str) -> OmdbRatings | dict:
    """Fetch OMDb ratings for a given IMDb ID.

    Returns an OmdbRatings object on success, or an empty dict when OMDb is
    disabled, the ID is invalid, or a network error occurs.

    Args:
        imdb_id: An IMDb identifier such as ``tt0133093``.  Must start with
                 ``"tt"`` — requests with other patterns are rejected with an
                 empty response rather than a 4xx so callers can treat the
                 result uniformly.
    """
    if not re.fullmatch(r"tt\d{7,10}", imdb_id):
        logger.debug("omdb route: rejected invalid imdb_id=%r", imdb_id)
        return {}

    ratings = await omdb_client.get_ratings(imdb_id)
    if ratings is None:
        return {}
    return ratings
