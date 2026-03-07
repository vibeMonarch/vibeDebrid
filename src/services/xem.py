"""Async client for TheXEM scene numbering API.

TheXEM provides mappings between different episode numbering schemes
(TVDB, scene, AniDB). Used to convert TMDB/TVDB numbering to scene
numbering for anime and other shows where torrent sites use different
season/episode numbers than metadata databases.

Design notes:
- Stateless client: each public method opens/closes its own httpx session.
- No authentication required.
- All public methods swallow network/API failures and return empty results.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
from pydantic import BaseModel

from src.config import settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class XemMapping(BaseModel):
    """A single episode mapping between TVDB and scene numbering."""

    tvdb_season: int
    tvdb_episode: int
    scene_season: int
    scene_episode: int


class XemShowMappings(BaseModel):
    """All mappings for a single show."""

    tvdb_id: int
    mappings: list[XemMapping] = []


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class XemClient:
    """Async client for TheXEM API.

    This client is intentionally stateless — no persistent HTTP session is kept
    between calls so that config changes (base_url, timeout) take effect without
    a restart.  Each public method opens and closes its own httpx client.

    All public methods swallow network/API failures, log them, and return empty
    results so that a XEM outage never crashes the scrape pipeline.
    """

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_client(self) -> httpx.AsyncClient:
        """Create a new httpx.AsyncClient pointed at the XEM API."""
        cfg = settings.xem
        return httpx.AsyncClient(
            base_url=cfg.base_url.rstrip("/"),
            timeout=cfg.timeout_seconds,
            headers={"User-Agent": "vibeDebrid/0.1"},
            follow_redirects=True,
        )

    def _handle_error_status(self, response: httpx.Response, context: str) -> bool:
        """Log error status codes and return True when the caller should abort.

        Args:
            response: The httpx response object.
            context: A short description of the call site for log messages.

        Returns:
            True if the caller should abort and return empty, False if the
            response is a success.
        """
        if response.status_code == 429:
            logger.warning("xem.%s: rate limited (429)", context)
            return True

        if response.status_code >= 500:
            logger.error(
                "xem.%s: server error %d body=%s",
                context,
                response.status_code,
                response.text[:200],
            )
            return True

        if not response.is_success:
            logger.error(
                "xem.%s: unexpected status %d",
                context,
                response.status_code,
            )
            return True

        return False

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    async def get_show_mappings(self, tvdb_id: int) -> XemShowMappings:
        """Fetch all episode mappings for a show from XEM.

        Queries /map/all with origin=tvdb to get TVDB->scene mappings.
        Only entries where TVDB and scene numbers actually differ are included
        in the returned mappings list.

        Args:
            tvdb_id: The TVDB numeric identifier for the show.

        Returns:
            XemShowMappings with parsed mappings, or empty mappings on failure.
        """
        if not settings.xem.enabled:
            logger.debug("xem.get_show_mappings: XEM disabled, skipping")
            return XemShowMappings(tvdb_id=tvdb_id)

        params: dict[str, Any] = {"id": tvdb_id, "origin": "tvdb"}

        try:
            async with self._build_client() as client:
                response = await client.get("/map/all", params=params)
        except httpx.ConnectError as exc:
            logger.warning(
                "xem.get_show_mappings: connection error tvdb_id=%d (%s)",
                tvdb_id,
                exc,
            )
            return XemShowMappings(tvdb_id=tvdb_id)
        except httpx.TimeoutException as exc:
            logger.warning(
                "xem.get_show_mappings: request timed out tvdb_id=%d (%s)",
                tvdb_id,
                exc,
            )
            return XemShowMappings(tvdb_id=tvdb_id)
        except httpx.RequestError as exc:
            logger.warning(
                "xem.get_show_mappings: network error tvdb_id=%d (%s)",
                tvdb_id,
                exc,
            )
            return XemShowMappings(tvdb_id=tvdb_id)

        if self._handle_error_status(response, "get_show_mappings"):
            return XemShowMappings(tvdb_id=tvdb_id)

        try:
            data: dict[str, Any] = response.json()
        except ValueError as exc:
            logger.error(
                "xem.get_show_mappings: malformed JSON tvdb_id=%d (%s)", tvdb_id, exc
            )
            return XemShowMappings(tvdb_id=tvdb_id)

        if data.get("result") != "success":
            logger.warning(
                "xem.get_show_mappings: non-success result tvdb_id=%d message=%r",
                tvdb_id,
                data.get("message", ""),
            )
            return XemShowMappings(tvdb_id=tvdb_id)

        raw_entries: list[Any] = data.get("data") or []
        mappings: list[XemMapping] = []

        for entry in raw_entries:
            if not isinstance(entry, dict):
                continue

            tvdb_block = entry.get("tvdb")
            scene_block = entry.get("scene")

            if not isinstance(tvdb_block, dict) or not isinstance(scene_block, dict):
                continue

            tvdb_season = tvdb_block.get("season")
            tvdb_episode = tvdb_block.get("episode")
            scene_season = scene_block.get("season")
            scene_episode = scene_block.get("episode")

            # Skip entries with missing or non-integer season/episode values.
            if not all(
                isinstance(v, int)
                for v in (tvdb_season, tvdb_episode, scene_season, scene_episode)
            ):
                continue

            # Only include entries where TVDB and scene numbers actually differ.
            if tvdb_season == scene_season and tvdb_episode == scene_episode:
                continue

            mappings.append(
                XemMapping(
                    tvdb_season=tvdb_season,
                    tvdb_episode=tvdb_episode,
                    scene_season=scene_season,
                    scene_episode=scene_episode,
                )
            )

        logger.debug(
            "xem.get_show_mappings: tvdb_id=%d entries=%d mappings=%d",
            tvdb_id,
            len(raw_entries),
            len(mappings),
        )
        return XemShowMappings(tvdb_id=tvdb_id, mappings=mappings)

    async def get_shows_with_mappings(self) -> set[int]:
        """Fetch the list of TVDB IDs that have XEM mappings.

        Queries /map/havemap with origin=tvdb. This is useful as a pre-check
        before calling get_show_mappings to avoid unnecessary API calls for
        shows that are not indexed in XEM.

        Returns:
            Set of TVDB IDs, or empty set on failure.
        """
        if not settings.xem.enabled:
            logger.debug("xem.get_shows_with_mappings: XEM disabled, skipping")
            return set()

        try:
            async with self._build_client() as client:
                response = await client.get(
                    "/map/havemap", params={"origin": "tvdb"}
                )
        except httpx.ConnectError as exc:
            logger.warning(
                "xem.get_shows_with_mappings: connection error (%s)", exc
            )
            return set()
        except httpx.TimeoutException as exc:
            logger.warning(
                "xem.get_shows_with_mappings: request timed out (%s)", exc
            )
            return set()
        except httpx.RequestError as exc:
            logger.warning(
                "xem.get_shows_with_mappings: network error (%s)", exc
            )
            return set()

        if self._handle_error_status(response, "get_shows_with_mappings"):
            return set()

        try:
            data: dict[str, Any] = response.json()
        except ValueError as exc:
            logger.error(
                "xem.get_shows_with_mappings: malformed JSON (%s)", exc
            )
            return set()

        if data.get("result") != "success":
            logger.warning(
                "xem.get_shows_with_mappings: non-success result message=%r",
                data.get("message", ""),
            )
            return set()

        raw_ids: list[Any] = data.get("data") or []
        tvdb_ids: set[int] = {
            entry for entry in raw_ids if isinstance(entry, int)
        }

        logger.debug(
            "xem.get_shows_with_mappings: found %d shows with mappings",
            len(tvdb_ids),
        )
        return tvdb_ids


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

xem_client = XemClient()
