"""Jellyfin Media Server API client for vibeDebrid."""

from __future__ import annotations

import logging

import httpx
from pydantic import BaseModel

from src.config import settings
from src.services.http_client import CircuitOpenError, get_circuit_breaker, get_client

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class JellyfinError(Exception):
    """Base exception for Jellyfin API errors."""


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class JellyfinLibrary(BaseModel):
    """A Jellyfin library folder."""

    library_id: str
    name: str
    collection_type: str


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class JellyfinClient:
    """Async client for the Jellyfin Media Server API.

    All public methods swallow network/API failures, log them, and return safe
    defaults (False / []) so that a Jellyfin outage never crashes the queue.
    """

    def _headers(self, api_key: str | None = None) -> dict[str, str]:
        """Build Jellyfin auth headers.

        Args:
            api_key: Override the API key.  When None, uses
                     ``settings.jellyfin.api_key``.

        Returns:
            A dict with the ``X-Emby-Token`` header, or an empty dict when no
            key is configured.
        """
        key = api_key or settings.jellyfin.api_key
        return {"X-Emby-Token": key} if key else {}

    async def test_connection(self) -> bool:
        """Test connectivity to the Jellyfin server.

        Sends a GET request to ``/System/Info`` and checks for HTTP 200.
        Bypasses the circuit breaker (used for one-off settings checks).

        Returns:
            True if the server is reachable and the key is valid, False
            otherwise.
        """
        url = settings.jellyfin.url.rstrip("/")
        if not url or not settings.jellyfin.api_key:
            return False
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(
                    f"{url}/System/Info",
                    headers=self._headers(),
                )
                if resp.status_code == 200:
                    logger.info("Jellyfin connection test succeeded: %s", url)
                    return True
                logger.warning(
                    "Jellyfin connection test failed: HTTP %d", resp.status_code
                )
                return False
        except httpx.ConnectError as exc:
            logger.warning("Jellyfin connection test failed: %s", exc)
            return False
        except httpx.TimeoutException as exc:
            logger.warning("Jellyfin connection test timed out: %s", exc)
            return False
        except httpx.RequestError as exc:
            logger.warning("Jellyfin connection test request error: %s", exc)
            return False

    async def get_libraries(self) -> list[JellyfinLibrary]:
        """Fetch library folders from Jellyfin.

        Calls ``GET /Library/VirtualFolders`` and filters the result to movie
        and TV show libraries only.

        Returns:
            A list of JellyfinLibrary objects, or an empty list on any failure.
        """
        url = settings.jellyfin.url.rstrip("/")
        if not url or not settings.jellyfin.api_key:
            return []
        cb = get_circuit_breaker("jellyfin")
        try:
            await cb.before_request()
        except CircuitOpenError:
            logger.warning("Jellyfin circuit breaker is open, skipping get_libraries")
            return []

        try:
            client = await get_client(
                "jellyfin",
                url,
                headers=self._headers(),
                timeout=10.0,
            )
            resp = await client.get("/Library/VirtualFolders")
        except httpx.ConnectError as exc:
            await cb.record_failure()
            logger.warning("Jellyfin get_libraries: connection error (%s)", exc)
            return []
        except httpx.TimeoutException as exc:
            await cb.record_failure()
            logger.warning("Jellyfin get_libraries: request timed out (%s)", exc)
            return []
        except httpx.RequestError as exc:
            await cb.record_failure()
            logger.warning("Jellyfin get_libraries: network error (%s)", exc)
            return []

        if resp.status_code != 200:
            await cb.record_failure()
            logger.warning(
                "Jellyfin get_libraries failed: HTTP %d", resp.status_code
            )
            return []

        await cb.record_success()

        try:
            data = resp.json()
        except (ValueError, KeyError) as exc:
            logger.error("Jellyfin get_libraries: malformed JSON (%s)", exc)
            return []

        libraries: list[JellyfinLibrary] = []
        if not isinstance(data, list):
            logger.warning("Jellyfin get_libraries: unexpected response type %s", type(data).__name__)
            return []
        for folder in data:
            ct = folder.get("CollectionType", "")
            if ct in ("movies", "tvshows"):
                libraries.append(
                    JellyfinLibrary(
                        library_id=folder.get("ItemId", ""),
                        name=folder.get("Name", ""),
                        collection_type=ct,
                    )
                )

        logger.debug("Jellyfin get_libraries: found %d qualifying libraries", len(libraries))
        return libraries

    async def scan_library(self, library_id: str) -> bool:
        """Trigger a library scan in Jellyfin.

        Calls ``POST /Items/{library_id}/Refresh``.

        Args:
            library_id: The GUID of the library to refresh.

        Returns:
            True on success (HTTP 200 or 204), False on any failure.
        """
        url = settings.jellyfin.url.rstrip("/")
        if not url or not settings.jellyfin.api_key:
            return False
        cb = get_circuit_breaker("jellyfin")
        try:
            await cb.before_request()
        except CircuitOpenError:
            logger.warning(
                "Jellyfin circuit breaker is open, skipping scan_library %s",
                library_id,
            )
            return False

        try:
            client = await get_client(
                "jellyfin",
                url,
                headers=self._headers(),
                timeout=30.0,
            )
            resp = await client.post(
                f"/Items/{library_id}/Refresh",
                params={"metadataRefreshMode": "FullRefresh"},
            )
        except httpx.ConnectError as exc:
            await cb.record_failure()
            logger.warning(
                "Jellyfin scan_library: connection error for %s (%s)", library_id, exc
            )
            return False
        except httpx.TimeoutException as exc:
            await cb.record_failure()
            logger.warning(
                "Jellyfin scan_library: request timed out for %s (%s)", library_id, exc
            )
            return False
        except httpx.RequestError as exc:
            await cb.record_failure()
            logger.warning(
                "Jellyfin scan_library: network error for %s (%s)", library_id, exc
            )
            return False

        if resp.status_code in (200, 204):
            await cb.record_success()
            logger.info("Jellyfin scan triggered for library %s", library_id)
            return True

        await cb.record_failure()
        logger.warning(
            "Jellyfin scan_library failed: HTTP %d for %s",
            resp.status_code,
            library_id,
        )
        return False


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

jellyfin_client = JellyfinClient()
