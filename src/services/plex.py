"""Async client for the Plex Media Server API.

Used to trigger library scans after symlinks are created, and to support
OAuth PIN-based authentication for obtaining a Plex token.

Design notes:
- Stateless client: each public method opens and closes its own httpx session
  so that config changes (url, token) take effect without a restart.
- All public methods swallow network/API failures, log them, and return safe
  defaults so that a Plex outage never crashes the queue or routes.
- OAuth flow (create_pin / check_pin) targets plex.tv, not the local server.
"""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urlencode

import httpx
from pydantic import BaseModel

from src.config import settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class PlexError(Exception):
    """Base exception for Plex client errors."""


class PlexAuthError(PlexError):
    """Raised when Plex returns a 401/403 response."""


class PlexConnectionError(PlexError):
    """Raised when the Plex server is unreachable."""


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class PlexLibrarySection(BaseModel):
    """A single Plex library section (e.g. 'Movies' or 'TV Shows')."""

    section_id: int
    title: str
    type: str


class PlexPinResponse(BaseModel):
    """Response from the Plex PIN creation endpoint."""

    pin_id: int
    code: str
    auth_url: str


class WatchlistItem(BaseModel):
    """A single item from the Plex watchlist."""

    title: str
    year: int | None = None
    media_type: str  # "movie" or "show"
    tmdb_id: str | None = None
    imdb_id: str | None = None


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

_PLEX_CLIENT_ID = "f0f4c4b8-a6d3-4c2e-b9a7-3e1d5f8a9b0c"
_PLEX_PRODUCT = "vibeDebrid"
_PLEX_VERSION = "0.1.0"


class PlexClient:
    """Async client for the Plex Media Server and plex.tv APIs.

    This client is intentionally stateless — no persistent HTTP session is kept
    between calls so that config changes are picked up without restart.  Each
    public method opens and closes its own httpx client.

    All public methods swallow network/API failures, log them, and return safe
    defaults (False / [] / None) so that a Plex outage never crashes the queue.
    """

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_client(
        self, base_url: str | None = None, timeout: int = 10
    ) -> httpx.AsyncClient:
        """Create a new httpx.AsyncClient for the Plex server.

        Args:
            base_url: Override the target base URL.  When None, uses
                      ``settings.plex.url``.
            timeout: Request timeout in seconds.

        Returns:
            A configured httpx.AsyncClient with Plex auth headers.
        """
        resolved_url = (base_url or settings.plex.url).rstrip("/")
        return httpx.AsyncClient(
            base_url=resolved_url,
            timeout=timeout,
            headers={
                "X-Plex-Token": settings.plex.token,
                "Accept": "application/json",
                "X-Plex-Client-Identifier": _PLEX_CLIENT_ID,
                "X-Plex-Product": _PLEX_PRODUCT,
            },
            follow_redirects=True,
        )

    def _build_plex_tv_client(self, timeout: int = 10) -> httpx.AsyncClient:
        """Create a new httpx.AsyncClient targeting plex.tv (no token header).

        This is used for the OAuth PIN flow where the token is not yet known.

        Args:
            timeout: Request timeout in seconds.

        Returns:
            A configured httpx.AsyncClient without X-Plex-Token.
        """
        return httpx.AsyncClient(
            base_url="https://plex.tv",
            timeout=timeout,
            headers={
                "Accept": "application/json",
                "X-Plex-Client-Identifier": _PLEX_CLIENT_ID,
                "X-Plex-Product": _PLEX_PRODUCT,
            },
            follow_redirects=True,
        )

    def _handle_auth_error(self, status_code: int, context: str) -> None:
        """Raise PlexAuthError for 401/403 responses.

        Args:
            status_code: HTTP response status code.
            context: A short description of the call site for log messages.

        Raises:
            PlexAuthError: If status_code is 401 or 403.
        """
        if status_code in (401, 403):
            raise PlexAuthError(
                f"plex.{context}: authentication failure (HTTP {status_code}) "
                "— check Plex token"
            )

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    async def test_connection(self) -> bool:
        """Test connectivity to the configured Plex server.

        Sends a GET request to the server root and checks for HTTP 200.

        Returns:
            True if the server is reachable and the token is valid, False
            otherwise.
        """
        try:
            async with self._build_client() as client:
                response = await client.get("/")
        except httpx.ConnectError as exc:
            logger.warning("plex.test_connection: connection error (%s)", exc)
            return False
        except httpx.TimeoutException as exc:
            logger.warning("plex.test_connection: request timed out (%s)", exc)
            return False
        except httpx.RequestError as exc:
            logger.warning("plex.test_connection: network error (%s)", exc)
            return False

        if response.status_code == 200:
            logger.debug("plex.test_connection: success")
            return True

        logger.warning(
            "plex.test_connection: unexpected status %d", response.status_code
        )
        return False

    async def get_libraries(self) -> list[PlexLibrarySection]:
        """Fetch all library sections from the Plex server.

        Calls ``GET /library/sections`` and parses the ``MediaContainer.Directory``
        array from the JSON response.

        Returns:
            A list of PlexLibrarySection objects, or an empty list on any
            failure.
        """
        try:
            async with self._build_client() as client:
                response = await client.get("/library/sections")
        except httpx.ConnectError as exc:
            logger.warning("plex.get_libraries: connection error (%s)", exc)
            return []
        except httpx.TimeoutException as exc:
            logger.warning("plex.get_libraries: request timed out (%s)", exc)
            return []
        except httpx.RequestError as exc:
            logger.warning("plex.get_libraries: network error (%s)", exc)
            return []

        try:
            self._handle_auth_error(response.status_code, "get_libraries")
        except PlexAuthError:
            logger.error(
                "plex.get_libraries: authentication failure — check Plex token"
            )
            return []

        if not response.is_success:
            logger.error(
                "plex.get_libraries: unexpected status %d body=%s",
                response.status_code,
                response.text[:200],
            )
            return []

        try:
            data: dict[str, Any] = response.json()
        except (ValueError, KeyError) as exc:
            logger.error("plex.get_libraries: malformed JSON (%s)", exc)
            return []

        container: dict[str, Any] = data.get("MediaContainer") or {}
        directories: list[dict[str, Any]] = container.get("Directory") or []

        sections: list[PlexLibrarySection] = []
        for directory in directories:
            try:
                section = PlexLibrarySection(
                    section_id=int(directory["key"]),
                    title=str(directory.get("title") or ""),
                    type=str(directory.get("type") or ""),
                )
                sections.append(section)
            except (KeyError, ValueError, TypeError) as exc:
                logger.warning(
                    "plex.get_libraries: could not parse directory entry %r (%s)",
                    directory,
                    exc,
                )

        logger.debug("plex.get_libraries: found %d sections", len(sections))
        return sections

    async def scan_section(
        self, section_id: int, path: str | None = None
    ) -> bool:
        """Trigger a library scan for a specific Plex section.

        Calls ``GET /library/sections/{section_id}/refresh``.  An optional
        ``path`` parameter can restrict the scan to a specific directory.

        Args:
            section_id: The numeric Plex library section ID.
            path: Optional filesystem path to restrict the scan scope.

        Returns:
            True if the scan was accepted (HTTP 200), False otherwise.
        """
        params: dict[str, Any] = {}
        if path is not None:
            params["path"] = path

        try:
            async with self._build_client() as client:
                response = await client.get(
                    f"/library/sections/{section_id}/refresh",
                    params=params,
                )
        except httpx.ConnectError as exc:
            logger.warning(
                "plex.scan_section: connection error section_id=%d (%s)",
                section_id,
                exc,
            )
            return False
        except httpx.TimeoutException as exc:
            logger.warning(
                "plex.scan_section: request timed out section_id=%d (%s)",
                section_id,
                exc,
            )
            return False
        except httpx.RequestError as exc:
            logger.warning(
                "plex.scan_section: network error section_id=%d (%s)",
                section_id,
                exc,
            )
            return False

        if response.status_code == 200:
            logger.debug(
                "plex.scan_section: accepted section_id=%d path=%s",
                section_id,
                path,
            )
            return True

        logger.warning(
            "plex.scan_section: unexpected status %d for section_id=%d",
            response.status_code,
            section_id,
        )
        return False

    async def create_pin(self) -> PlexPinResponse | None:
        """Create a Plex OAuth PIN for browser-based authentication.

        Posts to ``https://plex.tv/api/v2/pins`` with ``strong=true``.  No
        token is required — this is the pre-authentication step.

        Returns:
            A PlexPinResponse with the PIN id, code, and browser auth URL, or
            None on any failure.
        """
        try:
            async with self._build_plex_tv_client() as client:
                response = await client.post(
                    "/api/v2/pins",
                    data={"strong": "true"},
                )
        except httpx.ConnectError as exc:
            logger.warning("plex.create_pin: connection error (%s)", exc)
            return None
        except httpx.TimeoutException as exc:
            logger.warning("plex.create_pin: request timed out (%s)", exc)
            return None
        except httpx.RequestError as exc:
            logger.warning("plex.create_pin: network error (%s)", exc)
            return None

        if not response.is_success:
            logger.error(
                "plex.create_pin: unexpected status %d body=%s",
                response.status_code,
                response.text[:200],
            )
            return None

        try:
            data: dict[str, Any] = response.json()
        except (ValueError, KeyError) as exc:
            logger.error("plex.create_pin: malformed JSON (%s)", exc)
            return None

        pin_id_raw = data.get("id")
        code = data.get("code")
        if not isinstance(pin_id_raw, int) or not code:
            logger.error(
                "plex.create_pin: unexpected response shape %r", data
            )
            return None

        pin_id = int(pin_id_raw)
        auth_params = urlencode(
            {
                "clientID": _PLEX_CLIENT_ID,
                "code": code,
                "context[device][product]": _PLEX_PRODUCT,
                "context[device][version]": _PLEX_VERSION,
                "context[device][platform]": "Web",
                "context[device][platformVersion]": "1.0",
                "context[device][device]": "Browser",
                "context[device][deviceName]": "vibeDebrid_browser",
                "context[device][model]": "hosted",
                "context[device][screenResolution]": "1920x1080",
                "context[device][language]": "en",
            }
        )
        auth_url = f"https://app.plex.tv/auth#?{auth_params}"

        logger.debug("plex.create_pin: created pin_id=%d", pin_id)
        return PlexPinResponse(pin_id=pin_id, code=code, auth_url=auth_url)

    async def check_pin(self, pin_id: int) -> str | None:
        """Poll a Plex OAuth PIN to see if the user has authenticated.

        Calls ``GET https://plex.tv/api/v2/pins/{pin_id}`` and returns the
        ``authToken`` field if the user has completed the auth flow.

        Args:
            pin_id: The numeric PIN id returned by ``create_pin``.

        Returns:
            The Plex auth token string if authentication is complete, or None
            if still pending or on any failure.
        """
        try:
            async with self._build_plex_tv_client() as client:
                response = await client.get(f"/api/v2/pins/{pin_id}")
        except httpx.ConnectError as exc:
            logger.warning(
                "plex.check_pin: connection error pin_id=%d (%s)", pin_id, exc
            )
            return None
        except httpx.TimeoutException as exc:
            logger.warning(
                "plex.check_pin: request timed out pin_id=%d (%s)", pin_id, exc
            )
            return None
        except httpx.RequestError as exc:
            logger.warning(
                "plex.check_pin: network error pin_id=%d (%s)", pin_id, exc
            )
            return None

        if not response.is_success:
            logger.error(
                "plex.check_pin: unexpected status %d pin_id=%d body=%s",
                response.status_code,
                pin_id,
                response.text[:200],
            )
            return None

        try:
            data: dict[str, Any] = response.json()
        except (ValueError, KeyError) as exc:
            logger.error(
                "plex.check_pin: malformed JSON pin_id=%d (%s)", pin_id, exc
            )
            return None

        auth_token: str | None = data.get("authToken") or None

        if auth_token:
            logger.debug("plex.check_pin: pin_id=%d authenticated", pin_id)
        else:
            logger.debug("plex.check_pin: pin_id=%d still pending", pin_id)

        return auth_token

    async def get_watchlist(self) -> list[WatchlistItem]:
        """Fetch all items from the Plex watchlist.

        Calls ``GET https://discover.provider.plex.tv/library/sections/watchlist/all``
        with pagination (page size 100) and parses each item's title, year, type,
        and GUIDs (tmdb://, imdb://).

        Uses the plex.tv metadata API (not the local server) so the token must
        be a valid Plex account token obtained via OAuth.

        Returns:
            A list of WatchlistItem objects, or an empty list on any failure.
        """
        items: list[WatchlistItem] = []
        page_start = 0
        page_size = 100

        async with httpx.AsyncClient(
            base_url="https://discover.provider.plex.tv",
            timeout=15.0,
            headers={
                "X-Plex-Token": settings.plex.token,
                "Accept": "application/json",
                "X-Plex-Client-Identifier": _PLEX_CLIENT_ID,
                "X-Plex-Product": _PLEX_PRODUCT,
            },
            follow_redirects=True,
        ) as client:
            while True:
                params: dict[str, Any] = {
                    "X-Plex-Container-Start": page_start,
                    "X-Plex-Container-Size": page_size,
                    "includeGuids": 1,
                }
                try:
                    response = await client.get(
                        "/library/sections/watchlist/all",
                        params=params,
                    )
                except httpx.ConnectError as exc:
                    logger.warning("plex.get_watchlist: connection error (%s)", exc)
                    return items
                except httpx.TimeoutException as exc:
                    logger.warning("plex.get_watchlist: request timed out (%s)", exc)
                    return items
                except httpx.RequestError as exc:
                    logger.warning("plex.get_watchlist: network error (%s)", exc)
                    return items

                if response.status_code == 401:
                    logger.error(
                        "plex.get_watchlist: authentication failure (HTTP 401) "
                        "— Plex token is invalid or expired; re-authentication needed"
                    )
                    return items

                if not response.is_success:
                    logger.error(
                        "plex.get_watchlist: unexpected status %d body=%s",
                        response.status_code,
                        response.text[:200],
                    )
                    return items

                try:
                    data: dict[str, Any] = response.json()
                except ValueError as exc:
                    logger.error("plex.get_watchlist: malformed JSON (%s)", exc)
                    return items

                container: dict[str, Any] = data.get("MediaContainer") or {}
                metadata: list[dict[str, Any]] = container.get("Metadata") or []
                total_size = int(container.get("totalSize") or 0)

                for entry in metadata:
                    try:
                        raw_type = str(entry.get("type") or "")
                        if raw_type == "movie":
                            media_type = "movie"
                        elif raw_type in ("show", "tv"):
                            media_type = "show"
                        else:
                            # Skip non-movie/show types (e.g. music, photos)
                            logger.debug(
                                "plex.get_watchlist: skipping unsupported type %r for %r",
                                raw_type,
                                entry.get("title"),
                            )
                            continue

                        title = str(entry.get("title") or "")
                        year_raw = entry.get("year")
                        year: int | None = int(year_raw) if year_raw is not None else None

                        tmdb_id: str | None = None
                        imdb_id: str | None = None
                        for guid in entry.get("Guid") or []:
                            guid_id = str(guid.get("id") or "")
                            if guid_id.startswith("tmdb://"):
                                tmdb_id = guid_id[len("tmdb://"):]
                            elif guid_id.startswith("imdb://"):
                                imdb_id = guid_id[len("imdb://"):]

                        items.append(
                            WatchlistItem(
                                title=title,
                                year=year,
                                media_type=media_type,
                                tmdb_id=tmdb_id,
                                imdb_id=imdb_id,
                            )
                        )
                    except (KeyError, ValueError, TypeError) as exc:
                        logger.warning(
                            "plex.get_watchlist: could not parse entry %r (%s)",
                            entry.get("title"),
                            exc,
                        )

                fetched_so_far = page_start + len(metadata)
                logger.debug(
                    "plex.get_watchlist: page start=%d fetched=%d total=%d",
                    page_start,
                    fetched_so_far,
                    total_size,
                )

                if fetched_so_far >= total_size or not metadata:
                    break

                page_start += page_size

        logger.info("plex.get_watchlist: fetched %d watchlist items", len(items))
        return items


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

plex_client = PlexClient()
