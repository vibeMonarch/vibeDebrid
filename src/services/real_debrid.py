"""Async wrapper for the Real-Debrid REST API v1.0."""

import logging
from typing import Any

import httpx
from pydantic import BaseModel

from src.config import settings

logger = logging.getLogger(__name__)

# Statuses that indicate a torrent's content is already cached on RD servers.
_CACHED_STATUSES = frozenset({"downloaded", "waiting_files_selection"})

# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------


class RealDebridError(Exception):
    """Raised when the Real-Debrid API returns an unexpected error response."""

    def __init__(self, message: str, status_code: int | None = None, error_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code


class RealDebridAuthError(RealDebridError):
    """Raised when the API key is invalid or expired (HTTP 401)."""


class RealDebridRateLimitError(RealDebridError):
    """Raised when the API rate limit is exceeded (HTTP 429)."""


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------


class RdFile(BaseModel):
    """A single file entry within a torrent."""

    id: int
    path: str
    bytes: int
    selected: int  # 0 or 1


class RdTorrentInfo(BaseModel):
    """Full torrent info as returned by /torrents/info/{id}.

    Fields match the Real-Debrid API response keys. Optional fields may be
    absent on torrents that have not been processed yet.
    """

    id: str
    filename: str
    original_filename: str | None = None
    hash: str
    bytes: int
    original_bytes: int | None = None
    host: str | None = None
    split: int | None = None
    progress: float = 0.0
    status: str
    added: str | None = None
    files: list[RdFile] = []
    links: list[str] = []
    ended: str | None = None
    speed: int | None = None
    seeders: int | None = None


class RdUser(BaseModel):
    """User account information returned by /user."""

    id: int
    username: str
    email: str
    points: int
    locale: str | None = None
    avatar: str | None = None
    type: str
    premium: int
    expiration: str | None = None


class RdAddMagnetResponse(BaseModel):
    """Response from /torrents/addMagnet."""

    id: str
    uri: str


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

_DEFAULT_TIMEOUT = 15.0


class RealDebridClient:
    """Async client for the Real-Debrid REST API v1.0.

    All methods raise subclasses of RealDebridError on API-level failures.
    Network-layer errors (timeouts, connection refused) propagate as httpx
    exceptions so the caller can decide whether to retry or surface the error.
    """

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_client(self, timeout: float = _DEFAULT_TIMEOUT) -> httpx.AsyncClient:
        """Create a new httpx.AsyncClient with auth headers and timeout."""
        return httpx.AsyncClient(
            base_url=settings.real_debrid.api_url.rstrip("/"),
            headers={
                "Authorization": f"Bearer {settings.real_debrid.api_key}",
                "User-Agent": "vibeDebrid/0.1",
            },
            timeout=timeout,
        )

    def _raise_for_status(self, response: httpx.Response) -> None:
        """Parse RD error responses and raise the appropriate exception.

        Args:
            response: The httpx response to inspect.

        Raises:
            RealDebridAuthError: On HTTP 401.
            RealDebridRateLimitError: On HTTP 429.
            RealDebridError: On any other 4xx/5xx status.
        """
        if response.is_success:
            return

        # Attempt to extract RD error payload
        error_msg: str = response.text
        error_code: int | None = None
        try:
            body = response.json()
            error_msg = body.get("error", error_msg)
            error_code = body.get("error_code")
        except Exception:
            pass

        status = response.status_code

        if status == 401:
            logger.error(
                "Real-Debrid API key is invalid or expired. Update it in Settings. "
                "(error=%s, error_code=%s)",
                error_msg,
                error_code,
            )
            raise RealDebridAuthError(
                "Real-Debrid API key is invalid or expired. Update it in Settings.",
                status_code=status,
                error_code=error_code,
            )

        if status == 429:
            logger.warning(
                "Real-Debrid rate limit exceeded. (error=%s, error_code=%s)",
                error_msg,
                error_code,
            )
            raise RealDebridRateLimitError(
                "Real-Debrid rate limit exceeded.",
                status_code=status,
                error_code=error_code,
            )

        logger.error(
            "Real-Debrid API error %d: %s (error_code=%s)",
            status,
            error_msg,
            error_code,
        )
        raise RealDebridError(
            f"Real-Debrid API error {status}: {error_msg}",
            status_code=status,
            error_code=error_code,
        )

    # ------------------------------------------------------------------
    # Public API methods
    # ------------------------------------------------------------------

    async def get_user(self) -> dict[str, Any]:
        """Return current account info from GET /user.

        Useful for validating the API key and checking premium status.

        Returns:
            Parsed JSON dict conforming to RdUser schema.

        Raises:
            RealDebridAuthError: If the API key is invalid.
            RealDebridError: On other API failures.
        """
        async with self._build_client() as client:
            response = await client.get("/user")
            self._raise_for_status(response)
            data: dict[str, Any] = response.json()
        logger.debug("get_user: username=%s type=%s", data.get("username"), data.get("type"))
        return data

    async def add_magnet(self, magnet_uri: str) -> dict[str, Any]:
        """Add a torrent to the RD account via magnet link.

        Posts to POST /torrents/addMagnet. The torrent will be in "magnet_conversion"
        status immediately after — call select_files() to start downloading.

        Args:
            magnet_uri: A valid BitTorrent magnet URI.

        Returns:
            Dict with 'id' (RD torrent ID string) and 'uri' keys.

        Raises:
            RealDebridAuthError: If the API key is invalid.
            RealDebridError: On other API failures.
        """
        async with self._build_client() as client:
            response = await client.post("/torrents/addMagnet", data={"magnet": magnet_uri})
            self._raise_for_status(response)
            data: dict[str, Any] = response.json()
        logger.info("add_magnet: added torrent id=%s", data.get("id"))
        return data

    async def check_instant_availability(self, hashes: list[str]) -> dict[str, Any]:
        """DEPRECATED: Real-Debrid disabled this endpoint (403, error_code=37).

        This method now returns an empty dict immediately. Cache status is
        derived from Torrentio's ⚡ indicator instead (when the opts URL
        includes an RD API key).

        Args:
            hashes: List of torrent info hashes (ignored).

        Returns:
            Always returns an empty dict.
        """
        logger.warning(
            "check_instant_availability is deprecated — RD disabled the "
            "/torrents/instantAvailability endpoint (403, error_code=37). "
            "Cache status is now derived from Torrentio stream metadata."
        )
        return {}

    async def list_torrents(self, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        """Return torrents currently in the RD account.

        GET /torrents with pagination params. Used by the dedup engine to
        detect torrents already present before adding duplicates.

        Args:
            limit: Maximum number of results to return (RD cap: 2500).
            offset: Zero-based page offset.

        Returns:
            List of torrent summary dicts from RD.

        Raises:
            RealDebridAuthError: If the API key is invalid.
            RealDebridError: On other API failures.
        """
        async with self._build_client() as client:
            response = await client.get("/torrents", params={"limit": limit, "offset": offset})
            self._raise_for_status(response)
            data: list[dict[str, Any]] = response.json()
        logger.debug("list_torrents: returned %d items (offset=%d)", len(data), offset)
        return data

    async def get_torrent_info(self, torrent_id: str) -> dict[str, Any]:
        """Return full torrent info including file list and download links.

        GET /torrents/info/{id}

        Args:
            torrent_id: The RD torrent ID string (e.g. "AABBCCDD...").

        Returns:
            Dict conforming to RdTorrentInfo schema, including the 'files'
            list and 'links' list once the torrent has been processed.

        Raises:
            RealDebridAuthError: If the API key is invalid.
            RealDebridError: On other API failures, including unknown ID.
        """
        async with self._build_client() as client:
            response = await client.get(f"/torrents/info/{torrent_id}")
            self._raise_for_status(response)
            data: dict[str, Any] = response.json()
        logger.debug(
            "get_torrent_info: id=%s status=%s progress=%s%%",
            torrent_id,
            data.get("status"),
            data.get("progress"),
        )
        return data

    async def select_files(
        self,
        torrent_id: str,
        file_ids: list[int] | str = "all",
    ) -> None:
        """Select files to download for a torrent added to RD.

        POST /torrents/selectFiles/{id}

        Must be called after add_magnet() before RD starts downloading. Passing
        "all" selects every file in the torrent, which is the typical path for
        season packs and single-file releases.

        Args:
            torrent_id: The RD torrent ID string.
            file_ids: Either the string "all" or a list of integer file IDs
                      as returned in the 'files' field of get_torrent_info().

        Raises:
            RealDebridAuthError: If the API key is invalid.
            RealDebridError: On other API failures.
        """
        if isinstance(file_ids, list):
            files_value = ",".join(str(fid) for fid in file_ids)
        else:
            files_value = "all"

        async with self._build_client() as client:
            response = await client.post(
                f"/torrents/selectFiles/{torrent_id}",
                data={"files": files_value},
            )

            # RD returns 204 No Content on success; treat any 2xx as success.
            if response.status_code == 204 or response.is_success:
                logger.info(
                    "select_files: torrent_id=%s files=%s",
                    torrent_id,
                    files_value if len(files_value) < 80 else files_value[:77] + "...",
                )
                return

            self._raise_for_status(response)

    async def check_cached(self, info_hash: str) -> bool:
        """Check whether a single torrent is cached on RD servers.

        Adds the magnet, inspects the torrent status, then deletes it.
        A torrent whose status is ``"downloaded"`` or
        ``"waiting_files_selection"`` immediately after add is cached.

        This is expensive (3 API calls per hash).  Prefer
        ``check_cached_batch`` for checking multiple hashes.

        Args:
            info_hash: 40-character hex info hash.

        Returns:
            True if the torrent is cached, False otherwise (including on any
            API error).
        """
        magnet_uri = f"magnet:?xt=urn:btih:{info_hash}"
        rd_id: str | None = None
        try:
            add_resp = await self.add_magnet(magnet_uri)
            rd_id = str(add_resp.get("id", ""))
            if not rd_id:
                logger.warning("check_cached: add_magnet returned empty id for hash=%s", info_hash)
                return False

            info = await self.get_torrent_info(rd_id)
            status = info.get("status", "")
            cached = status in _CACHED_STATUSES
            logger.debug(
                "check_cached: hash=%s rd_id=%s status=%s cached=%s",
                info_hash, rd_id, status, cached,
            )
            return cached
        except Exception as exc:
            logger.warning("check_cached: failed for hash=%s: %s", info_hash, exc)
            return False
        finally:
            if rd_id:
                try:
                    await self.delete_torrent(rd_id)
                except Exception as exc:
                    logger.warning(
                        "check_cached: cleanup delete failed for rd_id=%s: %s",
                        rd_id, exc,
                    )

    async def check_cached_batch(
        self,
        hashes: list[str],
        max_checks: int = 10,
    ) -> set[str]:
        """Check up to *max_checks* hashes for RD cache status.

        Hashes are checked sequentially to avoid tripping RD's rate limit.
        Stops early if a rate-limit error is encountered.

        Args:
            hashes: Info hashes to check, ordered by priority (best first).
            max_checks: Maximum number of hashes to probe.

        Returns:
            Set of info hashes confirmed as cached.
        """
        cached: set[str] = set()
        to_check = hashes[:max_checks]
        for idx, h in enumerate(to_check):
            try:
                if await self.check_cached(h):
                    cached.add(h)
            except RealDebridRateLimitError:
                logger.warning(
                    "check_cached_batch: rate limited after %d/%d checks, stopping early",
                    idx, len(to_check),
                )
                break
        logger.info(
            "check_cached_batch: checked %d/%d hashes, %d cached",
            min(len(to_check), idx + 1) if to_check else 0,
            len(hashes),
            len(cached),
        )
        return cached

    async def delete_torrent(self, torrent_id: str) -> None:
        """Remove a torrent from the RD account.

        DELETE /torrents/delete/{id}

        Real-world quirk: RD returns 204 No Content on success. A 404 may be
        returned if the torrent was already deleted — this is logged as a
        warning and not re-raised so callers can treat delete as idempotent.

        Args:
            torrent_id: The RD torrent ID string to delete.

        Raises:
            RealDebridAuthError: If the API key is invalid.
            RealDebridError: On non-404 API failures.
        """
        async with self._build_client() as client:
            response = await client.delete(f"/torrents/delete/{torrent_id}")

            if response.status_code == 404:
                logger.warning(
                    "delete_torrent: torrent_id=%s not found (already deleted?)", torrent_id
                )
                return

            self._raise_for_status(response)
        logger.info("delete_torrent: torrent_id=%s deleted", torrent_id)


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

rd_client = RealDebridClient()
