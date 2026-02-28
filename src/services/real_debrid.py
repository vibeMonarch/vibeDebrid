"""Async wrapper for the Real-Debrid REST API v1.0."""

import asyncio
import logging
from typing import Any

import httpx
from pydantic import BaseModel

from src.config import settings

logger = logging.getLogger(__name__)

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


# RD instant availability shape:
#   { "<hash>": { "rd": [ { "<file_id>": { "filename": "...", "filesize": N } } ] } }
# We expose it as a raw dict because the nested structure varies per torrent.
RdInstantAvailability = dict[str, Any]


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

_MAX_HASHES_PER_REQUEST = 100
_DEFAULT_TIMEOUT = 15.0
_BATCH_TIMEOUT = 30.0


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

    async def check_instant_availability(self, hashes: list[str]) -> RdInstantAvailability:
        """Check RD cache availability for a batch of info hashes.

        GET /torrents/instantAvailability/{hash1}/{hash2}/...

        Automatically splits requests into chunks of at most 100 hashes and
        merges the results. Uses a 30-second timeout because large batches can
        be slow.

        Real-world quirk: RD lowercases hashes in the response keys, so this
        method lowercases input hashes before sending and returns results keyed
        by lowercase hash.

        Args:
            hashes: List of torrent info hashes (40-char hex strings).

        Returns:
            Dict mapping lowercase hash to its availability structure.
            Hashes with no cached files are omitted from the result.

        Raises:
            RealDebridAuthError: If the API key is invalid.
            RealDebridError: On other API failures.
        """
        if not hashes:
            return {}

        normalized = [h.lower() for h in hashes]
        chunks = [
            normalized[i : i + _MAX_HASHES_PER_REQUEST]
            for i in range(0, len(normalized), _MAX_HASHES_PER_REQUEST)
        ]

        merged: RdInstantAvailability = {}

        async def _fetch_chunk(client: httpx.AsyncClient, chunk: list[str]) -> dict[str, Any]:
            path = "/torrents/instantAvailability/" + "/".join(chunk)
            response = await client.get(path)
            self._raise_for_status(response)
            return response.json()

        async with self._build_client(timeout=_BATCH_TIMEOUT) as client:
            chunk_results = await asyncio.gather(
                *(_fetch_chunk(client, chunk) for chunk in chunks)
            )
            for chunk_data in chunk_results:
                # Filter out hashes with empty/no cached variants so callers
                # can treat presence in the dict as "is cached".
                for hash_key, availability in chunk_data.items():
                    rd_variants: list = availability.get("rd", []) if isinstance(availability, dict) else []
                    if rd_variants:
                        merged[hash_key] = availability

        logger.debug(
            "check_instant_availability: queried %d hashes, %d cached",
            len(normalized),
            len(merged),
        )
        return merged

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
