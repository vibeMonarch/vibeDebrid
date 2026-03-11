"""Async wrapper for the Real-Debrid REST API v1.0."""

import asyncio
import logging
import os
from typing import Any

import httpx
from pydantic import BaseModel

from src.config import settings
from src.core.mount_scanner import VIDEO_EXTENSIONS

logger = logging.getLogger(__name__)

# Statuses that indicate a torrent's content is already cached on RD servers.
_CACHED_STATUSES = frozenset({"downloaded", "waiting_files_selection"})

# Statuses that indicate a torrent is broken and should not be kept.
_ERROR_STATUSES = frozenset({"magnet_error", "error", "virus", "dead"})

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


class CacheCheckResult(BaseModel):
    """Result of probing a single info hash for RD cache status.

    Attributes:
        info_hash: The torrent info hash that was checked.
        cached: True if cached, False if not cached, None if an error/no-video
                prevented determination.
        rd_id: The RD torrent ID if the torrent was kept (keep_if_cached=True
               and cached), otherwise None.
        has_video_files: Whether the torrent contains video files.
        blocked: True when RD returned HTTP 451 (infringing_file) — the hash
                 is permanently unavailable and must not be retried.
    """

    info_hash: str
    cached: bool | None
    rd_id: str | None = None
    has_video_files: bool = True
    blocked: bool = False


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

    @staticmethod
    def _has_video_files(files: list[dict]) -> bool:
        """Check whether any file in the torrent has a video extension."""
        for f in files:
            path = f.get("path", "")
            _, ext = os.path.splitext(path)
            if ext.lower() in VIDEO_EXTENSIONS:
                return True
        return False

    async def _api_call_with_retry(
        self,
        coro_factory: Any,
        max_retries: int = 3,
        label: str = "",
    ) -> Any:
        """Call an async function with exponential backoff on rate limit.

        Args:
            coro_factory: A callable that returns an awaitable (called each attempt).
            max_retries: Maximum number of attempts.
            label: Label for log messages.

        Returns:
            The result of the successful call.

        Raises:
            The last exception if all retries are exhausted.
        """
        for attempt in range(max_retries):
            try:
                return await coro_factory()
            except RealDebridRateLimitError:
                if attempt == max_retries - 1:
                    raise
                delay = 2 ** (attempt + 1)  # 2s, 4s, 8s
                logger.warning(
                    "%s: rate limited, retrying in %ds (attempt %d/%d)",
                    label or "api_call",
                    delay,
                    attempt + 1,
                    max_retries,
                )
                await asyncio.sleep(delay)
        # Unreachable, but satisfies type checker
        raise RuntimeError("unreachable")  # pragma: no cover

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

    async def list_torrents(self, limit: int = 100, page: int = 1) -> list[dict[str, Any]]:
        """Return torrents currently in the RD account.

        GET /torrents with pagination params. Used by the dedup engine to
        detect torrents already present before adding duplicates.

        Args:
            limit: Maximum number of results to return (RD cap: 2500).
            page: 1-based page number.

        Returns:
            List of torrent summary dicts from RD.

        Raises:
            RealDebridAuthError: If the API key is invalid.
            RealDebridError: On other API failures.
        """
        async with self._build_client() as client:
            response = await client.get("/torrents", params={"limit": limit, "page": page})
            self._raise_for_status(response)
            if response.status_code == 204 or not response.text:
                return []
            data: list[dict[str, Any]] = response.json()
        logger.debug("list_torrents: returned %d items (page=%d)", len(data), page)
        return data

    async def list_all_torrents(
        self, page_size: int = 2500, max_pages: int = 10
    ) -> list[dict[str, Any]]:
        """Fetch all torrents from the RD account with pagination.

        Calls ``list_torrents`` repeatedly, incrementing the page number, until
        a response contains fewer than ``page_size`` items (i.e. the last page)
        or ``max_pages`` is reached.

        Args:
            page_size: Number of results per page (RD cap: 2500).
            max_pages: Safety limit to prevent infinite loops.

        Returns:
            Combined list of torrent summary dicts from all pages.

        Raises:
            RealDebridAuthError: If the API key is invalid.
            RealDebridError: On other API failures.
        """
        all_torrents: list[dict[str, Any]] = []
        for page in range(1, max_pages + 1):
            page_results = await self.list_torrents(limit=page_size, page=page)
            all_torrents.extend(page_results)
            logger.debug(
                "list_all_torrents: page=%d returned %d items (total so far: %d)",
                page,
                len(page_results),
                len(all_torrents),
            )
            if len(page_results) < page_size:
                # Last page — no more results to fetch.
                break
        logger.info("list_all_torrents: fetched %d total torrents", len(all_torrents))
        return all_torrents

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

    async def check_cached(
        self, info_hash: str, *, keep_if_cached: bool = False
    ) -> CacheCheckResult:
        """Check whether a single torrent is cached on RD servers.

        Adds the magnet, inspects the torrent status, checks for error statuses
        and video files, then either keeps or deletes the torrent.

        When *keep_if_cached* is True and the torrent IS cached, the torrent
        is left in the RD account and the returned ``rd_id`` can be reused by
        the caller (avoiding a redundant ``add_magnet`` later).

        Args:
            info_hash: 40-character hex info hash.
            keep_if_cached: If True, keep cached torrents instead of deleting.

        Returns:
            CacheCheckResult with cached status and optional rd_id.
        """
        magnet_uri = f"magnet:?xt=urn:btih:{info_hash}"
        rd_id: str | None = None
        should_keep = False
        try:
            add_resp = await self._api_call_with_retry(
                lambda: self.add_magnet(magnet_uri),
                label=f"check_cached({info_hash[:8]}...).add_magnet",
            )
            rd_id = str(add_resp.get("id", ""))
            if not rd_id:
                logger.warning("check_cached: add_magnet returned empty id for hash=%s", info_hash)
                return CacheCheckResult(info_hash=info_hash, cached=None)

            info = await self._api_call_with_retry(
                lambda: self.get_torrent_info(rd_id),
                label=f"check_cached({info_hash[:8]}...).get_info",
            )
            status = info.get("status", "")

            # Error status → treat as indeterminate
            if status in _ERROR_STATUSES:
                logger.warning(
                    "check_cached: hash=%s rd_id=%s has error status=%s",
                    info_hash, rd_id, status,
                )
                return CacheCheckResult(
                    info_hash=info_hash, cached=None, has_video_files=False,
                )

            # Check for video files
            files = info.get("files", [])
            has_video = self._has_video_files(files) if files else True
            if files and not has_video:
                logger.warning(
                    "check_cached: hash=%s rd_id=%s has no video files",
                    info_hash, rd_id,
                )
                return CacheCheckResult(
                    info_hash=info_hash, cached=None, has_video_files=False,
                )

            cached = status in _CACHED_STATUSES
            logger.debug(
                "check_cached: hash=%s rd_id=%s status=%s cached=%s keep=%s",
                info_hash, rd_id, status, cached, keep_if_cached,
            )

            if cached and keep_if_cached:
                should_keep = True
                return CacheCheckResult(
                    info_hash=info_hash, cached=True, rd_id=rd_id,
                    has_video_files=has_video,
                )

            return CacheCheckResult(
                info_hash=info_hash, cached=cached, has_video_files=has_video,
            )
        except RealDebridError as exc:
            if exc.status_code == 451:
                logger.warning(
                    "check_cached: hash=%s blocked by RD (451 infringing_file)",
                    info_hash,
                )
                # should_keep is False so the finally block will attempt cleanup.
                return CacheCheckResult(info_hash=info_hash, cached=False, blocked=True)
            logger.warning("check_cached: failed for hash=%s: %s", info_hash, exc)
            return CacheCheckResult(info_hash=info_hash, cached=None)
        except Exception as exc:
            logger.warning("check_cached: failed for hash=%s: %s", info_hash, exc)
            return CacheCheckResult(info_hash=info_hash, cached=None)
        finally:
            if rd_id and not should_keep:
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
        *,
        keep_if_cached: bool = False,
    ) -> dict[str, CacheCheckResult]:
        """Check up to *max_checks* hashes for RD cache status.

        Hashes are checked sequentially to avoid tripping RD's rate limit.
        Stops early if a rate-limit error is encountered.

        Args:
            hashes: Info hashes to check, ordered by priority (best first).
            max_checks: Maximum number of hashes to probe.
            keep_if_cached: If True, keep cached torrents in the RD account.

        Returns:
            Dict mapping info hash to CacheCheckResult.
        """
        results: dict[str, CacheCheckResult] = {}
        to_check = hashes[:max_checks]
        checked_count = 0
        for idx, h in enumerate(to_check):
            try:
                results[h] = await self.check_cached(h, keep_if_cached=keep_if_cached)
                checked_count = idx + 1
            except RealDebridRateLimitError:
                logger.warning(
                    "check_cached_batch: rate limited after %d/%d checks, stopping early",
                    idx, len(to_check),
                )
                checked_count = idx
                break
        logger.info(
            "check_cached_batch: checked %d/%d hashes, %d cached",
            checked_count if to_check else 0,
            len(hashes),
            sum(1 for cr in results.values() if cr.cached is True),
        )
        return results

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
