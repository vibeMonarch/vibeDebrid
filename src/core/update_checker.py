"""Periodic update checker that polls GitHub for the latest release."""

import logging

import httpx

from src.__version__ import __version__

logger = logging.getLogger(__name__)

# Cached latest release info.  None means either "not yet checked" or
# "current version is up to date".
_latest_release: dict | None = None


async def check_for_updates() -> dict | None:
    """Check GitHub for the latest release. Returns release info or None.

    Updates the module-level ``_latest_release`` cache.  A non-critical
    operation — all exceptions are silently swallowed so a failed check
    never interrupts the queue or any other subsystem.

    Returns:
        A dict with ``version``, ``current``, ``url``, ``published_at``,
        and ``update_available`` keys when a newer release exists, or
        ``None`` when the current version is up to date or the check fails.
    """
    global _latest_release
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                "https://api.github.com/repos/vibeMonarch/vibeDebrid/releases/latest",
                headers={"Accept": "application/vnd.github.v3+json"},
            )
            if resp.status_code != 200:
                logger.debug(
                    "update_checker: GitHub API returned %d, skipping",
                    resp.status_code,
                )
                return None
            data = resp.json()
            tag = data.get("tag_name", "")
            version = tag.lstrip("v")
            if version and version != __version__:
                _latest_release = {
                    "version": version,
                    "current": __version__,
                    "url": data.get("html_url", ""),
                    "published_at": data.get("published_at", ""),
                    "update_available": _is_newer(version, __version__),
                }
                logger.info(
                    "update_checker: new release available %s (current=%s)",
                    version,
                    __version__,
                )
            else:
                _latest_release = None
                logger.debug(
                    "update_checker: already on latest version %s", __version__
                )
    except httpx.TimeoutException:
        logger.debug("update_checker: request timed out")
    except httpx.HTTPError as exc:
        logger.debug("update_checker: HTTP error: %s", exc)
    except Exception as exc:
        logger.debug("update_checker: unexpected error: %s", exc)
    return _latest_release


def _is_newer(remote: str, local: str) -> bool:
    """Compare semver strings. Returns True if remote > local.

    Args:
        remote: The remote version string (e.g. "0.2.0").
        local: The local version string (e.g. "0.1.0").

    Returns:
        True when remote is strictly newer than local.
    """
    try:
        r = tuple(int(x) for x in remote.split("."))
        loc = tuple(int(x) for x in local.split("."))
        return r > loc
    except (ValueError, AttributeError):
        return False


def get_latest_release() -> dict | None:
    """Return the cached latest release info.

    Returns:
        The most recently fetched release dict, or None if the current
        version is up to date or no check has been performed yet.
    """
    return _latest_release
