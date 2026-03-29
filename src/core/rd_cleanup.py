"""RD Account Categorization and Cleanup — Phase 2.

Categorizes every torrent in the user's Real-Debrid account into one of five
buckets:

- **Protected**: linked to an active local MediaItem (via rd_torrents or a
  Symlink whose source path resolves to a known mount name).  These must never
  be deleted automatically.
- **Dead**: RD reports status ``error``, ``virus``, ``dead``, or
  ``magnet_error``.  Safe to remove.
- **Stale**: stuck in ``downloading`` or ``magnet_conversion`` for more than
  ``_STALE_THRESHOLD_DAYS`` days.  Safe to remove.
- **Duplicate**: same PTN-parsed (title, season, episode) group as a Protected
  torrent, but the current entry is not itself Protected.  Safe to remove after
  review.
- **Orphaned**: everything else — downloaded content with no local record.
  Listed for review; the user decides.

Design notes:

- No auto-deletion.  ``execute_rd_cleanup`` is only called when the user
  explicitly selects specific ``rd_id`` values.
- A module-level cache (``_last_scan_cache``) stores the last scan result and
  the raw RD torrent list so ``execute_rd_cleanup`` can skip a second
  ``list_all_torrents()`` call when run shortly after a scan.
- Category priority: Protected > Dead > Stale > Duplicate > Orphaned.
- The ``mark_torrent_removed`` call after each successful deletion keeps the
  local dedup registry consistent.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any

import PTN
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.rd_bridge import (
    _extract_mount_name_any_base,
    _extract_mount_relative_name,
    _normalize_name,
)
from src.models.symlink import Symlink
from src.models.torrent import RdTorrent, TorrentStatus

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_STALE_THRESHOLD_DAYS: int = 7
_SCAN_TTL_SECONDS: int = 300

_DEAD_STATUSES: frozenset[str] = frozenset({"error", "virus", "dead", "magnet_error"})
_STALE_STATUSES: frozenset[str] = frozenset({"downloading", "magnet_conversion"})

# Strips non-alphanumeric characters for title-level grouping.
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


class RdTorrentCategory(StrEnum):
    """Categorization bucket for a single RD account torrent."""

    PROTECTED = "protected"
    DEAD = "dead"
    STALE = "stale"
    ORPHANED = "orphaned"
    DUPLICATE = "duplicate"


class CategorizedTorrent(BaseModel):
    """A single RD account torrent with its assigned category and reason.

    Attributes:
        rd_id: Real-Debrid torrent identifier string.
        info_hash: Lowercase info hash (may be empty string when unavailable).
        filename: Torrent display name as reported by RD.
        filesize: Size in bytes (0 when not reported).
        status: Raw RD status string (e.g. ``"downloaded"``, ``"error"``).
        added: ISO 8601 timestamp from RD, or None.
        category: Assigned category bucket.
        reason: Human-readable explanation of why this category was assigned.
        protected_by: For DUPLICATE: the filename of the Protected torrent in
            the same group.  None for all other categories.
        parsed_title: PTN-derived title for display purposes, or None when PTN
            could not parse the filename.
    """

    rd_id: str
    info_hash: str
    filename: str
    filesize: int
    status: str
    added: str | None = None
    category: RdTorrentCategory
    reason: str
    protected_by: str | None = None
    parsed_title: str | None = None


class CategorySummary(BaseModel):
    """Aggregate counts and size for one category bucket.

    Attributes:
        category: The category bucket.
        count: Number of torrents in this bucket.
        total_bytes: Sum of filesize values (bytes) for all torrents in bucket.
    """

    category: RdTorrentCategory
    count: int
    total_bytes: int


class RdCleanupScan(BaseModel):
    """Full scan result returned by ``scan_rd_account``.

    Attributes:
        scanned_at: ISO 8601 UTC timestamp of when the scan completed.
        total_torrents: Total number of RD account torrents examined.
        summaries: Per-category aggregate counts and sizes.
        torrents: Every categorized torrent, sorted Protected-last so that
            actionable items appear first in the UI.
        warnings: Non-fatal issues encountered during the scan.
    """

    scanned_at: str
    total_torrents: int
    summaries: list[CategorySummary]
    torrents: list[CategorizedTorrent]
    warnings: list[str] = []


class RdCleanupExecuteRequest(BaseModel):
    """Request body for the cleanup execute endpoint.

    Attributes:
        rd_ids: List of Real-Debrid torrent IDs the user wants deleted.
    """

    rd_ids: list[str] = Field(max_length=2500)


class RdCleanupExecuteResult(BaseModel):
    """Result of an execute_rd_cleanup call.

    Attributes:
        requested: Total number of rd_ids submitted by the user.
        rejected_protected: IDs skipped because they are PROTECTED.
        rejected_not_found: IDs not present in the (possibly cached) scan.
        deleted: Number of torrents successfully deleted from RD.
        failed: Number of deletion attempts that raised a non-rate-limit error.
        rate_limited: True when a 429 response caused early termination.
        errors: Per-item error messages for failed deletions.
    """

    requested: int
    rejected_protected: int
    rejected_not_found: int
    deleted: int
    failed: int
    rate_limited: bool = False
    errors: list[str] = []


# ---------------------------------------------------------------------------
# Module-level scan cache
# ---------------------------------------------------------------------------

# Populated by scan_rd_account; consumed (and optionally reused) by
# execute_rd_cleanup.  Both fields are updated together under the implicit
# single-threaded asyncio event loop — no explicit lock required because the
# Tools route enforces mutual exclusion via _cleanup_lock.
_last_scan_cache: dict[str, Any] = {
    "scanned_at": None,        # datetime | None
    "rd_torrents": None,       # list[dict] | None
    "category_map": None,      # dict[str, RdTorrentCategory] | None
    "hash_map": None,          # dict[str, str] | None  (rd_id -> info_hash)
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _normalize_title(title: str) -> str:
    """Return a lowercase, alphanumeric-only version of *title* for grouping.

    Strips all non-alphanumeric characters so that ``"Breaking Bad"`` and
    ``"Breaking.Bad"`` collapse to the same grouping key ``"breakingbad"``.

    Args:
        title: Raw title string as returned by PTN.

    Returns:
        Normalised, lowercased title string.
    """
    return _NON_ALNUM_RE.sub("", title.lower())


def _parse_filename(filename: str) -> dict[str, Any]:
    """Parse *filename* with PTN, returning an empty dict on failure.

    Args:
        filename: Raw torrent filename / release name.

    Returns:
        PTN result dict, or ``{}`` on parse failure.
    """
    try:
        result: dict[str, Any] = PTN.parse(filename) or {}
        return result
    except Exception as exc:
        logger.debug("rd_cleanup: PTN failed for filename=%r: %s", filename, exc)
        return {}


def _parse_added(added: str | None) -> datetime | None:
    """Parse an RD ``added`` timestamp string into a timezone-aware datetime.

    RD returns timestamps in two formats:
    - ``"2024-01-15T10:30:00.000Z"`` (Z suffix)
    - ``"2024-01-15T10:30:00.000+00:00"`` (explicit offset)

    Args:
        added: Raw string from RD API, or None.

    Returns:
        UTC-aware datetime, or None when *added* is falsy or unparseable.
    """
    if not added:
        return None
    # Normalise trailing Z to +00:00 so fromisoformat works on all Python
    # versions (fromisoformat only accepts Z from Python 3.11+).
    normalised = added.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalised)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt
    except ValueError as exc:
        logger.warning("rd_cleanup: could not parse added=%r: %s", added, exc)
        return None


def _build_ptn_groups(
    rd_torrents: list[dict[str, Any]],
) -> dict[tuple[str, int | None, int | None], list[str]]:
    """Group RD torrent IDs by their PTN-parsed (title, season, episode) key.

    Only torrents whose filename PTN-parses to a non-empty title are included.
    Empty-title results are silently skipped.

    Args:
        rd_torrents: Raw RD API torrent list dicts.

    Returns:
        Mapping from ``(norm_title, season, episode)`` to a list of RD ID
        strings in that group.
    """
    groups: dict[tuple[str, int | None, int | None], list[str]] = {}
    for rd in rd_torrents:
        filename: str = rd.get("filename") or ""
        rd_id: str = str(rd.get("id") or "")
        if not filename or not rd_id:
            continue
        ptn = _parse_filename(filename)
        raw_title: str = ptn.get("title") or ""
        if not raw_title:
            continue
        norm = _normalize_title(raw_title)
        season: int | None = ptn.get("season")
        episode: int | None = ptn.get("episode")
        key = (norm, season, episode)
        groups.setdefault(key, []).append(rd_id)
    return groups


async def _check_source_paths_exist(
    source_paths: list[str],
    semaphore: asyncio.Semaphore,
) -> list[bool]:
    """Check whether each path in *source_paths* exists on the filesystem.

    Uses ``asyncio.to_thread`` to avoid blocking the event loop on FUSE I/O.
    Each check is gated by *semaphore* to limit concurrency.

    Args:
        source_paths: List of absolute paths to check.
        semaphore: Semaphore controlling maximum concurrent filesystem checks.

    Returns:
        List of booleans, one per path, in the same order as *source_paths*.
    """
    async def _exists(path: str) -> bool:
        async with semaphore:
            try:
                return await asyncio.wait_for(
                    asyncio.to_thread(os.path.exists, path), timeout=5.0
                )
            except (TimeoutError, OSError) as exc:
                logger.debug("rd_cleanup: os.path.exists failed for %r: %s", path, exc)
                return False

    return list(await asyncio.gather(*(_exists(p) for p in source_paths)))


async def _build_live_mount_set(
    source_paths: list[str],
) -> set[tuple[str, int | None]]:
    """Build a set of ``(normalized_title, season)`` tuples from live symlink sources.

    For each source_path, checks whether the path still exists on the
    filesystem.  When it does, the parent directory name is PTN-parsed to
    extract (title, season).  This enables the categorizer to protect
    replacement torrents that Zurg auto-recovered (different hash/rd_id but
    the mount directory name matches a live symlink source path).

    The filesystem checks run concurrently (up to 20 at once) via
    ``asyncio.to_thread``.  If the mount appears to be entirely unavailable
    (all checks fail), an empty set is returned so the caller can skip this
    protection layer gracefully.

    Args:
        source_paths: All ``source_path`` values from the symlinks table.

    Returns:
        Set of ``(normalized_title, season)`` tuples for live mount paths.
        Returns empty set when the mount is unavailable or no paths parse.
    """
    if not source_paths:
        return set()

    semaphore = asyncio.Semaphore(20)
    existence = await _check_source_paths_exist(source_paths, semaphore)

    live_set: set[tuple[str, int | None]] = set()
    live_count = 0
    for path, exists in zip(source_paths, existence):
        if not exists:
            continue
        live_count += 1
        # Extract parent directory name for PTN parsing.
        # e.g. /mnt/__all__/Attack on Titan S02/S02E01.mkv → "Attack on Titan S02"
        # For single-file torrents directly under __all__/, fall back to filename.
        parent_dir = Path(path).parent.name
        if not parent_dir:
            continue
        if parent_dir == "__all__":
            # Single-file torrent — use the filename itself for PTN parsing
            ptn_source = Path(path).name
        else:
            ptn_source = parent_dir
        ptn = _parse_filename(ptn_source)
        raw_title: str = ptn.get("title") or ""
        if not raw_title:
            continue
        norm_title = _normalize_title(raw_title)
        if not norm_title:
            # Title has no alphanumeric characters — skip
            continue
        season: int | None = ptn.get("season")
        live_set.add((norm_title, season))

    if source_paths and live_count == 0:
        logger.warning(
            "rd_cleanup: all %d symlink source paths are missing — "
            "Zurg mount may be unavailable; skipping live mount protection",
            len(source_paths),
        )
        return set()

    logger.debug(
        "rd_cleanup: live_mount_set=%d entries from %d/%d live paths",
        len(live_set),
        live_count,
        len(source_paths),
    )
    return live_set


async def _build_protection_sets(
    session: AsyncSession,
) -> tuple[set[str], set[str], set[str], set[tuple[str, int | None]]]:
    """Load protection identifiers from the local database.

    Four complementary sets are returned so every protection avenue is covered:

    1. ``active_hashes`` — info hashes in ``rd_torrents`` with status=ACTIVE.
    2. ``active_rd_ids`` — ``rd_id`` values in ``rd_torrents`` with status=ACTIVE.
    3. ``symlink_mount_names`` — lowercase first-path-component extracted from
       every ``symlinks.source_path`` using ``_extract_mount_relative_name``.
    4. ``live_mount_set`` — ``(normalized_title, season)`` tuples from symlinks
       whose ``source_path`` still exists on the filesystem.  Used to protect
       Zurg auto-recovered torrents that have a different hash/rd_id.

    Args:
        session: Async database session.

    Returns:
        Tuple of
        ``(active_hashes, active_rd_ids, symlink_mount_names, live_mount_set)``.
    """
    from src.config import settings

    # Active info hashes
    hash_rows = await session.execute(
        select(RdTorrent.info_hash).where(
            RdTorrent.status == TorrentStatus.ACTIVE,
            RdTorrent.info_hash.isnot(None),
        )
    )
    active_hashes: set[str] = {row[0].lower() for row in hash_rows if row[0]}

    # Active rd_ids
    rdid_rows = await session.execute(
        select(RdTorrent.rd_id).where(
            RdTorrent.status == TorrentStatus.ACTIVE,
            RdTorrent.rd_id.isnot(None),
        )
    )
    active_rd_ids: set[str] = {row[0] for row in rdid_rows if row[0]}

    # Symlink mount names — try configured zurg_mount first, fall back to
    # /__all__/ marker detection for alternative mount paths.
    symlink_rows = await session.execute(select(Symlink.source_path))
    zurg_mount: str = settings.paths.zurg_mount.rstrip("/")
    symlink_mount_names: set[str] = set()
    source_paths: list[str] = []
    fallback_count = 0
    for (source_path,) in symlink_rows:
        if not source_path:
            continue
        source_paths.append(source_path)
        mount_name = _extract_mount_relative_name(source_path, zurg_mount)
        if mount_name is None:
            mount_name = _extract_mount_name_any_base(source_path)
            if mount_name is not None:
                fallback_count += 1
        if mount_name:
            symlink_mount_names.add(mount_name.lower())
            symlink_mount_names.add(_normalize_name(mount_name).lower())

    if fallback_count:
        logger.warning(
            "rd_cleanup: %d symlinks used /__all__/ fallback extraction "
            "(source_path does not start with zurg_mount=%r)",
            fallback_count,
            zurg_mount,
        )

    # Live mount set — filesystem existence checks (non-blocking via asyncio.to_thread)
    live_mount_set = await _build_live_mount_set(source_paths)

    logger.debug(
        "rd_cleanup: protection sets — active_hashes=%d active_rd_ids=%d "
        "symlink_mount_names=%d live_mount_set=%d",
        len(active_hashes),
        len(active_rd_ids),
        len(symlink_mount_names),
        len(live_mount_set),
    )
    return active_hashes, active_rd_ids, symlink_mount_names, live_mount_set


def _categorize_torrent(
    rd: dict[str, Any],
    active_hashes: set[str],
    active_rd_ids: set[str],
    symlink_mount_names: set[str],
    live_mount_set: set[tuple[str, int | None]],
    protected_rd_ids: set[str],
    ptn_groups: dict[tuple[str, int | None, int | None], list[str]],
    rd_id_to_filename: dict[str, str],
) -> CategorizedTorrent:
    """Assign a category to a single RD torrent dict.

    Priority: Protected > Dead > Stale > Duplicate > Orphaned.

    Args:
        rd: Raw RD API dict for one torrent.
        active_hashes: Set of lowercase info hashes considered protected.
        active_rd_ids: Set of RD ID strings considered protected.
        symlink_mount_names: Set of lowercase mount-relative names from symlinks.
        live_mount_set: Set of ``(normalized_title, season)`` tuples from
            symlinks whose source_path still exists on the filesystem.  Protects
            Zurg auto-recovered torrents that have a new hash/rd_id.
        protected_rd_ids: Set of RD IDs already determined to be Protected (built
            incrementally in the caller to enable Duplicate detection).
        ptn_groups: Mapping from PTN group key to list of RD IDs in that group.
        rd_id_to_filename: Mapping from rd_id to filename (for protected_by label).

    Returns:
        A fully populated CategorizedTorrent.
    """
    rd_id: str = str(rd.get("id") or "")
    raw_hash: str = str(rd.get("hash") or "")
    info_hash: str = raw_hash.lower()
    filename: str = rd.get("filename") or ""
    filesize: int = int(rd.get("bytes") or 0)
    status: str = rd.get("status") or ""
    added: str | None = rd.get("added")

    # Parse filename for display title
    ptn = _parse_filename(filename)
    parsed_title: str | None = ptn.get("title") or None

    # ------------------------------------------------------------------
    # 1. Protected check
    # ------------------------------------------------------------------
    is_protected = False
    protect_reason = ""

    if info_hash and info_hash in active_hashes:
        is_protected = True
        protect_reason = "info_hash in active rd_torrents registry"
    elif rd_id and rd_id in active_rd_ids:
        is_protected = True
        protect_reason = "rd_id in active rd_torrents registry"
    else:
        # Check against symlink mount names (exact and normalized filename match)
        name_candidates: list[str] = []
        if filename:
            name_candidates.append(filename.lower())
            name_candidates.append(_normalize_name(filename).lower())
        if name_candidates:
            for candidate in name_candidates:
                if candidate in symlink_mount_names:
                    is_protected = True
                    protect_reason = f"filename matches symlink mount path ({candidate!r})"
                    break

    # Check 4: live mount set — PTN-parsed (title, season) match.
    # Catches Zurg auto-recovered torrents: Zurg deleted and re-added the torrent
    # (new hash/rd_id) but the mount directory name still matches a live symlink
    # source path.  Only run when not already protected.
    if not is_protected and live_mount_set and parsed_title:
        norm_rd_title = _normalize_title(parsed_title)
        rd_season: int | None = ptn.get("season")
        if (norm_rd_title, rd_season) in live_mount_set:
            is_protected = True
            protect_reason = "backs active Zurg mount path"

    if is_protected:
        return CategorizedTorrent(
            rd_id=rd_id,
            info_hash=info_hash,
            filename=filename,
            filesize=filesize,
            status=status,
            added=added,
            category=RdTorrentCategory.PROTECTED,
            reason=protect_reason,
            parsed_title=parsed_title,
        )

    # ------------------------------------------------------------------
    # 2. Dead check
    # ------------------------------------------------------------------
    if status in _DEAD_STATUSES:
        return CategorizedTorrent(
            rd_id=rd_id,
            info_hash=info_hash,
            filename=filename,
            filesize=filesize,
            status=status,
            added=added,
            category=RdTorrentCategory.DEAD,
            reason=f"RD status is '{status}'",
            parsed_title=parsed_title,
        )

    # ------------------------------------------------------------------
    # 3. Stale check
    # ------------------------------------------------------------------
    if status in _STALE_STATUSES:
        added_dt = _parse_added(added)
        if added_dt is not None:
            age = datetime.now(tz=UTC) - added_dt
            if age > timedelta(days=_STALE_THRESHOLD_DAYS):
                return CategorizedTorrent(
                    rd_id=rd_id,
                    info_hash=info_hash,
                    filename=filename,
                    filesize=filesize,
                    status=status,
                    added=added,
                    category=RdTorrentCategory.STALE,
                    reason=(
                        f"status='{status}' for {age.days} days "
                        f"(threshold={_STALE_THRESHOLD_DAYS}d)"
                    ),
                    parsed_title=parsed_title,
                )

    # ------------------------------------------------------------------
    # 4. Duplicate check
    # ------------------------------------------------------------------
    # For each PTN group that contains this rd_id AND at least one Protected
    # member, this torrent is a Duplicate.
    if parsed_title and rd_id:
        norm = _normalize_title(parsed_title)
        season: int | None = ptn.get("season")
        episode: int | None = ptn.get("episode")
        key = (norm, season, episode)
        group_members = ptn_groups.get(key, [])
        for member_id in group_members:
            if member_id != rd_id and member_id in protected_rd_ids:
                protected_filename = rd_id_to_filename.get(member_id, member_id)
                return CategorizedTorrent(
                    rd_id=rd_id,
                    info_hash=info_hash,
                    filename=filename,
                    filesize=filesize,
                    status=status,
                    added=added,
                    category=RdTorrentCategory.DUPLICATE,
                    reason=(
                        f"same PTN group (title={parsed_title!r}, "
                        f"season={season}, episode={episode}) "
                        f"as protected torrent"
                    ),
                    protected_by=protected_filename,
                    parsed_title=parsed_title,
                )

    # ------------------------------------------------------------------
    # 5. Orphaned (default)
    # ------------------------------------------------------------------
    return CategorizedTorrent(
        rd_id=rd_id,
        info_hash=info_hash,
        filename=filename,
        filesize=filesize,
        status=status,
        added=added,
        category=RdTorrentCategory.ORPHANED,
        reason="no matching local registry entry or symlink",
        parsed_title=parsed_title,
    )


def _build_summaries(torrents: list[CategorizedTorrent]) -> list[CategorySummary]:
    """Build per-category count and byte-sum summaries.

    Args:
        torrents: List of already-categorized torrents.

    Returns:
        One CategorySummary per RdTorrentCategory, sorted by category value.
    """
    totals: dict[RdTorrentCategory, tuple[int, int]] = {
        cat: (0, 0) for cat in RdTorrentCategory
    }
    for t in torrents:
        count, total_bytes = totals[t.category]
        totals[t.category] = (count + 1, total_bytes + t.filesize)

    return [
        CategorySummary(category=cat, count=count, total_bytes=total_bytes)
        for cat, (count, total_bytes) in sorted(totals.items(), key=lambda kv: kv[0].value)
    ]


def _categorize_all(
    rd_torrents: list[dict[str, Any]],
    active_hashes: set[str],
    active_rd_ids: set[str],
    symlink_mount_names: set[str],
    live_mount_set: set[tuple[str, int | None]] | None = None,
) -> tuple[list[CategorizedTorrent], dict[str, RdTorrentCategory], dict[str, str]]:
    """Categorize all torrents and return auxiliary lookup dicts.

    Two-pass approach:
    - Pass 1: identify all PROTECTED rd_ids.
    - Pass 2: categorize remaining torrents, using the protected set to detect
      DUPLICATE entries.

    Args:
        rd_torrents: Raw RD API torrent list.
        active_hashes: Lowercase info hashes from local db (ACTIVE).
        active_rd_ids: RD ID strings from local db (ACTIVE).
        symlink_mount_names: Lowercase mount-relative names from symlinks.
        live_mount_set: Optional set of ``(normalized_title, season)`` tuples
            from symlinks whose source_path still exists on the filesystem.
            When provided, enables Zurg auto-recovery protection.  Defaults to
            None (disables the 4th protection check).

    Returns:
        Tuple of:
        - ``categorized``: List of CategorizedTorrent in display order.
        - ``category_map``: ``rd_id -> RdTorrentCategory`` for execute lookups.
        - ``hash_map``: ``rd_id -> info_hash`` for mark_torrent_removed calls.
    """
    _live_mount_set: set[tuple[str, int | None]] = live_mount_set or set()

    ptn_groups = _build_ptn_groups(rd_torrents)
    rd_id_to_filename: dict[str, str] = {
        str(rd.get("id") or ""): rd.get("filename") or ""
        for rd in rd_torrents
        if rd.get("id")
    }

    # Pass 1 — collect all Protected rd_ids
    protected_rd_ids: set[str] = set()
    for rd in rd_torrents:
        rd_id: str = str(rd.get("id") or "")
        raw_hash: str = str(rd.get("hash") or "")
        info_hash = raw_hash.lower()
        filename: str = rd.get("filename") or ""

        if info_hash and info_hash in active_hashes:
            protected_rd_ids.add(rd_id)
        elif rd_id and rd_id in active_rd_ids:
            protected_rd_ids.add(rd_id)
        else:
            candidates: list[str] = []
            if filename:
                candidates.append(filename.lower())
                candidates.append(_normalize_name(filename).lower())
            matched = False
            for candidate in candidates:
                if candidate in symlink_mount_names:
                    protected_rd_ids.add(rd_id)
                    matched = True
                    break
            # Live mount check: PTN-parse the RD filename and look up the
            # (normalized_title, season) tuple in the live mount set.
            if not matched and _live_mount_set and filename:
                ptn = _parse_filename(filename)
                raw_title: str = ptn.get("title") or ""
                if raw_title:
                    norm_title = _normalize_title(raw_title)
                    season: int | None = ptn.get("season")
                    if (norm_title, season) in _live_mount_set:
                        protected_rd_ids.add(rd_id)

    # Pass 2 — full categorization
    categorized: list[CategorizedTorrent] = []
    category_map: dict[str, RdTorrentCategory] = {}
    hash_map: dict[str, str] = {}

    for rd in rd_torrents:
        ct = _categorize_torrent(
            rd=rd,
            active_hashes=active_hashes,
            active_rd_ids=active_rd_ids,
            symlink_mount_names=symlink_mount_names,
            live_mount_set=_live_mount_set,
            protected_rd_ids=protected_rd_ids,
            ptn_groups=ptn_groups,
            rd_id_to_filename=rd_id_to_filename,
        )
        categorized.append(ct)
        category_map[ct.rd_id] = ct.category
        if ct.info_hash:
            hash_map[ct.rd_id] = ct.info_hash

    return categorized, category_map, hash_map


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def scan_rd_account(session: AsyncSession) -> RdCleanupScan:
    """Fetch and categorize all torrents in the user's Real-Debrid account.

    Builds three protection sets from the local database, then assigns every
    RD account torrent to a category bucket (Protected > Dead > Stale >
    Duplicate > Orphaned).  The result is cached in ``_last_scan_cache`` for up
    to ``_SCAN_TTL_SECONDS`` seconds so that a subsequent ``execute_rd_cleanup``
    call can reuse the RD data without a second round-trip.

    Args:
        session: Async database session (read-only queries only).

    Returns:
        RdCleanupScan with summaries and per-torrent categorization.

    Raises:
        Exception: When the RD API call fails (propagated to caller so the route
            can return an appropriate HTTP error).
    """
    from src.services.real_debrid import RealDebridClient

    rd_client = RealDebridClient()
    warnings: list[str] = []

    logger.info("rd_cleanup.scan_rd_account: fetching all RD account torrents")
    try:
        rd_torrents = await rd_client.list_all_torrents()
    except Exception as exc:
        logger.error("rd_cleanup.scan_rd_account: list_all_torrents failed: %s", exc)
        raise

    logger.info("rd_cleanup.scan_rd_account: fetched %d torrents", len(rd_torrents))

    # Build protection sets from local DB.  An incomplete protection set risks
    # classifying protected torrents as deletable — let the exception propagate
    # so the caller can return an appropriate HTTP error rather than silently
    # presenting unsafe categorization results.
    active_hashes, active_rd_ids, symlink_mount_names, live_mount_set = (
        await _build_protection_sets(session)
    )

    # Warn if live mount protection is unavailable (mount down).
    if not live_mount_set and symlink_mount_names:
        warnings.append(
            "Zurg mount appears unavailable — live mount protection disabled for "
            "this scan. Zurg auto-recovered torrents may not be detected as protected."
        )

    # Detect stale rd_ids: active db records whose rd_id is not present in the
    # current RD account — a sign that Zurg auto-recovery replaced the torrent.
    rd_account_ids: set[str] = {str(rd.get("id") or "") for rd in rd_torrents if rd.get("id")}
    stale_rd_id_count = sum(
        1 for rid in active_rd_ids if rid and rid not in rd_account_ids
    )
    if stale_rd_id_count:
        warning_msg = (
            f"{stale_rd_id_count} tracked torrent(s) have rd_ids not found in your "
            "RD account (possible Zurg auto-recovery)"
        )
        warnings.append(warning_msg)
        logger.warning("rd_cleanup.scan_rd_account: %s", warning_msg)

    categorized, category_map, hash_map = _categorize_all(
        rd_torrents, active_hashes, active_rd_ids, symlink_mount_names, live_mount_set
    )

    summaries = _build_summaries(categorized)
    scanned_at = datetime.now(tz=UTC).isoformat()

    # Sort: actionable categories first (Dead, Stale, Duplicate, Orphaned),
    # Protected last since those are informational only.
    _ORDER = {
        RdTorrentCategory.DEAD: 0,
        RdTorrentCategory.STALE: 1,
        RdTorrentCategory.DUPLICATE: 2,
        RdTorrentCategory.ORPHANED: 3,
        RdTorrentCategory.PROTECTED: 4,
    }
    categorized.sort(key=lambda t: (_ORDER[t.category], t.filename.lower()))

    # Store in module-level cache for reuse by execute_rd_cleanup
    _last_scan_cache["scanned_at"] = datetime.now(tz=UTC)
    _last_scan_cache["rd_torrents"] = rd_torrents
    _last_scan_cache["category_map"] = category_map
    _last_scan_cache["hash_map"] = hash_map

    scan = RdCleanupScan(
        scanned_at=scanned_at,
        total_torrents=len(rd_torrents),
        summaries=summaries,
        torrents=categorized,
        warnings=warnings,
    )

    protected_count = sum(1 for t in categorized if t.category == RdTorrentCategory.PROTECTED)
    dead_count = sum(1 for t in categorized if t.category == RdTorrentCategory.DEAD)
    stale_count = sum(1 for t in categorized if t.category == RdTorrentCategory.STALE)
    dup_count = sum(1 for t in categorized if t.category == RdTorrentCategory.DUPLICATE)
    orphan_count = sum(1 for t in categorized if t.category == RdTorrentCategory.ORPHANED)

    logger.info(
        "rd_cleanup.scan_rd_account: total=%d protected=%d dead=%d stale=%d "
        "duplicate=%d orphaned=%d",
        len(rd_torrents),
        protected_count,
        dead_count,
        stale_count,
        dup_count,
        orphan_count,
    )

    return scan


async def execute_rd_cleanup(
    session: AsyncSession, rd_ids: list[str]
) -> RdCleanupExecuteResult:
    """Delete the specified RD torrents, skipping Protected and unknown entries.

    Reuses the cached scan data when it is less than ``_SCAN_TTL_SECONDS`` old;
    otherwise re-fetches from RD.  Protected torrents in the request list are
    silently rejected (counted in ``rejected_protected``).

    Deletions are issued sequentially; the first ``RealDebridRateLimitError``
    halts all further attempts so that no additional API budget is wasted.

    After each successful deletion, ``mark_torrent_removed`` is called to keep
    the local dedup registry in sync.

    Args:
        session: Async database session (used for ``mark_torrent_removed``).
        rd_ids: List of RD torrent IDs the user has selected for deletion.

    Returns:
        RdCleanupExecuteResult with counts for all outcomes.
    """
    from src.core.dedup import dedup_engine
    from src.services.real_debrid import RealDebridClient, RealDebridRateLimitError

    result = RdCleanupExecuteResult(
        requested=len(rd_ids),
        rejected_protected=0,
        rejected_not_found=0,
        deleted=0,
        failed=0,
    )

    if not rd_ids:
        return result

    # ------------------------------------------------------------------
    # Step 1: Obtain category_map and hash_map (from cache or fresh scan)
    # ------------------------------------------------------------------
    cached_at: datetime | None = _last_scan_cache.get("scanned_at")
    now = datetime.now(tz=UTC)
    cache_fresh = (
        cached_at is not None
        and (now - cached_at).total_seconds() < _SCAN_TTL_SECONDS
        and _last_scan_cache.get("category_map") is not None
    )

    if cache_fresh:
        logger.info(
            "rd_cleanup.execute_rd_cleanup: reusing cached scan "
            "(age=%.0fs)", (now - cached_at).total_seconds()  # type: ignore[operator]
        )
        category_map: dict[str, RdTorrentCategory] = _last_scan_cache["category_map"]  # type: ignore[assignment]
        hash_map: dict[str, str] = _last_scan_cache["hash_map"]  # type: ignore[assignment]

        # Re-validate protection sets from fresh DB state (cheap: DB queries).
        # One-way upgrade only: items can become Protected, never downgraded.
        fresh_hashes, fresh_rd_ids, fresh_mount_names, fresh_live_mount_set = (
            await _build_protection_sets(session)
        )
        upgraded = 0
        # Build an O(1) lookup from rd_id → (filename_lower, filename_norm)
        # once before the loop to avoid an O(N*M) scan on every iteration.
        rd_torrents_cache: list[dict] | None = _last_scan_cache.get("rd_torrents")
        rd_id_to_filename: dict[str, tuple[str, str, str]] = {}
        if rd_torrents_cache:
            for rd in rd_torrents_cache:
                rid_key = str(rd.get("id") or "")
                if rid_key:
                    fn = rd.get("filename") or ""
                    rd_id_to_filename[rid_key] = (fn, fn.lower(), _normalize_name(fn).lower())

        for rid, cat in list(category_map.items()):
            if cat == RdTorrentCategory.PROTECTED:
                continue
            # Check if this entry now qualifies as protected
            cached_hash = hash_map.get(rid, "")
            rd_filename_orig, rd_filename_lower, rd_filename_norm = rd_id_to_filename.get(rid, ("", "", ""))
            newly_protected = (
                (cached_hash and cached_hash in fresh_hashes)
                or (rid and rid in fresh_rd_ids)
                or (rd_filename_lower and rd_filename_lower in fresh_mount_names)
                or (rd_filename_norm and rd_filename_norm in fresh_mount_names)
            )
            # Live mount set check: PTN-parse the original RD filename and look up
            # (normalized_title, season) in the fresh live mount set.
            if not newly_protected and fresh_live_mount_set:
                if rd_filename_orig:
                    ptn = _parse_filename(rd_filename_orig)
                    raw_title: str = ptn.get("title") or ""
                    if raw_title:
                        norm_title = _normalize_title(raw_title)
                        rd_season: int | None = ptn.get("season")
                        if (norm_title, rd_season) in fresh_live_mount_set:
                            newly_protected = True
            if newly_protected:
                category_map[rid] = RdTorrentCategory.PROTECTED
                upgraded += 1

        if upgraded:
            logger.info(
                "rd_cleanup.execute_rd_cleanup: re-validation upgraded %d "
                "cached item(s) to PROTECTED based on fresh DB state",
                upgraded,
            )
    else:
        logger.info(
            "rd_cleanup.execute_rd_cleanup: cache expired or missing — "
            "re-fetching RD torrent list"
        )
        rd_client_for_scan = RealDebridClient()
        try:
            rd_torrents = await rd_client_for_scan.list_all_torrents()
        except Exception as exc:
            logger.error(
                "rd_cleanup.execute_rd_cleanup: list_all_torrents failed: %s", exc
            )
            raise

        active_hashes, active_rd_ids, symlink_mount_names, live_mount_set = (
            await _build_protection_sets(session)
        )
        _, category_map, hash_map = _categorize_all(
            rd_torrents, active_hashes, active_rd_ids, symlink_mount_names, live_mount_set
        )
        _last_scan_cache["scanned_at"] = now
        _last_scan_cache["rd_torrents"] = rd_torrents
        _last_scan_cache["category_map"] = category_map
        _last_scan_cache["hash_map"] = hash_map

    # ------------------------------------------------------------------
    # Step 2: Partition requested ids into delete_list vs. rejected
    # ------------------------------------------------------------------
    delete_list: list[str] = []

    for rd_id in rd_ids:
        cat = category_map.get(rd_id)
        if cat is None:
            result.rejected_not_found += 1
            logger.warning(
                "rd_cleanup.execute_rd_cleanup: rd_id=%s not found in scan — skipped",
                rd_id,
            )
        elif cat == RdTorrentCategory.PROTECTED:
            result.rejected_protected += 1
            logger.warning(
                "rd_cleanup.execute_rd_cleanup: rd_id=%s is PROTECTED — skipped",
                rd_id,
            )
        else:
            delete_list.append(rd_id)

    if not delete_list:
        logger.info(
            "rd_cleanup.execute_rd_cleanup: nothing to delete after filtering "
            "(rejected_protected=%d rejected_not_found=%d)",
            result.rejected_protected,
            result.rejected_not_found,
        )
        return result

    logger.info(
        "rd_cleanup.execute_rd_cleanup: deleting %d torrents "
        "(rejected_protected=%d rejected_not_found=%d)",
        len(delete_list),
        result.rejected_protected,
        result.rejected_not_found,
    )

    # ------------------------------------------------------------------
    # Step 3: Sequential deletion — stops immediately on rate limit
    # ------------------------------------------------------------------
    # Unlike the gather+Semaphore(5) pattern used elsewhere, we iterate
    # sequentially here so that the first 429 prevents ALL subsequent
    # API calls.  With gather, 5 in-flight requests would race and
    # waste rate-limit budget.
    rd_client = RealDebridClient()
    deleted_ids: list[str] = []
    failed_ids: list[str] = []
    rate_limited = False

    for rd_id in delete_list:
        if rate_limited:
            failed_ids.append(rd_id)
            continue
        try:
            await rd_client.delete_torrent(rd_id)
            deleted_ids.append(rd_id)
            logger.info(
                "rd_cleanup.execute_rd_cleanup: deleted rd_id=%s", rd_id
            )
        except RealDebridRateLimitError:
            rate_limited = True
            failed_ids.append(rd_id)
            logger.warning(
                "rd_cleanup.execute_rd_cleanup: rate-limited on rd_id=%s — "
                "stopping further deletions (%d remaining)",
                rd_id,
                len(delete_list) - len(deleted_ids) - len(failed_ids),
            )
        except Exception as exc:
            failed_ids.append(rd_id)
            msg = f"rd_id={rd_id}: {exc}"
            result.errors.append(msg)
            logger.warning(
                "rd_cleanup.execute_rd_cleanup: delete failed %s", msg
            )

    result.deleted = len(deleted_ids)
    result.failed = len(failed_ids)
    result.rate_limited = rate_limited

    # ------------------------------------------------------------------
    # Step 4: Mark successfully deleted torrents as REMOVED in local DB
    # ------------------------------------------------------------------
    for rd_id in deleted_ids:
        info_hash = hash_map.get(rd_id)
        if info_hash:
            try:
                await dedup_engine.mark_torrent_removed(session, info_hash)
            except Exception as exc:
                logger.warning(
                    "rd_cleanup.execute_rd_cleanup: mark_torrent_removed failed "
                    "rd_id=%s info_hash=%s: %s",
                    rd_id,
                    info_hash,
                    exc,
                )
        else:
            logger.debug(
                "rd_cleanup.execute_rd_cleanup: no info_hash in hash_map for "
                "rd_id=%s — skipping mark_torrent_removed",
                rd_id,
            )

    # ------------------------------------------------------------------
    # Step 5: Invalidate cache so the next scan reflects reality
    # ------------------------------------------------------------------
    if deleted_ids:
        _last_scan_cache["scanned_at"] = None
        _last_scan_cache["rd_torrents"] = None
        _last_scan_cache["category_map"] = None
        _last_scan_cache["hash_map"] = None
        logger.debug("rd_cleanup.execute_rd_cleanup: scan cache invalidated")

    logger.info(
        "rd_cleanup.execute_rd_cleanup: complete — deleted=%d failed=%d "
        "rate_limited=%s",
        result.deleted,
        result.failed,
        result.rate_limited,
    )
    return result
