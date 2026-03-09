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

import enum
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import PTN
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.rd_bridge import _extract_mount_relative_name, _normalize_name

# Matches the Zurg /__all__/ virtual directory in any mount path.
_ALL_DIR_MARKER = "/__all__/"
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


def _extract_mount_name_any_base(source_path: str) -> str | None:
    """Extract the torrent directory/file name from a Zurg mount path.

    Unlike ``_extract_mount_relative_name`` (which requires the exact mount
    prefix), this finds ``/__all__/`` anywhere in the path and returns the
    first component after it.  Handles symlinks that reference alternative
    mount points (e.g. ``rclone_RD/__all__/`` vs ``__all__/``).

    Args:
        source_path: Absolute symlink source path.

    Returns:
        The torrent directory name (first component after ``/__all__/``),
        or None when the marker is not found.
    """
    idx = source_path.find(_ALL_DIR_MARKER)
    if idx == -1:
        return None
    rest = source_path[idx + len(_ALL_DIR_MARKER):]
    if not rest:
        return None
    # First path component after /__all__/
    slash = rest.find("/")
    return rest[:slash] if slash != -1 else rest


class RdTorrentCategory(str, enum.Enum):
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
            dt = dt.replace(tzinfo=timezone.utc)
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


async def _build_protection_sets(
    session: AsyncSession,
) -> tuple[set[str], set[str], set[str]]:
    """Load protection identifiers from the local database.

    Three complementary sets are returned so every protection avenue is covered:

    1. ``active_hashes`` — info hashes in ``rd_torrents`` with status=ACTIVE.
    2. ``active_rd_ids`` — ``rd_id`` values in ``rd_torrents`` with status=ACTIVE.
    3. ``symlink_mount_names`` — lowercase first-path-component extracted from
       every ``symlinks.source_path`` using ``_extract_mount_relative_name``.

    Args:
        session: Async database session.

    Returns:
        Tuple of ``(active_hashes, active_rd_ids, symlink_mount_names)``.
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
    fallback_count = 0
    for (source_path,) in symlink_rows:
        if not source_path:
            continue
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

    logger.debug(
        "rd_cleanup: protection sets — active_hashes=%d active_rd_ids=%d "
        "symlink_mount_names=%d",
        len(active_hashes),
        len(active_rd_ids),
        len(symlink_mount_names),
    )
    return active_hashes, active_rd_ids, symlink_mount_names


def _categorize_torrent(
    rd: dict[str, Any],
    active_hashes: set[str],
    active_rd_ids: set[str],
    symlink_mount_names: set[str],
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
        # Check against symlink mount names
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
            age = datetime.now(tz=timezone.utc) - added_dt
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

    Returns:
        Tuple of:
        - ``categorized``: List of CategorizedTorrent in display order.
        - ``category_map``: ``rd_id -> RdTorrentCategory`` for execute lookups.
        - ``hash_map``: ``rd_id -> info_hash`` for mark_torrent_removed calls.
    """
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
            for candidate in candidates:
                if candidate in symlink_mount_names:
                    protected_rd_ids.add(rd_id)
                    break

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

    # Build protection sets from local DB
    try:
        active_hashes, active_rd_ids, symlink_mount_names = (
            await _build_protection_sets(session)
        )
    except Exception as exc:
        logger.error(
            "rd_cleanup.scan_rd_account: failed to build protection sets: %s", exc
        )
        warnings.append(f"Could not load all protection data — results may miss some protected items: {exc}")
        active_hashes, active_rd_ids, symlink_mount_names = set(), set(), set()

    categorized, category_map, hash_map = _categorize_all(
        rd_torrents, active_hashes, active_rd_ids, symlink_mount_names
    )

    summaries = _build_summaries(categorized)
    scanned_at = datetime.now(tz=timezone.utc).isoformat()

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
    _last_scan_cache["scanned_at"] = datetime.now(tz=timezone.utc)
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

    Deletions are issued concurrently with a ``Semaphore(5)`` and a
    ``stop_flag`` that halts further attempts on the first ``RealDebridRateLimitError``
    (matching the pattern used elsewhere in the tools routes).

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
    now = datetime.now(tz=timezone.utc)
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

        active_hashes, active_rd_ids, symlink_mount_names = (
            await _build_protection_sets(session)
        )
        _, category_map, hash_map = _categorize_all(
            rd_torrents, active_hashes, active_rd_ids, symlink_mount_names
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
