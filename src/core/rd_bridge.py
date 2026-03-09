"""RD Torrent Bridging — link Real-Debrid account torrents to migrated MediaItems.

Migration tool creates MediaItems (state=COMPLETE, source='migration') with Symlinks
but NO RdTorrent records, because the original content came from cli_debrid and the RD
account already held the actual torrents.

This module bridges the gap by:

1. Fetching all torrents currently in the RD account via ``list_all_torrents()``.
2. Building a lookup dict keyed by lowercased filename.
3. Querying migration items that have at least one Symlink but no RdTorrent.
4. For each unmatched item: extracting the directory/file name from the symlink's
   ``source_path`` relative to the configured ``zurg_mount`` root, then matching it
   against RD filenames (exact first, then normalised fallback).
5. On match: creating an ``RdTorrent`` record via ``dedup_engine.register_torrent()``
   so the dedup engine knows about it and the enhanced ``remove_duplicates`` can later
   clean up the RD account when duplicates are removed.

Design notes:
- Mutual exclusion is enforced by the caller (``_cleanup_lock`` in the route handler).
- Shared season-pack torrents (one RD torrent backing multiple items) are handled
  gracefully: ``register_torrent`` upserts by ``info_hash``, so the second item
  that points to the same hash just updates the existing record (no duplicate row).
- Single-file torrents (source_path is directly under mount root) are matched by
  filename directly rather than directory name.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.dedup import dedup_engine
from src.models.symlink import Symlink

logger = logging.getLogger(__name__)

# Regex to strip dots and underscores for normalised matching.
_NORM_RE = re.compile(r"[._\s]+")


# ---------------------------------------------------------------------------
# Result schema
# ---------------------------------------------------------------------------


class BridgeResult(BaseModel):
    """Result of an RD torrent bridging run."""

    total_rd_torrents: int = 0
    """Number of torrents fetched from the RD account."""
    total_migration_items: int = 0
    """Number of migration items that lacked an RdTorrent record."""
    matched: int = 0
    """Number of items successfully bridged to an RD torrent."""
    already_bridged: int = 0
    """Number of items that already had an RdTorrent record (skipped)."""
    unmatched_items: int = 0
    """Number of items where no matching RD torrent could be found."""
    errors: list[str] = []
    """Per-item error messages for unexpected failures."""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _normalize_name(name: str) -> str:
    """Return a lowercased, separator-collapsed version of *name*.

    Replaces consecutive dots, underscores, and spaces with a single space
    so that ``Breaking.Bad`` and ``Breaking_Bad`` map to the same key.

    Args:
        name: Raw filename or directory name.

    Returns:
        Lowercased, normalised string.
    """
    return _NORM_RE.sub(" ", name).lower().strip()


def _extract_mount_relative_name(source_path: str, zurg_mount: str) -> str | None:
    """Extract the first path component of *source_path* relative to *zurg_mount*.

    For a directory-backed torrent the symlink source_path will be something like
    ``/mnt/__all__/Breaking.Bad.S01.1080p/Breaking.Bad.S01E01.mkv`` — we want
    ``Breaking.Bad.S01.1080p`` (the directory name, which matches the RD filename).

    For a single-file torrent the source_path may be directly under the mount root:
    ``/mnt/__all__/Some.Movie.2024.mkv`` — in that case we return the filename itself
    (the caller checks whether it is a file or directory).

    Args:
        source_path: Absolute path to the symlink target (source in FUSE mount).
        zurg_mount: Absolute path to the Zurg FUSE mount root.

    Returns:
        The first path component relative to the mount, or None when source_path
        is not under zurg_mount.
    """
    try:
        rel = Path(source_path).relative_to(zurg_mount)
    except ValueError:
        return None

    parts = rel.parts
    if not parts:
        return None

    # First part is either the torrent directory name or the file itself.
    return parts[0]


# ---------------------------------------------------------------------------
# Core function
# ---------------------------------------------------------------------------


async def bridge_rd_torrents(session: AsyncSession) -> BridgeResult:
    """Bridge RD account torrents to migration MediaItems that lack an RdTorrent row.

    Mutual exclusion is enforced by the caller (``_cleanup_lock`` in the route).
    This function performs its work directly without any inner locking.

    Args:
        session: Async database session.  The caller is responsible for
                 committing or rolling back.

    Returns:
        A BridgeResult with counts for all outcomes.
    """
    return await _bridge_rd_torrents_inner(session)


async def _bridge_rd_torrents_inner(session: AsyncSession) -> BridgeResult:
    """Inner implementation of bridge_rd_torrents."""
    from src.config import settings
    from src.services.real_debrid import RealDebridClient

    result = BridgeResult()
    rd_client = RealDebridClient()

    # ------------------------------------------------------------------
    # Step 1: Fetch all RD torrents
    # ------------------------------------------------------------------
    logger.info("bridge_rd_torrents: fetching all RD account torrents")
    try:
        rd_torrents = await rd_client.list_all_torrents()
    except Exception as exc:
        logger.error("bridge_rd_torrents: failed to fetch RD torrents: %s", exc)
        result.errors.append(f"Failed to fetch RD torrents: {exc}")
        return result

    result.total_rd_torrents = len(rd_torrents)
    logger.info("bridge_rd_torrents: fetched %d RD torrents", result.total_rd_torrents)

    if not rd_torrents:
        return result

    # ------------------------------------------------------------------
    # Step 2: Build lookup dicts (exact + normalised)
    # ------------------------------------------------------------------
    # exact_map: lowercase filename -> rd_dict
    exact_map: dict[str, dict] = {}
    # norm_map: normalised filename -> rd_dict (used as fallback)
    norm_map: dict[str, dict] = {}

    for rd in rd_torrents:
        filename: str = rd.get("filename") or ""
        if not filename:
            continue
        lower = filename.lower()
        exact_map[lower] = rd
        norm_key = _normalize_name(filename)
        if norm_key and norm_key not in norm_map:
            norm_map[norm_key] = rd

    # ------------------------------------------------------------------
    # Step 3: Build set of info_hashes already registered in rd_torrents
    # ------------------------------------------------------------------
    rows_hashes = await session.execute(
        text("SELECT info_hash FROM rd_torrents WHERE info_hash IS NOT NULL")
    )
    known_hashes: set[str] = {row[0] for row in rows_hashes}

    # ------------------------------------------------------------------
    # Step 4: Query ALL migration items that have symlinks
    # ------------------------------------------------------------------
    rows_items = await session.execute(
        text(
            "SELECT DISTINCT mi.id "
            "FROM media_items mi "
            "INNER JOIN symlinks sl ON sl.media_item_id = mi.id "
            "WHERE mi.source = 'migration'"
        )
    )
    migration_item_ids: list[int] = [row[0] for row in rows_items]
    result.total_migration_items = len(migration_item_ids)

    logger.info(
        "bridge_rd_torrents: %d migration items to process",
        result.total_migration_items,
    )

    if not migration_item_ids:
        return result

    # ------------------------------------------------------------------
    # Step 5: Match each item to an RD torrent
    # ------------------------------------------------------------------
    # Track hashes we've already registered in THIS run so we don't keep
    # calling register_torrent for every episode of the same season pack.
    seen_hashes: set[str] = set()
    zurg_mount = settings.paths.zurg_mount.rstrip("/")

    for item_id in migration_item_ids:
        try:
            await _bridge_single_item(
                session=session,
                item_id=item_id,
                zurg_mount=zurg_mount,
                exact_map=exact_map,
                norm_map=norm_map,
                known_hashes=known_hashes,
                seen_hashes=seen_hashes,
                result=result,
            )
        except Exception as exc:
            logger.error(
                "bridge_rd_torrents: unexpected error for item_id=%d: %s", item_id, exc
            )
            result.errors.append(f"Item {item_id}: {exc}")

    logger.info(
        "bridge_rd_torrents: complete — matched=%d unmatched=%d errors=%d",
        result.matched,
        result.unmatched_items,
        len(result.errors),
    )
    return result


async def _bridge_single_item(
    session: AsyncSession,
    item_id: int,
    zurg_mount: str,
    exact_map: dict[str, dict],
    norm_map: dict[str, dict],
    known_hashes: set[str],
    seen_hashes: set[str],
    result: BridgeResult,
) -> None:
    """Attempt to find and register an RD torrent for one migration item.

    Args:
        session: Async database session.
        item_id: MediaItem primary key.
        zurg_mount: Absolute path to the Zurg FUSE mount root.
        exact_map: Lowercase RD filename -> RD torrent dict.
        norm_map: Normalised RD filename -> RD torrent dict.
        known_hashes: Info hashes already in rd_torrents before this run.
        seen_hashes: Info hashes already registered in this run (mutable).
        result: Mutable BridgeResult to update with outcome.
    """
    # Fetch the first symlink for this item.
    sym_result = await session.execute(
        select(Symlink)
        .where(Symlink.media_item_id == item_id)
        .order_by(Symlink.id)
        .limit(1)
    )
    symlink = sym_result.scalar_one_or_none()

    if symlink is None:
        logger.debug("bridge_rd_torrents: item_id=%d has no symlinks, skipping", item_id)
        result.unmatched_items += 1
        return

    source_path: str = symlink.source_path
    mount_name = _extract_mount_relative_name(source_path, zurg_mount)

    if mount_name is None:
        logger.warning(
            "bridge_rd_torrents: item_id=%d source_path=%r is not under zurg_mount=%r",
            item_id,
            source_path,
            zurg_mount,
        )
        result.unmatched_items += 1
        return

    # Try exact match first.
    rd_dict = exact_map.get(mount_name.lower())

    # Fall back to normalised match.
    if rd_dict is None:
        norm_key = _normalize_name(mount_name)
        rd_dict = norm_map.get(norm_key)

    if rd_dict is None:
        logger.debug(
            "bridge_rd_torrents: item_id=%d mount_name=%r — no RD match found",
            item_id,
            mount_name,
        )
        result.unmatched_items += 1
        return

    rd_id: str = str(rd_dict.get("id") or "")
    raw_hash: str | None = rd_dict.get("hash") or None
    info_hash: str | None = raw_hash.lower() if raw_hash else None
    filename: str = rd_dict.get("filename") or mount_name
    filesize: int = int(rd_dict.get("bytes") or 0)

    if not info_hash:
        logger.warning(
            "bridge_rd_torrents: item_id=%d rd_id=%s has no info_hash — skipping "
            "(cannot dedup without a hash)",
            item_id,
            rd_id,
        )
        result.unmatched_items += 1
        return

    # If this hash was already in the DB or registered earlier in this run,
    # the item is covered — no need to call register_torrent again (which
    # would just overwrite media_item_id on the same row).
    if info_hash in known_hashes or info_hash in seen_hashes:
        result.already_bridged += 1
        return

    # First item for this hash — create the RdTorrent record.
    try:
        await dedup_engine.register_torrent(
            session,
            rd_id=rd_id or None,
            info_hash=info_hash or None,
            magnet_uri=None,
            media_item_id=item_id,
            filename=filename,
            filesize=filesize,
            resolution=None,
            cached=True,
        )
    except Exception as exc:
        logger.error(
            "bridge_rd_torrents: register_torrent failed item_id=%d rd_id=%s: %s",
            item_id,
            rd_id,
            exc,
        )
        result.errors.append(f"Item {item_id} (rd_id={rd_id}): register failed: {exc}")
        return

    seen_hashes.add(info_hash)
    logger.info(
        "bridge_rd_torrents: bridged item_id=%d -> rd_id=%s info_hash=%s filename=%r",
        item_id,
        rd_id,
        info_hash,
        filename,
    )
    result.matched += 1
