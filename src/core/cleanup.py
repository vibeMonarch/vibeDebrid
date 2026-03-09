"""Liveness-aware duplicate cleanup for migrated media items.

This module provides a smarter alternative to the basic ``find_duplicates`` /
``remove_duplicates`` approach in ``backfill.py``.  Instead of blindly keeping
the lowest-ID row, it inspects the actual filesystem state (does the source
file exist on the Zurg mount?) and the RD account bridge (does the item have an
``RdTorrent`` record?) to decide which duplicate to keep.

Liveness tiers (best to worst):
    LIVE    — source_path exists on the Zurg mount right now.
    BRIDGED — no source_path, but an ``RdTorrent`` record is present.
    DEAD    — no source_path and no ``RdTorrent`` record.

Keeper selection:
    1. Highest liveness tier.
    2. Among equal liveness: non-migration source preferred.
    3. Tiebreaker: lowest ``media_items.id``.

The public workflow is:
    assessed = await assess_migration_items(session)
    preview  = await build_cleanup_preview(session, assessed)
    result   = await execute_cleanup(session, preview)
    # route then calls RD API to delete preview.rd_ids_to_delete

Design notes:
- All filesystem checks use ``asyncio.to_thread`` to avoid blocking the loop.
- Path checks are batched (50 at a time) for large libraries.
- The RD-protection logic mirrors ``remove_duplicates`` in ``backfill.py``:
  only delete an ``rd_id`` when no kept item references the same hash.
- ``execute_cleanup`` does NOT commit — the caller owns the transaction.
"""

from __future__ import annotations

import asyncio
import enum
import logging
import os
import re
from pathlib import Path
from typing import Any

from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

_BATCH_SIZE = 50


# ---------------------------------------------------------------------------
# Enums and schemas
# ---------------------------------------------------------------------------


class ItemLiveness(str, enum.Enum):
    """Filesystem + RD presence classification for a MediaItem."""

    LIVE = "live"
    """Source path exists on the Zurg mount."""
    BRIDGED = "bridged"
    """No source path, but an RdTorrent record is present."""
    DEAD = "dead"
    """Neither a source path nor an RdTorrent record."""


class AssessedItem(BaseModel):
    """Full liveness assessment for a single MediaItem."""

    item_id: int
    title: str
    season: int | None
    episode: int | None
    imdb_id: str | None
    source: str | None
    state: str
    liveness: ItemLiveness
    source_path: str | None
    link_path: str | None
    source_exists: bool
    link_valid: bool
    has_rd_torrent: bool
    rd_id: str | None
    info_hash: str | None
    rd_filename: str | None


class SmartDuplicateGroup(BaseModel):
    """One duplicate group with a chosen keeper and a list of items to remove."""

    imdb_id: str | None
    title: str
    season: int | None
    episode: int | None
    keep: AssessedItem
    remove: list[AssessedItem]
    reason: str
    """Human-readable explanation of why the keeper was chosen."""
    count: int
    """Total items in the group (1 keeper + len(remove))."""


class CleanupPreview(BaseModel):
    """Full preview of what a smart cleanup run will do."""

    total_items_assessed: int = 0
    live_count: int = 0
    bridged_count: int = 0
    dead_count: int = 0
    groups: list[SmartDuplicateGroup] = []
    total_to_remove: int = 0
    rd_ids_to_delete: list[str] = []
    """RD torrent IDs safe to delete (not referenced by any kept item)."""
    rd_ids_protected: list[str] = []
    """RD torrent IDs shared with kept items — must NOT be deleted."""
    symlinks_to_delete: int = 0
    warnings: list[str] = []


class CleanupResult(BaseModel):
    """Result returned after executing a cleanup."""

    groups_processed: int = 0
    items_removed: int = 0
    items_kept: int = 0
    symlinks_deleted_from_disk: int = 0
    rd_torrents_deleted: int = 0
    rd_torrents_failed: int = 0
    errors: list[str] = []


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


async def _batch_check_paths(paths: list[str]) -> dict[str, bool]:
    """Check ``os.path.exists`` for *paths* in parallel batches of 50.

    Args:
        paths: Absolute filesystem paths to check.

    Returns:
        Mapping of path -> True/False.
    """
    results: dict[str, bool] = {}

    async def _check_one(path: str) -> tuple[str, bool]:
        exists = await asyncio.to_thread(os.path.exists, path)
        return path, exists

    for i in range(0, len(paths), _BATCH_SIZE):
        batch = paths[i : i + _BATCH_SIZE]
        pairs = await asyncio.gather(*[_check_one(p) for p in batch])
        for path, exists in pairs:
            results[path] = exists

    return results


async def _batch_check_symlinks(
    link_paths: list[str],
    expected_sources: list[str],
) -> dict[str, bool]:
    """Check whether symlinks point to the expected source path.

    For each ``(link_path, expected_source)`` pair, returns True only when:
    - ``os.path.islink(link_path)`` is True AND
    - ``os.readlink(link_path) == expected_source``

    Args:
        link_paths: Paths of the symlinks to check.
        expected_sources: Corresponding expected target paths.

    Returns:
        Mapping of link_path -> True/False.
    """
    results: dict[str, bool] = {}

    async def _check_one(link: str, expected: str) -> tuple[str, bool]:
        def _sync_check(link: str, expected: str) -> bool:
            if not os.path.islink(link):
                return False
            try:
                return os.readlink(link) == expected
            except OSError:
                return False

        valid = await asyncio.to_thread(_sync_check, link, expected)
        return link, valid

    pairs_input = list(zip(link_paths, expected_sources))
    for i in range(0, len(pairs_input), _BATCH_SIZE):
        batch = pairs_input[i : i + _BATCH_SIZE]
        checked = await asyncio.gather(*[_check_one(lp, es) for lp, es in batch])
        for link_path, valid in checked:
            results[link_path] = valid

    return results


def _classify_liveness(
    source_exists: bool,
    link_valid: bool,  # noqa: ARG001  (kept for symmetry / future use)
    has_rd_torrent: bool,
) -> ItemLiveness:
    """Classify an item's liveness tier.

    Args:
        source_exists: Whether the source file/dir is present on disk.
        link_valid: Whether the library symlink resolves correctly (unused in
                    tier logic but preserved for callers).
        has_rd_torrent: Whether an RdTorrent record is present.

    Returns:
        The ItemLiveness tier.
    """
    if source_exists:
        return ItemLiveness.LIVE
    if has_rd_torrent:
        return ItemLiveness.BRIDGED
    return ItemLiveness.DEAD


def _select_keeper(
    items: list[AssessedItem],
) -> tuple[AssessedItem, list[AssessedItem], str]:
    """Pick the best item to keep from a duplicate group.

    Priority:
        1. Highest liveness tier (LIVE > BRIDGED > DEAD).
        2. Among equal liveness: non-migration source preferred.
        3. Tiebreaker: lowest ``item_id``.

    Args:
        items: All items in the duplicate group (at least 2).

    Returns:
        ``(keeper, removals, reason_string)`` where removals = items minus keeper.
    """
    _tier_order = {ItemLiveness.LIVE: 0, ItemLiveness.BRIDGED: 1, ItemLiveness.DEAD: 2}

    def _sort_key(item: AssessedItem) -> tuple[int, int, int]:
        tier = _tier_order[item.liveness]
        is_migration = 1 if item.source == "migration" else 0
        return (tier, is_migration, item.item_id)

    sorted_items = sorted(items, key=_sort_key)
    keeper = sorted_items[0]
    rest = sorted_items[1:]

    # Only migration-sourced items may be removed.  Non-migration items that
    # are not the keeper are simply not touched — they will not appear in the
    # remove list even if they rank worse than the keeper.
    removals = [item for item in rest if item.source == "migration"]

    best_tier = keeper.liveness
    reason_parts: list[str] = []

    if best_tier == ItemLiveness.LIVE:
        reason_parts.append("source file present on mount")
    elif best_tier == ItemLiveness.BRIDGED:
        reason_parts.append("has RD torrent record")
    else:
        reason_parts.append("no live source (all dead)")

    if keeper.source != "migration":
        reason_parts.append(f"preferred non-migration source '{keeper.source}'")
    elif len(reason_parts) == 1 and best_tier == ItemLiveness.DEAD:
        reason_parts.append("kept lowest ID as tiebreaker")

    return keeper, removals, "; ".join(reason_parts)


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------


async def assess_migration_items(session: AsyncSession) -> list[AssessedItem]:
    """Assess liveness of all items that belong to migration duplicate groups.

    Only items whose (imdb_id, season, episode) key appears more than once in
    ``media_items WHERE source='migration' AND imdb_id IS NOT NULL`` are
    included.  ALL items sharing that key are fetched (regardless of source),
    so that queue-pipeline items competing with migration items are also
    assessed.

    Algorithm:
        1. Find duplicate group keys (imdb_id, season, episode) among migration
           items.
        2. Fetch all items (any source) matching those keys, LEFT JOIN symlinks
           and rd_torrents.
        3. Build an info_hash -> rd_torrent mapping from the full rd_torrents
           table to detect shared season-pack hashes even when the FK points
           to a different item.
        4. Batch-check source_path existence and symlink validity.
        5. Classify each item as LIVE / BRIDGED / DEAD.

    Args:
        session: Async database session.

    Returns:
        List of AssessedItem, one per item in any duplicate group.
        Returns an empty list when no migration duplicates exist.
    """
    # ------------------------------------------------------------------
    # Step 1: Find duplicate group keys
    # ------------------------------------------------------------------
    group_rows = await session.execute(
        text(
            "SELECT imdb_id, season, episode "
            "FROM media_items "
            "WHERE source = 'migration' AND imdb_id IS NOT NULL "
            "GROUP BY imdb_id, season, episode "
            "HAVING COUNT(*) > 1"
        )
    )
    group_keys: list[tuple[str, Any, Any]] = list(group_rows.fetchall())

    if not group_keys:
        logger.info("assess_migration_items: no migration duplicates found")
        return []

    logger.info(
        "assess_migration_items: %d duplicate group key(s) found", len(group_keys)
    )

    # ------------------------------------------------------------------
    # Step 2: Build comprehensive rd_torrents lookup
    # info_hash -> (rd_id, filename) for ALL rd_torrents so we can detect
    # items whose symlink source_path maps to a known torrent directory.
    # ------------------------------------------------------------------
    all_rd_rows = await session.execute(
        text(
            "SELECT media_item_id, rd_id, info_hash, filename "
            "FROM rd_torrents "
            "WHERE info_hash IS NOT NULL"
        )
    )
    # media_item_id -> (rd_id, info_hash, filename)
    rd_by_item: dict[int, tuple[str | None, str | None, str | None]] = {}
    # info_hash -> (rd_id, filename) — for shared-hash detection
    rd_by_hash: dict[str, tuple[str | None, str | None]] = {}
    for row in all_rd_rows:
        mi_id: int | None = row[0]
        rd_id_: str | None = row[1]
        ih: str | None = row[2]
        fn: str | None = row[3]
        if mi_id is not None:
            rd_by_item[mi_id] = (rd_id_, ih, fn)
        if ih and ih not in rd_by_hash:
            rd_by_hash[ih] = (rd_id_, fn)

    # Build a normalised-filename -> (rd_id, info_hash, filename) map for
    # items whose symlink source dir matches an RD torrent by name.
    rd_by_filename_lower: dict[str, tuple[str | None, str | None, str | None]] = {}
    for mi_id, (rd_id_, ih, fn) in rd_by_item.items():
        if fn:
            rd_by_filename_lower[fn.lower()] = (rd_id_, ih, fn)

    # ------------------------------------------------------------------
    # Step 3: Fetch all items in duplicate groups (any source)
    # LEFT JOIN to symlinks + direct rd_torrents FK
    # ------------------------------------------------------------------
    # Build IN clause for group keys.  SQLite doesn't support tuple IN,
    # so we OR them together.
    where_clauses: list[str] = []
    bind_params: dict[str, Any] = {}
    for idx, (imdb_id, season, episode) in enumerate(group_keys):
        imdb_param = f"imdb_{idx}"
        season_param = f"season_{idx}"
        ep_param = f"ep_{idx}"
        bind_params[imdb_param] = imdb_id
        bind_params[season_param] = season
        bind_params[ep_param] = episode
        where_clauses.append(
            f"(mi.imdb_id = :{imdb_param} "
            f"AND mi.season IS :{season_param} "
            f"AND mi.episode IS :{ep_param})"
        )

    where_sql = " OR ".join(where_clauses)

    item_rows = await session.execute(
        text(
            "SELECT mi.id, mi.title, mi.season, mi.episode, mi.imdb_id, "
            "       mi.source, mi.state, "
            "       sl.source_path, sl.target_path "
            "FROM media_items mi "
            "LEFT JOIN symlinks sl ON sl.media_item_id = mi.id "
            f"WHERE ({where_sql}) "
            "ORDER BY mi.id ASC"
        ).bindparams(**bind_params)
    )
    raw_items = item_rows.fetchall()

    if not raw_items:
        return []

    # Deduplicate rows — LEFT JOIN can produce multiple rows when an item has
    # multiple symlinks.  Keep the first symlink per item.
    seen_items: dict[int, list] = {}
    for row in raw_items:
        item_id = row[0]
        if item_id not in seen_items:
            seen_items[item_id] = list(row)

    # ------------------------------------------------------------------
    # Step 4: Batch path checks
    # ------------------------------------------------------------------
    source_paths: list[str] = []
    link_paths: list[str] = []
    link_expected: list[str] = []

    for item_id, row in seen_items.items():
        sp: str | None = row[7]
        lp: str | None = row[8]
        if sp:
            source_paths.append(sp)
        if lp and sp:
            link_paths.append(lp)
            link_expected.append(sp)

    source_exists_map = await _batch_check_paths(source_paths) if source_paths else {}
    link_valid_map = (
        await _batch_check_symlinks(link_paths, link_expected) if link_paths else {}
    )

    # ------------------------------------------------------------------
    # Step 5: Classify items
    # ------------------------------------------------------------------
    from src.config import settings

    _norm = re.compile(r"[._\s]+")

    assessed: list[AssessedItem] = []

    for item_id, row in seen_items.items():
        title: str = row[1] or ""
        season: int | None = row[2]
        episode: int | None = row[3]
        imdb_id: str | None = row[4]
        source: str | None = row[5]
        state: str = str(row[6]) if row[6] else ""
        source_path: str | None = row[7]
        link_path: str | None = row[8]

        source_exists = source_exists_map.get(source_path, False) if source_path else False
        link_valid = link_valid_map.get(link_path, False) if link_path else False

        # Direct rd_torrents FK
        rd_tuple = rd_by_item.get(item_id)
        has_rd = rd_tuple is not None
        rd_id: str | None = rd_tuple[0] if rd_tuple else None
        info_hash: str | None = rd_tuple[1] if rd_tuple else None
        rd_filename: str | None = rd_tuple[2] if rd_tuple else None

        # Indirect detection: source_path directory matches an RD torrent filename
        if not has_rd and source_path:
            zurg_mount = settings.paths.zurg_mount.rstrip("/")
            try:
                rel = Path(source_path).relative_to(zurg_mount)
                if rel.parts:
                    dir_name = rel.parts[0]
                    rd_match = rd_by_filename_lower.get(dir_name.lower())
                    if rd_match is None:
                        # Try normalised match
                        norm_key = _norm.sub(" ", dir_name).lower().strip()
                        for fn_lower, val in rd_by_filename_lower.items():
                            if _norm.sub(" ", fn_lower).lower().strip() == norm_key:
                                rd_match = val
                                break
                    if rd_match:
                        has_rd = True
                        rd_id = rd_match[0]
                        info_hash = rd_match[1]
                        rd_filename = rd_match[2]
            except ValueError:
                pass

        liveness = _classify_liveness(source_exists, link_valid, has_rd)

        assessed.append(
            AssessedItem(
                item_id=item_id,
                title=title,
                season=season,
                episode=episode,
                imdb_id=imdb_id,
                source=source,
                state=state,
                liveness=liveness,
                source_path=source_path,
                link_path=link_path,
                source_exists=source_exists,
                link_valid=link_valid,
                has_rd_torrent=has_rd,
                rd_id=rd_id,
                info_hash=info_hash,
                rd_filename=rd_filename,
            )
        )

    logger.info(
        "assess_migration_items: assessed %d item(s) — live=%d bridged=%d dead=%d",
        len(assessed),
        sum(1 for a in assessed if a.liveness == ItemLiveness.LIVE),
        sum(1 for a in assessed if a.liveness == ItemLiveness.BRIDGED),
        sum(1 for a in assessed if a.liveness == ItemLiveness.DEAD),
    )
    return assessed


async def build_cleanup_preview(
    session: AsyncSession,
    assessed: list[AssessedItem],
) -> CleanupPreview:
    """Group assessed items and select keepers to produce a cleanup plan.

    Grouping key: (imdb_id, season, episode).  Items with NULL imdb_id are
    skipped with a warning.

    RD protection: an info_hash is only queued for deletion when NO kept item
    references it — shared season-pack hashes are always preserved.

    Args:
        session: Async database session (used for RD protection query).
        assessed: Items from ``assess_migration_items``.

    Returns:
        A CleanupPreview describing what will happen on execute.
    """
    preview = CleanupPreview()

    if not assessed:
        return preview

    # Tally liveness
    for item in assessed:
        preview.total_items_assessed += 1
        if item.liveness == ItemLiveness.LIVE:
            preview.live_count += 1
        elif item.liveness == ItemLiveness.BRIDGED:
            preview.bridged_count += 1
        else:
            preview.dead_count += 1

    # Group by (imdb_id, season, episode)
    GroupKey = tuple[str | None, int | None, int | None]
    groups_map: dict[GroupKey, list[AssessedItem]] = {}
    for item in assessed:
        if item.imdb_id is None:
            preview.warnings.append(
                f"Item {item.item_id} ({item.title!r}) has NULL imdb_id — skipped"
            )
            continue
        key: GroupKey = (item.imdb_id, item.season, item.episode)
        groups_map.setdefault(key, []).append(item)

    # Build SmartDuplicateGroup for each key with 2+ items
    all_remove_ids: set[int] = set()

    for (imdb_id, season, episode), items in groups_map.items():
        if len(items) < 2:
            # Only one item for this key — not a real duplicate (shouldn't happen
            # given we used HAVING COUNT(*) > 1 on migration items, but another
            # source item could now be the only member here).
            continue

        keeper, removals, reason = _select_keeper(items)
        all_remove_ids.update(r.item_id for r in removals)

        preview.groups.append(
            SmartDuplicateGroup(
                imdb_id=imdb_id,
                title=keeper.title,
                season=season,
                episode=episode,
                keep=keeper,
                remove=removals,
                reason=reason,
                count=len(items),
            )
        )
        preview.total_to_remove += len(removals)
        preview.symlinks_to_delete += sum(1 for r in removals if r.link_path)

    if not all_remove_ids:
        return preview

    # ------------------------------------------------------------------
    # RD protection: collect info_hashes from removed items, subtract any
    # hash referenced by items NOT in the remove set.
    # ------------------------------------------------------------------
    remove_id_list = list(all_remove_ids)
    id_placeholders = ", ".join(f":id{i}" for i in range(len(remove_id_list)))
    bind = {f"id{i}": v for i, v in enumerate(remove_id_list)}

    remove_hash_rows = await session.execute(
        text(
            f"SELECT rd_id, info_hash FROM rd_torrents "
            f"WHERE media_item_id IN ({id_placeholders}) "
            f"AND rd_id IS NOT NULL AND info_hash IS NOT NULL"
        ).bindparams(**bind)
    )
    remove_rd_map: dict[str, str] = {}  # rd_id -> info_hash
    for rd_id_, ih in remove_hash_rows:
        if rd_id_ and ih:
            remove_rd_map[rd_id_] = ih

    if remove_rd_map:
        keep_hash_rows = await session.execute(
            text(
                f"SELECT info_hash FROM rd_torrents "
                f"WHERE media_item_id NOT IN ({id_placeholders}) "
                f"AND info_hash IS NOT NULL"
            ).bindparams(**bind)
        )
        keep_hashes: set[str] = {row[0] for row in keep_hash_rows if row[0]}
    else:
        keep_hashes = set()

    for rd_id_, ih in remove_rd_map.items():
        if ih in keep_hashes:
            preview.rd_ids_protected.append(rd_id_)
        else:
            preview.rd_ids_to_delete.append(rd_id_)

    logger.info(
        "build_cleanup_preview: %d group(s), %d to remove, "
        "%d RD to delete, %d RD protected",
        len(preview.groups),
        preview.total_to_remove,
        len(preview.rd_ids_to_delete),
        len(preview.rd_ids_protected),
    )
    return preview


async def execute_cleanup(
    session: AsyncSession,
    preview: CleanupPreview,
) -> CleanupResult:
    """Execute the database cleanup described by *preview*.

    Deletes DB records in FK-safe order (scrape_log → rd_torrents → symlinks →
    media_items) then removes broken symlinks from disk and cleans empty parent
    directories.

    The caller is responsible for committing the transaction and for calling
    the RD API to delete ``preview.rd_ids_to_delete`` after the commit.

    Args:
        session: Async database session.
        preview: The plan produced by ``build_cleanup_preview``.

    Returns:
        A CleanupResult with counts for all outcomes.
    """
    result = CleanupResult()

    # Flatten remove IDs and collect link_paths for disk cleanup
    remove_ids: list[int] = []
    link_paths_to_delete: list[str] = []

    for group in preview.groups:
        result.items_kept += 1
        for item in group.remove:
            remove_ids.append(item.item_id)
            if item.link_path:
                link_paths_to_delete.append(item.link_path)

    if not remove_ids:
        return result

    # Build parameterised IN clause
    id_placeholders = ", ".join(f":id{i}" for i in range(len(remove_ids)))
    bind = {f"id{i}": v for i, v in enumerate(remove_ids)}

    # All four DELETEs run inside a savepoint so a partial failure rolls back
    # all of them atomically, leaving DB state consistent.
    try:
        async with session.begin_nested():
            # 1. Delete scrape_log rows
            await session.execute(
                text(
                    f"DELETE FROM scrape_log WHERE media_item_id IN ({id_placeholders})"
                ).bindparams(**bind)
            )

            # 2. Delete rd_torrents rows
            await session.execute(
                text(
                    f"DELETE FROM rd_torrents WHERE media_item_id IN ({id_placeholders})"
                ).bindparams(**bind)
            )

            # 3. Delete symlinks rows
            await session.execute(
                text(
                    f"DELETE FROM symlinks WHERE media_item_id IN ({id_placeholders})"
                ).bindparams(**bind)
            )

            # 4. Delete media_items
            del_result = await session.execute(
                text(
                    f"DELETE FROM media_items WHERE id IN ({id_placeholders})"
                ).bindparams(**bind)
            )
            result.items_removed = del_result.rowcount or 0
    except Exception as exc:
        logger.error(
            "execute_cleanup: DB delete failed (savepoint rolled back): %s", exc
        )
        result.errors.append(f"DB delete failed: {exc}")
        return result

    result.groups_processed = len(preview.groups)

    logger.info(
        "execute_cleanup: DB done — groups=%d removed=%d kept=%d",
        result.groups_processed,
        result.items_removed,
        result.items_kept,
    )

    # ------------------------------------------------------------------
    # 5. Remove broken symlinks from disk and clean empty parent dirs
    # ------------------------------------------------------------------
    deleted_links = 0
    for link_path in link_paths_to_delete:
        try:
            is_link = await asyncio.to_thread(os.path.islink, link_path)
            if is_link:
                await asyncio.to_thread(os.unlink, link_path)
                deleted_links += 1
                logger.debug("execute_cleanup: removed symlink %r", link_path)

                # Clean empty parent and grandparent dirs
                parent = os.path.dirname(link_path)
                grandparent = os.path.dirname(parent)
                await _try_remove_empty_dir(parent)
                await _try_remove_empty_dir(grandparent)
        except OSError as exc:
            logger.warning(
                "execute_cleanup: could not remove symlink %r: %s", link_path, exc
            )
            result.errors.append(f"symlink unlink failed {link_path!r}: {exc}")

    result.symlinks_deleted_from_disk = deleted_links

    logger.info(
        "execute_cleanup: disk cleanup done — symlinks_deleted=%d errors=%d",
        deleted_links,
        len(result.errors),
    )
    return result


async def _try_remove_empty_dir(dir_path: str) -> None:
    """Attempt to remove an empty directory.

    Mirrors ``SymlinkManager._try_remove_empty_dir``.  Never removes
    library root directories (movies/shows roots from settings).

    Args:
        dir_path: Absolute path to attempt removal.
    """
    from src.config import settings

    library_roots = (
        settings.paths.library_movies,
        settings.paths.library_shows,
    )
    norm_path = os.path.normpath(dir_path)
    for root in library_roots:
        if norm_path == os.path.normpath(root):
            return
    try:
        await asyncio.to_thread(os.rmdir, norm_path)
        logger.info("execute_cleanup: removed empty dir %r", norm_path)
    except OSError:
        # Non-empty, already removed, or permission denied — all acceptable.
        pass
