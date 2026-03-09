"""Backfill utilities for migrated media items.

Provides two operations:
- ``backfill_tmdb_ids``: resolve TMDB IDs for items that have an IMDB ID but no
  TMDB ID (typically items imported by the Library Migration tool).
- ``find_duplicates`` / ``remove_duplicates``: detect and delete duplicate queue
  entries that share the same (imdb_id, season, episode) key, keeping only the
  earliest-inserted row.

Design notes:
- ``backfill_tmdb_ids`` batches by unique imdb_id so N items sharing one IMDB ID
  trigger only one TMDB API call, then updates all of them in one SQL statement.
- asyncio.Semaphore(10) limits concurrency to stay within TMDB rate limits.
- Caller owns the transaction — this module flushes but does NOT commit.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.services.tmdb import tmdb_client

logger = logging.getLogger(__name__)

# Prevents concurrent backfill runs (background task vs. API endpoint).
# Callers should check `_backfill_lock.locked()` before acquiring to return
# a 409 quickly rather than queuing behind a long-running operation.
_backfill_lock = asyncio.Lock()


# ---------------------------------------------------------------------------
# Result schemas
# ---------------------------------------------------------------------------


class BackfillResult(BaseModel):
    """Result of a tmdb_id backfill run."""

    total: int = 0
    """Total number of distinct IMDB IDs that needed resolution."""
    resolved: int = 0
    """Number of IMDB IDs successfully resolved to a TMDB ID."""
    failed: int = 0
    """Number of IMDB IDs for which TMDB returned no result."""
    updated_rows: int = 0
    """Total database rows updated (multiple items may share one IMDB ID)."""


class DuplicateGroup(BaseModel):
    """A group of duplicate MediaItems sharing the same (imdb_id, season, episode)."""

    imdb_id: str | None
    title: str
    season: int | None
    episode: int | None
    keep_id: int
    """The ID of the item to retain (lowest id in the group)."""
    remove_ids: list[int]
    """IDs of items to delete."""
    count: int
    """Total number of items in the group (kept + removed)."""


class DeduplicateResult(BaseModel):
    """Result of a deduplication run."""

    groups: int = 0
    """Number of duplicate groups processed."""
    removed: int = 0
    """Total rows deleted."""
    kept: int = 0
    """Total rows retained (one per group)."""
    rd_ids_to_delete: list[str] = []
    """RD torrent IDs that should be deleted from the RD account.

    Populated by ``remove_duplicates`` — contains only rd_ids belonging to the
    removed items that are NOT also referenced by the kept items.  The route
    handler performs the actual RD API deletions after the DB commit.
    """
    rd_torrents_deleted: int = 0
    """Number of RD torrents successfully deleted from the account (set by route)."""
    rd_torrents_failed: int = 0
    """Number of RD torrent deletions that failed (set by route)."""


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------


async def backfill_tmdb_ids(session: AsyncSession) -> BackfillResult:
    """Resolve tmdb_id for all items where tmdb_id IS NULL AND imdb_id IS NOT NULL.

    Batches by unique imdb_id to minimise API calls.  Updates all rows sharing
    the same imdb_id in one UPDATE statement.  Concurrent TMDB calls are limited
    to 10 at a time via asyncio.Semaphore.

    Acquires ``_backfill_lock`` for the duration of the run so that only one
    backfill operation can proceed at a time (whether triggered by startup or
    by the API endpoint).

    Args:
        session: Async database session.  The caller is responsible for
                 committing or rolling back.

    Returns:
        A BackfillResult with counts of resolved/failed/updated.
    """
    async with _backfill_lock:
        return await _backfill_tmdb_ids_inner(session)


async def _backfill_tmdb_ids_inner(session: AsyncSession) -> BackfillResult:
    """Inner implementation of backfill_tmdb_ids (called under _backfill_lock)."""
    result = BackfillResult()

    # Collect all distinct imdb_ids that need a tmdb_id
    rows = await session.execute(
        text(
            "SELECT DISTINCT imdb_id FROM media_items "
            "WHERE tmdb_id IS NULL AND imdb_id IS NOT NULL"
        )
    )
    imdb_ids: list[str] = [row[0] for row in rows if row[0]]
    result.total = len(imdb_ids)

    if not imdb_ids:
        logger.debug("backfill_tmdb_ids: nothing to backfill")
        return result

    logger.info("backfill_tmdb_ids: %d unique IMDB IDs to resolve", result.total)

    semaphore = asyncio.Semaphore(10)

    async def _resolve_one(imdb_id: str) -> dict[str, Any] | None:
        async with semaphore:
            return await tmdb_client.find_by_imdb_id(imdb_id)

    # Fan out all lookups concurrently (bounded by semaphore).
    # return_exceptions=True prevents a single failure from cancelling the rest.
    lookup_results: list[dict[str, Any] | None | BaseException] = await asyncio.gather(
        *[_resolve_one(iid) for iid in imdb_ids],
        return_exceptions=True,
    )

    for imdb_id, found in zip(imdb_ids, lookup_results):
        if isinstance(found, BaseException):
            result.failed += 1
            logger.warning(
                "backfill_tmdb_ids: task failed for imdb_id=%s: %s",
                imdb_id,
                found,
            )
            continue
        if found is None:
            result.failed += 1
            logger.debug(
                "backfill_tmdb_ids: no TMDB result for imdb_id=%s", imdb_id
            )
            continue

        tmdb_id_int: int = found["tmdb_id"]
        tvdb_id: int | None = found.get("tvdb_id")

        # Update all rows that share this imdb_id and still lack a tmdb_id
        update_result = await session.execute(
            text(
                "UPDATE media_items "
                "SET tmdb_id = :tmdb_id, tvdb_id = COALESCE(tvdb_id, :tvdb_id) "
                "WHERE imdb_id = :imdb_id AND tmdb_id IS NULL"
            ).bindparams(
                tmdb_id=str(tmdb_id_int),
                tvdb_id=tvdb_id,
                imdb_id=imdb_id,
            )
        )
        rows_updated: int = update_result.rowcount or 0
        result.updated_rows += rows_updated
        result.resolved += 1

        if result.resolved % 10 == 0:
            logger.info(
                "backfill_tmdb_ids: progress %d/%d resolved (%d rows updated so far)",
                result.resolved,
                result.total,
                result.updated_rows,
            )

        logger.debug(
            "backfill_tmdb_ids: imdb_id=%s → tmdb_id=%d tvdb_id=%s rows_updated=%d",
            imdb_id,
            tmdb_id_int,
            tvdb_id,
            rows_updated,
        )

    logger.info(
        "backfill_tmdb_ids: complete — total=%d resolved=%d failed=%d rows_updated=%d",
        result.total,
        result.resolved,
        result.failed,
        result.updated_rows,
    )
    return result


async def find_duplicates(session: AsyncSession) -> list[DuplicateGroup]:
    """Find duplicate migration items grouped by (imdb_id, season, episode).

    Only considers items with ``source='migration'``.  For each group with more
    than one item the item with the lowest ``id`` is kept; the rest are flagged
    for removal.

    Args:
        session: Async database session.

    Returns:
        A list of DuplicateGroup objects, one per group with count > 1.
        Returns an empty list when no duplicates exist.
    """
    # Find groups with more than one item.
    # Exclude rows where imdb_id IS NULL: SQLite treats all NULLs as equal in
    # GROUP BY, so NULL-imdb_id items would be falsely grouped together.
    group_rows = await session.execute(
        text(
            "SELECT imdb_id, season, episode, COUNT(*) AS cnt "
            "FROM media_items "
            "WHERE source = 'migration' "
            "AND imdb_id IS NOT NULL "
            "GROUP BY imdb_id, season, episode "
            "HAVING COUNT(*) > 1"
        )
    )
    groups_raw = group_rows.fetchall()

    if not groups_raw:
        return []

    duplicate_groups: list[DuplicateGroup] = []

    for imdb_id, season, episode, count in groups_raw:
        # Fetch all item ids and titles in this group, ordered by id ascending
        member_rows = await session.execute(
            text(
                "SELECT id, title FROM media_items "
                "WHERE source = 'migration' "
                "AND imdb_id IS :imdb_id "
                "AND season IS :season "
                "AND episode IS :episode "
                "ORDER BY id ASC"
            ).bindparams(imdb_id=imdb_id, season=season, episode=episode)
        )
        members = member_rows.fetchall()
        if not members:
            continue

        keep_id: int = members[0][0]
        keep_title: str = members[0][1] or ""
        remove_ids: list[int] = [row[0] for row in members[1:]]

        duplicate_groups.append(
            DuplicateGroup(
                imdb_id=imdb_id,
                title=keep_title,
                season=season,
                episode=episode,
                keep_id=keep_id,
                remove_ids=remove_ids,
                count=int(count),
            )
        )

    logger.info(
        "find_duplicates: found %d duplicate group(s) among migration items",
        len(duplicate_groups),
    )
    return duplicate_groups


async def remove_duplicates(
    session: AsyncSession,
    groups: list[DuplicateGroup],
) -> DeduplicateResult:
    """Delete duplicate MediaItems identified by the given duplicate groups.

    Deletes all child records that reference ``media_items.id`` before
    removing the parent rows, to satisfy ``PRAGMA foreign_keys = ON``.
    Delete order: scrape_log → rd_torrents → symlinks → media_items.

    The caller owns the transaction.

    Args:
        session: Async database session.
        groups: Duplicate groups as returned by ``find_duplicates()``.  Each
                group carries a ``remove_ids`` list; the ``keep_id`` row is
                left untouched.

    Returns:
        A DeduplicateResult with counts of removed/kept/groups.
    """
    result = DeduplicateResult()

    # Flatten remove_ids across all groups.
    remove_ids: list[int] = []
    for g in groups:
        remove_ids.extend(g.remove_ids)

    if not remove_ids:
        return result

    # Build IN clause — expand each id as a separate named parameter so SQLite
    # can bind them without a Python list (not supported by text() bindparams).
    id_placeholders = ", ".join(f":id{i}" for i in range(len(remove_ids)))
    bind_params = {f"id{i}": v for i, v in enumerate(remove_ids)}

    # ------------------------------------------------------------------
    # Collect RD IDs to delete BEFORE the DELETE statements.
    # We only delete an RD torrent from the account if it is NOT referenced
    # by any of the kept items — shared season-pack torrents must be preserved.
    # ------------------------------------------------------------------
    remove_rd_rows = await session.execute(
        text(
            f"SELECT rd_id FROM rd_torrents "
            f"WHERE media_item_id IN ({id_placeholders}) AND rd_id IS NOT NULL"
        ).bindparams(**bind_params)
    )
    remove_rd_ids: set[str] = {row[0] for row in remove_rd_rows if row[0]}

    if remove_rd_ids:
        # Protect any rd_id referenced by ANY item outside the remove set —
        # not just the kept duplicates. This guards queue-pipeline items that
        # happen to share a season-pack torrent with a migration item.
        keep_rd_rows = await session.execute(
            text(
                f"SELECT rd_id FROM rd_torrents "
                f"WHERE media_item_id NOT IN ({id_placeholders}) AND rd_id IS NOT NULL"
            ).bindparams(**bind_params)
        )
        keep_rd_ids: set[str] = {row[0] for row in keep_rd_rows if row[0]}
    else:
        keep_rd_ids = set()

    # Only delete RD torrents that are not also linked to a kept item.
    result.rd_ids_to_delete = list(remove_rd_ids - keep_rd_ids)

    if result.rd_ids_to_delete:
        logger.info(
            "remove_duplicates: %d RD torrent(s) queued for account deletion "
            "(%d preserved as shared with kept items)",
            len(result.rd_ids_to_delete),
            len(remove_rd_ids) - len(result.rd_ids_to_delete),
        )

    # 1. Delete scrape_log rows (FK → media_items.id)
    await session.execute(
        text(
            f"DELETE FROM scrape_log WHERE media_item_id IN ({id_placeholders})"
        ).bindparams(**bind_params)
    )

    # 2. Delete rd_torrents rows (FK → media_items.id)
    await session.execute(
        text(
            f"DELETE FROM rd_torrents WHERE media_item_id IN ({id_placeholders})"
        ).bindparams(**bind_params)
    )

    # 3. Delete symlinks rows (FK → media_items.id)
    await session.execute(
        text(
            f"DELETE FROM symlinks WHERE media_item_id IN ({id_placeholders})"
        ).bindparams(**bind_params)
    )

    # 4. Delete the media items themselves
    del_result = await session.execute(
        text(
            f"DELETE FROM media_items WHERE id IN ({id_placeholders})"
        ).bindparams(**bind_params)
    )
    removed = del_result.rowcount or 0
    result.removed = removed
    result.groups = len(groups)
    result.kept = len(groups)  # one kept item per group

    logger.info(
        "remove_duplicates: deleted %d media item(s) across %d group(s)",
        removed,
        result.groups,
    )
    return result
