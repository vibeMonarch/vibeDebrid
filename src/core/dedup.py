"""Deduplication engine for vibeDebrid.

Provides two complementary deduplication strategies:

1. **Pre-Add Dedup** — before adding a torrent to Real-Debrid, check whether
   the same content (by info_hash or by IMDB ID + season + episode +
   resolution) already exists in the local ``rd_torrents`` registry.  If a
   match is found the caller can skip the RD add and proceed directly to
   CHECKING state.

2. **Account Dedup** — given a raw list of RD account torrents, group them by
   parsed title/season/episode to surface duplicates for the user to resolve
   via the duplicate manager UI.  No auto-deletion occurs here.

All public methods that interact with the database accept an
``AsyncSession`` and call ``session.flush()`` but never ``session.commit()``.
Transaction management is the caller's responsibility (CLAUDE.md convention).

Usage::

    from src.core.dedup import dedup_engine

    existing = await dedup_engine.check_local_duplicate(session, info_hash)
    if existing:
        # skip RD add, proceed to CHECKING
        ...

    torrent = await dedup_engine.register_torrent(session, rd_id=..., ...)
    groups  = await dedup_engine.find_account_duplicates(session, rd_data)
"""

from __future__ import annotations

import logging
import re
from typing import Any

import PTN
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.media_item import MediaItem
from src.models.torrent import RdTorrent, TorrentStatus

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pydantic output models
# ---------------------------------------------------------------------------


class DuplicateEntry(BaseModel):
    """A single torrent within a duplicate group.

    Attributes:
        rd_id: Real-Debrid torrent ID string (e.g. ``"XYZABC..."``).
        info_hash: 40-char hex info hash of the torrent.
        filename: Torrent display name as returned by RD.
        filesize: Size in bytes (``0`` when not available).
        resolution: Parsed resolution string (e.g. ``"1080p"``), or None.
        added: ISO date string from the RD ``"added"`` field, or None.
    """

    rd_id: str
    info_hash: str
    filename: str
    filesize: int
    resolution: str | None = None
    added: str | None = None


class DuplicateGroup(BaseModel):
    """A group of RD torrents that are duplicates of the same content.

    Attributes:
        imdb_id: IMDB ID of the matched media item, or the parsed title
            string when no local media item was found.
        title: Human-readable title derived from filename parsing.
        season: Season number, or None for movies / when unknown.
        episode: Episode number, or None for movies / season packs.
        torrents: List of DuplicateEntry objects comprising the group.
    """

    imdb_id: str
    title: str
    season: int | None = None
    episode: int | None = None
    torrents: list[DuplicateEntry]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

# Regex that replaces dots and underscores with a space for title normalisation.
_SEPARATOR_RE = re.compile(r"[._]+")


def _normalize_title(raw: str) -> str:
    """Return a lowercased, separator-stripped version of *raw* for grouping.

    Dots and underscores are replaced with spaces so that
    ``"Breaking.Bad"`` and ``"Breaking_Bad"`` collapse to the same key.

    Args:
        raw: The raw title string as returned by PTN.

    Returns:
        Lowercased, normalised title string.
    """
    normalised = _SEPARATOR_RE.sub(" ", raw)
    return normalised.lower().strip()


def _parse_filename(filename: str) -> dict[str, Any]:
    """Wrap PTN.parse with error handling and return an empty dict on failure.

    Args:
        filename: Raw torrent filename / release name to parse.

    Returns:
        PTN result dict on success, or an empty dict on parse failure.
    """
    try:
        result: dict[str, Any] = PTN.parse(filename) or {}
        return result
    except Exception as exc:  # PTN can raise ValueError or AttributeError
        logger.warning("dedup: PTN failed to parse filename=%r (%s)", filename, exc)
        return {}


# ---------------------------------------------------------------------------
# DedupEngine
# ---------------------------------------------------------------------------


class DedupEngine:
    """Stateless deduplication engine.

    All DB-interacting methods receive an ``AsyncSession`` from the caller.
    No global state is held — the module-level ``dedup_engine`` singleton is
    safe for concurrent coroutine use.
    """

    # ------------------------------------------------------------------
    # Pre-Add checks
    # ------------------------------------------------------------------

    async def check_local_duplicate(
        self,
        session: AsyncSession,
        info_hash: str,
    ) -> RdTorrent | None:
        """Check the local rd_torrents table for an active entry with *info_hash*.

        This is the fast, DB-only check used before every RD add operation.
        Only ACTIVE torrents are considered — REMOVED and REPLACED entries are
        ignored.

        Args:
            session: Caller-managed async database session.
            info_hash: 40-char hex info hash to look up.

        Returns:
            The matching RdTorrent ORM object if found, otherwise None.
        """
        normalised = info_hash.lower().strip()
        stmt = select(RdTorrent).where(
            RdTorrent.info_hash == normalised,
            RdTorrent.status == TorrentStatus.ACTIVE,
        )
        result = await session.execute(stmt)
        torrent = result.scalar_one_or_none()

        if torrent is not None:
            logger.debug(
                "dedup.check_local_duplicate: found info_hash=%s rd_id=%s",
                normalised,
                torrent.rd_id,
            )
        return torrent

    async def check_content_duplicate(
        self,
        session: AsyncSession,
        imdb_id: str,
        season: int | None,
        episode: int | None,
        resolution: str | None,
    ) -> RdTorrent | None:
        """Check for an existing ACTIVE torrent matching the requested content.

        Joins ``rd_torrents`` with ``media_items`` to find an ACTIVE torrent
        that matches the supplied IMDB ID, season, and episode.  When
        *resolution* is provided the match is limited to an identical
        resolution string; when *resolution* is None any resolution is
        accepted.

        Season and episode comparisons are NULL-aware: a None season on the
        request matches only torrents whose linked media_item also has a None
        season (i.e. movies).

        Args:
            session: Caller-managed async database session.
            imdb_id: IMDB ID of the media item to check (e.g. ``"tt1234567"``).
            season: Season number, or None for movies.
            episode: Episode number, or None for movies / season packs.
            resolution: Resolution string to match (e.g. ``"1080p"``), or None
                to accept any resolution.

        Returns:
            The first matching ACTIVE RdTorrent, or None when no match exists.
        """
        stmt = (
            select(RdTorrent)
            .join(MediaItem, RdTorrent.media_item_id == MediaItem.id)
            .where(
                RdTorrent.status == TorrentStatus.ACTIVE,
                MediaItem.imdb_id == imdb_id,
            )
        )

        # Season: match NULL-for-NULL or value-for-value
        if season is None:
            stmt = stmt.where(MediaItem.season.is_(None))
        else:
            stmt = stmt.where(MediaItem.season == season)

        # Episode: match NULL-for-NULL or value-for-value
        if episode is None:
            stmt = stmt.where(MediaItem.episode.is_(None))
        else:
            stmt = stmt.where(MediaItem.episode == episode)

        # Resolution: only filter when caller cares about a specific resolution
        if resolution is not None:
            stmt = stmt.where(RdTorrent.resolution == resolution)

        result = await session.execute(stmt)
        torrent = result.scalars().first()

        if torrent is not None:
            logger.debug(
                "dedup.check_content_duplicate: found content duplicate "
                "imdb_id=%s season=%s episode=%s resolution=%s -> rd_torrent.id=%d",
                imdb_id,
                season,
                episode,
                resolution,
                torrent.id,
            )
        return torrent

    # ------------------------------------------------------------------
    # Registry management
    # ------------------------------------------------------------------

    async def register_torrent(
        self,
        session: AsyncSession,
        *,
        rd_id: str | None,
        info_hash: str | None,
        magnet_uri: str | None,
        media_item_id: int | None,
        filename: str | None,
        filesize: int | None,
        resolution: str | None,
        cached: bool | None,
    ) -> RdTorrent:
        """Create and persist a new RdTorrent registry entry.

        The new record is flushed (but not committed) so the caller can
        include it in a broader transaction.

        Args:
            session: Caller-managed async database session.
            rd_id: Real-Debrid torrent ID, or None if not yet assigned.
            info_hash: 40-char hex info hash, or None if unknown.
            magnet_uri: Full magnet URI, or None.
            media_item_id: FK to the associated MediaItem, or None.
            filename: Torrent display filename, or None.
            filesize: Total size in bytes, or None.
            resolution: Parsed resolution string (e.g. ``"1080p"``), or None.
            cached: Whether the torrent was cached in RD at add time, or None.

        Returns:
            The newly created and flushed RdTorrent ORM object.
        """
        normalised_hash = info_hash.lower().strip() if info_hash else None

        torrent = RdTorrent(
            rd_id=rd_id,
            info_hash=normalised_hash,
            magnet_uri=magnet_uri,
            media_item_id=media_item_id,
            filename=filename,
            filesize=filesize,
            resolution=resolution,
            cached=cached,
            status=TorrentStatus.ACTIVE,
        )
        session.add(torrent)
        await session.flush()

        logger.info(
            "dedup.register_torrent: registered rd_id=%s info_hash=%s "
            "filename=%r resolution=%s media_item_id=%s",
            rd_id,
            normalised_hash,
            filename,
            resolution,
            media_item_id,
        )
        return torrent

    async def mark_torrent_removed(
        self,
        session: AsyncSession,
        info_hash: str,
    ) -> None:
        """Mark the RdTorrent with *info_hash* as REMOVED.

        Used when a torrent is deleted from the RD account so the local
        registry stays consistent.  If no matching active torrent is found,
        the operation is a silent no-op (idempotent).

        Args:
            session: Caller-managed async database session.
            info_hash: 40-char hex info hash of the torrent to mark removed.
        """
        normalised = info_hash.lower().strip()
        stmt = select(RdTorrent).where(
            RdTorrent.info_hash == normalised,
            RdTorrent.status == TorrentStatus.ACTIVE,
        )
        result = await session.execute(stmt)
        torrent = result.scalar_one_or_none()

        if torrent is None:
            logger.warning(
                "dedup.mark_torrent_removed: no active record for info_hash=%s", normalised
            )
            return

        torrent.status = TorrentStatus.REMOVED
        await session.flush()

        logger.info(
            "dedup.mark_torrent_removed: info_hash=%s status ACTIVE -> REMOVED",
            normalised,
        )

    async def mark_torrent_replaced(
        self,
        session: AsyncSession,
        old_info_hash: str,
        new_info_hash: str,
    ) -> None:
        """Mark the torrent with *old_info_hash* as REPLACED.

        Called during quality upgrades — the old torrent is superseded by the
        new one.  If the old hash is not found, a warning is logged and the
        operation continues (idempotent).

        Args:
            session: Caller-managed async database session.
            old_info_hash: Info hash of the torrent being replaced.
            new_info_hash: Info hash of the replacement torrent (for logging).
        """
        normalised_old = old_info_hash.lower().strip()
        normalised_new = new_info_hash.lower().strip()

        stmt = select(RdTorrent).where(
            RdTorrent.info_hash == normalised_old,
            RdTorrent.status == TorrentStatus.ACTIVE,
        )
        result = await session.execute(stmt)
        torrent = result.scalar_one_or_none()

        if torrent is None:
            logger.warning(
                "dedup.mark_torrent_replaced: no active record for old_info_hash=%s",
                normalised_old,
            )
            return

        torrent.status = TorrentStatus.REPLACED
        await session.flush()

        logger.info(
            "dedup.mark_torrent_replaced: old_info_hash=%s status ACTIVE -> REPLACED "
            "(replaced by new_info_hash=%s)",
            normalised_old,
            normalised_new,
        )

    # ------------------------------------------------------------------
    # Account duplicate detection
    # ------------------------------------------------------------------

    async def find_account_duplicates(
        self,
        session: AsyncSession,
        rd_torrents_data: list[dict[str, Any]],
    ) -> list[DuplicateGroup]:
        """Detect duplicate content groups within an RD account torrent list.

        Parses each torrent's filename with PTN, normalises the title, and
        groups entries by ``(normalised_title, season, episode)``.  Any group
        with two or more entries is considered a duplicate group.

        For each torrent the method attempts to look up the associated
        MediaItem via the local ``rd_torrents`` registry to obtain the real
        IMDB ID.  When no local record is found, the normalised parsed title
        is used as the ``imdb_id`` placeholder so the UI can still display the
        group.

        Args:
            session: Caller-managed async database session.
            rd_torrents_data: List of RD API torrent dicts.  Each dict must
                contain at minimum ``"id"``, ``"filename"``, ``"hash"``,
                ``"bytes"``.  The ``"added"`` and ``"status"`` keys are
                optional.

        Returns:
            Sorted (by title) list of DuplicateGroup objects, one per group
            that contains two or more distinct torrents.
        """
        # ------------------------------------------------------------------
        # Step 1: Pre-fetch all local rd_torrent records keyed by lowercase
        #         info_hash so we can resolve IMDB IDs without per-row queries.
        # ------------------------------------------------------------------
        local_stmt = (
            select(RdTorrent, MediaItem)
            .outerjoin(MediaItem, RdTorrent.media_item_id == MediaItem.id)
            .where(RdTorrent.status == TorrentStatus.ACTIVE)
        )
        local_result = await session.execute(local_stmt)
        local_rows = local_result.all()

        # Map: lowercase_hash -> (RdTorrent, MediaItem | None)
        local_map: dict[str, tuple[RdTorrent, MediaItem | None]] = {}
        for row_torrent, row_media in local_rows:
            if row_torrent.info_hash:
                local_map[row_torrent.info_hash.lower()] = (row_torrent, row_media)

        # ------------------------------------------------------------------
        # Step 2: Parse each RD torrent and group by normalised key.
        # ------------------------------------------------------------------
        # Groups mapping: grouping_key -> list of (ptn_data, rd_dict)
        groups: dict[
            tuple[str, int | None, int | None],
            list[tuple[dict[str, Any], dict[str, Any]]],
        ] = {}

        for rd_dict in rd_torrents_data:
            filename: str = rd_dict.get("filename") or ""
            if not filename:
                logger.debug(
                    "dedup.find_account_duplicates: skipping entry with empty "
                    "filename (rd_id=%s)",
                    rd_dict.get("id"),
                )
                continue

            ptn_data = _parse_filename(filename)
            if not ptn_data:
                # PTN returned an empty dict — skip this entry
                logger.debug(
                    "dedup.find_account_duplicates: PTN returned empty result "
                    "for filename=%r, skipping",
                    filename,
                )
                continue

            raw_title: str = ptn_data.get("title") or filename
            norm_title = _normalize_title(raw_title)
            season: int | None = ptn_data.get("season")
            episode: int | None = ptn_data.get("episode")

            key: tuple[str, int | None, int | None] = (norm_title, season, episode)
            groups.setdefault(key, []).append((ptn_data, rd_dict))

        # ------------------------------------------------------------------
        # Step 3: Build DuplicateGroup objects for groups with 2+ entries.
        # ------------------------------------------------------------------
        duplicate_groups: list[DuplicateGroup] = []

        for (norm_title, season, episode), members in groups.items():
            if len(members) < 2:
                continue

            entries: list[DuplicateEntry] = []
            imdb_id_for_group: str = norm_title  # default placeholder

            for ptn_data, rd_dict in members:
                rd_id: str = str(rd_dict.get("id") or "")
                raw_hash: str = str(rd_dict.get("hash") or "")
                info_hash: str = raw_hash.lower()
                filename = rd_dict.get("filename") or ""
                filesize: int = int(rd_dict.get("bytes") or 0)
                added: str | None = rd_dict.get("added")
                resolution: str | None = ptn_data.get("resolution")

                # Attempt to resolve IMDB ID from local registry
                local_entry = local_map.get(info_hash)
                if local_entry is not None:
                    _, media_item = local_entry
                    if media_item is not None and media_item.imdb_id:
                        imdb_id_for_group = media_item.imdb_id

                entries.append(
                    DuplicateEntry(
                        rd_id=rd_id,
                        info_hash=info_hash,
                        filename=filename,
                        filesize=filesize,
                        resolution=resolution,
                        added=added,
                    )
                )

            # Use the human-readable (un-normalised) title from the first entry
            first_ptn = members[0][0]
            display_title: str = str(first_ptn.get("title") or norm_title)

            duplicate_groups.append(
                DuplicateGroup(
                    imdb_id=imdb_id_for_group,
                    title=display_title,
                    season=season,
                    episode=episode,
                    torrents=entries,
                )
            )

        duplicate_groups.sort(key=lambda g: g.title.lower())

        logger.info(
            "dedup.find_account_duplicates: scanned %d RD torrents, "
            "found %d duplicate groups",
            len(rd_torrents_data),
            len(duplicate_groups),
        )
        return duplicate_groups


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

dedup_engine = DedupEngine()
