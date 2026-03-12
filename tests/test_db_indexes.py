"""Tests for GitHub issue #10: database indexes and BigInteger filesize.

Covers:
  - Metadata index declarations: rd_torrents.media_item_id, scrape_log.media_item_id,
    symlinks.media_item_id, mount_index.parsed_title, media_items.tmdb_id
  - SQLite index creation: PRAGMA index_list / PRAGMA index_info confirm indexes
    are physically present after create_all
  - BigInteger filesize: RdTorrent.filesize and MountIndex.filesize can store and
    retrieve values larger than 2^31 (e.g. 50 GB = 53_687_091_200 bytes)
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# Register all models on Base before any metadata introspection
import src.models.mount_index  # noqa: F401
import src.models.scrape_result  # noqa: F401
import src.models.symlink  # noqa: F401
import src.models.torrent  # noqa: F401
from src.database import Base
from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.mount_index import MountIndex
from src.models.scrape_result import ScrapeLog
from src.models.symlink import Symlink
from src.models.torrent import RdTorrent, TorrentStatus

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_50_GB = 53_687_091_200  # 50 * 1024**3 — well above 2^31 - 1 (2_147_483_647)
_INT32_MAX = 2_147_483_647


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _indexed_columns_for_table(table_name: str) -> set[str]:
    """Return the set of column names that have an index declared in SQLAlchemy metadata.

    This walks the Index objects attached to the table's metadata so it is
    independent of naming conventions or how the index was created.
    """
    table = Base.metadata.tables[table_name]
    columns: set[str] = set()
    for idx in table.indexes:
        for col in idx.columns:
            columns.add(col.name)
    return columns


async def _sqlite_indexed_columns(session: AsyncSession, table_name: str) -> set[str]:
    """Return column names that have a SQLite index after create_all.

    Uses PRAGMA index_list + PRAGMA index_info to interrogate the live database.
    Excludes the implicit rowid / primary-key index (sqlite_autoindex_*).
    """
    result = await session.execute(text(f"PRAGMA index_list('{table_name}')"))
    index_rows = result.fetchall()

    covered: set[str] = set()
    for row in index_rows:
        index_name: str = row[1]
        # Skip SQLite's auto-generated unique constraint indexes
        if index_name.startswith("sqlite_autoindex_"):
            continue
        info_result = await session.execute(text(f"PRAGMA index_info('{index_name}')"))
        for info_row in info_result.fetchall():
            covered.add(info_row[2])  # column name is third field
    return covered


async def _make_media_item(session: AsyncSession, imdb_id: str = "tt0000001") -> MediaItem:
    item = MediaItem(
        imdb_id=imdb_id,
        title="Test Item",
        year=2024,
        media_type=MediaType.MOVIE,
        state=QueueState.WANTED,
        retry_count=0,
    )
    session.add(item)
    await session.flush()
    return item


# ===========================================================================
# 1. Metadata index declaration tests
# ===========================================================================


class TestMetadataIndexDeclarations:
    """Verify that index=True is declared in SQLAlchemy column metadata."""

    def test_rd_torrents_media_item_id_has_index(self) -> None:
        """rd_torrents.media_item_id must have an index in SQLAlchemy metadata."""
        assert "media_item_id" in _indexed_columns_for_table("rd_torrents")

    def test_scrape_log_media_item_id_has_index(self) -> None:
        """scrape_log.media_item_id must have an index in SQLAlchemy metadata."""
        assert "media_item_id" in _indexed_columns_for_table("scrape_log")

    def test_symlinks_media_item_id_has_index(self) -> None:
        """symlinks.media_item_id must have an index in SQLAlchemy metadata."""
        assert "media_item_id" in _indexed_columns_for_table("symlinks")

    def test_mount_index_parsed_title_has_index(self) -> None:
        """mount_index.parsed_title must have an index in SQLAlchemy metadata."""
        assert "parsed_title" in _indexed_columns_for_table("mount_index")

    def test_media_items_tmdb_id_has_index(self) -> None:
        """media_items.tmdb_id must have an index in SQLAlchemy metadata."""
        assert "tmdb_id" in _indexed_columns_for_table("media_items")


# ===========================================================================
# 2. SQLite physical index creation tests
# ===========================================================================


class TestSQLiteIndexCreation:
    """Verify that indexes are physically present in the SQLite database after create_all."""

    async def test_rd_torrents_media_item_id_index_in_sqlite(self, session: AsyncSession) -> None:
        """SQLite must have an index covering rd_torrents.media_item_id."""
        covered = await _sqlite_indexed_columns(session, "rd_torrents")
        assert "media_item_id" in covered, (
            f"No SQLite index on rd_torrents.media_item_id; indexes cover: {covered}"
        )

    async def test_scrape_log_media_item_id_index_in_sqlite(self, session: AsyncSession) -> None:
        """SQLite must have an index covering scrape_log.media_item_id."""
        covered = await _sqlite_indexed_columns(session, "scrape_log")
        assert "media_item_id" in covered, (
            f"No SQLite index on scrape_log.media_item_id; indexes cover: {covered}"
        )

    async def test_symlinks_media_item_id_index_in_sqlite(self, session: AsyncSession) -> None:
        """SQLite must have an index covering symlinks.media_item_id."""
        covered = await _sqlite_indexed_columns(session, "symlinks")
        assert "media_item_id" in covered, (
            f"No SQLite index on symlinks.media_item_id; indexes cover: {covered}"
        )

    async def test_mount_index_parsed_title_index_in_sqlite(self, session: AsyncSession) -> None:
        """SQLite must have an index covering mount_index.parsed_title."""
        covered = await _sqlite_indexed_columns(session, "mount_index")
        assert "parsed_title" in covered, (
            f"No SQLite index on mount_index.parsed_title; indexes cover: {covered}"
        )

    async def test_media_items_tmdb_id_index_in_sqlite(self, session: AsyncSession) -> None:
        """SQLite must have an index covering media_items.tmdb_id."""
        covered = await _sqlite_indexed_columns(session, "media_items")
        assert "tmdb_id" in covered, (
            f"No SQLite index on media_items.tmdb_id; indexes cover: {covered}"
        )

    async def test_all_five_indexes_present_together(self, session: AsyncSession) -> None:
        """Smoke test: all five issue-#10 indexes exist at the same time."""
        checks = {
            "rd_torrents": "media_item_id",
            "scrape_log": "media_item_id",
            "symlinks": "media_item_id",
            "mount_index": "parsed_title",
            "media_items": "tmdb_id",
        }
        missing: list[str] = []
        for table, column in checks.items():
            covered = await _sqlite_indexed_columns(session, table)
            if column not in covered:
                missing.append(f"{table}.{column}")
        assert not missing, f"Missing SQLite indexes: {missing}"


# ===========================================================================
# 3. BigInteger filesize tests
# ===========================================================================


class TestBigIntegerFilesize:
    """Verify that filesize columns handle values larger than 2^31."""

    async def test_rd_torrent_filesize_stores_50gb(self, session: AsyncSession) -> None:
        """RdTorrent.filesize must round-trip a 50 GB value (> INT32 max)."""
        item = await _make_media_item(session, "tt1000001")
        torrent = RdTorrent(
            rd_id="ABC123",
            info_hash="aabbccddeeff00112233445566778899aabbccdd",
            media_item_id=item.id,
            filesize=_50_GB,
            status=TorrentStatus.ACTIVE,
        )
        session.add(torrent)
        await session.flush()
        await session.refresh(torrent)

        assert torrent.filesize == _50_GB
        assert torrent.filesize > _INT32_MAX

    async def test_rd_torrent_filesize_stores_exactly_int32_max_plus_one(
        self, session: AsyncSession
    ) -> None:
        """RdTorrent.filesize must store INT32_MAX + 1 without overflow."""
        item = await _make_media_item(session, "tt1000002")
        boundary = _INT32_MAX + 1
        torrent = RdTorrent(
            rd_id="DEF456",
            info_hash="00112233445566778899aabbccddeeff00112233",
            media_item_id=item.id,
            filesize=boundary,
            status=TorrentStatus.ACTIVE,
        )
        session.add(torrent)
        await session.flush()
        await session.refresh(torrent)

        assert torrent.filesize == boundary

    async def test_rd_torrent_filesize_accepts_none(self, session: AsyncSession) -> None:
        """RdTorrent.filesize is nullable; None must round-trip correctly."""
        item = await _make_media_item(session, "tt1000003")
        torrent = RdTorrent(
            rd_id="GHI789",
            info_hash="ffeeddccbbaa99887766554433221100ffeeddcc",
            media_item_id=item.id,
            filesize=None,
            status=TorrentStatus.ACTIVE,
        )
        session.add(torrent)
        await session.flush()
        await session.refresh(torrent)

        assert torrent.filesize is None

    async def test_mount_index_filesize_stores_50gb(self, session: AsyncSession) -> None:
        """MountIndex.filesize must round-trip a 50 GB value (> INT32 max)."""
        entry = MountIndex(
            filepath="/mnt/media/BigMovie.mkv",
            filename="BigMovie.mkv",
            parsed_title="bigmovie",
            filesize=_50_GB,
        )
        session.add(entry)
        await session.flush()
        await session.refresh(entry)

        assert entry.filesize == _50_GB
        assert entry.filesize > _INT32_MAX

    async def test_mount_index_filesize_stores_exactly_int32_max_plus_one(
        self, session: AsyncSession
    ) -> None:
        """MountIndex.filesize must store INT32_MAX + 1 without overflow."""
        boundary = _INT32_MAX + 1
        entry = MountIndex(
            filepath="/mnt/media/BorderlineFile.mkv",
            filename="BorderlineFile.mkv",
            parsed_title="borderlinefile",
            filesize=boundary,
        )
        session.add(entry)
        await session.flush()
        await session.refresh(entry)

        assert entry.filesize == boundary

    async def test_mount_index_filesize_accepts_none(self, session: AsyncSession) -> None:
        """MountIndex.filesize is nullable; None must round-trip correctly."""
        entry = MountIndex(
            filepath="/mnt/media/UnknownSize.mkv",
            filename="UnknownSize.mkv",
            parsed_title="unknownsize",
            filesize=None,
        )
        session.add(entry)
        await session.flush()
        await session.refresh(entry)

        assert entry.filesize is None

    async def test_rd_torrent_filesize_is_biginteger_type(self) -> None:
        """RdTorrent.filesize column type must be BigInteger in SQLAlchemy metadata."""
        from sqlalchemy import BigInteger

        table = Base.metadata.tables["rd_torrents"]
        col = table.c["filesize"]
        assert isinstance(col.type, BigInteger), (
            f"Expected BigInteger but got {type(col.type).__name__}"
        )

    async def test_mount_index_filesize_is_biginteger_type(self) -> None:
        """MountIndex.filesize column type must be BigInteger in SQLAlchemy metadata."""
        from sqlalchemy import BigInteger

        table = Base.metadata.tables["mount_index"]
        col = table.c["filesize"]
        assert isinstance(col.type, BigInteger), (
            f"Expected BigInteger but got {type(col.type).__name__}"
        )
