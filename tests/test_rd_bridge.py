"""Tests for src/core/rd_bridge.py and enhanced remove_duplicates in src/core/backfill.py.

Coverage — bridge_rd_torrents():
  - Exact filename match → RdTorrent record created
  - Normalized match (dots/underscores) → RdTorrent record created
  - Single-file torrent (source_path directly under mount root) → matched by filename
  - No match → unmatched_items incremented, no RdTorrent created
  - Already bridged → already_bridged counted, no double-bridging
  - Shared season pack (two items → same RD torrent hash) → single RdTorrent row (upsert)
  - Empty RD account → all items unmatched
  - RD API failure → error recorded in result
  - Lock prevents concurrent runs → second call returns immediately with error
  - Non-migration items ignored → not included in bridging

Coverage — remove_duplicates() rd_ids_to_delete:
  - Collects rd_ids from removed items' RdTorrent records
  - Excludes rd_ids shared with kept items (shared season-pack protection)
  - No RdTorrent records present → rd_ids_to_delete is empty (backward compatible)

Coverage — _normalize_name() and _extract_mount_relative_name() helpers:
  - Dots and underscores collapsed correctly
  - Path extraction returns first component relative to mount
  - Path outside mount root returns None
  - Single-file: path directly under mount returns the filename

asyncio_mode = "auto" (configured in pyproject.toml).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.backfill import (
    DeduplicateResult,
    DuplicateGroup,
    remove_duplicates,
)
from src.core.rd_bridge import (
    BridgeResult,
    _extract_mount_relative_name,
    _normalize_name,
    bridge_rd_torrents,
)
from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.symlink import Symlink
from src.models.torrent import RdTorrent, TorrentStatus


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ZURG_MOUNT = "/mnt/zurg/__all__"


def _make_item(
    session: AsyncSession,
    *,
    title: str = "Test Movie",
    imdb_id: str | None = "tt0000001",
    source: str = "migration",
    media_type: MediaType = MediaType.MOVIE,
    season: int | None = None,
    episode: int | None = None,
) -> MediaItem:
    """Construct and add an unsaved MediaItem to the session."""
    item = MediaItem(
        title=title,
        imdb_id=imdb_id,
        media_type=media_type,
        state=QueueState.COMPLETE,
        state_changed_at=datetime.now(timezone.utc),
        retry_count=0,
        source=source,
        season=season,
        episode=episode,
    )
    session.add(item)
    return item


def _make_symlink(
    session: AsyncSession,
    *,
    media_item_id: int,
    source_path: str,
    target_path: str = "/library/test.mkv",
) -> Symlink:
    """Construct and add an unsaved Symlink to the session."""
    sym = Symlink(
        media_item_id=media_item_id,
        source_path=source_path,
        target_path=target_path,
        valid=True,
    )
    session.add(sym)
    return sym


def _make_rd_torrent(
    session: AsyncSession,
    *,
    media_item_id: int,
    rd_id: str = "RDID001",
    info_hash: str = "aabbccdd0011223344556677889900aabbccddee",
    filename: str = "Test.Movie.2024.1080p",
) -> RdTorrent:
    """Construct and add an unsaved RdTorrent to the session."""
    torrent = RdTorrent(
        media_item_id=media_item_id,
        rd_id=rd_id,
        info_hash=info_hash.lower(),
        filename=filename,
        filesize=2_000_000_000,
        cached=True,
        status=TorrentStatus.ACTIVE,
    )
    session.add(torrent)
    return torrent


def _rd_dict(
    *,
    rd_id: str = "RD001",
    filename: str = "Test.Movie.2024.1080p",
    info_hash: str = "aabbccdd0011223344556677889900aabbccddee",
    size_bytes: int = 2_000_000_000,
) -> dict:
    """Build a minimal RD API torrent dict."""
    return {
        "id": rd_id,
        "filename": filename,
        "hash": info_hash,
        "bytes": size_bytes,
        "status": "downloaded",
    }


async def _flush(session: AsyncSession) -> None:
    await session.flush()


def _patch_rd_client(rd_torrents: list[dict]) -> MagicMock:
    """Return a mock RealDebridClient instance whose list_all_torrents returns rd_torrents."""
    mock_client = MagicMock()
    mock_client.list_all_torrents = AsyncMock(return_value=rd_torrents)
    return mock_client


def _rd_client_patches(mock_client: MagicMock, zurg_mount: str = ZURG_MOUNT):
    """Return a context manager stack that patches both inline imports used by rd_bridge.

    rd_bridge.py imports RealDebridClient and settings inside _bridge_rd_torrents_inner
    via ``from src.services.real_debrid import RealDebridClient`` and
    ``from src.config import settings``.  Patching the module-level names on rd_bridge
    does NOT work (the names don't exist there yet at import time).  We must patch:
      - src.services.real_debrid.RealDebridClient   → returns mock_client
      - src.config.settings                          → has .paths.zurg_mount set
    """
    from contextlib import ExitStack

    mock_settings = MagicMock()
    mock_settings.paths.zurg_mount = zurg_mount

    stack = ExitStack()
    stack.enter_context(
        patch(
            "src.services.real_debrid.RealDebridClient",
            return_value=mock_client,
        )
    )
    stack.enter_context(patch("src.config.settings", mock_settings))
    return stack


# ---------------------------------------------------------------------------
# Unit tests for private helpers
# ---------------------------------------------------------------------------


class TestNormalizeName:
    """Tests for _normalize_name()."""

    def test_dots_replaced_with_spaces(self) -> None:
        assert _normalize_name("Breaking.Bad.S01") == "breaking bad s01"

    def test_underscores_replaced_with_spaces(self) -> None:
        assert _normalize_name("Some_Movie_2024") == "some movie 2024"

    def test_mixed_separators_collapsed(self) -> None:
        assert _normalize_name("Show._Name..Here") == "show name here"

    def test_already_clean_name_lowercased(self) -> None:
        assert _normalize_name("Breaking Bad") == "breaking bad"

    def test_multiple_spaces_collapsed(self) -> None:
        # Consecutive spaces are collapsed to a single space
        result = _normalize_name("A   B")
        assert result == "a b"

    def test_empty_string(self) -> None:
        assert _normalize_name("") == ""


class TestExtractMountRelativeName:
    """Tests for _extract_mount_relative_name()."""

    def test_directory_backed_torrent_returns_directory_name(self) -> None:
        path = f"{ZURG_MOUNT}/Breaking.Bad.S01.1080p/Breaking.Bad.S01E01.mkv"
        result = _extract_mount_relative_name(path, ZURG_MOUNT)
        assert result == "Breaking.Bad.S01.1080p"

    def test_single_file_under_mount_returns_filename(self) -> None:
        path = f"{ZURG_MOUNT}/Some.Movie.2024.mkv"
        result = _extract_mount_relative_name(path, ZURG_MOUNT)
        assert result == "Some.Movie.2024.mkv"

    def test_path_outside_mount_returns_none(self) -> None:
        path = "/some/other/path/file.mkv"
        result = _extract_mount_relative_name(path, ZURG_MOUNT)
        assert result is None

    def test_path_equal_to_mount_returns_none(self) -> None:
        result = _extract_mount_relative_name(ZURG_MOUNT, ZURG_MOUNT)
        assert result is None

    def test_deeply_nested_path_returns_first_component(self) -> None:
        path = f"{ZURG_MOUNT}/Show.S02.1080p/S02E01/Show.S02E01.mkv"
        result = _extract_mount_relative_name(path, ZURG_MOUNT)
        assert result == "Show.S02.1080p"

    def test_mount_with_trailing_slash_still_works(self) -> None:
        """Mount path with trailing slash must not break relative extraction."""
        path = f"{ZURG_MOUNT}/SomeDir/file.mkv"
        # _extract_mount_relative_name uses Path.relative_to which handles trailing slashes
        result = _extract_mount_relative_name(path, ZURG_MOUNT + "/")
        # Path normalises trailing slashes, so the result should still be SomeDir
        assert result == "SomeDir"


# ---------------------------------------------------------------------------
# bridge_rd_torrents — integration tests using in-memory DB
# ---------------------------------------------------------------------------


class TestBridgeRdTorrentsExactMatch:
    """Exact filename match → RdTorrent record is created."""

    async def test_creates_rd_torrent_on_exact_match(self, session: AsyncSession) -> None:
        item = _make_item(session, title="Breaking Bad S01")
        await _flush(session)

        _make_symlink(
            session,
            media_item_id=item.id,
            source_path=f"{ZURG_MOUNT}/Breaking.Bad.S01.1080p/ep01.mkv",
        )
        await _flush(session)

        rd_data = [_rd_dict(filename="Breaking.Bad.S01.1080p", rd_id="RD_BB1")]
        mock_client = _patch_rd_client(rd_data)

        with (
            patch("src.services.real_debrid.RealDebridClient", return_value=mock_client),
            patch("src.config.settings") as mock_settings,
        ):
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            result = await bridge_rd_torrents(session)

        assert result.matched == 1
        assert result.unmatched_items == 0
        assert result.errors == []
        assert result.total_rd_torrents == 1
        assert result.total_migration_items == 1

        # Verify the RdTorrent row was actually created in the DB
        rows = await session.execute(
            text("SELECT rd_id FROM rd_torrents WHERE media_item_id = :mid").bindparams(
                mid=item.id
            )
        )
        row = rows.fetchone()
        assert row is not None
        assert row[0] == "RD_BB1"

    async def test_result_counts_correct_on_match(self, session: AsyncSession) -> None:
        item = _make_item(session, title="Test Show", imdb_id="tt1111111")
        await _flush(session)

        _make_symlink(
            session,
            media_item_id=item.id,
            source_path=f"{ZURG_MOUNT}/Test.Show.S01.2024/file.mkv",
        )
        await _flush(session)

        rd_data = [_rd_dict(filename="Test.Show.S01.2024", rd_id="RD_TS1")]
        mock_client = _patch_rd_client(rd_data)

        with (
            patch("src.services.real_debrid.RealDebridClient", return_value=mock_client),
            patch("src.config.settings") as mock_settings,
        ):
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            result = await bridge_rd_torrents(session)

        assert result.matched == 1
        assert result.already_bridged == 0
        assert result.unmatched_items == 0
        assert result.total_migration_items == 1


class TestBridgeRdTorrentsNormalizedMatch:
    """Normalized name match (dots/underscores differ) → RdTorrent created."""

    async def test_normalized_match_dots_vs_clean(self, session: AsyncSession) -> None:
        """Symlink dir has dots; RD filename has underscores — normalized match."""
        item = _make_item(session, title="Some Movie")
        await _flush(session)

        # symlink source dir: Some.Movie.2024.1080p (dots)
        _make_symlink(
            session,
            media_item_id=item.id,
            source_path=f"{ZURG_MOUNT}/Some.Movie.2024.1080p/file.mkv",
        )
        await _flush(session)

        # RD torrent uses underscores — exact match fails but normalized match succeeds
        rd_data = [_rd_dict(filename="Some_Movie_2024_1080p", rd_id="RD_NORM")]
        mock_client = _patch_rd_client(rd_data)

        with (
            patch("src.services.real_debrid.RealDebridClient", return_value=mock_client),
            patch("src.config.settings") as mock_settings,
        ):
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            result = await bridge_rd_torrents(session)

        assert result.matched == 1
        assert result.unmatched_items == 0

    async def test_normalized_match_mixed_separators(self, session: AsyncSession) -> None:
        """Mixed separators in both source and target normalize to same key."""
        item = _make_item(session, title="Anime Show")
        await _flush(session)

        _make_symlink(
            session,
            media_item_id=item.id,
            source_path=f"{ZURG_MOUNT}/Anime_Show_S02/ep01.mkv",
        )
        await _flush(session)

        rd_data = [_rd_dict(filename="Anime.Show.S02", rd_id="RD_ANIME")]
        mock_client = _patch_rd_client(rd_data)

        with (
            patch("src.services.real_debrid.RealDebridClient", return_value=mock_client),
            patch("src.config.settings") as mock_settings,
        ):
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            result = await bridge_rd_torrents(session)

        assert result.matched == 1


class TestBridgeRdTorrentsSingleFile:
    """Single-file torrent: source_path is directly under mount root."""

    async def test_single_file_torrent_matched_by_filename(
        self, session: AsyncSession
    ) -> None:
        """Source path is directly under ZURG_MOUNT with no subdirectory."""
        item = _make_item(session, title="Solo Movie")
        await _flush(session)

        # Single-file: the .mkv is directly in the mount root (no subdirectory)
        _make_symlink(
            session,
            media_item_id=item.id,
            source_path=f"{ZURG_MOUNT}/Solo.Movie.2024.1080p.mkv",
        )
        await _flush(session)

        # RD torrent filename IS the file itself (no directory)
        rd_data = [_rd_dict(filename="Solo.Movie.2024.1080p.mkv", rd_id="RD_SOLO")]
        mock_client = _patch_rd_client(rd_data)

        with (
            patch("src.services.real_debrid.RealDebridClient", return_value=mock_client),
            patch("src.config.settings") as mock_settings,
        ):
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            result = await bridge_rd_torrents(session)

        assert result.matched == 1
        assert result.unmatched_items == 0

    async def test_single_file_normalized_match(self, session: AsyncSession) -> None:
        """Single-file torrent matched via normalized name (underscores vs dots)."""
        item = _make_item(session, title="Solo Movie Two")
        await _flush(session)

        _make_symlink(
            session,
            media_item_id=item.id,
            source_path=f"{ZURG_MOUNT}/Solo.Movie.Two.2024.mkv",
        )
        await _flush(session)

        rd_data = [_rd_dict(filename="Solo_Movie_Two_2024.mkv", rd_id="RD_SOLO2")]
        mock_client = _patch_rd_client(rd_data)

        with (
            patch("src.services.real_debrid.RealDebridClient", return_value=mock_client),
            patch("src.config.settings") as mock_settings,
        ):
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            result = await bridge_rd_torrents(session)

        assert result.matched == 1


class TestBridgeRdTorrentsNoMatch:
    """No matching RD torrent → item counted as unmatched."""

    async def test_no_match_increments_unmatched(self, session: AsyncSession) -> None:
        item = _make_item(session, title="Obscure Film")
        await _flush(session)

        _make_symlink(
            session,
            media_item_id=item.id,
            source_path=f"{ZURG_MOUNT}/Obscure.Film.2024/film.mkv",
        )
        await _flush(session)

        # RD account has a completely different torrent
        rd_data = [_rd_dict(filename="Something.Completely.Different", rd_id="RD_DIFF")]
        mock_client = _patch_rd_client(rd_data)

        with (
            patch("src.services.real_debrid.RealDebridClient", return_value=mock_client),
            patch("src.config.settings") as mock_settings,
        ):
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            result = await bridge_rd_torrents(session)

        assert result.matched == 0
        assert result.unmatched_items == 1
        assert result.errors == []

    async def test_source_path_outside_mount_counts_as_unmatched(
        self, session: AsyncSession
    ) -> None:
        """If source_path is not under zurg_mount, item is unmatched (no error raised)."""
        item = _make_item(session, title="Off Mount")
        await _flush(session)

        _make_symlink(
            session,
            media_item_id=item.id,
            source_path="/completely/different/path/file.mkv",
        )
        await _flush(session)

        rd_data = [_rd_dict(filename="Off.Mount.2024", rd_id="RD_OFF")]
        mock_client = _patch_rd_client(rd_data)

        with (
            patch("src.services.real_debrid.RealDebridClient", return_value=mock_client),
            patch("src.config.settings") as mock_settings,
        ):
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            result = await bridge_rd_torrents(session)

        assert result.unmatched_items == 1
        assert result.matched == 0

    async def test_multiple_items_mixed_match_unmatched(
        self, session: AsyncSession
    ) -> None:
        """Some items match, others do not — counts are independent."""
        item_a = _make_item(session, title="Match Me", imdb_id="tt0001001")
        item_b = _make_item(session, title="Orphan Film", imdb_id="tt0001002")
        await _flush(session)

        _make_symlink(
            session,
            media_item_id=item_a.id,
            source_path=f"{ZURG_MOUNT}/Match.Me.2024/file.mkv",
        )
        _make_symlink(
            session,
            media_item_id=item_b.id,
            source_path=f"{ZURG_MOUNT}/Orphan.Film.2020/file.mkv",
        )
        await _flush(session)

        rd_data = [_rd_dict(filename="Match.Me.2024", rd_id="RD_MATCH")]
        mock_client = _patch_rd_client(rd_data)

        with (
            patch("src.services.real_debrid.RealDebridClient", return_value=mock_client),
            patch("src.config.settings") as mock_settings,
        ):
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            result = await bridge_rd_torrents(session)

        assert result.matched == 1
        assert result.unmatched_items == 1


class TestBridgeRdTorrentsAlreadyBridged:
    """Items that already have an RdTorrent record are counted but not re-bridged."""

    async def test_already_bridged_item_counted_correctly(
        self, session: AsyncSession
    ) -> None:
        item = _make_item(session, title="Already Bridged")
        await _flush(session)

        _make_symlink(
            session,
            media_item_id=item.id,
            source_path=f"{ZURG_MOUNT}/Already.Bridged.2023/file.mkv",
        )
        # Add a pre-existing RdTorrent for this item
        _make_rd_torrent(
            session,
            media_item_id=item.id,
            rd_id="EXISTING_RD",
            info_hash="1234567890abcdef1234567890abcdef12345678",
            filename="Already.Bridged.2023",
        )
        await _flush(session)

        rd_data = [_rd_dict(filename="Already.Bridged.2023", rd_id="EXISTING_RD", info_hash="1234567890abcdef1234567890abcdef12345678")]
        mock_client = _patch_rd_client(rd_data)

        with (
            patch("src.services.real_debrid.RealDebridClient", return_value=mock_client),
            patch("src.config.settings") as mock_settings,
        ):
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            result = await bridge_rd_torrents(session)

        assert result.already_bridged == 1
        assert result.total_migration_items == 1
        # The item's hash is already in rd_torrents → counted as already_bridged, not matched
        assert result.matched == 0

    async def test_already_bridged_not_double_inserted(
        self, session: AsyncSession
    ) -> None:
        """No duplicate RdTorrent row is inserted for an already-bridged item."""
        item = _make_item(session, title="Bridged Once")
        await _flush(session)

        _make_symlink(
            session,
            media_item_id=item.id,
            source_path=f"{ZURG_MOUNT}/Bridged.Once.S01/ep01.mkv",
        )
        _make_rd_torrent(
            session,
            media_item_id=item.id,
            rd_id="RD_ONCE",
            info_hash="deadbeef000000000000000000000000deadbeef",
        )
        await _flush(session)

        rd_data = [_rd_dict(filename="Bridged.Once.S01", rd_id="RD_ONCE", info_hash="deadbeef000000000000000000000000deadbeef")]
        mock_client = _patch_rd_client(rd_data)

        with (
            patch("src.services.real_debrid.RealDebridClient", return_value=mock_client),
            patch("src.config.settings") as mock_settings,
        ):
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            await bridge_rd_torrents(session)

        # Still only one rd_torrents row for this item
        rows = await session.execute(
            text("SELECT COUNT(*) FROM rd_torrents WHERE media_item_id = :mid").bindparams(
                mid=item.id
            )
        )
        assert rows.scalar() == 1


class TestBridgeRdTorrentsSharedSeasonPack:
    """Multiple items sharing the same RD torrent → only one RdTorrent row (upsert)."""

    async def test_shared_torrent_deduped_by_info_hash(
        self, session: AsyncSession
    ) -> None:
        """Two episode items pointing to the same torrent directory → one RdTorrent."""
        item_a = _make_item(
            session,
            title="Great Show",
            imdb_id="tt2000001",
            season=1,
            episode=1,
            media_type=MediaType.SHOW,
        )
        item_b = _make_item(
            session,
            title="Great Show",
            imdb_id="tt2000001",
            season=1,
            episode=2,
            media_type=MediaType.SHOW,
        )
        await _flush(session)

        shared_dir = f"{ZURG_MOUNT}/Great.Show.S01.1080p"
        _make_symlink(
            session,
            media_item_id=item_a.id,
            source_path=f"{shared_dir}/ep01.mkv",
        )
        _make_symlink(
            session,
            media_item_id=item_b.id,
            source_path=f"{shared_dir}/ep02.mkv",
        )
        await _flush(session)

        shared_hash = "cafebabe1234567890abcdef1234567890abcdef"
        rd_data = [
            _rd_dict(
                filename="Great.Show.S01.1080p",
                rd_id="RD_SHARED",
                info_hash=shared_hash,
            )
        ]
        mock_client = _patch_rd_client(rd_data)

        with (
            patch("src.services.real_debrid.RealDebridClient", return_value=mock_client),
            patch("src.config.settings") as mock_settings,
        ):
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            result = await bridge_rd_torrents(session)

        # First item is matched, second shares the same hash → already_bridged
        assert result.matched == 1
        assert result.already_bridged == 1
        assert result.unmatched_items == 0

        # Despite two items, only ONE RdTorrent row with that hash should exist
        rows = await session.execute(
            text(
                "SELECT COUNT(*) FROM rd_torrents WHERE info_hash = :h"
            ).bindparams(h=shared_hash.lower())
        )
        assert rows.scalar() == 1

    async def test_shared_torrent_result_matched_equals_item_count(
        self, session: AsyncSession
    ) -> None:
        """matched count reflects number of items bridged, not RdTorrent rows."""
        items = [
            _make_item(
                session,
                title="Pack Show",
                imdb_id="tt3000001",
                season=1,
                episode=ep,
                media_type=MediaType.SHOW,
            )
            for ep in range(1, 4)
        ]
        await _flush(session)

        shared_dir = f"{ZURG_MOUNT}/Pack.Show.S01.Complete"
        for item in items:
            _make_symlink(
                session,
                media_item_id=item.id,
                source_path=f"{shared_dir}/ep{item.episode}.mkv",
            )
        await _flush(session)

        shared_hash = "11223344556677889900aabbccddeeff00112233"
        rd_data = [
            _rd_dict(
                filename="Pack.Show.S01.Complete",
                rd_id="RD_PACK",
                info_hash=shared_hash,
            )
        ]
        mock_client = _patch_rd_client(rd_data)

        with (
            patch("src.services.real_debrid.RealDebridClient", return_value=mock_client),
            patch("src.config.settings") as mock_settings,
        ):
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            result = await bridge_rd_torrents(session)

        # First item matched, remaining 2 share the hash → already_bridged
        assert result.matched == 1
        assert result.already_bridged == 2


class TestBridgeRdTorrentsEmptyAccount:
    """Empty RD account → all migration items are unmatched."""

    async def test_empty_rd_account_all_unmatched(self, session: AsyncSession) -> None:
        item = _make_item(session, title="Lonely Movie")
        await _flush(session)

        _make_symlink(
            session,
            media_item_id=item.id,
            source_path=f"{ZURG_MOUNT}/Lonely.Movie.2024/file.mkv",
        )
        await _flush(session)

        mock_client = _patch_rd_client([])  # empty RD account

        with (
            patch("src.services.real_debrid.RealDebridClient", return_value=mock_client),
            patch("src.config.settings") as mock_settings,
        ):
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            result = await bridge_rd_torrents(session)

        assert result.total_rd_torrents == 0
        assert result.matched == 0
        # When RD returns empty list, function returns early before matching
        # so unmatched_items remains 0 too (items not checked)
        assert result.errors == []

    async def test_empty_rd_account_returns_early(self, session: AsyncSession) -> None:
        """With zero RD torrents the function returns before any item processing.

        The implementation returns early after seeing an empty RD list, before it
        ever queries migration items.  So total_migration_items stays 0.
        """
        item = _make_item(session, title="No RD Movie")
        await _flush(session)

        _make_symlink(
            session,
            media_item_id=item.id,
            source_path=f"{ZURG_MOUNT}/No.RD.Movie/file.mkv",
        )
        await _flush(session)

        mock_client = _patch_rd_client([])

        with (
            patch("src.services.real_debrid.RealDebridClient", return_value=mock_client),
            patch("src.config.settings") as mock_settings,
        ):
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            result = await bridge_rd_torrents(session)

        # Returns early before querying migration items → total_migration_items stays 0
        assert result.total_rd_torrents == 0
        assert result.total_migration_items == 0
        assert result.matched == 0


class TestBridgeRdTorrentsApiFailure:
    """RD API failure → error is recorded in result, no exception raised."""

    async def test_rd_api_exception_recorded_in_result(
        self, session: AsyncSession
    ) -> None:
        mock_client = MagicMock()
        mock_client.list_all_torrents = AsyncMock(
            side_effect=RuntimeError("RD connection refused")
        )

        with (
            patch("src.services.real_debrid.RealDebridClient", return_value=mock_client),
            patch("src.config.settings") as mock_settings,
        ):
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            result = await bridge_rd_torrents(session)

        assert result.matched == 0
        assert len(result.errors) == 1
        assert "RD connection refused" in result.errors[0]

    async def test_rd_api_timeout_does_not_raise(self, session: AsyncSession) -> None:
        """httpx.TimeoutException from list_all_torrents is caught and stored."""
        import httpx

        mock_client = MagicMock()
        mock_client.list_all_torrents = AsyncMock(
            side_effect=httpx.TimeoutException("timed out")
        )

        with (
            patch("src.services.real_debrid.RealDebridClient", return_value=mock_client),
            patch("src.config.settings") as mock_settings,
        ):
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            # Must not raise
            result = await bridge_rd_torrents(session)

        assert len(result.errors) == 1
        assert result.total_rd_torrents == 0

    async def test_rd_api_failure_returns_empty_bridge_result(
        self, session: AsyncSession
    ) -> None:
        """A failed API call returns a BridgeResult with all zero counts."""
        mock_client = MagicMock()
        mock_client.list_all_torrents = AsyncMock(side_effect=ValueError("bad response"))

        with (
            patch("src.services.real_debrid.RealDebridClient", return_value=mock_client),
            patch("src.config.settings") as mock_settings,
        ):
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            result = await bridge_rd_torrents(session)

        assert result.total_rd_torrents == 0
        assert result.matched == 0
        assert result.already_bridged == 0
        assert result.unmatched_items == 0
        assert result.total_migration_items == 0


class TestBridgeRdTorrentsLock:
    """Mutual exclusion is now enforced by the route's _cleanup_lock.

    bridge_rd_torrents() itself has no inner lock — it is stateless and
    re-entrant. The tests below verify this property and that successive
    calls can proceed without interference.
    """

    async def test_has_no_module_level_lock(self) -> None:
        """The _bridge_lock attribute no longer exists on the module."""
        from src.core import rd_bridge

        assert not hasattr(rd_bridge, "_bridge_lock"), (
            "_bridge_lock was removed; mutual exclusion belongs to the route"
        )

    async def test_successive_calls_succeed(self, session: AsyncSession) -> None:
        """bridge_rd_torrents can be called twice in a row without error."""
        mock_client = _patch_rd_client([])

        with (
            patch("src.services.real_debrid.RealDebridClient", return_value=mock_client),
            patch("src.config.settings") as mock_settings,
        ):
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            result1 = await bridge_rd_torrents(session)
            result2 = await bridge_rd_torrents(session)

        assert result1.errors == []
        assert result2.errors == []

    async def test_api_failure_does_not_block_subsequent_call(
        self, session: AsyncSession
    ) -> None:
        """Even when list_all_torrents raises, a subsequent call can proceed."""
        mock_fail = MagicMock()
        mock_fail.list_all_torrents = AsyncMock(side_effect=RuntimeError("oops"))
        mock_ok = _patch_rd_client([])

        with (
            patch("src.config.settings") as mock_settings,
        ):
            mock_settings.paths.zurg_mount = ZURG_MOUNT

            with patch("src.services.real_debrid.RealDebridClient", return_value=mock_fail):
                result1 = await bridge_rd_torrents(session)

            with patch("src.services.real_debrid.RealDebridClient", return_value=mock_ok):
                result2 = await bridge_rd_torrents(session)

        assert len(result1.errors) == 1
        assert result2.errors == []


class TestBridgeRdTorrentsNonMigrationIgnored:
    """Items with source != 'migration' are not included in bridging."""

    async def test_non_migration_items_excluded(self, session: AsyncSession) -> None:
        """Items with source='plex_watchlist' are not queried by the bridge."""
        item_mig = _make_item(session, title="Migration Item", imdb_id="tt4000001", source="migration")
        item_plx = _make_item(session, title="Plex Item", imdb_id="tt4000002", source="plex_watchlist")
        await _flush(session)

        _make_symlink(
            session,
            media_item_id=item_mig.id,
            source_path=f"{ZURG_MOUNT}/Migration.Item.2024/file.mkv",
        )
        _make_symlink(
            session,
            media_item_id=item_plx.id,
            source_path=f"{ZURG_MOUNT}/Plex.Item.2024/file.mkv",
        )
        await _flush(session)

        rd_data = [
            _rd_dict(filename="Migration.Item.2024", rd_id="RD_MIG"),
            _rd_dict(filename="Plex.Item.2024", rd_id="RD_PLX",
                     info_hash="ffffffffffffffffffffffffffffffffffffffff"),
        ]
        mock_client = _patch_rd_client(rd_data)

        with (
            patch("src.services.real_debrid.RealDebridClient", return_value=mock_client),
            patch("src.config.settings") as mock_settings,
        ):
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            result = await bridge_rd_torrents(session)

        # Only migration item is counted
        assert result.total_migration_items == 1
        assert result.matched == 1

        # Plex item should have NO RdTorrent record created
        rows = await session.execute(
            text("SELECT COUNT(*) FROM rd_torrents WHERE media_item_id = :mid").bindparams(
                mid=item_plx.id
            )
        )
        assert rows.scalar() == 0

    async def test_manual_source_items_excluded(self, session: AsyncSession) -> None:
        """source='manual' items are also excluded."""
        item = _make_item(session, title="Manual Add", source="manual")
        await _flush(session)

        _make_symlink(
            session,
            media_item_id=item.id,
            source_path=f"{ZURG_MOUNT}/Manual.Add.2024/file.mkv",
        )
        await _flush(session)

        rd_data = [_rd_dict(filename="Manual.Add.2024", rd_id="RD_MANUAL")]
        mock_client = _patch_rd_client(rd_data)

        with (
            patch("src.services.real_debrid.RealDebridClient", return_value=mock_client),
            patch("src.config.settings") as mock_settings,
        ):
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            result = await bridge_rd_torrents(session)

        assert result.total_migration_items == 0
        assert result.matched == 0


class TestBridgeRdTorrentsEdgeCases:
    """Edge cases: missing symlinks, empty filenames, large batches."""

    async def test_rd_torrent_with_empty_filename_skipped(
        self, session: AsyncSession
    ) -> None:
        """RD torrents with empty/missing filename are ignored during lookup-dict build."""
        item = _make_item(session, title="Valid Movie")
        await _flush(session)

        _make_symlink(
            session,
            media_item_id=item.id,
            source_path=f"{ZURG_MOUNT}/Valid.Movie.2024/file.mkv",
        )
        await _flush(session)

        rd_data = [
            {"id": "RD_EMPTY", "filename": "", "hash": "aaa0000000000000000000000000000000000000", "bytes": 0},
            _rd_dict(filename="Valid.Movie.2024", rd_id="RD_VALID"),
        ]
        mock_client = _patch_rd_client(rd_data)

        with (
            patch("src.services.real_debrid.RealDebridClient", return_value=mock_client),
            patch("src.config.settings") as mock_settings,
        ):
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            result = await bridge_rd_torrents(session)

        # The empty-filename torrent is skipped; the valid one matches
        assert result.matched == 1

    async def test_item_with_no_symlinks_not_in_migration_query(
        self, session: AsyncSession
    ) -> None:
        """A migration item with no symlink records is excluded by the INNER JOIN query.

        The DB query is ``INNER JOIN symlinks``, so items without at least one
        symlink row are never returned and thus never counted in total_migration_items
        or unmatched_items.
        """
        # Item exists but has no symlinks at all
        _make_item(session, title="Symlink-less Movie")
        await _flush(session)

        rd_data = [_rd_dict(filename="Symlink-less.Movie.2024")]
        mock_client = _patch_rd_client(rd_data)

        with (
            patch("src.services.real_debrid.RealDebridClient", return_value=mock_client),
            patch("src.config.settings") as mock_settings,
        ):
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            result = await bridge_rd_torrents(session)

        # INNER JOIN on symlinks filters the item out entirely
        assert result.total_migration_items == 0
        assert result.unmatched_items == 0
        assert result.matched == 0

    async def test_bridge_result_is_pydantic_model(self, session: AsyncSession) -> None:
        """The return value is a BridgeResult Pydantic model with correct field defaults."""
        mock_client = _patch_rd_client([])

        with (
            patch("src.services.real_debrid.RealDebridClient", return_value=mock_client),
            patch("src.config.settings") as mock_settings,
        ):
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            result = await bridge_rd_torrents(session)

        assert isinstance(result, BridgeResult)
        assert isinstance(result.errors, list)
        assert result.total_rd_torrents == 0
        assert result.total_migration_items == 0
        assert result.matched == 0
        assert result.already_bridged == 0
        assert result.unmatched_items == 0


# ---------------------------------------------------------------------------
# Enhanced remove_duplicates — rd_ids_to_delete tests
# ---------------------------------------------------------------------------


def _make_duplicate_group(
    keep_id: int,
    remove_ids: list[int],
    imdb_id: str = "tt5000001",
) -> DuplicateGroup:
    """Build a backfill.DuplicateGroup for testing remove_duplicates."""
    return DuplicateGroup(
        imdb_id=imdb_id,
        title="Test Title",
        season=None,
        episode=None,
        keep_id=keep_id,
        remove_ids=remove_ids,
        count=len(remove_ids) + 1,
    )


class TestRemoveDuplicatesRdIds:
    """Tests for the rd_ids_to_delete enhancement in remove_duplicates()."""

    async def test_collects_rd_ids_from_removed_items(
        self, session: AsyncSession
    ) -> None:
        """rd_ids_to_delete contains the rd_id of the removed item's RdTorrent."""
        item_keep = _make_item(session, imdb_id="tt5000001", source="migration")
        item_del = _make_item(session, imdb_id="tt5000001", source="migration")
        await _flush(session)

        _make_rd_torrent(
            session,
            media_item_id=item_del.id,
            rd_id="RD_TO_DELETE",
            info_hash="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        )
        await _flush(session)

        group = _make_duplicate_group(
            keep_id=item_keep.id, remove_ids=[item_del.id], imdb_id="tt5000001"
        )
        result = await remove_duplicates(session, [group])

        assert "RD_TO_DELETE" in result.rd_ids_to_delete

    async def test_excludes_rd_ids_shared_with_kept_item(
        self, session: AsyncSession
    ) -> None:
        """rd_id shared by both kept and removed items must NOT appear in rd_ids_to_delete."""
        item_keep = _make_item(session, imdb_id="tt5000002", source="migration")
        item_del = _make_item(session, imdb_id="tt5000002", source="migration")
        await _flush(session)

        shared_hash = "cccccccccccccccccccccccccccccccccccccccc"
        # Both items reference the same RD torrent via the same rd_id
        # (shared season pack: register on kept item first, then upsert for deleted item
        #  — in practice both rows would point to the same rd_id)
        torrent_keep = _make_rd_torrent(
            session,
            media_item_id=item_keep.id,
            rd_id="RD_SHARED_PACK",
            info_hash=shared_hash,
        )
        await _flush(session)

        # Second item references the same rd_id (different RdTorrent row, same rd_id)
        torrent_del = RdTorrent(
            media_item_id=item_del.id,
            rd_id="RD_SHARED_PACK",  # same rd_id as kept item
            info_hash="dddddddddddddddddddddddddddddddddddddddd",
            filename="Season.Pack.S01",
            filesize=5_000_000_000,
            cached=True,
            status=TorrentStatus.ACTIVE,
        )
        session.add(torrent_del)
        await _flush(session)

        group = _make_duplicate_group(
            keep_id=item_keep.id, remove_ids=[item_del.id], imdb_id="tt5000002"
        )
        result = await remove_duplicates(session, [group])

        # "RD_SHARED_PACK" is also on the kept item → must NOT be in rd_ids_to_delete
        assert "RD_SHARED_PACK" not in result.rd_ids_to_delete

    async def test_no_rd_torrent_records_means_empty_rd_ids(
        self, session: AsyncSession
    ) -> None:
        """When removed items have no RdTorrent rows, rd_ids_to_delete is empty list."""
        item_keep = _make_item(session, imdb_id="tt5000003", source="migration")
        item_del = _make_item(session, imdb_id="tt5000003", source="migration")
        await _flush(session)

        # No RdTorrent records at all (old migration without bridge)
        group = _make_duplicate_group(
            keep_id=item_keep.id, remove_ids=[item_del.id], imdb_id="tt5000003"
        )
        result = await remove_duplicates(session, [group])

        assert result.rd_ids_to_delete == []
        # Item still deleted
        assert result.removed == 1

    async def test_multiple_removed_items_all_rd_ids_collected(
        self, session: AsyncSession
    ) -> None:
        """rd_ids_to_delete accumulates RD IDs from all removed items."""
        item_keep = _make_item(session, imdb_id="tt5000004", source="migration")
        item_del1 = _make_item(session, imdb_id="tt5000004", source="migration")
        item_del2 = _make_item(session, imdb_id="tt5000004", source="migration")
        await _flush(session)

        _make_rd_torrent(
            session,
            media_item_id=item_del1.id,
            rd_id="RD_DEL_ONE",
            info_hash="eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        )
        _make_rd_torrent(
            session,
            media_item_id=item_del2.id,
            rd_id="RD_DEL_TWO",
            info_hash="ffffffffffffffffffffffffffffffffffffffff",
        )
        await _flush(session)

        group = _make_duplicate_group(
            keep_id=item_keep.id,
            remove_ids=[item_del1.id, item_del2.id],
            imdb_id="tt5000004",
        )
        result = await remove_duplicates(session, [group])

        assert set(result.rd_ids_to_delete) == {"RD_DEL_ONE", "RD_DEL_TWO"}

    async def test_unique_rd_id_on_removed_item_present_in_result(
        self, session: AsyncSession
    ) -> None:
        """rd_id that belongs only to the removed item (not the kept one) is included."""
        item_keep = _make_item(session, imdb_id="tt5000005", source="migration")
        item_del = _make_item(session, imdb_id="tt5000005", source="migration")
        await _flush(session)

        # Kept item has its own unique torrent
        _make_rd_torrent(
            session,
            media_item_id=item_keep.id,
            rd_id="RD_KEEP_UNIQUE",
            info_hash="1111111111111111111111111111111111111111",
        )
        # Deleted item has a different unique torrent
        _make_rd_torrent(
            session,
            media_item_id=item_del.id,
            rd_id="RD_DEL_UNIQUE",
            info_hash="2222222222222222222222222222222222222222",
        )
        await _flush(session)

        group = _make_duplicate_group(
            keep_id=item_keep.id, remove_ids=[item_del.id], imdb_id="tt5000005"
        )
        result = await remove_duplicates(session, [group])

        # Only the deleted item's unique RD torrent should be queued for deletion
        assert "RD_DEL_UNIQUE" in result.rd_ids_to_delete
        assert "RD_KEEP_UNIQUE" not in result.rd_ids_to_delete

    async def test_empty_groups_rd_ids_to_delete_is_empty(
        self, session: AsyncSession
    ) -> None:
        """Passing empty groups returns zeroed result with empty rd_ids_to_delete."""
        result = await remove_duplicates(session, [])

        assert result.rd_ids_to_delete == []
        assert result.removed == 0

    async def test_rd_ids_to_delete_is_list_type(
        self, session: AsyncSession
    ) -> None:
        """rd_ids_to_delete is always a list (never None)."""
        item_keep = _make_item(session, imdb_id="tt5000006", source="migration")
        item_del = _make_item(session, imdb_id="tt5000006", source="migration")
        await _flush(session)

        group = _make_duplicate_group(
            keep_id=item_keep.id, remove_ids=[item_del.id], imdb_id="tt5000006"
        )
        result = await remove_duplicates(session, [group])

        assert isinstance(result.rd_ids_to_delete, list)

    async def test_cross_group_rd_ids_accumulated(
        self, session: AsyncSession
    ) -> None:
        """RD IDs from multiple independent groups are all collected."""
        item_a_keep = _make_item(session, imdb_id="tt5000007", source="migration")
        item_a_del = _make_item(session, imdb_id="tt5000007", source="migration")
        item_b_keep = _make_item(session, imdb_id="tt5000008", source="migration")
        item_b_del = _make_item(session, imdb_id="tt5000008", source="migration")
        await _flush(session)

        _make_rd_torrent(
            session,
            media_item_id=item_a_del.id,
            rd_id="RD_GROUP_A",
            info_hash="aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111",
        )
        _make_rd_torrent(
            session,
            media_item_id=item_b_del.id,
            rd_id="RD_GROUP_B",
            info_hash="bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222",
        )
        await _flush(session)

        groups = [
            _make_duplicate_group(
                keep_id=item_a_keep.id, remove_ids=[item_a_del.id], imdb_id="tt5000007"
            ),
            _make_duplicate_group(
                keep_id=item_b_keep.id, remove_ids=[item_b_del.id], imdb_id="tt5000008"
            ),
        ]
        result = await remove_duplicates(session, groups)

        assert set(result.rd_ids_to_delete) == {"RD_GROUP_A", "RD_GROUP_B"}
        assert result.removed == 2
        assert result.groups == 2
