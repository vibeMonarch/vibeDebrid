"""Tests for src/core/dedup.py.

Covers:
  - check_local_duplicate: hit, miss, status filtering
  - check_content_duplicate: movies, shows, season packs, resolution filter
  - register_torrent: creates record, normalises hash, sets ACTIVE status
  - mark_torrent_removed: status update, missing hash no-op
  - mark_torrent_replaced: status update, old/new hash logging
  - find_account_duplicates: grouping, dedup detection, IMDB ID resolution,
    PTN parse failure handling, empty filename skip, single-entry groups skipped
  - _normalize_title: dots/underscores/case handling
  - Module-level singleton is a DedupEngine instance
"""

from __future__ import annotations

import logging
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.dedup import (
    DedupEngine,
    _normalize_title,
    dedup_engine,
)
from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.torrent import RdTorrent, TorrentStatus

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _make_media_item(
    session: AsyncSession,
    *,
    imdb_id: str = "tt1234567",
    title: str = "Test Movie",
    media_type: MediaType = MediaType.MOVIE,
    season: int | None = None,
    episode: int | None = None,
) -> MediaItem:
    """Persist and return a minimal MediaItem."""
    item = MediaItem(
        imdb_id=imdb_id,
        title=title,
        year=2024,
        media_type=media_type,
        state=QueueState.WANTED,
        retry_count=0,
        season=season,
        episode=episode,
    )
    session.add(item)
    await session.flush()
    return item


async def _make_rd_torrent(
    session: AsyncSession,
    *,
    rd_id: str = "RDTEST001",
    info_hash: str = "aabbccdd" * 5,
    media_item_id: int | None = None,
    resolution: str | None = "1080p",
    status: TorrentStatus = TorrentStatus.ACTIVE,
    filename: str = "Test.Movie.2024.1080p.WEB-DL.mkv",
) -> RdTorrent:
    """Persist and return a minimal RdTorrent."""
    torrent = RdTorrent(
        rd_id=rd_id,
        info_hash=info_hash.lower(),
        media_item_id=media_item_id,
        resolution=resolution,
        status=status,
        filename=filename,
        filesize=1_500_000_000,
        cached=True,
    )
    session.add(torrent)
    await session.flush()
    return torrent


def _make_rd_dict(
    *,
    rd_id: str = "RDTEST001",
    filename: str = "Test.Movie.2024.1080p.WEB-DL.mkv",
    info_hash: str = "aabbccdd" * 5,
    size_bytes: int = 1_500_000_000,
    added: str | None = "2024-01-15T10:00:00.000Z",
    status: str = "downloaded",
) -> dict[str, Any]:
    """Build a minimal RD API torrent dict (mimics list_torrents() output)."""
    return {
        "id": rd_id,
        "filename": filename,
        "hash": info_hash,
        "bytes": size_bytes,
        "added": added,
        "status": status,
    }


# ---------------------------------------------------------------------------
# Group 0: Helper / pure function tests
# ---------------------------------------------------------------------------


class TestNormalizeTitle:
    """Tests for the _normalize_title helper."""

    def test_dots_replaced_by_spaces(self) -> None:
        """Dots in the title are replaced by spaces."""
        assert _normalize_title("Breaking.Bad") == "breaking bad"

    def test_underscores_replaced_by_spaces(self) -> None:
        """Underscores in the title are replaced by spaces."""
        assert _normalize_title("The_Dark_Knight") == "the dark knight"

    def test_lowercased(self) -> None:
        """Output is always lowercase."""
        assert _normalize_title("Jujutsu Kaisen") == "jujutsu kaisen"

    def test_mixed_separators(self) -> None:
        """Mixed dots and underscores are all replaced."""
        assert _normalize_title("Show.Name_2024") == "show name 2024"

    def test_consecutive_separators_collapsed(self) -> None:
        """Consecutive separators collapse to a single space."""
        assert _normalize_title("Show..Name") == "show name"

    def test_leading_trailing_stripped(self) -> None:
        """Leading and trailing separators are stripped."""
        assert _normalize_title(".Title.") == "title"

    def test_plain_string_unchanged(self) -> None:
        """A plain lowercase string with spaces passes through unchanged."""
        assert _normalize_title("already clean") == "already clean"


# ---------------------------------------------------------------------------
# Group 1: check_local_duplicate
# ---------------------------------------------------------------------------


class TestCheckLocalDuplicate:
    """Tests for DedupEngine.check_local_duplicate."""

    async def test_returns_active_torrent_on_match(
        self, session: AsyncSession
    ) -> None:
        """Returns the ACTIVE RdTorrent when info_hash matches."""
        engine = DedupEngine()
        torrent = await _make_rd_torrent(
            session, info_hash="deadbeef" * 5, status=TorrentStatus.ACTIVE
        )
        found = await engine.check_local_duplicate(session, "deadbeef" * 5)
        assert found is not None
        assert found.id == torrent.id

    async def test_returns_none_when_no_match(self, session: AsyncSession) -> None:
        """Returns None when the hash is not in the registry."""
        engine = DedupEngine()
        found = await engine.check_local_duplicate(session, "00000000" * 5)
        assert found is None

    async def test_ignores_removed_torrent(self, session: AsyncSession) -> None:
        """A REMOVED torrent with the same hash does not satisfy the check."""
        engine = DedupEngine()
        await _make_rd_torrent(
            session, info_hash="cafebabe" * 5, status=TorrentStatus.REMOVED
        )
        found = await engine.check_local_duplicate(session, "cafebabe" * 5)
        assert found is None

    async def test_ignores_replaced_torrent(self, session: AsyncSession) -> None:
        """A REPLACED torrent with the same hash does not satisfy the check."""
        engine = DedupEngine()
        await _make_rd_torrent(
            session, info_hash="f00dcafe" * 5, status=TorrentStatus.REPLACED
        )
        found = await engine.check_local_duplicate(session, "f00dcafe" * 5)
        assert found is None

    async def test_case_insensitive_hash_lookup(self, session: AsyncSession) -> None:
        """Hash comparison is case-insensitive (stored lowercase, query uppercase)."""
        engine = DedupEngine()
        await _make_rd_torrent(session, info_hash="abcd1234" * 5)
        # Query with uppercase hash
        found = await engine.check_local_duplicate(session, "ABCD1234" * 5)
        assert found is not None

    async def test_active_torrent_with_media_item(self, session: AsyncSession) -> None:
        """Returns the torrent even when a media_item is linked."""
        engine = DedupEngine()
        media = await _make_media_item(session)
        torrent = await _make_rd_torrent(
            session,
            info_hash="11223344" * 5,
            media_item_id=media.id,
        )
        found = await engine.check_local_duplicate(session, "11223344" * 5)
        assert found is not None
        assert found.id == torrent.id


# ---------------------------------------------------------------------------
# Group 2: check_content_duplicate
# ---------------------------------------------------------------------------


class TestCheckContentDuplicate:
    """Tests for DedupEngine.check_content_duplicate."""

    async def test_movie_match(self, session: AsyncSession) -> None:
        """Finds an ACTIVE movie torrent matching IMDB ID with no season/episode."""
        engine = DedupEngine()
        media = await _make_media_item(session, imdb_id="tt9876543")
        torrent = await _make_rd_torrent(
            session, info_hash="11111111" * 5, media_item_id=media.id
        )
        found = await engine.check_content_duplicate(
            session,
            imdb_id="tt9876543",
            season=None,
            episode=None,
            resolution=None,
        )
        assert found is not None
        assert found.id == torrent.id

    async def test_movie_no_match_different_imdb(self, session: AsyncSession) -> None:
        """Returns None when IMDB ID differs."""
        engine = DedupEngine()
        media = await _make_media_item(session, imdb_id="tt1111111")
        await _make_rd_torrent(
            session, info_hash="22222222" * 5, media_item_id=media.id
        )
        found = await engine.check_content_duplicate(
            session,
            imdb_id="tt9999999",  # different
            season=None,
            episode=None,
            resolution=None,
        )
        assert found is None

    async def test_show_episode_match(self, session: AsyncSession) -> None:
        """Finds an ACTIVE episode torrent matching IMDB ID + S + E."""
        engine = DedupEngine()
        media = await _make_media_item(
            session,
            imdb_id="tt5555555",
            media_type=MediaType.SHOW,
            season=2,
            episode=6,
        )
        torrent = await _make_rd_torrent(
            session, info_hash="33333333" * 5, media_item_id=media.id
        )
        found = await engine.check_content_duplicate(
            session,
            imdb_id="tt5555555",
            season=2,
            episode=6,
            resolution=None,
        )
        assert found is not None
        assert found.id == torrent.id

    async def test_show_episode_wrong_episode(self, session: AsyncSession) -> None:
        """Returns None when episode numbers differ."""
        engine = DedupEngine()
        media = await _make_media_item(
            session,
            imdb_id="tt5555555",
            media_type=MediaType.SHOW,
            season=2,
            episode=6,
        )
        await _make_rd_torrent(
            session, info_hash="44444444" * 5, media_item_id=media.id
        )
        found = await engine.check_content_duplicate(
            session,
            imdb_id="tt5555555",
            season=2,
            episode=7,  # different
            resolution=None,
        )
        assert found is None

    async def test_season_pack_match(self, session: AsyncSession) -> None:
        """Finds a season-pack torrent (season set, episode None)."""
        engine = DedupEngine()
        media = await _make_media_item(
            session,
            imdb_id="tt7777777",
            media_type=MediaType.SHOW,
            season=1,
            episode=None,
        )
        torrent = await _make_rd_torrent(
            session, info_hash="55555555" * 5, media_item_id=media.id
        )
        found = await engine.check_content_duplicate(
            session,
            imdb_id="tt7777777",
            season=1,
            episode=None,
            resolution=None,
        )
        assert found is not None
        assert found.id == torrent.id

    async def test_resolution_filter_match(self, session: AsyncSession) -> None:
        """Resolution filter matches when stored and requested resolution are equal."""
        engine = DedupEngine()
        media = await _make_media_item(session, imdb_id="tt8888888")
        torrent = await _make_rd_torrent(
            session,
            info_hash="66666666" * 5,
            media_item_id=media.id,
            resolution="1080p",
        )
        found = await engine.check_content_duplicate(
            session,
            imdb_id="tt8888888",
            season=None,
            episode=None,
            resolution="1080p",
        )
        assert found is not None
        assert found.id == torrent.id

    async def test_resolution_filter_no_match(self, session: AsyncSession) -> None:
        """Returns None when stored resolution differs from requested."""
        engine = DedupEngine()
        media = await _make_media_item(session, imdb_id="tt8888888")
        await _make_rd_torrent(
            session,
            info_hash="77777777" * 5,
            media_item_id=media.id,
            resolution="720p",
        )
        found = await engine.check_content_duplicate(
            session,
            imdb_id="tt8888888",
            season=None,
            episode=None,
            resolution="1080p",  # different
        )
        assert found is None

    async def test_resolution_none_matches_any(self, session: AsyncSession) -> None:
        """resolution=None accepts any stored resolution."""
        engine = DedupEngine()
        media = await _make_media_item(session, imdb_id="tt2222222")
        torrent = await _make_rd_torrent(
            session,
            info_hash="88888888" * 5,
            media_item_id=media.id,
            resolution="2160p",
        )
        found = await engine.check_content_duplicate(
            session,
            imdb_id="tt2222222",
            season=None,
            episode=None,
            resolution=None,
        )
        assert found is not None
        assert found.id == torrent.id

    async def test_ignores_removed_torrent(self, session: AsyncSession) -> None:
        """REMOVED content torrents are not returned as duplicates."""
        engine = DedupEngine()
        media = await _make_media_item(session, imdb_id="tt3333333")
        await _make_rd_torrent(
            session,
            info_hash="99999999" * 5,
            media_item_id=media.id,
            status=TorrentStatus.REMOVED,
        )
        found = await engine.check_content_duplicate(
            session,
            imdb_id="tt3333333",
            season=None,
            episode=None,
            resolution=None,
        )
        assert found is None

    async def test_returns_none_when_torrent_has_no_media_item(
        self, session: AsyncSession
    ) -> None:
        """A torrent with no linked media_item is not returned."""
        engine = DedupEngine()
        await _make_rd_torrent(
            session,
            info_hash="aaaabbbb" * 5,
            media_item_id=None,  # no link
        )
        found = await engine.check_content_duplicate(
            session,
            imdb_id="tt9090909",
            season=None,
            episode=None,
            resolution=None,
        )
        assert found is None


# ---------------------------------------------------------------------------
# Group 3: register_torrent
# ---------------------------------------------------------------------------


class TestRegisterTorrent:
    """Tests for DedupEngine.register_torrent."""

    async def test_creates_active_record(self, session: AsyncSession) -> None:
        """New record is created with ACTIVE status."""
        engine = DedupEngine()
        torrent = await engine.register_torrent(
            session,
            rd_id="NEW001",
            info_hash="ccccdddd" * 5,
            magnet_uri="magnet:?xt=urn:btih:ccccdddd" * 5,
            media_item_id=None,
            filename="Test.Movie.2024.1080p.mkv",
            filesize=2_000_000_000,
            resolution="1080p",
            cached=True,
        )
        assert torrent.id is not None
        assert torrent.status == TorrentStatus.ACTIVE
        assert torrent.rd_id == "NEW001"

    async def test_normalises_hash_to_lowercase(self, session: AsyncSession) -> None:
        """Uppercase info_hash is stored as lowercase."""
        engine = DedupEngine()
        torrent = await engine.register_torrent(
            session,
            rd_id="NEW002",
            info_hash="EEEEEEEE" * 5,
            magnet_uri=None,
            media_item_id=None,
            filename="Another.Movie.1080p.mkv",
            filesize=1_000_000_000,
            resolution="1080p",
            cached=False,
        )
        assert torrent.info_hash == "eeeeeeee" * 5

    async def test_accepts_none_hash(self, session: AsyncSession) -> None:
        """None info_hash is stored as None without error."""
        engine = DedupEngine()
        torrent = await engine.register_torrent(
            session,
            rd_id="NEW003",
            info_hash=None,
            magnet_uri=None,
            media_item_id=None,
            filename="Unknown.Movie.mkv",
            filesize=0,
            resolution=None,
            cached=None,
        )
        assert torrent.info_hash is None
        assert torrent.status == TorrentStatus.ACTIVE

    async def test_links_to_media_item(self, session: AsyncSession) -> None:
        """Supplied media_item_id is persisted on the new record."""
        engine = DedupEngine()
        media = await _make_media_item(session, imdb_id="tt0000001")
        torrent = await engine.register_torrent(
            session,
            rd_id="NEW004",
            info_hash="ffffffff" * 5,
            magnet_uri=None,
            media_item_id=media.id,
            filename="Linked.Movie.1080p.mkv",
            filesize=500_000_000,
            resolution="1080p",
            cached=True,
        )
        assert torrent.media_item_id == media.id

    async def test_logs_at_info_level(
        self, session: AsyncSession, caplog: pytest.LogCaptureFixture
    ) -> None:
        """An INFO log entry is emitted after registration."""
        engine = DedupEngine()
        with caplog.at_level(logging.INFO, logger="src.core.dedup"):
            await engine.register_torrent(
                session,
                rd_id="LOG001",
                info_hash="12345678" * 5,
                magnet_uri=None,
                media_item_id=None,
                filename="Logged.Movie.mkv",
                filesize=0,
                resolution=None,
                cached=None,
            )
        assert any("register_torrent" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Group 4: mark_torrent_removed
# ---------------------------------------------------------------------------


class TestMarkTorrentRemoved:
    """Tests for DedupEngine.mark_torrent_removed."""

    async def test_sets_status_to_removed(self, session: AsyncSession) -> None:
        """ACTIVE torrent status is updated to REMOVED."""
        engine = DedupEngine()
        torrent = await _make_rd_torrent(
            session, info_hash="aabbccdd" * 5, status=TorrentStatus.ACTIVE
        )
        await engine.mark_torrent_removed(session, "aabbccdd" * 5)
        await session.refresh(torrent)
        assert torrent.status == TorrentStatus.REMOVED

    async def test_case_insensitive_hash(self, session: AsyncSession) -> None:
        """Hash lookup is case-insensitive."""
        engine = DedupEngine()
        torrent = await _make_rd_torrent(
            session, info_hash="aaccbbdd" * 5, status=TorrentStatus.ACTIVE
        )
        await engine.mark_torrent_removed(session, "AACCBBDD" * 5)
        await session.refresh(torrent)
        assert torrent.status == TorrentStatus.REMOVED

    async def test_noop_when_hash_not_found(
        self, session: AsyncSession, caplog: pytest.LogCaptureFixture
    ) -> None:
        """No exception when hash is not in the registry; a warning is logged."""
        engine = DedupEngine()
        with caplog.at_level(logging.WARNING, logger="src.core.dedup"):
            # Should not raise
            await engine.mark_torrent_removed(session, "00000000" * 5)
        assert any("mark_torrent_removed" in r.message for r in caplog.records)

    async def test_logs_status_change(
        self, session: AsyncSession, caplog: pytest.LogCaptureFixture
    ) -> None:
        """An INFO log entry records the status transition."""
        engine = DedupEngine()
        await _make_rd_torrent(
            session, info_hash="ddddeeee" * 5, status=TorrentStatus.ACTIVE
        )
        with caplog.at_level(logging.INFO, logger="src.core.dedup"):
            await engine.mark_torrent_removed(session, "ddddeeee" * 5)
        assert any("mark_torrent_removed" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Group 5: mark_torrent_replaced
# ---------------------------------------------------------------------------


class TestMarkTorrentReplaced:
    """Tests for DedupEngine.mark_torrent_replaced."""

    async def test_sets_status_to_replaced(self, session: AsyncSession) -> None:
        """Old torrent status is updated to REPLACED."""
        engine = DedupEngine()
        old_torrent = await _make_rd_torrent(
            session, info_hash="oldold00" * 5, status=TorrentStatus.ACTIVE
        )
        await engine.mark_torrent_replaced(
            session,
            old_info_hash="oldold00" * 5,
            new_info_hash="newnew00" * 5,
        )
        await session.refresh(old_torrent)
        assert old_torrent.status == TorrentStatus.REPLACED

    async def test_case_insensitive_old_hash(self, session: AsyncSession) -> None:
        """Old hash lookup is case-insensitive."""
        engine = DedupEngine()
        old_torrent = await _make_rd_torrent(
            session, info_hash="oldold11" * 5, status=TorrentStatus.ACTIVE
        )
        await engine.mark_torrent_replaced(
            session,
            old_info_hash="OLDOLD11" * 5,
            new_info_hash="newnew11" * 5,
        )
        await session.refresh(old_torrent)
        assert old_torrent.status == TorrentStatus.REPLACED

    async def test_noop_when_old_hash_not_found(
        self, session: AsyncSession, caplog: pytest.LogCaptureFixture
    ) -> None:
        """No exception when old hash is missing; a warning is logged."""
        engine = DedupEngine()
        with caplog.at_level(logging.WARNING, logger="src.core.dedup"):
            await engine.mark_torrent_replaced(
                session,
                old_info_hash="missing0" * 5,
                new_info_hash="newnew22" * 5,
            )
        assert any("mark_torrent_replaced" in r.message for r in caplog.records)

    async def test_logs_both_hashes(
        self, session: AsyncSession, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Log message records both old and new hashes."""
        engine = DedupEngine()
        await _make_rd_torrent(
            session, info_hash="oldold33" * 5, status=TorrentStatus.ACTIVE
        )
        with caplog.at_level(logging.INFO, logger="src.core.dedup"):
            await engine.mark_torrent_replaced(
                session,
                old_info_hash="oldold33" * 5,
                new_info_hash="newnew33" * 5,
            )
        combined = " ".join(r.message for r in caplog.records)
        assert "oldold33" in combined
        assert "newnew33" in combined


# ---------------------------------------------------------------------------
# Group 6: find_account_duplicates
# ---------------------------------------------------------------------------


class TestFindAccountDuplicates:
    """Tests for DedupEngine.find_account_duplicates."""

    async def test_returns_empty_when_no_data(self, session: AsyncSession) -> None:
        """Empty input list produces no duplicate groups."""
        engine = DedupEngine()
        groups = await engine.find_account_duplicates(session, [])
        assert groups == []

    async def test_single_torrent_not_a_duplicate(
        self, session: AsyncSession
    ) -> None:
        """A single torrent for a title is not returned as a duplicate group."""
        engine = DedupEngine()
        rd_data = [
            _make_rd_dict(
                filename="Breaking.Bad.S05E01.1080p.WEB-DL.mkv",
                info_hash="aa" * 20,
                rd_id="RD001",
            )
        ]
        groups = await engine.find_account_duplicates(session, rd_data)
        assert groups == []

    async def test_two_copies_of_movie_detected(self, session: AsyncSession) -> None:
        """Two torrents with the same normalised title are grouped as duplicates."""
        engine = DedupEngine()
        rd_data = [
            _make_rd_dict(
                filename="The.Dark.Knight.2008.1080p.BluRay.mkv",
                info_hash="bb" * 20,
                rd_id="RD001",
            ),
            _make_rd_dict(
                filename="The.Dark.Knight.2008.2160p.UHD.BluRay.mkv",
                info_hash="cc" * 20,
                rd_id="RD002",
            ),
        ]
        groups = await engine.find_account_duplicates(session, rd_data)
        assert len(groups) == 1
        group = groups[0]
        assert len(group.torrents) == 2

    async def test_two_copies_of_same_episode(self, session: AsyncSession) -> None:
        """Two torrents for the same episode are grouped."""
        engine = DedupEngine()
        rd_data = [
            _make_rd_dict(
                filename="Breaking.Bad.S05E01.1080p.WEB-DL.mkv",
                info_hash="dd" * 20,
                rd_id="RD001",
            ),
            _make_rd_dict(
                filename="Breaking.Bad.S05E01.720p.HDTV.mkv",
                info_hash="ee" * 20,
                rd_id="RD002",
            ),
        ]
        groups = await engine.find_account_duplicates(session, rd_data)
        assert len(groups) == 1
        assert groups[0].season == 5
        assert groups[0].episode == 1

    async def test_different_episodes_not_grouped(self, session: AsyncSession) -> None:
        """Torrents for different episodes are not considered duplicates."""
        engine = DedupEngine()
        rd_data = [
            _make_rd_dict(
                filename="Breaking.Bad.S05E01.1080p.mkv",
                info_hash="ff" * 20,
                rd_id="RD001",
            ),
            _make_rd_dict(
                filename="Breaking.Bad.S05E02.1080p.mkv",
                info_hash="gg" * 20,
                rd_id="RD002",
            ),
        ]
        groups = await engine.find_account_duplicates(session, rd_data)
        assert groups == []

    async def test_groups_sorted_by_title(self, session: AsyncSession) -> None:
        """Returned groups are sorted alphabetically by title (case-insensitive)."""
        engine = DedupEngine()
        rd_data = [
            _make_rd_dict(
                filename="Zorro.2024.1080p.mkv", info_hash="11" * 20, rd_id="RD001"
            ),
            _make_rd_dict(
                filename="Zorro.2024.2160p.mkv", info_hash="22" * 20, rd_id="RD002"
            ),
            _make_rd_dict(
                filename="Amelie.2001.1080p.mkv", info_hash="33" * 20, rd_id="RD003"
            ),
            _make_rd_dict(
                filename="Amelie.2001.720p.mkv", info_hash="44" * 20, rd_id="RD004"
            ),
        ]
        groups = await engine.find_account_duplicates(session, rd_data)
        assert len(groups) == 2
        assert groups[0].title.lower() < groups[1].title.lower()
        assert "amelie" in groups[0].title.lower()

    async def test_resolves_imdb_id_from_local_registry(
        self, session: AsyncSession
    ) -> None:
        """IMDB ID is pulled from the local rd_torrents registry when available."""
        engine = DedupEngine()
        media = await _make_media_item(session, imdb_id="tt1111999")
        # Register one of the hashes in the local registry
        await _make_rd_torrent(
            session,
            info_hash="55" * 20,
            media_item_id=media.id,
        )
        rd_data = [
            _make_rd_dict(
                filename="Inception.2010.1080p.BluRay.mkv",
                info_hash="55" * 20,
                rd_id="RD001",
            ),
            _make_rd_dict(
                filename="Inception.2010.2160p.BluRay.mkv",
                info_hash="66" * 20,
                rd_id="RD002",
            ),
        ]
        groups = await engine.find_account_duplicates(session, rd_data)
        assert len(groups) == 1
        assert groups[0].imdb_id == "tt1111999"

    async def test_uses_title_placeholder_when_no_local_record(
        self, session: AsyncSession
    ) -> None:
        """Placeholder IMDB ID (normalised title) is used when no local record exists."""
        engine = DedupEngine()
        rd_data = [
            _make_rd_dict(
                filename="Parasite.2019.1080p.BluRay.mkv",
                info_hash="77" * 20,
                rd_id="RD001",
            ),
            _make_rd_dict(
                filename="Parasite.2019.2160p.mkv",
                info_hash="88" * 20,
                rd_id="RD002",
            ),
        ]
        groups = await engine.find_account_duplicates(session, rd_data)
        assert len(groups) == 1
        # imdb_id is the normalised title placeholder (PTN strips the year from the
        # title field), not a real tt ID
        assert groups[0].imdb_id == "parasite"

    async def test_duplicate_entry_fields(self, session: AsyncSession) -> None:
        """DuplicateEntry fields are correctly populated from the RD dict."""
        engine = DedupEngine()
        rd_data = [
            _make_rd_dict(
                filename="Dune.2021.1080p.WEB-DL.mkv",
                info_hash="99" * 20,
                rd_id="RD001",
                size_bytes=5_000_000_000,
                added="2024-01-10T08:00:00.000Z",
            ),
            _make_rd_dict(
                filename="Dune.2021.2160p.UHD.BluRay.mkv",
                info_hash="aa" * 20,
                rd_id="RD002",
                size_bytes=30_000_000_000,
                added="2024-01-12T12:00:00.000Z",
            ),
        ]
        groups = await engine.find_account_duplicates(session, rd_data)
        assert len(groups) == 1
        entries = {e.rd_id: e for e in groups[0].torrents}

        entry_1 = entries["RD001"]
        assert entry_1.info_hash == "99" * 20
        assert entry_1.filesize == 5_000_000_000
        assert entry_1.added == "2024-01-10T08:00:00.000Z"

        entry_2 = entries["RD002"]
        assert entry_2.filesize == 30_000_000_000

    async def test_resolution_populated_from_ptn(self, session: AsyncSession) -> None:
        """DuplicateEntry.resolution is set from PTN parse of the filename."""
        engine = DedupEngine()
        rd_data = [
            _make_rd_dict(
                filename="Arrival.2016.1080p.BluRay.mkv",
                info_hash="ab" * 20,
                rd_id="RD001",
            ),
            _make_rd_dict(
                filename="Arrival.2016.2160p.BluRay.mkv",
                info_hash="cd" * 20,
                rd_id="RD002",
            ),
        ]
        groups = await engine.find_account_duplicates(session, rd_data)
        assert len(groups) == 1
        resolutions = {e.resolution for e in groups[0].torrents}
        assert "1080p" in resolutions
        assert "2160p" in resolutions

    async def test_skips_entry_with_empty_filename(
        self, session: AsyncSession
    ) -> None:
        """Entries with empty or missing filenames are silently skipped."""
        engine = DedupEngine()
        rd_data = [
            {"id": "RD001", "filename": "", "hash": "ef" * 20, "bytes": 0},
            _make_rd_dict(
                filename="Memento.2000.1080p.mkv",
                info_hash="ef" * 20,
                rd_id="RD002",
            ),
        ]
        # Only one real entry — no duplicate group should be formed
        groups = await engine.find_account_duplicates(session, rd_data)
        assert groups == []

    async def test_logs_summary_at_info_level(
        self, session: AsyncSession, caplog: pytest.LogCaptureFixture
    ) -> None:
        """An INFO log entry is emitted with the scan summary."""
        engine = DedupEngine()
        with caplog.at_level(logging.INFO, logger="src.core.dedup"):
            await engine.find_account_duplicates(session, [])
        assert any("find_account_duplicates" in r.message for r in caplog.records)

    async def test_three_copies_all_in_one_group(
        self, session: AsyncSession
    ) -> None:
        """Three copies of the same content produce a single group with three entries."""
        engine = DedupEngine()
        rd_data = [
            _make_rd_dict(
                filename="Interstellar.2014.720p.mkv",
                info_hash="aa" * 20,
                rd_id="RD001",
            ),
            _make_rd_dict(
                filename="Interstellar.2014.1080p.mkv",
                info_hash="bb" * 20,
                rd_id="RD002",
            ),
            _make_rd_dict(
                filename="Interstellar.2014.2160p.mkv",
                info_hash="cc" * 20,
                rd_id="RD003",
            ),
        ]
        groups = await engine.find_account_duplicates(session, rd_data)
        assert len(groups) == 1
        assert len(groups[0].torrents) == 3

    async def test_info_hash_normalised_to_lowercase_in_entry(
        self, session: AsyncSession
    ) -> None:
        """DuplicateEntry.info_hash is stored as lowercase regardless of RD response."""
        engine = DedupEngine()
        rd_data = [
            _make_rd_dict(
                filename="Blade.Runner.2049.1080p.mkv",
                info_hash="AABB" * 10,  # uppercase from RD
                rd_id="RD001",
            ),
            _make_rd_dict(
                filename="Blade.Runner.2049.2160p.mkv",
                info_hash="CCDD" * 10,
                rd_id="RD002",
            ),
        ]
        groups = await engine.find_account_duplicates(session, rd_data)
        assert len(groups) == 1
        for entry in groups[0].torrents:
            assert entry.info_hash == entry.info_hash.lower()

    async def test_multiple_distinct_duplicate_groups(
        self, session: AsyncSession
    ) -> None:
        """Multiple unrelated duplicate pairs produce separate groups."""
        engine = DedupEngine()
        rd_data = [
            _make_rd_dict(
                filename="Movie.A.2020.1080p.mkv", info_hash="11" * 20, rd_id="RD001"
            ),
            _make_rd_dict(
                filename="Movie.A.2020.2160p.mkv", info_hash="22" * 20, rd_id="RD002"
            ),
            _make_rd_dict(
                filename="Movie.B.2021.1080p.mkv", info_hash="33" * 20, rd_id="RD003"
            ),
            _make_rd_dict(
                filename="Movie.B.2021.2160p.mkv", info_hash="44" * 20, rd_id="RD004"
            ),
        ]
        groups = await engine.find_account_duplicates(session, rd_data)
        assert len(groups) == 2
        titles = {g.title for g in groups}
        assert len(titles) == 2


# ---------------------------------------------------------------------------
# Group 7: Module-level singleton
# ---------------------------------------------------------------------------


class TestModuleSingleton:
    """Tests for the module-level dedup_engine singleton."""

    def test_singleton_is_dedup_engine(self) -> None:
        """dedup_engine is a DedupEngine instance."""
        assert isinstance(dedup_engine, DedupEngine)

    def test_singleton_is_same_object(self) -> None:
        """Importing dedup_engine twice yields the same object."""
        from src.core.dedup import dedup_engine as engine2

        assert dedup_engine is engine2
