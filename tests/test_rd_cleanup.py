"""Tests for src/core/rd_cleanup.py.

Coverage
--------
Helper functions (pure, no mocking):
  _normalize_title — strips non-alphanumeric, lowercases
  _parse_filename  — PTN wrapper that returns {} on failure
  _parse_added     — ISO timestamp parser, handles Z suffix, +00:00, None, garbage, naive
  _build_ptn_groups — groups RD torrent dicts by (norm_title, season, episode)
  _build_summaries  — per-category count + byte totals, all categories present

Categorization (_categorize_torrent):
  Protected: via active_hashes, active_rd_ids, symlink exact match, symlink normalized match
  Dead: all four _DEAD_STATUSES values
  Stale: downloading/magnet_conversion >7 days; NOT stale <7 days or no date
  Duplicate: same PTN group as Protected; protected_by set; group with no Protected stays Orphaned
  Orphaned: default fallthrough
  Priority: Protected overrides Dead; Dead overrides Duplicate

_categorize_all (two-pass):
  Mixed list → correct distribution
  Two-pass: Duplicate detected even when Protected member appears after it in list
  Empty input → empty results

scan_rd_account (mock RD client + DB):
  Empty account → total=0, all summaries zero
  Mixed categories with DB fixtures → correct counts
  Cache populated after scan
  Protection set failure → warning emitted, scan still completes
  RD API failure → exception propagated

execute_rd_cleanup (mock RD client + DB):
  Delete single Dead torrent
  Delete multiple torrents
  Reject Protected torrent
  Reject unknown rd_id
  Mixed request: deletable + protected + unknown
  Rate limit → stop_flag triggers, rate_limited=True
  Delete error → failed incremented, rest of list continues
  Empty rd_ids → no-op
  Cache reuse: fresh cache skips list_all_torrents
  Cache expired: triggers fresh list_all_torrents
  Successful deletion → mark_torrent_removed called
  Successful deletion → cache invalidated
  Torrent not in rd_torrents table → deleted count correct, no error

asyncio_mode = "auto" (configured in pyproject.toml).
"""

from __future__ import annotations

import importlib
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

import src.core.rd_cleanup as rd_cleanup_module
from src.core.rd_cleanup import (
    CategorizedTorrent,
    RdCleanupExecuteRequest,
    RdTorrentCategory,
    _build_ptn_groups,
    _build_summaries,
    _categorize_all,
    _categorize_torrent,
    _last_scan_cache,
    _normalize_title,
    _parse_added,
    _parse_filename,
    execute_rd_cleanup,
    scan_rd_account,
)
from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.symlink import Symlink
from src.models.torrent import RdTorrent, TorrentStatus


# ---------------------------------------------------------------------------
# Constants used across tests
# ---------------------------------------------------------------------------

ZURG_MOUNT = "/mnt/zurg/__all__"

_NOW = datetime.now(tz=timezone.utc)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_scan_cache():
    """Reset the module-level scan cache before every test."""
    _last_scan_cache["scanned_at"] = None
    _last_scan_cache["rd_torrents"] = None
    _last_scan_cache["category_map"] = None
    _last_scan_cache["hash_map"] = None
    yield
    # Clean up after test too
    _last_scan_cache["scanned_at"] = None
    _last_scan_cache["rd_torrents"] = None
    _last_scan_cache["category_map"] = None
    _last_scan_cache["hash_map"] = None


def _rd(
    rd_id: str = "RDID001",
    filename: str = "Test.Movie.2024.1080p.BluRay-GROUP",
    info_hash: str = "aabbccdd0011223344556677889900aabbccddee",
    status: str = "downloaded",
    added: str | None = "2024-01-15T10:30:00.000Z",
    filesize: int = 5_000_000_000,
) -> dict[str, Any]:
    """Helper to build a minimal RD torrent dict."""
    return {
        "id": rd_id,
        "filename": filename,
        "hash": info_hash,
        "bytes": filesize,
        "status": status,
        "added": added,
    }


def _make_active_rd_torrent(
    session: AsyncSession,
    *,
    rd_id: str = "RDID001",
    info_hash: str = "aabbccdd0011223344556677889900aabbccddee",
    filename: str = "Test.Movie.2024.1080p",
) -> RdTorrent:
    """Create and add an ACTIVE RdTorrent to the session (not flushed)."""
    torrent = RdTorrent(
        rd_id=rd_id,
        info_hash=info_hash.lower(),
        filename=filename,
        filesize=2_000_000_000,
        status=TorrentStatus.ACTIVE,
    )
    session.add(torrent)
    return torrent


def _make_symlink(
    session: AsyncSession,
    *,
    source_path: str,
    target_path: str = "/library/movies/Test Movie (2024)/Test Movie (2024).mkv",
) -> Symlink:
    """Create and add a Symlink to the session (not flushed)."""
    sym = Symlink(
        source_path=source_path,
        target_path=target_path,
        valid=True,
    )
    session.add(sym)
    return sym


def _empty_protection_sets():
    """Return empty protection sets for tests that don't need DB protection."""
    return set(), set(), set()


def _build_categorize_args(
    active_hashes: set[str] | None = None,
    active_rd_ids: set[str] | None = None,
    symlink_mount_names: set[str] | None = None,
    protected_rd_ids: set[str] | None = None,
    ptn_groups: dict | None = None,
    rd_id_to_filename: dict | None = None,
) -> dict:
    """Build keyword args for _categorize_torrent with sensible defaults."""
    return dict(
        active_hashes=active_hashes or set(),
        active_rd_ids=active_rd_ids or set(),
        symlink_mount_names=symlink_mount_names or set(),
        protected_rd_ids=protected_rd_ids or set(),
        ptn_groups=ptn_groups or {},
        rd_id_to_filename=rd_id_to_filename or {},
    )


# ---------------------------------------------------------------------------
# 1. _normalize_title — pure function tests
# ---------------------------------------------------------------------------


class TestNormalizeTitle:
    def test_strips_spaces_and_lowercases(self):
        assert _normalize_title("Breaking Bad") == "breakingbad"

    def test_strips_dots_and_underscores(self):
        assert _normalize_title("Breaking.Bad_S01") == "breakingbads01"

    def test_empty_string_returns_empty(self):
        assert _normalize_title("") == ""

    def test_all_non_alnum(self):
        assert _normalize_title("... ---") == ""

    def test_numbers_preserved(self):
        assert _normalize_title("The 100") == "the100"

    def test_already_normalized(self):
        assert _normalize_title("breakingbad") == "breakingbad"


# ---------------------------------------------------------------------------
# 2. _parse_filename — PTN wrapper
# ---------------------------------------------------------------------------


class TestParseFilename:
    def test_valid_filename_returns_title(self):
        result = _parse_filename("Breaking.Bad.S01E01.1080p.BluRay-GROUP")
        assert isinstance(result, dict)
        assert result.get("title") is not None

    def test_garbage_returns_empty_dict(self):
        # PTN.parse should not raise; we get an empty or minimal dict back
        result = _parse_filename("ÿÿÿÿ\x00\x01\x02")
        assert isinstance(result, dict)

    def test_empty_string_returns_dict(self):
        result = _parse_filename("")
        assert isinstance(result, dict)

    def test_does_not_raise_on_exception(self):
        # Patch PTN.parse to raise unexpectedly — should return {}
        with patch("src.core.rd_cleanup.PTN.parse", side_effect=RuntimeError("boom")):
            result = _parse_filename("anything")
        assert result == {}


# ---------------------------------------------------------------------------
# 3. _parse_added — timestamp parser
# ---------------------------------------------------------------------------


class TestParseAdded:
    def test_z_suffix_parses_correctly(self):
        dt = _parse_added("2024-01-15T10:30:00.000Z")
        assert dt is not None
        assert dt.tzinfo is not None
        assert dt.year == 2024
        assert dt.month == 1
        assert dt.day == 15

    def test_plus_offset_parses_correctly(self):
        dt = _parse_added("2024-06-20T08:00:00.000+00:00")
        assert dt is not None
        assert dt.tzinfo is not None
        assert dt.year == 2024

    def test_none_returns_none(self):
        assert _parse_added(None) is None

    def test_empty_string_returns_none(self):
        assert _parse_added("") is None

    def test_garbage_returns_none(self):
        assert _parse_added("not-a-date") is None

    def test_naive_datetime_gets_utc_attached(self):
        # Construct a string that fromisoformat parses as naive
        dt = _parse_added("2024-01-15T10:30:00")
        assert dt is not None
        assert dt.tzinfo == timezone.utc

    def test_result_is_utc_aware(self):
        dt = _parse_added("2024-03-01T00:00:00.000Z")
        assert dt.tzinfo is not None
        assert dt.utcoffset().total_seconds() == 0


# ---------------------------------------------------------------------------
# 4. _build_ptn_groups
# ---------------------------------------------------------------------------


class TestBuildPtnGroups:
    def test_groups_same_title_season_episode(self):
        t1 = _rd("ID1", "Breaking.Bad.S01E01.720p")
        t2 = _rd("ID2", "Breaking.Bad.S01E01.1080p")
        groups = _build_ptn_groups([t1, t2])
        # Both should land in the same key
        found = any(ids for ids in groups.values() if "ID1" in ids and "ID2" in ids)
        assert found, f"Expected ID1 and ID2 in same group, got {groups}"

    def test_different_episodes_different_groups(self):
        t1 = _rd("ID1", "Breaking.Bad.S01E01.720p")
        t2 = _rd("ID2", "Breaking.Bad.S01E02.720p")
        groups = _build_ptn_groups([t1, t2])
        all_ids = [rid for ids in groups.values() for rid in ids]
        # Each ID present exactly once, in separate groups
        assert all_ids.count("ID1") == 1
        assert all_ids.count("ID2") == 1
        # They must NOT share a group
        shared = any("ID1" in ids and "ID2" in ids for ids in groups.values())
        assert not shared

    def test_skips_empty_filename(self):
        t = {"id": "ID1", "filename": "", "hash": "aaa", "bytes": 0, "status": "downloaded", "added": None}
        groups = _build_ptn_groups([t])
        all_ids = [rid for ids in groups.values() for rid in ids]
        assert "ID1" not in all_ids

    def test_skips_none_filename(self):
        t = {"id": "ID1", "filename": None, "hash": "aaa", "bytes": 0, "status": "downloaded", "added": None}
        groups = _build_ptn_groups([t])
        all_ids = [rid for ids in groups.values() for rid in ids]
        assert "ID1" not in all_ids

    def test_skips_when_ptn_cant_extract_title(self):
        # Patch _parse_filename to return a dict without 'title' key, simulating
        # a torrent whose filename PTN cannot extract a title from.
        t = _rd("ID1", "some.file.mkv")
        with patch("src.core.rd_cleanup._parse_filename", return_value={}):
            groups = _build_ptn_groups([t])
        all_ids = [rid for ids in groups.values() for rid in ids]
        assert "ID1" not in all_ids

    def test_empty_list_returns_empty(self):
        assert _build_ptn_groups([]) == {}


# ---------------------------------------------------------------------------
# 5. _build_summaries
# ---------------------------------------------------------------------------


class TestBuildSummaries:
    def _make_ct(self, rd_id: str, category: RdTorrentCategory, filesize: int) -> CategorizedTorrent:
        return CategorizedTorrent(
            rd_id=rd_id,
            info_hash="aabbcc",
            filename="Test.File",
            filesize=filesize,
            status="downloaded",
            category=category,
            reason="test",
        )

    def test_correct_counts_and_bytes(self):
        torrents = [
            self._make_ct("ID1", RdTorrentCategory.DEAD, 1_000),
            self._make_ct("ID2", RdTorrentCategory.DEAD, 2_000),
            self._make_ct("ID3", RdTorrentCategory.ORPHANED, 5_000),
        ]
        summaries = _build_summaries(torrents)
        dead_summary = next(s for s in summaries if s.category == RdTorrentCategory.DEAD)
        orphan_summary = next(s for s in summaries if s.category == RdTorrentCategory.ORPHANED)
        assert dead_summary.count == 2
        assert dead_summary.total_bytes == 3_000
        assert orphan_summary.count == 1
        assert orphan_summary.total_bytes == 5_000

    def test_all_categories_present_when_empty(self):
        summaries = _build_summaries([])
        categories = {s.category for s in summaries}
        assert categories == set(RdTorrentCategory)

    def test_zero_counts_for_empty_categories(self):
        torrents = [self._make_ct("ID1", RdTorrentCategory.PROTECTED, 0)]
        summaries = _build_summaries(torrents)
        for s in summaries:
            if s.category != RdTorrentCategory.PROTECTED:
                assert s.count == 0
                assert s.total_bytes == 0


# ---------------------------------------------------------------------------
# 6. _categorize_torrent — categorization logic
# ---------------------------------------------------------------------------


class TestCategorizeTorrentProtected:
    def test_protected_by_info_hash(self):
        rd = _rd("ID1", info_hash="aabbccddeeff00112233445566778899aabbccdd")
        result = _categorize_torrent(
            rd,
            **_build_categorize_args(
                active_hashes={"aabbccddeeff00112233445566778899aabbccdd"},
            ),
        )
        assert result.category == RdTorrentCategory.PROTECTED
        assert "info_hash" in result.reason

    def test_protected_by_rd_id(self):
        rd = _rd("MYRDI", info_hash="deadbeef" * 5)
        result = _categorize_torrent(
            rd,
            **_build_categorize_args(
                active_rd_ids={"MYRDI"},
            ),
        )
        assert result.category == RdTorrentCategory.PROTECTED
        assert "rd_id" in result.reason

    def test_protected_by_symlink_exact_match(self):
        rd = _rd("ID1", filename="Breaking.Bad.S01.BluRay-GROUP")
        result = _categorize_torrent(
            rd,
            **_build_categorize_args(
                symlink_mount_names={"breaking.bad.s01.bluray-group"},
            ),
        )
        assert result.category == RdTorrentCategory.PROTECTED
        assert "symlink" in result.reason.lower()

    def test_protected_by_symlink_normalized_match(self):
        rd = _rd("ID1", filename="Breaking.Bad.S01.BluRay-GROUP")
        # _normalize_name converts dots/underscores to spaces/collapses them
        # The protected set should contain the normalized variant
        from src.core.rd_bridge import _normalize_name
        normalized = _normalize_name("Breaking.Bad.S01.BluRay-GROUP").lower()
        result = _categorize_torrent(
            rd,
            **_build_categorize_args(
                symlink_mount_names={normalized},
            ),
        )
        assert result.category == RdTorrentCategory.PROTECTED


class TestCategorizeTorrentDead:
    @pytest.mark.parametrize("dead_status", ["error", "virus", "dead", "magnet_error"])
    def test_dead_status(self, dead_status: str):
        rd = _rd("ID1", status=dead_status)
        result = _categorize_torrent(rd, **_build_categorize_args())
        assert result.category == RdTorrentCategory.DEAD
        assert dead_status in result.reason


class TestCategorizeTorrentStale:
    def test_stale_downloading_over_threshold(self):
        added_old = (_NOW - timedelta(days=8)).isoformat()
        rd = _rd("ID1", status="downloading", added=added_old)
        result = _categorize_torrent(rd, **_build_categorize_args())
        assert result.category == RdTorrentCategory.STALE

    def test_stale_magnet_conversion_over_threshold(self):
        added_old = (_NOW - timedelta(days=10)).isoformat()
        rd = _rd("ID1", status="magnet_conversion", added=added_old)
        result = _categorize_torrent(rd, **_build_categorize_args())
        assert result.category == RdTorrentCategory.STALE

    def test_not_stale_downloading_under_threshold(self):
        added_recent = (_NOW - timedelta(days=3)).isoformat()
        rd = _rd("ID1", status="downloading", added=added_recent)
        result = _categorize_torrent(rd, **_build_categorize_args())
        # Falls through to Orphaned (not stale)
        assert result.category == RdTorrentCategory.ORPHANED

    def test_not_stale_when_no_added_date(self):
        rd = _rd("ID1", status="downloading", added=None)
        result = _categorize_torrent(rd, **_build_categorize_args())
        # No date — cannot confirm stale, becomes Orphaned
        assert result.category == RdTorrentCategory.ORPHANED


class TestCategorizeTorrentDuplicate:
    def test_duplicate_when_group_has_protected_member(self):
        dup_filename = "Breaking.Bad.S01E01.720p.BluRay-GROUP"
        prot_filename = "Breaking.Bad.S01E01.1080p.BluRay-OTHER"
        dup_rd = _rd("ID_DUP", filename=dup_filename)
        prot_rd = _rd("ID_PROT", filename=prot_filename)
        ptn_groups = _build_ptn_groups([dup_rd, prot_rd])
        rd_id_to_filename = {"ID_DUP": dup_filename, "ID_PROT": prot_filename}

        result = _categorize_torrent(
            dup_rd,
            **_build_categorize_args(
                protected_rd_ids={"ID_PROT"},
                ptn_groups=ptn_groups,
                rd_id_to_filename=rd_id_to_filename,
            ),
        )
        assert result.category == RdTorrentCategory.DUPLICATE
        assert result.protected_by == prot_filename

    def test_duplicate_protected_by_field_set_to_protected_filename(self):
        dup_filename = "The.Wire.S02E05.720p"
        prot_filename = "The.Wire.S02E05.1080p.AMZN"
        dup_rd = _rd("ID_DUP", filename=dup_filename)
        prot_rd = _rd("ID_PROT", filename=prot_filename)
        ptn_groups = _build_ptn_groups([dup_rd, prot_rd])
        rd_id_to_filename = {"ID_DUP": dup_filename, "ID_PROT": prot_filename}

        result = _categorize_torrent(
            dup_rd,
            **_build_categorize_args(
                protected_rd_ids={"ID_PROT"},
                ptn_groups=ptn_groups,
                rd_id_to_filename=rd_id_to_filename,
            ),
        )
        assert result.protected_by == prot_filename

    def test_not_duplicate_when_group_has_no_protected_member(self):
        dup_filename = "Breaking.Bad.S01E01.720p.BluRay-GROUP"
        other_filename = "Breaking.Bad.S01E01.1080p.BluRay-OTHER"
        dup_rd = _rd("ID_A", filename=dup_filename)
        other_rd = _rd("ID_B", filename=other_filename)
        ptn_groups = _build_ptn_groups([dup_rd, other_rd])
        rd_id_to_filename = {"ID_A": dup_filename, "ID_B": other_filename}

        result = _categorize_torrent(
            dup_rd,
            **_build_categorize_args(
                protected_rd_ids=set(),  # no protected members
                ptn_groups=ptn_groups,
                rd_id_to_filename=rd_id_to_filename,
            ),
        )
        # No protected member in group → Orphaned, not Duplicate
        assert result.category == RdTorrentCategory.ORPHANED


class TestCategorizeTorrentOrphaned:
    def test_orphaned_by_default(self):
        rd = _rd("ID1", status="downloaded")
        result = _categorize_torrent(rd, **_build_categorize_args())
        assert result.category == RdTorrentCategory.ORPHANED

    def test_orphaned_reason_mentions_no_registry(self):
        rd = _rd("ID1", status="downloaded")
        result = _categorize_torrent(rd, **_build_categorize_args())
        assert "no matching" in result.reason.lower()


class TestCategorizeTorrentPriority:
    def test_protected_overrides_dead_status(self):
        """A torrent with dead status AND a matching hash is PROTECTED, not DEAD."""
        rd = _rd("ID1", status="error", info_hash="deadhash00" * 4)
        result = _categorize_torrent(
            rd,
            **_build_categorize_args(
                active_hashes={"deadhash00" * 4},
            ),
        )
        assert result.category == RdTorrentCategory.PROTECTED

    def test_dead_overrides_duplicate_membership(self):
        """A dead torrent in a duplicate group is categorized as DEAD, not DUPLICATE."""
        dead_filename = "Breaking.Bad.S01E01.720p.BluRay-GROUP"
        prot_filename = "Breaking.Bad.S01E01.1080p.BluRay-OTHER"
        dead_rd = _rd("ID_DEAD", filename=dead_filename, status="error")
        prot_rd = _rd("ID_PROT", filename=prot_filename, status="downloaded")
        ptn_groups = _build_ptn_groups([dead_rd, prot_rd])
        rd_id_to_filename = {"ID_DEAD": dead_filename, "ID_PROT": prot_filename}

        result = _categorize_torrent(
            dead_rd,
            **_build_categorize_args(
                protected_rd_ids={"ID_PROT"},
                ptn_groups=ptn_groups,
                rd_id_to_filename=rd_id_to_filename,
            ),
        )
        assert result.category == RdTorrentCategory.DEAD


# ---------------------------------------------------------------------------
# 7. _categorize_all — two-pass tests
# ---------------------------------------------------------------------------


class TestCategorizeAll:
    def test_mixed_list_correct_distribution(self):
        torrents = [
            _rd("PROT1", info_hash="a" * 40),
            _rd("DEAD1", status="error"),
            _rd("ORPH1", status="downloaded", filename="Some.Unique.Movie.2020.BluRay"),
        ]
        categorized, category_map, hash_map = _categorize_all(
            torrents,
            active_hashes={"a" * 40},
            active_rd_ids=set(),
            symlink_mount_names=set(),
        )
        assert category_map["PROT1"] == RdTorrentCategory.PROTECTED
        assert category_map["DEAD1"] == RdTorrentCategory.DEAD
        assert category_map["ORPH1"] == RdTorrentCategory.ORPHANED

    def test_two_pass_detects_duplicate_even_when_protected_appears_after(self):
        """Protected member listed AFTER the potential duplicate — two-pass catches it."""
        dup_filename = "Breaking.Bad.S01E01.720p.BluRay-GROUP"
        prot_filename = "Breaking.Bad.S01E01.1080p.BluRay-OTHER"
        dup_rd = _rd("ID_DUP", filename=dup_filename, info_hash="b" * 40)
        prot_rd = _rd("ID_PROT", filename=prot_filename, info_hash="a" * 40)

        # Protected member (ID_PROT) comes SECOND in the list
        categorized, category_map, _ = _categorize_all(
            [dup_rd, prot_rd],
            active_hashes={"a" * 40},
            active_rd_ids=set(),
            symlink_mount_names=set(),
        )
        assert category_map["ID_PROT"] == RdTorrentCategory.PROTECTED
        assert category_map["ID_DUP"] == RdTorrentCategory.DUPLICATE

    def test_empty_list_returns_empty(self):
        categorized, category_map, hash_map = _categorize_all(
            [],
            active_hashes=set(),
            active_rd_ids=set(),
            symlink_mount_names=set(),
        )
        assert categorized == []
        assert category_map == {}
        assert hash_map == {}

    def test_hash_map_populated(self):
        torrents = [_rd("ID1", info_hash="c" * 40)]
        _, _, hash_map = _categorize_all(
            torrents,
            active_hashes=set(),
            active_rd_ids=set(),
            symlink_mount_names=set(),
        )
        assert hash_map["ID1"] == "c" * 40


# ---------------------------------------------------------------------------
# 8. scan_rd_account — integration tests (mock RD + DB)
# ---------------------------------------------------------------------------


class TestScanRdAccount:
    async def test_empty_rd_account(self, session: AsyncSession):
        with patch("src.services.real_debrid.RealDebridClient") as MockRd:
            mock_client = AsyncMock()
            mock_client.list_all_torrents.return_value = []
            MockRd.return_value = mock_client

            with patch("src.core.rd_cleanup._build_protection_sets", new_callable=AsyncMock) as mock_prot:
                mock_prot.return_value = (set(), set(), set())
                result = await scan_rd_account(session)

        assert result.total_torrents == 0
        for s in result.summaries:
            assert s.count == 0
            assert s.total_bytes == 0

    async def test_mixed_categories_with_db_fixtures(self, session: AsyncSession):
        # Create an active RdTorrent so its hash is protected
        torrent_rec = _make_active_rd_torrent(
            session, rd_id="PROT_RDID", info_hash="a" * 40
        )
        await session.flush()

        rd_data = [
            _rd("PROT_RDID", info_hash="a" * 40),
            _rd("DEAD1", status="error", info_hash="b" * 40),
            _rd("ORPH1", status="downloaded", info_hash="c" * 40, filename="Orphan.Movie.2023"),
        ]

        with patch("src.services.real_debrid.RealDebridClient") as MockRd:
            mock_client = AsyncMock()
            mock_client.list_all_torrents.return_value = rd_data
            MockRd.return_value = mock_client
            with patch("src.config.settings") as mock_settings:
                mock_settings.paths.zurg_mount = ZURG_MOUNT
                result = await scan_rd_account(session)

        assert result.total_torrents == 3
        protected_summary = next(s for s in result.summaries if s.category == RdTorrentCategory.PROTECTED)
        dead_summary = next(s for s in result.summaries if s.category == RdTorrentCategory.DEAD)
        assert protected_summary.count == 1
        assert dead_summary.count == 1

    async def test_cache_populated_after_scan(self, session: AsyncSession):
        rd_data = [_rd("ID1", info_hash="d" * 40)]

        with patch("src.services.real_debrid.RealDebridClient") as MockRd:
            mock_client = AsyncMock()
            mock_client.list_all_torrents.return_value = rd_data
            MockRd.return_value = mock_client
            with patch("src.core.rd_cleanup._build_protection_sets", new_callable=AsyncMock) as mock_prot:
                mock_prot.return_value = (set(), set(), set())
                await scan_rd_account(session)

        assert _last_scan_cache["scanned_at"] is not None
        assert _last_scan_cache["rd_torrents"] == rd_data
        assert _last_scan_cache["category_map"] is not None
        assert _last_scan_cache["hash_map"] is not None

    async def test_protection_set_failure_emits_warning(self, session: AsyncSession):
        rd_data = [_rd("ID1", info_hash="e" * 40)]

        with patch("src.services.real_debrid.RealDebridClient") as MockRd:
            mock_client = AsyncMock()
            mock_client.list_all_torrents.return_value = rd_data
            MockRd.return_value = mock_client
            with patch(
                "src.core.rd_cleanup._build_protection_sets",
                side_effect=RuntimeError("DB locked"),
            ):
                result = await scan_rd_account(session)

        assert len(result.warnings) > 0
        assert any("protection" in w.lower() for w in result.warnings)

    async def test_rd_api_failure_propagates_exception(self, session: AsyncSession):
        with patch("src.services.real_debrid.RealDebridClient") as MockRd:
            mock_client = AsyncMock()
            mock_client.list_all_torrents.side_effect = RuntimeError("RD down")
            MockRd.return_value = mock_client
            with pytest.raises(RuntimeError, match="RD down"):
                await scan_rd_account(session)

    async def test_scan_sorted_protected_last(self, session: AsyncSession):
        rd_data = [
            _rd("PROT1", info_hash="a" * 40),
            _rd("DEAD1", status="error", info_hash="b" * 40),
        ]

        with patch("src.services.real_debrid.RealDebridClient") as MockRd:
            mock_client = AsyncMock()
            mock_client.list_all_torrents.return_value = rd_data
            MockRd.return_value = mock_client
            with patch("src.core.rd_cleanup._build_protection_sets", new_callable=AsyncMock) as mock_prot:
                mock_prot.return_value = ({"a" * 40}, set(), set())
                result = await scan_rd_account(session)

        # Dead should appear before Protected
        categories = [t.category for t in result.torrents]
        assert categories.index(RdTorrentCategory.DEAD) < categories.index(RdTorrentCategory.PROTECTED)


# ---------------------------------------------------------------------------
# 9. execute_rd_cleanup — execution tests
# ---------------------------------------------------------------------------


def _seed_cache(
    category_map: dict[str, RdTorrentCategory],
    hash_map: dict[str, str] | None = None,
    age_seconds: int = 10,
) -> None:
    """Populate _last_scan_cache as if scan_rd_account just ran."""
    _last_scan_cache["scanned_at"] = datetime.now(tz=timezone.utc) - timedelta(seconds=age_seconds)
    _last_scan_cache["category_map"] = category_map
    _last_scan_cache["hash_map"] = hash_map or {}
    # rd_torrents only needed for cache-miss path
    _last_scan_cache["rd_torrents"] = []


class TestExecuteRdCleanup:
    async def test_delete_single_dead_torrent(self, session: AsyncSession):
        _seed_cache({"DEAD1": RdTorrentCategory.DEAD})

        with patch("src.services.real_debrid.RealDebridClient") as MockRd:
            mock_client = AsyncMock()
            mock_client.delete_torrent = AsyncMock(return_value=None)
            MockRd.return_value = mock_client
            with patch("src.core.dedup.dedup_engine") as mock_dedup:
                mock_dedup.mark_torrent_removed = AsyncMock()
                result = await execute_rd_cleanup(session, ["DEAD1"])

        assert result.deleted == 1
        assert result.failed == 0
        assert result.rejected_protected == 0
        assert result.rejected_not_found == 0
        mock_client.delete_torrent.assert_called_once_with("DEAD1")

    async def test_delete_multiple_torrents(self, session: AsyncSession):
        _seed_cache({
            "DEAD1": RdTorrentCategory.DEAD,
            "STALE1": RdTorrentCategory.STALE,
            "ORPH1": RdTorrentCategory.ORPHANED,
        })

        with patch("src.services.real_debrid.RealDebridClient") as MockRd:
            mock_client = AsyncMock()
            mock_client.delete_torrent = AsyncMock(return_value=None)
            MockRd.return_value = mock_client
            with patch("src.core.dedup.dedup_engine") as mock_dedup:
                mock_dedup.mark_torrent_removed = AsyncMock()
                result = await execute_rd_cleanup(session, ["DEAD1", "STALE1", "ORPH1"])

        assert result.deleted == 3
        assert result.requested == 3

    async def test_reject_protected_torrent(self, session: AsyncSession):
        _seed_cache({"PROT1": RdTorrentCategory.PROTECTED})

        with patch("src.services.real_debrid.RealDebridClient") as MockRd:
            mock_client = AsyncMock()
            mock_client.delete_torrent = AsyncMock()
            MockRd.return_value = mock_client
            with patch("src.core.dedup.dedup_engine"):
                result = await execute_rd_cleanup(session, ["PROT1"])

        assert result.rejected_protected == 1
        assert result.deleted == 0
        mock_client.delete_torrent.assert_not_called()

    async def test_reject_unknown_rd_id(self, session: AsyncSession):
        _seed_cache({})  # empty category map

        with patch("src.services.real_debrid.RealDebridClient") as MockRd:
            mock_client = AsyncMock()
            MockRd.return_value = mock_client
            with patch("src.core.dedup.dedup_engine"):
                result = await execute_rd_cleanup(session, ["GHOST_ID"])

        assert result.rejected_not_found == 1
        assert result.deleted == 0

    async def test_mixed_request_correct_counts(self, session: AsyncSession):
        _seed_cache({
            "DEAD1": RdTorrentCategory.DEAD,
            "PROT1": RdTorrentCategory.PROTECTED,
        })

        with patch("src.services.real_debrid.RealDebridClient") as MockRd:
            mock_client = AsyncMock()
            mock_client.delete_torrent = AsyncMock(return_value=None)
            MockRd.return_value = mock_client
            with patch("src.core.dedup.dedup_engine") as mock_dedup:
                mock_dedup.mark_torrent_removed = AsyncMock()
                result = await execute_rd_cleanup(session, ["DEAD1", "PROT1", "GHOST"])

        assert result.requested == 3
        assert result.deleted == 1
        assert result.rejected_protected == 1
        assert result.rejected_not_found == 1

    async def test_rate_limit_stops_further_deletions(self, session: AsyncSession):
        from src.services.real_debrid import RealDebridRateLimitError

        _seed_cache({
            "DEAD1": RdTorrentCategory.DEAD,
            "DEAD2": RdTorrentCategory.DEAD,
            "DEAD3": RdTorrentCategory.DEAD,
        })

        with patch("src.services.real_debrid.RealDebridClient") as MockRd:
            mock_client = AsyncMock()
            mock_client.delete_torrent = AsyncMock(
                side_effect=RealDebridRateLimitError("429 Too Many Requests")
            )
            MockRd.return_value = mock_client
            with patch("src.core.dedup.dedup_engine") as mock_dedup:
                mock_dedup.mark_torrent_removed = AsyncMock()
                result = await execute_rd_cleanup(session, ["DEAD1", "DEAD2", "DEAD3"])

        assert result.rate_limited is True
        # Sequential: first call rate-limits, remaining are skipped without API calls
        assert mock_client.delete_torrent.call_count == 1
        assert result.failed == 3
        assert result.deleted == 0

    async def test_delete_error_increments_failed_continues(self, session: AsyncSession):
        _seed_cache({
            "DEAD1": RdTorrentCategory.DEAD,
            "DEAD2": RdTorrentCategory.DEAD,
        })

        async def _delete(rd_id: str) -> None:
            if rd_id == "DEAD1":
                raise OSError("connection reset")
            # DEAD2 succeeds

        with patch("src.services.real_debrid.RealDebridClient") as MockRd:
            mock_client = AsyncMock()
            mock_client.delete_torrent = AsyncMock(side_effect=_delete)
            MockRd.return_value = mock_client
            with patch("src.core.dedup.dedup_engine") as mock_dedup:
                mock_dedup.mark_torrent_removed = AsyncMock()
                result = await execute_rd_cleanup(session, ["DEAD1", "DEAD2"])

        assert result.failed == 1
        assert result.deleted == 1
        assert len(result.errors) == 1
        assert "DEAD1" in result.errors[0]

    async def test_empty_rd_ids_returns_zeroes(self, session: AsyncSession):
        with patch("src.services.real_debrid.RealDebridClient") as MockRd:
            mock_client = AsyncMock()
            MockRd.return_value = mock_client
            result = await execute_rd_cleanup(session, [])

        assert result.requested == 0
        assert result.deleted == 0
        mock_client.list_all_torrents.assert_not_called()
        mock_client.delete_torrent.assert_not_called()

    async def test_cache_reuse_skips_list_all_torrents(self, session: AsyncSession):
        _seed_cache({"DEAD1": RdTorrentCategory.DEAD}, age_seconds=10)

        with patch("src.services.real_debrid.RealDebridClient") as MockRd:
            mock_client = AsyncMock()
            mock_client.delete_torrent = AsyncMock(return_value=None)
            MockRd.return_value = mock_client
            with patch("src.core.dedup.dedup_engine") as mock_dedup:
                mock_dedup.mark_torrent_removed = AsyncMock()
                await execute_rd_cleanup(session, ["DEAD1"])

        # list_all_torrents should NOT be called when cache is fresh
        mock_client.list_all_torrents.assert_not_called()

    async def test_cache_expired_triggers_fresh_list(self, session: AsyncSession):
        # Seed cache with expired timestamp (400 seconds old, TTL is 300)
        _last_scan_cache["scanned_at"] = datetime.now(tz=timezone.utc) - timedelta(seconds=400)
        _last_scan_cache["category_map"] = None  # expired — missing category_map
        _last_scan_cache["hash_map"] = {}
        _last_scan_cache["rd_torrents"] = []

        fresh_category_map = {"DEAD1": RdTorrentCategory.DEAD}

        with patch("src.services.real_debrid.RealDebridClient") as MockRd:
            mock_client = AsyncMock()
            mock_client.list_all_torrents = AsyncMock(return_value=[
                _rd("DEAD1", status="error", info_hash="f" * 40)
            ])
            mock_client.delete_torrent = AsyncMock(return_value=None)
            MockRd.return_value = mock_client
            with patch(
                "src.core.rd_cleanup._build_protection_sets", new_callable=AsyncMock
            ) as mock_prot:
                mock_prot.return_value = (set(), set(), set())
                with patch("src.core.dedup.dedup_engine") as mock_dedup:
                    mock_dedup.mark_torrent_removed = AsyncMock()
                    result = await execute_rd_cleanup(session, ["DEAD1"])

        mock_client.list_all_torrents.assert_called_once()

    async def test_successful_deletion_calls_mark_torrent_removed(self, session: AsyncSession):
        info_hash = "f" * 40
        _seed_cache(
            {"DEAD1": RdTorrentCategory.DEAD},
            hash_map={"DEAD1": info_hash},
        )

        with patch("src.services.real_debrid.RealDebridClient") as MockRd:
            mock_client = AsyncMock()
            mock_client.delete_torrent = AsyncMock(return_value=None)
            MockRd.return_value = mock_client
            with patch("src.core.dedup.dedup_engine") as mock_dedup:
                mock_dedup.mark_torrent_removed = AsyncMock()
                await execute_rd_cleanup(session, ["DEAD1"])

        mock_dedup.mark_torrent_removed.assert_called_once_with(session, info_hash)

    async def test_successful_deletion_invalidates_cache(self, session: AsyncSession):
        _seed_cache({"DEAD1": RdTorrentCategory.DEAD})

        with patch("src.services.real_debrid.RealDebridClient") as MockRd:
            mock_client = AsyncMock()
            mock_client.delete_torrent = AsyncMock(return_value=None)
            MockRd.return_value = mock_client
            with patch("src.core.dedup.dedup_engine") as mock_dedup:
                mock_dedup.mark_torrent_removed = AsyncMock()
                await execute_rd_cleanup(session, ["DEAD1"])

        assert _last_scan_cache["scanned_at"] is None
        assert _last_scan_cache["category_map"] is None
        assert _last_scan_cache["rd_torrents"] is None
        assert _last_scan_cache["hash_map"] is None

    async def test_no_info_hash_in_map_skips_mark_torrent_removed(self, session: AsyncSession):
        """Torrent not in rd_torrents table: deleted count correct, no error."""
        _seed_cache(
            {"DEAD1": RdTorrentCategory.DEAD},
            hash_map={},  # no hash for DEAD1
        )

        with patch("src.services.real_debrid.RealDebridClient") as MockRd:
            mock_client = AsyncMock()
            mock_client.delete_torrent = AsyncMock(return_value=None)
            MockRd.return_value = mock_client
            with patch("src.core.dedup.dedup_engine") as mock_dedup:
                mock_dedup.mark_torrent_removed = AsyncMock()
                result = await execute_rd_cleanup(session, ["DEAD1"])

        assert result.deleted == 1
        assert result.failed == 0
        mock_dedup.mark_torrent_removed.assert_not_called()

    async def test_rd_api_failure_on_cache_miss_propagates(self, session: AsyncSession):
        # Force cache miss
        _last_scan_cache["scanned_at"] = None

        with patch("src.services.real_debrid.RealDebridClient") as MockRd:
            mock_client = AsyncMock()
            mock_client.list_all_torrents.side_effect = RuntimeError("RD API unreachable")
            MockRd.return_value = mock_client
            with pytest.raises(RuntimeError, match="RD API unreachable"):
                await execute_rd_cleanup(session, ["SOME_ID"])

    async def test_mark_torrent_removed_failure_does_not_abort(self, session: AsyncSession):
        """If mark_torrent_removed raises, the result still reflects successful deletion."""
        info_hash = "0a" * 20
        _seed_cache(
            {"DEAD1": RdTorrentCategory.DEAD},
            hash_map={"DEAD1": info_hash},
        )

        with patch("src.services.real_debrid.RealDebridClient") as MockRd:
            mock_client = AsyncMock()
            mock_client.delete_torrent = AsyncMock(return_value=None)
            MockRd.return_value = mock_client
            with patch("src.core.dedup.dedup_engine") as mock_dedup:
                mock_dedup.mark_torrent_removed = AsyncMock(
                    side_effect=RuntimeError("DB error")
                )
                result = await execute_rd_cleanup(session, ["DEAD1"])

        # Deletion still counted as successful even if mark_torrent_removed failed
        assert result.deleted == 1
        assert result.failed == 0


# ---------------------------------------------------------------------------
# 10. _build_protection_sets — DB-backed tests
# ---------------------------------------------------------------------------


class TestBuildProtectionSets:
    async def test_active_hash_included(self, session: AsyncSession):
        from src.core.rd_cleanup import _build_protection_sets

        _make_active_rd_torrent(session, rd_id="RD1", info_hash="a" * 40)
        await session.flush()

        with patch("src.config.settings") as mock_settings:
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            active_hashes, active_rd_ids, symlink_names = await _build_protection_sets(session)

        assert "a" * 40 in active_hashes

    async def test_active_rd_id_included(self, session: AsyncSession):
        from src.core.rd_cleanup import _build_protection_sets

        _make_active_rd_torrent(session, rd_id="MYRDI", info_hash="b" * 40)
        await session.flush()

        with patch("src.config.settings") as mock_settings:
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            active_hashes, active_rd_ids, symlink_names = await _build_protection_sets(session)

        assert "MYRDI" in active_rd_ids

    async def test_removed_torrent_not_in_active_sets(self, session: AsyncSession):
        from src.core.rd_cleanup import _build_protection_sets

        removed = RdTorrent(
            rd_id="REMOVED1",
            info_hash="c" * 40,
            filename="Old.Movie.2020",
            filesize=1000,
            status=TorrentStatus.REMOVED,
        )
        session.add(removed)
        await session.flush()

        with patch("src.config.settings") as mock_settings:
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            active_hashes, active_rd_ids, symlink_names = await _build_protection_sets(session)

        assert "c" * 40 not in active_hashes
        assert "REMOVED1" not in active_rd_ids

    async def test_symlink_source_path_extracted(self, session: AsyncSession):
        from src.core.rd_cleanup import _build_protection_sets

        source_path = f"{ZURG_MOUNT}/Breaking.Bad.S01.BluRay/Breaking.Bad.S01E01.mkv"
        _make_symlink(session, source_path=source_path)
        await session.flush()

        with patch("src.config.settings") as mock_settings:
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            _, _, symlink_names = await _build_protection_sets(session)

        # The mount-relative name should be present (lowercased)
        assert len(symlink_names) > 0
        assert any("breaking" in name for name in symlink_names)

    async def test_empty_db_returns_empty_sets(self, session: AsyncSession):
        from src.core.rd_cleanup import _build_protection_sets

        with patch("src.config.settings") as mock_settings:
            mock_settings.paths.zurg_mount = ZURG_MOUNT
            active_hashes, active_rd_ids, symlink_names = await _build_protection_sets(session)

        assert active_hashes == set()
        assert active_rd_ids == set()
        assert symlink_names == set()
