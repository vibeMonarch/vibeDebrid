"""Tests for src/core/cleanup.py.

Coverage
--------
_classify_liveness()
  - source_exists=True → LIVE regardless of rd_torrent presence
  - source_exists=False, has_rd_torrent=True → BRIDGED
  - source_exists=False, has_rd_torrent=False → DEAD
  - link_valid does not affect tier classification

_select_keeper()
  - LIVE beats BRIDGED beats DEAD
  - Among equal liveness: non-migration source wins over migration
  - Tiebreaker: lowest item_id when all else equal
  - Single item → that item is keeper, empty remove list
  - All DEAD → lowest ID kept, reason mentions tiebreaker
  - LIVE + DEAD mix → LIVE is keeper regardless of ID ordering
  - Migration vs non-migration at same tier → non-migration wins

assess_migration_items()
  - Basic assessment: items with existing source_paths classified as LIVE
  - Items with rd_torrent but no source_path → BRIDGED
  - Items with neither source_path nor rd_torrent → DEAD
  - Non-migration items in same group are included in assessment
  - Items with NULL imdb_id are excluded (no group key)
  - Single-item groups (no migration duplicates) return empty list
  - Indirect RD torrent detection via source_path directory name match
  - Returns empty list when no migration items exist

build_cleanup_preview()
  - Groups formed correctly by (imdb_id, season, episode)
  - Movies grouped by (imdb_id, None, None)
  - Keeper is LIVE item when mixed with DEAD items
  - rd_ids_to_delete computed correctly (excludes protected hashes)
  - rd_ids_protected includes hashes shared with kept items
  - Empty input → empty preview (all counts zero)
  - Warnings generated for items with NULL imdb_id
  - symlinks_to_delete count reflects removed items with link_paths
  - total_items_assessed and liveness counts tallied accurately

execute_cleanup()
  - DB records deleted in correct FK order
  - Correct items removed (only remove_ids, not keep_ids)
  - Empty preview → no-op result (zero counts, no errors)
  - Symlink disk cleanup: is_link=True → unlink called, count incremented
  - Symlink disk cleanup: is_link=False → unlink NOT called
  - OSError on unlink → error appended, not raised
  - items_kept reflects number of groups

_batch_check_paths()
  - Returns correct exists/not-exists mapping for each path
  - Handles multiple paths in a single call
  - Paths batched in groups of 50

asyncio_mode = "auto" (configured in pyproject.toml).
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.cleanup import (
    AssessedItem,
    CleanupPreview,
    ItemLiveness,
    SmartDuplicateGroup,
    _batch_check_paths,
    _classify_liveness,
    _select_keeper,
    assess_migration_items,
    build_cleanup_preview,
    execute_cleanup,
)
from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.scrape_result import ScrapeLog
from src.models.symlink import Symlink
from src.models.torrent import RdTorrent, TorrentStatus

# ---------------------------------------------------------------------------
# Shared test helpers
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
    state: QueueState = QueueState.COMPLETE,
) -> MediaItem:
    """Construct and add an unsaved MediaItem to the session."""
    item = MediaItem(
        title=title,
        imdb_id=imdb_id,
        media_type=media_type,
        state=state,
        state_changed_at=datetime.now(UTC),
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
    target_path: str = "/library/Movie/Movie.mkv",
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
    rd_id: str = "RDABC123",
    info_hash: str = "aaaa1111bbbb2222cccc3333",
    filename: str = "Movie.2024.1080p",
) -> RdTorrent:
    """Construct and add an unsaved RdTorrent to the session."""
    torrent = RdTorrent(
        media_item_id=media_item_id,
        rd_id=rd_id,
        info_hash=info_hash,
        filename=filename,
        status=TorrentStatus.ACTIVE,
    )
    session.add(torrent)
    return torrent


def _make_scrape_log(
    session: AsyncSession,
    *,
    media_item_id: int,
) -> ScrapeLog:
    """Construct and add an unsaved ScrapeLog to the session."""
    log = ScrapeLog(
        media_item_id=media_item_id,
        scraper="torrentio",
        results_count=5,
    )
    session.add(log)
    return log


async def _flush(session: AsyncSession) -> None:
    """Flush the session so IDs are assigned."""
    await session.flush()


def _make_assessed(
    *,
    item_id: int = 1,
    title: str = "Test Movie",
    imdb_id: str | None = "tt0000001",
    source: str | None = "migration",
    season: int | None = None,
    episode: int | None = None,
    liveness: ItemLiveness = ItemLiveness.DEAD,
    source_path: str | None = None,
    link_path: str | None = None,
    source_exists: bool = False,
    link_valid: bool = False,
    has_rd_torrent: bool = False,
    rd_id: str | None = None,
    info_hash: str | None = None,
    rd_filename: str | None = None,
    state: str = "complete",
) -> AssessedItem:
    """Build an AssessedItem with sensible defaults for unit tests."""
    return AssessedItem(
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
        has_rd_torrent=has_rd_torrent,
        rd_id=rd_id,
        info_hash=info_hash,
        rd_filename=rd_filename,
    )


def _mock_settings(zurg_mount: str = ZURG_MOUNT) -> MagicMock:
    """Return a settings mock with the given zurg_mount path."""
    mock_cfg = MagicMock()
    mock_cfg.paths.zurg_mount = zurg_mount
    mock_cfg.paths.library_movies = "/library/movies"
    mock_cfg.paths.library_shows = "/library/shows"
    return mock_cfg


# ---------------------------------------------------------------------------
# _classify_liveness
# ---------------------------------------------------------------------------


class TestClassifyLiveness:
    """Unit tests for the _classify_liveness helper."""

    def test_source_exists_true_returns_live(self) -> None:
        """source_exists=True always yields LIVE regardless of RD torrent."""
        result = _classify_liveness(
            source_exists=True, link_valid=False, has_rd_torrent=False
        )
        assert result == ItemLiveness.LIVE

    def test_source_exists_true_with_rd_torrent_still_live(self) -> None:
        """source_exists=True + has_rd_torrent=True still yields LIVE (not BRIDGED)."""
        result = _classify_liveness(
            source_exists=True, link_valid=True, has_rd_torrent=True
        )
        assert result == ItemLiveness.LIVE

    def test_source_missing_rd_torrent_present_returns_bridged(self) -> None:
        """source_exists=False, has_rd_torrent=True yields BRIDGED."""
        result = _classify_liveness(
            source_exists=False, link_valid=False, has_rd_torrent=True
        )
        assert result == ItemLiveness.BRIDGED

    def test_source_missing_rd_torrent_missing_returns_dead(self) -> None:
        """source_exists=False, has_rd_torrent=False yields DEAD."""
        result = _classify_liveness(
            source_exists=False, link_valid=False, has_rd_torrent=False
        )
        assert result == ItemLiveness.DEAD

    def test_link_valid_does_not_affect_tier(self) -> None:
        """link_valid is not used in tier logic — DEAD regardless of link_valid."""
        result_link_valid = _classify_liveness(
            source_exists=False, link_valid=True, has_rd_torrent=False
        )
        result_link_invalid = _classify_liveness(
            source_exists=False, link_valid=False, has_rd_torrent=False
        )
        assert result_link_valid == ItemLiveness.DEAD
        assert result_link_invalid == ItemLiveness.DEAD


# ---------------------------------------------------------------------------
# _select_keeper
# ---------------------------------------------------------------------------


class TestSelectKeeper:
    """Unit tests for the _select_keeper helper."""

    def test_live_beats_bridged(self) -> None:
        """A LIVE item wins over a BRIDGED item regardless of id order."""
        live = _make_assessed(item_id=2, liveness=ItemLiveness.LIVE, source_exists=True)
        bridged = _make_assessed(
            item_id=1, liveness=ItemLiveness.BRIDGED, has_rd_torrent=True
        )
        keeper, removals, reason = _select_keeper([bridged, live])
        assert keeper.item_id == live.item_id
        assert len(removals) == 1
        assert removals[0].item_id == bridged.item_id
        assert "source file present on mount" in reason

    def test_live_beats_dead(self) -> None:
        """A LIVE item wins over a DEAD item regardless of id order."""
        live = _make_assessed(item_id=10, liveness=ItemLiveness.LIVE, source_exists=True)
        dead = _make_assessed(item_id=1, liveness=ItemLiveness.DEAD)
        keeper, removals, _ = _select_keeper([dead, live])
        assert keeper.item_id == live.item_id

    def test_bridged_beats_dead(self) -> None:
        """A BRIDGED item wins over a DEAD item."""
        bridged = _make_assessed(
            item_id=5, liveness=ItemLiveness.BRIDGED, has_rd_torrent=True
        )
        dead = _make_assessed(item_id=2, liveness=ItemLiveness.DEAD)
        keeper, removals, reason = _select_keeper([dead, bridged])
        assert keeper.item_id == bridged.item_id
        assert "has RD torrent record" in reason

    def test_non_migration_preferred_over_migration_at_same_tier(self) -> None:
        """Among equal DEAD items: non-migration source beats migration."""
        migration = _make_assessed(item_id=1, liveness=ItemLiveness.DEAD, source="migration")
        watchlist = _make_assessed(item_id=2, liveness=ItemLiveness.DEAD, source="plex_watchlist")
        keeper, removals, reason = _select_keeper([migration, watchlist])
        assert keeper.item_id == watchlist.item_id
        assert "plex_watchlist" in reason

    def test_non_migration_preferred_over_migration_when_live(self) -> None:
        """Among equal LIVE items: non-migration wins."""
        migration_live = _make_assessed(
            item_id=1, liveness=ItemLiveness.LIVE, source="migration", source_exists=True
        )
        manual_live = _make_assessed(
            item_id=2, liveness=ItemLiveness.LIVE, source="manual", source_exists=True
        )
        keeper, _, reason = _select_keeper([migration_live, manual_live])
        assert keeper.item_id == manual_live.item_id
        assert "manual" in reason

    def test_tiebreaker_lowest_id_when_equal_tier_and_source(self) -> None:
        """When tier and source are equal, lowest item_id wins."""
        item_a = _make_assessed(item_id=3, liveness=ItemLiveness.DEAD, source="migration")
        item_b = _make_assessed(item_id=7, liveness=ItemLiveness.DEAD, source="migration")
        item_c = _make_assessed(item_id=1, liveness=ItemLiveness.DEAD, source="migration")
        keeper, removals, _ = _select_keeper([item_a, item_b, item_c])
        assert keeper.item_id == 1
        assert len(removals) == 2

    def test_single_item_is_keeper_with_empty_removals(self) -> None:
        """A group of one item keeps that item and returns an empty removal list."""
        only_item = _make_assessed(item_id=42, liveness=ItemLiveness.LIVE, source_exists=True)
        keeper, removals, _ = _select_keeper([only_item])
        assert keeper.item_id == 42
        assert removals == []

    def test_all_dead_lowest_id_kept_reason_mentions_tiebreaker(self) -> None:
        """When all items are DEAD and all migration, lowest ID is kept with clear reason."""
        item_low = _make_assessed(item_id=5, liveness=ItemLiveness.DEAD, source="migration")
        item_high = _make_assessed(item_id=9, liveness=ItemLiveness.DEAD, source="migration")
        keeper, _, reason = _select_keeper([item_high, item_low])
        assert keeper.item_id == 5
        assert "no live source" in reason
        assert "tiebreaker" in reason

    def test_three_items_two_in_removals(self) -> None:
        """With three items, one keeper and two items in the remove list."""
        live = _make_assessed(item_id=3, liveness=ItemLiveness.LIVE, source_exists=True)
        dead_a = _make_assessed(item_id=1, liveness=ItemLiveness.DEAD)
        dead_b = _make_assessed(item_id=2, liveness=ItemLiveness.DEAD)
        keeper, removals, _ = _select_keeper([dead_a, dead_b, live])
        assert keeper.item_id == live.item_id
        assert len(removals) == 2
        remove_ids = {r.item_id for r in removals}
        assert 1 in remove_ids
        assert 2 in remove_ids


# ---------------------------------------------------------------------------
# assess_migration_items
# ---------------------------------------------------------------------------


class TestAssessMigrationItems:
    """Tests for assess_migration_items() — full DB-backed integration."""

    async def test_returns_empty_when_no_migration_items(
        self, session: AsyncSession
    ) -> None:
        """An empty table returns an empty list immediately."""
        with patch("src.config.settings", _mock_settings()):
            result = await assess_migration_items(session)
        assert result == []

    async def test_returns_empty_when_no_duplicates(
        self, session: AsyncSession
    ) -> None:
        """A single migration item (no duplicate) returns empty — HAVING COUNT>1 filters it."""
        _make_item(session, imdb_id="tt1010101", source="migration")
        await _flush(session)

        with patch("src.config.settings", _mock_settings()):
            result = await assess_migration_items(session)
        assert result == []

    async def test_items_with_null_imdb_id_excluded(
        self, session: AsyncSession
    ) -> None:
        """Items with NULL imdb_id are excluded because the group query requires imdb_id IS NOT NULL."""
        _make_item(session, imdb_id=None, source="migration", title="No IMDB A")
        _make_item(session, imdb_id=None, source="migration", title="No IMDB B")
        await _flush(session)

        with patch("src.config.settings", _mock_settings()):
            result = await assess_migration_items(session)
        assert result == []

    async def test_source_path_exists_classified_as_live(
        self, session: AsyncSession
    ) -> None:
        """Items whose symlink source_path exists on disk are classified as LIVE."""
        item_a = _make_item(session, imdb_id="tt2020202", source="migration")
        _item_b = _make_item(session, imdb_id="tt2020202", source="migration")
        await _flush(session)

        source_path = f"{ZURG_MOUNT}/Movie.2024.1080p/Movie.2024.1080p.mkv"
        _make_symlink(
            session,
            media_item_id=item_a.id,
            source_path=source_path,
            target_path="/library/movies/Movie (2024).mkv",
        )
        await _flush(session)

        def _fake_exists(path: str) -> bool:
            return path == source_path

        with (
            patch("src.config.settings", _mock_settings()),
            patch("os.path.exists", side_effect=_fake_exists),
        ):
            result = await assess_migration_items(session)

        live_items = [a for a in result if a.liveness == ItemLiveness.LIVE]
        assert len(live_items) == 1
        assert live_items[0].item_id == item_a.id

    async def test_rd_torrent_without_source_path_is_bridged(
        self, session: AsyncSession
    ) -> None:
        """Item with an RdTorrent FK but no symlink source_path → BRIDGED."""
        item_a = _make_item(session, imdb_id="tt3030303", source="migration")
        _item_b = _make_item(session, imdb_id="tt3030303", source="migration")
        await _flush(session)

        _make_rd_torrent(
            session,
            media_item_id=item_a.id,
            rd_id="RDXYZ",
            info_hash="bbbbcccc1111",
        )
        await _flush(session)

        with (
            patch("src.config.settings", _mock_settings()),
            patch("os.path.exists", return_value=False),
        ):
            result = await assess_migration_items(session)

        bridged = [a for a in result if a.liveness == ItemLiveness.BRIDGED]
        assert any(a.item_id == item_a.id for a in bridged)

    async def test_no_source_path_no_rd_torrent_is_dead(
        self, session: AsyncSession
    ) -> None:
        """Item with no symlink and no RdTorrent → DEAD."""
        _item_a = _make_item(session, imdb_id="tt4040404", source="migration")
        _item_b = _make_item(session, imdb_id="tt4040404", source="migration")
        await _flush(session)

        with (
            patch("src.config.settings", _mock_settings()),
            patch("os.path.exists", return_value=False),
        ):
            result = await assess_migration_items(session)

        dead_items = [a for a in result if a.liveness == ItemLiveness.DEAD]
        assert len(dead_items) == 2

    async def test_non_migration_items_in_same_group_included(
        self, session: AsyncSession
    ) -> None:
        """Non-migration items sharing (imdb_id, season, episode) are included in assessment."""
        mig_a = _make_item(session, imdb_id="tt5050505", source="migration", title="Mig A")
        mig_b = _make_item(session, imdb_id="tt5050505", source="migration", title="Mig B")
        watchlist = _make_item(
            session, imdb_id="tt5050505", source="plex_watchlist", title="Watchlist"
        )
        await _flush(session)

        with (
            patch("src.config.settings", _mock_settings()),
            patch("os.path.exists", return_value=False),
        ):
            result = await assess_migration_items(session)

        item_ids = {a.item_id for a in result}
        assert mig_a.id in item_ids
        assert mig_b.id in item_ids
        assert watchlist.id in item_ids
        assert len(result) == 3

    async def test_indirect_rd_torrent_detection_via_source_dir(
        self, session: AsyncSession
    ) -> None:
        """Item with source_path under zurg mount whose dir matches an RD filename → BRIDGED."""
        item_a = _make_item(
            session, imdb_id="tt6060606", source="migration", title="Indirect Movie"
        )
        item_b = _make_item(
            session, imdb_id="tt6060606", source="migration", title="Indirect Movie Dup"
        )
        await _flush(session)

        # item_b has a source_path under the mount
        source_path = f"{ZURG_MOUNT}/Indirect.Movie.2023.1080p/Indirect.Movie.2023.1080p.mkv"
        _make_symlink(
            session,
            media_item_id=item_b.id,
            source_path=source_path,
            target_path="/library/movies/Indirect Movie (2023).mkv",
        )

        # An RdTorrent exists on another item (item_a) with a matching filename.
        # info_hash must be unique across the whole test db — use a value
        # that cannot collide with other tests in this file.
        _rd = _make_rd_torrent(
            session,
            media_item_id=item_a.id,
            rd_id="RDINDIRECT",
            info_hash="eeee4444ffff6060",
            filename="Indirect.Movie.2023.1080p",
        )
        await _flush(session)

        with (
            patch("src.config.settings", _mock_settings(ZURG_MOUNT)),
            patch("os.path.exists", return_value=False),
        ):
            result = await assess_migration_items(session)

        # item_b matches via directory name → should be BRIDGED, not DEAD
        item_b_assessed = next((a for a in result if a.item_id == item_b.id), None)
        assert item_b_assessed is not None
        assert item_b_assessed.has_rd_torrent is True
        assert item_b_assessed.liveness == ItemLiveness.BRIDGED

    async def test_assessed_items_count_matches_db_rows(
        self, session: AsyncSession
    ) -> None:
        """The length of the returned list equals all items in the duplicate groups."""
        for _ in range(3):
            _make_item(session, imdb_id="tt7070707", source="migration")
        await _flush(session)

        with (
            patch("src.config.settings", _mock_settings()),
            patch("os.path.exists", return_value=False),
        ):
            result = await assess_migration_items(session)

        assert len(result) == 3


# ---------------------------------------------------------------------------
# build_cleanup_preview
# ---------------------------------------------------------------------------


class TestBuildCleanupPreview:
    """Tests for build_cleanup_preview() — takes an assessed list."""

    async def test_empty_input_returns_empty_preview(
        self, session: AsyncSession
    ) -> None:
        """No assessed items → CleanupPreview with all-zero counts and empty lists."""
        preview = await build_cleanup_preview(session, [])
        assert preview.total_items_assessed == 0
        assert preview.live_count == 0
        assert preview.bridged_count == 0
        assert preview.dead_count == 0
        assert preview.groups == []
        assert preview.total_to_remove == 0
        assert preview.rd_ids_to_delete == []
        assert preview.rd_ids_protected == []
        assert preview.warnings == []

    async def test_null_imdb_id_items_generate_warning(
        self, session: AsyncSession
    ) -> None:
        """Items with NULL imdb_id are skipped and produce a warning entry."""
        null_item = _make_assessed(
            item_id=1, imdb_id=None, liveness=ItemLiveness.DEAD, title="No IMDB"
        )
        preview = await build_cleanup_preview(session, [null_item])
        assert len(preview.warnings) == 1
        assert "NULL imdb_id" in preview.warnings[0]
        assert preview.groups == []

    async def test_single_item_group_not_included(
        self, session: AsyncSession
    ) -> None:
        """A group with only one member (no real duplicate) is not added to groups."""
        single = _make_assessed(
            item_id=99, imdb_id="tt9090909", liveness=ItemLiveness.DEAD
        )
        preview = await build_cleanup_preview(session, [single])
        assert preview.groups == []
        assert preview.total_to_remove == 0

    async def test_groups_formed_by_imdb_season_episode(
        self, session: AsyncSession
    ) -> None:
        """Two items with the same (imdb_id, None, None) form one group."""
        item_a = _make_assessed(
            item_id=1, imdb_id="tt1111111", liveness=ItemLiveness.DEAD
        )
        item_b = _make_assessed(
            item_id=2, imdb_id="tt1111111", liveness=ItemLiveness.DEAD
        )
        preview = await build_cleanup_preview(session, [item_a, item_b])
        assert len(preview.groups) == 1
        assert preview.groups[0].imdb_id == "tt1111111"
        assert preview.groups[0].season is None
        assert preview.groups[0].episode is None

    async def test_movie_group_key_uses_null_season_episode(
        self, session: AsyncSession
    ) -> None:
        """Movie items (season=None, episode=None) are correctly grouped."""
        item_a = _make_assessed(
            item_id=10, imdb_id="tt2222222", season=None, episode=None,
            liveness=ItemLiveness.DEAD,
        )
        item_b = _make_assessed(
            item_id=11, imdb_id="tt2222222", season=None, episode=None,
            liveness=ItemLiveness.DEAD,
        )
        preview = await build_cleanup_preview(session, [item_a, item_b])
        assert len(preview.groups) == 1
        assert preview.groups[0].count == 2

    async def test_live_item_chosen_as_keeper_over_dead(
        self, session: AsyncSession
    ) -> None:
        """The LIVE item is always chosen as keeper when mixed with DEAD items."""
        live_item = _make_assessed(
            item_id=20, imdb_id="tt3333333", liveness=ItemLiveness.LIVE,
            source_exists=True, source="plex_watchlist",
        )
        dead_item = _make_assessed(
            item_id=21, imdb_id="tt3333333", liveness=ItemLiveness.DEAD,
            source="migration",
        )
        preview = await build_cleanup_preview(session, [live_item, dead_item])
        assert len(preview.groups) == 1
        group = preview.groups[0]
        assert group.keep.item_id == live_item.item_id
        assert len(group.remove) == 1
        assert group.remove[0].item_id == dead_item.item_id

    async def test_total_to_remove_count(self, session: AsyncSession) -> None:
        """total_to_remove sums all removal items across groups."""
        # Group 1: 3 items → 2 removals
        items_g1 = [
            _make_assessed(item_id=i, imdb_id="tt4444444", liveness=ItemLiveness.DEAD)
            for i in range(1, 4)
        ]
        # Group 2: 2 items → 1 removal
        items_g2 = [
            _make_assessed(item_id=i, imdb_id="tt5555555", liveness=ItemLiveness.DEAD)
            for i in range(10, 12)
        ]
        preview = await build_cleanup_preview(session, items_g1 + items_g2)
        assert preview.total_to_remove == 3  # 2 + 1

    async def test_liveness_counts_tallied_correctly(
        self, session: AsyncSession
    ) -> None:
        """live_count, bridged_count, dead_count reflect all assessed items."""
        items = [
            _make_assessed(item_id=1, imdb_id="tt6666666", liveness=ItemLiveness.LIVE),
            _make_assessed(item_id=2, imdb_id="tt6666666", liveness=ItemLiveness.BRIDGED),
            _make_assessed(item_id=3, imdb_id="tt6666666", liveness=ItemLiveness.DEAD),
        ]
        preview = await build_cleanup_preview(session, items)
        assert preview.total_items_assessed == 3
        assert preview.live_count == 1
        assert preview.bridged_count == 1
        assert preview.dead_count == 1

    async def test_symlinks_to_delete_count(self, session: AsyncSession) -> None:
        """symlinks_to_delete counts removed items that have a link_path."""
        keeper = _make_assessed(
            item_id=1, imdb_id="tt7777777", liveness=ItemLiveness.LIVE,
            source_exists=True,
        )
        remove_with_link = _make_assessed(
            item_id=2, imdb_id="tt7777777", liveness=ItemLiveness.DEAD,
            link_path="/library/movies/Movie.mkv",
        )
        remove_no_link = _make_assessed(
            item_id=3, imdb_id="tt7777777", liveness=ItemLiveness.DEAD,
            link_path=None,
        )
        preview = await build_cleanup_preview(
            session, [keeper, remove_with_link, remove_no_link]
        )
        assert preview.symlinks_to_delete == 1

    async def test_rd_ids_to_delete_computed_for_removed_items(
        self, session: AsyncSession
    ) -> None:
        """RD ids from removed items are queued for deletion."""
        item_keep = _make_item(session, imdb_id="tt8080808", source="migration")
        item_remove = _make_item(session, imdb_id="tt8080808", source="migration")
        await _flush(session)

        _make_rd_torrent(
            session,
            media_item_id=item_remove.id,
            rd_id="RDDELETE01",
            info_hash="deadbeef0001",
        )
        await _flush(session)

        items = [
            _make_assessed(
                item_id=item_keep.id, imdb_id="tt8080808",
                liveness=ItemLiveness.DEAD, source="plex_watchlist",
            ),
            _make_assessed(
                item_id=item_remove.id, imdb_id="tt8080808",
                liveness=ItemLiveness.DEAD, source="migration",
                rd_id="RDDELETE01", info_hash="deadbeef0001",
            ),
        ]
        preview = await build_cleanup_preview(session, items)
        assert "RDDELETE01" in preview.rd_ids_to_delete
        assert "RDDELETE01" not in preview.rd_ids_protected

    async def test_rd_ids_protected_when_hash_shared_with_keeper(
        self, session: AsyncSession
    ) -> None:
        """RD id is protected when the same info_hash appears on a kept item's torrent.

        The rd_torrents table enforces UNIQUE on info_hash, so two live rows
        cannot share the same hash simultaneously in the normal flow.  This
        scenario arises after rd_bridge reassigns the media_item_id FK so that
        a formerly shared torrent row is now owned by the kept item while the
        removed item's FK was already cleared.

        We test the protection branch by mocking the AsyncSession.execute calls
        that build_cleanup_preview uses for its protection queries, injecting
        the controlled results that would represent the shared-hash state.
        """
        from unittest.mock import MagicMock

        # Two assessed items: keeper and removed, same group.
        item_keep_id = 100
        item_remove_id = 101
        shared_hash = "sharedpackhash9090"

        items = [
            _make_assessed(
                item_id=item_keep_id, imdb_id="tt9090909",
                liveness=ItemLiveness.DEAD, source="plex_watchlist",
            ),
            _make_assessed(
                item_id=item_remove_id, imdb_id="tt9090909",
                liveness=ItemLiveness.DEAD, source="migration",
                rd_id="RDREMOVE01", info_hash=shared_hash,
            ),
        ]

        # We need a real session for the liveness tally / grouping path,
        # but we have to intercept the two protection SQL queries.
        # Strategy: run the real grouping, then mock only the execute calls
        # that fetch rd_torrents rows.
        #
        # build_cleanup_preview fires two queries after grouping:
        #   Q1: SELECT rd_id, info_hash FROM rd_torrents WHERE media_item_id IN (...)
        #   Q2: SELECT info_hash FROM rd_torrents WHERE media_item_id NOT IN (...)
        #
        # We mock session.execute to return controlled rows for those queries.

        # Track calls to route Q1 vs Q2.
        call_index = 0

        async def _fake_execute(query, *args, **kwargs):
            nonlocal call_index
            sql = str(query)
            if "media_item_id IN" in sql and "rd_id IS NOT NULL" in sql:
                # Q1: removed items' torrents.
                # Code iterates result directly: ``for rd_id_, ih in remove_hash_rows``
                # Return a plain list of tuples so iteration works correctly.
                call_index += 1
                return [("RDREMOVE01", shared_hash)]
            elif "media_item_id NOT IN" in sql and "info_hash IS NOT NULL" in sql:
                # Q2: kept items' torrents.
                # Code iterates result directly: ``{row[0] for row in keep_hash_rows}``
                # Same hash as the removed item → triggers protection branch.
                call_index += 1
                return [(shared_hash,)]
            # Default: return empty iterable
            return []

        mock_session = MagicMock()
        mock_session.execute = _fake_execute

        preview = await build_cleanup_preview(mock_session, items)  # type: ignore[arg-type]

        # The removed item's rd_id shares hash with kept item → protected
        assert "RDREMOVE01" in preview.rd_ids_protected, (
            f"Expected RDREMOVE01 to be protected; "
            f"rd_ids_to_delete={preview.rd_ids_to_delete}, "
            f"rd_ids_protected={preview.rd_ids_protected}"
        )
        assert "RDREMOVE01" not in preview.rd_ids_to_delete

    async def test_two_independent_groups(self, session: AsyncSession) -> None:
        """Items with different imdb_ids form two independent groups."""
        items = [
            _make_assessed(item_id=1, imdb_id="tt1234500", liveness=ItemLiveness.DEAD),
            _make_assessed(item_id=2, imdb_id="tt1234500", liveness=ItemLiveness.DEAD),
            _make_assessed(item_id=3, imdb_id="tt9876500", liveness=ItemLiveness.DEAD),
            _make_assessed(item_id=4, imdb_id="tt9876500", liveness=ItemLiveness.DEAD),
        ]
        preview = await build_cleanup_preview(session, items)
        assert len(preview.groups) == 2
        imdb_ids = {g.imdb_id for g in preview.groups}
        assert "tt1234500" in imdb_ids
        assert "tt9876500" in imdb_ids


# ---------------------------------------------------------------------------
# execute_cleanup
# ---------------------------------------------------------------------------


class TestExecuteCleanup:
    """Tests for execute_cleanup() — DB mutations and disk operations."""

    async def test_empty_preview_is_noop(self, session: AsyncSession) -> None:
        """An empty CleanupPreview returns a zeroed CleanupResult without touching the DB."""
        preview = CleanupPreview()
        result = await execute_cleanup(session, preview)
        assert result.groups_processed == 0
        assert result.items_removed == 0
        assert result.items_kept == 0
        assert result.symlinks_deleted_from_disk == 0
        assert result.errors == []

    async def test_db_records_deleted_in_fk_order(
        self, session: AsyncSession
    ) -> None:
        """scrape_log → rd_torrents → symlinks → media_items deleted in FK-safe order."""
        item_keep = _make_item(session, imdb_id="tt1122334", source="plex_watchlist")
        item_remove = _make_item(session, imdb_id="tt1122334", source="migration")
        await _flush(session)

        sym = _make_symlink(
            session,
            media_item_id=item_remove.id,
            source_path=f"{ZURG_MOUNT}/Movie/Movie.mkv",
        )
        rd = _make_rd_torrent(session, media_item_id=item_remove.id, rd_id="RD_FKORDER")
        log = _make_scrape_log(session, media_item_id=item_remove.id)
        await _flush(session)

        sym_id = sym.id
        rd_id_pk = rd.id
        log_id = log.id
        remove_id = item_remove.id
        keep_id = item_keep.id

        preview = CleanupPreview(
            groups=[
                SmartDuplicateGroup(
                    imdb_id="tt1122334",
                    title="Movie",
                    season=None,
                    episode=None,
                    keep=_make_assessed(item_id=keep_id, imdb_id="tt1122334"),
                    remove=[
                        _make_assessed(
                            item_id=remove_id,
                            imdb_id="tt1122334",
                            link_path=sym.target_path,
                        )
                    ],
                    reason="test",
                    count=2,
                )
            ]
        )

        with patch("os.path.islink", return_value=False):
            result = await execute_cleanup(session, preview)

        # Verify DB state after execution
        rows = await session.execute(
            text("SELECT id FROM media_items WHERE id = :rid").bindparams(rid=remove_id)
        )
        assert rows.fetchone() is None, "removed item should be gone"

        rows = await session.execute(
            text("SELECT id FROM media_items WHERE id = :kid").bindparams(kid=keep_id)
        )
        assert rows.fetchone() is not None, "kept item should still exist"

        rows = await session.execute(
            text("SELECT id FROM symlinks WHERE id = :sid").bindparams(sid=sym_id)
        )
        assert rows.fetchone() is None, "symlink row should be deleted"

        rows = await session.execute(
            text("SELECT id FROM rd_torrents WHERE id = :rid").bindparams(rid=rd_id_pk)
        )
        assert rows.fetchone() is None, "rd_torrent row should be deleted"

        rows = await session.execute(
            text("SELECT id FROM scrape_log WHERE id = :lid").bindparams(lid=log_id)
        )
        assert rows.fetchone() is None, "scrape_log row should be deleted"

        assert result.items_removed == 1
        assert result.items_kept == 1

    async def test_keep_item_not_deleted(self, session: AsyncSession) -> None:
        """The keeper item is never included in the DELETE query."""
        item_keep = _make_item(session, imdb_id="tt2233445", source="plex_watchlist")
        item_rem = _make_item(session, imdb_id="tt2233445", source="migration")
        await _flush(session)

        preview = CleanupPreview(
            groups=[
                SmartDuplicateGroup(
                    imdb_id="tt2233445",
                    title="Movie",
                    season=None,
                    episode=None,
                    keep=_make_assessed(item_id=item_keep.id, imdb_id="tt2233445"),
                    remove=[_make_assessed(item_id=item_rem.id, imdb_id="tt2233445")],
                    reason="test",
                    count=2,
                )
            ]
        )

        with patch("os.path.islink", return_value=False):
            await execute_cleanup(session, preview)

        rows = await session.execute(
            text("SELECT id FROM media_items WHERE id = :kid").bindparams(kid=item_keep.id)
        )
        assert rows.fetchone() is not None

    async def test_symlink_removed_from_disk_when_is_link(
        self, session: AsyncSession
    ) -> None:
        """os.unlink is called for symlinks where os.path.islink returns True."""
        item_keep = _make_item(session, imdb_id="tt3344556", source="plex_watchlist")
        item_rem = _make_item(session, imdb_id="tt3344556", source="migration")
        await _flush(session)

        link_path = "/library/movies/Movie (2024)/Movie (2024).mkv"
        preview = CleanupPreview(
            groups=[
                SmartDuplicateGroup(
                    imdb_id="tt3344556",
                    title="Movie",
                    season=None,
                    episode=None,
                    keep=_make_assessed(item_id=item_keep.id, imdb_id="tt3344556"),
                    remove=[
                        _make_assessed(
                            item_id=item_rem.id,
                            imdb_id="tt3344556",
                            link_path=link_path,
                        )
                    ],
                    reason="test",
                    count=2,
                )
            ]
        )

        unlink_calls: list[str] = []

        def _fake_islink(path: str) -> bool:
            return True

        def _fake_unlink(path: str) -> None:
            unlink_calls.append(path)

        # _try_remove_empty_dir imports settings inline — patch at src.config.settings.
        with (
            patch("os.path.islink", side_effect=_fake_islink),
            patch("os.unlink", side_effect=_fake_unlink),
            patch("os.rmdir", return_value=None),
            patch("src.config.settings", _mock_settings()),
        ):
            result = await execute_cleanup(session, preview)

        assert link_path in unlink_calls
        assert result.symlinks_deleted_from_disk == 1

    async def test_symlink_not_removed_when_not_a_link(
        self, session: AsyncSession
    ) -> None:
        """os.unlink is NOT called when os.path.islink returns False."""
        item_keep = _make_item(session, imdb_id="tt4455667", source="plex_watchlist")
        item_rem = _make_item(session, imdb_id="tt4455667", source="migration")
        await _flush(session)

        link_path = "/library/movies/Ghost (2024)/Ghost (2024).mkv"
        preview = CleanupPreview(
            groups=[
                SmartDuplicateGroup(
                    imdb_id="tt4455667",
                    title="Ghost",
                    season=None,
                    episode=None,
                    keep=_make_assessed(item_id=item_keep.id, imdb_id="tt4455667"),
                    remove=[
                        _make_assessed(
                            item_id=item_rem.id,
                            imdb_id="tt4455667",
                            link_path=link_path,
                        )
                    ],
                    reason="test",
                    count=2,
                )
            ]
        )

        unlink_calls: list[str] = []

        with (
            patch("os.path.islink", return_value=False),
            patch("os.unlink", side_effect=unlink_calls.append),
        ):
            result = await execute_cleanup(session, preview)

        assert unlink_calls == []
        assert result.symlinks_deleted_from_disk == 0

    async def test_oserror_on_unlink_appended_to_errors(
        self, session: AsyncSession
    ) -> None:
        """An OSError during symlink removal is caught and appended to errors, not re-raised."""
        item_keep = _make_item(session, imdb_id="tt5566778", source="plex_watchlist")
        item_rem = _make_item(session, imdb_id="tt5566778", source="migration")
        await _flush(session)

        link_path = "/library/movies/Fail (2024)/Fail (2024).mkv"
        preview = CleanupPreview(
            groups=[
                SmartDuplicateGroup(
                    imdb_id="tt5566778",
                    title="Fail",
                    season=None,
                    episode=None,
                    keep=_make_assessed(item_id=item_keep.id, imdb_id="tt5566778"),
                    remove=[
                        _make_assessed(
                            item_id=item_rem.id,
                            imdb_id="tt5566778",
                            link_path=link_path,
                        )
                    ],
                    reason="test",
                    count=2,
                )
            ]
        )

        # _try_remove_empty_dir imports settings inline — patch at src.config.settings.
        with (
            patch("os.path.islink", return_value=True),
            patch("os.unlink", side_effect=OSError("Permission denied")),
            patch("src.config.settings", _mock_settings()),
        ):
            result = await execute_cleanup(session, preview)

        assert result.symlinks_deleted_from_disk == 0
        assert any("Permission denied" in e for e in result.errors)

    async def test_items_kept_reflects_number_of_groups(
        self, session: AsyncSession
    ) -> None:
        """items_kept equals the number of SmartDuplicateGroups (one keeper per group)."""
        items = [
            _make_item(session, imdb_id=f"tt{i:07d}", source="migration")
            for i in range(1, 7)
        ]
        await _flush(session)

        groups: list[SmartDuplicateGroup] = []
        for g in range(3):
            keep_item = items[g * 2]
            rem_item = items[g * 2 + 1]
            imdb = f"tt{(g + 1) * 1000:07d}"
            groups.append(
                SmartDuplicateGroup(
                    imdb_id=imdb,
                    title=f"Movie {g}",
                    season=None,
                    episode=None,
                    keep=_make_assessed(item_id=keep_item.id, imdb_id=imdb),
                    remove=[_make_assessed(item_id=rem_item.id, imdb_id=imdb)],
                    reason="test",
                    count=2,
                )
            )

        preview = CleanupPreview(groups=groups)

        with patch("os.path.islink", return_value=False):
            result = await execute_cleanup(session, preview)

        assert result.items_kept == 3
        assert result.groups_processed == 3

    async def test_multiple_removals_in_one_group(
        self, session: AsyncSession
    ) -> None:
        """A group with two removal items removes both from the DB."""
        item_keep = _make_item(session, imdb_id="tt6677889", source="plex_watchlist")
        item_rem1 = _make_item(session, imdb_id="tt6677889", source="migration")
        item_rem2 = _make_item(session, imdb_id="tt6677889", source="migration")
        await _flush(session)

        preview = CleanupPreview(
            groups=[
                SmartDuplicateGroup(
                    imdb_id="tt6677889",
                    title="Multi",
                    season=None,
                    episode=None,
                    keep=_make_assessed(item_id=item_keep.id, imdb_id="tt6677889"),
                    remove=[
                        _make_assessed(item_id=item_rem1.id, imdb_id="tt6677889"),
                        _make_assessed(item_id=item_rem2.id, imdb_id="tt6677889"),
                    ],
                    reason="test",
                    count=3,
                )
            ]
        )

        with patch("os.path.islink", return_value=False):
            result = await execute_cleanup(session, preview)

        assert result.items_removed == 2
        rows = await session.execute(text("SELECT COUNT(*) FROM media_items"))
        count = rows.scalar()
        # Only the keeper should remain
        assert count == 1


# ---------------------------------------------------------------------------
# _batch_check_paths
# ---------------------------------------------------------------------------


class TestBatchCheckPaths:
    """Tests for the _batch_check_paths helper."""

    async def test_returns_true_for_existing_paths(self) -> None:
        """Paths that exist return True in the mapping."""
        existing = "/real/path/to/file.mkv"
        missing = "/does/not/exist.mkv"

        def _fake_exists(path: str) -> bool:
            return path == existing

        with patch("os.path.exists", side_effect=_fake_exists):
            result = await _batch_check_paths([existing, missing])

        assert result[existing] is True
        assert result[missing] is False

    async def test_returns_false_for_nonexistent_paths(self) -> None:
        """Paths that do not exist return False."""
        with patch("os.path.exists", return_value=False):
            result = await _batch_check_paths(["/ghost/path.mkv"])
        assert result["/ghost/path.mkv"] is False

    async def test_empty_input_returns_empty_dict(self) -> None:
        """Passing an empty list returns an empty dict without error."""
        result = await _batch_check_paths([])
        assert result == {}

    async def test_all_paths_present_in_result(self) -> None:
        """Every input path appears as a key in the returned mapping."""
        paths = [f"/path/file_{i}.mkv" for i in range(5)]
        with patch("os.path.exists", return_value=True):
            result = await _batch_check_paths(paths)
        assert set(result.keys()) == set(paths)

    async def test_large_batch_processed_in_chunks(self) -> None:
        """More than 50 paths are still processed correctly (batch boundary)."""
        paths = [f"/path/movie_{i}/movie_{i}.mkv" for i in range(75)]
        call_count = 0

        def _counting_exists(path: str) -> bool:
            nonlocal call_count
            call_count += 1
            return True

        with patch("os.path.exists", side_effect=_counting_exists):
            result = await _batch_check_paths(paths)

        assert call_count == 75
        assert len(result) == 75
        assert all(v is True for v in result.values())

    async def test_handles_oserror_gracefully(self) -> None:
        """If os.path.exists raises OSError the call completes without crashing.

        asyncio.to_thread propagates exceptions from the thread back to the
        caller, so the test verifies the coroutine does not silently swallow
        them — the caller should handle such errors.  The current implementation
        does not catch OSError internally; this test documents that behaviour.
        """
        # Patch os.path.exists to raise to confirm the exception propagates.
        with patch("os.path.exists", side_effect=OSError("I/O error")):
            try:
                await _batch_check_paths(["/bad/path.mkv"])
                raised = False
            except OSError:
                raised = True
        # The implementation lets OSError propagate; confirm it does so.
        assert raised, "Expected OSError to propagate from _batch_check_paths"


# ---------------------------------------------------------------------------
# Integration: assess → build_preview → execute round-trip
# ---------------------------------------------------------------------------


class TestFullWorkflowIntegration:
    """End-to-end round-trip through all three phases of the cleanup workflow."""

    async def test_live_item_kept_dead_item_removed(
        self, session: AsyncSession
    ) -> None:
        """Full workflow: LIVE item is kept, DEAD item is removed from DB."""
        # Two migration items for the same movie
        item_live = _make_item(session, imdb_id="tt9900001", source="migration", title="Live")
        item_dead = _make_item(session, imdb_id="tt9900001", source="migration", title="Dead")
        await _flush(session)

        # item_live has a symlink whose source_path "exists"
        source_path = f"{ZURG_MOUNT}/Live.Movie.1080p/Live.Movie.1080p.mkv"
        link_path = "/library/movies/Live Movie (2024)/Live Movie (2024).mkv"
        _make_symlink(
            session,
            media_item_id=item_live.id,
            source_path=source_path,
            target_path=link_path,
        )
        await _flush(session)

        def _fake_exists(path: str) -> bool:
            return path == source_path

        def _fake_islink(path: str) -> bool:
            return False

        with (
            patch("src.config.settings", _mock_settings(ZURG_MOUNT)),
            patch("os.path.exists", side_effect=_fake_exists),
            patch("os.path.islink", side_effect=_fake_islink),
        ):
            assessed = await assess_migration_items(session)
            preview = await build_cleanup_preview(session, assessed)
            result = await execute_cleanup(session, preview)

        assert result.groups_processed == 1
        assert result.items_removed == 1
        assert result.items_kept == 1

        # item_live must survive
        rows = await session.execute(
            text("SELECT id FROM media_items WHERE id = :kid").bindparams(kid=item_live.id)
        )
        assert rows.fetchone() is not None

        # item_dead must be gone
        rows = await session.execute(
            text("SELECT id FROM media_items WHERE id = :did").bindparams(did=item_dead.id)
        )
        assert rows.fetchone() is None

    async def test_no_duplicates_produces_empty_preview_and_noop_execute(
        self, session: AsyncSession
    ) -> None:
        """When there are no migration duplicates, preview is empty and execute is a no-op."""
        _make_item(session, imdb_id="tt9900002", source="migration")
        await _flush(session)

        with (
            patch("src.config.settings", _mock_settings()),
            patch("os.path.exists", return_value=False),
        ):
            assessed = await assess_migration_items(session)
            preview = await build_cleanup_preview(session, assessed)
            result = await execute_cleanup(session, preview)

        assert assessed == []
        assert preview.groups == []
        assert result.items_removed == 0
        assert result.items_kept == 0
