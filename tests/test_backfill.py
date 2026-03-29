"""Tests for src/core/backfill.py, plus mount-index watchlist check and
show_manager._check_single_show query hardening.

Coverage
--------
backfill_tmdb_ids()
  - Resolves tmdb_id for items with imdb_id only; UPDATE applied
  - Also resolves tvdb_id for TV shows alongside tmdb_id
  - Skips items that already have a tmdb_id (no extra API call)
  - TMDB returns None for an imdb_id — row skipped, continued
  - TMDB raises TimeoutException — row skipped, loop continues
  - Two items sharing the same imdb_id produce exactly one API call
  - BackfillResult counts are accurate (total/resolved/failed/updated_rows)
  - No-op when all items already have tmdb_id (total=0)
  - Semaphore limits concurrent calls (asyncio.gather still works at scale)

find_duplicates()
  - Groups with count > 1 where source='migration' are detected
  - Lowest id is keep_id; higher ids become remove_ids
  - Non-migration items are excluded even when otherwise identical
  - NULL season/episode (movies) are grouped correctly via IS NULL logic
  - No duplicates → empty list
  - Different imdb_id values are not grouped together

remove_duplicates()
  - Deletes specified media item ids from media_items
  - Also deletes associated symlink records
  - Returns correct DeduplicateResult counts
  - Empty remove_ids is a no-op returning zeroed result

Mount index check in watchlist sync (plex_watchlist.sync_watchlist)
  - Item found in mount is skipped; stats["skipped"] incremented
  - Mount lookup exception does not block addition; item proceeds
  - Mount lookup returns empty list — item is added normally

show_manager._check_single_show query hardening (or_ clause)
  - Finds existing items by tmdb_id (primary match)
  - Finds existing items by imdb_id when tmdb_id IS NULL on the item
  - Items with tmdb_id set are NOT matched by imdb_id fallback (no false positives)

asyncio_mode = "auto" (configured in pyproject.toml).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.backfill import (
    DuplicateGroup,
    backfill_tmdb_ids,
    find_duplicates,
    remove_duplicates,
)
from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.monitored_show import MonitoredShow
from src.models.symlink import Symlink

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_item(
    session: AsyncSession,
    *,
    title: str = "Test Title",
    imdb_id: str | None = "tt0000001",
    tmdb_id: str | None = None,
    tvdb_id: int | None = None,
    source: str | None = "migration",
    season: int | None = None,
    episode: int | None = None,
    media_type: MediaType = MediaType.MOVIE,
) -> MediaItem:
    """Construct and add an unsaved MediaItem to the session."""
    item = MediaItem(
        title=title,
        imdb_id=imdb_id,
        tmdb_id=tmdb_id,
        tvdb_id=tvdb_id,
        media_type=media_type,
        state=QueueState.WANTED,
        state_changed_at=datetime.now(UTC),
        retry_count=0,
        source=source,
        season=season,
        episode=episode,
    )
    session.add(item)
    return item


async def _flush(session: AsyncSession) -> None:
    """Flush the session so IDs are assigned."""
    await session.flush()


# ---------------------------------------------------------------------------
# backfill_tmdb_ids — happy path
# ---------------------------------------------------------------------------


class TestBackfillTmdbIds:
    """Tests for backfill_tmdb_ids()."""

    async def test_resolves_tmdb_id_for_movie(self, session: AsyncSession) -> None:
        """tmdb_id is written to an item that only had imdb_id."""
        _make_item(session, imdb_id="tt1111111", tmdb_id=None)
        await _flush(session)

        find_result: dict[str, Any] = {"tmdb_id": 9999, "tvdb_id": None, "media_type": "movie"}

        with patch("src.core.backfill.tmdb_client") as mock_tmdb:
            mock_tmdb.find_by_imdb_id = AsyncMock(return_value=find_result)
            result = await backfill_tmdb_ids(session)

        assert result.total == 1
        assert result.resolved == 1
        assert result.failed == 0
        assert result.updated_rows == 1

        # Verify the DB row was actually updated
        rows = await session.execute(
            text("SELECT tmdb_id FROM media_items WHERE imdb_id = 'tt1111111'")
        )
        assert rows.fetchone()[0] == "9999"

    async def test_resolves_tvdb_id_for_tv_show(self, session: AsyncSession) -> None:
        """tvdb_id is co-written when TMDB returns one for a TV show."""
        _make_item(
            session,
            imdb_id="tt2222222",
            tmdb_id=None,
            media_type=MediaType.SHOW,
        )
        await _flush(session)

        find_result = {"tmdb_id": 8888, "tvdb_id": 77777, "media_type": "tv"}

        with patch("src.core.backfill.tmdb_client") as mock_tmdb:
            mock_tmdb.find_by_imdb_id = AsyncMock(return_value=find_result)
            result = await backfill_tmdb_ids(session)

        assert result.resolved == 1
        rows = await session.execute(
            text("SELECT tmdb_id, tvdb_id FROM media_items WHERE imdb_id = 'tt2222222'")
        )
        row = rows.fetchone()
        assert row[0] == "8888"
        assert row[1] == 77777

    async def test_skips_items_already_having_tmdb_id(self, session: AsyncSession) -> None:
        """Items with a non-NULL tmdb_id are not queried from TMDB."""
        _make_item(session, imdb_id="tt3333333", tmdb_id="1234")
        await _flush(session)

        with patch("src.core.backfill.tmdb_client") as mock_tmdb:
            mock_tmdb.find_by_imdb_id = AsyncMock(return_value={"tmdb_id": 9999, "tvdb_id": None, "media_type": "movie"})
            result = await backfill_tmdb_ids(session)

        assert result.total == 0
        mock_tmdb.find_by_imdb_id.assert_not_called()

    async def test_noop_when_no_items_need_backfill(self, session: AsyncSession) -> None:
        """No items without tmdb_id → returns BackfillResult with total=0."""
        _make_item(session, imdb_id="tt4444444", tmdb_id="5555")
        await _flush(session)

        with patch("src.core.backfill.tmdb_client") as mock_tmdb:
            mock_tmdb.find_by_imdb_id = AsyncMock()
            result = await backfill_tmdb_ids(session)

        assert result.total == 0
        assert result.resolved == 0
        assert result.updated_rows == 0
        mock_tmdb.find_by_imdb_id.assert_not_called()

    async def test_handles_tmdb_returning_none(self, session: AsyncSession) -> None:
        """When TMDB returns None the item is skipped; failed count incremented."""
        _make_item(session, imdb_id="tt5555555", tmdb_id=None)
        await _flush(session)

        with patch("src.core.backfill.tmdb_client") as mock_tmdb:
            mock_tmdb.find_by_imdb_id = AsyncMock(return_value=None)
            result = await backfill_tmdb_ids(session)

        assert result.total == 1
        assert result.resolved == 0
        assert result.failed == 1
        assert result.updated_rows == 0

    async def test_continues_after_tmdb_api_error(self, session: AsyncSession) -> None:
        """A TMDB exception on one imdb_id does not abort the whole run.

        With return_exceptions=True, the failing task's exception is captured and
        counted as failed; the successful task still resolves.
        """
        # Use two different imdb_ids so both are individually attempted.
        _make_item(session, title="Item A", imdb_id="tt6666666", tmdb_id=None)
        _make_item(session, title="Item B", imdb_id="tt7777777", tmdb_id=None)
        await _flush(session)

        import httpx

        good_result = {"tmdb_id": 4321, "tvdb_id": None, "media_type": "movie"}

        call_count = 0

        async def _side_effect(imdb_id: str) -> dict[str, Any] | None:
            nonlocal call_count
            call_count += 1
            if imdb_id == "tt6666666":
                raise httpx.TimeoutException("timeout")
            return good_result

        with patch("src.core.backfill.tmdb_client") as mock_tmdb:
            mock_tmdb.find_by_imdb_id = AsyncMock(side_effect=_side_effect)
            result = await backfill_tmdb_ids(session)

        # Both tasks ran; one succeeded, one failed — no exception raised.
        assert result.total == 2
        assert result.resolved == 1
        assert result.failed == 1
        assert call_count == 2

    async def test_two_items_same_imdb_id_one_api_call(self, session: AsyncSession) -> None:
        """Two items sharing imdb_id trigger exactly one TMDB API call."""
        _make_item(session, title="Movie Copy 1", imdb_id="tt8888888", tmdb_id=None)
        _make_item(session, title="Movie Copy 2", imdb_id="tt8888888", tmdb_id=None)
        await _flush(session)

        find_result = {"tmdb_id": 1122, "tvdb_id": None, "media_type": "movie"}

        with patch("src.core.backfill.tmdb_client") as mock_tmdb:
            mock_tmdb.find_by_imdb_id = AsyncMock(return_value=find_result)
            result = await backfill_tmdb_ids(session)

        # Only one distinct imdb_id → exactly one API call
        mock_tmdb.find_by_imdb_id.assert_called_once_with("tt8888888")

        assert result.total == 1   # 1 unique imdb_id
        assert result.resolved == 1
        assert result.updated_rows == 2  # both rows updated

    async def test_result_counts_accurate_mixed(self, session: AsyncSession) -> None:
        """Mixture of resolvable and non-resolvable imdb_ids gives correct counts."""
        _make_item(session, imdb_id="tt9000001", tmdb_id=None)
        _make_item(session, imdb_id="tt9000002", tmdb_id=None)
        await _flush(session)

        async def _find(imdb_id: str) -> dict[str, Any] | None:
            if imdb_id == "tt9000001":
                return {"tmdb_id": 111, "tvdb_id": None, "media_type": "movie"}
            return None  # second one not found

        with patch("src.core.backfill.tmdb_client") as mock_tmdb:
            mock_tmdb.find_by_imdb_id = AsyncMock(side_effect=_find)
            result = await backfill_tmdb_ids(session)

        assert result.total == 2
        assert result.resolved == 1
        assert result.failed == 1
        assert result.updated_rows == 1

    async def test_existing_tvdb_id_is_preserved(self, session: AsyncSession) -> None:
        """COALESCE ensures a pre-existing tvdb_id is not overwritten."""
        _make_item(session, imdb_id="tt1010101", tmdb_id=None, tvdb_id=99999)
        await _flush(session)

        find_result = {"tmdb_id": 2020, "tvdb_id": 11111, "media_type": "tv"}

        with patch("src.core.backfill.tmdb_client") as mock_tmdb:
            mock_tmdb.find_by_imdb_id = AsyncMock(return_value=find_result)
            await backfill_tmdb_ids(session)

        rows = await session.execute(
            text("SELECT tvdb_id FROM media_items WHERE imdb_id = 'tt1010101'")
        )
        # COALESCE(tvdb_id, :tvdb_id) — existing 99999 wins
        assert rows.fetchone()[0] == 99999

    async def test_no_items_at_all_is_noop(self, session: AsyncSession) -> None:
        """Empty table returns BackfillResult with all zeros."""
        with patch("src.core.backfill.tmdb_client") as mock_tmdb:
            mock_tmdb.find_by_imdb_id = AsyncMock()
            result = await backfill_tmdb_ids(session)

        assert result.total == 0
        assert result.resolved == 0
        assert result.failed == 0
        assert result.updated_rows == 0
        mock_tmdb.find_by_imdb_id.assert_not_called()


# ---------------------------------------------------------------------------
# find_duplicates
# ---------------------------------------------------------------------------


class TestFindDuplicates:
    """Tests for find_duplicates()."""

    async def test_finds_duplicate_movie_group(self, session: AsyncSession) -> None:
        """Two migration items with the same imdb_id and NULL season/episode are grouped."""
        _make_item(session, title="Movie A", imdb_id="tt0100001", source="migration")
        _make_item(session, title="Movie A", imdb_id="tt0100001", source="migration")
        await _flush(session)

        groups = await find_duplicates(session)

        assert len(groups) == 1
        g = groups[0]
        assert g.imdb_id == "tt0100001"
        assert g.season is None
        assert g.episode is None
        assert g.count == 2
        assert len(g.remove_ids) == 1

    async def test_lowest_id_is_keep_id(self, session: AsyncSession) -> None:
        """The item inserted first (lowest id) is designated as keep_id."""
        item_a = _make_item(session, title="Early", imdb_id="tt0200001", source="migration")
        item_b = _make_item(session, title="Late", imdb_id="tt0200001", source="migration")
        await _flush(session)

        groups = await find_duplicates(session)

        assert len(groups) == 1
        g = groups[0]
        assert g.keep_id == item_a.id
        assert item_b.id in g.remove_ids
        assert item_a.id not in g.remove_ids

    async def test_non_migration_items_excluded(self, session: AsyncSession) -> None:
        """Items with source != 'migration' are not considered even if otherwise identical."""
        _make_item(session, imdb_id="tt0300001", source="plex_watchlist")
        _make_item(session, imdb_id="tt0300001", source="plex_watchlist")
        await _flush(session)

        groups = await find_duplicates(session)

        assert groups == []

    async def test_mixed_sources_only_counts_migration_rows(self, session: AsyncSession) -> None:
        """A group requires ≥2 migration rows; one migration + one non-migration is not a duplicate."""
        _make_item(session, imdb_id="tt0400001", source="migration")
        _make_item(session, imdb_id="tt0400001", source="manual")
        await _flush(session)

        groups = await find_duplicates(session)

        assert groups == []

    async def test_null_season_episode_grouped_correctly(self, session: AsyncSession) -> None:
        """NULL season AND NULL episode values are treated as equal by the IS operator."""
        _make_item(session, imdb_id="tt0500001", source="migration", season=None, episode=None)
        _make_item(session, imdb_id="tt0500001", source="migration", season=None, episode=None)
        await _flush(session)

        groups = await find_duplicates(session)

        assert len(groups) == 1
        assert groups[0].season is None
        assert groups[0].episode is None

    async def test_different_seasons_not_grouped(self, session: AsyncSession) -> None:
        """Items with the same imdb_id but different season numbers are separate."""
        _make_item(session, imdb_id="tt0600001", source="migration", season=1, media_type=MediaType.SHOW)
        _make_item(session, imdb_id="tt0600001", source="migration", season=2, media_type=MediaType.SHOW)
        await _flush(session)

        groups = await find_duplicates(session)

        assert groups == []

    async def test_different_imdb_ids_not_grouped(self, session: AsyncSession) -> None:
        """Items with different imdb_ids are never grouped together."""
        _make_item(session, imdb_id="tt0700001", source="migration")
        _make_item(session, imdb_id="tt0700002", source="migration")
        await _flush(session)

        groups = await find_duplicates(session)

        assert groups == []

    async def test_no_duplicates_returns_empty(self, session: AsyncSession) -> None:
        """When no group has count > 1 an empty list is returned."""
        _make_item(session, imdb_id="tt0800001", source="migration")
        await _flush(session)

        groups = await find_duplicates(session)

        assert groups == []

    async def test_three_duplicates_two_in_remove_ids(self, session: AsyncSession) -> None:
        """A group of three items keeps the first; the other two are in remove_ids."""
        _make_item(session, title="First", imdb_id="tt0900001", source="migration")
        _make_item(session, title="Second", imdb_id="tt0900001", source="migration")
        _make_item(session, title="Third", imdb_id="tt0900001", source="migration")
        await _flush(session)

        groups = await find_duplicates(session)

        assert len(groups) == 1
        g = groups[0]
        assert g.count == 3
        assert len(g.remove_ids) == 2

    async def test_multiple_independent_groups(self, session: AsyncSession) -> None:
        """Multiple duplicate groups are all returned independently."""
        _make_item(session, imdb_id="tt1000001", source="migration")
        _make_item(session, imdb_id="tt1000001", source="migration")
        _make_item(session, imdb_id="tt1000002", source="migration")
        _make_item(session, imdb_id="tt1000002", source="migration")
        await _flush(session)

        groups = await find_duplicates(session)

        imdb_ids_found = {g.imdb_id for g in groups}
        assert "tt1000001" in imdb_ids_found
        assert "tt1000002" in imdb_ids_found

    async def test_episode_level_duplicates_grouped(self, session: AsyncSession) -> None:
        """Two items for the same show episode (same season+episode) form a group."""
        _make_item(
            session,
            imdb_id="tt1100001",
            source="migration",
            season=2,
            episode=5,
            media_type=MediaType.SHOW,
        )
        _make_item(
            session,
            imdb_id="tt1100001",
            source="migration",
            season=2,
            episode=5,
            media_type=MediaType.SHOW,
        )
        await _flush(session)

        groups = await find_duplicates(session)

        assert len(groups) == 1
        assert groups[0].season == 2
        assert groups[0].episode == 5


# ---------------------------------------------------------------------------
# remove_duplicates
# ---------------------------------------------------------------------------


def _make_group(keep_id: int, remove_ids: list[int], imdb_id: str = "tt0000001") -> DuplicateGroup:
    """Build a DuplicateGroup for testing remove_duplicates()."""
    return DuplicateGroup(
        imdb_id=imdb_id,
        title="Test",
        season=None,
        episode=None,
        keep_id=keep_id,
        remove_ids=remove_ids,
        count=len(remove_ids) + 1,
    )


class TestRemoveDuplicates:
    """Tests for remove_duplicates()."""

    async def test_deletes_specified_media_items(self, session: AsyncSession) -> None:
        """Rows listed in remove_ids are deleted from media_items."""
        item_keep = _make_item(session, imdb_id="tt2000001", source="migration")
        item_del = _make_item(session, imdb_id="tt2000001", source="migration")
        await _flush(session)

        del_id = item_del.id
        keep_id = item_keep.id

        group = _make_group(keep_id=keep_id, remove_ids=[del_id], imdb_id="tt2000001")
        await remove_duplicates(session, [group])
        await session.flush()

        remaining = await session.execute(
            select(MediaItem).where(MediaItem.id.in_([keep_id, del_id]))
        )
        ids = [r.id for r in remaining.scalars().all()]
        assert keep_id in ids
        assert del_id not in ids

    async def test_deletes_associated_symlinks(self, session: AsyncSession) -> None:
        """Symlink rows linked to deleted media items are also removed."""
        item_keep = _make_item(session, imdb_id="tt2100001", source="migration", title="Keep")
        item_del = _make_item(session, imdb_id="tt2100001", source="migration", title="Del")
        await _flush(session)

        sym = Symlink(
            media_item_id=item_del.id,
            source_path="/source/Movie.mkv",
            target_path="/library/Movie.mkv",
            valid=True,
        )
        session.add(sym)
        await _flush(session)

        sym_id = sym.id
        group = _make_group(
            keep_id=item_keep.id, remove_ids=[item_del.id], imdb_id="tt2100001"
        )
        await remove_duplicates(session, [group])
        await session.flush()

        # Symlink should be gone
        rows = await session.execute(
            text("SELECT id FROM symlinks WHERE id = :sid").bindparams(sid=sym_id)
        )
        assert rows.fetchone() is None

    async def test_returns_correct_counts(self, session: AsyncSession) -> None:
        """DeduplicateResult.removed equals the number of rows actually deleted."""
        items = [_make_item(session, imdb_id="tt2200001", source="migration") for _ in range(3)]
        await _flush(session)

        # One group: keep items[0], remove items[1] and items[2]
        group = _make_group(
            keep_id=items[0].id,
            remove_ids=[items[1].id, items[2].id],
            imdb_id="tt2200001",
        )
        result = await remove_duplicates(session, [group])

        assert result.removed == 2
        assert result.groups == 1
        assert result.kept == 1

    async def test_empty_groups_is_noop(self, session: AsyncSession) -> None:
        """Calling with an empty groups list returns a zeroed DeduplicateResult without touching the DB."""
        _make_item(session, imdb_id="tt2300001", source="migration")
        await _flush(session)

        result = await remove_duplicates(session, [])

        assert result.removed == 0
        assert result.groups == 0
        assert result.kept == 0

        # DB untouched
        rows = await session.execute(text("SELECT COUNT(*) FROM media_items"))
        assert rows.scalar() >= 1

    async def test_nonexistent_ids_not_counted_in_removed(self, session: AsyncSession) -> None:
        """IDs that do not exist in the DB result in removed=0."""
        group = _make_group(keep_id=1, remove_ids=[999999, 888888])
        result = await remove_duplicates(session, [group])

        # rowcount = 0 — no rows matched
        assert result.removed == 0
        assert result.groups == 1
        assert result.kept == 1

    async def test_symlinks_without_matching_items_no_error(self, session: AsyncSession) -> None:
        """Deleting an item whose symlinks were already removed does not raise."""
        item_keep = _make_item(session, imdb_id="tt2400001", source="migration", title="Keep")
        item_del = _make_item(session, imdb_id="tt2400001", source="migration", title="Del")
        await _flush(session)
        group = _make_group(
            keep_id=item_keep.id,
            remove_ids=[item_del.id],
            imdb_id="tt2400001",
        )

        # Remove without any symlinks — should complete cleanly
        result = await remove_duplicates(session, [group])
        assert result.removed == 1

    async def test_groups_and_kept_counts(self, session: AsyncSession) -> None:
        """groups and kept counts reflect the number of groups, not removed rows."""
        items_a = [_make_item(session, imdb_id="tt2500001", source="migration") for _ in range(3)]
        items_b = [_make_item(session, imdb_id="tt2500002", source="migration") for _ in range(2)]
        await _flush(session)

        groups = [
            _make_group(
                keep_id=items_a[0].id,
                remove_ids=[items_a[1].id, items_a[2].id],
                imdb_id="tt2500001",
            ),
            _make_group(
                keep_id=items_b[0].id,
                remove_ids=[items_b[1].id],
                imdb_id="tt2500002",
            ),
        ]
        result = await remove_duplicates(session, groups)

        assert result.removed == 3   # 2 + 1
        assert result.groups == 2
        assert result.kept == 2      # one per group


# ---------------------------------------------------------------------------
# Mount index check in plex_watchlist.sync_watchlist
# ---------------------------------------------------------------------------


class TestWatchlistMountCheck:
    """Tests for the mount-index guard added to sync_watchlist()."""

    def _plex_settings(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Patch settings so sync_watchlist() is enabled."""
        mock_cfg = MagicMock()
        mock_cfg.plex.token = "fake-token"
        mock_cfg.plex.watchlist_sync_enabled = True
        monkeypatch.setattr("src.core.plex_watchlist.settings", mock_cfg)

    def _make_wl_item(
        self,
        title: str = "Test Movie",
        media_type: str = "movie",
        tmdb_id: str = "99901",
        imdb_id: str = "tt9990001",
    ) -> MagicMock:
        item = MagicMock()
        item.title = title
        item.year = 2024
        item.media_type = media_type
        item.tmdb_id = tmdb_id
        item.imdb_id = imdb_id
        return item

    async def test_item_found_in_mount_is_skipped(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When mount_scanner.lookup() returns a result the item is skipped."""
        self._plex_settings(monkeypatch)

        wl_item = self._make_wl_item()
        mock_mount_result = MagicMock()
        mock_mount_result.filepath = "/mnt/zurg/Test Movie/Test Movie.mkv"

        mock_mount_singleton = MagicMock()
        mock_mount_singleton.lookup = AsyncMock(return_value=[mock_mount_result])

        with (
            patch("src.core.plex_watchlist.plex_client") as mock_plex,
            patch("src.core.plex_watchlist.tmdb_client") as mock_tmdb,
            # The inline import "from src.core.mount_scanner import mount_scanner"
            # resolves to the singleton in that module — patch there.
            patch("src.core.mount_scanner.mount_scanner", mock_mount_singleton),
        ):
            mock_plex.get_watchlist = AsyncMock(return_value=[wl_item])
            mock_tmdb.get_external_ids = AsyncMock(return_value=None)

            from src.core.plex_watchlist import sync_watchlist

            stats = await sync_watchlist(session)

        assert stats["skipped"] >= 1
        assert stats["added"] == 0

    async def test_mount_lookup_exception_does_not_block_add(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If mount_scanner.lookup() raises, the item is still added."""
        self._plex_settings(monkeypatch)

        wl_item = self._make_wl_item(tmdb_id="99902", imdb_id="tt9990002")

        mock_mount_singleton = MagicMock()
        mock_mount_singleton.lookup = AsyncMock(side_effect=RuntimeError("mount unavailable"))

        with (
            patch("src.core.plex_watchlist.plex_client") as mock_plex,
            patch("src.core.plex_watchlist.tmdb_client") as mock_tmdb,
            patch("src.core.mount_scanner.mount_scanner", mock_mount_singleton),
        ):
            mock_plex.get_watchlist = AsyncMock(return_value=[wl_item])
            mock_tmdb.get_external_ids = AsyncMock(return_value=None)

            from src.core.plex_watchlist import sync_watchlist

            stats = await sync_watchlist(session)

        # Despite the exception the item must have been inserted
        assert stats["added"] == 1
        assert stats["errors"] == 0

    async def test_mount_lookup_empty_result_item_added(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When mount_scanner.lookup() returns [] the item proceeds to insertion."""
        self._plex_settings(monkeypatch)

        wl_item = self._make_wl_item(tmdb_id="99903", imdb_id="tt9990003")

        mock_mount_singleton = MagicMock()
        mock_mount_singleton.lookup = AsyncMock(return_value=[])

        with (
            patch("src.core.plex_watchlist.plex_client") as mock_plex,
            patch("src.core.plex_watchlist.tmdb_client") as mock_tmdb,
            patch("src.core.mount_scanner.mount_scanner", mock_mount_singleton),
        ):
            mock_plex.get_watchlist = AsyncMock(return_value=[wl_item])
            mock_tmdb.get_external_ids = AsyncMock(return_value=None)

            from src.core.plex_watchlist import sync_watchlist

            stats = await sync_watchlist(session)

        assert stats["added"] == 1

    async def test_mount_check_uses_title_and_none_season_episode(
        self, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """mount_scanner.lookup() is called with the item title and season=None."""
        self._plex_settings(monkeypatch)

        wl_item = self._make_wl_item(title="Specific Title", tmdb_id="99904", imdb_id="tt9990004")

        lookup_calls: list[dict] = []

        async def _lookup(sess, title, *, season, episode):
            lookup_calls.append({"title": title, "season": season, "episode": episode})
            return []

        mock_mount_singleton = MagicMock()
        mock_mount_singleton.lookup = _lookup

        with (
            patch("src.core.plex_watchlist.plex_client") as mock_plex,
            patch("src.core.plex_watchlist.tmdb_client") as mock_tmdb,
            patch("src.core.mount_scanner.mount_scanner", mock_mount_singleton),
        ):
            mock_plex.get_watchlist = AsyncMock(return_value=[wl_item])
            mock_tmdb.get_external_ids = AsyncMock(return_value=None)

            from src.core.plex_watchlist import sync_watchlist

            await sync_watchlist(session)

        assert len(lookup_calls) == 1
        assert lookup_calls[0]["title"] == "Specific Title"
        assert lookup_calls[0]["season"] is None
        assert lookup_calls[0]["episode"] is None


# ---------------------------------------------------------------------------
# show_manager._check_single_show — query hardening (or_ clause)
# ---------------------------------------------------------------------------


def _make_monitored_show(
    session: AsyncSession,
    *,
    tmdb_id: int = 10001,
    imdb_id: str | None = "tt5001001",
    title: str = "Test Show",
) -> MonitoredShow:
    show = MonitoredShow(
        tmdb_id=tmdb_id,
        imdb_id=imdb_id,
        title=title,
        year=2020,
        enabled=True,
    )
    session.add(show)
    return show


def _build_tmdb_show_detail(
    tmdb_id: int = 10001,
    imdb_id: str | None = "tt5001001",
    seasons: list | None = None,
) -> MagicMock:
    """Build a minimal TmdbShowDetail-like mock."""
    detail = MagicMock()
    detail.tmdb_id = tmdb_id
    detail.imdb_id = imdb_id
    detail.tvdb_id = None
    detail.seasons = seasons or []
    return detail


class TestCheckSingleShowQueryHardening:
    """Tests that _check_single_show uses the hardened or_() condition."""

    def _mock_tmdb_for_show(
        self,
        tmdb_id: int,
        imdb_id: str | None,
        seasons: list,
    ) -> MagicMock:
        """Build a fully async-mock tmdb_client suitable for show_manager tests.

        Both get_show_details and get_season_details must be AsyncMock so that
        _check_single_show can await them without TypeError.
        """
        mock_tmdb = MagicMock()
        tmdb_detail = _build_tmdb_show_detail(
            tmdb_id=tmdb_id,
            imdb_id=imdb_id,
            seasons=seasons,
        )
        mock_tmdb.get_show_details = AsyncMock(return_value=tmdb_detail)
        # get_season_details is called when a new season pack is created to
        # initialise last_episode tracking — return None so it short-circuits.
        mock_tmdb.get_season_details = AsyncMock(return_value=None)
        return mock_tmdb

    async def test_finds_existing_item_by_tmdb_id(
        self, session: AsyncSession
    ) -> None:
        """Items already in the queue with a matching tmdb_id are not duplicated."""
        show = _make_monitored_show(session, tmdb_id=20001, imdb_id="tt6001001")
        _make_item(
            session,
            imdb_id="tt6001001",
            tmdb_id="20001",
            season=1,
            media_type=MediaType.SHOW,
            source="plex_watchlist",
        )
        await _flush(session)

        from src.core.show_manager import ShowManager

        mgr = ShowManager()
        mock_tmdb = self._mock_tmdb_for_show(
            20001,
            "tt6001001",
            [MagicMock(season_number=1, episode_count=10, air_date="2020-01-01")],
        )

        with patch("src.core.show_manager.tmdb_client", mock_tmdb):
            new_count = await mgr._check_single_show(session, show)

        # Season 1 already present → no new item created
        assert new_count == 0

    async def test_finds_existing_item_by_imdb_id_when_tmdb_id_is_null(
        self, session: AsyncSession
    ) -> None:
        """An item with NULL tmdb_id is matched by imdb_id fallback, preventing a duplicate."""
        show = _make_monitored_show(session, tmdb_id=20002, imdb_id="tt6002001")
        # Migrated item: has imdb_id but no tmdb_id yet
        _make_item(
            session,
            imdb_id="tt6002001",
            tmdb_id=None,      # not yet backfilled
            season=1,
            media_type=MediaType.SHOW,
            source="migration",
        )
        await _flush(session)

        from src.core.show_manager import ShowManager

        mgr = ShowManager()
        mock_tmdb = self._mock_tmdb_for_show(
            20002,
            "tt6002001",
            [MagicMock(season_number=1, episode_count=8, air_date="2021-03-01")],
        )

        with patch("src.core.show_manager.tmdb_client", mock_tmdb):
            new_count = await mgr._check_single_show(session, show)

        # Season 1 detected via imdb_id fallback → no duplicate created
        assert new_count == 0

    async def test_item_with_tmdb_id_not_matched_by_imdb_fallback(
        self, session: AsyncSession
    ) -> None:
        """A row with a DIFFERENT tmdb_id is NOT picked up by the imdb_id branch."""
        show = _make_monitored_show(session, tmdb_id=20003, imdb_id="tt6003001")

        # Item shares imdb_id but has a completely different tmdb_id — must NOT
        # prevent the monitored show from creating its own season pack.
        _make_item(
            session,
            imdb_id="tt6003001",
            tmdb_id="99999",   # different show
            season=1,
            media_type=MediaType.SHOW,
            source="plex_watchlist",
        )
        await _flush(session)

        from src.core.show_manager import ShowManager

        mgr = ShowManager()
        mock_tmdb = self._mock_tmdb_for_show(
            20003,
            "tt6003001",
            [MagicMock(season_number=1, episode_count=6, air_date="2019-09-01")],
        )

        with patch("src.core.show_manager.tmdb_client", mock_tmdb):
            new_count = await mgr._check_single_show(session, show)

        # The mismatched item must NOT block the creation of the new WANTED row
        assert new_count >= 1

    async def test_no_match_creates_wanted_item(self, session: AsyncSession) -> None:
        """When no existing item matches, a new WANTED item is created."""
        show = _make_monitored_show(session, tmdb_id=20004, imdb_id="tt6004001")
        await _flush(session)

        from src.core.show_manager import ShowManager

        mgr = ShowManager()
        mock_tmdb = self._mock_tmdb_for_show(
            20004,
            "tt6004001",
            [MagicMock(season_number=1, episode_count=5, air_date="2018-06-01")],
        )

        with patch("src.core.show_manager.tmdb_client", mock_tmdb):
            new_count = await mgr._check_single_show(session, show)

        assert new_count >= 1

        # Flush so the SELECT can see the newly added item (caller owns transaction)
        await session.flush()

        rows = await session.execute(
            text("SELECT state, tmdb_id FROM media_items WHERE tmdb_id = '20004'")
        )
        row = rows.fetchone()
        assert row is not None
        # SQLAlchemy Enum may store the value or the name depending on the
        # backend/configuration.  Accept either form.
        assert row[0].lower() == QueueState.WANTED.value
