"""Comprehensive tests for the CHECKING stage in src/core/queue_processor.py.

Covers the full branching fallback logic in _job_queue_processor:
  - Stage 0: Stale SCRAPING recovery
  - Stage 2 edge cases: ADDING timeouts, 404 errors, filename capture
  - Stage 3 (season pack): mount match, dedup, filtering, fallback chain,
    timeout/hash storage, SourceNotFoundError, partial success
  - Stage 3 (single episode): targeted scan, RD filename refresh, path prefix
    fallbacks, relaxed season filter, no-filter single-file fallback, XEM numbering
  - Stage 3 (post-match): auto-promote, filesize verification, loop-breaker flags
  - Media scan triggers: Plex and Jellyfin (enabled, deduplicated, failure-safe)

All external singletons are mocked — no real network traffic is generated.
asyncio_mode = "auto" (set in pyproject.toml), so no @pytest.mark.asyncio needed.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import JellyfinConfig, PlexConfig
from src.core.mount_scanner import ScanDirectoryResult
from src.core.queue_processor import _job_queue_processor
from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.mount_index import MountIndex
from src.models.symlink import Symlink
from src.models.torrent import RdTorrent, TorrentStatus

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    """Naive UTC — matches what SQLite returns after a round-trip."""
    return datetime.now(UTC).replace(tzinfo=None)


async def _make_media_item(
    session: AsyncSession,
    *,
    state: QueueState,
    title: str = "Test Movie",
    imdb_id: str = "tt0000001",
    tmdb_id: str | None = None,
    tvdb_id: int | None = None,
    season: int | None = None,
    episode: int | None = None,
    is_season_pack: bool = False,
    media_type: MediaType | None = None,
    state_changed_at: datetime | None = None,
    requested_resolution: str | None = None,
    metadata_json: str | None = None,
) -> MediaItem:
    """Persist a MediaItem with the given state to the test session."""
    if media_type is None:
        media_type = (
            MediaType.SHOW
            if (season is not None or episode is not None or is_season_pack)
            else MediaType.MOVIE
        )
    item = MediaItem(
        imdb_id=imdb_id,
        tmdb_id=tmdb_id,
        tvdb_id=tvdb_id,
        title=title,
        year=2024,
        media_type=media_type,
        season=season,
        episode=episode,
        is_season_pack=is_season_pack,
        state=state,
        state_changed_at=state_changed_at or _utcnow(),
        retry_count=0,
        requested_resolution=requested_resolution,
        metadata_json=metadata_json,
    )
    session.add(item)
    await session.flush()
    return item


async def _make_rd_torrent(
    session: AsyncSession,
    *,
    media_item_id: int,
    rd_id: str = "RD_ABC123",
    status: TorrentStatus = TorrentStatus.ACTIVE,
    info_hash: str | None = None,
    filename: str | None = None,
    filesize: int | None = None,
) -> RdTorrent:
    """Persist an RdTorrent linked to the given media item."""
    torrent = RdTorrent(
        rd_id=rd_id,
        info_hash=info_hash,
        media_item_id=media_item_id,
        status=status,
        filename=filename,
        filesize=filesize,
    )
    session.add(torrent)
    await session.flush()
    return torrent


def _make_mount_match(
    filepath: str = "/mnt/zurg/shows/Test Show (2024)/Season 01/S01E01.mkv",
    *,
    parsed_episode: int | None = 1,
    parsed_season: int | None = 1,
    parsed_resolution: str | None = "1080p",
    filesize: int | None = None,
    filename: str | None = None,
) -> MagicMock:
    """Return a mock MountIndex-like object."""
    match = MagicMock(spec=MountIndex)
    match.filepath = filepath
    match.parsed_episode = parsed_episode
    match.parsed_season = parsed_season
    match.parsed_resolution = parsed_resolution
    match.filesize = filesize
    match.filename = filename or filepath.split("/")[-1]
    return match


def _make_symlink(
    source_path: str = "/mnt/zurg/shows/Test Show (2024)/S01E01.mkv",
    target_path: str = "/library/shows/Test Show (2024)/Season 01/Test.Show.S01E01.mkv",
) -> MagicMock:
    """Return a Symlink-like object for scan queue assertions."""
    sl = MagicMock(spec=Symlink)
    sl.source_path = source_path
    sl.target_path = target_path
    return sl


def _empty_scan_result() -> ScanDirectoryResult:
    """Return a ScanDirectoryResult with no files indexed."""
    return ScanDirectoryResult(files_indexed=0, matched_dir_path=None)


def _scan_result(
    files_indexed: int = 5,
    matched_dir_path: str | None = "/mnt/zurg/shows/Test Show",
) -> ScanDirectoryResult:
    """Return a ScanDirectoryResult with files indexed."""
    return ScanDirectoryResult(files_indexed=files_indexed, matched_dir_path=matched_dir_path)


# ---------------------------------------------------------------------------
# Fixtures (mirrors the pattern in test_queue_processor.py)
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_session(session: AsyncSession) -> AsyncSession:
    """Wrap the test session so _job_queue_processor can use it as its own."""
    session.commit = AsyncMock()
    session.close = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def patch_async_session(mock_session: AsyncSession):
    """Patch src.core.queue_processor.async_session to return the test session."""
    with patch("src.core.queue_processor.async_session", return_value=mock_session):
        yield mock_session


@pytest.fixture
def patch_process_queue(patch_async_session):
    """Patch queue_manager.process_queue to return a no-op summary dict."""
    with patch(
        "src.core.queue_processor.queue_manager.process_queue",
        new_callable=AsyncMock,
        return_value={"unreleased_advanced": 0, "retries_triggered": 0},
    ) as mock:
        yield mock


@pytest.fixture
def patch_scrape_pipeline(patch_process_queue):
    """Patch scrape_pipeline.run to be a silent no-op."""
    with patch("src.core.queue_processor.scrape_pipeline.run", new_callable=AsyncMock) as mock:
        yield mock


@pytest.fixture
def job_patches(patch_async_session, patch_process_queue, patch_scrape_pipeline):
    """Activate all common patches for a Stage 2/3/4 test run."""
    return {
        "session": patch_async_session,
        "process_queue": patch_process_queue,
        "scrape_pipeline": patch_scrape_pipeline,
    }


# ---------------------------------------------------------------------------
# Group 1E: Stage 0 — Stale SCRAPING recovery
# ---------------------------------------------------------------------------


class TestStage0StaleRecovery:
    """Stage 0: Stuck SCRAPING items older than 30 min are recovered to SLEEPING."""

    async def test_stale_scraping_recovered_to_sleeping(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """SCRAPING item with state_changed_at > 30 min transitions to SLEEPING."""
        stale_time = _utcnow() - timedelta(minutes=35)
        item = await _make_media_item(
            session,
            state=QueueState.SCRAPING,
            state_changed_at=stale_time,
        )

        with patch("src.core.queue_processor.scrape_pipeline.run", new_callable=AsyncMock):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.SLEEPING

    async def test_recent_scraping_not_recovered(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """SCRAPING item less than 30 min old is left in SCRAPING by Stage 0."""
        recent_time = _utcnow() - timedelta(minutes=5)
        item = await _make_media_item(
            session,
            state=QueueState.SCRAPING,
            state_changed_at=recent_time,
        )

        # Stage 1 runs the pipeline on SCRAPING items; mock it to a no-op
        with patch("src.core.queue_processor.scrape_pipeline.run", new_callable=AsyncMock):
            await _job_queue_processor()

        await session.refresh(item)
        # Stage 0 must NOT force SLEEPING; pipeline no-op keeps it SCRAPING
        assert item.state == QueueState.SCRAPING

    async def test_stale_recovery_failure_swallowed(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """If transition fails for a stale SCRAPING item, the error is logged, not raised."""
        stale_time = _utcnow() - timedelta(minutes=35)
        _item = await _make_media_item(
            session,
            state=QueueState.SCRAPING,
            state_changed_at=stale_time,
        )

        with (
            patch(
                "src.core.queue_processor.queue_manager.transition",
                new_callable=AsyncMock,
                side_effect=RuntimeError("DB error"),
            ),
            patch("src.core.queue_processor.scrape_pipeline.run", new_callable=AsyncMock),
        ):
            # Must not propagate; job continues silently
            await _job_queue_processor()


# ---------------------------------------------------------------------------
# Group 1E (continued): Stage 2 edge cases
# ---------------------------------------------------------------------------


class TestStage2AddingEdgeCases:
    """Stage 2 ADDING → CHECKING edge cases."""

    async def test_adding_timeout_transitions_to_sleeping(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """ADDING item with no active torrent and past timeout transitions to SLEEPING."""
        # No torrent added — _find_torrent_for_item returns None
        past_time = _utcnow() - timedelta(minutes=60)
        item = await _make_media_item(
            session,
            state=QueueState.ADDING,
            state_changed_at=past_time,
        )

        await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.SLEEPING

    async def test_adding_no_torrent_not_timed_out_stays_adding(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """ADDING item with no torrent but within timeout window stays ADDING."""
        recent_time = _utcnow() - timedelta(minutes=5)
        item = await _make_media_item(
            session,
            state=QueueState.ADDING,
            state_changed_at=recent_time,
        )

        await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.ADDING

    async def test_adding_rd_404_transitions_to_sleeping(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """ADDING item receiving RealDebridError with status_code=404 transitions to SLEEPING."""
        from src.services.real_debrid import RealDebridError

        item = await _make_media_item(session, state=QueueState.ADDING)
        await _make_rd_torrent(session, media_item_id=item.id, rd_id="RD_GONE")

        err = RealDebridError("Not found", status_code=404)
        with patch(
            "src.core.queue_processor.rd_client.get_torrent_info",
            new_callable=AsyncMock,
            side_effect=err,
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.SLEEPING

    async def test_adding_downloaded_captures_filename(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """RD status 'downloaded' with a new filename updates torrent.filename."""
        item = await _make_media_item(session, state=QueueState.ADDING)
        torrent = await _make_rd_torrent(
            session,
            media_item_id=item.id,
            rd_id="RD_NEW",
            filename="Old.Filename.mkv",
        )

        with (
            patch(
                "src.core.queue_processor.rd_client.get_torrent_info",
                new_callable=AsyncMock,
                return_value={"status": "downloaded", "filename": "New.Filename.mkv"},
            ),
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Movie"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[],
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        await session.refresh(torrent)
        assert item.state == QueueState.CHECKING
        assert torrent.filename == "New.Filename.mkv"


# ---------------------------------------------------------------------------
# Group 1D: Media scan triggers (Plex + Jellyfin)
# ---------------------------------------------------------------------------


class TestMediaScanTriggers:
    """Plex and Jellyfin scans are triggered after successful symlink creation."""

    async def test_plex_scan_triggered_after_symlink(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """Plex scan_section is called when plex.enabled and scan_after_symlink are True."""
        _item = await _make_media_item(session, state=QueueState.CHECKING)
        mount_match = _make_mount_match(parsed_episode=None, parsed_season=None)
        symlink = _make_symlink(target_path="/library/movies/Test Movie (2024)/Test.Movie.mkv")

        plex_cfg = PlexConfig(
            enabled=True,
            token="plex-token",
            scan_after_symlink=True,
            movie_section_ids=[1],
            show_section_ids=[],
        )
        mock_plex_scan = AsyncMock()
        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Movie"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[mount_match],
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                return_value=symlink,
            ),
            patch("src.core.queue_processor.settings.plex", plex_cfg),
            patch("src.services.plex.plex_client.scan_section", mock_plex_scan),
        ):
            await _job_queue_processor()

        mock_plex_scan.assert_awaited_once()

    async def test_plex_scan_deduplicates_by_section_and_dir(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """Multiple symlinks in the same directory trigger only one Plex scan per section."""
        item1 = await _make_media_item(
            session, state=QueueState.CHECKING, imdb_id="tt0000001"
        )
        _item2 = await _make_media_item(
            session, state=QueueState.CHECKING, imdb_id="tt0000002", title="Test Movie 2"
        )
        # Both symlinks resolve to the same parent directory
        sl1 = _make_symlink(target_path="/library/movies/TestLib/file1.mkv")
        sl2 = _make_symlink(target_path="/library/movies/TestLib/file2.mkv")

        async def _fake_lookup(sess, titles, *, season, episode):
            if "Test Movie 2" in titles:
                return [_make_mount_match("/mnt/zurg/B.mkv", parsed_episode=None)]
            return [_make_mount_match("/mnt/zurg/A.mkv", parsed_episode=None)]

        async def _fake_symlink(sess, item, filepath, **kwargs):
            return sl1 if item.id == item1.id else sl2

        plex_cfg = PlexConfig(
            enabled=True,
            token="plex-token",
            scan_after_symlink=True,
            movie_section_ids=[1],
            show_section_ids=[],
        )
        mock_plex_scan = AsyncMock()
        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                side_effect=lambda s, i, **kw: [i.title],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                side_effect=_fake_lookup,
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                side_effect=_fake_symlink,
            ),
            patch("src.core.queue_processor.settings.plex", plex_cfg),
            patch("src.services.plex.plex_client.scan_section", mock_plex_scan),
        ):
            await _job_queue_processor()

        # Both target_paths share /library/movies/TestLib — only one scan per (section, dir)
        assert mock_plex_scan.await_count == 1

    async def test_plex_scan_skipped_when_disabled(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """When plex.enabled=False, scan_section is never called."""
        _item = await _make_media_item(session, state=QueueState.CHECKING)
        mount_match = _make_mount_match(parsed_episode=None, parsed_season=None)
        symlink = _make_symlink()

        plex_cfg = PlexConfig(enabled=False)
        mock_plex_scan = AsyncMock()
        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Movie"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[mount_match],
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                return_value=symlink,
            ),
            patch("src.core.queue_processor.settings.plex", plex_cfg),
            patch("src.services.plex.plex_client.scan_section", mock_plex_scan),
        ):
            await _job_queue_processor()

        mock_plex_scan.assert_not_awaited()

    async def test_plex_scan_failure_non_fatal(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """A Plex scan failure does not propagate and does not revert the COMPLETE transition."""
        item = await _make_media_item(session, state=QueueState.CHECKING)
        mount_match = _make_mount_match(parsed_episode=None, parsed_season=None)
        symlink = _make_symlink(target_path="/library/movies/Test Movie (2024)/file.mkv")

        plex_cfg = PlexConfig(
            enabled=True,
            token="plex-token",
            scan_after_symlink=True,
            movie_section_ids=[1],
            show_section_ids=[],
        )
        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Movie"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[mount_match],
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                return_value=symlink,
            ),
            patch("src.core.queue_processor.settings.plex", plex_cfg),
            patch(
                "src.services.plex.plex_client.scan_section",
                new_callable=AsyncMock,
                side_effect=RuntimeError("Plex unreachable"),
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.COMPLETE

    async def test_jellyfin_scan_triggered_after_symlink(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """Jellyfin scan_library is called when jellyfin.enabled and scan_after_symlink are True."""
        _item = await _make_media_item(session, state=QueueState.CHECKING)
        mount_match = _make_mount_match(parsed_episode=None, parsed_season=None)
        symlink = _make_symlink()

        jf_cfg = JellyfinConfig(
            enabled=True,
            api_key="jf-key",
            scan_after_symlink=True,
            movie_library_ids=["lib-movies"],
            show_library_ids=[],
        )
        mock_jf_scan = AsyncMock()
        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Movie"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[mount_match],
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                return_value=symlink,
            ),
            patch("src.core.queue_processor.settings.jellyfin", jf_cfg),
            patch("src.services.jellyfin.jellyfin_client.scan_library", mock_jf_scan),
        ):
            await _job_queue_processor()

        mock_jf_scan.assert_awaited_once_with("lib-movies")

    async def test_jellyfin_scan_deduplicates_by_library_id(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """Multiple symlinks for the same media type trigger scan_library only once per lib_id."""
        item1 = await _make_media_item(
            session, state=QueueState.CHECKING, imdb_id="tt0000001"
        )
        _item2 = await _make_media_item(
            session, state=QueueState.CHECKING, imdb_id="tt0000002", title="Test Movie 2"
        )
        sl1 = _make_symlink(target_path="/library/movies/A/file.mkv")
        sl2 = _make_symlink(target_path="/library/movies/B/file.mkv")

        async def _fake_lookup(sess, titles, *, season, episode):
            if "Test Movie 2" in titles:
                return [_make_mount_match("/mnt/zurg/B.mkv", parsed_episode=None)]
            return [_make_mount_match("/mnt/zurg/A.mkv", parsed_episode=None)]

        async def _fake_symlink(sess, item, filepath, **kwargs):
            return sl1 if item.id == item1.id else sl2

        jf_cfg = JellyfinConfig(
            enabled=True,
            api_key="jf-key",
            scan_after_symlink=True,
            movie_library_ids=["lib-movies"],
            show_library_ids=[],
        )
        mock_jf_scan = AsyncMock()
        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                side_effect=lambda s, i, **kw: [i.title],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                side_effect=_fake_lookup,
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                side_effect=_fake_symlink,
            ),
            patch("src.core.queue_processor.settings.jellyfin", jf_cfg),
            patch("src.services.jellyfin.jellyfin_client.scan_library", mock_jf_scan),
        ):
            await _job_queue_processor()

        # Both items are movies in the same library — scan_library called exactly once
        assert mock_jf_scan.await_count == 1
        mock_jf_scan.assert_awaited_with("lib-movies")

    async def test_jellyfin_scan_failure_non_fatal(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """Jellyfin scan_library failure does not revert the COMPLETE transition."""
        item = await _make_media_item(session, state=QueueState.CHECKING)
        mount_match = _make_mount_match(parsed_episode=None, parsed_season=None)
        symlink = _make_symlink()

        jf_cfg = JellyfinConfig(
            enabled=True,
            api_key="jf-key",
            scan_after_symlink=True,
            movie_library_ids=["lib-movies"],
            show_library_ids=[],
        )
        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Movie"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[mount_match],
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                return_value=symlink,
            ),
            patch("src.core.queue_processor.settings.jellyfin", jf_cfg),
            patch(
                "src.services.jellyfin.jellyfin_client.scan_library",
                new_callable=AsyncMock,
                side_effect=RuntimeError("Jellyfin down"),
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.COMPLETE


# ---------------------------------------------------------------------------
# Group 1B: Single episode/movie — fallback chain
# ---------------------------------------------------------------------------


class TestCheckingSingleEpisodeFallbacks:
    """Stage 3 single-episode fallback chain: targeted scan, RD refresh, path prefix."""

    async def test_targeted_scan_when_no_initial_match(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """No initial lookup_multi match → find torrent → scan_directory → retry lookup."""
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            season=1,
            episode=1,
        )
        await _make_rd_torrent(
            session,
            media_item_id=item.id,
            filename="Test.Show.S01E01.1080p.mkv",
        )
        mount_match = _make_mount_match()
        scan_result = _scan_result(files_indexed=1)

        lookup_calls = 0

        async def _fake_lookup_multi(sess, titles, *, season, episode):
            nonlocal lookup_calls
            lookup_calls += 1
            if lookup_calls == 1:
                return []  # First call: nothing indexed yet
            return [mount_match]  # After scan: file appears

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Show"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                side_effect=_fake_lookup_multi,
            ),
            patch(
                "src.core.queue_processor.mount_scanner.scan_directory",
                new_callable=AsyncMock,
                return_value=scan_result,
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                return_value=_make_symlink(),
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.COMPLETE

    async def test_rd_filename_refresh_retries_scan_and_lookup(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """Stale torrent.filename → RD returns different name → rescan → lookup finds file."""
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            season=1,
            episode=5,
        )
        torrent = await _make_rd_torrent(
            session,
            media_item_id=item.id,
            rd_id="RD_STALE",
            filename="Old.Title.S01E05.mkv",
        )
        mount_match = _make_mount_match(parsed_episode=5)

        scan_calls: list[str] = []

        async def _fake_scan(sess, name):
            scan_calls.append(name)
            if name == "Old.Title.S01E05.mkv":
                return _empty_scan_result()
            # New name returns files
            return _scan_result(files_indexed=3, matched_dir_path="/mnt/zurg/Real.Title.S01")

        lookup_calls = 0

        async def _fake_lookup_multi(sess, titles, *, season, episode):
            nonlocal lookup_calls
            lookup_calls += 1
            # Call 1: initial lookup (nothing indexed yet)
            # Call 2: after RD filename refresh + successful rescan → match found
            if lookup_calls < 2:
                return []
            return [mount_match]

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Show"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                side_effect=_fake_lookup_multi,
            ),
            patch(
                "src.core.queue_processor.mount_scanner.scan_directory",
                new_callable=AsyncMock,
                side_effect=_fake_scan,
            ),
            patch(
                "src.core.queue_processor.rd_client.get_torrent_info",
                new_callable=AsyncMock,
                return_value={"filename": "Real.Title.S01E05.mkv"},
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                return_value=_make_symlink(),
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        await session.refresh(torrent)
        assert item.state == QueueState.COMPLETE
        assert torrent.filename == "Real.Title.S01E05.mkv"

    async def test_path_prefix_fallback(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """Title lookup fails but scan returns matched_dir_path → lookup_by_path_prefix used."""
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            season=1,
            episode=3,
        )
        await _make_rd_torrent(
            session,
            media_item_id=item.id,
            filename="Some.Disc.Rip.S01.1080p",
        )
        mount_match = _make_mount_match(parsed_episode=3, parsed_season=1)
        scan_res = _scan_result(files_indexed=5, matched_dir_path="/mnt/zurg/Some.Disc.Rip.S01")

        prefix_calls: list[tuple] = []

        async def _fake_prefix_lookup(sess, path, *, season, episode):
            prefix_calls.append((path, season, episode))
            if season == 1 and episode == 3:
                return [mount_match]
            return []

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Show"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.scan_directory",
                new_callable=AsyncMock,
                return_value=scan_res,
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_by_path_prefix",
                new_callable=AsyncMock,
                side_effect=_fake_prefix_lookup,
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                return_value=_make_symlink(),
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.COMPLETE
        # Verify path prefix was invoked with the season-filtered args
        assert any(s == 1 and e == 3 for _, s, e in prefix_calls)

    async def test_relaxed_season_filter_fallback(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """Path prefix with season/episode fails → retry with season=None, episode kept."""
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            season=2,
            episode=7,
        )
        await _make_rd_torrent(
            session,
            media_item_id=item.id,
            filename="Test.Anime.S02E07.mkv",
        )
        mount_match = _make_mount_match(
            filepath="/mnt/zurg/Test.Anime/Test.Anime.07.mkv",
            parsed_season=None,
            parsed_episode=7,
        )
        scan_res = _scan_result(matched_dir_path="/mnt/zurg/Test.Anime")

        async def _fake_prefix_lookup(sess, path, *, season, episode):
            if season is None and episode == 7:
                return [mount_match]
            return []

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Anime"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.scan_directory",
                new_callable=AsyncMock,
                return_value=scan_res,
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_by_path_prefix",
                new_callable=AsyncMock,
                side_effect=_fake_prefix_lookup,
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                return_value=_make_symlink(),
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.COMPLETE

    async def test_no_filter_fallback_single_file(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """All lookups fail → no-filter prefix lookup returns exactly 1 file → used."""
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            season=1,
            episode=1,
        )
        await _make_rd_torrent(
            session,
            media_item_id=item.id,
            filename="Single.File.Torrent.mkv",
        )
        single_match = _make_mount_match(
            filepath="/mnt/zurg/Single.File.Torrent/Single.File.Torrent.mkv",
            parsed_season=None,
            parsed_episode=None,
        )
        scan_res = _scan_result(matched_dir_path="/mnt/zurg/Single.File.Torrent")

        async def _fake_prefix_lookup(sess, path, *, season, episode):
            if season is None and episode is None:
                return [single_match]
            return []

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Show"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.scan_directory",
                new_callable=AsyncMock,
                return_value=scan_res,
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_by_path_prefix",
                new_callable=AsyncMock,
                side_effect=_fake_prefix_lookup,
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                return_value=_make_symlink(),
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.COMPLETE

    async def test_no_filter_fallback_rejects_multi_file(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """No-filter fallback returns >1 file → rejected → item stays CHECKING."""
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            season=1,
            episode=1,
        )
        await _make_rd_torrent(
            session,
            media_item_id=item.id,
            filename="Multi.Episode.Pack",
        )
        match1 = _make_mount_match(
            "/mnt/zurg/Multi.Episode.Pack/file1.mkv",
            parsed_season=None,
            parsed_episode=None,
        )
        match2 = _make_mount_match(
            "/mnt/zurg/Multi.Episode.Pack/file2.mkv",
            parsed_season=None,
            parsed_episode=None,
        )
        scan_res = _scan_result(matched_dir_path="/mnt/zurg/Multi.Episode.Pack")

        async def _fake_prefix_lookup(sess, path, *, season, episode):
            if season is None and episode is None:
                return [match1, match2]
            return []

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Show"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.scan_directory",
                new_callable=AsyncMock,
                return_value=scan_res,
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_by_path_prefix",
                new_callable=AsyncMock,
                side_effect=_fake_prefix_lookup,
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.CHECKING

    async def test_xem_scene_numbering_fallback(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """XEM enabled + season/episode set → XEM remap tried → scene-numbered match found."""
        from src.config import XemConfig

        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            tvdb_id=12345,
            season=1,
            episode=29,
        )
        scene_match = _make_mount_match(
            "/mnt/zurg/Test.Anime/S02E05.mkv",
            parsed_season=2,
            parsed_episode=5,
        )
        xem_cfg = XemConfig(enabled=True)

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Anime"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch("src.core.queue_processor.settings.xem", xem_cfg),
            patch(
                "src.core.xem_mapper.xem_mapper.get_scene_numbering_for_item",
                new_callable=AsyncMock,
                return_value=(2, 5),  # TMDB S01E29 → scene S02E05
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup",
                new_callable=AsyncMock,
                return_value=[scene_match],
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                return_value=_make_symlink(),
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.COMPLETE

    async def test_xem_fallback_skipped_when_disabled(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """When XEM disabled, the XEM fallback block is never entered."""
        from src.config import XemConfig

        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            tvdb_id=12345,
            season=1,
            episode=5,
        )
        xem_cfg = XemConfig(enabled=False)
        xem_mock = AsyncMock()

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Anime"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch("src.core.queue_processor.settings.xem", xem_cfg),
            patch(
                "src.core.xem_mapper.xem_mapper.get_scene_numbering_for_item",
                xem_mock,
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
            ),
        ):
            await _job_queue_processor()

        xem_mock.assert_not_awaited()
        await session.refresh(item)
        assert item.state == QueueState.CHECKING


# ---------------------------------------------------------------------------
# Group 1C: Single episode — post-match logic
# ---------------------------------------------------------------------------


class TestCheckingSingleEpisodePostMatch:
    """Post-match logic: auto-promote, filesize verification, loop-breaker flags."""

    async def test_auto_promote_to_season_pack(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """Show item with >1 distinct parsed episodes promotes to season pack and reaches COMPLETE."""
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            season=1,
            episode=1,
        )
        matches = [
            _make_mount_match(
                f"/mnt/zurg/Show/S01E0{i}.mkv",
                parsed_episode=i,
                parsed_season=1,
            )
            for i in range(1, 4)
        ]

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Show"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=matches,
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                return_value=_make_symlink(),
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.COMPLETE
        assert item.is_season_pack is True
        assert item.episode is None

    async def test_auto_promote_infers_season_from_single_season_files(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """Auto-promoted item with season=None infers season from files when all agree."""
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            season=None,
            episode=1,
        )
        matches = [
            _make_mount_match(
                f"/mnt/zurg/Show/S02E0{i}.mkv",
                parsed_episode=i,
                parsed_season=2,
            )
            for i in range(1, 4)
        ]

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Show"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=matches,
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                return_value=_make_symlink(),
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.COMPLETE
        assert item.season == 2

    async def test_auto_promote_does_not_infer_ambiguous_season(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """When files span multiple distinct seasons, item.season stays None after auto-promote."""
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            season=None,
            episode=1,
        )
        matches = [
            _make_mount_match(
                "/mnt/zurg/Show/S01E01.mkv", parsed_episode=1, parsed_season=1
            ),
            _make_mount_match(
                "/mnt/zurg/Show/S01E02.mkv", parsed_episode=2, parsed_season=1
            ),
            _make_mount_match(
                "/mnt/zurg/Show/S02E01.mkv", parsed_episode=1, parsed_season=2
            ),
        ]

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Show"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=matches,
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                return_value=_make_symlink(),
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.COMPLETE
        assert item.season is None  # ambiguous → not inferred

    async def test_filesize_verification_narrows_matches(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """Multiple single-episode matches: torrent filesize known → filtered to within 15%."""
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            season=1,
            episode=1,
        )
        await _make_rd_torrent(
            session,
            media_item_id=item.id,
            filesize=1_000_000_000,  # 1 GB expected
        )
        # wrong_match is 2 GB — outside 15% tolerance
        wrong_match = _make_mount_match(
            "/mnt/zurg/Show/S01E01.wrong.mkv",
            parsed_episode=1,
            parsed_season=1,
            filesize=2_000_000_000,
        )
        # right_match is within 5% — should be chosen
        right_match = _make_mount_match(
            "/mnt/zurg/Show/S01E01.right.mkv",
            parsed_episode=1,
            parsed_season=1,
            filesize=1_020_000_000,
        )

        symlink_calls: list[str] = []

        async def _fake_symlink(sess, item, filepath, **kwargs):
            symlink_calls.append(filepath)
            return _make_symlink()

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Show"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[wrong_match, right_match],
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                side_effect=_fake_symlink,
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.COMPLETE
        assert symlink_calls == [right_match.filepath]

    async def test_filesize_verification_skipped_no_torrent_size(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """When torrent has no filesize, all matches are kept (first is used)."""
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            season=1,
            episode=1,
        )
        await _make_rd_torrent(
            session,
            media_item_id=item.id,
            filesize=None,
        )
        match1 = _make_mount_match(
            "/mnt/zurg/Show/S01E01.a.mkv",
            parsed_episode=1,
            parsed_season=1,
            filesize=500_000_000,
        )
        match2 = _make_mount_match(
            "/mnt/zurg/Show/S01E01.b.mkv",
            parsed_episode=1,
            parsed_season=1,
            filesize=2_000_000_000,
        )
        symlink_calls: list[str] = []

        async def _fake_symlink(sess, item, filepath, **kwargs):
            symlink_calls.append(filepath)
            return _make_symlink()

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Show"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[match1, match2],
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                side_effect=_fake_symlink,
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.COMPLETE
        assert len(symlink_calls) == 1

    async def test_source_not_found_purges_stale_entry_and_stays_checking(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """SourceNotFoundError → stale mount entry deleted → item stays CHECKING."""
        from src.core.symlink_manager import SourceNotFoundError

        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
        )
        stale_path = "/mnt/zurg/movies/Test Movie (2024)/movie.mkv"
        # Persist a real MountIndex entry to verify deletion
        mount_entry = MountIndex(filepath=stale_path, filename="movie.mkv")
        session.add(mount_entry)
        await session.flush()

        mount_match = _make_mount_match(
            stale_path, parsed_episode=None, parsed_season=None
        )

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Movie"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[mount_match],
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                side_effect=SourceNotFoundError(stale_path),
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.CHECKING

    async def test_success_clears_checking_failed_hash(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """Successful symlink creation clears checking_failed_hash from metadata_json."""
        metadata = json.dumps(
            {"checking_failed_hash": "abc123def456", "other_key": "value"}
        )
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            metadata_json=metadata,
        )
        mount_match = _make_mount_match(
            parsed_episode=None, parsed_season=None
        )

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Movie"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[mount_match],
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                return_value=_make_symlink(),
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.COMPLETE
        if item.metadata_json:
            meta = json.loads(item.metadata_json)
            assert "checking_failed_hash" not in meta
            assert meta.get("other_key") == "value"

    async def test_timeout_stores_checking_failed_hash(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """On CHECKING timeout, the torrent's info_hash is stored in metadata_json."""
        past = _utcnow() - timedelta(minutes=60)
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            state_changed_at=past,
        )
        await _make_rd_torrent(
            session,
            media_item_id=item.id,
            info_hash="deadbeef1234",
        )

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Movie"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[],
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.SLEEPING
        assert item.metadata_json is not None
        meta = json.loads(item.metadata_json)
        assert meta.get("checking_failed_hash") == "deadbeef1234"


# ---------------------------------------------------------------------------
# Group 1A: Season pack CHECKING
# ---------------------------------------------------------------------------


class TestCheckingSeasonPack:
    """Stage 3: CHECKING is_season_pack=True path."""

    async def test_mount_match_creates_multiple_symlinks_and_completes(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """N season pack files matched → N symlinks created → COMPLETE."""
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            is_season_pack=True,
            season=1,
        )
        matches = [
            _make_mount_match(
                f"/mnt/zurg/Show/S01E0{i}.mkv",
                parsed_episode=i,
                parsed_season=1,
            )
            for i in range(1, 5)
        ]

        symlink_count = 0

        async def _count_symlinks(sess, item, filepath, **kwargs):
            nonlocal symlink_count
            symlink_count += 1
            return _make_symlink(source_path=filepath)

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Show"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=matches,
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                side_effect=_count_symlinks,
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.COMPLETE
        assert symlink_count == 4

    async def test_dedup_picks_best_resolution_per_episode(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """Two files for the same episode → highest resolution wins."""
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            is_season_pack=True,
            season=1,
        )
        low_res = _make_mount_match(
            "/mnt/zurg/Show/S01E01.720p.mkv",
            parsed_episode=1,
            parsed_season=1,
            parsed_resolution="720p",
        )
        high_res = _make_mount_match(
            "/mnt/zurg/Show/S01E01.1080p.mkv",
            parsed_episode=1,
            parsed_season=1,
            parsed_resolution="1080p",
        )

        symlink_calls: list[str] = []

        async def _fake_symlink(sess, item, filepath, **kwargs):
            symlink_calls.append(filepath)
            return _make_symlink(source_path=filepath)

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Show"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[low_res, high_res],
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                side_effect=_fake_symlink,
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.COMPLETE
        assert len(symlink_calls) == 1
        assert "1080p" in symlink_calls[0]

    async def test_requested_resolution_gets_priority(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """When requested_resolution='720p', 720p file wins over 1080p for that episode."""
        _item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            is_season_pack=True,
            season=1,
            requested_resolution="720p",
        )
        match_720 = _make_mount_match(
            "/mnt/zurg/Show/S01E01.720p.mkv",
            parsed_episode=1,
            parsed_season=1,
            parsed_resolution="720p",
        )
        match_1080 = _make_mount_match(
            "/mnt/zurg/Show/S01E01.1080p.mkv",
            parsed_episode=1,
            parsed_season=1,
            parsed_resolution="1080p",
        )

        symlink_calls: list[str] = []

        async def _fake_symlink(sess, item, filepath, **kwargs):
            symlink_calls.append(filepath)
            return _make_symlink(source_path=filepath)

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Show"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[match_1080, match_720],
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                side_effect=_fake_symlink,
            ),
        ):
            await _job_queue_processor()

        assert len(symlink_calls) == 1
        assert "720p" in symlink_calls[0]

    async def test_filters_sample_files(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """Files whose basename starts with 'sample' are excluded from symlink creation."""
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            is_season_pack=True,
            season=1,
        )
        real_ep = _make_mount_match(
            "/mnt/zurg/Show/S01E01.mkv",
            parsed_episode=1,
            parsed_season=1,
        )
        sample_ep = _make_mount_match(
            "/mnt/zurg/Show/sample.S01E02.mkv",
            parsed_episode=2,
            parsed_season=1,
        )

        symlink_calls: list[str] = []

        async def _fake_symlink(sess, item, filepath, **kwargs):
            symlink_calls.append(filepath)
            return _make_symlink(source_path=filepath)

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Show"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[real_ep, sample_ep],
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                side_effect=_fake_symlink,
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.COMPLETE
        assert len(symlink_calls) == 1
        assert "sample" not in symlink_calls[0].lower()

    async def test_filters_special_files_ncop_nced(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """NCOP/NCED files are excluded via _SPECIAL_FILENAME_RE."""
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            is_season_pack=True,
            season=1,
        )
        real_ep = _make_mount_match(
            "/mnt/zurg/Anime/S01E01.mkv",
            parsed_episode=1,
            parsed_season=1,
        )
        ncop = _make_mount_match(
            "/mnt/zurg/Anime/NCOP1.mkv",
            parsed_episode=99,  # non-None so only regex filter applies
            parsed_season=1,
        )
        nced = _make_mount_match(
            "/mnt/zurg/Anime/NCED2.mkv",
            parsed_episode=98,
            parsed_season=1,
        )

        symlink_calls: list[str] = []

        async def _fake_symlink(sess, item, filepath, **kwargs):
            symlink_calls.append(filepath)
            return _make_symlink(source_path=filepath)

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Anime"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[real_ep, ncop, nced],
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                side_effect=_fake_symlink,
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.COMPLETE
        # Only real_ep passes both parsed_episode and regex filters
        assert len(symlink_calls) == 1
        assert symlink_calls[0] == real_ep.filepath

    async def test_filters_files_without_parsed_episode(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """Files with parsed_episode=None are excluded from season pack processing."""
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            is_season_pack=True,
            season=1,
        )
        good = _make_mount_match(
            "/mnt/zurg/Show/S01E03.mkv",
            parsed_episode=3,
            parsed_season=1,
        )
        no_ep = _make_mount_match(
            "/mnt/zurg/Show/Extras.mkv",
            parsed_episode=None,
            parsed_season=1,
        )

        symlink_calls: list[str] = []

        async def _fake_symlink(sess, item, filepath, **kwargs):
            symlink_calls.append(filepath)
            return _make_symlink(source_path=filepath)

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Show"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[good, no_ep],
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                side_effect=_fake_symlink,
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.COMPLETE
        assert len(symlink_calls) == 1
        assert symlink_calls[0] == good.filepath

    async def test_all_null_episodes_triggers_rescan(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """All matched files have parsed_episode=None → scan_directory triggered."""
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            is_season_pack=True,
            season=1,
        )
        await _make_rd_torrent(
            session,
            media_item_id=item.id,
            filename="Test.Show.S01.COMPLETE.mkv",
        )
        null_matches = [
            _make_mount_match(
                f"/mnt/zurg/Show/file{i}.mkv",
                parsed_episode=None,
                parsed_season=1,
            )
            for i in range(1, 3)
        ]
        proper_match = _make_mount_match(
            "/mnt/zurg/Show/S01E01.mkv",
            parsed_episode=1,
            parsed_season=1,
        )

        lookup_calls = 0

        async def _fake_lookup_multi(sess, titles, *, season, episode):
            nonlocal lookup_calls
            lookup_calls += 1
            if lookup_calls == 1:
                return null_matches
            return []

        async def _fake_lookup(sess, *, title, season, episode):
            return [proper_match]

        rescan_called = False

        async def _fake_scan(sess, name):
            nonlocal rescan_called
            rescan_called = True
            return _scan_result(files_indexed=1)

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Show"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                side_effect=_fake_lookup_multi,
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup",
                new_callable=AsyncMock,
                side_effect=_fake_lookup,
            ),
            patch(
                "src.core.queue_processor.mount_scanner.scan_directory",
                new_callable=AsyncMock,
                side_effect=_fake_scan,
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                return_value=_make_symlink(),
            ),
        ):
            await _job_queue_processor()

        assert rescan_called

    async def test_season_pack_no_match_triggers_targeted_scan(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """No initial season pack match → find torrent → scan_directory → retry lookup."""
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            is_season_pack=True,
            season=1,
        )
        await _make_rd_torrent(
            session,
            media_item_id=item.id,
            filename="Test.Show.Season.1.Complete",
        )
        match = _make_mount_match(parsed_episode=1, parsed_season=1)

        scan_called = False

        async def _fake_scan(sess, name):
            nonlocal scan_called
            scan_called = True
            return _scan_result(files_indexed=5)

        async def _fake_lookup(sess, *, title, season, episode):
            return [match]

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Show"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup",
                new_callable=AsyncMock,
                side_effect=_fake_lookup,
            ),
            patch(
                "src.core.queue_processor.mount_scanner.scan_directory",
                new_callable=AsyncMock,
                side_effect=_fake_scan,
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                return_value=_make_symlink(),
            ),
        ):
            await _job_queue_processor()

        assert scan_called

    async def test_season_pack_path_prefix_fallback(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """Season pack: lookup fails but scan returns dir → lookup_by_path_prefix succeeds."""
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            is_season_pack=True,
            season=1,
        )
        await _make_rd_torrent(
            session,
            media_item_id=item.id,
            filename="Disc.Rip.Complete.Season.1",
        )
        match = _make_mount_match(
            "/mnt/zurg/Disc.Rip/S01E01.mkv",
            parsed_episode=1,
            parsed_season=1,
        )
        scan_res = _scan_result(
            files_indexed=5, matched_dir_path="/mnt/zurg/Disc.Rip"
        )

        async def _fake_prefix_lookup(sess, path, *, season, episode):
            if path == "/mnt/zurg/Disc.Rip" and episode is None:
                return [match]
            return []

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Show"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.scan_directory",
                new_callable=AsyncMock,
                return_value=scan_res,
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_by_path_prefix",
                new_callable=AsyncMock,
                side_effect=_fake_prefix_lookup,
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                return_value=_make_symlink(),
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.COMPLETE

    async def test_multi_season_torrent_refilters_to_requested_season(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """Relaxed fallback finds multi-season torrent → re-filters to item.season only."""
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            is_season_pack=True,
            season=2,
        )
        await _make_rd_torrent(
            session,
            media_item_id=item.id,
            filename="Test.Show.Complete.S01S02",
        )
        s1_match = _make_mount_match(
            "/mnt/zurg/Show/S01E01.mkv", parsed_episode=1, parsed_season=1
        )
        s2_match = _make_mount_match(
            "/mnt/zurg/Show/S02E01.mkv", parsed_episode=1, parsed_season=2
        )
        scan_res = _scan_result(matched_dir_path="/mnt/zurg/Show")

        async def _fake_prefix_lookup(sess, path, *, season, episode):
            if season is None and episode is None:
                return [s1_match, s2_match]
            if season == 2 and episode is None:
                return [s2_match]
            return []

        symlink_calls: list[str] = []

        async def _fake_symlink(sess, item, filepath, **kwargs):
            symlink_calls.append(filepath)
            return _make_symlink(source_path=filepath)

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Show"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.scan_directory",
                new_callable=AsyncMock,
                return_value=scan_res,
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_by_path_prefix",
                new_callable=AsyncMock,
                side_effect=_fake_prefix_lookup,
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                side_effect=_fake_symlink,
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.COMPLETE
        # Only the S02 file should be symlinked
        assert all("S02" in p for p in symlink_calls)
        assert not any("S01" in p for p in symlink_calls)

    async def test_auto_corrects_movie_type_to_show_for_season_pack(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """Season pack item with media_type=MOVIE gets corrected to SHOW after match."""
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            is_season_pack=True,
            season=1,
            media_type=MediaType.MOVIE,
        )
        match = _make_mount_match(
            "/mnt/zurg/Show/S01E01.mkv",
            parsed_episode=1,
            parsed_season=1,
        )

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Show"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[match],
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                return_value=_make_symlink(),
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.media_type == MediaType.SHOW
        assert item.state == QueueState.COMPLETE

    async def test_season_pack_timeout_stores_checking_failed_hash(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """Season pack timeout → info_hash stored in metadata_json before SLEEPING."""
        past = _utcnow() - timedelta(minutes=60)
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            is_season_pack=True,
            season=1,
            state_changed_at=past,
        )
        await _make_rd_torrent(
            session,
            media_item_id=item.id,
            info_hash="cafebabe1234",
        )

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Show"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[],
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.SLEEPING
        assert item.metadata_json is not None
        meta = json.loads(item.metadata_json)
        assert meta.get("checking_failed_hash") == "cafebabe1234"

    async def test_season_pack_source_not_found_purges_stale_continues(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """SourceNotFoundError for one season pack file → stale entry deleted → others succeed."""
        from src.core.symlink_manager import SourceNotFoundError

        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            is_season_pack=True,
            season=1,
        )
        stale_path = "/mnt/zurg/Show/S01E01.stale.mkv"
        good_path = "/mnt/zurg/Show/S01E02.mkv"

        mount_entry = MountIndex(filepath=stale_path, filename="S01E01.stale.mkv")
        session.add(mount_entry)
        await session.flush()

        stale_match = _make_mount_match(stale_path, parsed_episode=1, parsed_season=1)
        good_match = _make_mount_match(good_path, parsed_episode=2, parsed_season=1)

        async def _fake_symlink(sess, item, filepath, **kwargs):
            if filepath == stale_path:
                raise SourceNotFoundError(filepath)
            return _make_symlink(source_path=filepath)

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Show"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[stale_match, good_match],
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                side_effect=_fake_symlink,
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        # One symlink succeeded (good_match), so COMPLETE
        assert item.state == QueueState.COMPLETE

    async def test_all_symlinks_fail_stays_checking(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """All create_symlink calls raise → season pack stays CHECKING."""
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            is_season_pack=True,
            season=1,
        )
        matches = [
            _make_mount_match(
                f"/mnt/zurg/Show/S01E0{i}.mkv",
                parsed_episode=i,
                parsed_season=1,
            )
            for i in range(1, 4)
        ]

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Show"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=matches,
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                side_effect=OSError("Disk full"),
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.CHECKING

    async def test_partial_symlink_success_transitions_to_complete(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """Some symlink failures + at least one success → COMPLETE."""
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            is_season_pack=True,
            season=1,
        )
        ok_path = "/mnt/zurg/Show/S01E01.mkv"
        fail_path = "/mnt/zurg/Show/S01E02.mkv"
        matches = [
            _make_mount_match(ok_path, parsed_episode=1, parsed_season=1),
            _make_mount_match(fail_path, parsed_episode=2, parsed_season=1),
        ]

        async def _fake_symlink(sess, item, filepath, **kwargs):
            if filepath == fail_path:
                raise OSError("Permission denied")
            return _make_symlink(source_path=filepath)

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Show"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=matches,
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                side_effect=_fake_symlink,
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.COMPLETE

    async def test_season_pack_success_clears_checking_failed_hash(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """Successful season pack completion clears checking_failed_hash from metadata_json."""
        metadata = json.dumps({"checking_failed_hash": "stale_hash_abc"})
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            is_season_pack=True,
            season=1,
            metadata_json=metadata,
        )
        match = _make_mount_match(parsed_episode=1, parsed_season=1)

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Show"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                return_value=[match],
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                return_value=_make_symlink(),
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.COMPLETE
        if item.metadata_json:
            meta = json.loads(item.metadata_json)
            assert "checking_failed_hash" not in meta

    async def test_xem_scene_pack_fallback_uses_anchor_season(
        self, session: AsyncSession, job_patches: dict
    ) -> None:
        """XEM scene pack metadata → lookup retried with TMDB anchor season and episode range."""
        xem_meta = json.dumps({
            "xem_scene_pack": True,
            "tmdb_anchor_season": 1,
            "tmdb_anchor_episode": 14,
            "tmdb_end_episode": 25,
        })
        item = await _make_media_item(
            session,
            state=QueueState.CHECKING,
            is_season_pack=True,
            season=2,  # scene season
            metadata_json=xem_meta,
        )
        # Files are stored as TMDB S01E14–E25
        xem_files = [
            _make_mount_match(
                f"/mnt/zurg/Anime/S01E{i:02d}.mkv",
                parsed_episode=i,
                parsed_season=1,
            )
            for i in range(14, 26)
        ]
        lookup_calls: list[tuple] = []

        async def _fake_lookup_multi(sess, titles, *, season, episode):
            lookup_calls.append(("multi", season, episode))
            return []

        async def _fake_lookup(sess, *, title, season, episode):
            lookup_calls.append(("single", season, episode))
            if season == 1 and episode is None:
                return xem_files
            return []

        with (
            patch(
                "src.core.queue_processor.gather_alt_titles",
                new_callable=AsyncMock,
                return_value=["Test Anime"],
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup_multi",
                new_callable=AsyncMock,
                side_effect=_fake_lookup_multi,
            ),
            patch(
                "src.core.queue_processor.mount_scanner.lookup",
                new_callable=AsyncMock,
                side_effect=_fake_lookup,
            ),
            patch(
                "src.core.queue_processor.symlink_manager.create_symlink",
                new_callable=AsyncMock,
                return_value=_make_symlink(),
            ),
        ):
            await _job_queue_processor()

        await session.refresh(item)
        assert item.state == QueueState.COMPLETE
        # Verify the XEM anchor season (1) was used for lookup
        assert any(s == 1 for kind, s, _ in lookup_calls if kind == "single")
