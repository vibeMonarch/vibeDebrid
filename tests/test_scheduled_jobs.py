"""Tests for the five scheduled job functions defined in src/main.py.

Each job follows the same pattern:
    session = async_session()
    try:
        await <service_call>(session)
        await session.commit()
    except Exception:
        await session.rollback()
    finally:
        await session.close()

Strategy
--------
- Patch ``src.main.async_session`` to return a mock session with AsyncMock
  ``commit``, ``rollback``, and ``close`` methods.
- Patch the underlying service call so no real I/O occurs.
- For jobs with lazy imports (show_manager, sync_watchlist, anidb_client),
  patch at the module path where the name is looked up inside the function.
- For jobs with early-return guards (plex_watchlist_sync), monkeypatch
  ``src.main.settings.plex`` to control guard conditions.

asyncio_mode = "auto" is set in pyproject.toml — no @pytest.mark.asyncio needed.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_mock_session() -> MagicMock:
    """Return a mock async session with commit/rollback/close as AsyncMocks."""
    session = MagicMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.close = AsyncMock()
    return session


def _make_session_factory(session: MagicMock) -> MagicMock:
    """Return a callable that returns *session* when called (mimics async_session())."""
    factory = MagicMock(return_value=session)
    return factory


# ---------------------------------------------------------------------------
# TestJobMountScan
# ---------------------------------------------------------------------------


class TestJobMountScan:
    """Scheduled job: scan the Zurg mount and update the file index."""

    async def test_success_calls_scan_and_commits(self) -> None:
        """Happy path: mount_scanner.scan is awaited and session is committed."""
        from src.main import _job_mount_scan

        session = _make_mock_session()
        scan_result = MagicMock(files_found=10, files_added=3, files_removed=1)

        with (
            patch("src.main.async_session", _make_session_factory(session)),
            patch(
                "src.main.mount_scanner.scan",
                new_callable=AsyncMock,
                return_value=scan_result,
            ) as mock_scan,
        ):
            await _job_mount_scan()

        mock_scan.assert_awaited_once_with(session)
        session.commit.assert_awaited_once()
        session.rollback.assert_not_awaited()
        session.close.assert_awaited_once()

    async def test_failure_rolls_back_and_does_not_crash(self) -> None:
        """When mount_scanner.scan raises, rollback is called and exception is swallowed."""
        from src.main import _job_mount_scan

        session = _make_mock_session()

        with (
            patch("src.main.async_session", _make_session_factory(session)),
            patch(
                "src.main.mount_scanner.scan",
                new_callable=AsyncMock,
                side_effect=RuntimeError("mount unavailable"),
            ),
        ):
            # Must not raise
            await _job_mount_scan()

        session.commit.assert_not_awaited()
        session.rollback.assert_awaited_once()
        session.close.assert_awaited_once()


# ---------------------------------------------------------------------------
# TestJobSymlinkVerifier
# ---------------------------------------------------------------------------


class TestJobSymlinkVerifier:
    """Scheduled job: verify existing symlinks are still valid."""

    async def test_success_calls_verify_and_commits(self) -> None:
        """Happy path: symlink_manager.verify_symlinks is awaited and session is committed."""
        from src.main import _job_symlink_verifier

        session = _make_mock_session()
        verify_result = MagicMock(total_checked=50, broken_count=2)

        with (
            patch("src.main.async_session", _make_session_factory(session)),
            patch(
                "src.main.symlink_manager.verify_symlinks",
                new_callable=AsyncMock,
                return_value=verify_result,
            ) as mock_verify,
        ):
            await _job_symlink_verifier()

        mock_verify.assert_awaited_once_with(session)
        session.commit.assert_awaited_once()
        session.rollback.assert_not_awaited()
        session.close.assert_awaited_once()

    async def test_failure_rolls_back_and_does_not_crash(self) -> None:
        """When verify_symlinks raises, rollback is called and exception is swallowed."""
        from src.main import _job_symlink_verifier

        session = _make_mock_session()

        with (
            patch("src.main.async_session", _make_session_factory(session)),
            patch(
                "src.main.symlink_manager.verify_symlinks",
                new_callable=AsyncMock,
                side_effect=OSError("filesystem error"),
            ),
        ):
            await _job_symlink_verifier()

        session.commit.assert_not_awaited()
        session.rollback.assert_awaited_once()
        session.close.assert_awaited_once()


# ---------------------------------------------------------------------------
# TestJobCheckMonitoredShows
# ---------------------------------------------------------------------------


class TestJobCheckMonitoredShows:
    """Scheduled job: check monitored shows for new episodes."""

    async def test_success_calls_check_and_commits(self) -> None:
        """Happy path: show_manager.check_monitored_shows is awaited and committed."""
        from src.main import _job_check_monitored_shows

        session = _make_mock_session()
        check_result: dict[str, Any] = {"checked": 5, "new_items": 2}

        mock_show_manager = MagicMock()
        mock_show_manager.check_monitored_shows = AsyncMock(return_value=check_result)

        with (
            patch("src.main.async_session", _make_session_factory(session)),
            patch("src.core.show_manager.show_manager", mock_show_manager),
        ):
            await _job_check_monitored_shows()

        mock_show_manager.check_monitored_shows.assert_awaited_once_with(session)
        session.commit.assert_awaited_once()
        session.rollback.assert_not_awaited()
        session.close.assert_awaited_once()

    async def test_failure_rolls_back_and_does_not_crash(self) -> None:
        """When check_monitored_shows raises, rollback is called and exception is swallowed."""
        from src.main import _job_check_monitored_shows

        session = _make_mock_session()

        mock_show_manager = MagicMock()
        mock_show_manager.check_monitored_shows = AsyncMock(
            side_effect=ValueError("DB error")
        )

        with (
            patch("src.main.async_session", _make_session_factory(session)),
            patch("src.core.show_manager.show_manager", mock_show_manager),
        ):
            await _job_check_monitored_shows()

        session.commit.assert_not_awaited()
        session.rollback.assert_awaited_once()
        session.close.assert_awaited_once()


# ---------------------------------------------------------------------------
# TestJobPlexWatchlistSync
# ---------------------------------------------------------------------------


class TestJobPlexWatchlistSync:
    """Scheduled job: sync Plex watchlist items to the queue."""

    async def test_skipped_when_no_plex_token(self) -> None:
        """Returns immediately without creating a session when plex.token is empty."""
        from src.main import _job_plex_watchlist_sync

        mock_plex_cfg = MagicMock()
        mock_plex_cfg.token = ""
        mock_plex_cfg.watchlist_sync_enabled = True

        session = _make_mock_session()

        with (
            patch("src.main.settings.plex", mock_plex_cfg),
            patch("src.main.async_session", _make_session_factory(session)),
        ):
            await _job_plex_watchlist_sync()

        # No session interaction expected — function returned before creating one
        session.commit.assert_not_awaited()
        session.rollback.assert_not_awaited()
        session.close.assert_not_awaited()

    async def test_skipped_when_watchlist_sync_disabled(self) -> None:
        """Returns immediately without creating a session when watchlist_sync_enabled=False."""
        from src.main import _job_plex_watchlist_sync

        mock_plex_cfg = MagicMock()
        mock_plex_cfg.token = "valid-plex-token"
        mock_plex_cfg.watchlist_sync_enabled = False

        session = _make_mock_session()

        with (
            patch("src.main.settings.plex", mock_plex_cfg),
            patch("src.main.async_session", _make_session_factory(session)),
        ):
            await _job_plex_watchlist_sync()

        session.commit.assert_not_awaited()
        session.rollback.assert_not_awaited()
        session.close.assert_not_awaited()

    async def test_success_with_added_items(self) -> None:
        """Happy path: sync_watchlist is called, session committed, result logged."""
        from src.main import _job_plex_watchlist_sync

        mock_plex_cfg = MagicMock()
        mock_plex_cfg.token = "valid-plex-token"
        mock_plex_cfg.watchlist_sync_enabled = True

        session = _make_mock_session()
        sync_result: dict[str, Any] = {"added": 3, "skipped": 1, "errors": 0}
        mock_sync = AsyncMock(return_value=sync_result)

        with (
            patch("src.main.settings.plex", mock_plex_cfg),
            patch("src.main.async_session", _make_session_factory(session)),
            patch("src.core.plex_watchlist.sync_watchlist", mock_sync),
        ):
            await _job_plex_watchlist_sync()

        mock_sync.assert_awaited_once_with(session)
        session.commit.assert_awaited_once()
        session.rollback.assert_not_awaited()
        session.close.assert_awaited_once()

    async def test_failure_rolls_back_and_does_not_crash(self) -> None:
        """When sync_watchlist raises, rollback is called and exception is swallowed."""
        from src.main import _job_plex_watchlist_sync

        mock_plex_cfg = MagicMock()
        mock_plex_cfg.token = "valid-plex-token"
        mock_plex_cfg.watchlist_sync_enabled = True

        session = _make_mock_session()

        with (
            patch("src.main.settings.plex", mock_plex_cfg),
            patch("src.main.async_session", _make_session_factory(session)),
            patch(
                "src.core.plex_watchlist.sync_watchlist",
                AsyncMock(side_effect=ConnectionError("Plex unreachable")),
            ),
        ):
            await _job_plex_watchlist_sync()

        session.commit.assert_not_awaited()
        session.rollback.assert_awaited_once()
        session.close.assert_awaited_once()


# ---------------------------------------------------------------------------
# TestJobAnidbRefresh
# ---------------------------------------------------------------------------


class TestJobAnidbRefresh:
    """Scheduled job: refresh AniDB title dump and Fribb mapping."""

    async def test_success_calls_refresh_and_commits(self) -> None:
        """Happy path: anidb_client.refresh_data is awaited and session is committed."""
        from src.main import _job_anidb_refresh

        session = _make_mock_session()
        mock_anidb = MagicMock()
        mock_anidb.refresh_data = AsyncMock(return_value=None)

        with (
            patch("src.main.async_session", _make_session_factory(session)),
            patch("src.services.anidb.anidb_client", mock_anidb),
        ):
            await _job_anidb_refresh()

        mock_anidb.refresh_data.assert_awaited_once_with(session)
        session.commit.assert_awaited_once()
        session.rollback.assert_not_awaited()
        session.close.assert_awaited_once()

    async def test_failure_rolls_back_and_does_not_crash(self) -> None:
        """When refresh_data raises, rollback is called and exception is swallowed."""
        from src.main import _job_anidb_refresh

        session = _make_mock_session()
        mock_anidb = MagicMock()
        mock_anidb.refresh_data = AsyncMock(side_effect=OSError("network timeout"))

        with (
            patch("src.main.async_session", _make_session_factory(session)),
            patch("src.services.anidb.anidb_client", mock_anidb),
        ):
            await _job_anidb_refresh()

        session.commit.assert_not_awaited()
        session.rollback.assert_awaited_once()
        session.close.assert_awaited_once()
