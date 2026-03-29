"""Tests for issue #21: no hardcoded developer paths in config defaults.

Covers:
  - PathsConfig defaults are empty strings (no hardcoded paths)
  - _validate_configured_paths logs WARNING for empty paths
  - _validate_configured_paths logs WARNING for non-existent paths
  - _validate_configured_paths logs nothing for valid existing paths
  - MountScanner.is_mount_available returns False when zurg_mount is empty
  - MountScanner.scan short-circuits when zurg_mount is empty
  - SymlinkManager.create_symlink raises SymlinkCreationError when
    library_movies is empty (movie item)
  - SymlinkManager.create_symlink raises SymlinkCreationError when
    library_shows is empty (show item)
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import PathsConfig
from src.core.mount_scanner import MountScanner, ScanResult
from src.core.symlink_manager import SymlinkCreationError, SymlinkManager
from src.models.media_item import MediaItem, MediaType, QueueState

# ---------------------------------------------------------------------------
# PathsConfig defaults
# ---------------------------------------------------------------------------


class TestPathsConfigDefaults:
    """PathsConfig must default to empty strings, not hardcoded developer paths."""

    def test_zurg_mount_default_is_empty(self) -> None:
        config = PathsConfig()
        assert config.zurg_mount == ""

    def test_library_movies_default_is_empty(self) -> None:
        config = PathsConfig()
        assert config.library_movies == ""

    def test_library_shows_default_is_empty(self) -> None:
        config = PathsConfig()
        assert config.library_shows == ""

    def test_no_hardcoded_developer_paths(self) -> None:
        """Ensure none of the defaults contain a developer-specific home path."""
        config = PathsConfig()
        for field_name, value in config.model_dump().items():
            assert not value.startswith("/home/"), (
                f"PathsConfig.{field_name} still contains a hardcoded developer path: {value!r}"
            )

    def test_paths_can_be_set_explicitly(self) -> None:
        """Paths set via config.json override must still work."""
        config = PathsConfig(
            zurg_mount="/mnt/zurg/__all__",
            library_movies="/media/library/movies",
            library_shows="/media/library/shows",
        )
        assert config.zurg_mount == "/mnt/zurg/__all__"
        assert config.library_movies == "/media/library/movies"
        assert config.library_shows == "/media/library/shows"


# ---------------------------------------------------------------------------
# _validate_configured_paths
# ---------------------------------------------------------------------------


class TestValidateConfiguredPaths:
    """Startup path validator logs appropriate warnings."""

    def _call_validate(self, monkeypatch, paths_config: PathsConfig) -> None:
        """Patch settings.paths and call _validate_configured_paths."""
        from src import config as cfg_module
        from src.main import _validate_configured_paths

        real_settings = cfg_module.settings
        monkeypatch.setattr(real_settings, "paths", paths_config)
        _validate_configured_paths()

    def test_warns_when_zurg_mount_empty(self, monkeypatch, caplog) -> None:
        import logging
        with caplog.at_level(logging.WARNING, logger="src.main"):
            self._call_validate(
                monkeypatch,
                PathsConfig(zurg_mount="", library_movies="", library_shows=""),
            )
        assert any("paths.zurg_mount" in msg for msg in caplog.messages)
        assert any("is not set" in msg for msg in caplog.messages)

    def test_warns_when_library_movies_empty(self, monkeypatch, caplog) -> None:
        import logging
        with caplog.at_level(logging.WARNING, logger="src.main"):
            self._call_validate(
                monkeypatch,
                PathsConfig(zurg_mount="", library_movies="", library_shows=""),
            )
        assert any("paths.library_movies" in msg for msg in caplog.messages)

    def test_warns_when_library_shows_empty(self, monkeypatch, caplog) -> None:
        import logging
        with caplog.at_level(logging.WARNING, logger="src.main"):
            self._call_validate(
                monkeypatch,
                PathsConfig(zurg_mount="", library_movies="", library_shows=""),
            )
        assert any("paths.library_shows" in msg for msg in caplog.messages)

    def test_warns_when_path_does_not_exist(self, monkeypatch, caplog) -> None:
        import logging
        nonexistent = "/this/path/definitely/does/not/exist/on/any/machine"
        with caplog.at_level(logging.WARNING, logger="src.main"):
            self._call_validate(
                monkeypatch,
                PathsConfig(
                    zurg_mount=nonexistent,
                    library_movies=nonexistent,
                    library_shows=nonexistent,
                ),
            )
        assert any("does not exist" in msg for msg in caplog.messages)

    def test_no_warning_when_all_paths_exist(self, monkeypatch, caplog, tmp_path: Path) -> None:
        """No warnings emitted when all paths are non-empty and exist on disk."""
        import logging
        mount_dir = tmp_path / "zurg"
        movies_dir = tmp_path / "movies"
        shows_dir = tmp_path / "shows"
        for d in (mount_dir, movies_dir, shows_dir):
            d.mkdir()

        with caplog.at_level(logging.WARNING, logger="src.main"):
            self._call_validate(
                monkeypatch,
                PathsConfig(
                    zurg_mount=str(mount_dir),
                    library_movies=str(movies_dir),
                    library_shows=str(shows_dir),
                ),
            )
        # No WARNING-level messages should be present
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert warnings == [], f"Unexpected warnings: {[r.message for r in warnings]}"


# ---------------------------------------------------------------------------
# MountScanner — empty zurg_mount guard
# ---------------------------------------------------------------------------


class TestMountScannerEmptyPath:
    """MountScanner degrades gracefully when zurg_mount is not configured."""

    @pytest.fixture
    def scanner(self) -> MountScanner:
        return MountScanner()

    async def test_is_mount_available_returns_false_when_empty(
        self, scanner: MountScanner, monkeypatch
    ) -> None:
        from src import config as cfg_module
        monkeypatch.setattr(cfg_module.settings.paths, "zurg_mount", "")
        result = await scanner.is_mount_available()
        assert result is False

    async def test_is_mount_available_logs_warning_when_empty(
        self, scanner: MountScanner, monkeypatch, caplog
    ) -> None:
        import logging

        from src import config as cfg_module
        monkeypatch.setattr(cfg_module.settings.paths, "zurg_mount", "")
        with caplog.at_level(logging.WARNING, logger="src.core.mount_scanner"):
            await scanner.is_mount_available()
        assert any("zurg_mount" in msg for msg in caplog.messages)

    async def test_scan_returns_empty_result_when_mount_empty(
        self, scanner: MountScanner, monkeypatch, session: AsyncSession
    ) -> None:
        """scan() must not crash when zurg_mount is empty — returns zero counts."""
        from src import config as cfg_module
        monkeypatch.setattr(cfg_module.settings.paths, "zurg_mount", "")
        result = await scanner.scan(session)
        assert isinstance(result, ScanResult)
        assert result.files_found == 0
        assert result.files_added == 0
        assert result.files_removed == 0

    async def test_is_mount_available_still_checks_real_path(
        self, scanner: MountScanner, monkeypatch, tmp_path: Path
    ) -> None:
        """When path is set and the directory exists and is non-empty, returns True."""
        from src import config as cfg_module
        (tmp_path / "dummy").touch()  # Must be non-empty (empty = rclone down)
        monkeypatch.setattr(cfg_module.settings.paths, "zurg_mount", str(tmp_path))
        result = await scanner.is_mount_available()
        assert result is True


# ---------------------------------------------------------------------------
# SymlinkManager — empty library path guard
# ---------------------------------------------------------------------------


class TestSymlinkManagerEmptyPaths:
    """SymlinkManager raises SymlinkCreationError when library paths are not configured."""

    @pytest.fixture
    def manager(self) -> SymlinkManager:
        return SymlinkManager()

    @pytest.fixture
    async def movie_item(self, session: AsyncSession) -> MediaItem:
        item = MediaItem(
            imdb_id="tt0000001",
            title="Empty Path Movie",
            year=2024,
            media_type=MediaType.MOVIE,
            state=QueueState.COMPLETE,
            retry_count=0,
        )
        session.add(item)
        await session.flush()
        return item

    @pytest.fixture
    async def show_item(self, session: AsyncSession) -> MediaItem:
        item = MediaItem(
            imdb_id="tt0000002",
            title="Empty Path Show",
            year=2024,
            media_type=MediaType.SHOW,
            state=QueueState.COMPLETE,
            season=1,
            episode=1,
            retry_count=0,
        )
        session.add(item)
        await session.flush()
        return item

    async def test_movie_raises_when_library_movies_empty(
        self,
        manager: SymlinkManager,
        movie_item: MediaItem,
        session: AsyncSession,
        monkeypatch,
        tmp_path: Path,
    ) -> None:
        """create_symlink raises SymlinkCreationError for movies when library_movies is empty."""
        from src import config as cfg_module

        # Create a real source file so the source-exists check passes
        source_file = tmp_path / "movie.mkv"
        source_file.write_text("fake")

        monkeypatch.setattr(cfg_module.settings.paths, "library_movies", "")

        with pytest.raises(SymlinkCreationError) as exc_info:
            await manager.create_symlink(session, movie_item, str(source_file))

        assert "library_movies" in str(exc_info.value)

    async def test_show_raises_when_library_shows_empty(
        self,
        manager: SymlinkManager,
        show_item: MediaItem,
        session: AsyncSession,
        monkeypatch,
        tmp_path: Path,
    ) -> None:
        """create_symlink raises SymlinkCreationError for shows when library_shows is empty."""
        from src import config as cfg_module

        source_file = tmp_path / "episode.mkv"
        source_file.write_text("fake")

        monkeypatch.setattr(cfg_module.settings.paths, "library_shows", "")

        with pytest.raises(SymlinkCreationError) as exc_info:
            await manager.create_symlink(session, show_item, str(source_file))

        assert "library_shows" in str(exc_info.value)

    async def test_movie_succeeds_when_library_movies_set(
        self,
        manager: SymlinkManager,
        movie_item: MediaItem,
        session: AsyncSession,
        monkeypatch,
        tmp_path: Path,
    ) -> None:
        """Sanity check: create_symlink works when library_movies is properly configured."""
        from src import config as cfg_module
        from src.config import SymlinkNamingConfig

        movies_dir = tmp_path / "movies"
        movies_dir.mkdir()
        source_file = tmp_path / "movie.mkv"
        source_file.write_text("fake")

        monkeypatch.setattr(cfg_module.settings.paths, "library_movies", str(movies_dir))
        monkeypatch.setattr(
            cfg_module.settings,
            "symlink_naming",
            SymlinkNamingConfig(date_prefix=False, release_year=True, resolution=False),
        )

        symlink = await manager.create_symlink(session, movie_item, str(source_file))
        assert symlink is not None
        assert os.path.islink(symlink.target_path)
