"""Application configuration loaded from environment variables and config.json."""

import json
import logging
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

CONFIG_FILE = Path("config.json")
CONFIG_EXAMPLE_FILE = Path("config.example.json")


# --- Nested config models ---


class RealDebridConfig(BaseModel):
    api_key: str = ""
    api_url: str = "https://api.real-debrid.com/rest/1.0"
    prefer_cached: bool = True
    allow_uncached: bool = False
    max_uncached_size_gb: int = 50


class TorrentioConfig(BaseModel):
    enabled: bool = True
    base_url: str = "https://torrentio.strem.fun"
    opts: str = ""
    timeout_seconds: int = 30
    max_results: int = 100


class ZileanConfig(BaseModel):
    enabled: bool = True
    base_url: str = "http://localhost:8182"
    timeout_seconds: int = 10


class ScrapersConfig(BaseModel):
    torrentio: TorrentioConfig = TorrentioConfig()
    zilean: ZileanConfig = ZileanConfig()


class PathsConfig(BaseModel):
    zurg_mount: str = "/home/hakan/Projects/homeserver/Zurg/mnt/__all__"
    library_movies: str = "/home/hakan/Projects/homeserver/vibeDebrid/library/movies"
    library_shows: str = "/home/hakan/Projects/homeserver/vibeDebrid/library/shows"


class QualityProfile(BaseModel):
    resolution_order: list[str] = ["2160p", "1080p", "720p"]
    min_resolution: str = "720p"
    preferred_codec: list[str] = ["x265", "hevc", "av1", "x264"]
    preferred_audio: list[str] = ["atmos", "truehd", "dts-hd", "dts", "aac"]
    preferred_source: list[str] = ["bluray", "web-dl", "webrip", "hdtv"]
    min_size_mb: int = 200
    max_size_gb: int = 80


class QualityConfig(BaseModel):
    default_profile: str = "high"
    profiles: dict[str, QualityProfile] = Field(default_factory=lambda: {
        "high": QualityProfile(),
        "standard": QualityProfile(
            resolution_order=["1080p", "720p"],
            min_resolution="720p",
            preferred_codec=["x265", "x264"],
            preferred_audio=["dts", "aac"],
            preferred_source=["web-dl", "webrip", "bluray"],
            min_size_mb=100,
            max_size_gb=20,
        ),
    })


class FiltersConfig(BaseModel):
    blocked_release_groups: list[str] = Field(default_factory=list)
    blocked_keywords: list[str] = Field(
        default_factory=lambda: ["cam", "ts", "telesync", "telecine", "hdcam"]
    )
    required_language: str | None = None
    allow_multi_audio: bool = True


class RetryConfig(BaseModel):
    schedule_minutes: list[int] = Field(
        default_factory=lambda: [30, 60, 120, 360, 720, 1440]
    )
    dormant_recheck_days: int = 7
    max_active_retries: int = 7
    unreleased_check_hours: int = 6


class UpgradeConfig(BaseModel):
    enabled: bool = True
    window_hours: int = 24
    check_interval_minutes: int = 60


class MountScannerConfig(BaseModel):
    scan_interval_minutes: int = 15
    scan_on_startup: bool = True


class TraktConfig(BaseModel):
    enabled: bool = False
    client_id: str = ""
    client_secret: str = ""
    access_token: str = ""
    refresh_token: str = ""
    poll_interval_minutes: int = 15
    lists: list[str] = Field(default_factory=lambda: ["watchlist"])


class PlexConfig(BaseModel):
    enabled: bool = False
    url: str = "http://localhost:32400"
    token: str = ""
    movie_section_ids: list[int] = Field(default_factory=list)
    show_section_ids: list[int] = Field(default_factory=list)
    scan_after_symlink: bool = True


class ServerConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 5100


class BackupConfig(BaseModel):
    enabled: bool = True
    interval_hours: int = 24
    max_backups: int = 7
    backup_dir: str = "./backups"


# --- Main settings ---


def _load_config_json() -> dict[str, Any]:
    """Load configuration from config.json, falling back to config.example.json."""
    for path in (CONFIG_FILE, CONFIG_EXAMPLE_FILE):
        if path.exists():
            logger.info("Loading configuration from %s", path)
            with open(path) as f:
                return json.load(f)
    return {}


class Settings(BaseSettings):
    """Application settings loaded from environment variables and config.json."""

    model_config = SettingsConfigDict(
        env_prefix="VIBE_",
        env_nested_delimiter="__",
    )

    database_url: str = "sqlite+aiosqlite:///./vibeDebrid.db"

    real_debrid: RealDebridConfig = RealDebridConfig()
    scrapers: ScrapersConfig = ScrapersConfig()
    paths: PathsConfig = PathsConfig()
    quality: QualityConfig = QualityConfig()
    filters: FiltersConfig = FiltersConfig()
    retry: RetryConfig = RetryConfig()
    upgrade: UpgradeConfig = UpgradeConfig()
    mount_scanner: MountScannerConfig = MountScannerConfig()
    trakt: TraktConfig = TraktConfig()
    plex: PlexConfig = PlexConfig()
    server: ServerConfig = ServerConfig()
    backup: BackupConfig = BackupConfig()

    @classmethod
    def load(cls) -> "Settings":
        """Load settings from config.json and environment variable overrides."""
        config_data = _load_config_json()
        return cls(**config_data)


# Module-level singleton — import `settings` from here.
settings = Settings.load()
