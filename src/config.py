"""Application configuration loaded from environment variables and config.json."""

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

# VIBE_DATA_DIR lets Docker deployments redirect all runtime data (config, DB)
# to a mounted volume.  Without it the behaviour is unchanged: "." (cwd).
_data_dir = Path(os.environ.get("VIBE_DATA_DIR", "."))
CONFIG_FILE = _data_dir / "config.json"
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
    timeout_seconds: int = 10
    max_results: int = 100


class ZileanConfig(BaseModel):
    enabled: bool = True
    base_url: str = "http://localhost:8182"
    timeout_seconds: int = 10


class TmdbConfig(BaseModel):
    enabled: bool = True
    api_key: str = ""
    base_url: str = "https://api.themoviedb.org/3"
    image_base_url: str = "https://image.tmdb.org/t/p"
    timeout_seconds: int = 10


class ScrapersConfig(BaseModel):
    torrentio: TorrentioConfig = TorrentioConfig()
    zilean: ZileanConfig = ZileanConfig()


class PathsConfig(BaseModel):
    zurg_mount: str = ""
    library_movies: str = ""
    library_shows: str = ""


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
    preferred_languages: list[str] = Field(default_factory=list)  # Ordered list, e.g. ["English", "Japanese"]
    required_language: str | None = None  # Legacy fallback; ignored when preferred_languages is set
    allow_multi_audio: bool = True
    prefer_original_language: bool = False
    dub_penalty: int = Field(default=20, ge=0, le=50)
    dual_audio_bonus: int = Field(default=10, ge=0, le=30)
    cached_bonus: int = Field(default=25, ge=0, le=100)
    title_similarity_threshold: float = Field(default=0.0, ge=0.0, le=1.0)
    title_similarity_bonus: float = Field(default=15.0, ge=0.0, le=50.0)


class RetryConfig(BaseModel):
    schedule_minutes: list[int] = Field(
        default_factory=lambda: [30, 60, 120, 360, 720, 1440]
    )
    dormant_recheck_days: int = 7
    max_active_retries: int = 7
    unreleased_check_hours: int = 6
    checking_timeout_minutes: int = Field(default=30, ge=1)


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
    watchlist_sync_enabled: bool = False
    watchlist_poll_minutes: int = Field(default=30, ge=15)


class JellyfinConfig(BaseModel):
    enabled: bool = False
    url: str = "http://localhost:8096"
    api_key: str = ""
    movie_library_ids: list[str] = Field(default_factory=list)
    show_library_ids: list[str] = Field(default_factory=list)
    scan_after_symlink: bool = True


class ServerConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 5100


class BackupConfig(BaseModel):
    enabled: bool = True
    interval_hours: int = 24
    max_backups: int = 7
    backup_dir: str = str(_data_dir / "backups")


class SchedulerConfig(BaseModel):
    queue_processor_minutes: int = 5
    symlink_verifier_minutes: int = 30
    unreleased_check_minutes: int = 360
    monitored_shows_hours: int = 6


class SymlinkNamingConfig(BaseModel):
    date_prefix: bool = True
    release_year: bool = True
    resolution: bool = False
    plex_naming: bool = False  # When True, generate Plex-formatted filenames, overriding above settings
    generate_nfo: bool = False


class SearchConfig(BaseModel):
    cache_check_limit: int = Field(default=10, ge=0)  # Number of search results to auto-check RD cache status


class XemConfig(BaseModel):
    """Configuration for TheXEM scene numbering service."""

    enabled: bool = True
    base_url: str = "https://thexem.info"
    cache_hours: int = 24
    timeout_seconds: int = 10


class OmdbConfig(BaseModel):
    """Configuration for the Open Movie Database (OMDb) API."""

    enabled: bool = False
    api_key: str = ""
    base_url: str = "http://www.omdbapi.com"
    cache_hours: int = 168  # 7 days
    timeout_seconds: int = 10


class AnidbConfig(BaseModel):
    """Configuration for AniDB title enrichment and episode count lookups."""

    enabled: bool = False
    api_enabled: bool = False
    client_name: str = ""
    client_version: int = 1
    titles_url: str = "https://anidb.net/api/anime-titles.xml.gz"
    mappings_url: str = "https://raw.githubusercontent.com/Fribb/anime-lists/master/anime-lists-reduced.json"
    api_base_url: str = "http://api.anidb.net:9001/httpapi"
    refresh_hours: int = Field(default=168, ge=1)
    timeout_seconds: int = 30
    title_languages: list[str] = ["x-jat", "en", "ja"]


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

    database_url: str = f"sqlite+aiosqlite:///{_data_dir / 'vibeDebrid.db'}"

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
    jellyfin: JellyfinConfig = JellyfinConfig()
    server: ServerConfig = ServerConfig()
    backup: BackupConfig = BackupConfig()
    scheduler: SchedulerConfig = SchedulerConfig()
    symlink_naming: SymlinkNamingConfig = SymlinkNamingConfig()
    search: SearchConfig = SearchConfig()
    tmdb: TmdbConfig = TmdbConfig()
    xem: XemConfig = XemConfig()
    omdb: OmdbConfig = OmdbConfig()
    anidb: AnidbConfig = AnidbConfig()

    @classmethod
    def load(cls) -> "Settings":
        """Load settings from config.json and environment variable overrides."""
        config_data = _load_config_json()
        return cls(**config_data)


# Module-level singleton — import `settings` from here.
settings = Settings.load()

# Shared lock for concurrent config.json writes.
# Import this in any module that reads/writes config.json to avoid races.
config_lock = asyncio.Lock()
