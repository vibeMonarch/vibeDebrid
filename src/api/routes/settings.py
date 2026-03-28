"""Settings endpoints."""

import asyncio
import json
import logging
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, model_validator

from src.config import (
    CONFIG_FILE,
    AnidbConfig,
    BackupConfig,
    FiltersConfig,
    JellyfinConfig,
    MountScannerConfig,
    OmdbConfig,
    PathsConfig,
    PlexConfig,
    QualityConfig,
    RealDebridConfig,
    RetryConfig,
    SchedulerConfig,
    ScrapersConfig,
    SearchConfig,
    ServerConfig,
    Settings,
    SymlinkNamingConfig,
    TmdbConfig,
    TraktConfig,
    UpgradeConfig,
    XemConfig,
    config_lock,
    settings,
)
from src.services.http_client import invalidate_all_clients
from src.services.nyaa import nyaa_client
from src.services.plex import plex_client
from src.services.real_debrid import RealDebridAuthError, RealDebridError, rd_client
from src.services.torrentio import torrentio_client
from src.services.zilean import zilean_client

logger = logging.getLogger(__name__)

router = APIRouter()

# Known top-level settings fields. Keys not in this set are rejected.
class SettingsUpdate(BaseModel):
    """Validated subset of Settings that the PUT endpoint accepts.

    Only known top-level keys are permitted.  Unknown keys raise 422 before
    any data is written so arbitrary config injection is prevented.
    """

    real_debrid: RealDebridConfig | None = None
    scrapers: ScrapersConfig | None = None
    paths: PathsConfig | None = None
    quality: QualityConfig | None = None
    filters: FiltersConfig | None = None
    retry: RetryConfig | None = None
    upgrade: UpgradeConfig | None = None
    mount_scanner: MountScannerConfig | None = None
    trakt: TraktConfig | None = None
    plex: PlexConfig | None = None
    jellyfin: JellyfinConfig | None = None
    server: ServerConfig | None = None
    backup: BackupConfig | None = None
    scheduler: SchedulerConfig | None = None
    symlink_naming: SymlinkNamingConfig | None = None
    search: SearchConfig | None = None
    tmdb: TmdbConfig | None = None
    xem: XemConfig | None = None
    omdb: OmdbConfig | None = None
    anidb: AnidbConfig | None = None

    model_config = {"extra": "forbid"}

    @model_validator(mode="before")
    @classmethod
    def _reject_unknown_keys(cls, values: Any) -> Any:
        if isinstance(values, dict):
            unknown = set(values.keys()) - _KNOWN_SETTINGS_KEYS
            if unknown:
                raise ValueError(f"Unknown settings keys: {sorted(unknown)}")
        return values

    def to_partial_dict(self) -> dict[str, Any]:
        """Return only the fields that were explicitly provided."""
        return self.model_dump(exclude_none=True)


# Known keys derived from SettingsUpdate (not Settings) to exclude database_url etc.
_KNOWN_SETTINGS_KEYS: frozenset[str] = frozenset(SettingsUpdate.model_fields.keys())


# Pydantic schemas
class TestResult(BaseModel):
    status: str
    message: str


def _mask_api_keys(data: dict[str, Any]) -> dict[str, Any]:
    """Recursively mask fields containing 'api_key', 'token', 'secret'."""
    masked = {}
    for key, value in data.items():
        if isinstance(value, dict):
            masked[key] = _mask_api_keys(value)
        elif any(s in key.lower() for s in ("api_key", "token", "secret")) and isinstance(value, str) and value:
            # Show only last 4 characters to minimize key exposure
            masked[key] = "***" + value[-4:] if len(value) >= 4 else "***"
        else:
            masked[key] = value
    return masked


@router.get("")
async def get_settings() -> dict[str, Any]:
    """Current configuration with masked API keys."""
    data = settings.model_dump()
    return {"settings": _mask_api_keys(data)}


@router.put("")
async def update_settings(body: SettingsUpdate) -> dict[str, Any]:
    """Update configuration by writing to config.json and reloading."""
    async with config_lock:
        # Read existing config
        existing: dict[str, Any] = {}
        if CONFIG_FILE.exists():
            raw = await asyncio.to_thread(CONFIG_FILE.read_text)
            existing = json.loads(raw)

        # Deep merge new values (only the fields that were provided)
        # Credential fields are never overwritten with empty strings — the
        # frontend intentionally omits them when unchanged, but Pydantic
        # fills sub-model defaults with "", which would wipe saved keys.
        _CREDENTIAL_SUBSTRINGS = ("api_key", "token", "secret")

        def _is_credential_key(key: str) -> bool:
            kl = key.lower()
            return any(s in kl for s in _CREDENTIAL_SUBSTRINGS)

        def _deep_merge(base: dict, update: dict) -> dict:
            for key, value in update.items():
                if isinstance(value, dict) and isinstance(base.get(key), dict):
                    base[key] = _deep_merge(base[key], value)
                elif _is_credential_key(key) and value == "" and base.get(key):
                    continue  # preserve existing credential
                else:
                    base[key] = value
            return base

        merged = _deep_merge(existing, body.to_partial_dict())

        # Final validation by constructing a full Settings instance
        try:
            Settings(**merged)
        except (ValueError, TypeError) as exc:
            logger.warning("Settings validation failed on fields: %s",
                           [e["loc"] for e in exc.errors()] if hasattr(exc, "errors") else str(type(exc).__name__))
            raise HTTPException(status_code=422, detail="Invalid settings: validation failed") from exc

        # Write to config.json
        await asyncio.to_thread(CONFIG_FILE.write_text, json.dumps(merged, indent=2))

        # Reload the in-memory singleton so changes take effect immediately
        reloaded = Settings.load()
        for field in reloaded.model_fields:
            setattr(settings, field, getattr(reloaded, field))

        # Invalidate all pooled HTTP clients so they are recreated with the
        # new credentials on their next request.
        await invalidate_all_clients()

    logger.info("Settings updated and written to %s", CONFIG_FILE)
    return {"status": "ok", "message": "Settings updated."}


@router.post("/test/realdebrid")
async def test_realdebrid() -> TestResult:
    """Test RD API connection by calling get_user."""
    try:
        user = await rd_client.get_user()
        username = user.get("username", "unknown")
        days_left = int(user.get("premium") or 0) // 86400
        raw_exp = user.get("expiration", "")
        expires = raw_exp.split("T")[0] if raw_exp else "unknown"
        return TestResult(
            status="ok",
            message=f"Connected as {username} ({days_left}d remaining, expires {expires})",
        )
    except RealDebridAuthError as exc:
        return TestResult(status="error", message=f"Authentication failed: {exc}")
    except RealDebridError as exc:
        return TestResult(status="error", message=f"API error: {exc}")
    except Exception as exc:
        return TestResult(status="error", message=f"Connection failed: {exc}")


@router.post("/test/torrentio")
async def test_torrentio() -> TestResult:
    """Test Torrentio endpoint by scraping a known movie (The Matrix tt0133093)."""
    try:
        results = await torrentio_client.scrape_movie("tt0133093")
        return TestResult(
            status="ok",
            message=f"Torrentio reachable, returned {len(results)} results for test query",
        )
    except Exception as exc:
        return TestResult(status="error", message=f"Connection failed: {exc}")


@router.post("/test/zilean")
async def test_zilean() -> TestResult:
    """Test Zilean endpoint by running a simple search."""
    try:
        results = await zilean_client.search("test")
        return TestResult(
            status="ok",
            message=f"Zilean reachable, returned {len(results)} results for test query",
        )
    except Exception as exc:
        return TestResult(status="error", message=f"Connection failed: {exc}")


@router.post("/test/nyaa")
async def test_nyaa() -> TestResult:
    """Test Nyaa.si connection by running a simple RSS search."""
    cfg = settings.scrapers.nyaa
    if not cfg.enabled:
        return TestResult(status="error", message="Nyaa scraper is disabled — enable it in settings first")
    try:
        results = await nyaa_client.search("1080p")
        if results:
            return TestResult(
                status="ok",
                message=f"Nyaa reachable, returned {len(results)} result(s) for test query",
            )
        # An empty result list is still a successful connection — the RSS feed
        # parsed without error (0 hits for "1080p" is unusual but valid).
        return TestResult(
            status="ok",
            message="Nyaa reachable, RSS feed parsed (0 results for test query)",
        )
    except Exception as exc:
        return TestResult(status="error", message=f"Connection failed: {exc}")


@router.post("/test/tmdb")
async def test_tmdb() -> TestResult:
    """Test TMDB API connection."""
    from src.services.tmdb import tmdb_client
    try:
        ok = await tmdb_client.test_connection()
        if ok:
            return TestResult(status="ok", message="TMDB API reachable and key is valid")
        return TestResult(status="error", message="TMDB API returned an error — check your API key")
    except Exception as exc:
        return TestResult(status="error", message=f"Connection failed: {exc}")


@router.post("/test/plex")
async def test_plex() -> TestResult:
    """Test Plex connection and report library count."""
    try:
        ok = await plex_client.test_connection()
        if not ok:
            return TestResult(status="error", message="Could not connect to Plex — check URL and token")
        libs = await plex_client.get_libraries()
        return TestResult(status="ok", message=f"Connected to Plex, found {len(libs)} library section(s)")
    except Exception as exc:
        return TestResult(status="error", message=f"Connection failed: {exc}")


@router.post("/plex/auth/start")
async def plex_auth_start() -> dict[str, Any]:
    """Start Plex OAuth PIN-based auth flow."""
    pin = await plex_client.create_pin()
    if pin is None:
        raise HTTPException(status_code=502, detail="Could not create Plex auth PIN")
    return {"pin_id": pin.pin_id, "code": pin.code, "auth_url": pin.auth_url}


@router.post("/plex/auth/check/{pin_id}")
async def plex_auth_check(pin_id: int) -> dict[str, Any]:
    """Poll a Plex auth PIN. Returns token (masked) if user has authenticated."""
    token = await plex_client.check_pin(pin_id)
    if token is None:
        return {"status": "pending"}

    # Save token to config.json and reload singleton
    async with config_lock:
        existing: dict[str, Any] = {}
        if CONFIG_FILE.exists():
            raw = await asyncio.to_thread(CONFIG_FILE.read_text)
            existing = json.loads(raw)
        existing.setdefault("plex", {})["token"] = token
        await asyncio.to_thread(CONFIG_FILE.write_text, json.dumps(existing, indent=2))

        # Reload in-memory settings
        reloaded = Settings.load()
        for field in reloaded.model_fields:
            setattr(settings, field, getattr(reloaded, field))

        await invalidate_all_clients()

    masked = "***" + token[-4:] if len(token) >= 4 else "***"
    return {"status": "authenticated", "token_masked": masked}


@router.post("/test/omdb")
async def test_omdb() -> TestResult:
    """Test OMDb API connection by fetching The Matrix (tt0133093)."""
    from src.services.omdb import omdb_client
    try:
        result = await omdb_client.test_connection()
        if result:
            return TestResult(
                status="ok",
                message=f"Connected — tested with: {result.get('Title', 'unknown')}",
            )
        return TestResult(status="error", message="OMDb not configured or disabled")
    except Exception as exc:
        return TestResult(status="error", message=f"Connection failed: {exc}")


@router.get("/plex/libraries")
async def plex_libraries() -> dict[str, Any]:
    """Fetch Plex library sections."""
    libs = await plex_client.get_libraries()
    return {"sections": [s.model_dump() for s in libs]}


@router.post("/test/jellyfin")
async def test_jellyfin() -> TestResult:
    """Test Jellyfin server connection."""
    from src.services.jellyfin import jellyfin_client
    try:
        ok = await jellyfin_client.test_connection()
        if not ok:
            return TestResult(
                status="error",
                message="Could not connect to Jellyfin — check URL and API key",
            )
        libs = await jellyfin_client.get_libraries()
        return TestResult(
            status="ok",
            message=f"Connected to Jellyfin, found {len(libs)} library(ies)",
        )
    except Exception as exc:
        return TestResult(status="error", message=f"Connection failed: {exc}")


@router.get("/jellyfin/libraries")
async def jellyfin_libraries() -> dict[str, Any]:
    """Fetch Jellyfin library folders."""
    from src.services.jellyfin import jellyfin_client
    libs = await jellyfin_client.get_libraries()
    return {"libraries": [lib.model_dump() for lib in libs]}
