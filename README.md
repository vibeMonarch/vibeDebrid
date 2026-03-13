# vibeDebrid

A self-hosted Real-Debrid media automation system. Automates torrent scraping, cache checking, downloading via Real-Debrid, and symlink-based library organization for Plex.

Built as a replacement for CLI Debrid, addressing its critical shortcomings: aggressive blacklisting, broken episode scraping, mount unawareness, duplicate accumulation, and fragile symlink paths.

## How It Works

```
TMDB/Plex Watchlist → vibeDebrid Queue → Scrape (Torrentio + Zilean)
    → Filter & Rank → Add to Real-Debrid → Wait for Zurg mount
    → Create symlinks → Plex library scan
```

vibeDebrid manages a queue of wanted media. For each item, it scrapes torrent metadata from multiple sources, filters and ranks results by quality preferences, checks Real-Debrid's cache, adds the best match, waits for the file to appear on the Zurg/rclone mount, creates organized symlinks in your library directory, and optionally triggers a Plex scan.

## Features

**Queue Management**
- 8-state queue machine: UNRELEASED, WANTED, SCRAPING, ADDING, CHECKING, SLEEPING, DORMANT, COMPLETE
- Exponential backoff retry (30min → 1hr → 2hr → 6hr → 12hr → 24hr → weekly)
- No permanent blacklist — items always get retried
- Airing season support with automatic UNRELEASED → WANTED promotion on air date

**Scraping**
- Multi-source scraping: Torrentio (Stremio addon) + Zilean (DMM hashlists)
- Episode fallback chain: episode query → season query → show query
- XEM scene numbering for anime (TMDB → broadcast season mapping)
- Season pack auto-split into individual episodes when no packs are available

**Three-Tier Filtering**
- Tier 1 — hard reject: size limits, blocked keywords/groups, resolution floor, language filter
- Tier 2 — quality scoring: resolution, codec, audio, source, seeders, cache status, language preference
- Two quality profiles (high / standard) with full customization

**Deduplication**
- Mount-first: checks Zurg mount index before any API calls
- Hash-based: checks local registry before adding to RD
- Account-aware: detects existing matching torrents in your RD account
- Duplicate manager UI for reviewing and resolving RD account duplicates

**Symlink Library**
- Standard mode: configurable naming with optional date prefix, year, resolution
- Plex mode: `Title (Year) {tmdb-XXXXX}/Season XX/Title - S01E01.ext`
- Periodic symlink health verification
- All paths are absolute host filesystem paths (safe for multi-container setups)

**Discovery & Monitoring**
- TMDB integration: trending, search, top rated, genre browsing
- Show detail pages with season/episode status
- Show monitoring: auto-checks TMDB every 6 hours for new episodes
- Airing season tracking with per-episode air dates

**Plex Integration**
- OAuth authentication
- Automatic library scan after symlink creation
- Configurable movie/show section IDs

**Web UI**
- Dashboard with queue stats and system health
- Queue management with filtering, bulk actions, detail panels
- Manual search with progressive RD cache checking
- Settings page for all configuration
- Live updates via Server-Sent Events
- Responsive design (mobile + desktop)

**Plex Watchlist Sync**
- Automatic watchlist polling (configurable interval, min 15min)
- Movies added as WANTED, shows get S1 pack + monitoring
- Mount index lookup before creating items (catches content already in RD)

**Tools**
- Library migration: import existing libraries from other tools with preview → execute flow
- TMDB ID backfill: resolve tmdb_id for all items with only imdb_id
- RD Bridge: link Real-Debrid account torrents to migrated items by matching symlink paths to RD filenames
- Smart Cleanup: liveness-aware duplicate removal — checks actual filesystem state (LIVE/BRIDGED/DEAD) before deciding what to keep, with RD torrent cleanup
- RD Account Cleanup: scan entire RD account, categorize torrents as Protected/Dead/Stale/Orphaned/Duplicate, selective removal with safety checks

## Prerequisites

- Python 3.12+
- [Zurg](https://github.com/debridmediamanager/zurg) + rclone (FUSE mount for Real-Debrid)
- A [Real-Debrid](https://real-debrid.com/) account with API key
- A [TMDB](https://www.themoviedb.org/settings/api) API key
- Optional: [Zilean](https://github.com/iPromKnight/zilean) for additional scraping
- Optional: Plex Media Server

## Setup

```bash
git clone https://github.com/YOUR_USERNAME/vibeDebrid.git
cd vibeDebrid
python3 -m venv .venv
.venv/bin/pip install -e .
```

### Configuration

Copy the example config and fill in your details:

```bash
cp config.example.json config.json
```

**Minimum required settings** in `config.json`:

```json
{
    "real_debrid": {
        "api_key": "YOUR_RD_API_KEY"
    },
    "tmdb": {
        "api_key": "YOUR_TMDB_API_KEY"
    },
    "paths": {
        "zurg_mount": "/path/to/zurg/mount",
        "library_movies": "/path/to/library/movies",
        "library_shows": "/path/to/library/shows"
    }
}
```

Everything else has sensible defaults. All settings can also be configured via the web UI at Settings, or via environment variables with the `VIBE_` prefix:

```bash
export VIBE_REAL_DEBRID__API_KEY=your_key
export VIBE_TMDB__API_KEY=your_key
export VIBE_PATHS__ZURG_MOUNT=/mnt/zurg
```

### Run

```bash
.venv/bin/python src/main.py
```

The web UI is available at `http://localhost:5100`.

## Configuration Reference

All settings are configurable via `config.json`, the web UI, or environment variables.

| Section | Key Settings | Default |
|---------|-------------|---------|
| `real_debrid` | `api_key`, `prefer_cached`, `allow_uncached` | cached only |
| `scrapers.torrentio` | `enabled`, `base_url`, `opts`, `timeout_seconds` | enabled, 10s timeout |
| `scrapers.zilean` | `enabled`, `base_url`, `timeout_seconds` | enabled, 10s timeout |
| `paths` | `zurg_mount`, `library_movies`, `library_shows` | must configure |
| `quality` | `default_profile`, profiles with resolution/codec/audio/source prefs | "high" profile |
| `filters` | `blocked_keywords`, `preferred_languages`, `allow_multi_audio` | cam/ts/telesync blocked |
| `retry` | `schedule_minutes`, `max_active_retries`, `dormant_recheck_days` | 7 retries, then weekly |
| `mount_scanner` | `scan_interval_minutes`, `scan_on_startup` | 15min, scan on start |
| `plex` | `enabled`, `url`, `token`, `section_ids`, `scan_after_symlink` | disabled |
| `symlink_naming` | `date_prefix`, `release_year`, `resolution`, `plex_naming` | date prefix on |
| `xem` | `enabled`, `cache_hours` | enabled, 24h cache |

### Quality Profiles

Two built-in profiles:

**high** — 2160p > 1080p > 720p, x265/HEVC preferred, Atmos/TrueHD/DTS-HD audio, BluRay > WEB-DL, 200MB–80GB

**standard** — 1080p > 720p, x265 > x264, DTS/AAC audio, WEB-DL > BluRay, 100MB–20GB

### Language Filtering

Set `filters.preferred_languages` to an ordered list (e.g., `["English", "Japanese"]`). Results with detected languages not in the list are rejected. Untagged results are assumed English. Supports Cyrillic detection and abbreviated language tokens.

Available: English, French, German, Spanish, Portuguese, Italian, Dutch, Russian, Japanese, Korean, Chinese.

## Architecture

```
src/
  main.py              FastAPI app, startup hooks, scheduler
  config.py            Pydantic Settings (env + config.json)
  database.py          SQLAlchemy async engine (SQLite + WAL)
  api/routes/          FastAPI routers (dashboard, queue, search, settings, discover, show, tools, duplicates, sse)
  core/                Business logic (queue_manager, scrape_pipeline, filter_engine, dedup, symlink_manager, mount_scanner, show_manager, xem_mapper)
  services/            External API wrappers (real_debrid, torrentio, zilean, plex, tmdb, xem)
  models/              SQLAlchemy ORM models
  templates/           Jinja2 + htmx templates
```

- **Async-first**: all I/O uses async/await (aiosqlite, httpx, asyncio.to_thread for filesystem)
- **SQLite with WAL**: single-file database, concurrent reads, no external DB dependency
- **Stateless services**: fresh httpx client per call, config changes take effect without restart
- **APScheduler**: periodic mount scanning, queue processing, symlink verification, show monitoring

## Infrastructure Context

vibeDebrid sits alongside your existing Zurg + rclone + Plex stack:

```
Real-Debrid (cloud)
    ↓
Zurg (WebDAV bridge)
    ↓
rclone (FUSE mount)  ←── vibeDebrid reads mount, creates symlinks
    ↓
Plex/Jellyfin        ←── reads symlinks
```

<!-- ## Docker

Docker deployment instructions coming soon. -->


## Development

```bash
# Install with dev dependencies
.venv/bin/pip install -e ".[dev]"

# Run tests
.venv/bin/python -m pytest tests/ -q

# Lint
.venv/bin/ruff check src/
```

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

Apache 2.0 — see [LICENSE](LICENSE).
