# vibeDebrid — Product Specification

## 1. Overview

vibeDebrid is a replacement for CLI Debrid, purpose-built to automate media acquisition through Real-Debrid with intelligent queue management, deduplication, and symlink creation for Plex integration.

This project exists because CLI Debrid has critical shortcomings discovered through production use:

- Torrentio scraping fails for running/airing TV seasons (episode-level queries return 0 results)
- Items are aggressively blacklisted instead of gracefully retried
- No awareness of files already present in the Zurg/rclone mount
- Duplicate torrents accumulate in Real-Debrid (e.g., multiple copies of Jujutsu Kaisen)
- Season pack vs individual episode selection logic is fragile
- Symlink paths break when container paths don't match host paths

---

## 2. System Context

### Current Infrastructure (Must Coexist With)

```
Real-Debrid (cloud) → Zurg (WebDAV, port 9090) → rclone (FUSE mount) → Plex (host)
```

### Host Environment

- Server: Linux (Ubuntu), user `hakan`, UID/PGID 1000
- Project root: `/home/hakan/Projects/homeserver/`
- Zurg rclone mount: `/home/hakan/Projects/homeserver/Zurg/mnt/__all__`
- Symlink target directory: configurable, e.g., `/home/hakan/Projects/homeserver/vibeDebrid/library/`
- Plex runs on host (not in container) — symlink paths MUST be absolute host paths
- Timezone: Europe/Berlin
- Deployment: Docker container via docker-compose, alongside existing Zurg/rclone stack

### External Services

- **Real-Debrid API** — torrent management, cache checking, file retrieval
- **Torrentio** — Stremio addon, torrent scraping by IMDB ID
- **Zilean** — local torrent metadata search (DMM hashlists), runs at `http://localhost:8182`
- **Trakt** — watchlist and collection tracking
- **Plex** — media server, library scanning, watchlist source
- **TMDB** — metadata, release dates, episode air dates

---

## 3. Core Features

### 3.1 Queue State Machine

Items flow through the following states:

```
UNRELEASED ──(air date reached)──→ WANTED
WANTED ──(scraping triggered)──→ SCRAPING
SCRAPING ──(results found)──→ ADDING
SCRAPING ──(no results)──→ SLEEPING
ADDING ──(added to RD)──→ CHECKING
CHECKING ──(file found in mount)──→ COMPLETE
CHECKING ──(file not found)──→ SLEEPING
SLEEPING ──(retry timer elapsed)──→ SCRAPING
SLEEPING ──(max retries exceeded)──→ DORMANT
DORMANT ──(weekly recheck or manual trigger)──→ SCRAPING
COMPLETE ──(symlink created + verified)──→ DONE

Manual override: Any state can be manually moved to any other state via UI.
```

**Critical difference from CLI Debrid:** There is NO blacklist state. Items that cannot be found transition to DORMANT, which is a low-frequency retry state, not a dead end. Users can always manually retry or remove items.

### 3.2 Three-Tier Filtering System

#### Tier 1 — Hard Reject (instant, never retry)
Applied before any RD interaction. Configurable rules:
- File size below minimum threshold (likely fake/spam)
- File size above maximum threshold (wrong content)
- Known spam release groups (configurable list)
- Wrong media type (e.g., audiobook when looking for video)
- CAM/TS/screener quality (unless user explicitly allows)
- Wrong language (unless multi-audio)

#### Tier 2 — Quality Preferences (score and rank results)
Applied when choosing between multiple scraping results:
- Resolution preference order (e.g., 2160p > 1080p > 720p)
- Codec preference (x265/HEVC > x264, AV1 as option)
- Audio preference (Atmos > TrueHD > DTS-HD > DTS > AAC)
- Source preference (BluRay > WEB-DL > WEBRip > HDTV)
- Release group preference/blocklist
- Prefer season packs over individual episodes when all episodes are available
- Prefer cached results in Real-Debrid over uncached

#### Tier 3 — Retry Strategy (exponential backoff, not blacklist)
When no suitable results are found:
- Retry schedule: 30min → 1hr → 2hr → 6hr → 12hr → 24hr → daily for 7 days
- After 7 days: transition to DORMANT (weekly recheck)
- DORMANT items are rechecked every 7 days indefinitely or until manually removed
- Unreleased content: park in UNRELEASED with air date trigger, do NOT consume retry cycles

### 3.3 Deduplication

#### Pre-Add Dedup (before sending to Real-Debrid)
1. Query RD API for existing torrents in account
2. Compare using: IMDB ID + Season + Episode + Resolution
3. If matching file already exists in RD → skip add, proceed to CHECKING
4. Maintain a local hash registry of all torrents added through vibeDebrid

#### Zurg Mount Awareness (scan what already exists)
1. Periodically scan the rclone mount directory to build a local file index
2. Parse filenames to extract: title, year, season, episode, resolution, codec
3. Before scraping any item, check this index first
4. If file exists in mount → skip scraping entirely, proceed to symlink creation
5. Index refresh: on startup + every 15 minutes + on-demand via API

#### RD Account Dedup (cleanup existing duplicates)
- Provide a UI view showing detected duplicates in RD account
- Allow user to select which copy to keep
- Do NOT auto-delete without explicit user confirmation

### 3.4 Manual Search & Add

The web UI must support:
1. **Search page**: query all configured scrapers (Torrentio, Zilean) with a text query
2. **Results display**: show title, size, resolution, seeders, cached status in RD
3. **One-click add**: add selected torrent to RD and enter the normal queue pipeline
4. **Magnet/hash add**: paste a magnet link or info hash directly
5. **All manually added items** enter the standard CHECKING → COMPLETE → DONE pipeline so they get tracked and symlinked like automated items

### 3.5 Scraping Pipeline

#### Scraper Priority & Fallback
1. Check Zurg mount index (instant, no API call)
2. Check RD account torrents (fast API call)
3. Query Zilean (local, fast)
4. Query Torrentio (remote, slower)
5. Future: pluggable scraper interface for adding new sources

#### Episode Scraping Fix (CLI Debrid Bug)
The known issue: CLI Debrid's Torrentio queries for individual episodes of running seasons return 0 results, causing premature blacklisting.

Root cause: Torrentio's API works differently for episode-level vs season-level queries. The scraper must:
1. First attempt: query for the specific episode (IMDB ID + S##E##)
2. If 0 results: query for the full season (IMDB ID + S##)
3. If season pack found: add the pack, then check for individual episodes within it
4. If still 0 results: query with show IMDB ID only (no season/episode filter)
5. Parse results to find matching episodes within returned torrents
6. Never assume "0 results = content doesn't exist" — transition to SLEEPING, not blacklist

#### Torrentio Configuration
- Endpoint: configurable (default Stremio addon URL)
- Remove `limit=1` parameter (was throttling results in CLI Debrid)
- Remove `cachedonly` for broader results, use RD cache check separately
- Support multiple Torrentio instances/configurations

### 3.6 Symlink Management

- Create symlinks from organized library structure to Zurg rclone mount
- Library structure:
  ```
  library/
    movies/
      Movie Name (2024)/
        Movie.Name.2024.2160p.WEB-DL.mkv → /path/to/zurg/mount/__all__/actual_file.mkv
    shows/
      Show Name (2024)/
        Season 01/
          Show.Name.S01E01.1080p.mkv → /path/to/zurg/mount/__all__/actual_file.mkv
  ```
- All paths must be absolute and valid on the HOST filesystem
- Validate symlink targets exist before creating
- Periodic symlink health check: verify targets still exist, log broken links
- Support configurable library path

### 3.7 Watchlist Integration

#### Trakt
- OAuth authentication
- Monitor: watchlist, custom lists, collection
- Configurable polling interval (default: 15 minutes)
- Map Trakt items to IMDB IDs for scraping

#### Plex
- Monitor Plex watchlist
- Detect items added via Plex Discover
- Trigger library scan after symlinks are created
- Configurable section IDs for movies/shows

### 3.8 Upgrade System

- After initial acquisition, monitor for higher-quality versions for a configurable window (default: 24 hours)
- Quality upgrade path: 720p → 1080p → 2160p (configurable)
- When upgrade found: add new version, update symlink, optionally remove old from RD
- Upgrade checking interval: configurable (default: 1 hour)

---

## 4. Web UI

### Dashboard
- Queue overview with counts per state
- Recent activity log
- System health indicators (RD API status, Zurg mount status, scraper availability)
- Quick stats: items added today, storage used in RD, active downloads

### Queue Views
- Filterable/sortable table for each queue state
- Inline actions: retry, skip, remove, change state, view scraping history
- Bulk actions: retry all sleeping, clear dormant, etc.

### Manual Search
- Search bar with scraper selection
- Results table with quality info and RD cache status
- One-click add to queue
- Magnet/hash direct input

### Settings
- All configuration editable via UI
- Real-Debrid API key management with token refresh
- Scraper configuration (Torrentio URL/options, Zilean endpoint)
- Quality preferences editor
- Filter rules editor
- Path configuration (mount path, library path)
- Notification settings (future: Discord, Telegram)

### Duplicate Manager
- View detected duplicates across RD account
- Compare versions (resolution, size, codec)
- Select keeper, remove others (with confirmation)

---

## 5. Technical Requirements

### Stack
- **Language**: Python 3.12+
- **Web Framework**: FastAPI (async)
- **Database**: SQLite (via SQLAlchemy async + aiosqlite)
- **HTTP Client**: httpx (async)
- **Task Scheduling**: APScheduler
- **Frontend**: FastAPI serving Jinja2 templates + htmx for interactivity (keep it simple, no heavy JS framework)
- **Filename Parsing**: PTN (parse-torrent-name) or custom parser
- **Containerization**: Docker with docker-compose

### Project Structure
```
vibeDebrid/
├── CLAUDE.md                    # Claude Code project instructions
├── SPEC.md                      # This document
├── .claude/
│   ├── agents/                  # Subagent definitions
│   └── settings.json            # Claude Code project settings
├── src/
│   ├── __init__.py
│   ├── main.py                  # FastAPI app entrypoint
│   ├── config.py                # Settings management (env + JSON config)
│   ├── database.py              # SQLAlchemy engine, session, migrations
│   ├── api/
│   │   ├── __init__.py
│   │   ├── routes/
│   │   │   ├── dashboard.py     # Dashboard endpoints
│   │   │   ├── queue.py         # Queue management endpoints
│   │   │   ├── search.py        # Manual search endpoints
│   │   │   ├── settings.py      # Settings endpoints
│   │   │   └── duplicates.py    # Duplicate manager endpoints
│   │   └── deps.py              # Shared dependencies
│   ├── core/
│   │   ├── __init__.py
│   │   ├── queue_manager.py     # State machine, transitions, scheduling
│   │   ├── scrape_pipeline.py   # Orchestrates scraper calls with priority/fallback
│   │   ├── filter_engine.py     # Three-tier filtering
│   │   ├── dedup.py             # Deduplication logic
│   │   ├── symlink_manager.py   # Symlink creation and health checking
│   │   ├── mount_scanner.py     # Zurg mount file indexing
│   │   └── upgrade_manager.py   # Quality upgrade monitoring
│   ├── services/
│   │   ├── __init__.py
│   │   ├── real_debrid.py       # RD API wrapper
│   │   ├── torrentio.py         # Torrentio scraper
│   │   ├── zilean.py            # Zilean scraper
│   │   ├── trakt.py             # Trakt API integration
│   │   ├── plex.py              # Plex API integration
│   │   └── tmdb.py              # TMDB metadata and release dates
│   ├── models/
│   │   ├── __init__.py
│   │   ├── media_item.py        # Core media item model with state
│   │   ├── scrape_result.py     # Scraping result model
│   │   ├── torrent.py           # Torrent/hash tracking model
│   │   └── config_model.py      # Configuration model
│   └── templates/               # Jinja2 HTML templates
│       ├── base.html
│       ├── dashboard.html
│       ├── queue.html
│       ├── search.html
│       ├── settings.html
│       └── duplicates.html
├── tests/
│   ├── conftest.py
│   ├── test_queue_manager.py
│   ├── test_filter_engine.py
│   ├── test_dedup.py
│   ├── test_scrape_pipeline.py
│   ├── test_real_debrid.py
│   ├── test_torrentio.py
│   └── test_symlink_manager.py
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── pyproject.toml
```

### Database Schema (Core Tables)

```sql
-- Core media items with queue state
media_items (
    id INTEGER PRIMARY KEY,
    imdb_id TEXT NOT NULL,
    tmdb_id TEXT,
    title TEXT NOT NULL,
    year INTEGER,
    media_type TEXT NOT NULL,  -- 'movie' | 'show'
    season INTEGER,            -- NULL for movies
    episode INTEGER,           -- NULL for movies and season packs
    state TEXT NOT NULL,        -- queue state
    quality_profile TEXT,       -- reference to quality preference
    requested_resolution TEXT,  -- '2160p', '1080p', etc.
    added_at TIMESTAMP,
    state_changed_at TIMESTAMP,
    retry_count INTEGER DEFAULT 0,
    next_retry_at TIMESTAMP,
    source TEXT,               -- 'trakt', 'plex', 'manual'
    metadata JSON,             -- cached TMDB/Trakt metadata
    air_date DATE,             -- for unreleased content
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)

-- Tracking what's been added to RD through vibeDebrid
rd_torrents (
    id INTEGER PRIMARY KEY,
    rd_id TEXT,                -- Real-Debrid torrent ID
    info_hash TEXT UNIQUE,
    magnet_uri TEXT,
    media_item_id INTEGER REFERENCES media_items(id),
    filename TEXT,
    filesize INTEGER,
    resolution TEXT,
    cached BOOLEAN,
    added_at TIMESTAMP,
    status TEXT                -- 'active', 'removed', 'replaced'
)

-- Zurg mount file index
mount_index (
    id INTEGER PRIMARY KEY,
    filepath TEXT UNIQUE,      -- full path in mount
    filename TEXT,
    parsed_title TEXT,
    parsed_year INTEGER,
    parsed_season INTEGER,
    parsed_episode INTEGER,
    parsed_resolution TEXT,
    parsed_codec TEXT,
    filesize INTEGER,
    last_seen_at TIMESTAMP
)

-- Scraping history for debugging
scrape_log (
    id INTEGER PRIMARY KEY,
    media_item_id INTEGER REFERENCES media_items(id),
    scraper TEXT,              -- 'torrentio', 'zilean', 'mount_scan', 'rd_check'
    query_params JSON,
    results_count INTEGER,
    results_summary JSON,      -- top results with quality info
    selected_result JSON,      -- which result was chosen and why
    duration_ms INTEGER,
    scraped_at TIMESTAMP
)

-- Symlinks created
symlinks (
    id INTEGER PRIMARY KEY,
    media_item_id INTEGER REFERENCES media_items(id),
    source_path TEXT,          -- path in zurg mount
    target_path TEXT,          -- organized library path
    valid BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP,
    last_checked_at TIMESTAMP
)
```

### Configuration File Structure

```json
{
    "real_debrid": {
        "api_key": "",
        "api_url": "https://api.real-debrid.com/rest/1.0",
        "prefer_cached": true,
        "allow_uncached": false,
        "max_uncached_size_gb": 50
    },
    "scrapers": {
        "torrentio": {
            "enabled": true,
            "base_url": "https://torrentio.strem.fun",
            "opts": "",
            "timeout_seconds": 30,
            "max_results": 100
        },
        "zilean": {
            "enabled": true,
            "base_url": "http://localhost:8182",
            "timeout_seconds": 10
        }
    },
    "paths": {
        "zurg_mount": "/home/hakan/Projects/homeserver/Zurg/mnt/__all__",
        "library_movies": "/home/hakan/Projects/homeserver/vibeDebrid/library/movies",
        "library_shows": "/home/hakan/Projects/homeserver/vibeDebrid/library/shows"
    },
    "quality": {
        "default_profile": "high",
        "profiles": {
            "high": {
                "resolution_order": ["2160p", "1080p", "720p"],
                "min_resolution": "720p",
                "preferred_codec": ["x265", "hevc", "av1", "x264"],
                "preferred_audio": ["atmos", "truehd", "dts-hd", "dts", "aac"],
                "preferred_source": ["bluray", "web-dl", "webrip", "hdtv"],
                "min_size_mb": 200,
                "max_size_gb": 80
            },
            "standard": {
                "resolution_order": ["1080p", "720p"],
                "min_resolution": "720p",
                "preferred_codec": ["x265", "x264"],
                "preferred_audio": ["dts", "aac"],
                "preferred_source": ["web-dl", "webrip", "bluray"],
                "min_size_mb": 100,
                "max_size_gb": 20
            }
        }
    },
    "filters": {
        "blocked_release_groups": [],
        "blocked_keywords": ["cam", "ts", "telesync", "telecine", "hdcam"],
        "required_language": null,
        "allow_multi_audio": true
    },
    "retry": {
        "schedule_minutes": [30, 60, 120, 360, 720, 1440],
        "dormant_recheck_days": 7,
        "max_active_retries": 7,
        "unreleased_check_hours": 6
    },
    "upgrade": {
        "enabled": true,
        "window_hours": 24,
        "check_interval_minutes": 60
    },
    "mount_scanner": {
        "scan_interval_minutes": 15,
        "scan_on_startup": true
    },
    "trakt": {
        "enabled": false,
        "client_id": "",
        "client_secret": "",
        "access_token": "",
        "refresh_token": "",
        "poll_interval_minutes": 15,
        "lists": ["watchlist"]
    },
    "plex": {
        "enabled": false,
        "url": "http://localhost:32400",
        "token": "",
        "movie_section_ids": [],
        "show_section_ids": [],
        "scan_after_symlink": true
    },
    "server": {
        "host": "0.0.0.0",
        "port": 5100
    }
}
```

---

## 6. API Endpoints

### Dashboard
- `GET /` — Dashboard page
- `GET /api/stats` — Queue counts, system health, recent activity

### Queue Management
- `GET /api/queue` — List items with filtering (state, media_type, title search)
- `GET /api/queue/{id}` — Item detail with full history
- `POST /api/queue/{id}/retry` — Force retry a sleeping/dormant item
- `POST /api/queue/{id}/state` — Manually change item state
- `DELETE /api/queue/{id}` — Remove item from all queues
- `POST /api/queue/bulk/retry` — Bulk retry
- `POST /api/queue/bulk/remove` — Bulk remove

### Manual Search
- `POST /api/search` — Search scrapers (body: `{query, imdb_id?, type?, season?, episode?}`)
- `POST /api/add` — Add torrent to RD and queue (body: `{magnet_or_hash, media_item_id?}`)

### Settings
- `GET /api/settings` — Current configuration
- `PUT /api/settings` — Update configuration
- `POST /api/settings/test/realdebrid` — Test RD API connection
- `POST /api/settings/test/torrentio` — Test Torrentio endpoint
- `POST /api/settings/test/zilean` — Test Zilean endpoint

### Duplicates
- `GET /api/duplicates` — Detected duplicates
- `POST /api/duplicates/resolve` — Keep selected, remove others

### Mount Scanner
- `POST /api/mount/scan` — Trigger immediate mount scan
- `GET /api/mount/index` — View current mount index

---

## 7. Known Pitfalls from CLI Debrid (Must Solve)

### P1: Torrentio Episode Scraping Failure
**Problem**: Querying Torrentio for `S02E06` of a running show returns 0 results even though the torrent exists and works in Stremio.
**Solution**: Multi-level query fallback (episode → season → show → broad). Never assume 0 results means content doesn't exist.

### P2: Aggressive Blacklisting
**Problem**: Items hit a fixed wake count and get permanently blacklisted, including old movies that just need a patient retry.
**Solution**: Exponential backoff → DORMANT state with periodic recheck. No permanent blacklist.

### P3: No Mount Awareness
**Problem**: CLI Debrid doesn't know what files already exist in the Zurg mount, leading to redundant scraping and duplicate downloads.
**Solution**: Mount scanner that indexes files and is checked before any scraping.

### P4: Duplicate Torrents in RD
**Problem**: Same content (e.g., Jujutsu Kaisen) added multiple times to RD account in different qualities or releases.
**Solution**: Pre-add dedup check against RD account + local hash registry.

### P5: Broken Symlinks from Path Mismatch
**Problem**: Symlinks created with container-internal paths that don't exist on the host where Plex runs.
**Solution**: All paths configurable, all symlinks use absolute host paths, symlink health checker runs periodically.

### P6: `limit=1` Throttling Torrentio
**Problem**: Torrentio `opts` had `limit=1`, severely restricting results.
**Solution**: Default to no limit or high limit (100+), configurable per scraper.

### P7: Zilean Missing Recent Content
**Problem**: Zilean's DMM database doesn't index new releases quickly, causing failures for recently aired episodes.
**Solution**: Zilean is just one scraper in the pipeline, not the only one. Torrentio fallback handles fresh content. Mount scanner catches manually added torrents.

---

## 8. Non-Functional Requirements

- **Startup time**: < 5 seconds to web UI, mount scan runs async in background
- **Memory**: < 200MB base, scales with queue size
- **Logging**: Structured logging with configurable levels, separate log files per component
- **Error handling**: Every external API call has timeout, retry, and graceful degradation
- **Data safety**: SQLite WAL mode for concurrent reads, transactions for state changes
- **Observability**: Scrape history logged for every attempt (visible in UI for debugging)
- **Migration path**: Tool to import existing CLI Debrid queue/config (nice-to-have, not MVP)

---

## 9. Mount Failure Handling

The Zurg/rclone mount can go down (rclone crash, RD API outage, Docker restart). vibeDebrid must handle this gracefully:

1. **Health check**: Verify mount is available by checking for a known sentinel path (e.g., the `__all__` directory exists and is listable)
2. **Mount down behavior**:
   - PAUSE all CHECKING state transitions (don't transition to SLEEPING because the file "wasn't found")
   - PAUSE mount scanner (don't wipe the index because the directory appears empty)
   - Surface a critical health warning on the dashboard
   - Continue SCRAPING and ADDING (these don't need the mount)
3. **Mount recovery**: When mount comes back, resume paused operations automatically. Re-scan mount index.
4. **Check interval**: Mount health check every 30 seconds

---

## 10. Notification Hooks (Future-Ready)

The architecture must support an event system from day one, even if notification delivery is implemented later:

1. **Event types**: `item_added`, `item_complete`, `item_failed`, `upgrade_found`, `mount_down`, `mount_recovered`, `rd_auth_failed`
2. **Implementation**: Simple callback pattern — core modules emit events, a notification manager routes them
3. **Phase 1 (MVP)**: Events logged only
4. **Phase 2**: Discord webhook, Telegram bot, Apprise integration

---

## 11. Database Backup

- SQLite is a single file — easy to back up, catastrophic to lose
- Scheduled backup: copy `.db` file to `backups/` directory every 24 hours (configurable)
- Retain last 7 backups (configurable)
- Backup on startup before any migrations run
- Backup path configurable in `config.json`
