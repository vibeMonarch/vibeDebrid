# vibeDebrid — Project Status

## Last Updated: 2026-03-02

## Completed

### Phase 1: Foundation ✅
- pyproject.toml, requirements.txt, project structure
- src/config.py — Pydantic settings from config.json (includes SchedulerConfig)
- src/database.py — async SQLAlchemy + SQLite WAL
- All SQLAlchemy models in src/models/
- src/main.py — FastAPI app with lifespan, stub routes, Jinja2
- Stub routers for all 5 route groups (dashboard, queue, search, settings, duplicates)

### Phase 2: Services ✅
- src/services/real_debrid.py — 7 methods, 21 tests
- src/services/torrentio.py — fallback chain (episode→season), 42 tests
  - Fixed: wrong emoji in seeders regex (U+1F465 → U+1F464)
  - Fixed: infoHash parsing with fallbacks
- src/services/zilean.py — v2 API schema only, 42+ tests
  - Fixed: was coded against wrong (legacy) field names
  - Cleaned: removed all v1/legacy fallback code
- Live smoke tests passed: RD API ✓, Torrentio 99 results for Fallout S02E06 ✓, Zilean ✓

### Phase 3: Core Logic ✅
- src/core/queue_manager.py — state machine, retry logic, 84 tests
- src/core/filter_engine.py — 3-tier filtering, scoring, 102 tests
- src/core/dedup.py — RD dedup + local hash registry, 53 tests
- src/core/mount_scanner.py — Zurg mount indexing, 59 tests
- src/core/scrape_pipeline.py — full orchestrator, 42 tests
- src/core/symlink_manager.py — symlink creation + health check, 77 tests

### Phase 4: Code Review ✅
- 3 critical issues fixed (stuck SCRAPING state, UTC date bug, Zilean batch parsing)
- 10 high issues fixed (RD key reload, async correctness, dead fallback URL, etc.)

### Phase 5: API Routes + Scheduler ✅
- All 5 route groups wired to real core modules (dashboard, queue, search, settings, duplicates)
- Scheduler wired: mount scanner, queue processor, symlink verifier
- All scheduler intervals from config (no hardcoded values)
- Queue processor: WANTED→SCRAPING→pipeline, failed items recover to SLEEPING
- max_instances=1 on queue processor to prevent overlapping runs
- POST /api/search: live scraping with RD cache check + filtering
- POST /api/add: create item + add magnet + register dedup
- Settings: masked API keys, deep-merge update, connectivity tests
- Duplicates: RD account scan + resolve (delete unwanted torrents)
- 76 API route tests added

### Phase 6: Web UI
- Frontend-dev agent builds Jinja2 + htmx + Tailwind templates
- Dashboard, queue management, manual search, settings, duplicates
- Dark mode default, mobile-friendly

**Total: 850 tests, all passing**

### Symlink Naming Convention ✅
- SymlinkNamingConfig: date_prefix, release_year, resolution toggles
- Movie dirs: optional YYYYMMDDHHMM prefix, year, resolution
- Show dirs: timestamp from first episode (persists via dir reuse), season dir unchanged
- Episode files: per-file timestamp prefix for shows
- Settings UI: collapsible card with 3 toggles + live preview
- Exact dir matching (strips timestamp/resolution before compare, prevents false positives)
- build_show_dir wrapped in asyncio.to_thread (blocking I/O fix)
- 22 new tests, 22 existing tests hardened

### Season Pack Support ✅
- is_season_pack column on MediaItem with DB migration
- Search UI: Season/Episode columns, "Full" badge for season packs
- Season pack add sets episode=None, symlinks ALL episodes (not just searched one)
- Per-episode dedup: groups mount matches by episode, picks best resolution
- Partial-failure tolerance in symlink loop (skips broken, retries if all fail)
- Scrape pipeline allows season packs through Torrentio (episode=1 anchor)
- 25 new tests

## Next Steps (In Order)
### Step 0: Bugfixes and improvements
- ~~Symlink Folder Naming Convention~~ ✅ (completed above)

- ~~Seeders display fix~~ ✅ Removed seeders column from search results UI
  - Investigated: Torrentio parses seeders correctly from emoji metadata (👤 count)
  - Zilean has no seeders data at all (confirmed via API schema, OpenAPI spec, and torznab dummy values)
  - Fix: removed seeders display from desktop grid and mobile stats row
  - Seeders still used internally by filter_engine for scoring (up to 10 pts)

- ~~Manual RD cache check button~~ ✅ Added "Check RD" button per search result
  - Configurable auto-check limit via Settings → Search → "Auto cache check limit"
  - Results beyond limit show clickable "Check RD" button instead of "N/A"
  - Clicking triggers `/api/check-cached`, shows spinner → cached/uncached badge
  - Fixed: settings save now reloads in-memory singleton (all settings take effect immediately)

### Discovery Feature ✅
- src/services/tmdb.py — TMDB API client (trending, top rated, genres, discover, search, external IDs)
- src/api/routes/discover.py — 7 endpoints (trending, top_rated, genres, by-genre, search, add, resolve)
- src/templates/discover.html — browsable discovery page with Movies/TV/Search tabs
- Settings UI: TMDB section with API key config + connection test
- Config: TmdbConfig with enabled, api_key, base_url, image_base_url, timeout
- DB migration: tmdb_id index for batch lookups
- Nav: Discover link in sidebar
- Movies/TV tabs: trending row, top rated row, genre chip browsing (3 parallel API calls on load)
- Genre browsing: clickable chips, poster grid with Load More pagination
- "Add to Library" flow: resolve TMDB→IMDB, navigate to search page for manual torrent selection, redirect back to discover after add
- GET /api/discover/resolve/{media_type}/{tmdb_id} — lightweight TMDB→IMDB ID resolution
- Search page auto-fills and auto-submits when opened with URL params from discover
- 15 new tests (9 tmdb service + 6 API route)

### SSE: Live Queue & Dashboard Updates ✅
- src/core/event_bus.py — QueueEvent dataclass, EventBus (subscribe/publish/shutdown, maxsize=64 per client)
- src/api/routes/sse.py — GET /api/events SSE endpoint (30s heartbeat, shutdown sentinel)
- queue_manager.py — publish events on transition() and force_transition()
- base.html — VD_SSE global manager (lazy EventSource, auto-reconnect)
- queue.html — live state badge updates, filter-aware removal, 2s debounced reload
- dashboard.html — SSE-driven stats refresh (1s debounce), fallback poll extended to 60s
- Fixed: naive vs aware datetime comparison bug in CHECKING timeout
- 21 new tests (19 event_bus + 2 queue_manager integration)

### Discover Page State Preservation ✅ (Step 0.4)
- sessionStorage-based state preservation for discover page round-trips
- Saves full TMDB response data (trending, top rated, genre chips, genre results) — zero API calls on restore
- Both tab caches saved/restored — switching tabs after return needs no re-fetch
- Genre page counter + total pages restored — Load More continues from correct page
- Scroll position restored via setTimeout(100ms) after layout settles
- One-shot: sessionStorage cleared immediately after restore, refresh starts fresh
- try/catch on sessionStorage.setItem for QuotaExceededError safety
- Input validation: tab value whitelist, genre ID regex, JSON.parse error handling

### Step 0.5: Fast CHECKING Resolution + Mount Scan Performance ✅
- Mount scanner: `_scandir_walk()` replaces `os.walk` + `os.path.getsize` with `os.scandir` + `DirEntry.stat()` (fewer FUSE syscalls)
- Mount scanner: `_upsert_records()` batch helper — `WHERE filepath IN` batches of 500, not 1 SELECT per file
- Mount scanner: `scan_directory(session, dir_name)` — targeted single-dir scan (additive, no stale deletion)
- Both scandir walks have FUSE hang protection (120s full scan, 30s targeted scan timeouts)
- ADDING stage: captures `rd_info["filename"]` → `torrent.filename` (actual Zurg directory name)
- CHECKING stage: targeted scan fallback — if `lookup()` empty, `scan_directory(torrent.filename)`, re-lookup
- Turns ~15-20 min wait for cached torrents into ~1-2 seconds
- 19 new tests (scan_directory, batch upsert, scandir_walk)

### Step 1: Trakt + Plex Integration
- src/services/trakt.py — OAuth, watchlist polling
- src/services/plex.py — watchlist, library scan trigger
- Wire into scheduler with config intervals


### Step 2: Upgrade Manager
- src/core/upgrade_manager.py — monitor for higher quality versions within window

### Step 3: Docker
- Dockerfile + docker-compose.yml alongside existing Zurg/rclone stack

## Key Files to Read
- SPEC.md — full product requirements
- CLAUDE.md — coding conventions, architecture
- QUICKSTART.md — agent usage guide
- config.example.json — all configuration options
