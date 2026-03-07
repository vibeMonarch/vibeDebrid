# vibeDebrid — Project Status

## Last Updated: 2026-03-07

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

**Total: 868 tests, all passing**

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

### Queue Detail Panel + Discover SSE ✅ (Step 0.6)
- Queue detail panel: fixed API response flattening (nested `item` field wasn't spread)
- Queue detail panel: fixed field mappings (`size_bytes`→`filesize`, `cached !== undefined`→`!= null`)
- Queue detail panel: fixed scrape log field names (`query`→`query_params`, `created_at`→`scraped_at`, removed dead `scraper_name`)
- ScrapeLog for manual adds: `search.py:add_torrent()` now creates a `ScrapeLog(scraper="manual")` entry with query context and selected result
- Discover SSE: subscribe to `VD_SSE.onStateChange()` for real-time badge updates (title-based matching)
- Discover SSE: patches in-memory mediaCache + sessionStorage so badges survive re-renders and navigation
- Known limitation: title-based SSE matching can false-positive for same-name media (needs `tmdb_id` in QueueEvent to fix)

### Fuzzy Directory Match + Word Subsequence ✅ (Step 0.7)
- RD torrent filenames may contain special chars (colons, etc.) that Zurg/rclone sanitizes on the filesystem
- scan_directory fuzzy match: word subsequence instead of startswith — handles missing tokens (e.g. "Terminator Judgement Day" → "Terminator.2.Judgement.Day.1991...")
- lookup() fallback: exact match first, then SQL LIKE + Python word-subsequence verification (2+ word minimum)
- `_is_word_subsequence()` helper: all query words must appear in target words in order
- Collects all candidates, picks shortest normalized name (closest match), deterministic alphabetical tiebreaker
- Timeout-protected FUSE I/O via asyncio.to_thread + 5s timeout
- 3 new tests (subsequence fallback, order enforcement, single-word guard)

### Plex Integration ✅ (Step 1a)
- src/services/plex.py — stateless PlexClient (test_connection, get_libraries, scan_section, create_pin, check_pin)
- OAuth PIN-based auth flow: create_pin → browser popup → poll check_pin → save token
- Settings UI: Plex test button, OAuth connect flow, library section checkboxes, scan toggle
- 4 new API endpoints: POST /test/plex, POST /plex/auth/start, POST /plex/auth/check/{pin_id}, GET /plex/libraries
- Auto scan trigger: after COMPLETE transition, scans configured movie/show Plex sections
- Targeted Plex scan: passes symlink directory path to `scan_section(path=)` instead of full library rescan (reduces FUSE/RD traffic)
- Config lock: asyncio.Lock prevents race condition on concurrent config.json writes
- 12 new tests (mocked httpx transport)

### Discover Page Mobile/Desktop UX ✅ (Step 0.8)
- CSS scroll snap (`scroll-snap-type: x proximity`) with `overscroll-behavior-x: contain` on section rows
- Fade gradient scroll indicators (`::before`/`::after` pseudo-elements) with JS-driven `at-start`/`at-end` classes
- Responsive card sizing via `--card-w` CSS variable (140px mobile, 160px desktop)
- `main { overflow-x: hidden }` prevents horizontal page scroll from section rows on mobile
- Tap-to-reveal overlay for touch devices (`matchMedia('(hover: none)')` detection, `.overlay-visible` class)
- Responsive search bar (column layout on mobile) and genre chips horizontal scroll
- Section rows wrapped in `.section-row-wrap` divs for gradient positioning

### Search UX Timing Fixes ✅ (Step 0.9)
- Parallelized Torrentio + Zilean scrapers via `asyncio.gather` (previously sequential — Torrentio 522 timeouts blocked Zilean results)
- Reduced Torrentio timeout from 30s to 10s (`src/config.py`)
- Frontend: results render immediately, RD cache checks run progressively after paint
- `scrollIntoView({ behavior: 'instant', block: 'start' })` for instant result visibility on search page
- `requestAnimationFrame` paint forcing before cache check loop

### Progressive Search Results ✅ (Step 0.10)
- Backend: `scrapers` parameter on `/api/search` (validated `Literal["torrentio", "zilean"]` list)
- Frontend fires two parallel requests: Zilean renders instantly (~500ms), Torrentio merges in when ready
- Results deduped by info_hash, re-sorted by score, re-rendered with cache statuses preserved
- `_cacheStatusMap` persists RD cache check results across re-renders
- "Loading more results from Torrentio…" indicator while slow scraper is pending
- `cacheBadgeHtml` checks cache map first (correct badges after merge re-sort)
- 3 new tests (scrapers filtering: zilean-only, torrentio-only, default-runs-both)

### Search Score + Layout Fixes ✅ (Step 0.11)
- Score updates live when RD cache status resolves (+10 cached bonus reflected in number, bar, and breakdown)
- Removed broken tooltip on desktop — score breakdown now shown inline (same as mobile)
- Unified button text to "+ Add" on both desktop and mobile (was "Add to Real-Debrid" on mobile)
- Added `.score-value`, `.score-bar-fill`, `.score-breakdown` CSS hooks for dynamic score updates

### Code Review + Fixes ✅ (Step 0.12)
- Full codebase review: 4 parallel domain-scoped agents (core, services, API, frontend)
- Mount scanner: exact title matching replaces ILIKE substring (prevents "It" matching "It Follows")
- Stuck SCRAPING recovery: Stage 0 in queue processor force-transitions items stale >30min back to WANTED
- Scheduler: `max_instances=1` added to mount_scan and symlink_verifier jobs
- CDN security: htmx pinned with SRI hash, Alpine.js pinned from `3.x.x` to `3.15.8` with SRI hash
- Search IMDB auto-resolve: direct searches auto-resolve IMDB ID via TMDB, enabling Torrentio results
- Known limitation: Zilean can't find titles like "xXx" (3-char non-alphanumeric) — only Torrentio (via IMDB) works

### Discover Sticky Header + Genre Chips ✅ (Step 0.13)
- Discover header (title, description, tabs) pinned at top of viewport via CSS `position: sticky`
- Genre chips stick below the header when scrolling through genre results
- `main` made a scroll container (`height: 100dvh; overflow-y: auto`) so sticky works despite `overflow-x: hidden`
- JS measures header height on load/resize, sets `--header-h` CSS var for genre chips offset
- Mobile: left padding on header avoids hamburger button overlap
- Scroll position save/restore updated to use `main.scrollTop`

### Sticky Headers All Pages + UX Fixes ✅ (Step 0.14)
- `.page-header` class in `base.html`: sticky positioning, background, border, padding, mobile hamburger offset
- Applied to all pages: dashboard, queue, search, settings, duplicates, discover (consolidated)
- Discover: `.discover-header` now extends `.page-header` (single source of truth for sticky styles)
- Discover: `:hover` overlay wrapped in `@media (hover: hover)` — prevents spurious overlay on touch scroll
- Discover: touch scroll suppression via `_touchMoved` flag (touchstart/touchmove tracking)
- Discover: genre switch scrolls chips to top via `scrollIntoView` + `scroll-margin-top`
- Search: `scroll-margin-top: 5rem` on results section prevents scrolling behind sticky header
- Search: IMDB auto-resolve now scores TMDB results (exact=3, startsWith=2, includes=1) instead of blind `items[0]`

### Tools Page + Library Migration (Step 1.5)
- Tools page with wrench icon in sidebar navigation (after Settings)
- Library Migration tool: scan any external library, import into vibeDebrid DB
- System-agnostic: works with CLI Debrid, manual folders, or any media library (no external DB dependency)
- Filesystem scanner: walks movies/shows dirs, parses title+year from 4 naming patterns (Plex, scene, bracket, fallback)
- Extracts IMDB ID from filenames (CLI Debrid `ttXXXXXXX` pattern), resolution, season/episode info
- Handles both symlinks (creates Symlink DB record) and real files (MediaItem only)
- Two-phase UX: Preview (scan + summary + tables) → Execute (import + dedup + move + config update)
- Duplicate detection: by symlink source_path (exact) or normalized title+year+type+season+episode (fuzzy)
- Moves unique vibeDebrid items to new location, removes duplicates, updates config.json paths
- Shared `config_lock` extracted to `src/config.py` — prevents concurrent config.json corruption
- Concurrency guard: 409 Conflict if migration already running
- Session commit after execute, rollback on import errors keeps session healthy
- 85 new tests (parsing, scanning, preview, execute, API routes)
- Total: 953 tests, all passing

### Show Detail Page + Monitoring Subscriptions ✅ (Step 1.6)
- Show detail page (`/show/{tmdb_id}`): replaces search redirect for TV shows with dedicated season picker
- TMDB integration: `get_show_details()` (with `append_to_response=external_ids`), `get_season_details()`
- Season picker UI: checkboxes per season with status badges (Available/In Queue/In Library/Upcoming)
- Season selection: each season creates `MediaItem(is_season_pack=True, episode=None, state=WANTED)`
- Subscribe toggle: creates `MonitoredShow` record for automatic new episode monitoring
- Monitoring scheduler: 6-hour job checks TMDB for new seasons/episodes on subscribed shows
- Monitoring logic: new complete seasons → season pack item, current season new episodes → per-episode items
- Dedup: existing queue items checked before creating new ones (by tmdb_id + season + episode)
- `MonitoredShow` model: tmdb_id (unique, indexed), last_season, last_episode tracking for incremental checks
- Discover flow: TV shows navigate to `/show/{tmdb_id}?from=discover`, movies keep existing `/search` redirect
- Return-to-discover: auto-redirect to `/discover` after adding (with sessionStorage state restore)
- Quality profile selector + sticky bottom action bar
- `_parse_air_date()` helper guards against malformed TMDB air_date strings
- UTC consistency: `datetime.now(timezone.utc).date()` instead of `date.today()`
- 85 new tests (53 show_manager + 32 API routes)
- Total: 1038 tests, all passing

### Airing Season Support ✅ (Step 1.7)
- TMDB model: `TmdbEpisodeAirInfo`, `next_episode_to_air`/`last_episode_to_air` on `TmdbShowDetail` (no extra API call)
- `SeasonStatus.AIRING`: detected when `next_episode_to_air.season_number` matches season
- Show detail page: airing seasons show "X of Y episodes aired" with accurate count from `get_season_details()`
- Per-episode creation: airing seasons create individual episode items (WANTED for aired, UNRELEASED for future) instead of season pack
- Completed seasons still create season packs (unchanged)
- Auto-subscribe: adding an airing season auto-enables monitoring subscription
- MonitoredShow stamping: `last_season`/`last_episode` set on auto-subscribe to prevent monitor duplicate creation
- Belt-and-suspenders: `_check_single_show()` checks for existing episode items before creating season packs
- `AddSeasonsResult`: new `created_episodes` and `created_unreleased` counter fields
- Frontend: sky-blue pulsing badge for AIRING status, "Select available" includes airing seasons
- Toast: "Added X episodes + Y upcoming to queue" for airing season adds
- 23 new tests (airing detection, per-episode creation, auto-subscribe, monitoring dedup, schema)
- Total: 1074 tests, all passing

### XEM Scene Numbering ✅ (Step 1.8)
- Solves anime numbering mismatch: TMDB S01E29 ≠ torrent S02E01 (e.g., Frieren)
- `src/services/xem.py`: XEM API client (get_show_mappings, get_shows_with_mappings)
- `src/core/xem_mapper.py`: cache-first mapper with 24h TTL in SQLite
- `src/models/xem_cache.py`: cache table with UniqueConstraint on (tvdb_id, season, episode)
- TVDB ID propagation: `tvdb_id` column on MediaItem, populated from TMDB external_ids
- Scrape pipeline: XEM resolved once in `_run_pipeline`, scene numbers passed to both Torrentio + Zilean
- Fallback: if tvdb_id missing, resolves via TMDB; if XEM down/no mapping, uses original numbers
- Config: `xem.enabled`, `xem.base_url`, `xem.cache_hours`, `xem.timeout_seconds`
- 31 new tests (14 client, 11 mapper/cache, 6 pipeline integration)
- Total: 1105 tests, all passing

## Remaining / Future Work

### Trakt Integration (Step 1b)
- src/services/trakt.py — OAuth, watchlist polling
- Wire into scheduler with config intervals

### Upgrade Manager (Step 2)
- src/core/upgrade_manager.py — monitor for higher quality versions within window

### Docker (Step 3)
- Dockerfile + docker-compose.yml alongside existing Zurg/rclone stack

## Key Files to Read
- SPEC.md — full product requirements
- CLAUDE.md — coding conventions, architecture
- QUICKSTART.md — agent usage guide
- config.example.json — all configuration options
