# vibeDebrid — Project Status

## Last Updated: 2026-03-14

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
- src/services/torrentio.py — fallback chain (episode→season), 55 tests
  - Fixed: wrong emoji in seeders regex (U+1F465 → U+1F464)
  - Fixed: infoHash parsing with fallbacks
  - Fixed: `realdebrid=` key in opts pre-filters results to cached-only (strips key for pipeline/search)
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

**Total: 1276 tests, all passing**

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
- AIRING status overrides IN_QUEUE/IN_LIBRARY so users can always add remaining episodes from an airing season
- Season pack cutoff: episodes aired before a COMPLETE/DONE season pack's `state_changed_at` are skipped (prevents re-adding already-covered episodes)
- `_add_airing_season()` accepts `existing_items` to inspect pack state for cutoff logic
- `add_seasons()` bypasses season-level skip for airing seasons (`if has_any and season_num != airing_season_num`)
- 27 new tests (airing detection, per-episode creation, auto-subscribe, monitoring dedup, schema, pack cutoff)
- Total: 1108 tests, all passing

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
- Total: 1108 tests, all passing

### XEM Scene Season Restructuring ✅ (Step 1.9)
- Show detail page presents scene seasons instead of TMDB seasons for XEM-mapped anime
- e.g., Frieren: TMDB has 1 season (38 eps) → now shows Scene S01 (28 eps, complete) + S02 (10 eps, airing)
- Complete scene seasons → 1 season pack (1 scrape request vs 28 individual)
- Airing scene seasons → per-episode items with TMDB numbering (pipeline XEM-remaps at scrape time)
- Key insight: XEM `tvdb_absolute` field bridges TMDB continuous numbering → TVDB/scene season structure
- XEM client: returns all entries (including identity), includes `tvdb_absolute` field
- XEM mapper: `get_absolute_scene_map()` → `{absolute: (scene_season, scene_episode)}`
- Show manager: `_derive_scene_seasons()` groups TMDB episodes by absolute→scene mapping
- `_add_xem_airing_season()`: creates items with TMDB numbering for airing scene seasons
- `SeasonInfo.xem_mapped: bool` flag for frontend scene numbering indicator
- DB migration: `tvdb_absolute` column on `xem_cache` table
- Frontend: "Scene numbering" chip next to Seasons heading
- Graceful fallback: no XEM data → standard TMDB season display (unchanged)
- 28 new tests (5 mapper, 23 show_manager XEM paths)
- Total: 1136 tests, all passing

### Language Filter + Plex Symlink Naming ✅ (Step 1.10)
- Language filter: `preferred_languages` ordered list, Tier 1 hard-reject + Tier 2 scoring
- Untagged results assumed English; Multi passes when `allow_multi_audio=True`
- Cyrillic detection (`\u0400-\u04FF`) + 11 abbreviated tokens (`RUS`, `JPN`, etc.)
- Plex symlink naming: `plex_naming` toggle overrides date_prefix/year/resolution
- Show dir: `Title (Year) {tmdb-XXXXX}/Season XX/`, metadata-driven episode filenames
- Settings UI: comma-separated language input, Plex naming toggle

### Bugfixes (2026-03-08)
- Cyrillic language bypass: Cyrillic-titled torrents had empty `languages` → assumed English → passed filter
- Single-file mount scan: `scan_directory()` now handles single-file RD torrents (`.mkv` filename)
- Season pack dedup + XEM scrape mapping: hash-based dedup before cache check, absolute fallback for TMDB→scene mapping
- Plex scan batching: targeted scan after COMPLETE, scan sections grouped by library
- Torrentio RD key pre-filtering: `realdebrid=<key>` in opts causes 41→3 results for niche content; stripped for pipeline and search

### Anime CHECKING Fixes + Season Pack Split ✅ (2026-03-08)
- Anime filename parsing: `_ANIME_DASH_EP_RE` regex fallback for `[Group] Title - NN` patterns PTN can't parse
- Directory season inference: `_DIR_SEASON_RE` + `_extract_season_from_path()` for flat directory structures
- TMDB absolute episode mapping: `_get_absolute_episode_range()` maps TMDB seasons to absolute episode numbers for complete collections
- Cascading CHECKING fallbacks: season+episode → episode-only → no-filter (single-file guard)
- Season pack filter: `prefer_season_packs=True` hard-rejects single episodes in Tier 1 (prevents wrong content)
- Season pack split: when no season packs available but episode results exist, auto-splits into individual WANTED episode items via TMDB
  - `_split_season_pack_to_episodes()` in scrape_pipeline.py
  - Dedup via tmdb_id, parent transitions to COMPLETE, child items inherit all metadata
  - 22 new tests in `tests/test_season_pack_split.py`
- Total: 1316 tests, all passing

### Manual Add for Shows + Season Pack CHECKING Fixes ✅ (2026-03-12)
- Manual add form: own Movie/TV Show selector inside the form (independent from search dropdown)
- Season + season pack fields shown only when TV Show selected
- Auto-detect "Season N" from title: switches to TV Show, fills season, checks season pack
- AddRequest.title now optional (str | None) with "Unknown" fallback
- CHECKING auto-promote: show items with multiple distinct episodes auto-promote to season pack
- Leading-number episode regex: `_LEADING_EP_RE` for "28. Episode Title.mp4" → episode 28
- Season pack dedup: filters sample files and NULL-episode files before dedup
- Auto-correct media_type movie→show for season packs
- Relaxed season filter: path-prefix fallback with season=None when season-filtered query returns nothing
- Absolute episode fallback reordered before relaxed query (TMDB-guided filtering gets priority)
- Stuck-in-CHECKING fix: filtered-empty matches fall through to timeout instead of looping forever

### Episode Mismatch Filter + Retry Cleanup ✅ (2026-03-13)
- Episode mismatch Tier 1 hard filter: rejects results whose parsed S/E doesn't match requested S/E
  - `requested_season` + `requested_episode` params on `filter_and_rank()` and `_apply_hard_filters()`
  - Skips for season packs, movies, search endpoint, and unparsed results (benefit of doubt)
  - 13 new tests in `tests/test_filter_engine.py`
- Retry cleanup in `force_transition()`: marks linked RdTorrent as REMOVED + removes symlinks when DONE/COMPLETE → WANTED
  - Prevents dedup short-circuit loop (old wrong hash caused instant CHECKING with wrong file)
- Anime parsing fallbacks in torrentio.py + zilean.py:
  - `_ORDINAL_SEASON_EP_RE`: parses "2nd Season - 01" → S2E01
  - `_ANIME_BARE_DASH_EP_RE`: parses "Title - 29" → E29
  - Both feed into the episode mismatch filter so anime-named results are correctly rejected
- Total: 1789 tests, all passing

### Security Hardening ✅ (2026-03-14, Issue #13)
- CSRF double-submit cookie middleware (`src/middleware/csrf.py`)
  - Sets `csrf_token` cookie (SameSite=Strict, HttpOnly=False) on GET responses
  - Rejects mutation requests (POST/PUT/DELETE/PATCH) without matching `X-CSRF-Token` header
  - Exempts `/health` and `/api/events` (SSE)
- `csrfFetch()` wrapper in `base.html`, all mutation `fetch()` calls updated across all templates
- `SettingsUpdate` Pydantic model with `extra="forbid"` rejects unknown settings keys (422)
- `RequestValidationError` handler strips submitted values from 422 error responses
- Tailwind CDN pinned to 3.4.17 with SRI hash
- `backdrop_url` validated against `https://image.tmdb.org/` origin
- API key masking reduced to last 4 chars only (`***WXYZ`)
- 27 new tests (11 CSRF + 16 settings security)

### Race Conditions + Performance ✅ (2026-03-14, Issue #17)
- Bulk remove + single-item remove: DB commit before RD deletion (prevents orphaned DB refs)
- Symlink verification: single batch `asyncio.to_thread` instead of per-symlink dispatches
- TMDB season fetches: `asyncio.gather` with `Semaphore(5)` + `return_exceptions=True` for anime scene seasons
- Mount scanner: `rowcount` instead of `fetchall()` for stale entry count
- 17 new tests
- Total: 1950 tests, all passing

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
