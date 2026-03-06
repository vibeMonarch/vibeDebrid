# vibeDebrid — Memory

## Project State
- 865 tests, all passing (as of 2026-03-06)
- Python 3.14, FastAPI, SQLite async, htmx frontend
- Test runner: `.venv/bin/python -m pytest tests/ -q`

## Completed Features
- [Symlink Naming Convention](symlink-naming.md) — 2026-03-01
- [Season Pack Support](season-packs.md) — 2026-03-01
- Seeders display removal — 2026-03-02
- Manual RD cache check button + configurable limit — 2026-03-02
- Discovery feature (TMDB trending/search/top_rated/genres/discover + add to queue) — 2026-03-02
- SSE live updates (event_bus + /api/events + queue/dashboard frontend) — 2026-03-02
- Discover state preservation (sessionStorage cache + restore) — 2026-03-04
- Queue detail panel fixes + Discover SSE badges + manual ScrapeLog — 2026-03-04
- Plex integration (OAuth, library scan, settings UI) — 2026-03-04
- Discover page mobile/desktop UX (scroll snap, fade gradients, responsive cards, tap overlay) — 2026-03-06
- Search UX timing (parallel scrapers, instant result render, progressive cache checks) — 2026-03-06
- Progressive search results (two-fetch pattern, Zilean instant + Torrentio merge) — 2026-03-06
- Search score + layout fixes (live score update on cache resolve, inline breakdown, unified button text) — 2026-03-06
- Code review + fixes (mount scanner exact match, stuck SCRAPING recovery, CDN SRI, IMDB auto-resolve) — 2026-03-06

## Fast CHECKING Resolution (Step 0.5) — 2026-03-04
- `mount_scanner.py`: `_scandir_walk()` replaces `os.walk`+`os.path.getsize` with `os.scandir`+`DirEntry.stat()` (fewer FUSE syscalls)
- `mount_scanner.py`: `_upsert_records()` batch helper — WHERE filepath IN batches of 500, not 1 SELECT per file
- `mount_scanner.py`: `scan_directory(session, dir_name)` — targeted single-dir scan for fast CHECKING
- `main.py` ADDING: captures `rd_info["filename"]` → `torrent.filename` before CHECKING transition
- `main.py` CHECKING: targeted scan fallback — if lookup() empty, scan_directory(torrent.filename), re-lookup
- Both `_scandir_walk` calls have timeouts (120s full scan, 30s targeted)

## Plex Integration (Step 1a) — 2026-03-04
- `src/services/plex.py`: stateless PlexClient, per-request httpx sessions, module singleton `plex_client`
- Exceptions: `PlexError`, `PlexAuthError`, `PlexConnectionError` (last one defined but not raised yet)
- OAuth PIN flow: `create_pin()` → browser popup to `app.plex.tv/auth#?...` → poll `check_pin(pin_id)` every 2s
- Client ID is a fixed UUID (`f0f4c4b8-...`), auth URL needs full `context[device]` params (product, version, platform, device, deviceName, model, screenResolution, language)
- `_build_plex_tv_client()` — separate helper for plex.tv calls, deliberately omits `X-Plex-Token`
- `settings.py`: 4 new endpoints + `_config_lock = asyncio.Lock()` for race-safe config.json writes
- `main.py`: Plex scan trigger after COMPLETE transition, guarded by enabled + scan_after_symlink + token, non-fatal
- Frontend: OAuth popup flow with 2s polling, popup-close detection, library section checkboxes, token intentionally omitted from save payload
- 12 tests with mocked httpx transport

## Discover Page Mobile UX — 2026-03-06
- `scroll-snap-type: x proximity` (not `mandatory` — mandatory absorbs swipe momentum, gets stuck around item 13)
- `overscroll-behavior-x: contain` prevents horizontal overscroll from blocking vertical page scroll
- `main { overflow-x: hidden }` is critical — without it, section rows push `main` wider than viewport, breaking genre grid
- Fade gradients: `::before`/`::after` on `.section-row-wrap`, toggled by `at-start`/`at-end` classes via scroll listener
- Must add `at-end` class in HTML alongside `at-start` to prevent flash on empty/short rows before JS initializes
- Touch overlay: `matchMedia('(hover: none)')` detection, delegated click handler toggles `.overlay-visible`
- SSE badge data: store `data-media-type`/`data-year` on card element (not on disabled buttons that get replaced)

## Search UX Timing — 2026-03-06
- Torrentio 522 errors (Cloudflare) take ~20s — was blocking Zilean results when run sequentially
- Torrentio timeout reduced from 30s to 10s in `TorrentioConfig`
- `scrollIntoView({ behavior: 'instant' })` not `'smooth'` — smooth delays visibility on mobile

## Search Score + Layout Fixes — 2026-03-06
- `updateCacheBadge()` now recalculates score: finds item in `_currentResults`, updates `score_breakdown.cached` (+10 or 0), recomputes total
- Uses `.score-value`, `.score-bar-fill`, `.score-breakdown` class hooks to update DOM (both desktop + mobile layouts)
- Removed tooltip (`.tooltip-wrapper`/`.tooltip-box`) — was clipping inside card overflow, only showed partial breakdown
- Desktop score breakdown shown inline (`<p class="score-breakdown">`) same as mobile
- Button text unified to "Add" on both layouts — "Add to Real-Debrid" was misleading for cached items

## Progressive Search (Two-Fetch Pattern) — 2026-03-06
- Backend: `scrapers: list[Literal["torrentio", "zilean"]] | None` on `SearchRequest` — `None` = both (backward-compatible)
- Frontend fires two parallel fetches: zilean-only (fast) + torrentio-only (slow, only if `imdb_id` set)
- Zilean results render immediately; Torrentio results deduped by `info_hash`, merged, re-sorted by score
- `_cacheStatusMap: Map<hash, bool>` persists cache check results across re-renders
- `cacheBadgeHtml` checks `_cacheStatusMap` first — shows resolved badge instead of stale spinner after merge re-render
- `_cacheCheckGeneration` bumped at search start (not inside `checkCachedProgressive`) to close stale-write window
- Torrentio merge passes existing generation to `checkCachedProgressive` so zilean cache loop isn't cancelled
- `showMoreResultsIndicator(false)` in `finally` block for defensive cleanup
- `_totalFiltered` uses `torrentioData.total_filtered` not `newResults.length` (avoids "Showing 85 of 80" bug)

## Code Review Fixes — 2026-03-06
- Full codebase review: 4 parallel code-reviewer agents (core logic, services, API+main, frontend)
- Mount scanner `lookup()`: changed from `ILIKE %title%` (substring) to `== normalized` (exact match) — prevents false COMPLETE transitions
- Stuck SCRAPING recovery: Stage 0 in `_job_queue_processor` finds items in SCRAPING >30min, force-transitions to WANTED
- `max_instances=1` added to mount_scan and symlink_verifier scheduler jobs (was only on queue_processor)
- CDN: htmx pinned to exact URL with SHA-384 SRI, Alpine.js pinned from `3.x.x` to `3.15.8` with SRI
- Search IMDB auto-resolve: `handleSearch()` calls `/api/discover/search` + `/api/discover/resolve` when no IMDB ID provided
- Zilean limitation: can't find "xXx" (3-char non-alphanumeric title) — only Torrentio via IMDB ID works for such titles

## Remaining Review Findings (not yet fixed)
- `scrape_pipeline.py:868`: `session.rollback()` mid-pipeline can corrupt ORM state — use savepoints or re-fetch
- `settings.py:51`: unvalidated `dict[str,Any]` body allows arbitrary key injection into config.json
- `search.py:272`: no error handling on `check_cached` endpoint — RD down = 500 errors
- `search.py:430-441`: direct `item.state =` bypasses queue_manager (known tech debt)
- Services: broad `except Exception` on JSON parsing (~8 sites) — should be `except ValueError`
- Services: duplicated language tokens/regex between torrentio.py and zilean.py
- `filter_engine.py:287`: regex compiled inside hot loop (500+ re.compile per filter_and_rank)
- Frontend: no CSRF protection, duplicated `escapeHtml`/`formatBytes` across templates

## STATUS.md Next Steps
Pending: Trakt integration (Step 1b)

## SSE Feature Notes
- Event bus: `src/core/event_bus.py` — module singleton, `put_nowait()` never blocks, maxsize=64 per client
- SSE endpoint: `src/api/routes/sse.py` — `GET /api/events`, 30s heartbeat, None sentinel for shutdown
- Publishing: inline imports in `queue_manager.transition()`/`force_transition()` to avoid circular deps
- Frontend: `VD_SSE` IIFE in base.html, lazy EventSource, handler fanout
- Discover SSE: title-based card matching (limitation: same-name media false positives, needs tmdb_id in QueueEvent)
- `add_torrent()` in search.py still bypasses queue_manager → no SSE events for manual adds (tech debt)

## Queue Detail Panel Notes
- API returns `ItemDetailResponse { item: MediaItemResponse, scrape_logs: [], torrents: [] }`
- Frontend flattens: `{ ...data.item, scrape_logs: data.scrape_logs, torrents: data.torrents }`
- ScrapeLog fields: `scraper`, `query_params` (JSON string), `scraped_at`, `selected_result` (JSON string)
- RdTorrent fields: `filename`, `filesize` (not size_bytes), `rd_id`, `resolution`, `cached` (nullable), `status`

## Fuzzy Directory Match in scan_directory — 2026-03-04
- RD filenames have special chars (colons etc.) that Zurg/rclone sanitizes on filesystem
- `scan_directory()` fuzzy fallback: list mount root, normalize names, startswith + word-boundary check
- Collects all candidates, shortest normalized name wins, alphabetical tiebreaker for determinism
- `os.scandir` context manager for proper fd cleanup, 5s timeout via `asyncio.to_thread`
- Boundary check prevents mid-word matches ("predator" ≠ "predators") but NOT sequel matches ("predator" = "predator 2") — shortest-name sort handles disambiguation

## Bugs Fixed
- Naive vs aware datetime comparison in CHECKING timeout (`main.py` lines 170, 233) — `state_changed_at` is tz-aware when set in-memory by queue_manager but naive when loaded from SQLite. Fix: normalize naive values to tz-aware UTC before comparing. NOTE: `.replace(tzinfo=None)` approach fails for in-memory objects; adding tz to naive values handles both paths.

## Discovery Feature Notes
- TMDB client: `src/services/tmdb.py` — stateless, each method opens/closes its own httpx session
- 7 API endpoints under `/api/discover/`: trending, top_rated, genres, by-genre, search, add, resolve
- Frontend tabs: Movies, TV Shows, Search — each media tab loads 3 parallel fetches (trending + top_rated + genres)
- Genre browsing: chips from `/genres`, click loads `/by-genre` with `vote_count_gte=50` filter
- `_enrich_with_queue_status()` does batch DB lookup for queue badges (available/in_queue/in_library)
- "Add to Library" flow: resolve TMDB→IMDB → navigate to `/search?query=...&imdb_id=...&media_type=...&from=discover` → auto-search → user picks torrent → redirect back to `/discover` after 1.5s
- State preservation: sessionStorage caches full TMDB response data (both tabs) + genre items + page counters + scroll position. Restored on return from /search with zero API calls. One-shot (cleared after restore).

## Agent Routing Patterns
- Backend changes (models, routes, pipeline): backend-dev (sequential, shared state)
- Frontend (templates, JS): frontend-dev (after backend, needs new JSON fields)
- Tests: test-writer (after implementation)
- Review: code-reviewer (final pass, findings only)
- Always run code-reviewer after implementation — it catches real bugs

## Key Conventions
- Commit style: imperative summary, bullet details, `Co-Authored-By: Claude Opus 4.6`
- DB migrations: add to `_migrate_add_columns()` in `src/database.py`, catch specific errors
- State transitions: use `queue_manager.transition()`, never direct `item.state =` assignment
- `add_torrent()` in search.py bypasses queue_manager for state changes (pre-existing tech debt)
- Settings PUT handler reloads in-memory singleton via `setattr` loop — no restart needed for any setting
