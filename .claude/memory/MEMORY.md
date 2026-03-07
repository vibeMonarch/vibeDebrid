# vibeDebrid — Memory

## Project State
- 1038 tests, all passing (as of 2026-03-07)
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
- Discover sticky header + genre chips (title/tabs/genre chips pin on scroll) — 2026-03-06
- Sticky headers all pages + touch/IMDB fixes — 2026-03-07
- Tools page + Library Migration tool — 2026-03-07
- Show Detail Page + Monitoring subscriptions — 2026-03-07

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
- `settings.py`: 4 new endpoints, uses shared `config_lock` from `src/config.py` for race-safe config.json writes
- `main.py`: Plex scan trigger after COMPLETE transition, guarded by enabled + scan_after_symlink + token + symlink not None, non-fatal
- `main.py`: targeted scan — passes `os.path.dirname(symlink.target_path)` to `scan_section(path=)` instead of full library rescan
- Season pack path: `symlink` captured from loop (last successful), `None` guard prevents UnboundLocalError
- Frontend: OAuth popup flow with 2s polling, popup-close detection, library section checkboxes, token intentionally omitted from save payload
- 12 tests with mocked httpx transport

## Discover Page Mobile UX — 2026-03-06
- `scroll-snap-type: x proximity` (not `mandatory` — mandatory absorbs swipe momentum, gets stuck around item 13)
- `overscroll-behavior-x: contain` prevents horizontal overscroll from blocking vertical page scroll
- Fade gradients: `::before`/`::after` on `.section-row-wrap`, toggled by `at-start`/`at-end` classes via scroll listener
- Must add `at-end` class in HTML alongside `at-start` to prevent flash on empty/short rows before JS initializes
- Touch overlay: `matchMedia('(hover: none)')` detection, delegated click handler toggles `.overlay-visible`
- SSE badge data: store `data-media-type`/`data-year` on card element (not on disabled buttons that get replaced)

## Sticky Headers (All Pages) — 2026-03-07
- `main` scroll container + `.page-header` class defined in `base.html` (single source of truth)
- `main { height: 100dvh; overflow-y: auto; overflow-x: hidden; min-height: unset; }` — required for CSS sticky
- `.page-header`: `position: sticky; top: 0; z-index: 20; background: #0f172a; padding-top/bottom: 0.75rem; border-bottom`
- Mobile (`max-width: 1023px`): `padding-left: 3.5rem` avoids hamburger overlap
- Discover: `.discover-header` extends `.page-header`, only overrides `padding-bottom: 1rem`
- `.genre-chips-sticky`: `sticky; top: var(--header-h); scroll-margin-top: var(--header-h)` — genre switch scrolls chips to top
- `updateHeaderHeight()` JS measures header, sets `--header-h` CSS var; runs on `resize` + initial `requestAnimationFrame`
- Scroll save/restore uses `document.querySelector('main').scrollTop` (not `window.scrollY`)
- Touch: `:hover` overlay must be `@media (hover: hover)` — mobile browsers assign sticky `:hover` to elements under finger on render
- Touch: `_touchMoved` flag (touchstart resets, touchmove sets) prevents scroll-triggered overlay toggles
- `scrollIntoView({ block: 'start' })` needs `scroll-margin-top` on target to offset sticky header

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

## Library Migration Tool — 2026-03-07
- `src/core/migration.py`: filesystem scanner + migration engine, system-agnostic (no external DB dependency)
- `src/api/routes/tools.py`: Tools page render + preview/execute API endpoints
- `src/templates/tools.html`: two-phase wizard UI (preview → execute)
- Parsing: 4 patterns — `Title (Year)`, `Title [Year]`, scene dots (`Title.Year.Res...`), fallback
- `extract_imdb_id()`: regex `tt\d{7,}` from filenames (CLI Debrid embeds IMDB IDs)
- Scan: walks movies/shows dirs, detects symlinks vs real files, extracts metadata
- Preview: duplicate detection by source_path (exact) or normalized title+year+type+season+episode (fuzzy)
- Execute: import as COMPLETE (source="migration"), remove dups, move unique items, update config
- Shared `config_lock` in `src/config.py` — both settings.py and migration.py import it (was separate locks before)
- `_migration_lock` in tools.py — 409 Conflict if migration already running
- Session commit after execute, rollback on per-item import errors
- Found items table capped at 50 rows in UI (expandable) for large libraries
- 85 tests (parsing, scanning with tmp_path, preview, execute, API routes)
- Known limitation: scene parser edge case `"1917 2019 1080p"` — use parenthesized `"1917 (2019)"` instead

## Show Detail Page + Monitoring — 2026-03-07
- `src/models/monitored_show.py`: `MonitoredShow` ORM — tmdb_id (unique, indexed), imdb_id, title, year, quality_profile, enabled, last_season, last_episode, last_checked_at
- `src/core/show_manager.py`: `ShowManager` singleton — `get_show_detail()`, `add_seasons()`, `set_subscription()`, `check_monitored_shows()`
- `src/api/routes/show.py`: GET `/api/show/{tmdb_id}`, POST `/api/show/add`, PUT `/api/show/{tmdb_id}/subscribe`
- `src/templates/show.html`: season picker with status badges, subscribe toggle, quality dropdown, sticky add bar
- `src/services/tmdb.py`: `get_show_details()` (uses `append_to_response=external_ids`), `get_season_details()`
- Discover click flow: TV shows → `/show/{tmdb_id}?from=discover` (no IMDB resolve needed), movies → `/search` (unchanged)
- Season selection: each season creates MediaItem(is_season_pack=True, episode=None, state=WANTED)
- Subscribe: MonitoredShow record, 6h scheduler job checks TMDB for new seasons/episodes
- Monitoring logic: new complete seasons → season pack, current season new episodes → per-episode items
- `_parse_air_date()` helper guards against malformed TMDB air_date strings
- `date.today()` replaced with `datetime.now(timezone.utc).date()` for UTC consistency
- 85 tests (53 show_manager + 32 API routes)
- Deferred: season pack exploder (fallback to individual episodes when no pack found)

## Remaining / Future Work
- Season pack exploder: when no pack found, auto-create per-episode items (deferred from show detail feature)
- Trakt integration (Step 1b): OAuth, watchlist polling, scheduler
- Upgrade manager (Step 2): monitor for higher quality versions within window
- Docker (Step 3): Dockerfile + docker-compose.yml

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

## Fuzzy Directory Match + Word Subsequence — 2026-03-07
- RD filenames have special chars (colons etc.) that Zurg/rclone sanitizes on filesystem
- `_is_word_subsequence(query_words, target_words)` — checks all query words appear in target in order (not necessarily consecutive)
- `scan_directory()` fuzzy fallback: list mount root, normalize names, word subsequence check (was `startswith`)
- Handles titles with missing tokens: "Terminator Judgement Day" matches "Terminator.2.Judgement.Day.1991..."
- `lookup()` fallback: exact match first, then SQL LIKE `%word1%word2%` + Python word-subsequence verification
- Fallback requires 2+ query words (prevents "dark" matching "the dark knight")
- Empty input guard in fuzzy match prevents matching all directories
- Known limitation: sequel false positives ("Iron Man" could match "Iron Man 3" if only sequel exists)

## Bugs Fixed
- Naive vs aware datetime comparison in CHECKING timeout (`main.py` lines 170, 233) — `state_changed_at` is tz-aware when set in-memory by queue_manager but naive when loaded from SQLite. Fix: normalize naive values to tz-aware UTC before comparing. NOTE: `.replace(tzinfo=None)` approach fails for in-memory objects; adding tz to naive values handles both paths.

## Discovery Feature Notes
- TMDB client: `src/services/tmdb.py` — stateless, each method opens/closes its own httpx session
- 7 API endpoints under `/api/discover/`: trending, top_rated, genres, by-genre, search, add, resolve
- Frontend tabs: Movies, TV Shows, Search — each media tab loads 3 parallel fetches (trending + top_rated + genres)
- Genre browsing: chips from `/genres`, click loads `/by-genre` with `vote_count_gte=50` filter
- `_enrich_with_queue_status()` does batch DB lookup for queue badges (available/in_queue/in_library)
- "Add to Library" flow (movies): resolve TMDB→IMDB → navigate to `/search?...&from=discover` → auto-search → user picks torrent → redirect back to `/discover` after 1.5s
- "Add to Library" flow (TV shows): navigate to `/show/{tmdb_id}?from=discover` → season picker → add seasons → redirect back to `/discover` after 1.5s
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
