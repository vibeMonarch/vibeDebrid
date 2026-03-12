# vibeDebrid — Memory

## Project State
- 1767 tests, all passing (as of 2026-03-12)
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
- Search UX timing + progressive results + score fixes — 2026-03-06
- Code review + fixes (mount scanner, stuck SCRAPING, CDN SRI, IMDB auto-resolve) — 2026-03-06
- Sticky headers all pages + Discover genre chips — 2026-03-07
- Tools page + Library Migration tool — 2026-03-07
- [Show Detail Page + Monitoring + Airing Seasons](show-monitoring.md) — 2026-03-07
- Path-prefix fallback for mount lookup — 2026-03-07
- [XEM scene numbering](xem-integration.md) — 2026-03-07
- XEM scene season restructuring (show detail + add) — 2026-03-08
- Language filter/preference — 2026-03-08
- Plex symlink naming mode — 2026-03-08
- Plex Watchlist sync — 2026-03-09
- TMDB ID backfill + migration dedup — 2026-03-09
- [RD Bridge + Smart Cleanup + Account Cleanup](rd-cleanup.md) — 2026-03-09
- Search add fixes: silent failure UX, check_cached race condition, missing year/tmdb_id — 2026-03-09
- [Original language preference](original-language.md) — 2026-03-11
- RD 451 infringing_file fallback — 2026-03-11
- [Issue #29 complete](rd-cleanup.md): mount path fallback + deletion safety hardening — 2026-03-12
- [Manual add for shows + season pack CHECKING fixes](manual-add-shows.md) — 2026-03-12
- [Symlink Health Check tool](symlink-health.md) — 2026-03-12
- [Issue #30](reverse-containment.md): bidirectional mount scanner lookup — 2026-03-12
- Issue #10: DB indexes on foreign keys + BigInteger filesize — 2026-03-12
- Issue #9: session.rollback() → savepoints in scrape_pipeline + migration — 2026-03-12

## Remaining / Future Work
- Plex watchlist removal sync (remove from watchlist on COMPLETE/DONE)
- 1 unresolved IMDB ID: tt0203082 (Rurouni Trust&Betrayal) — not in TMDB
  - tt1088540 was a truncated tt10885406 (Ascendance of a Bookworm) — resolved
- Docker (Step 3): Dockerfile + docker-compose.yml

## Remaining Review Findings (not yet fixed)
- ~~`scrape_pipeline.py:868`: `session.rollback()` mid-pipeline can corrupt ORM state~~ — fixed (issue #9)
- `migration.py` Steps 2-3 (Remove duplicates, Move): same class of bug — bare `except Exception` without savepoints
- `settings.py:51`: unvalidated `dict[str,Any]` body allows arbitrary key injection
- `search.py:430-441`: direct `item.state =` bypasses queue_manager (tech debt)
- Services: broad `except Exception` on JSON parsing — should be `except ValueError`
- `filter_engine.py:287`: regex compiled inside hot loop
- Frontend: no CSRF protection, duplicated `escapeHtml`/`formatBytes`
- `queue.py` rescrape endpoint: no server-side state validation (frontend guards only)
- `filter_engine.py`: `_score_original_language` double-penalty stacking (-30 for dubbed + no tags)
- `filter_engine.py`: `original_language` param accepts ISO or name but only name matches torrent tags
- `tmdb.py`: `ISO_639_1_TO_LANGUAGE` only covers 11 languages (Hindi, Arabic, Thai etc. missing)
- `scrape_pipeline.py`: `force_original_language` flag cleared before pipeline success

## XEM Negative Cache — 2026-03-08
- In-memory `_empty_response_cache: dict[int, float]` on XemMapper instance (not class-level — breaks tests)
- 5-minute TTL prevents cascading 429s when processing many items for same show
- Only deletes stale DB entries when new data arrives; returns stale entries on empty/failed response
- Cleared on successful API response with data

## Bulk Remove Concurrency — 2026-03-08
- Collects unique `rd_id` set during item loop, deletes concurrently via `asyncio.gather`
- `asyncio.Semaphore(5)` limits concurrent RD API calls
- Failed RD deletions reported in `BulkResponse.errors`
- Loading spinner on "Remove Selected" button (`bulkRemoving` state)

## Mount Scan Optimization (Issue #25) — 2026-03-09
- FUSE `readdir` is the dominant cost (~41s for 3104 files) — can't be optimized away
- `WalkEntry` NamedTuple: lightweight walk with no PTN parsing (deferred to upsert)
- `UpsertResult` NamedTuple: `(added, updated, unchanged, errors)`
- Skip-unchanged: compare `(filename, filesize)` in `_upsert_records`; batch `UPDATE last_seen_at`
- Stat-skip: `_load_known_files()` pre-loads DB; known files skip both `is_dir()` AND `stat()`
- Startup scan skipped when DB has data — `get_index_stats()` check in `main.py`
- `scan_directory()` always stats (passes `{}`) — targeted scans are small
- `None == None` is True in Python (correct for filesize comparison); SQL NULL=NULL is FALSE

## Fast CHECKING Resolution (Step 0.5) — 2026-03-04
- `mount_scanner.py`: `_scandir_walk()` + `_upsert_records()` batch helper + `scan_directory()` targeted scan
- `main.py`: captures `rd_info["filename"]` → `torrent.filename`, targeted scan fallback in CHECKING
- Timeouts: 120s full scan, 30s targeted

## Plex Integration (Step 1a) — 2026-03-04
- Stateless PlexClient, per-request httpx, OAuth PIN flow, targeted scan after COMPLETE
- `config_lock` in `src/config.py` for race-safe config.json writes
- Frontend: popup OAuth, library checkboxes, token omitted from save payload

## Search Architecture — 2026-03-06
- Two-fetch pattern: Zilean (fast) + Torrentio (slow, parallel)
- `_cacheStatusMap` persists RD cache results across re-renders
- `_cacheCheckGeneration` prevents stale writes
- Torrentio timeout: 10s (reduced from 30s due to 522 errors)
- Both pipeline and search strip `realdebrid=` from Torrentio opts (`include_debrid_key=False`) — see Torrentio RD Key Stripping

## Fuzzy Directory Match + Path-Prefix Fallback — 2026-03-07
- `_is_word_subsequence()` for fuzzy mount directory matching
- `lookup()`: 3-tier strategy: exact → forward word-subsequence → reverse containment (issue #30)
- Reverse containment (tier 3): `func.instr(literal(normalized), parsed_title) > 0` with 3-word min guard
- `lookup_by_path_prefix()`: WHERE filepath LIKE '{prefix}/%' with escape
- `ScanDirectoryResult(files_indexed, matched_dir_path)` for CHECKING fallback chain
- `symlink_health._find_mount_match()`: same bidirectional lookup (phase 1 LIKE + phase 2 reverse)

## SSE Feature Notes
- Event bus: module singleton, `put_nowait()`, maxsize=64 per client
- Publishing: inline imports in queue_manager to avoid circular deps
- Discover SSE: title-based matching (limitation: needs tmdb_id in QueueEvent)

## Sticky Headers — 2026-03-07
- `main { height: 100dvh; overflow-y: auto }` for CSS sticky
- `.page-header` in `base.html` (single source of truth)
- Touch: `@media (hover: hover)`, `_touchMoved` flag, `scroll-margin-top`

## Library Migration Tool — 2026-03-07
- `src/core/migration.py`: 4 parsing patterns, IMDB extraction, symlink detection
- Two-phase UI: preview → execute, 85 tests
- Shared `config_lock`, `_migration_lock` for concurrency

## Bugs Fixed
- Naive vs aware datetime in CHECKING timeout — normalize to tz-aware UTC
- Season pack duplicate add + XEM scrape mapping (2026-03-08) — see below
- Language filter Cyrillic bypass (2026-03-08): `_parse_languages()` only checked English tokens like "RUSSIAN"; Cyrillic-titled torrents had empty `languages` → assumed English → passed filter. Fix: added Cyrillic char detection (`\u0400-\u04FF`) + 11 abbreviated tokens (`RUS`,`JAP`,`JPN`,etc.) with `\b` word-boundary regex
- Single-file mount scan (2026-03-08): `scan_directory()` only handled directories; single-file RD torrents (`.mkv` filename) couldn't be found. Fix: added `_scan_single_file()` — detects video extensions, checks file in mount root directly, with fuzzy fallback
- Torrentio RD key filtering (2026-03-08): see Torrentio RD Key Stripping section below
- Season pack false positive (2026-03-08): PTN can't parse anime `S2 - 06` notation → `_SEASON_DASH_EP_RE` regex fallback in both torrentio.py and zilean.py
- Season pack scoring bias (2026-03-08): `prefer_season_packs` param on `filter_and_rank`; pipeline passes `item.is_season_pack` so episode items don't give +5 bonus to season pack results
- Anime CHECKING failures (2026-03-08): PTN can't parse `[Group] Title - NN` filenames → `_ANIME_DASH_EP_RE` regex fallback. Complete collections (flat, absolute ep numbering) → TMDB-based absolute episode range mapping. Single-file anime torrents (title mismatch) → no-filter fallback with single-file guard. Season pack filter hard-rejects single-episode results (`prefer_season_packs=True` in Tier 1)
- Search add silent failure (2026-03-09): `/api/add` returns HTTP 200 with `status="queued"` when `add_magnet` fails (e.g. RD rate limit from cache checking burst). Frontend now shows yellow "Queued" warning instead of green "Added"
- check_cached race condition (2026-03-09): search `/api/check-cached` used `add_magnet`/`delete_torrent` probe with `keep_if_cached=False`. If hash already in user's RD account, `add_magnet` returns existing rd_id, then `finally` deletes it. Fix: query `rd_torrents` table first, skip probe if hash already tracked as ACTIVE
- Search add missing metadata (2026-03-09): `AddRequest` lacked `tmdb_id`/`tvdb_id` fields, frontend hardcoded `year: null`. TMDB auto-resolve had the data but only saved `imdb_id`. Fix: capture year/tmdb_id/tvdb_id from TMDB resolve, pass in add payload, populate on MediaItem. Also added `tvdb_id` to discover resolve endpoint response

## Season Pack Split — 2026-03-08
- When no season packs available, auto-splits into individual episode queue items
- `scrape_pipeline.py:_split_season_pack_to_episodes()`: queries TMDB for episode count, creates individual WANTED items
- Triggers when: `item.is_season_pack` and scrapers returned results (`total_count > 0`) but `best is None` after filtering
- Dedup: uses `tmdb_id` (not `imdb_id`) to find existing episode items — SQL `NULL=NULL` is FALSE
- Parent item transitions to COMPLETE (not DONE — SCRAPING→DONE is invalid)
- Created items: `source="season_pack_split"`, inherit all IDs from parent, `is_season_pack=False`
- 22 tests in `tests/test_season_pack_split.py`

## Season Pack Dedup + XEM Scrape Fix — 2026-03-08
Three interrelated bugs when adding anime with XEM scene seasons:
1. **XEM absolute fallback** (`xem_mapper.py`): `get_scene_numbering_for_item` now
   falls back to `get_absolute_scene_map` when TVDB season/episode lookup fails.
   Fixes TMDB continuous seasons (e.g., S01E29) mapping to scene (S02E01).
2. **Hash-based dedup** (`scrape_pipeline.py`): After filter+rank, checks
   `check_local_duplicate(info_hash)` BEFORE cache check. If hash already in
   dedup registry, skips all RD API calls and transitions directly to CHECKING.
3. **Torrent lookup fallback** (`main.py`): `_find_torrent_for_item()` helper —
   direct media_item_id lookup → scrape_log info_hash → RdTorrent by hash.
   Used in ADDING and CHECKING stages for shared season pack torrents.

## Discovery Feature Notes
- TMDB client stateless, 7 API endpoints under `/api/discover/`
- State preservation: sessionStorage cache + one-shot restore

## Agent Routing Patterns
- Backend: backend-dev (sequential, shared state)
- Frontend: frontend-dev (after backend)
- Tests: test-writer (after implementation)
- Review: code-reviewer (final pass) — always run, catches real bugs

## Language Filter — 2026-03-08
- `FiltersConfig.preferred_languages: list[str]` — ordered list, e.g. ["English", "Japanese"]
- Tier 1: hard-reject results with detected languages not in preferred list
- Untagged results assumed English; Multi passes when `allow_multi_audio=True`
- Tier 2: `_score_language()` — 15pts 1st preferred, 12 2nd, 9 3rd, etc. Multi=10pts fallback
- Legacy `required_language` still works when `preferred_languages` is empty
- Settings UI: comma-separated input in Filters section
- Available languages: English, French, German, Spanish, Portuguese, Italian, Dutch, Russian, Japanese, Korean, Chinese

## Plex Symlink Naming — 2026-03-08
- `SymlinkNamingConfig.plex_naming: bool = False` — overrides date_prefix/year/resolution
- Show dir: `Title (Year) {tmdb-XXXXX}/Season XX/`
- Show file: `Title (Year) - S01E01.ext` (metadata-driven, not raw torrent name)
- Movie file: `Title (Year).ext`
- Season packs: `_parse_episode_from_filename()` extracts ep number from source filename via PTN + regex
- `_find_existing_show_dir` strips `{tmdb|tvdb|imdb-XXX}` tags before matching
- tmdb_id validated as digits-only before embedding in path
- Settings UI: toggle in Symlink Naming card, dims other toggles when active

## Torrentio RD Key Stripping — 2026-03-08
- `realdebrid=<key>` in Torrentio opts causes addon to pre-filter to RD-cached results only
- For niche content (anime): 41 results without key → 3 results with key (all Russian dubs)
- `_DEBRID_OPT_RE = re.compile(r"realdebrid=[^|]*")` strips segment from pipe-separated opts
- `include_debrid_key: bool = True` param threaded through `_build_base_url` → `_build_client` → `_query` → `scrape_movie`/`scrape_episode`
- Pipeline (`scrape_pipeline.py`) and search (`search.py`) both pass `include_debrid_key=False`
- Settings test endpoint keeps default `True` (tests user's configured opts work)
- Zilean unaffected — Frieren S02 not in DMM hashlists yet (expected for airing content)

## Plex Watchlist Sync — 2026-03-09
- `PlexConfig.watchlist_sync_enabled: bool = False`, `watchlist_poll_minutes: int = Field(default=30, ge=15)`
- `plex.py:get_watchlist()` — discover.provider.plex.tv (NOT metadata), JSON, paginated, `includeGuids=1`
- `src/core/plex_watchlist.py:sync_watchlist()` — batch dedup (tmdb_id + imdb_id IN query), per-item savepoints
- Movies: WANTED, source="plex_watchlist"; Shows: S1 pack + monitoring + immediate `_check_single_show`
- Mount index lookup before creating items (catches content added to RD outside vibeDebrid)
- Scheduler: always registered (early-exit when disabled), `max_instances=1`, min 15min interval
- 38 tests in `tests/test_plex_watchlist.py`

## TMDB ID Backfill — 2026-03-09
- `tmdb.find_by_imdb_id()`: `/find/{imdb_id}?external_source=imdb_id`, resolves tmdb_id + tvdb_id
- `backfill.py:backfill_tmdb_ids()`: Semaphore(10), gather(return_exceptions=True), batch UPDATE by imdb_id
- Startup: non-blocking background task with `_backfill_lock` shared with API endpoint
- `show_manager._check_single_show`: or_ query fallback for imdb_id when tmdb_id is NULL
- Real data: 73/75 resolved, 1350 rows enriched
- 35 tests in `tests/test_backfill.py`

## Key Conventions
- Commit style: imperative summary, bullet details, `Co-Authored-By: Claude Opus 4.6`
- DB migrations: `_migrate_add_columns()` in `src/database.py`
- State transitions: `queue_manager.transition()`, never direct assignment
- Settings PUT: reloads in-memory singleton via `setattr` loop
