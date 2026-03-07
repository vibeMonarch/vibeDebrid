# vibeDebrid — Memory

## Project State
- 1074 tests, all passing (as of 2026-03-07)
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

## Remaining / Future Work
- XEM scene numbering: TMDB↔scene mapping for anime (Frieren S01E29 ≠ torrent S02E01)
- Trakt integration (Step 1b): OAuth, watchlist polling, scheduler
- Upgrade manager (Step 2): monitor for higher quality versions within window
- Docker (Step 3): Dockerfile + docker-compose.yml

## Remaining Review Findings (not yet fixed)
- `scrape_pipeline.py:868`: `session.rollback()` mid-pipeline can corrupt ORM state
- `settings.py:51`: unvalidated `dict[str,Any]` body allows arbitrary key injection
- `search.py:272`: no error handling on `check_cached` — RD down = 500
- `search.py:430-441`: direct `item.state =` bypasses queue_manager (tech debt)
- Services: broad `except Exception` on JSON parsing — should be `except ValueError`
- `filter_engine.py:287`: regex compiled inside hot loop
- Frontend: no CSRF protection, duplicated `escapeHtml`/`formatBytes`

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

## Fuzzy Directory Match + Path-Prefix Fallback — 2026-03-07
- `_is_word_subsequence()` for fuzzy mount directory matching
- `lookup()`: exact match → SQL LIKE + Python verification (2+ words minimum)
- `lookup_by_path_prefix()`: WHERE filepath LIKE '{prefix}/%' with escape
- `ScanDirectoryResult(files_indexed, matched_dir_path)` for CHECKING fallback chain

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

## Discovery Feature Notes
- TMDB client stateless, 7 API endpoints under `/api/discover/`
- State preservation: sessionStorage cache + one-shot restore

## Agent Routing Patterns
- Backend: backend-dev (sequential, shared state)
- Frontend: frontend-dev (after backend)
- Tests: test-writer (after implementation)
- Review: code-reviewer (final pass) — always run, catches real bugs

## Key Conventions
- Commit style: imperative summary, bullet details, `Co-Authored-By: Claude Opus 4.6`
- DB migrations: `_migrate_add_columns()` in `src/database.py`
- State transitions: `queue_manager.transition()`, never direct assignment
- Settings PUT: reloads in-memory singleton via `setattr` loop
