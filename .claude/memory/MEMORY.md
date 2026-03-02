# vibeDebrid — Memory

## Project State
- 831 tests, all passing (as of 2026-03-02)
- Python 3.14, FastAPI, SQLite async, htmx frontend
- Test runner: `.venv/bin/python -m pytest tests/ -q`

## Completed Features
- [Symlink Naming Convention](symlink-naming.md) — 2026-03-01
- [Season Pack Support](season-packs.md) — 2026-03-01
- Seeders display removal — 2026-03-02
- Manual RD cache check button + configurable limit — 2026-03-02
- Discovery feature (TMDB trending/search/top_rated/genres/discover + add to queue) — 2026-03-02
- SSE live updates (event_bus + /api/events + queue/dashboard frontend) — 2026-03-02

## STATUS.md Next Steps
Pending: Discover state preservation (Step 0.4), Fast CHECKING resolution (Step 0.5), then Trakt + Plex integration (Step 1)

## SSE Feature Notes
- Event bus: `src/core/event_bus.py` — module singleton, `put_nowait()` never blocks, maxsize=64 per client
- SSE endpoint: `src/api/routes/sse.py` — `GET /api/events`, 30s heartbeat, None sentinel for shutdown
- Publishing: inline imports in `queue_manager.transition()`/`force_transition()` to avoid circular deps
- Frontend: `VD_SSE` IIFE in base.html, lazy EventSource, handler fanout
- `add_torrent()` in search.py still bypasses queue_manager → no SSE events for manual adds (tech debt)

## Bugs Fixed
- Naive vs aware datetime comparison in CHECKING timeout (`main.py` lines 170, 233) — `state_changed_at` is tz-aware when set in-memory by queue_manager but naive when loaded from SQLite. Fix: normalize naive values to tz-aware UTC before comparing. NOTE: `.replace(tzinfo=None)` approach fails for in-memory objects; adding tz to naive values handles both paths.

## Discovery Feature Notes
- TMDB client: `src/services/tmdb.py` — stateless, each method opens/closes its own httpx session
- 7 API endpoints under `/api/discover/`: trending, top_rated, genres, by-genre, search, add, resolve
- Frontend tabs: Movies, TV Shows, Search — each media tab loads 3 parallel fetches (trending + top_rated + genres)
- Genre browsing: chips from `/genres`, click loads `/by-genre` with `vote_count_gte=50` filter
- `_enrich_with_queue_status()` does batch DB lookup for queue badges (available/in_queue/in_library)
- "Add to Library" flow: resolve TMDB→IMDB → navigate to `/search?query=...&imdb_id=...&media_type=...&from=discover` → auto-search → user picks torrent → redirect back to `/discover` after 1.5s
- Known issue: returning to discover loses tab/genre/scroll state (Step 0.4)

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
