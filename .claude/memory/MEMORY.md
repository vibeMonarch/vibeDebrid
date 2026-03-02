# vibeDebrid — Memory

## Project State
- 802 tests, all passing (as of 2026-03-02)
- Python 3.14, FastAPI, SQLite async, htmx frontend
- Test runner: `.venv/bin/python -m pytest tests/ -q`

## Completed Features
- [Symlink Naming Convention](symlink-naming.md) — 2026-03-01
- [Season Pack Support](season-packs.md) — 2026-03-01
- Seeders display removal — 2026-03-02
- Manual RD cache check button + configurable limit — 2026-03-02
- Discovery feature (TMDB trending/search/top_rated/genres/discover + add to queue) — 2026-03-02

## STATUS.md Next Steps
Pending features: Trakt + Plex integration (Step 1)

## Discovery Feature Notes
- TMDB client: `src/services/tmdb.py` — stateless, each method opens/closes its own httpx session
- 6 API endpoints under `/api/discover/`: trending, top_rated, genres, by-genre, search, add
- Frontend tabs: Movies, TV Shows, Search — each media tab loads 3 parallel fetches (trending + top_rated + genres)
- Genre browsing: chips from `/genres`, click loads `/by-genre` with `vote_count_gte=50` filter
- `_enrich_with_queue_status()` does batch DB lookup for queue badges (available/in_queue/in_library)

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
