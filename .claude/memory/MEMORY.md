# vibeDebrid — Memory

## Project State
- 2211 tests, all passing (as of 2026-03-14)
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
- [Episode mismatch filter + retry cleanup](episode-mismatch-filter.md) — 2026-03-13
- Issue #20: /health endpoint for monitoring + Docker health checks — 2026-03-13
- Issue #21: Replace hardcoded developer paths with empty defaults + startup warnings — 2026-03-13
- Issue #15: Pre-compile regex patterns in filter_engine hot loops — 2026-03-13
- Issue #19: SSE Discover badges use tmdb_id instead of fragile title matching — 2026-03-13
- Issue #11: Narrow broad except Exception to specific types across 8 files — 2026-03-13
- Issue #16: Search route error handling (429/502) + bulk remove concurrency fix — 2026-03-13
- Issue #26: Open source prep (Apache 2.0, CONTRIBUTING.md, config.example.json) — 2026-03-14
- Issue #23+#32: RD cleanup Zurg safety (live mount verification protection) — 2026-03-14
- Issue #33: Mount index directory name fallback for season packs — 2026-03-14
- Issue #14: Circuit breaker + HTTP connection pooling for all 6 services — 2026-03-14
- Search add flow fix: user season override + TMDB enrichment + multi-season symlink filtering — 2026-03-14
- Issue #13: Security hardening (CSRF middleware, settings validation, SRI, input sanitization) — 2026-03-14
- Issue #17: Race conditions (bulk remove ordering, batch symlink verify, concurrent TMDB, rowcount) — 2026-03-14
- XEM absolute fallback fix: skip remap for season > 1 (was mapping S03→S01 for multi-season shows) — 2026-03-14
- Mount scan UNIQUE constraint fix: ON CONFLICT DO UPDATE + input dedup — 2026-03-14
- Bare trailing episode parser: "Wolf's Rain 01.mkv" pattern for mount_scanner + symlink_manager — 2026-03-14
- Anime batch season pack detection: BATCH keyword, episode range "01~13", Season N keyword — 2026-03-14
- PTN list normalization: episode/season lists from multi-ep/multi-season titles → take first element — 2026-03-14
- Zilean PTN always called for season/episode even when resolution/codec populated — 2026-03-14
- Zilean title-only fallback: retry without IMDB/season/episode filters when 0 results — 2026-03-14
- Torrentio S01E01 default: search shows without season/episode defaults to S01E01 anchor — 2026-03-14
- Mount scanner re-parse: files with parsed_episode=NULL re-parsed on scan (picks up new parser fallbacks) — 2026-03-14
- Tailwind SRI removed: Play CDN is dynamic JIT, SRI hashes unreliable — 2026-03-14
- H.264/H.265 codec exclusion: 264/265 added to _NON_EPISODE_NUMBERS set — 2026-03-14
- Sequential RD cache check: check-and-stop replaces batch (2-3 API calls vs 12 per episode) — 2026-03-14
- XEM scene season packs: TMDB anchor metadata, pipeline remap, CHECKING episode range filter — 2026-03-14
- Issue #34: Alternative title fallback for Zilean (original_title + TMDB alt titles, lazy fetch, capped at 5) — 2026-03-14
- Issue #24: RD account health dashboard card (premium status, days remaining) — 2026-03-14
- Issue #12: Extract duplicated code (torrent_parser.py shared module + utils.js) — 2026-03-14
- Issue #22: Extract inline JS to static files (8 page JS files, ~5,300 lines moved) — 2026-03-14
- Issue #8: Dashboard upcoming episodes card (replaces Active Processing) — 2026-03-14
- Issues #6+#7: OMDb ratings on show detail page (IMDb, RT, Metascore) — 2026-03-14
- Movie detail page (`/movie/{tmdb_id}`) with hero, ratings, add flow — 2026-03-14

## Critical Domain Knowledge
- [Zurg auto-recovery](zurg-autorecovery.md) — Zurg replaces CDN-dropped files with different RD torrents, keeps mount paths stable; causes hash drift affecting cleanup safety

## Remaining / Future Work
- Issue #32: Zurg `on_library_update` webhook integration (blocked on dockerization #18)
- Issue #18: Dockerization (Dockerfile + docker-compose)
- Plex watchlist removal sync (remove from watchlist on COMPLETE/DONE)
- 1 unresolved IMDB ID: tt0203082 (Rurouni Trust&Betrayal) — not in TMDB

## Open Issues
- ~~#22: Extract inline JS to static files~~ resolved — 8 page JS files extracted
- #18: Dockerization (unblocks #32)
- #4: Per-episode TV discovery
- Movie detail page: no queue_status yet (always shows Add button)

## Remaining Review Findings (not yet fixed)
- `migration.py` Steps 2-3: bare `except Exception` without savepoints
- ~~Frontend: duplicated `escapeHtml`/`formatBytes`~~ resolved by #12 (utils.js)
- `queue.py` rescrape endpoint: no server-side state validation (frontend guards only)
- `filter_engine.py`: `_score_original_language` double-penalty stacking
- `filter_engine.py`: `original_language` param accepts ISO or name but only name matches
- `tmdb.py`: `ISO_639_1_TO_LANGUAGE` only covers 11 languages
- `scrape_pipeline.py`: `force_original_language` flag cleared before pipeline success
- ~~150 lines duplicated regex/parsing~~ resolved by #12 (torrent_parser.py)
- ~~Inconsistent fallback chains across 4 parsers~~ resolved by #12 (shared parse_episode_fallbacks)
- `search.py` bare `except Exception` in _scrape_torrentio/_scrape_zilean
- CHECKING no-filter fallback can match wrong single file (no episode verification)

## Circuit Breaker + HTTP Pooling — 2026-03-14
- `src/services/http_client.py`: shared `httpx.AsyncClient` pool + `CircuitBreaker` class
- Per-service circuit breakers (keyed by service name), 5 failure threshold, 60s recovery
- 429 rate limits excluded from circuit breaking (RD, TMDB, XEM all handle correctly)
- `close_all()` in main.py shutdown hook
- Known limitation: pooled clients bake in API key at creation time (config changes need restart)

## Multi-Season Symlink Handling — 2026-03-14
- Search add: user's season input takes priority over PTN-parsed season from torrent title
- TMDB enrichment: `add_torrent` fetches canonical title/year from TMDB when tmdb_id is provided
- CHECKING stage: detects multi-season torrents (distinct `parsed_season` values), re-filters to requested season
- `create_symlink(episode_offset=N)`: remaps absolute episode numbers to season-relative (ep 26 with offset 25 → E01)
- Mount index: falls back to parent directory name when filename parse yields only episode title

## Agent Routing Patterns
- Backend: backend-dev (sequential, shared state)
- Frontend: frontend-dev (after backend)
- Tests: test-writer needs bash access → use backend-dev instead
- Review: code-reviewer (final pass) — always run, catches real bugs
- Worktree cleanup: worktrees are deleted when agent completes — merge before dispatching dependent agents

## Key Conventions
- Commit style: imperative summary, bullet details, `Co-Authored-By: Claude Opus 4.6`
- DB migrations: `_migrate_add_columns()` in `src/database.py`
- State transitions: `queue_manager.transition()`, never direct assignment
- Settings PUT: reloads in-memory singleton via `setattr` loop

## User Feedback
- [Never push without consent](feedback-no-push.md) — always ask before pushing
- Always confirm approach before implementing — present plan, get approval first
- [Frontend JS convention](frontend-js-convention.md) — no inline JS in templates, use src/static/js/<pagename>.js
