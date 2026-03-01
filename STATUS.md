# vibeDebrid — Project Status

## Last Updated: 2026-03-01

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

**Total: 675 tests, all passing**

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
