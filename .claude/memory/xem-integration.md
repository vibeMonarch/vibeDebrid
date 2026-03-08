# XEM Scene Numbering Integration — 2026-03-07

## Problem
Anime numbering mismatch: TMDB says S01E29, torrent sites say S02E01 (e.g., Frieren).
Scrape pipeline passed TMDB numbers directly to Torrentio/Zilean → zero results.

## Architecture
- `src/services/xem.py`: XEM API client (`get_show_mappings`, `get_shows_with_mappings`)
- `src/core/xem_mapper.py`: `XemMapper` — cache-first lookup with 24h TTL
- `src/models/xem_cache.py`: `XemCacheEntry` ORM with UniqueConstraint(tvdb_id, tvdb_season, tvdb_episode)
- Config: `settings.xem` (enabled, base_url, cache_hours, timeout_seconds)

## XEM API Details
- No auth required, simple REST at `https://thexem.info`
- `/map/all?id={tvdb_id}&origin=tvdb` → all TVDB↔scene mappings for a show
- `/map/havemap?origin=tvdb` → list of TVDB IDs with mappings
- No TMDB support — must convert TMDB→TVDB first
- Response: `{"result": "success", "data": [{"tvdb": {season, episode}, "scene": {season, episode}}, ...]}`

## TVDB ID Flow
- `TmdbShowDetail.tvdb_id` parsed from `external_ids` in TMDB `/tv/{id}` response (no extra API call)
- `MediaItem.tvdb_id` column (nullable int) populated by show_manager when creating items
- Fallback: `xem_mapper.get_scene_numbering_for_item()` resolves via `tmdb_client.get_external_ids()` if tvdb_id missing

## Pipeline Integration
- XEM resolved ONCE in `_run_pipeline()` (not per-scraper step)
- `scene_season`/`scene_episode` passed to both `_step_zilean` and `_step_torrentio`
- Only applies to individual episodes (not season packs, not movies)
- XEM failure → uses original TMDB numbers (silent degradation)

## Cache Strategy
- SQLite `xem_cache` table, 24h TTL (configurable via `settings.xem.cache_hours`)
- Stale check: if any entry for tvdb_id has `fetched_at` older than cache_hours, refetch all
- On refresh: DELETE old entries → INSERT new ones (within same session flush)
- UniqueConstraint prevents duplicate rows from concurrent lookups

## Known Limitations
- `search.py:add_torrent()` and `discover.py:add` don't set tvdb_id — mapper falls back to TMDB resolution (extra API call per scrape)
- XEM mapper holds DB session during HTTP call to XEM (up to 10s) — acceptable for SQLite but noted
- XEM cache rollback: if scrape pipeline rolls back session, cache entries are also lost (re-fetched next time)

## Scene Season Restructuring (Show Detail Page) — 2026-03-07

### Problem
Show detail page used TMDB seasons (1 season, 35 episodes for Frieren).
Adding created 35 individual items → 35 scrape requests → RD rate limits.

### Solution: XEM-Aware Season Grouping
- `xem_mapper.get_all_scene_mappings()`: returns full `{(tvdb_s,tvdb_e) → (scene_s,scene_e)}` dict
- `_ensure_cached_entries()`: refactored shared helper for cache management
- `show_manager._derive_scene_seasons()`: re-groups TMDB episodes by scene season
  - Fetches `get_season_details()` per TMDB season (N calls, cached by XEM 24h TTL)
  - Identity fallback: unmapped episodes keep their TVDB season/episode
  - Returns `list[SceneSeasonGroup]` or None (no XEM → TMDB fallback)

### Data Flow
- `get_show_detail()`: if tvdb_id + XEM enabled → scene seasons in response (xem_mapped=True)
- `add_seasons()`: scene season numbers in request, re-derives groups
  - Complete scene season → 1 season pack with scene season number
  - Airing scene season → `_add_xem_airing_season()` → individual items with TMDB numbering
- Season packs: `season=scene_num` (scraper doesn't XEM-remap packs → correct torrent search)
- Individual episodes: `season=tmdb_season, episode=tmdb_episode` (pipeline XEM-remaps at scrape time)

### Models
- `SceneEpisodeInfo`: tmdb_season, tmdb_episode, scene_episode, air_date, has_aired
- `SceneSeasonGroup`: scene_season, episodes, total/aired counts, is_complete
- `SeasonInfo.xem_mapped: bool` — indicates scene-restructured season

### Monitoring Stamping
- XEM airing seasons auto-subscribe + stamp `last_season` with TMDB season (not scene)
- `airing_max_episode` uses `max()` across multiple airing scene seasons

### Frontend
- "Scene numbering" chip next to Seasons heading (hidden when no XEM data)

### Review Fixes Applied
- `item.episode is None` guard in queue item matching (Critical #2)
- `last_season` stamped for XEM airing path (Critical #1)
- `max()` for `airing_max_episode` across multiple scene seasons (High #4)
- try/except around XEM mapper call in `_derive_scene_seasons` (Medium #6)

### Known Limitations (from review, accepted)
- N TMDB API calls per `_derive_scene_seasons()` — mitigated by XEM cache; consider TMDB response cache
- AIRING status persists even when all aired episodes in library (by design for episode-adding UX)
- Pack cutoff doesn't find pre-XEM TMDB packs (edge case, downstream dedup catches it)
- Scene season names always "Season N" — original TMDB names lost (acceptable tradeoff)

## Tests: 59
- 14 client (HTTP mock, error handling, disabled guard, malformed responses)
- 16 mapper (cache hit/miss/stale, TVDB resolution, get_all_scene_mappings, shared cache)
- 6 pipeline integration (scene number passthrough, fallback, season pack skip)
- 23 show_manager (derive scene seasons, get_show_detail XEM, add_seasons XEM, dedup, subscribe)
