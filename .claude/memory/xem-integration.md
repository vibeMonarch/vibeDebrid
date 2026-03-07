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

## Tests: 31
- 14 client (HTTP mock, error handling, disabled guard, malformed responses)
- 11 mapper (cache hit/miss/stale, TVDB resolution, edge cases)
- 6 pipeline integration (scene number passthrough, fallback, season pack skip)
