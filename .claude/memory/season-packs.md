# Season Pack Implementation — 2026-03-01

## What was built
When a user adds a season pack torrent, all episodes get symlinked (not just the searched episode). Search UI shows Season/Episode columns with a "Full" badge for packs.

## Files changed
- `src/models/media_item.py` — `is_season_pack: Mapped[bool]` column
- `src/database.py` — `_migrate_add_columns()` with ALTER TABLE migration
- `src/api/routes/search.py` — `season`/`episode` on SearchResultItem, `is_season_pack` on AddRequest, episode=None for packs, validation that season is required
- `src/main.py` — CHECKING stage: multi-symlink with per-episode dedup
- `src/core/scrape_pipeline.py` — Guard changed from `season is None or episode is None` to `season is None`, uses episode=1 as Torrentio anchor
- `src/templates/search.html` — Season/Episode columns, data attributes, click handler passes is_season_pack
- `tests/test_season_pack.py` — 25 tests

## Key design decisions
- `episode=None` on MediaItem signals "all episodes" for season packs
- Mount matches deduplicated by `parsed_episode`, picking best per episode:
  1. Exact match to `requested_resolution`
  2. Higher resolution rank (2160p > 1080p > 720p > 480p)
  3. Largest filesize as tie-breaker
- Partial-failure tolerance: individual symlink failures are skipped, only retries if ALL fail
- Torrentio uses episode=1 as anchor when scraping season packs (known limitation: fails if ep1 not indexed)

## Bug found during implementation
- Without per-episode dedup, Fallout S01 produced 40 symlinks (5 releases x 8 episodes) instead of 8
- Root cause: `mount_scanner.lookup(episode=None)` returns all releases for all episodes
