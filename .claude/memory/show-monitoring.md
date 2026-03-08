# Show Detail Page, Monitoring & Airing Seasons

## Show Detail Page + Monitoring — 2026-03-07
- `src/models/monitored_show.py`: `MonitoredShow` ORM — tmdb_id (unique, indexed), imdb_id, title, year, quality_profile, enabled, last_season, last_episode, last_checked_at
- `src/core/show_manager.py`: `ShowManager` singleton — `get_show_detail()`, `add_seasons()`, `set_subscription()`, `check_monitored_shows()`
- `src/api/routes/show.py`: GET `/api/show/{tmdb_id}`, POST `/api/show/add`, PUT `/api/show/{tmdb_id}/subscribe`
- `src/templates/show.html`: season picker with status badges, subscribe toggle, quality dropdown, sticky add bar
- `src/services/tmdb.py`: `get_show_details()` (uses `append_to_response=external_ids`), `get_season_details()`
- Discover click flow: TV shows → `/show/{tmdb_id}?from=discover` (no IMDB resolve needed), movies → `/search` (unchanged)
- Season selection: completed seasons → season pack, airing seasons → per-episode items
- Subscribe: MonitoredShow record, 6h scheduler job checks TMDB for new seasons/episodes
- Monitoring logic: new complete seasons → season pack, current season new episodes → per-episode items
- `_parse_air_date()` helper guards against malformed TMDB air_date strings
- `date.today()` replaced with `datetime.now(timezone.utc).date()` for UTC consistency
- 108 tests (76 show_manager + 32 API routes)

## Airing Season Support — 2026-03-07
- `TmdbEpisodeAirInfo` model: `season_number`, `episode_number`, `air_date`
- `TmdbShowDetail`: new `next_episode_to_air`, `last_episode_to_air` fields (parsed from existing `/tv/{id}` response, no extra API call)
- `SeasonStatus.AIRING`: detected when `next_episode_to_air.season_number == season_number`
- `get_show_detail()`: for AIRING season, fetches `get_season_details()` (1 extra TMDB call) to count actual `aired_episodes`
- `add_seasons()`: airing season → `_add_airing_season()` creates per-episode items (WANTED for aired, UNRELEASED for future); completed season → season pack (unchanged)
- `_add_airing_season()` returns `(created_episodes, created_unreleased, max_aired_episode)` tuple
- Auto-subscribe: adding any airing season auto-enables MonitoredShow subscription
- MonitoredShow stamping: `last_season` and `last_episode` set on auto-subscribe to prevent `check_monitored_shows` duplicate creation
- Belt-and-suspenders: `_check_single_show()` also checks for existing episode items (not just season packs) before creating a season pack
- `AddSeasonsResult`: new `created_episodes` and `created_unreleased` fields
- Frontend: sky-blue `.status-badge-airing` with pulsing dot, "X of Y episodes aired" display, "Select available" includes airing seasons
- Toast: "Added X episodes + Y upcoming to queue" for airing seasons
- 23 new tests (airing detection, per-episode creation, auto-subscribe, monitoring dedup, schema)

## Airing Status Override — 2026-03-07
- AIRING status takes PRIORITY over IN_QUEUE/IN_LIBRARY in `get_show_detail()` — users can always add remaining episodes
- `add_seasons()` bypasses season-level skip for airing seasons (e.g., existing season pack doesn't block adding new episodes)
- Season pack cutoff: `_add_airing_season()` finds COMPLETE/DONE season packs, uses `state_changed_at` as cutoff date
- Episodes aired before cutoff are skipped (covered by pack), episodes after cutoff are created as WANTED
- `_add_airing_season()` signature extended with `existing_items` param to inspect pack state
- 4 new tests (airing override, pack cutoff, non-complete pack, cutoff day inclusivity)

## Anime Numbering — Solved via XEM (2026-03-07)
- TMDB may use continuous numbering (S01E29) while torrent sites use broadcast seasons (S02E01)
- Solved: [XEM scene numbering integration](xem-integration.md) maps TVDB↔scene numbers
- Scrape pipeline resolves XEM once in `_run_pipeline()`, passes scene numbers to both scrapers
- Show detail page: XEM-aware season restructuring shows "real" scene seasons instead of TMDB seasons
- Complete scene seasons → 1 season pack; airing scene seasons → per-episode items (dramatically reduces RD API calls)
