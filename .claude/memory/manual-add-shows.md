---
name: Manual Add for Shows + Season Pack CHECKING Fixes
description: Manual magnet add form supports shows with own type selector, season pack CHECKING improvements including auto-promote, leading-number episode regex, sample file filtering
type: project
---

Manual add form had hardcoded `media_type: 'movie'` — shows added as magnets were always classified as movies.

**Why:** User added Rurouni Kenshin Season 2 (35 episodes) as manual magnet, got single movie symlink.

**How to apply:** Multiple interrelated fixes:

## Frontend (`search.html`)
- `#manual-type` dropdown (Movie / TV Show) inside manual add form — independent from search form's `#media_type`
- Season + season pack fields only visible when TV Show selected
- Auto-detect "Season N" from title → switches to TV Show, fills season, checks season pack
- Payload reads from `#manual-type`, not search form dropdown

## Backend (`main.py` CHECKING handler)
- **Auto-promote**: show items with `is_season_pack=False` finding multiple distinct episodes auto-promote to season pack (metadata mutations deferred until after symlink success)
- **Sample file filter**: `os.path.basename().lower().startswith("sample")` excluded
- **NULL episode filter**: files with `parsed_episode=None` excluded from dedup
- **media_type auto-correct**: `movie` → `show` when `is_season_pack=True`
- **Relaxed season filter**: `lookup_by_path_prefix(season=None)` when season-filtered query returns nothing
- **Reordered fallbacks**: absolute episode fallback (TMDB-guided) runs BEFORE relaxed `season=None` query
- **Stuck-in-CHECKING fix**: filtered-empty matches fall through to timeout check instead of `continue` looping

## Filename Parsing (`mount_scanner.py`)
- `_LEADING_EP_RE = re.compile(r"^(\d{1,3})(?:\.|(?:\s*[-–]\s))")` — catches "28. Episode Title.mp4" → ep 28
- Last fallback after PTN, `_TV_N_EP_RE`, and `_ANIME_DASH_EP_RE`
- Won't match movies like "12 Monkeys" (space after number) or "1984" (4 digits)
- Edge case: "300 - Rise of an Empire.mp4" would match → ep 300 (only matters in season pack context)

## API (`search.py`)
- `AddRequest.title` now `str | None = None`, MediaItem defaults to `"Unknown"`
- Dedup registration: `filename=body.release_title or body.title or "Unknown"`
