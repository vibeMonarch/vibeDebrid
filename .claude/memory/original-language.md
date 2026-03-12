---
name: Original Language Preference
description: Prefer original language (Japanese, Korean, etc.) over dubs for anime and foreign content using TMDB metadata
type: project
---

## Original Language Preference — 2026-03-11

Issue #28. Automatically prefers original-language releases for non-English content.

### How it works
- `MediaItem.original_language` stores ISO 639-1 code from TMDB (e.g., "ja", "ko")
- `FiltersConfig.prefer_original_language: bool = False` — global opt-in
- `FiltersConfig.dub_penalty: int = 20`, `dual_audio_bonus: int = 10`
- `_score_original_language()` in filter_engine.py: +15 original lang, +25 dual audio, -30 dubbed, -10 untagged
- English-original content completely bypassed (returns 0.0)

### DUB/DUAL AUDIO detection
- `_DUB_RE = re.compile(r"\bDUB(?:BED)?\b", re.IGNORECASE)` — matches DUB, DUBBED
- `_DUAL_AUDIO_RE = re.compile(r"\bDUAL[\.\s-]?AUDIO\b", re.IGNORECASE)` — no standalone DUAL (false positives)
- Added to both torrentio.py and zilean.py `_parse_languages()`
- Tokens added to `languages` list as "Dubbed" and "Dual Audio"

### Data population
- Discover add: `AddToQueueRequest.original_language`, frontend passes from TMDB data
- Search add: `AddRequest.original_language`, forwarded from discover via URL param
- Show manager: threaded from `TmdbShowDetail.original_language`
- Plex watchlist: fetched via `get_movie_details()`/`get_show_details()`
- Season pack split: copied from parent item
- Pipeline backfill: lazy TMDB lookup when missing (only when feature enabled)

### Re-scrape endpoint
- `POST /api/queue/{id}/rescrape-original` — sets `force_original_language` in metadata_json
- Pipeline doubles penalty/bonus when flag is set, then clears it
- Frontend: button in queue detail panel, visible for non-English items in COMPLETE/DONE/SLEEPING/DORMANT

### TMDB integration
- `TmdbItem.original_language` and `TmdbShowDetail.original_language` parsed from API responses
- `ISO_639_1_TO_LANGUAGE`: 11-language mapping (en, ja, ko, zh, fr, de, es, pt, it, nl, ru)
- `iso_to_language_name()` converts ISO code to torrent-matching name
- `get_movie_details()` added to TmdbClient for movie metadata lookup

### UI
- Settings: toggle + dub_penalty/dual_audio_bonus numeric inputs in Filters section
- Discover: JA/KO badge on non-English content cards
- Queue detail: "Original Language: Japanese" display + "Re-scrape (Original Language)" button

### Test coverage
- 87 tests in `tests/test_original_language.py`
- Covers: regex detection, ISO mapping, TMDB parsing, scoring, filter_and_rank integration, rescrape endpoint, data population

### Verified with real data
- Cowboy Bebop S01: BluRay with JP+EN audio scored 114 (+15 OL bonus), untagged release scored 80 (-10 penalty)
