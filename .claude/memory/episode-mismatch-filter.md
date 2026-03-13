---
name: Episode Mismatch Filter + Retry Cleanup
description: Tier 1 hard filter rejects wrong episode numbers, retry cleans up stale dedup/symlinks, anime parsing fallbacks for ordinal season and bare dash notation
type: project
---

## Problem
Frieren S01E36 (TMDB) → XEM mapped to S02E08 → Torrentio step 1 returned 0 results → step 2 (season fallback) returned 50 S02 results → filter ranked by quality only → S02E01 torrent selected (wrong episode).

## Three Fixes (2026-03-13)

### 1. Episode Mismatch Filter (`filter_engine.py`)
- `requested_season` + `requested_episode` params on `filter_and_rank()`, `get_best()`, `_apply_hard_filters()`
- Tier 1 check #2 (after season pack gate, before size check):
  - Only fires when `not prefer_season_packs` AND both requested values are set
  - `result.season is not None` and differs → rejected
  - `result.episode is not None` and differs → rejected
  - `result.season/episode is None` → passes (PTN parse failure, benefit of doubt)
- Pipeline passes scene_season/scene_episode for show episodes (not season packs, not movies)
- Search endpoint doesn't pass them (shows all results to user)

### 2. Retry Cleanup (`queue_manager.py`)
- `force_transition()` from DONE/COMPLETE → WANTED now:
  - Marks linked ACTIVE RdTorrent entries as REMOVED (breaks dedup short-circuit)
  - Removes symlinks via `symlink_manager.remove_symlink()`
- Without this, retry looped: dedup found old hash → CHECKING → wrong file still in mount → COMPLETE

### 3. Anime Parsing Fallbacks (`torrentio.py`, `zilean.py`)
- `_ORDINAL_SEASON_EP_RE`: `(\d+)(?:st|nd|rd|th)\s+Season\s*[-–]\s*(\d{1,3})\b` → captures season + episode
  - Matches: "2nd Season - 01", "1st Season - 28"
- `_ANIME_BARE_DASH_EP_RE`: `\s-\s(\d{1,3})(?:\s|$|\[|\()` → captures episode only
  - Matches: "[Group] Title - 29 [1080p]"
- Both added as fallbacks after `_SEASON_DASH_EP_RE`, before season pack detection
- Parsing chain: PTN → `_SEASON_DASH_EP_RE` → `_ORDINAL_SEASON_EP_RE` → `_ANIME_BARE_DASH_EP_RE` → season pack check

## Edge Cases
- Absolute vs scene numbering: bare dash `- 36` parsed as episode=36, rejected if requested episode=8. Safe because Torrentio step 1 would have returned it if it matched the IMDB episode ID.
- Season packs: filter skipped entirely (`prefer_season_packs=True`)
- Movies: `requested_season`/`requested_episode` are None → filter inactive
