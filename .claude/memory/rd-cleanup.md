# RD Bridge + Smart Cleanup — 2026-03-09

## Overview
Three-phase cleanup for migration duplicates: Bridge → Preview → Execute.
Replaces the naive "lowest ID wins" dedup with liveness-aware keeper selection.

## Phase 1: RD Bridge (`src/core/rd_bridge.py`)
- `bridge_rd_torrents(session)` — matches RD account torrents to migrated items
- Compares symlink `source_path` dir name → RD torrent `filename` (exact, then normalized)
- Creates `RdTorrent` records via `dedup_engine.register_torrent()`
- Tracks `known_hashes` (DB) + `seen_hashes` (this run) to avoid overwriting season pack FKs
- First item per hash → `matched`, subsequent → `already_bridged`
- `list_all_torrents()` on RealDebridClient: paginated (2500/page, max 10 pages)
- 46 tests in `tests/test_rd_bridge.py`

## Phase 2: Smart Cleanup (`src/core/cleanup.py`)

### Liveness Assessment
- `assess_migration_items(session)` — finds duplicate groups, fetches ALL items per group
- Batch `os.path.exists` via `asyncio.to_thread` (batches of 50)
- Classification: `LIVE` (source exists) > `BRIDGED` (has RdTorrent) > `DEAD` (neither)
- Indirect RD detection: items without direct `rd_torrents` FK checked via source dir → filename match

### Keeper Selection (`_select_keeper`)
1. LIVE > BRIDGED > DEAD
2. Non-migration source preferred (queue items beat migration)
3. Tiebreaker: lowest ID
- Non-migration items can WIN but are NEVER placed in remove list

### Preview (`build_cleanup_preview`)
- Groups by (imdb_id, season, episode), NULL imdb_id skipped with warning
- RD protection: `rd_ids_to_delete` = hashes on removed items NOT IN any kept item's hashes
- `CleanupPreview` schema with full liveness data per item

### Execute (`execute_cleanup`)
- Savepoint wraps all 4 DELETEs (scrape_log → rd_torrents → symlinks → media_items)
- Broken symlinks removed from disk + empty parent dir cleanup
- Route re-derives preview server-side (ignores client data for safety)
- RD deletion: `Semaphore(5)` + `stop_flag` inside semaphore on 429

## API Endpoints
- `POST /api/tools/bridge-rd` → BridgeResult
- `GET /api/tools/cleanup/preview` → CleanupPreview
- `POST /api/tools/cleanup/execute` → CleanupResult (body: `{confirm: true}`)
- Old endpoints (`GET /api/tools/duplicates`, `POST /api/tools/deduplicate`) still exist

## UI (Tools page, Data Cleanup card)
- Bridge section: button + "Scanning RD account..." status + 6-stat result grid
- Smart Cleanup section: preview button → collapsible group accordion with liveness badges
  - Green = LIVE, Amber = BRIDGED, Red = DEAD
  - RD impact summary, warnings panel
  - Execute with confirmation mentioning RD deletion

## Real Data Results (2026-03-09)
- 433 RD torrents, 1358 migration items, 183 bridged, 1175 unmatched (torrents expired)
- 139 duplicate groups, 273 items to remove, 19 RD torrents to delete
- 323 LIVE, 0 BRIDGED, 89 DEAD assessed items
- All-dead groups: 5 (Rurouni Kenshin — unresolvable IMDB ID)
- Smoke tested: JJK keeps Anime Time (only LIVE), Fallout keeps AOC 2160p (all LIVE, lowest ID)

## Key Design Decisions
- Execute endpoint re-runs assessment server-side (no trust of client preview data)
- Savepoint for atomic DELETE (all-or-nothing, no partial DB corruption)
- `RdTorrent.media_item_id` is one-to-one but season packs are many-to-one; bridge only registers first item per hash
- `_cleanup_lock` in route provides mutual exclusion across bridge/backfill/cleanup operations

## Phase 3: RD Account Categorize & Clean (`src/core/rd_cleanup.py`) — 2026-03-09

### Categorization (priority order)
1. **Protected**: info_hash in `rd_torrents` (ACTIVE) OR rd_id in registry OR filename matches symlink source_path
2. **Dead**: RD status in {error, virus, dead, magnet_error}
3. **Stale**: downloading/magnet_conversion for >7 days (`_STALE_THRESHOLD_DAYS`)
4. **Duplicate**: PTN group has a Protected member → non-protected members are dupes
5. **Orphaned**: default bucket (no local reference at all)

### Two-Pass Design
- Pass 1: collect all Protected rd_ids (needed for Duplicate detection)
- Pass 2: full categorization using the protected set

### Module Cache
- `_last_scan_cache`: stores RD torrent list + category_map + hash_map
- 5-minute TTL (`_SCAN_TTL_SECONDS = 300`)
- Invalidated after any successful deletion in execute

### API Endpoints
- `POST /api/tools/rd-cleanup/scan` → `RdCleanupScan`
- `POST /api/tools/rd-cleanup/execute` → `RdCleanupExecuteResult`
- Both use `_cleanup_lock` (shared with Phase 1 bridge/cleanup)
- Error responses use `HTTPException(detail=...)` for proper frontend display

### UI (Tools page, RD Account Cleanup card)
- Scan button → 5 category summary badges (count + human-readable size)
- Category filter tabs (All/Dead/Stale/Orphaned/Duplicate/Protected) — client-side filtering
- Torrent table with checkboxes (Protected rows have no checkbox, green tint)
- Quick-select: "Select All Dead & Stale" / "Select All Orphaned" / "Deselect All"
- Live selection counter with byte total
- "Remove Selected" with confirmation dialog
- Execute result: stat cards + rate-limit warning + error list

### Schemas
- `RdCleanupExecuteRequest.rd_ids: list[str] = Field(max_length=2500)`
- `CategorizedTorrent`: rd_id, info_hash, filename, filesize, status, category, reason, protected_by, parsed_title

### Key Design Decisions
- No auto-delete — all deletion is user-initiated
- Execute re-validates categories server-side (cached or fresh)
- Sequential deletion loop (replaced Semaphore(5)+gather after rate-limit blast radius incident)
- `mark_torrent_removed()` for each deleted torrent in local registry
- Imports `_extract_mount_relative_name` and `_normalize_name` from `rd_bridge.py`

## Incident: Mount Path Mismatch (2026-03-09)
- **Root cause**: `_build_protection_sets` only checked `zurg_mount` prefix; symlinks via `rclone_RD/__all__/` invisible
- **Impact**: 205 RD torrents deleted, 984 broken symlinks across 46 titles
- **Fix applied**: `_extract_mount_name_any_base()` fallback finds `/__all__/` anywhere in path
- **Recovery**: items → WANTED → season pack consolidation → processed by pipeline
- **Remaining (Issue #29)**: same bug in `rd_bridge.py:287` and `cleanup.py:464`, plus safety hardening (empty protection set should hard-fail, stale cache risk, gather blast radius in tools.py)

## Tests
- 46 tests in `tests/test_rd_bridge.py`
- 51 tests in `tests/test_cleanup.py`
- 84 tests in `tests/test_rd_cleanup.py` (75 + 9 for mount fallback)
