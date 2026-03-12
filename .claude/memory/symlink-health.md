---
name: Symlink Health Check Tool
description: Tools page diagnostic that scans broken symlinks, classifies as recoverable/dead, and offers re-queue or cleanup
type: project
---

## Symlink Health Check — 2026-03-12

### Purpose
Diagnose and repair broken symlinks left by the 205-torrent deletion incident. User-initiated tool on the Tools page, NOT automatic.

### Flow
Scan → Preview (with recovery classification) → User selects → Execute (re-queue or cleanup)

### Module: `src/core/symlink_health.py`
- `scan_symlink_health(session)`: queries all symlinks joined with media_items, batch path checks with `Semaphore(20)`, dedup by item_id, mount index lookup for recovery
- `execute_symlink_health(session, request)`: per-item savepoints, re-queue → WANTED via `force_transition`, cleanup → remove symlinks only (item stays DONE)
- `_find_mount_match()`: searches mount_index by normalized title + season/episode (shows) or title + year (movies)

### Endpoints
- `POST /api/tools/symlink-health/scan` → `SymlinkHealthScan`
- `POST /api/tools/symlink-health/execute` → `SymlinkHealthResult`
- Both use `_cleanup_lock` via `_try_acquire`

### Key Bugs Fixed During Development
- `media_type` case mismatch: raw SQL returns "SHOW"/"MOVIE" (uppercase) but `_find_mount_match` compared lowercase → all shows treated as movies, skipping season/episode filters. Fix: `media_type.upper() == "SHOW"`
- `broken` count was raw symlink count, not deduped by item_id → summary said 26 but table had 33 rows. Fix: `scan.broken = len(scan.items)` after dedup

### Frontend
- Post-execute UI: processed items get "Re-queued" (blue) or "Cleaned" (gray) badges, rows fade to 50% opacity, checkboxes removed
- Filter tabs: All / Recoverable / Dead (client-side filtering)
- 48 tests in `tests/test_symlink_health.py`

### Re-queue Safety
Re-queued items go to WANTED → pipeline's mount scan step finds the files immediately (no RD API calls needed) → COMPLETE → symlinks created → DONE
