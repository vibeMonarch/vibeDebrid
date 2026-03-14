---
name: Zurg auto-recovery behavior
description: Zurg replaces CDN-dropped files with different RD torrents while keeping mount paths stable — causes hash drift in our DB and affects cleanup safety
type: project
---

Zurg has an auto-recover feature that triggers when RD CDN returns 503 for a file.

**Behavior:**
1. Detects 503 → starts repair process
2. Searches RD instant availability pool for same content (cycles through candidate IDs)
3. Adds a different cached torrent to the RD account (different hash, different rd_id, possibly different release/filename)
4. Maps it to the same Zurg mount path → symlinks and Plex keep working seamlessly
5. May attempt multiple repair cycles per episode (observed 4-5 attempts for Attack on Titan S02)

**Impact on vibeDebrid:**
- Our `rd_torrents` DB table becomes stale (old hash/rd_id no longer matches what's in RD)
- Cleanup tool: replacement torrent appears as ORPHANED (hash/rdid/filename all miss protection checks)
- Bridge tool: matches by filename, but replacement torrent may have a different filename
- This can happen frequently during active viewing

**Why:** Discovered 2026-03-13 while watching Attack on Titan S02. Zurg logs show rapid 503 → repair → redownload cycles with new RD torrent IDs appearing in each attempt.

**How to apply:**
- Cleanup tool (#23) must add live mount verification: check `os.path.exists(source_path)` for symlinks, then PTN title+season match against RD torrents to protect replacements
- Never assume hash/rd_id in our DB matches what's currently in the RD account
- Bridge tool could be enhanced to detect hash drift and re-link, but that's data hygiene, not safety-critical
