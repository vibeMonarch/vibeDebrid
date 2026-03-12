---
name: Reverse containment mount lookup (Issue #30)
description: Bidirectional title matching in mount_scanner.lookup() and symlink_health._find_mount_match() — fixes anime long TMDB titles vs short torrent parsed_titles
type: project
---

Issue #30: mount scanner lookup failed when MediaItem title (from TMDB) was longer than torrent's parsed_title in mount index. Common with anime where TMDB uses full Japanese titles but torrents use shortened versions (e.g. "Honzuki no Gekokujou Shisho ni Naru Tame..." vs "honzuki no gekokujou").

**Why:** LIKE '%pattern%' only checks if DB value contains the search term, not the reverse. When search term is longer, no match is found → unnecessary scraping + RD API calls.

**How to apply:**
- `mount_scanner.lookup()` now has 3 tiers: exact → forward word-subsequence → reverse containment via `func.instr(literal(normalized), parsed_title) > 0`
- `symlink_health._find_mount_match()` has 2 phases: forward LIKE → reverse containment
- Both use 3-word minimum guard on `parsed_title` to prevent overly broad matches
- Both use `_is_word_subsequence(db_words, search_words)` (reversed args) for Python verification
- Reverse queries bounded with `.limit(50)`
- 9 new tests (7 mount_scanner + 2 symlink_health)
