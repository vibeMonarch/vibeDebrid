# Migration Tool — Implementation Plan

## Overview
A "Tools" page with a "Library Migration" sub-tool that scans an external library directory,
imports found media into vibeDebrid's DB as COMPLETE, deduplicates with existing vibeDebrid
items, moves unique vibeDebrid symlinks to the new location, and updates config paths.

System-agnostic: works with any library (CLI Debrid, manual folders, other tools).
No dependency on external databases — filesystem scan only.

## Navigation
- Add "Tools" link in sidebar after "Settings" (before the bottom health indicator)
- `active_page = "tools"`
- SVG icon: wrench/tool icon

## Files to Create
1. `src/core/migration.py` — core scanning, parsing, dedup, execution logic
2. `src/api/routes/tools.py` — API endpoints + page render
3. `src/templates/tools.html` — UI template

## Files to Modify
1. `src/templates/base.html` — add Tools nav link after Settings
2. `src/main.py` — import and include_router for tools_router

---

## Phase 1: Backend (`backend-dev` agent)

### 1a. `src/core/migration.py`

**Title/Year Parsing** — `parse_media_name(name: str) -> tuple[str, int | None]`:
- Pattern 1: `Title (Year)` or `Title (Year) [extras]` — most common (Plex, CLI Debrid)
- Pattern 2: `Title [Year]`
- Pattern 3: `Title.Year.Resolution...` or `Title Year Resolution...` (scene naming)
  - Year must be 1900-2099
  - Stop at first resolution token (1080p, 2160p, 720p, etc.) or codec/source token
- Fallback: entire name as title, year=None
- Strip trailing resolution/codec/source info
- Replace dots with spaces for scene names

**IMDB Extraction** — `extract_imdb_id(filename: str) -> str | None`:
- Regex for `tt\d{7,}` anywhere in the filename string
- CLI Debrid format: `Title (Year) - ttXXXXXXX - Resolution - (Release).ext`

**Episode Parsing** — `parse_episode_info(filename: str) -> tuple[int | None, int | None]`:
- Regex for `S(\d{1,2})E(\d{1,3})` (case-insensitive)
- Returns (season, episode) or (None, None)

**Season Dir Parsing** — `parse_season_number(dirname: str) -> int | None`:
- Regex for `Season\s*(\d{1,2})` (case-insensitive)

**Scan Functions:**

```python
@dataclass
class FoundItem:
    title: str
    year: int | None
    media_type: str  # "movie" or "show"
    season: int | None
    episode: int | None
    imdb_id: str | None
    source_path: str | None  # readlink target (if symlink)
    target_path: str  # the symlink/file path itself
    is_symlink: bool
    resolution: str | None

@dataclass
class MigrationPreview:
    found_items: list[FoundItem]  # items at target location
    existing_items: list[dict]  # current vibeDebrid items (id, title, year, etc.)
    duplicates: list[dict]  # {found: FoundItem, existing_id: int, match_reason: str}
    to_move: list[dict]  # vibeDebrid items not at target (need relocation)
    errors: list[str]  # unparseable items

@dataclass
class MigrationResult:
    imported: int
    moved: int
    duplicates_removed: int
    errors: list[str]
```

`async def scan_library(movies_path: str, shows_path: str) -> list[FoundItem]`:
- Walk `movies_path`: each subdir = a movie
  - Parse dir name for title + year
  - List files inside, for each video file:
    - Check if symlink (os.path.islink) → readlink for source
    - Extract IMDB ID from filename
    - Extract resolution from filename
    - Create FoundItem(media_type="movie")
- Walk `shows_path`: each subdir = a show
  - Parse dir name for title + year
  - List subdirs, check for `Season XX` pattern
  - Inside season dirs, list video files
  - For each: parse SXXEXX, check symlink, extract IMDB
  - Create FoundItem(media_type="show")
- All filesystem I/O via `asyncio.to_thread`
- Video file detection: extensions set {".mkv", ".mp4", ".avi", ".m4v", ".ts", ".wmv"}

`async def preview_migration(session, movies_path, shows_path) -> MigrationPreview`:
- Call scan_library to get found_items
- Query DB for all current MediaItems (any state)
- Duplicate detection:
  - Primary: match by symlink source_path (same Zurg file = definite dup)
  - Secondary: match by normalized title + year + media_type + season + episode
- Separate vibeDebrid items into "duplicates" and "to_move" (unique items needing relocation)
- Return MigrationPreview

`async def execute_migration(session, movies_path, shows_path, items_to_import, items_to_move, items_to_remove) -> MigrationResult`:
- **Import**: For each FoundItem to import:
  - Create MediaItem (state=COMPLETE, source="migration", ...)
  - If is_symlink: create Symlink record (source_path, target_path, valid=True)
- **Move**: For each vibeDebrid item to move:
  - Determine new path at target location (use vibeDebrid naming convention)
  - Create new symlink at new location
  - Delete old symlink from filesystem
  - Update Symlink DB record paths
- **Remove duplicates**: For each duplicate:
  - Delete vibeDebrid's symlink from filesystem
  - Delete Symlink DB record
  - Delete MediaItem DB record (or update to reference the imported one)
- Update config.json paths
- Return MigrationResult

### 1b. `src/api/routes/tools.py`

```python
router = APIRouter()

# Page render
GET /tools → render tools.html (active_page="tools")

# Pydantic schemas
class MigrationPreviewRequest(BaseModel):
    movies_path: str
    shows_path: str

class MigrationExecuteRequest(BaseModel):
    movies_path: str
    shows_path: str
    import_ids: list[int]  # indices into preview's found_items to import
    move_ids: list[int]    # vibeDebrid item IDs to move
    remove_ids: list[int]  # vibeDebrid item IDs to remove (duplicates)

# API endpoints
POST /api/tools/migration/preview → MigrationPreview (JSON)
POST /api/tools/migration/execute → MigrationResult (JSON)
```

### 1c. Wire into app
- `src/main.py`: import tools_router, include_router with prefix="/api/tools"
  - Page route (GET /tools) is at the router level, no prefix needed
  - Actually: use same pattern as dashboard — page render at GET /tools on the router,
    API endpoints at POST /api/tools/migration/...
  - Include router as: `app.include_router(tools_router)` (no prefix, routes define their own paths)
- `src/templates/base.html`: add Tools nav link after Settings link (line ~105)

---

## Phase 2: Frontend (`frontend-dev` agent)

### `src/templates/tools.html`

Extends base.html. Structure:

```
Tools (page header, sticky)
├── Migration Card (collapsible section)
│   ├── Description text
│   ├── Path inputs (movies + shows) with current config paths as defaults
│   ├── "Scan & Preview" button
│   ├── Preview Results (hidden until scan)
│   │   ├── Summary stats bar (found, duplicates, to-move, errors)
│   │   ├── Found Items table (title, year, type, episodes, IMDB, symlink status)
│   │   ├── Duplicates table (vibeDebrid item vs found item, match reason)
│   │   ├── Items to Move table (vibeDebrid items that will be relocated)
│   │   ├── Errors list (unparseable items)
│   │   └── "Execute Migration" button (with confirmation)
│   └── Result Summary (after execution)
└── (future tools sections)
```

**UX Flow:**
1. User enters paths (pre-filled with current config if sensible, or empty)
2. Clicks "Scan & Preview" → POST /api/tools/migration/preview
3. Loading spinner while scanning
4. Results appear: summary cards + expandable tables
5. User reviews, can uncheck items they don't want to import
6. Clicks "Execute Migration" → confirmation modal
7. POST /api/tools/migration/execute with selected items
8. Success summary + "paths updated" notification

**Styling:** Match existing card/table patterns from settings.html and queue.html.
Use `.page-header` for sticky header. Dark mode, responsive.

---

## Phase 3: Tests (`test-writer` agent)

### `tests/test_migration.py`

**Parsing tests** (~20 tests):
- `parse_media_name` with various formats:
  - "Casino (1995)" → ("Casino", 1995)
  - "2001.A.Space.Odyssey.1968.1080p.BluRay" → ("2001 A Space Odyssey", 1968)
  - "Back To The Future Trilogy" → ("Back To The Future Trilogy", None)
  - "1917 (2019) [1080p]" → ("1917", 2019)
  - "Anora 2024 1080p WEB-DL" → ("Anora", 2024)
  - Unicode titles, titles starting with numbers
- `extract_imdb_id`:
  - CLI Debrid format: "Title - tt1234567 - 1080p" → "tt1234567"
  - No IMDB: "Title.mkv" → None
- `parse_episode_info`:
  - "Show.S01E05.mkv" → (1, 5)
  - "Show_S02E10_Title.mkv" → (2, 10)
  - "Movie.mkv" → (None, None)
- `parse_season_number`:
  - "Season 01" → 1
  - "Season 2" → 2
  - "Specials" → None

**Scan tests** (~10 tests):
- Mock filesystem with tmp_path fixture
- Create directory structures (movies + shows) with real symlinks and files
- Verify FoundItem fields are correct
- Test with empty directories, nested structures, mixed content

**Preview tests** (~8 tests):
- Mock scan results + DB with existing items
- Verify duplicate detection (by source_path, by title+year)
- Verify to_move identification
- Edge case: no duplicates, all duplicates

**Execute tests** (~8 tests):
- Mock filesystem operations
- Verify MediaItem + Symlink records created
- Verify old symlinks removed for duplicates
- Verify config update
- Error handling: permission denied, broken symlinks

**API route tests** (~6 tests):
- POST /api/tools/migration/preview with valid paths
- POST /api/tools/migration/preview with nonexistent path
- POST /api/tools/migration/execute happy path
- GET /tools returns HTML

---

## Phase 4: Code Review (`code-reviewer` agent)

Review these files:
1. `src/core/migration.py`
2. `src/api/routes/tools.py`
3. `src/templates/tools.html`

Focus areas:
- Path traversal safety (user provides arbitrary paths)
- Async correctness (filesystem I/O in to_thread)
- Transaction safety (bulk imports shouldn't leave partial state on error)
- Race conditions (concurrent access to config.json)
- Duplicate detection accuracy (false positives/negatives)

---

## Agent Dispatch Order

1. **backend-dev** (sequential) — creates migration.py, tools.py, modifies main.py + base.html
2. **frontend-dev** (sequential, after backend) — creates tools.html using the API contract
3. **test-writer** (after backend+frontend) — writes tests
4. **code-reviewer** (final) — reviews all new files

Sequential dispatch required: shared files (base.html), API contract dependency.

---

## Key Design Decisions

1. **No external DB dependency** — filesystem scan only (works with any library tool)
2. **Don't rename existing files** — import as-is, only vibeDebrid's own items get moved
3. **Preview before execute** — user sees exactly what will happen
4. **Duplicate preference** — keep the item at the target location, remove vibeDebrid's copy
5. **Config update last** — only after successful migration
6. **source="migration"** on imported MediaItems — distinguishes from items vibeDebrid found itself
7. **Real files (non-symlinks)** — create MediaItem only, no Symlink record
8. **COMPLETE state** for imported items — they exist and are available
