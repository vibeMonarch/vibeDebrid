# Symlink Naming Convention — 2026-03-01

## What was built
Configurable timestamp prefix + toggles for year/resolution in symlink folder names. Movies get timestamp on folder; shows get timestamp on show folder (first episode, persisted) and per-episode filename prefix. Settings UI with live preview.

## Files changed
- `src/config.py` — `SymlinkNamingConfig(date_prefix=True, release_year=True, resolution=False)` added to Settings
- `config.example.json` — `symlink_naming` section with defaults
- `src/core/symlink_manager.py` — `_format_timestamp()`, `_find_existing_show_dir()`, updated `build_movie_dir/build_show_dir/create_symlink`
- `src/templates/settings.html` — Symlink Naming card with 3 toggles + `updateNamingPreview()` live preview
- `tests/test_symlink_manager.py` — 22 new tests, 22 existing tests fixed to mock `symlink_naming`

## Key design decisions
- `_find_existing_show_dir` strips timestamp prefix (12 digits) and resolution suffix before exact comparison — prevents "The Matrix" matching "The Matrix Reloaded"
- `build_show_dir` wrapped in `asyncio.to_thread` since `_find_existing_show_dir` does blocking `os.listdir`
- `_format_timestamp()` uses local time intentionally (user-facing directory names)
- Movie files never get timestamp prefix; episode files do (for per-file dating)
- Season dir stays `Season XX` always — no timestamp, no resolution

## Code review findings addressed
- Blocking I/O: wrapped `build_show_dir` call in `asyncio.to_thread`
- Substring matching: switched from `in` to exact match after stripping decorations
- Existing tests: added `mock_settings.symlink_naming = _NAMING_NO_PREFIX` to 22 tests
- Removed dead `_make_settings_patch` helper
