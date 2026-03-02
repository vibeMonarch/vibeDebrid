# vibeDebrid — Memory

## Project State
- 697 tests, all passing (as of 2026-03-01)
- Python 3.14, FastAPI, SQLite async, htmx frontend
- Test runner: `.venv/bin/python -m pytest tests/ -q`

## Completed Features
- [Symlink Naming Convention](symlink-naming.md) — 2026-03-01
- [Season Pack Support](season-packs.md) — 2026-03-01
- Seeders display removal — 2026-03-02

## STATUS.md Next Steps
Pending features:
- Manual "RD Status" button in search results (per-torrent cache check)

## Agent Routing Patterns
- Backend changes (models, routes, pipeline): backend-dev (sequential, shared state)
- Frontend (templates, JS): frontend-dev (after backend, needs new JSON fields)
- Tests: test-writer (after implementation)
- Review: code-reviewer (final pass, findings only)
- Always run code-reviewer after implementation — it catches real bugs

## Key Conventions
- Commit style: imperative summary, bullet details, `Co-Authored-By: Claude Opus 4.6`
- DB migrations: add to `_migrate_add_columns()` in `src/database.py`, catch specific errors
- State transitions: use `queue_manager.transition()`, never direct `item.state =` assignment
- `add_torrent()` in search.py bypasses queue_manager for state changes (pre-existing tech debt)
