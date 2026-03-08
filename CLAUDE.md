# vibeDebrid

A Real-Debrid media automation system replacing CLI Debrid. Read `SPEC.md` for full requirements.

## Architecture

- Python 3.12+, FastAPI, SQLite (async via aiosqlite), httpx for HTTP
- Async-first: all I/O operations use async/await
- Modular services: each external integration is its own module in `src/services/`
- Core logic in `src/core/`: queue state machine, filtering, dedup, scraping pipeline
- Web UI: Jinja2 templates + htmx (no heavy JS frameworks)
- Scheduling: APScheduler for periodic tasks

## Project Structure

```
src/
  main.py              → FastAPI app, startup/shutdown hooks, scheduler init
  config.py            → Pydantic Settings loading from env + config.json
  database.py          → SQLAlchemy async engine, sessionmaker, model base
  api/routes/          → FastAPI routers (dashboard, queue, search, settings, duplicates)
  core/                → Business logic (queue_manager, scrape_pipeline, filter_engine, dedup, symlink_manager, mount_scanner)
  services/            → External API wrappers (real_debrid, torrentio, zilean, trakt, plex, tmdb)
  models/              → SQLAlchemy ORM models + Pydantic schemas
  templates/           → Jinja2 HTML templates
tests/                 → pytest tests, one file per core module
```

## Coding Conventions

- Type hints on all function signatures and return types
- Pydantic models for all API request/response schemas and configuration
- SQLAlchemy 2.0 style (mapped_column, async session)
- httpx.AsyncClient with explicit timeouts on every external call
- Structured logging via `logging` module, logger per module: `logger = logging.getLogger(__name__)`
- Error handling: never let an external API failure crash the queue — log, transition to appropriate state, continue
- No bare `except:` — always catch specific exceptions
- Docstrings on all public functions (single-line for simple, Google style for complex)
- Tests use pytest + pytest-asyncio, fixtures in conftest.py
- No print statements — use logging
- Always use the project virtualenv: `.venv/bin/python` (not system Python)

## Critical Design Decisions

1. **No blacklist state.** Items go SLEEPING → DORMANT (weekly recheck). User can always retry manually.
2. **Mount scan before scrape.** Always check Zurg mount index before making any API calls.
3. **Dedup before add.** Always check RD account for existing matching torrent before adding.
4. **Host paths only.** All symlinks use absolute host filesystem paths. Never use container-internal paths.
5. **Scraper fallback chain.** Episode query → Season query → Show query. Never assume 0 results = doesn't exist.
6. **Exponential backoff.** Retry schedule: 30min, 1hr, 2hr, 6hr, 12hr, 24hr, then daily, then DORMANT.

## Working Patterns

When implementing a new module:
1. Define Pydantic schemas first (input/output contracts)
2. Write the service/core logic with full error handling
3. Add API routes that use the service
4. Write tests with mocked external calls
5. Update templates if UI changes needed

When fixing a bug:
1. Check scrape_log table for debugging context
2. Write a failing test first
3. Fix the issue
4. Verify the test passes

## Git Policy

- **Never commit or push without explicit user command.** Always stage and present changes for review first.

## Docker Context

- Runs alongside Zurg + rclone in docker-compose
- Needs access to Zurg mount (volume mount with rshared propagation)
- Port 5100 for web UI
- SQLite database stored in a mounted volume for persistence

### Orchestration Rules

**NEVER execute implementation tasks directly.** Instead:
1. Analyze the request and create a plan
2. Delegate execution to the appropriate subagent(s)

### Sub-Agent Routing Rules
Check the .claude/agents descriptions before deciding which agents to route the task. If no fitting agent available, do the planning yourself.

**Parallel dispatch** (ALL conditions must be met):
- 3+ unrelated tasks or independent domains
- No shared state between tasks
- Clear file boundaries with no overlap

**Sequential dispatch** (ANY condition triggers):
- Tasks have dependencies (B needs output from A)
- Shared files or state (merge conflict risk)
- Unclear scope (need to understand before proceeding)
