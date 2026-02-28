# vibeDebrid — Claude Code Quick Start

## Getting Started

```bash
# Clone/copy the repo to your server
cd ~/Projects/vibeDebrid

# Start Claude Code (use Opus for the main session)
claude --model opus
```

Claude Code will automatically read `CLAUDE.md` and discover all agents in `.claude/agents/`.

## Agents Overview

| Agent | Model | Role | Access |
|-------|-------|------|--------|
| **architect** | Opus | Design decisions, module interfaces, schema reviews | Read-only |
| **backend-dev** | Sonnet | Core logic, API routes, database, config | Full write |
| **integrations-dev** | Sonnet | External service wrappers (RD, Torrentio, Zilean, etc.) | Full write |
| **frontend-dev** | Sonnet | Web UI templates, htmx, styling | Full write |
| **test-writer** | Sonnet | Tests for all modules | Full write |
| **code-reviewer** | Opus | Quality gate, bug finding | Read-only |

## Cost Optimization

Set subagent model to Sonnet to save tokens on focused implementation tasks:
```bash
export CLAUDE_CODE_SUBAGENT_MODEL="claude-sonnet-4-5-20250929"
```

The architect and code-reviewer agents override this with `model: opus` in their definitions because they need stronger reasoning.

## Recommended Build Sequence

### Phase 1: Foundation
```
You: "Read SPEC.md. Use the architect agent to review the project structure 
      and confirm the design. Then use the backend-dev agent to scaffold the 
      project: pyproject.toml, requirements.txt, src/ directory structure, 
      database models, and config loading."
```

### Phase 2: Core Services
```
You: "Use the integrations-dev agent to implement src/services/real_debrid.py — 
      the Real-Debrid API wrapper with cache checking, torrent adding, and 
      account torrent listing."

You: "Use the integrations-dev agent to implement src/services/torrentio.py — 
      with the episode→season→show fallback chain described in SPEC.md Section 3.5."

You: "Use the integrations-dev agent to implement src/services/zilean.py."
```

### Phase 3: Core Logic
```
You: "Use the backend-dev agent to implement src/core/queue_manager.py — 
      the state machine with all transitions from SPEC.md Section 3.1, 
      including exponential backoff retry scheduling."

You: "Use the backend-dev agent to implement src/core/filter_engine.py — 
      three-tier filtering from SPEC.md Section 3.2."

You: "Use the backend-dev agent to implement src/core/dedup.py and 
      src/core/mount_scanner.py."

You: "Use the backend-dev agent to implement src/core/scrape_pipeline.py — 
      the orchestrator that calls mount_scanner → RD check → Zilean → Torrentio 
      with priority and fallback."
```

### Phase 4: Review Checkpoint
```
You: "Use the code-reviewer agent to review all files in src/services/ and src/core/."
```

Then address findings:
```
You: "Use the backend-dev agent to fix the issues found by code-reviewer: 
      [paste specific findings]."
```

### Phase 5: API & UI
```
You: "Use the backend-dev agent to implement the FastAPI routes in src/api/routes/."

You: "Use the frontend-dev agent to build the web UI templates."
```

### Phase 6: Tests
```
You: "Use the test-writer agent to write tests for src/core/queue_manager.py 
      and src/core/filter_engine.py."

You: "Use the test-writer agent to write tests for src/services/torrentio.py, 
      especially the episode fallback chain."
```

### Phase 7: Docker & Integration
```
You: "Use the backend-dev agent to create the Dockerfile and docker-compose.yml 
      that integrates with the existing Zurg/rclone stack."
```

## How to Use the Review Workflow

After any module is implemented:

1. **Review**: `"Use the code-reviewer agent to review src/core/queue_manager.py."`
2. **Read findings**: The reviewer returns a structured report with Critical/Warning/Suggestion items.
3. **You decide**: Which findings matter. Not everything needs fixing.
4. **Fix**: `"Use the backend-dev agent to address these code-reviewer findings: [paste the critical/warning items]."`
5. **Move on**: Don't loop — one review pass per module is sufficient.

## Tips

- **Be specific in prompts**: "Implement the Torrentio scraper with episode fallback" is better than "build the scrapers"
- **Reference SPEC.md sections**: "Implement the retry strategy from SPEC.md Section 3.2 Tier 3" gives the agent exact context
- **One module at a time**: Don't ask for 3 modules in one prompt. Sequential focused tasks produce better code.
- **Check context usage**: If you're deep in a session and hitting context limits, start a new session for the next module
- **Use `/agents` command**: To see all available agents and their status
- **Use `/compact`**: When your session gets long, compact to free context space
