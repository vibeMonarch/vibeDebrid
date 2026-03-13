# Contributing to vibeDebrid

Thank you for your interest in contributing. This document covers how to get set up and what to expect.

## Getting Started

```bash
git clone https://github.com/YOUR_USERNAME/vibeDebrid.git
cd vibeDebrid
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
cp config.example.json config.json
# Edit config.json with your API keys and paths
```

## Development Workflow

### Running the app

```bash
.venv/bin/python src/main.py
```

### Running tests

```bash
.venv/bin/python -m pytest tests/ -q
```

All tests must pass before submitting a pull request.

### Linting

```bash
.venv/bin/ruff check src/
```

Fix any issues with:

```bash
.venv/bin/ruff check src/ --fix
```

## Code Standards

- **Type hints** on every function signature and return type — no exceptions.
- **Async everywhere** — all I/O must use `async/await`. No synchronous blocking calls in async context.
- **Explicit httpx timeouts** — every `httpx.AsyncClient` call must set a timeout.
- **Structured logging** — use `logging.getLogger(__name__)`, never `print()`.
- **Specific exception handling** — no bare `except:` or `except Exception:` unless truly warranted and commented.
- **Docstrings** on all public functions. Single-line for simple functions, Google style for complex ones.

See `CLAUDE.md` for the full architecture overview and design decisions.

## Submitting Changes

1. Fork the repository and create a branch from `main`.
2. Make your changes with appropriate tests.
3. Ensure `pytest tests/ -q` passes with no failures.
4. Ensure `ruff check src/` reports no issues.
5. Open a pull request with a clear description of what you changed and why.

## Pull Request Guidelines

- Keep PRs focused — one feature or fix per PR.
- Include tests for new functionality.
- Update `README.md` if you add a user-facing feature or change configuration.
- Reference any related issue numbers in the PR description.

## Reporting Bugs

Open a GitHub issue with:

- A clear description of the problem
- Steps to reproduce
- Expected vs actual behaviour
- Relevant log output (check the `logs/` directory or console output)
- Your configuration (redact any API keys)

## Feature Requests

Open a GitHub issue describing the feature and why it would be useful. Check existing issues first to avoid duplicates.

## Security

Do not report security vulnerabilities in public GitHub issues. If you find a security issue, please open a private security advisory on GitHub or contact the maintainers directly.

## License

By contributing, you agree that your contributions will be licensed under the Apache License, Version 2.0.
