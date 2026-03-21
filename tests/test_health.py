"""Tests for GET /health endpoint.

Covers:
  - 200 response when database and mount are both healthy
  - 200 response when only mount is unhealthy (non-critical)
  - 503 response when database is unreachable (critical)
  - 503 response when both database and mount fail
  - Response body shape matches the HealthResponse schema
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.deps import get_db
from src.main import app


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def override_db(session: AsyncSession):
    """Override get_db dependency with the in-memory test session."""

    async def _override() -> AsyncSession:
        yield session

    app.dependency_overrides[get_db] = _override
    yield
    app.dependency_overrides.pop(get_db, None)


@pytest.fixture
async def http(override_db) -> AsyncClient:
    """Async HTTP client wired to the FastAPI test app with DB override."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
async def http_no_db() -> AsyncClient:
    """Async HTTP client without DB override — callers mock the DB themselves."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


# ---------------------------------------------------------------------------
# Tests — happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_health_all_ok(http: AsyncClient) -> None:
    """Returns 200 and status=healthy when DB and mount are both available."""
    with patch(
        "src.api.routes.health.mount_scanner.is_mount_available",
        new=AsyncMock(return_value=True),
    ):
        response = await http.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "healthy"
    assert body["checks"]["database"] is True
    assert body["checks"]["zurg_mount"] is True


@pytest.mark.asyncio
async def test_health_mount_unavailable_is_non_critical(http: AsyncClient) -> None:
    """Returns 200 and status=healthy even when the mount is unavailable."""
    with patch(
        "src.api.routes.health.mount_scanner.is_mount_available",
        new=AsyncMock(return_value=False),
    ):
        response = await http.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "healthy"
    assert body["checks"]["database"] is True
    assert body["checks"]["zurg_mount"] is False


# ---------------------------------------------------------------------------
# Tests — database failure (critical)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_health_database_failure_returns_503(http_no_db: AsyncClient) -> None:
    """Returns 503 and status=unhealthy when the database check fails."""

    async def _broken_db():
        # Yield a session whose execute() always raises
        mock_session = AsyncMock(spec=AsyncSession)
        mock_session.execute.side_effect = SQLAlchemyError("DB unavailable")
        yield mock_session

    app.dependency_overrides[get_db] = _broken_db
    try:
        with patch(
            "src.api.routes.health.mount_scanner.is_mount_available",
            new=AsyncMock(return_value=True),
        ):
            response = await http_no_db.get("/health")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "unhealthy"
    assert body["checks"]["database"] is False
    assert body["checks"]["zurg_mount"] is True


@pytest.mark.asyncio
async def test_health_both_fail_returns_503(http_no_db: AsyncClient) -> None:
    """Returns 503 and status=unhealthy when both database and mount fail."""

    async def _broken_db():
        mock_session = AsyncMock(spec=AsyncSession)
        mock_session.execute.side_effect = SQLAlchemyError("DB unavailable")
        yield mock_session

    app.dependency_overrides[get_db] = _broken_db
    try:
        with patch(
            "src.api.routes.health.mount_scanner.is_mount_available",
            new=AsyncMock(return_value=False),
        ):
            response = await http_no_db.get("/health")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "unhealthy"
    assert body["checks"]["database"] is False
    assert body["checks"]["zurg_mount"] is False


# ---------------------------------------------------------------------------
# Tests — mount check exception handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_health_mount_exception_treated_as_unavailable(
    http: AsyncClient,
) -> None:
    """Mount check exception is caught and reported as zurg_mount=False, not 503."""
    with patch(
        "src.api.routes.health.mount_scanner.is_mount_available",
        new=AsyncMock(side_effect=OSError("mount hang")),
    ):
        response = await http.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "healthy"
    assert body["checks"]["database"] is True
    assert body["checks"]["zurg_mount"] is False


# ---------------------------------------------------------------------------
# Tests — response schema
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_health_response_schema(http: AsyncClient) -> None:
    """Response contains exactly the expected top-level keys and nested shape."""
    with patch(
        "src.api.routes.health.mount_scanner.is_mount_available",
        new=AsyncMock(return_value=True),
    ):
        response = await http.get("/health")

    body = response.json()
    assert set(body.keys()) == {"status", "version", "checks"}
    assert set(body["checks"].keys()) == {"database", "zurg_mount"}
    assert isinstance(body["status"], str)
    assert isinstance(body["version"], str)
    assert isinstance(body["checks"]["database"], bool)
    assert isinstance(body["checks"]["zurg_mount"], bool)


@pytest.mark.asyncio
async def test_health_no_auth_required(http: AsyncClient) -> None:
    """Endpoint is accessible without any authentication headers."""
    with patch(
        "src.api.routes.health.mount_scanner.is_mount_available",
        new=AsyncMock(return_value=True),
    ):
        response = await http.get("/health")

    # Any 2xx or 5xx is acceptable — 401/403 is not
    assert response.status_code not in (401, 403)
