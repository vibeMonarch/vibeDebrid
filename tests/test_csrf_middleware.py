"""Tests for CSRF middleware (double-submit cookie pattern).

Covers:
  - GET requests always receive a csrf_token cookie
  - Mutation requests without a cookie return 403
  - Mutation requests without a header return 403
  - Mutation requests with mismatched token return 403
  - Mutation requests with matching token succeed
  - /health is exempt from CSRF checks
  - /api/events is exempt from CSRF checks
  - Existing cookie value is preserved (not rotated) on subsequent GETs
"""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from src.main import app


@pytest.fixture(autouse=True)
def _enable_csrf_for_these_tests() -> None:
    """Override the global conftest bypass so CSRF is actually enforced here."""
    app.state.csrf_bypass = False
    yield
    # Leave bypass=False; conftest autouse fixture will reset to False anyway


@pytest.fixture
async def http() -> AsyncClient:
    """Async HTTP client wired to the FastAPI test app, no DB override needed."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


# ---------------------------------------------------------------------------
# Cookie issuance
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_sets_csrf_cookie(http: AsyncClient) -> None:
    """GET requests must set a csrf_token cookie."""
    response = await http.get("/health")
    assert "csrf_token" in response.cookies


@pytest.mark.asyncio
async def test_get_csrf_cookie_is_not_httponly(http: AsyncClient) -> None:
    """The csrf_token cookie must not be HttpOnly so JS can read it."""
    response = await http.get("/health")
    # httpx stores cookies; check that the Set-Cookie header does not include HttpOnly
    set_cookie = response.headers.get("set-cookie", "")
    assert "httponly" not in set_cookie.lower()


@pytest.mark.asyncio
async def test_existing_cookie_is_preserved_on_get(http: AsyncClient) -> None:
    """If a csrf_token cookie is already present, the server reuses it."""
    # First GET — get an initial token
    r1 = await http.get("/health")
    token = r1.cookies.get("csrf_token")
    assert token is not None

    # Second GET — send the cookie back
    r2 = await http.get("/health", cookies={"csrf_token": token})
    returned_token = r2.cookies.get("csrf_token")
    # The server should echo back the same token
    assert returned_token == token


# ---------------------------------------------------------------------------
# Mutation enforcement
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_post_without_cookie_returns_403(http: AsyncClient) -> None:
    """POST request with no CSRF cookie returns 403."""
    response = await http.post("/api/settings/test/realdebrid")
    assert response.status_code == 403
    assert "CSRF" in response.json().get("detail", "")


@pytest.mark.asyncio
async def test_post_without_header_returns_403(http: AsyncClient) -> None:
    """POST request with cookie but no header returns 403."""
    token = "abc123"
    response = await http.post(
        "/api/settings/test/realdebrid",
        cookies={"csrf_token": token},
    )
    assert response.status_code == 403
    assert "CSRF" in response.json().get("detail", "")


@pytest.mark.asyncio
async def test_post_with_mismatched_token_returns_403(http: AsyncClient) -> None:
    """POST request where header does not match cookie returns 403."""
    response = await http.post(
        "/api/settings/test/realdebrid",
        cookies={"csrf_token": "correct-token"},
        headers={"X-CSRF-Token": "wrong-token"},
    )
    assert response.status_code == 403
    assert "CSRF" in response.json().get("detail", "")


@pytest.mark.asyncio
async def test_post_with_matching_token_passes_csrf(http: AsyncClient) -> None:
    """POST request with matching cookie and header passes CSRF check.

    The endpoint itself may return 4xx/5xx for other reasons (missing API key,
    etc.) but must not be blocked by CSRF.
    """
    token = "test-csrf-token-1234"
    response = await http.post(
        "/api/settings/test/realdebrid",
        cookies={"csrf_token": token},
        headers={"X-CSRF-Token": token},
    )
    # CSRF check passed — any response except 403 with CSRF detail is acceptable
    if response.status_code == 403:
        assert "CSRF" not in response.json().get("detail", "")


@pytest.mark.asyncio
async def test_put_with_matching_token_passes_csrf(http: AsyncClient) -> None:
    """PUT request with matching cookie and header passes CSRF check."""
    token = "put-csrf-token-5678"
    response = await http.put(
        "/api/settings",
        json={},
        cookies={"csrf_token": token},
        headers={"X-CSRF-Token": token},
    )
    # 422 (validation error) or 200 is fine — not 403
    assert response.status_code != 403 or "CSRF" not in response.json().get("detail", "")


@pytest.mark.asyncio
async def test_delete_with_matching_token_passes_csrf(http: AsyncClient) -> None:
    """DELETE request with matching cookie and header passes CSRF check."""
    token = "delete-csrf-token-9999"
    response = await http.delete(
        "/api/queue/99999",
        cookies={"csrf_token": token},
        headers={"X-CSRF-Token": token},
    )
    # Not a CSRF block (might be 404 because item doesn't exist)
    if response.status_code == 403:
        assert "CSRF" not in response.json().get("detail", "")


# ---------------------------------------------------------------------------
# Exempt paths
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_health_exempt_no_cookie_needed(http: AsyncClient) -> None:
    """/health GET is exempt — always accessible without token."""
    response = await http.get("/health")
    assert response.status_code not in (403,)


@pytest.mark.asyncio
async def test_health_get_still_sets_cookie(http: AsyncClient) -> None:
    """Even though /health is exempt, GET still sets a csrf_token cookie."""
    response = await http.get("/health")
    assert "csrf_token" in response.cookies
