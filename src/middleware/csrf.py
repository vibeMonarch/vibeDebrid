"""CSRF protection middleware using the double-submit cookie pattern.

On every GET request a ``csrf_token`` cookie is set (SameSite=Strict,
HttpOnly=False so JavaScript can read it).  Mutation requests (POST, PUT,
DELETE, PATCH) must supply an ``X-CSRF-Token`` header whose value matches the
cookie.  A mismatch returns 403.

Exempt paths (exact or prefix):
- /health         — monitoring probe
- /api/events     — SSE stream (GET only, no mutations)
- /api/webhook/zurg — Zurg webhook (machine-to-machine, no browser context)

Test bypass:
Set ``app.state.csrf_bypass = True`` before tests to skip token verification.
This is only intended for the test suite; never set in production.
"""

import logging
import secrets

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger(__name__)

MUTATION_METHODS = {"POST", "PUT", "DELETE", "PATCH"}
CSRF_COOKIE_NAME = "csrf_token"
CSRF_HEADER_NAME = "x-csrf-token"
TOKEN_BYTES = 32

# Paths that are exempt from CSRF enforcement.
_EXEMPT_EXACT: frozenset[str] = frozenset({"/health", "/api/events", "/api/webhook/zurg"})


def _is_exempt(path: str) -> bool:
    """Return True if the request path is exempt from CSRF checks."""
    return path in _EXEMPT_EXACT


class CSRFMiddleware(BaseHTTPMiddleware):
    """Double-submit cookie CSRF middleware for FastAPI/Starlette."""

    async def dispatch(self, request: Request, call_next) -> Response:  # type: ignore[override]
        path = request.url.path

        # Allow tests to bypass CSRF enforcement by setting app.state.csrf_bypass = True.
        bypass = getattr(request.app.state, "csrf_bypass", False)

        if not bypass and request.method in MUTATION_METHODS and not _is_exempt(path):
            cookie_token = request.cookies.get(CSRF_COOKIE_NAME)
            header_token = request.headers.get(CSRF_HEADER_NAME)

            if not cookie_token or not header_token:
                logger.warning(
                    "CSRF token missing: method=%s path=%s has_cookie=%s has_header=%s",
                    request.method,
                    path,
                    bool(cookie_token),
                    bool(header_token),
                )
                return Response(
                    content='{"detail":"CSRF token missing"}',
                    status_code=403,
                    media_type="application/json",
                )

            if not secrets.compare_digest(cookie_token, header_token):
                logger.warning(
                    "CSRF token mismatch: method=%s path=%s",
                    request.method,
                    path,
                )
                return Response(
                    content='{"detail":"CSRF token mismatch"}',
                    status_code=403,
                    media_type="application/json",
                )

        response: Response = await call_next(request)

        # Set/refresh the CSRF cookie on every GET response so new browser
        # sessions always get a token, and existing sessions keep theirs.
        if request.method == "GET":
            existing = request.cookies.get(CSRF_COOKIE_NAME)
            token = existing if existing else secrets.token_hex(TOKEN_BYTES)
            response.set_cookie(
                key=CSRF_COOKIE_NAME,
                value=token,
                samesite="strict",
                httponly=False,  # JS must read this value
                secure=False,    # self-hosted HTTP; flip to True behind HTTPS proxy
            )

        return response
