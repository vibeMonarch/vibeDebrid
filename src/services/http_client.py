"""Shared HTTP client manager and circuit breaker for external services.

Provides:
- ``get_client(service_name, base_url, **kwargs)`` — returns a persistent
  ``httpx.AsyncClient`` with connection pooling.  One client per
  (service_name, base_url) pair is reused across calls, avoiding TCP
  connection setup overhead on every request.
- ``close_all()`` — cleanly closes all pooled clients.  Called from the
  FastAPI shutdown hook in ``main.py``.
- ``CircuitBreaker`` — a per-service open/half-open/closed state machine
  that rejects requests immediately when a service is known to be failing,
  preventing cascading timeouts.
- ``CircuitOpenError`` — raised when a circuit is OPEN and a request is
  rejected without making the HTTP call.

Design notes:
- Clients are keyed by ``(service_name, base_url)`` so services that
  target different base URLs (e.g. plex.tv vs local Plex server) each get
  their own client.
- The circuit breaker is keyed by ``service_name`` only, so all URLs for
  the same logical service share one breaker.
- 429 responses are NOT treated as failures for circuit-breaking purposes
  (they are expected from RD and other rate-limited APIs).
- Plex OAuth calls target plex.tv vs the local server — both share the
  "plex" circuit breaker but have separate pooled clients.
"""

from __future__ import annotations

import asyncio
import logging
import time
from enum import Enum, auto

import httpx

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Connection pool defaults
# ---------------------------------------------------------------------------

_DEFAULT_LIMITS = httpx.Limits(
    max_connections=20,
    max_keepalive_connections=10,
    keepalive_expiry=30.0,
)

# ---------------------------------------------------------------------------
# Circuit breaker
# ---------------------------------------------------------------------------


class CircuitState(Enum):
    """States for the circuit breaker state machine."""

    CLOSED = auto()    # Normal operation — requests pass through.
    OPEN = auto()      # Failing — requests rejected immediately.
    HALF_OPEN = auto() # Testing recovery — one request allowed through.


class CircuitOpenError(Exception):
    """Raised when a circuit is OPEN and a request is rejected.

    Callers should treat this like a network error (log and return a safe
    default) rather than letting it propagate to the user.
    """

    def __init__(self, service_name: str) -> None:
        super().__init__(
            f"Circuit breaker OPEN for service '{service_name}' — "
            "request rejected to prevent cascading failures."
        )
        self.service_name = service_name


class CircuitBreaker:
    """A simple three-state circuit breaker for a single service.

    State transitions:
    - CLOSED → OPEN: ``failure_threshold`` consecutive failures recorded.
    - OPEN → HALF_OPEN: ``recovery_timeout`` seconds elapsed since last failure.
    - HALF_OPEN → CLOSED: The probe request succeeds.
    - HALF_OPEN → OPEN: The probe request fails.

    Args:
        service_name: Identifier used in log messages.
        failure_threshold: Consecutive failures before opening the circuit.
        recovery_timeout: Seconds to wait in OPEN state before probing.
    """

    def __init__(
        self,
        service_name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
    ) -> None:
        self.service_name = service_name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_at: float | None = None
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        """Current circuit state (may be HALF_OPEN after timeout elapsed)."""
        return self._state

    async def before_request(self) -> None:
        """Check whether a request should be allowed through.

        Transitions OPEN → HALF_OPEN when the recovery timeout has elapsed.

        Raises:
            CircuitOpenError: If the circuit is OPEN and the recovery timeout
                              has not yet elapsed.
        """
        async with self._lock:
            if self._state == CircuitState.OPEN:
                elapsed = (
                    time.monotonic() - self._last_failure_at
                    if self._last_failure_at is not None
                    else 0.0
                )
                if elapsed >= self.recovery_timeout:
                    logger.warning(
                        "Circuit breaker HALF_OPEN for '%s' — "
                        "allowing probe request (%.1fs since last failure)",
                        self.service_name,
                        elapsed,
                    )
                    self._state = CircuitState.HALF_OPEN
                else:
                    raise CircuitOpenError(self.service_name)

    async def record_success(self) -> None:
        """Record a successful request.

        Resets the failure counter and transitions HALF_OPEN → CLOSED.
        """
        async with self._lock:
            if self._state != CircuitState.CLOSED:
                logger.warning(
                    "Circuit breaker CLOSED for '%s' — service recovered",
                    self.service_name,
                )
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._last_failure_at = None

    async def record_failure(self) -> None:
        """Record a failed request.

        Increments the failure counter and opens the circuit if the threshold
        is reached.  In HALF_OPEN state a single failure re-opens immediately.
        """
        async with self._lock:
            self._failure_count += 1
            self._last_failure_at = time.monotonic()

            if self._state == CircuitState.HALF_OPEN:
                logger.warning(
                    "Circuit breaker OPEN for '%s' — probe request failed, "
                    "re-opening circuit",
                    self.service_name,
                )
                self._state = CircuitState.OPEN
            elif (
                self._state == CircuitState.CLOSED
                and self._failure_count >= self.failure_threshold
            ):
                logger.warning(
                    "Circuit breaker OPEN for '%s' — %d consecutive failures reached threshold",
                    self.service_name,
                    self._failure_count,
                )
                self._state = CircuitState.OPEN

    def reset(self) -> None:
        """Force-reset the circuit to CLOSED state (e.g. after config change)."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_at = None
        logger.info("Circuit breaker manually reset for '%s'", self.service_name)


# ---------------------------------------------------------------------------
# Client pool
# ---------------------------------------------------------------------------

# Keyed by (service_name, base_url) — allows one pool entry per distinct URL.
_client_pool: dict[tuple[str, str], httpx.AsyncClient] = {}

# One circuit breaker per logical service name.
_circuit_breakers: dict[str, CircuitBreaker] = {}

# Lock for pool mutations (dict is not thread-safe for concurrent creation).
_pool_lock = asyncio.Lock()


def get_circuit_breaker(
    service_name: str,
    failure_threshold: int = 5,
    recovery_timeout: float = 60.0,
) -> CircuitBreaker:
    """Return (creating if needed) the circuit breaker for a service.

    Args:
        service_name: Logical service name (e.g. ``"real_debrid"``).
        failure_threshold: Consecutive failures before opening.
        recovery_timeout: Seconds before transitioning OPEN → HALF_OPEN.

    Returns:
        The CircuitBreaker instance for this service.
    """
    if service_name not in _circuit_breakers:
        _circuit_breakers[service_name] = CircuitBreaker(
            service_name=service_name,
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
        )
    return _circuit_breakers[service_name]


async def get_client(
    service_name: str,
    base_url: str,
    *,
    timeout: float | httpx.Timeout = 30.0,
    headers: dict[str, str] | None = None,
    follow_redirects: bool = False,
    limits: httpx.Limits | None = None,
) -> httpx.AsyncClient:
    """Return a persistent pooled ``httpx.AsyncClient`` for the given service.

    Clients are keyed by ``(service_name, base_url)`` so the same client is
    reused across calls, enabling HTTP keep-alive connection reuse.

    Args:
        service_name: Logical service name used for circuit breaker lookup
                      and logging.
        base_url: The base URL for the service.
        timeout: Request timeout, either a float (seconds) or
                 ``httpx.Timeout`` object.
        headers: Default headers to include on every request.
        follow_redirects: Whether the client should follow redirects.
        limits: Connection pool limits.  Defaults to
                ``_DEFAULT_LIMITS`` when not specified.

    Returns:
        A persistent ``httpx.AsyncClient`` from the pool.
    """
    key = (service_name, base_url)
    if key in _client_pool:
        client = _client_pool[key]
        if not client.is_closed:
            return client

    async with _pool_lock:
        # Double-checked locking — another coroutine may have created it.
        if key in _client_pool:
            client = _client_pool[key]
            if not client.is_closed:
                return client

        client = httpx.AsyncClient(
            base_url=base_url,
            timeout=timeout,
            headers=headers or {},
            follow_redirects=follow_redirects,
            limits=limits or _DEFAULT_LIMITS,
        )
        _client_pool[key] = client
        logger.debug(
            "http_client: created pooled client for service='%s' base_url=%r",
            service_name,
            base_url,
        )
        return client


async def close_all() -> None:
    """Close all pooled HTTP clients.

    Should be called from the FastAPI shutdown hook to cleanly release
    all open TCP connections.
    """
    async with _pool_lock:
        for (service_name, base_url), client in list(_client_pool.items()):
            try:
                await client.aclose()
                logger.debug(
                    "http_client: closed pooled client for service='%s' base_url=%r",
                    service_name,
                    base_url,
                )
            except Exception as exc:  # noqa: BLE001 — best-effort cleanup
                logger.warning(
                    "http_client: error closing client for service='%s': %s",
                    service_name,
                    exc,
                )
        _client_pool.clear()
        logger.info("http_client: all pooled clients closed")
