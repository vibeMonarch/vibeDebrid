"""Tests for src/services/http_client.py.

Covers:
  - CircuitBreaker: CLOSED → OPEN transition on consecutive failures
  - CircuitBreaker: OPEN raises CircuitOpenError until recovery_timeout elapses
  - CircuitBreaker: OPEN → HALF_OPEN transition after recovery_timeout
  - CircuitBreaker: HALF_OPEN → CLOSED on success
  - CircuitBreaker: HALF_OPEN → OPEN on failure
  - CircuitBreaker: failure_count reset on success
  - CircuitBreaker: reset() force-resets to CLOSED
  - get_client: returns same client on repeated calls (pool reuse)
  - get_client: different (service, url) keys return different clients
  - get_client: creates client with correct base_url
  - close_all: closes all pooled clients and clears the pool
  - get_circuit_breaker: returns same instance on repeated calls
"""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src.services.http_client import (
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
    close_all,
    get_circuit_breaker,
    get_client,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_breaker(
    failure_threshold: int = 3,
    recovery_timeout: float = 60.0,
    service_name: str = "test_svc",
) -> CircuitBreaker:
    """Return a fresh CircuitBreaker with custom threshold/timeout."""
    return CircuitBreaker(
        service_name=service_name,
        failure_threshold=failure_threshold,
        recovery_timeout=recovery_timeout,
    )


# ---------------------------------------------------------------------------
# CircuitBreaker — basic state
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_new_breaker_is_closed() -> None:
    """A freshly created CircuitBreaker starts in CLOSED state."""
    breaker = _make_breaker()
    assert breaker.state == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_record_success_keeps_closed() -> None:
    """record_success on a CLOSED breaker keeps it CLOSED."""
    breaker = _make_breaker()
    await breaker.record_success()
    assert breaker.state == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_record_failure_below_threshold_stays_closed() -> None:
    """Failures below the threshold do NOT open the circuit."""
    breaker = _make_breaker(failure_threshold=3)
    await breaker.record_failure()
    await breaker.record_failure()
    # 2 failures, threshold=3 — still CLOSED
    assert breaker.state == CircuitState.CLOSED


# ---------------------------------------------------------------------------
# CircuitBreaker — CLOSED → OPEN
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_open_after_threshold_failures() -> None:
    """Circuit opens after exactly ``failure_threshold`` consecutive failures."""
    breaker = _make_breaker(failure_threshold=3)
    for _ in range(3):
        await breaker.record_failure()
    assert breaker.state == CircuitState.OPEN


@pytest.mark.asyncio
async def test_before_request_raises_when_open() -> None:
    """before_request raises CircuitOpenError when the circuit is OPEN."""
    breaker = _make_breaker(failure_threshold=2, recovery_timeout=9999.0)
    await breaker.record_failure()
    await breaker.record_failure()

    with pytest.raises(CircuitOpenError) as exc_info:
        await breaker.before_request()

    assert "test_svc" in str(exc_info.value)


@pytest.mark.asyncio
async def test_before_request_passes_when_closed() -> None:
    """before_request does NOT raise when the circuit is CLOSED."""
    breaker = _make_breaker()
    # Should complete without raising
    await breaker.before_request()


@pytest.mark.asyncio
async def test_success_resets_failure_count() -> None:
    """A success after some (below-threshold) failures resets the counter."""
    breaker = _make_breaker(failure_threshold=5)
    await breaker.record_failure()
    await breaker.record_failure()
    await breaker.record_success()

    # Should not open the circuit even after 4 more failures (counter reset to 0)
    for _ in range(4):
        await breaker.record_failure()
    assert breaker.state == CircuitState.CLOSED


# ---------------------------------------------------------------------------
# CircuitBreaker — OPEN → HALF_OPEN
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_open_transitions_to_half_open_after_timeout() -> None:
    """When recovery_timeout elapses, before_request transitions OPEN → HALF_OPEN."""
    breaker = _make_breaker(failure_threshold=1, recovery_timeout=0.01)
    await breaker.record_failure()
    assert breaker.state == CircuitState.OPEN

    # Wait for the recovery timeout to elapse
    await asyncio.sleep(0.05)

    # before_request should NOT raise — it transitions to HALF_OPEN
    await breaker.before_request()
    assert breaker.state == CircuitState.HALF_OPEN


@pytest.mark.asyncio
async def test_open_raises_before_timeout_elapses() -> None:
    """before_request raises CircuitOpenError if timeout has NOT elapsed."""
    breaker = _make_breaker(failure_threshold=1, recovery_timeout=9999.0)
    await breaker.record_failure()

    with pytest.raises(CircuitOpenError):
        await breaker.before_request()

    # Circuit must still be OPEN (not transitioned)
    assert breaker.state == CircuitState.OPEN


# ---------------------------------------------------------------------------
# CircuitBreaker — HALF_OPEN transitions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_half_open_to_closed_on_success() -> None:
    """A success while HALF_OPEN closes the circuit."""
    breaker = _make_breaker(failure_threshold=1, recovery_timeout=0.01)
    await breaker.record_failure()
    await asyncio.sleep(0.05)
    await breaker.before_request()  # → HALF_OPEN
    assert breaker.state == CircuitState.HALF_OPEN

    await breaker.record_success()
    assert breaker.state == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_half_open_to_open_on_failure() -> None:
    """A failure while HALF_OPEN re-opens the circuit immediately."""
    breaker = _make_breaker(failure_threshold=1, recovery_timeout=0.01)
    await breaker.record_failure()
    await asyncio.sleep(0.05)
    await breaker.before_request()  # → HALF_OPEN

    await breaker.record_failure()
    assert breaker.state == CircuitState.OPEN


# ---------------------------------------------------------------------------
# CircuitBreaker — reset()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reset_force_closes_open_circuit() -> None:
    """reset() forcibly transitions OPEN → CLOSED."""
    breaker = _make_breaker(failure_threshold=1, recovery_timeout=9999.0)
    await breaker.record_failure()
    assert breaker.state == CircuitState.OPEN

    breaker.reset()
    assert breaker.state == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_after_reset_failures_count_from_zero() -> None:
    """After reset(), the failure counter is zeroed."""
    breaker = _make_breaker(failure_threshold=3, recovery_timeout=9999.0)
    # Record 2 failures (below threshold)
    await breaker.record_failure()
    await breaker.record_failure()

    breaker.reset()

    # Should need 3 NEW failures to open — 2 old ones were cleared
    await breaker.record_failure()
    await breaker.record_failure()
    assert breaker.state == CircuitState.CLOSED  # still closed

    await breaker.record_failure()
    assert breaker.state == CircuitState.OPEN


# ---------------------------------------------------------------------------
# CircuitOpenError
# ---------------------------------------------------------------------------


def test_circuit_open_error_contains_service_name() -> None:
    """CircuitOpenError message contains the service name."""
    err = CircuitOpenError("my_service")
    assert "my_service" in str(err)
    assert err.service_name == "my_service"


# ---------------------------------------------------------------------------
# get_client — pooling behaviour
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_client_same_key_returns_same_instance() -> None:
    """Two calls with identical (service_name, base_url) return the same client."""
    # Use a unique service name to avoid collision with other tests
    svc = "test_pool_reuse"
    url = "http://test.local:9999"

    client_a = await get_client(svc, url, timeout=5.0)
    client_b = await get_client(svc, url, timeout=5.0)

    assert client_a is client_b

    # Cleanup — close the pooled client
    await close_all()


@pytest.mark.asyncio
async def test_get_client_different_url_returns_different_instances() -> None:
    """Two calls with the same service but different URLs return different clients."""
    svc = "test_pool_diff_url"
    url_a = "http://host-a.local:9000"
    url_b = "http://host-b.local:9000"

    client_a = await get_client(svc, url_a, timeout=5.0)
    client_b = await get_client(svc, url_b, timeout=5.0)

    assert client_a is not client_b

    await close_all()


@pytest.mark.asyncio
async def test_get_client_different_service_returns_different_instances() -> None:
    """Two calls with the same URL but different service names return different clients."""
    url = "http://shared.local:9000"
    client_a = await get_client("test_svc_alpha", url, timeout=5.0)
    client_b = await get_client("test_svc_beta", url, timeout=5.0)

    assert client_a is not client_b

    await close_all()


@pytest.mark.asyncio
async def test_get_client_base_url_set_correctly() -> None:
    """The returned client has the correct base_url configured."""
    svc = "test_base_url"
    url = "http://base-url-test.local:8888"

    client = await get_client(svc, url, timeout=5.0)

    assert str(client.base_url).rstrip("/") == url

    await close_all()


# ---------------------------------------------------------------------------
# close_all — cleanup
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_close_all_closes_clients() -> None:
    """close_all() marks all pooled clients as closed."""
    svc = "test_close_all"
    url = "http://close-test.local:7777"

    client = await get_client(svc, url, timeout=5.0)
    assert not client.is_closed

    await close_all()

    assert client.is_closed


@pytest.mark.asyncio
async def test_get_client_after_close_creates_new() -> None:
    """After close_all(), get_client creates a fresh client."""
    svc = "test_after_close"
    url = "http://after-close.local:6666"

    client_before = await get_client(svc, url, timeout=5.0)
    await close_all()

    client_after = await get_client(svc, url, timeout=5.0)

    assert client_after is not client_before
    assert not client_after.is_closed

    await close_all()


# ---------------------------------------------------------------------------
# get_circuit_breaker — singleton behaviour
# ---------------------------------------------------------------------------


def test_get_circuit_breaker_returns_same_instance() -> None:
    """get_circuit_breaker with the same name returns the same object."""
    svc = "test_cb_singleton"
    breaker_a = get_circuit_breaker(svc)
    breaker_b = get_circuit_breaker(svc)

    assert breaker_a is breaker_b


def test_get_circuit_breaker_different_names_are_independent() -> None:
    """get_circuit_breaker with different names returns different objects."""
    breaker_a = get_circuit_breaker("test_cb_svc_one")
    breaker_b = get_circuit_breaker("test_cb_svc_two")

    assert breaker_a is not breaker_b
