"""Tests for settings endpoint security hardening.

Covers:
  - SettingsUpdate rejects unknown keys with 422
  - SettingsUpdate accepts known keys
  - PUT /api/settings rejects unknown top-level keys
  - PUT /api/settings error messages do not echo back submitted API key values
  - _mask_api_keys shows only last 4 chars (***WXYZ pattern)
  - GET /api/settings returns masked keys
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.api.routes.settings import SettingsUpdate, _mask_api_keys

# ---------------------------------------------------------------------------
# SettingsUpdate model unit tests
# ---------------------------------------------------------------------------


def test_settings_update_rejects_unknown_key() -> None:
    """SettingsUpdate raises ValidationError for unknown top-level keys."""
    with pytest.raises(ValidationError) as exc_info:
        SettingsUpdate(**{"unknown_field": "value"})
    errors = exc_info.value.errors()
    assert any("unknown_field" in str(e) or "extra" in str(e).lower() for e in errors)


def test_settings_update_rejects_multiple_unknown_keys() -> None:
    """SettingsUpdate raises ValidationError when multiple unknown keys are submitted."""
    with pytest.raises(ValidationError):
        SettingsUpdate(**{"foo": 1, "bar": 2, "real_debrid": {"api_key": "key"}})


def test_settings_update_accepts_known_keys() -> None:
    """SettingsUpdate accepts all known top-level keys without error."""
    update = SettingsUpdate(real_debrid={"api_key": "newkey"})
    assert update.real_debrid is not None
    assert update.real_debrid.api_key == "newkey"


def test_settings_update_accepts_empty_dict() -> None:
    """SettingsUpdate accepts an empty dict (no-op update)."""
    update = SettingsUpdate()
    assert update.to_partial_dict() == {}


def test_settings_update_to_partial_dict_excludes_none() -> None:
    """to_partial_dict() returns only explicitly provided fields."""
    update = SettingsUpdate(
        real_debrid={"api_key": "key123"},
        paths={"zurg_mount": "/mnt/zurg/__all__"},
    )
    result = update.to_partial_dict()
    assert "real_debrid" in result
    assert "paths" in result
    # Fields not provided should not appear
    assert "plex" not in result
    assert "trakt" not in result


def test_settings_update_nested_validation() -> None:
    """SettingsUpdate validates nested field types."""
    with pytest.raises(ValidationError):
        # port must be an integer, not a string that can't be coerced
        SettingsUpdate(server={"port": "not-a-number"})


# ---------------------------------------------------------------------------
# _mask_api_keys unit tests
# ---------------------------------------------------------------------------


def test_mask_api_keys_shows_only_last_four() -> None:
    """API keys are masked to show only the last 4 characters."""
    data = {"real_debrid": {"api_key": "ABCDEFGHIJKLMNOP"}}
    masked = _mask_api_keys(data)
    assert masked["real_debrid"]["api_key"] == "***MNOP"


def test_mask_api_keys_short_value() -> None:
    """Short API keys that are less than 4 chars become just '***'."""
    data = {"real_debrid": {"api_key": "abc"}}
    masked = _mask_api_keys(data)
    assert masked["real_debrid"]["api_key"] == "***"


def test_mask_api_keys_exactly_four_chars() -> None:
    """API key of exactly 4 characters shows ***WXYZ."""
    data = {"tmdb": {"api_key": "ABCD"}}
    masked = _mask_api_keys(data)
    assert masked["tmdb"]["api_key"] == "***ABCD"


def test_mask_api_keys_empty_value_unchanged() -> None:
    """Empty API key is not masked (nothing to mask)."""
    data = {"real_debrid": {"api_key": ""}}
    masked = _mask_api_keys(data)
    assert masked["real_debrid"]["api_key"] == ""


def test_mask_api_keys_token_field() -> None:
    """Fields named 'token' are also masked."""
    data = {"plex": {"token": "LONGPLEXTOKEN1234"}}
    masked = _mask_api_keys(data)
    assert masked["plex"]["token"] == "***1234"


def test_mask_api_keys_non_sensitive_fields_unchanged() -> None:
    """Non-sensitive fields are returned unchanged."""
    data = {"server": {"host": "0.0.0.0", "port": 5100}}
    masked = _mask_api_keys(data)
    assert masked["server"]["host"] == "0.0.0.0"
    assert masked["server"]["port"] == 5100


def test_mask_api_keys_does_not_expose_first_chars() -> None:
    """Masked output must not include the first characters of the key."""
    key = "FIRST-SECRET-PART-LAST"
    data = {"real_debrid": {"api_key": key}}
    masked = _mask_api_keys(data)
    masked_val = masked["real_debrid"]["api_key"]
    # Should start with *** not with first chars of key
    assert masked_val.startswith("***")
    # First 4 chars of original key must not appear at start of masked value
    assert not masked_val.startswith(key[:4])


# ---------------------------------------------------------------------------
# HTTP integration tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_put_settings_unknown_key_returns_422() -> None:
    """PUT /api/settings with an unknown top-level key returns 422."""
    from httpx import ASGITransport, AsyncClient

    from src.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        token = "test-csrf-token"
        response = await client.put(
            "/api/settings",
            json={"completely_unknown_key": "some_value"},
            cookies={"csrf_token": token},
            headers={"X-CSRF-Token": token},
        )

    assert response.status_code == 422
    # The error detail must not echo back the submitted value
    body = response.json()
    detail = str(body.get("detail", ""))
    assert "some_value" not in detail


@pytest.mark.asyncio
async def test_put_settings_error_does_not_echo_api_key() -> None:
    """PUT /api/settings validation error must not echo back submitted API key."""
    from httpx import ASGITransport, AsyncClient

    from src.main import app

    supersecret_key = "SUPERSECRETAPIKEY12345"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        token = "test-csrf-token"
        # Submit a key that will fail nested validation (invalid type)
        response = await client.put(
            "/api/settings",
            json={"server": {"port": supersecret_key}},
            cookies={"csrf_token": token},
            headers={"X-CSRF-Token": token},
        )

    body = response.json()
    # Key should not appear anywhere in the error response
    assert supersecret_key not in str(body)


@pytest.mark.asyncio
async def test_get_settings_masks_api_key() -> None:
    """GET /api/settings returns masked API key (only last 4 chars visible)."""
    from unittest.mock import patch

    from httpx import ASGITransport, AsyncClient

    from src.config import settings as app_settings
    from src.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        with patch.object(app_settings.real_debrid, "api_key", "ABCDEFGHIJKLMNOP"):
            response = await client.get("/api/settings")

    assert response.status_code == 200
    body = response.json()
    masked_key = body["settings"]["real_debrid"]["api_key"]
    # Should be ***MNOP — not the full key
    assert masked_key == "***MNOP"
    assert "ABCDEFGH" not in masked_key
