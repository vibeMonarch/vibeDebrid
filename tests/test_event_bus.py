"""Tests for src/core/event_bus.py.

Covers:
  - subscribe returns (str, asyncio.Queue) tuple
  - unsubscribe removes client and is a no-op for unknown IDs
  - publish delivers to one and multiple clients
  - publish silently drops events when a queue is full
  - QueueEvent.to_sse_data() produces valid JSON with all fields
  - shutdown sends the None sentinel to all clients and clears _clients
  - publish with no clients does not raise
  - module-level singleton exists and is an EventBus instance
"""

from __future__ import annotations

import asyncio
import json

from src.core.event_bus import EventBus, QueueEvent

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_event(
    *,
    item_id: int = 1,
    title: str = "Test Movie",
    old_state: str = "wanted",
    new_state: str = "scraping",
    retry_count: int = 0,
    media_type: str = "movie",
    tmdb_id: str | None = None,
) -> QueueEvent:
    """Build a QueueEvent with sensible defaults for testing."""
    return QueueEvent(
        item_id=item_id,
        title=title,
        old_state=old_state,
        new_state=new_state,
        retry_count=retry_count,
        media_type=media_type,
        tmdb_id=tmdb_id,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_subscribe_returns_id_and_queue() -> None:
    """subscribe() returns a (str, asyncio.Queue) tuple."""
    bus = EventBus()
    result = bus.subscribe()

    assert isinstance(result, tuple)
    assert len(result) == 2

    client_id, queue = result
    assert isinstance(client_id, str)
    assert len(client_id) > 0
    assert isinstance(queue, asyncio.Queue)


def test_subscribe_stores_client_in_internal_dict() -> None:
    """subscribe() stores the new client in _clients."""
    bus = EventBus()
    client_id, queue = bus.subscribe()

    assert client_id in bus._clients
    assert bus._clients[client_id] is queue


def test_unsubscribe_removes_client() -> None:
    """unsubscribe() removes the client_id from _clients."""
    bus = EventBus()
    client_id, _ = bus.subscribe()

    assert client_id in bus._clients
    bus.unsubscribe(client_id)
    assert client_id not in bus._clients


def test_unsubscribe_nonexistent_is_noop() -> None:
    """unsubscribe() with an unknown client_id does not raise."""
    bus = EventBus()
    # Should not raise any exception
    bus.unsubscribe("nonexistent-uuid-that-was-never-registered")


def test_unsubscribe_leaves_other_clients_intact() -> None:
    """unsubscribe() only removes the targeted client."""
    bus = EventBus()
    id_a, _ = bus.subscribe()
    id_b, _ = bus.subscribe()

    bus.unsubscribe(id_a)

    assert id_a not in bus._clients
    assert id_b in bus._clients


def test_publish_delivers_to_single_client() -> None:
    """publish() puts the event into the single subscriber's queue."""
    bus = EventBus()
    client_id, queue = bus.subscribe()

    event = _make_event()
    bus.publish(event)

    received = queue.get_nowait()
    assert received is event
    bus.unsubscribe(client_id)


def test_publish_delivers_to_multiple_clients() -> None:
    """publish() fans out the event to all subscribed clients."""
    bus = EventBus()
    id_a, queue_a = bus.subscribe()
    id_b, queue_b = bus.subscribe()

    event = _make_event(item_id=42)
    bus.publish(event)

    assert queue_a.get_nowait() is event
    assert queue_b.get_nowait() is event

    bus.unsubscribe(id_a)
    bus.unsubscribe(id_b)


def test_publish_drops_on_full_queue() -> None:
    """publish() silently drops the event when a client's queue is full (maxsize=64)."""
    bus = EventBus()
    client_id, queue = bus.subscribe()

    # Fill the queue to its maxsize of 64
    for i in range(64):
        queue.put_nowait(_make_event(item_id=i))

    assert queue.full()

    # This must not raise even though the queue is full
    overflow_event = _make_event(item_id=999)
    bus.publish(overflow_event)

    # Queue is still at maxsize; the overflow event was dropped
    assert queue.qsize() == 64
    bus.unsubscribe(client_id)


def test_publish_no_clients_is_noop() -> None:
    """publish() with no subscribers does not raise."""
    bus = EventBus()
    assert len(bus._clients) == 0

    event = _make_event()
    # Must not raise
    bus.publish(event)


def test_event_to_sse_data_is_valid_json() -> None:
    """to_sse_data() returns a string that is valid JSON."""
    event = _make_event(
        item_id=7,
        title="Some Film",
        old_state="sleeping",
        new_state="scraping",
        retry_count=3,
        media_type="show",
    )
    sse_string = event.to_sse_data()

    assert isinstance(sse_string, str)
    parsed = json.loads(sse_string)
    assert isinstance(parsed, dict)


def test_event_to_sse_data_contains_all_fields() -> None:
    """to_sse_data() JSON includes every field of the QueueEvent."""
    event = _make_event(
        item_id=7,
        title="Some Film",
        old_state="sleeping",
        new_state="scraping",
        retry_count=3,
        media_type="show",
        tmdb_id="12345",
    )
    parsed = json.loads(event.to_sse_data())

    assert parsed["item_id"] == 7
    assert parsed["title"] == "Some Film"
    assert parsed["old_state"] == "sleeping"
    assert parsed["new_state"] == "scraping"
    assert parsed["retry_count"] == 3
    assert parsed["media_type"] == "show"
    assert parsed["tmdb_id"] == "12345"


def test_event_to_sse_data_tmdb_id_none_by_default() -> None:
    """to_sse_data() serialises tmdb_id as null when not supplied."""
    event = _make_event()
    parsed = json.loads(event.to_sse_data())
    assert "tmdb_id" in parsed
    assert parsed["tmdb_id"] is None


def test_event_to_sse_data_tmdb_id_present_when_set() -> None:
    """to_sse_data() includes tmdb_id string when explicitly provided."""
    event = _make_event(tmdb_id="98765")
    parsed = json.loads(event.to_sse_data())
    assert parsed["tmdb_id"] == "98765"


def test_queue_event_tmdb_id_defaults_to_none() -> None:
    """QueueEvent.tmdb_id defaults to None when not supplied."""
    event = QueueEvent(
        item_id=1,
        title="Test",
        old_state="wanted",
        new_state="scraping",
        retry_count=0,
        media_type="movie",
    )
    assert event.tmdb_id is None


def test_event_to_sse_data_unicode_title() -> None:
    """to_sse_data() handles unicode characters in title correctly."""
    event = _make_event(title="Nausicaa \u306e\u8c37\u98a8\u306e\u5c71")
    parsed = json.loads(event.to_sse_data())
    assert parsed["title"] == "Nausicaa \u306e\u8c37\u98a8\u306e\u5c71"


def test_shutdown_sends_sentinel_to_all_clients() -> None:
    """shutdown() puts None into every client's queue."""
    bus = EventBus()
    id_a, queue_a = bus.subscribe()
    id_b, queue_b = bus.subscribe()

    bus.shutdown()

    assert queue_a.get_nowait() is None
    assert queue_b.get_nowait() is None


def test_shutdown_clears_clients_dict() -> None:
    """shutdown() empties the _clients dict."""
    bus = EventBus()
    bus.subscribe()
    bus.subscribe()

    assert len(bus._clients) == 2
    bus.shutdown()
    assert len(bus._clients) == 0


def test_shutdown_with_no_clients_is_noop() -> None:
    """shutdown() with zero subscribers does not raise."""
    bus = EventBus()
    bus.shutdown()
    assert len(bus._clients) == 0


def test_shutdown_on_full_queue_does_not_raise() -> None:
    """shutdown() does not raise when a client queue is already full."""
    bus = EventBus()
    client_id, queue = bus.subscribe()

    # Fill queue completely so there is no room for the sentinel
    for i in range(64):
        queue.put_nowait(_make_event(item_id=i))

    assert queue.full()

    # shutdown() must swallow the QueueFull and not raise
    bus.shutdown()
    assert len(bus._clients) == 0


def test_multiple_subscriptions_have_unique_ids() -> None:
    """Each call to subscribe() returns a distinct client_id."""
    bus = EventBus()
    ids = {bus.subscribe()[0] for _ in range(10)}
    assert len(ids) == 10


def test_publish_event_values_are_identical_across_clients() -> None:
    """All clients receive the same event object (not a copy)."""
    bus = EventBus()
    _, queue_a = bus.subscribe()
    _, queue_b = bus.subscribe()

    event = _make_event(item_id=55)
    bus.publish(event)

    received_a = queue_a.get_nowait()
    received_b = queue_b.get_nowait()

    assert received_a is received_b is event


def test_module_singleton_exists() -> None:
    """The module exposes a ready-to-use EventBus singleton called event_bus."""
    from src.core.event_bus import event_bus

    assert isinstance(event_bus, EventBus)
