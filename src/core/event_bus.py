"""In-process event bus for broadcasting queue state change events to SSE clients."""

import asyncio
import dataclasses
import json
import logging
import uuid

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class QueueEvent:
    """Represents a single queue state transition event.

    Attributes:
        item_id: Primary key of the MediaItem that changed state.
        title: Human-readable title of the media item.
        old_state: The state the item transitioned from.
        new_state: The state the item transitioned to.
        retry_count: Current retry count on the item after the transition.
        media_type: The media type of the item ("movie", "show", or empty string).
    """

    item_id: int
    title: str
    old_state: str
    new_state: str
    retry_count: int
    media_type: str

    def to_sse_data(self) -> str:
        """Serialise the event as a JSON string suitable for the SSE data field.

        Returns:
            A compact JSON string of all event fields.
        """
        return json.dumps(dataclasses.asdict(self))


class EventBus:
    """Pub/sub event bus that fans out QueueEvents to connected SSE clients.

    Each SSE connection subscribes to receive a personal asyncio.Queue.
    Publishing is non-blocking and fire-and-forget: if a client's queue is
    full, the event is dropped for that client and a warning is logged.
    """

    def __init__(self) -> None:
        self._clients: dict[str, asyncio.Queue[QueueEvent | None]] = {}

    def subscribe(self) -> tuple[str, asyncio.Queue[QueueEvent | None]]:
        """Register a new SSE client and return its dedicated queue.

        Returns:
            A tuple of ``(client_id, queue)`` where ``client_id`` is a UUID
            string and ``queue`` is the asyncio.Queue to read events from.
            The queue has a maximum size of 64 to bound memory usage.
        """
        client_id = str(uuid.uuid4())
        queue: asyncio.Queue[QueueEvent | None] = asyncio.Queue(maxsize=64)
        self._clients[client_id] = queue
        logger.info("SSE client subscribed: %s (total=%d)", client_id, len(self._clients))
        return client_id, queue

    def unsubscribe(self, client_id: str) -> None:
        """Remove a client from the event bus.

        Safe to call even if ``client_id`` is not currently subscribed.

        Args:
            client_id: The UUID string returned by :meth:`subscribe`.
        """
        self._clients.pop(client_id, None)
        logger.info("SSE client unsubscribed: %s (total=%d)", client_id, len(self._clients))

    def publish(self, event: QueueEvent) -> None:
        """Broadcast a QueueEvent to all subscribed clients.

        This method never blocks. If a client's queue is full the event is
        silently dropped for that client and a warning is emitted.

        Args:
            event: The QueueEvent to broadcast.
        """
        for client_id, queue in list(self._clients.items()):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                logger.warning(
                    "SSE client %s queue full, dropping event for item_id=%d",
                    client_id,
                    event.item_id,
                )

    def shutdown(self) -> None:
        """Send the shutdown sentinel to all clients and clear the subscriber list.

        Each client receives a ``None`` value in its queue, which the
        :func:`_event_generator` treats as a signal to stop iteration.
        """
        for client_id, queue in list(self._clients.items()):
            try:
                queue.put_nowait(None)
            except asyncio.QueueFull:
                logger.warning(
                    "SSE client %s queue full during shutdown, sentinel may be lost",
                    client_id,
                )
        self._clients.clear()
        logger.info("EventBus shut down, all clients notified")


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

event_bus = EventBus()
