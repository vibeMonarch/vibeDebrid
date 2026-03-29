"""SSE endpoint for real-time queue state change events."""

import asyncio
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter
from starlette.responses import StreamingResponse

from src.core.event_bus import QueueEvent, event_bus

logger = logging.getLogger(__name__)

router = APIRouter()


async def _event_generator(
    client_id: str,
    queue: asyncio.Queue[QueueEvent | None],
) -> AsyncGenerator[str, None]:
    """Yield SSE-formatted events from the client's queue.

    Sends a ``connected`` event immediately on connection, then streams
    ``state_change`` events as they arrive. A 30-second heartbeat comment is
    emitted whenever the queue is idle to prevent proxy timeouts. The generator
    exits when the shutdown sentinel (``None``) is received from the queue or
    when the client disconnects (GeneratorExit).

    Args:
        client_id: The UUID assigned to this SSE client by the event bus.
        queue: The asyncio.Queue dedicated to this client.
    """
    try:
        yield "event: connected\ndata: {}\n\n"
        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=30.0)
            except TimeoutError:
                # Send heartbeat comment to keep connection alive
                yield ": heartbeat\n\n"
                continue
            if event is None:
                # Graceful shutdown sentinel
                break
            yield f"event: state_change\ndata: {event.to_sse_data()}\n\n"
    finally:
        event_bus.unsubscribe(client_id)
        logger.info("SSE client %s disconnected", client_id)


@router.get("/events")
async def sse_events() -> StreamingResponse:
    """SSE endpoint for real-time queue updates.

    Returns a streaming ``text/event-stream`` response. Each connected client
    receives ``state_change`` events whenever a MediaItem transitions between
    queue states, plus periodic heartbeat comments every 30 seconds.
    """
    client_id, queue = event_bus.subscribe()
    return StreamingResponse(
        _event_generator(client_id, queue),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
