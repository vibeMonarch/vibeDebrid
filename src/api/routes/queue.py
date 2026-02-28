"""Queue management endpoints."""

from fastapi import APIRouter

router = APIRouter()


@router.get("")
async def list_queue() -> dict:
    """List items with filtering (state, media_type, title search)."""
    return {"items": []}


@router.get("/{item_id}")
async def get_item(item_id: int) -> dict:
    """Item detail with full history."""
    return {"id": item_id}


@router.post("/{item_id}/retry")
async def retry_item(item_id: int) -> dict:
    """Force retry a sleeping/dormant item."""
    return {"id": item_id, "action": "retry"}


@router.post("/{item_id}/state")
async def change_state(item_id: int) -> dict:
    """Manually change item state."""
    return {"id": item_id, "action": "state_change"}


@router.delete("/{item_id}")
async def remove_item(item_id: int) -> dict:
    """Remove item from all queues."""
    return {"id": item_id, "action": "removed"}


@router.post("/bulk/retry")
async def bulk_retry() -> dict:
    """Bulk retry."""
    return {"action": "bulk_retry"}


@router.post("/bulk/remove")
async def bulk_remove() -> dict:
    """Bulk remove."""
    return {"action": "bulk_remove"}
