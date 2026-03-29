"""Shared FastAPI dependencies."""

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession

from src.database import get_session


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Database session dependency."""
    async for session in get_session():
        yield session
