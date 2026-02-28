"""Async SQLAlchemy engine, session factory, and model base for SQLite with WAL mode."""

import logging
from collections.abc import AsyncGenerator

from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from src.config import settings

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


engine = create_async_engine(
    settings.database_url,
    echo=False,
)


@event.listens_for(engine.sync_engine, "connect")
def _set_sqlite_pragmas(dbapi_connection, connection_record) -> None:
    """Enable WAL mode and other SQLite performance pragmas on every connection."""
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.execute("PRAGMA busy_timeout=5000")
    cursor.close()


async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Yield an async database session for dependency injection."""
    async with async_session() as session:
        yield session


async def init_db() -> None:
    """Create all tables and verify WAL mode is active."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Verify WAL mode
    async with async_session() as session:
        result = await session.execute(text("PRAGMA journal_mode"))
        mode = result.scalar()
        logger.info("SQLite journal_mode: %s", mode)
