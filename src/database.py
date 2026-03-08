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


async def _migrate_add_columns() -> None:
    """Add columns introduced after initial schema creation."""
    async with engine.begin() as conn:
        # is_season_pack (added for season pack feature)
        try:
            await conn.execute(
                text("ALTER TABLE media_items ADD COLUMN is_season_pack BOOLEAN NOT NULL DEFAULT 0")
            )
            logger.info("Migration: added is_season_pack column")
        except Exception as exc:
            if "duplicate column" in str(exc).lower():
                pass  # Column already exists
            else:
                logger.warning("Migration: failed to add is_season_pack column: %s", exc)

        # tmdb_id index (added for discovery feature batch lookups)
        try:
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS ix_media_items_tmdb_id ON media_items (tmdb_id)")
            )
            logger.info("Migration: created tmdb_id index")
        except Exception as exc:
            if "already exists" in str(exc).lower():
                pass
            else:
                logger.warning("Migration: failed to create tmdb_id index: %s", exc)

        # tvdb_absolute on xem_cache (added for TMDB→scene absolute mapping)
        try:
            await conn.execute(text("ALTER TABLE xem_cache ADD COLUMN tvdb_absolute INTEGER"))
            logger.info("Migration: added tvdb_absolute column to xem_cache")
        except Exception as exc:
            if "duplicate column" not in str(exc).lower():
                logger.warning("Migration tvdb_absolute: %s", exc)

        # tvdb_id (added for XEM scene numbering support)
        try:
            await conn.execute(text("ALTER TABLE media_items ADD COLUMN tvdb_id INTEGER"))
            logger.info("Migration: added tvdb_id column")
        except Exception as exc:
            if "duplicate column" not in str(exc).lower():
                logger.warning("Migration tvdb_id: %s", exc)


async def init_db() -> None:
    """Create all tables and verify WAL mode is active."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await _migrate_add_columns()

    # Verify WAL mode
    async with async_session() as session:
        result = await session.execute(text("PRAGMA journal_mode"))
        mode = result.scalar()
        logger.info("SQLite journal_mode: %s", mode)
