"""Shared pytest fixtures for the vibeDebrid test suite.

Provides an in-memory SQLite engine and async session so every test module
gets a fresh, isolated database without touching the real SQLite file on disk.
All tables are created at engine-setup time and each session is rolled back
after the test completes.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.database import Base
from src.models.media_item import MediaItem, MediaType, QueueState

# Import all models so their metadata is registered on Base before create_all
import src.models.torrent  # noqa: F401
import src.models.scrape_result  # noqa: F401
import src.models.symlink  # noqa: F401
import src.models.mount_index  # noqa: F401
import src.models.monitored_show  # noqa: F401
import src.models.xem_cache  # noqa: F401


@pytest.fixture
async def engine():
    """In-memory async SQLite engine with all tables created."""
    eng = create_async_engine("sqlite+aiosqlite://", echo=False)
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield eng
    await eng.dispose()


@pytest.fixture
async def session(engine):
    """Async database session that is rolled back after each test."""
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as sess:
        yield sess
        await sess.rollback()


@pytest.fixture
async def wanted_item(session: AsyncSession) -> MediaItem:
    """A MediaItem in WANTED state persisted to the test database."""
    item = MediaItem(
        imdb_id="tt1234567",
        title="Test Movie",
        year=2024,
        media_type=MediaType.MOVIE,
        state=QueueState.WANTED,
        state_changed_at=datetime.now(timezone.utc),
        retry_count=0,
    )
    session.add(item)
    await session.flush()
    return item


@pytest.fixture
async def unreleased_item(session: AsyncSession) -> MediaItem:
    """A MediaItem in UNRELEASED state with a past air_date."""
    item = MediaItem(
        imdb_id="tt9999991",
        title="Unreleased Movie",
        year=2023,
        media_type=MediaType.MOVIE,
        state=QueueState.UNRELEASED,
        state_changed_at=datetime.now(timezone.utc),
        retry_count=0,
        air_date=date(2023, 1, 1),  # well in the past
    )
    session.add(item)
    await session.flush()
    return item


@pytest.fixture
async def sleeping_item(session: AsyncSession) -> MediaItem:
    """A MediaItem in SLEEPING state with retry_count=1."""
    item = MediaItem(
        imdb_id="tt9999992",
        title="Sleeping Movie",
        year=2024,
        media_type=MediaType.MOVIE,
        state=QueueState.SLEEPING,
        state_changed_at=datetime.now(timezone.utc),
        retry_count=1,
    )
    session.add(item)
    await session.flush()
    return item


@pytest.fixture
async def dormant_item(session: AsyncSession) -> MediaItem:
    """A MediaItem in DORMANT state."""
    item = MediaItem(
        imdb_id="tt9999993",
        title="Dormant Movie",
        year=2024,
        media_type=MediaType.MOVIE,
        state=QueueState.DORMANT,
        state_changed_at=datetime.now(timezone.utc),
        retry_count=7,
    )
    session.add(item)
    await session.flush()
    return item
