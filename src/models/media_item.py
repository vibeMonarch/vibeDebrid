"""Core media item model with queue state."""

from __future__ import annotations

import enum
from datetime import date, datetime
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, Date, DateTime, Enum, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.database import Base

if TYPE_CHECKING:
    from src.models.scrape_result import ScrapeLog
    from src.models.symlink import Symlink
    from src.models.torrent import RdTorrent


class MediaType(str, enum.Enum):
    MOVIE = "movie"
    SHOW = "show"


class QueueState(str, enum.Enum):
    UNRELEASED = "unreleased"
    WANTED = "wanted"
    SCRAPING = "scraping"
    ADDING = "adding"
    CHECKING = "checking"
    SLEEPING = "sleeping"
    DORMANT = "dormant"
    COMPLETE = "complete"
    DONE = "done"


class MediaItem(Base):
    __tablename__ = "media_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    imdb_id: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    tmdb_id: Mapped[str | None] = mapped_column(String, nullable=True)
    tvdb_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    title: Mapped[str] = mapped_column(String, nullable=False)
    year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    media_type: Mapped[MediaType] = mapped_column(Enum(MediaType), nullable=False)
    season: Mapped[int | None] = mapped_column(Integer, nullable=True)
    episode: Mapped[int | None] = mapped_column(Integer, nullable=True)
    is_season_pack: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    state: Mapped[QueueState] = mapped_column(
        Enum(QueueState), nullable=False, default=QueueState.WANTED, index=True
    )
    quality_profile: Mapped[str | None] = mapped_column(String, nullable=True)
    requested_resolution: Mapped[str | None] = mapped_column(String, nullable=True)
    added_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    state_changed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    source: Mapped[str | None] = mapped_column(String, nullable=True)
    metadata_json: Mapped[str | None] = mapped_column("metadata", Text, nullable=True)
    air_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    original_language: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )

    # Relationships (use string class names to avoid circular imports)
    torrents: Mapped[list[RdTorrent]] = relationship(back_populates="media_item")
    scrape_logs: Mapped[list[ScrapeLog]] = relationship(back_populates="media_item")
    symlinks: Mapped[list[Symlink]] = relationship(back_populates="media_item")

    def __repr__(self) -> str:
        if self.media_type == MediaType.SHOW and self.season is not None:
            ep = self.episode or 0
            return f"<MediaItem {self.title} S{self.season:02d}E{ep:02d} [{self.state.value}]>"
        return f"<MediaItem {self.title} ({self.year}) [{self.state.value}]>"
