"""Real-Debrid torrent tracking model."""

from __future__ import annotations

import enum
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, DateTime, Enum, ForeignKey, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.database import Base

if TYPE_CHECKING:
    from src.models.media_item import MediaItem


class TorrentStatus(str, enum.Enum):
    ACTIVE = "active"
    REMOVED = "removed"
    REPLACED = "replaced"


class RdTorrent(Base):
    __tablename__ = "rd_torrents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    rd_id: Mapped[str | None] = mapped_column(String, nullable=True)
    info_hash: Mapped[str | None] = mapped_column(String, unique=True, nullable=True, index=True)
    magnet_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    media_item_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("media_items.id"), nullable=True
    )
    filename: Mapped[str | None] = mapped_column(String, nullable=True)
    filesize: Mapped[int | None] = mapped_column(Integer, nullable=True)
    resolution: Mapped[str | None] = mapped_column(String, nullable=True)
    cached: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    added_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), nullable=False
    )
    status: Mapped[TorrentStatus] = mapped_column(
        Enum(TorrentStatus), default=TorrentStatus.ACTIVE, nullable=False
    )

    # Relationships
    media_item: Mapped[MediaItem | None] = relationship(back_populates="torrents")

    def __repr__(self) -> str:
        hash_prefix = self.info_hash[:8] if self.info_hash else "no-hash"
        return f"<RdTorrent {hash_prefix}... [{self.status.value}]>"
