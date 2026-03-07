"""XEM scene numbering cache model."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Integer, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from src.database import Base


class XemCacheEntry(Base):
    """Cached XEM episode mapping between TVDB and scene numbering."""

    __tablename__ = "xem_cache"
    __table_args__ = (
        UniqueConstraint("tvdb_id", "tvdb_season", "tvdb_episode", name="uq_xem_mapping"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tvdb_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    tvdb_season: Mapped[int] = mapped_column(Integer, nullable=False)
    tvdb_episode: Mapped[int] = mapped_column(Integer, nullable=False)
    scene_season: Mapped[int] = mapped_column(Integer, nullable=False)
    scene_episode: Mapped[int] = mapped_column(Integer, nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), nullable=False
    )

    def __repr__(self) -> str:
        return (
            f"<XemCacheEntry tvdb_id={self.tvdb_id} "
            f"S{self.tvdb_season:02d}E{self.tvdb_episode:02d} → "
            f"S{self.scene_season:02d}E{self.scene_episode:02d}>"
        )
