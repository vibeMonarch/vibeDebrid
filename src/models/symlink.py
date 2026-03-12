"""Symlink tracking model."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.database import Base

if TYPE_CHECKING:
    from src.models.media_item import MediaItem


class Symlink(Base):
    __tablename__ = "symlinks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    media_item_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("media_items.id"), nullable=True, index=True
    )
    source_path: Mapped[str] = mapped_column(String, nullable=False)
    target_path: Mapped[str] = mapped_column(String, nullable=False)
    valid: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), nullable=False
    )
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    # Relationships
    media_item: Mapped[MediaItem | None] = relationship(back_populates="symlinks")

    def __repr__(self) -> str:
        return f"<Symlink {self.target_path} valid={self.valid}>"
