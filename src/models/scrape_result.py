"""Scraping history log model for debugging."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.database import Base

if TYPE_CHECKING:
    from src.models.media_item import MediaItem


class ScrapeLog(Base):
    __tablename__ = "scrape_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    media_item_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("media_items.id"), nullable=True, index=True
    )
    scraper: Mapped[str] = mapped_column(String, nullable=False)
    query_params: Mapped[str | None] = mapped_column("query_params", Text, nullable=True)
    results_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    results_summary: Mapped[str | None] = mapped_column("results_summary", Text, nullable=True)
    selected_result: Mapped[str | None] = mapped_column("selected_result", Text, nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    scraped_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), nullable=False
    )

    # Relationships
    media_item: Mapped[MediaItem | None] = relationship(back_populates="scrape_logs")

    def __repr__(self) -> str:
        return f"<ScrapeLog {self.scraper} results={self.results_count}>"
