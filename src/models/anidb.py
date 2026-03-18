"""AniDB title and mapping cache models."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Integer, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from src.database import Base


class AnidbTitle(Base):
    """Cached AniDB title variants for a given anime entry."""

    __tablename__ = "anidb_titles"
    __table_args__ = (UniqueConstraint("anidb_id", "title", name="uq_anidb_title"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    anidb_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    title: Mapped[str] = mapped_column(String, nullable=False)
    title_type: Mapped[str] = mapped_column(String, nullable=False)  # main, official, short, syn
    language: Mapped[str] = mapped_column(String, nullable=False)     # en, ja, x-jat, etc.
    fetched_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)

    def __repr__(self) -> str:
        return (
            f"<AnidbTitle anidb_id={self.anidb_id} lang={self.language!r} "
            f"type={self.title_type!r} title={self.title!r}>"
        )


class AnidbMapping(Base):
    """Cached mapping between AniDB IDs and TMDB/TVDB/IMDb identifiers."""

    __tablename__ = "anidb_mappings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    anidb_id: Mapped[int] = mapped_column(Integer, nullable=False, unique=True, index=True)
    tmdb_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    tmdb_season: Mapped[int | None] = mapped_column(Integer, nullable=True)
    tvdb_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    imdb_id: Mapped[str | None] = mapped_column(String, nullable=True)
    episode_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)

    def __repr__(self) -> str:
        return (
            f"<AnidbMapping anidb_id={self.anidb_id} tmdb_id={self.tmdb_id} "
            f"tmdb_season={self.tmdb_season}>"
        )
