"""Zurg mount file index model."""

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Integer, String, func, text
from sqlalchemy.orm import Mapped, mapped_column

from src.database import Base


class MountIndex(Base):
    __tablename__ = "mount_index"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    filepath: Mapped[str] = mapped_column(String, unique=True, nullable=False, index=True)
    filename: Mapped[str] = mapped_column(String, nullable=False)
    parsed_title: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    parsed_year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    parsed_season: Mapped[int | None] = mapped_column(Integer, nullable=True)
    parsed_episode: Mapped[int | None] = mapped_column(Integer, nullable=True)
    parsed_resolution: Mapped[str | None] = mapped_column(String, nullable=True)
    parsed_codec: Mapped[str | None] = mapped_column(String, nullable=True)
    filesize: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    norm_version: Mapped[int] = mapped_column(default=3, server_default=text("3"))
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), nullable=False
    )

    def __repr__(self) -> str:
        return f"<MountIndex {self.filename}>"
