"""Library migration tool for vibeDebrid.

Scans an external media library directory (movies + shows), imports found
items into the vibeDebrid database as COMPLETE, detects duplicates against
existing items, moves vibeDebrid-managed symlinks to new library locations,
and updates config paths.

This module is filesystem-only — it has no dependency on any external database
or tool (CLI Debrid, etc.).  It works with any library folder layout that
follows standard conventions (Plex, Emby, Jellyfin, scene naming).

Key design rules:
- All filesystem I/O via ``asyncio.to_thread`` — never block the event loop.
- ``session.flush()`` but never ``session.commit()`` — callers own transactions.
- Duplicate detection uses source_path (strongest) then title+year+type+episode.
- Non-symlink files produce a MediaItem only; symlinks get a Symlink record too.
- Config update uses the shared ``config_lock`` from ``src.config``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import CONFIG_FILE, Settings, config_lock, settings
from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.symlink import Symlink

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VIDEO_EXTENSIONS: frozenset[str] = frozenset(
    {".mkv", ".mp4", ".avi", ".m4v", ".ts", ".wmv", ".flv", ".mov"}
)

# Scene-name tokens that signal the end of the title portion.
_SCENE_STOP_TOKENS: frozenset[str] = frozenset(
    {
        # Resolutions
        "2160p", "1080p", "720p", "480p", "576p", "4k", "uhd",
        # Sources
        "bluray", "blu-ray", "bdrip", "brrip", "web-dl", "webdl", "webrip",
        "web", "hdtv", "dvdrip", "dvd", "hdcam", "cam",
        # Codecs
        "x264", "x265", "h264", "h265", "hevc", "avc", "av1", "xvid", "divx",
        # Audio
        "aac", "ac3", "dts", "truehd", "atmos", "flac", "mp3", "dd5", "ddp5",
        # Misc
        "repack", "proper", "extended", "theatrical", "dc", "unrated",
        "remux", "hdr", "hdr10", "dv", "dolby", "imax",
    }
)

# Year range for scene-name detection.
_YEAR_RE = re.compile(r"\b(19[0-9]{2}|20[0-9]{2})\b")
_PAREN_YEAR_RE = re.compile(r"\(((19|20)\d{2})\)")
_BRACKET_YEAR_RE = re.compile(r"\[((19|20)\d{2})\]")
_IMDB_RE = re.compile(r"\btt(\d{7,})\b", re.IGNORECASE)
_EPISODE_RE = re.compile(r"[Ss](\d{1,2})[Ee](\d{1,3})")
_SEASON_DIR_RE = re.compile(r"[Ss]eason\s*(\d{1,2})", re.IGNORECASE)
_RESOLUTION_RE = re.compile(r"\b(2160p|1080p|720p|480p|576p)\b", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class FoundItem:
    """A media item discovered during a library scan.

    Attributes:
        title: Parsed media title.
        year: Release year, or None when not found.
        media_type: ``"movie"`` or ``"show"``.
        season: Season number for show episodes, None for movies.
        episode: Episode number, None for movies / season packs.
        imdb_id: IMDB ID extracted from the filename (``ttXXXXXXX``), or None.
        source_path: Absolute path the symlink resolves to (readlink target),
            or None when the file is not a symlink.
        target_path: Absolute path of the file or symlink itself.
        is_symlink: True when ``target_path`` is a symlink.
        resolution: Resolution string (e.g. ``"1080p"``), or None.
    """

    title: str
    year: int | None
    media_type: str
    season: int | None
    episode: int | None
    imdb_id: str | None
    source_path: str | None
    target_path: str
    is_symlink: bool
    resolution: str | None


@dataclass
class DuplicateMatch:
    """A found item that already exists in the vibeDebrid database.

    Attributes:
        found_item: The scanned item.
        existing_id: Primary key of the matching ``MediaItem``.
        existing_title: Title of the matching ``MediaItem``.
        match_reason: ``"same_source_path"`` or ``"title_year_match"``.
    """

    found_item: FoundItem
    existing_id: int
    existing_title: str
    match_reason: str


@dataclass
class MigrationPreview:
    """Preview of what a migration would do.

    Attributes:
        found_items: All items discovered in the library paths.
        duplicates: Items that already exist in vibeDebrid's DB.
        to_move: vibeDebrid ``MediaItem`` dicts whose symlinks are NOT under
            the target library paths (they would be relocated).
        errors: Human-readable descriptions of items that could not be parsed.
        summary: Aggregated counts for the UI.
    """

    found_items: list[FoundItem]
    duplicates: list[DuplicateMatch]
    to_move: list[dict[str, Any]]
    errors: list[str]
    summary: dict[str, int]


@dataclass
class MigrationResult:
    """Result of executing a migration.

    Attributes:
        imported: Number of new ``MediaItem`` records created.
        moved: Number of vibeDebrid symlinks relocated to new library paths.
        duplicates_removed: Number of vibeDebrid items removed because a copy
            already existed at the target location.
        config_updated: True when ``config.json`` paths were rewritten.
        errors: Non-fatal errors encountered during execution.
    """

    imported: int
    moved: int
    duplicates_removed: int
    config_updated: bool
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def parse_media_name(name: str) -> tuple[str, int | None]:
    """Extract title and year from a media directory or file name.

    Tries three patterns in order:

    1. ``Title (Year)`` or ``Title (Year) [extras]`` — Plex / CLI Debrid format.
    2. ``Title [Year]`` — alternative bracket notation.
    3. Scene naming: ``Title.Year.Resolution...`` or ``Title Year Resolution...``.
       The year must be 1900–2099.  Parsing stops at the first resolution or
       codec token.  Titles starting with a year-like number are handled
       correctly (e.g. ``2001.A.Space.Odyssey.1968.1080p`` → ("2001 A Space
       Odyssey", 1968)).
    4. Fallback: return the entire name (dots replaced with spaces) as the
       title with ``year=None``.

    Args:
        name: Raw directory or file name (without extension).

    Returns:
        ``(title, year)`` where ``year`` is an ``int`` or ``None``.
    """
    if not name:
        return ("", None)

    # Strip leading timestamp prefix produced by vibeDebrid's own naming.
    # Format: 12 digits followed by a space, e.g. "202603011430 Movie (2024)".
    stripped = re.sub(r"^\d{12}\s+", "", name)

    # Pattern 1: Title (Year) — possibly followed by more tokens
    m = _PAREN_YEAR_RE.search(stripped)
    if m:
        title = stripped[: m.start()].strip(" .-")
        # Replace dots with spaces (scene-ish titles before the year)
        title = title.replace(".", " ").strip()
        return (title if title else stripped, int(m.group(1)))

    # Pattern 2: Title [Year]
    m = _BRACKET_YEAR_RE.search(stripped)
    if m:
        title = stripped[: m.start()].strip(" .-")
        title = title.replace(".", " ").strip()
        return (title if title else stripped, int(m.group(1)))

    # Pattern 3: Scene naming — dots or spaces as separators
    # Normalise separators to spaces for uniform processing.
    normalised = stripped.replace(".", " ")
    tokens = normalised.split()

    title_tokens: list[str] = []
    found_year: int | None = None

    # We need to find the *last* year token that is followed by a stop token
    # (or is at/near the end), and treat everything before it as the title.
    # This correctly handles titles that START with a 4-digit number like "1917"
    # or "2001 A Space Odyssey".

    for i, token in enumerate(tokens):
        token_lower = token.lower().rstrip("-")
        if token_lower in _SCENE_STOP_TOKENS:
            # Everything collected so far is the title; stop here.
            break
        # Check if this token looks like a year (4 digits, 1900-2099).
        if re.fullmatch(r"(19|20)\d{2}", token):
            # Peek ahead: is the next meaningful token a stop token or end?
            rest = [t.lower().rstrip("-") for t in tokens[i + 1 :]]
            next_is_stop = not rest or any(t in _SCENE_STOP_TOKENS for t in rest[:3])
            if next_is_stop:
                found_year = int(token)
                # Title is everything collected before this year.
                break
        title_tokens.append(token)

    if title_tokens:
        title = " ".join(title_tokens).strip()
        return (title, found_year)

    # Fallback: whole name with dots replaced by spaces
    return (normalised.strip(), None)


def extract_imdb_id(filename: str) -> str | None:
    """Extract an IMDB ID from a filename or directory name.

    Matches patterns like ``tt1234567`` or ``tt12345678`` anywhere in the
    string (case-insensitive).

    Args:
        filename: Any string (file name, directory name, full path).

    Returns:
        The full IMDB ID string (``"tt..."``), or ``None`` when not found.
    """
    m = _IMDB_RE.search(filename)
    if m:
        return f"tt{m.group(1)}"
    return None


def parse_episode_info(filename: str) -> tuple[int | None, int | None]:
    """Parse season and episode numbers from a filename.

    Matches the common ``S01E05`` pattern (case-insensitive).

    Args:
        filename: File name or path string.

    Returns:
        ``(season, episode)`` as integers, or ``(None, None)`` when no match.
    """
    m = _EPISODE_RE.search(filename)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    return (None, None)


def parse_season_number(dirname: str) -> int | None:
    """Parse a season number from a directory name.

    Matches patterns like ``Season 01``, ``Season 1``, ``season02``
    (case-insensitive).

    Args:
        dirname: Directory name string.

    Returns:
        Season number as an integer, or ``None`` when no match.
    """
    m = _SEASON_DIR_RE.search(dirname)
    if m:
        return int(m.group(1))
    return None


def extract_resolution(name: str) -> str | None:
    """Find and return a resolution string (e.g. ``"1080p"``) in *name*.

    Args:
        name: Any string (file name, directory name).

    Returns:
        Resolution string (normalised to lower-case), or ``None``.
    """
    m = _RESOLUTION_RE.search(name)
    if m:
        return m.group(1).lower()
    return None


# ---------------------------------------------------------------------------
# Normalisation helper for duplicate detection
# ---------------------------------------------------------------------------


def _normalise_title(title: str) -> str:
    """Normalise a title for comparison: lowercase, strip punctuation, collapse whitespace.

    Args:
        title: Raw title string.

    Returns:
        Normalised string suitable for equality comparison.
    """
    # NFKD normalisation strips accents etc.
    nfkd = unicodedata.normalize("NFKD", title)
    # Remove non-ASCII characters and punctuation, keep alphanumerics + spaces.
    cleaned = re.sub(r"[^a-z0-9\s]", "", nfkd.lower())
    return re.sub(r"\s+", " ", cleaned).strip()


# ---------------------------------------------------------------------------
# Filesystem scan helpers (run in threads)
# ---------------------------------------------------------------------------


def _scan_movie_dir(movies_path: str) -> tuple[list[FoundItem], list[str]]:
    """Synchronous helper: scan *movies_path* for movie entries.

    Called via ``asyncio.to_thread``.

    Each immediate subdirectory of *movies_path* is treated as one movie.
    Video files inside it are enumerated; the first video file determines
    the symlink/source_path and IMDB ID for the item.

    Args:
        movies_path: Absolute path to the movies root directory.

    Returns:
        ``(found_items, errors)`` tuple.
    """
    items: list[FoundItem] = []
    errors: list[str] = []

    try:
        entries = os.scandir(movies_path)
    except OSError as exc:
        errors.append(f"Cannot scan movies path {movies_path!r}: {exc}")
        return items, errors

    for entry in sorted(entries, key=lambda e: e.name):
        if not entry.is_dir(follow_symlinks=False):
            continue

        movie_dir = entry.path
        dir_name = entry.name

        # Parse title + year from directory name.
        title, year = parse_media_name(dir_name)
        if not title:
            errors.append(f"Could not parse title from movie dir: {dir_name!r}")
            continue

        # Find video files inside this directory.
        try:
            file_entries = list(os.scandir(movie_dir))
        except OSError as exc:
            errors.append(f"Cannot read movie dir {movie_dir!r}: {exc}")
            continue

        video_files = [
            fe for fe in file_entries
            if fe.is_file(follow_symlinks=False) or fe.is_symlink()
            if os.path.splitext(fe.name)[1].lower() in VIDEO_EXTENSIONS
        ]

        if not video_files:
            # No video files — skip this directory silently (could be extras only).
            logger.debug("No video files in movie dir %r, skipping", movie_dir)
            continue

        # Process each video file.  Usually there is only one per movie dir.
        for fe in video_files:
            file_path = fe.path
            is_symlink = os.path.islink(file_path)
            source_path: str | None = None
            if is_symlink:
                try:
                    source_path = os.readlink(file_path)
                except OSError as exc:
                    errors.append(f"readlink failed for {file_path!r}: {exc}")

            imdb_id = extract_imdb_id(fe.name) or extract_imdb_id(dir_name)
            resolution = extract_resolution(fe.name) or extract_resolution(dir_name)

            items.append(
                FoundItem(
                    title=title,
                    year=year,
                    media_type="movie",
                    season=None,
                    episode=None,
                    imdb_id=imdb_id,
                    source_path=source_path,
                    target_path=file_path,
                    is_symlink=is_symlink,
                    resolution=resolution,
                )
            )

    return items, errors


def _scan_shows_dir(shows_path: str) -> tuple[list[FoundItem], list[str]]:
    """Synchronous helper: scan *shows_path* for TV episode entries.

    Called via ``asyncio.to_thread``.

    Structure expected::

        shows_path/
          Show Name (Year)/
            Season 01/
              Show.S01E01.mkv -> /zurg/mount/...
            Season 02/
              ...

    Args:
        shows_path: Absolute path to the shows root directory.

    Returns:
        ``(found_items, errors)`` tuple.
    """
    items: list[FoundItem] = []
    errors: list[str] = []

    try:
        show_entries = os.scandir(shows_path)
    except OSError as exc:
        errors.append(f"Cannot scan shows path {shows_path!r}: {exc}")
        return items, errors

    for show_entry in sorted(show_entries, key=lambda e: e.name):
        if not show_entry.is_dir(follow_symlinks=False):
            continue

        show_dir = show_entry.path
        show_dir_name = show_entry.name
        title, year = parse_media_name(show_dir_name)
        if not title:
            errors.append(f"Could not parse title from show dir: {show_dir_name!r}")
            continue

        # Walk season subdirectories.
        try:
            season_entries = list(os.scandir(show_dir))
        except OSError as exc:
            errors.append(f"Cannot read show dir {show_dir!r}: {exc}")
            continue

        for season_entry in sorted(season_entries, key=lambda e: e.name):
            if not season_entry.is_dir(follow_symlinks=False):
                continue

            season_num = parse_season_number(season_entry.name)
            if season_num is None:
                # Could be "Specials", "Extras", etc.  Skip gracefully.
                logger.debug(
                    "Skipping non-season directory %r in show %r",
                    season_entry.name,
                    show_dir_name,
                )
                continue

            season_dir = season_entry.path

            try:
                ep_entries = list(os.scandir(season_dir))
            except OSError as exc:
                errors.append(f"Cannot read season dir {season_dir!r}: {exc}")
                continue

            for ep_entry in ep_entries:
                if not (ep_entry.is_file(follow_symlinks=False) or ep_entry.is_symlink()):
                    continue
                if os.path.splitext(ep_entry.name)[1].lower() not in VIDEO_EXTENSIONS:
                    continue

                file_path = ep_entry.path
                is_symlink = os.path.islink(file_path)
                source_path: str | None = None
                if is_symlink:
                    try:
                        source_path = os.readlink(file_path)
                    except OSError as exc:
                        errors.append(f"readlink failed for {file_path!r}: {exc}")

                ep_season, ep_episode = parse_episode_info(ep_entry.name)
                imdb_id = extract_imdb_id(ep_entry.name) or extract_imdb_id(show_dir_name)
                resolution = extract_resolution(ep_entry.name) or extract_resolution(show_dir_name)

                items.append(
                    FoundItem(
                        title=title,
                        year=year,
                        media_type="show",
                        season=ep_season if ep_season is not None else season_num,
                        episode=ep_episode,
                        imdb_id=imdb_id,
                        source_path=source_path,
                        target_path=file_path,
                        is_symlink=is_symlink,
                        resolution=resolution,
                    )
                )

    return items, errors


# ---------------------------------------------------------------------------
# Public async API
# ---------------------------------------------------------------------------


async def scan_library(
    movies_path: str, shows_path: str
) -> tuple[list[FoundItem], list[str]]:
    """Scan *movies_path* and *shows_path* for media items.

    All filesystem I/O runs in a thread pool so the event loop is never
    blocked.

    Args:
        movies_path: Absolute path to the movies root directory.
        shows_path: Absolute path to the shows root directory.

    Returns:
        ``(found_items, errors)`` where ``errors`` contains human-readable
        descriptions of items that could not be parsed or accessed.
    """
    movie_items, movie_errors = await asyncio.to_thread(_scan_movie_dir, movies_path)
    show_items, show_errors = await asyncio.to_thread(_scan_shows_dir, shows_path)

    all_items = movie_items + show_items
    all_errors = movie_errors + show_errors

    logger.info(
        "scan_library: found %d movies, %d show episodes, %d errors",
        len(movie_items),
        len(show_items),
        len(all_errors),
    )
    return all_items, all_errors


async def preview_migration(
    session: AsyncSession,
    movies_path: str,
    shows_path: str,
) -> MigrationPreview:
    """Scan library paths and compute what a migration would do.

    Does NOT modify the database or filesystem.

    Args:
        session: Caller-managed async database session.
        movies_path: Absolute path to the target movies library.
        shows_path: Absolute path to the target shows library.

    Returns:
        A ``MigrationPreview`` describing found items, duplicates, items
        that would be moved, errors, and a summary dict.
    """
    # --- Scan filesystem ---
    found_items, errors = await scan_library(movies_path, shows_path)

    # --- Load existing vibeDebrid DB items ---
    result = await session.execute(select(MediaItem))
    existing_items: list[MediaItem] = list(result.scalars().all())

    # Build fast-lookup structures from existing items.
    # source_path → existing MediaItem (only for items that have symlinks)
    symlink_result = await session.execute(select(Symlink))
    existing_symlinks: list[Symlink] = list(symlink_result.scalars().all())

    # Build the id→MediaItem map once, outside the symlink loop (O(n) not O(n²)).
    _preview_item_map: dict[int, MediaItem] = {mi.id: mi for mi in existing_items}

    source_path_to_item: dict[str, MediaItem] = {}
    for sl in existing_symlinks:
        if sl.media_item_id is not None:
            mi = _preview_item_map.get(sl.media_item_id)
            if mi is not None:
                source_path_to_item[sl.source_path] = mi

    # Normalised key → existing MediaItem for title+year+type+episode matching.
    def _item_key(
        title: str,
        year: int | None,
        media_type: str,
        season: int | None,
        episode: int | None,
    ) -> str:
        return f"{_normalise_title(title)}|{year}|{media_type}|{season}|{episode}"

    existing_key_map: dict[str, MediaItem] = {}
    for mi in existing_items:
        k = _item_key(mi.title, mi.year, mi.media_type.value, mi.season, mi.episode)
        existing_key_map[k] = mi

    # --- Duplicate detection ---
    duplicates: list[DuplicateMatch] = []
    non_duplicate_items: list[FoundItem] = []

    for fi in found_items:
        # Primary: match by symlink source_path
        if fi.source_path and fi.source_path in source_path_to_item:
            existing_mi = source_path_to_item[fi.source_path]
            duplicates.append(
                DuplicateMatch(
                    found_item=fi,
                    existing_id=existing_mi.id,
                    existing_title=existing_mi.title,
                    match_reason="same_source_path",
                )
            )
            continue

        # Secondary: normalised title + year + type + season + episode
        k = _item_key(fi.title, fi.year, fi.media_type, fi.season, fi.episode)
        if k in existing_key_map:
            existing_mi = existing_key_map[k]
            duplicates.append(
                DuplicateMatch(
                    found_item=fi,
                    existing_id=existing_mi.id,
                    existing_title=existing_mi.title,
                    match_reason="title_year_match",
                )
            )
            continue

        non_duplicate_items.append(fi)

    # --- Identify vibeDebrid items that need to be moved ---
    # An item needs to move if it has symlinks whose target_path is NOT under
    # movies_path or shows_path.  (After migration the user wants everything
    # under the new paths.)
    norm_movies = os.path.normpath(movies_path)
    norm_shows = os.path.normpath(shows_path)

    item_id_to_mi: dict[int, MediaItem] = {mi.id: mi for mi in existing_items}
    items_needing_move: dict[int, dict[str, Any]] = {}

    for sl in existing_symlinks:
        norm_target = os.path.normpath(sl.target_path)
        under_movies = norm_target.startswith(norm_movies + os.sep) or norm_target == norm_movies
        under_shows = norm_target.startswith(norm_shows + os.sep) or norm_target == norm_shows
        if not under_movies and not under_shows:
            if sl.media_item_id is not None and sl.media_item_id not in items_needing_move:
                mi = item_id_to_mi.get(sl.media_item_id)
                if mi is not None:
                    items_needing_move[mi.id] = {
                        "id": mi.id,
                        "title": mi.title,
                        "year": mi.year,
                        "media_type": mi.media_type.value,
                        "season": mi.season,
                        "episode": mi.episode,
                        "state": mi.state.value,
                    }

    to_move = list(items_needing_move.values())

    # --- Build summary ---
    found_movies = sum(1 for fi in found_items if fi.media_type == "movie")
    found_shows = sum(1 for fi in found_items if fi.media_type == "show")
    summary: dict[str, int] = {
        "found_movies": found_movies,
        "found_shows": found_shows,
        "found_episodes": found_shows,
        "duplicate_count": len(duplicates),
        "to_import_count": len(non_duplicate_items),
        "to_move_count": len(to_move),
        "error_count": len(errors),
    }

    return MigrationPreview(
        found_items=found_items,
        duplicates=duplicates,
        to_move=to_move,
        errors=errors,
        summary=summary,
    )


async def execute_migration(
    session: AsyncSession,
    preview: MigrationPreview,
    movies_path: str,
    shows_path: str,
) -> MigrationResult:
    """Execute the migration described by *preview*.

    Operations performed (in order):

    1. **Import** — create ``MediaItem`` + optional ``Symlink`` for every
       non-duplicate ``FoundItem``.
    2. **Remove duplicates** — delete vibeDebrid's ``Symlink`` (filesystem +
       DB) and ``MediaItem`` for each duplicate.
    3. **Move** — relocate vibeDebrid symlinks to the new library paths,
       update ``Symlink.target_path`` in the DB.
    4. **Update config** — write new ``paths.library_movies`` and
       ``paths.library_shows`` to ``config.json`` and reload the in-memory
       ``settings`` singleton.

    ``session.flush()`` is called after each batch; ``session.commit()`` is
    intentionally NOT called — the caller owns the transaction.

    Args:
        session: Caller-managed async database session.
        preview: The ``MigrationPreview`` returned by ``preview_migration``.
        movies_path: Absolute path to the target movies library.
        shows_path: Absolute path to the target shows library.

    Returns:
        A ``MigrationResult`` summarising what was done.
    """
    from src.core.symlink_manager import build_movie_dir, build_show_dir

    result = MigrationResult(
        imported=0,
        moved=0,
        duplicates_removed=0,
        config_updated=False,
    )
    now = datetime.now(timezone.utc)

    # Determine which found items to import (non-duplicates).
    duplicate_target_paths = {dm.found_item.target_path for dm in preview.duplicates}
    items_to_import = [
        fi for fi in preview.found_items
        if fi.target_path not in duplicate_target_paths
    ]

    # -----------------------------------------------------------------------
    # Step 1: Import
    # -----------------------------------------------------------------------
    logger.info("execute_migration: importing %d items", len(items_to_import))
    for fi in items_to_import:
        try:
            media_type = MediaType(fi.media_type)
            new_item = MediaItem(
                title=fi.title,
                year=fi.year,
                media_type=media_type,
                season=fi.season,
                episode=fi.episode,
                imdb_id=fi.imdb_id,
                state=QueueState.COMPLETE,
                source="migration",
                added_at=now,
                state_changed_at=now,
            )
            session.add(new_item)
            await session.flush()  # Obtain new_item.id before creating Symlink.

            if fi.is_symlink and fi.source_path:
                symlink = Symlink(
                    media_item_id=new_item.id,
                    source_path=fi.source_path,
                    target_path=fi.target_path,
                    valid=True,
                )
                session.add(symlink)
                await session.flush()

            result.imported += 1
            logger.debug(
                "execute_migration: imported %r (%s) id=%s",
                fi.title,
                fi.media_type,
                new_item.id,
            )
        except Exception as exc:
            msg = f"Failed to import {fi.target_path!r}: {exc}"
            logger.warning("execute_migration: %s", msg)
            # Roll back any partial state so the session remains usable for
            # subsequent iterations.
            await session.rollback()
            result.errors.append(msg)

    # -----------------------------------------------------------------------
    # Step 2: Remove duplicates
    # -----------------------------------------------------------------------
    logger.info("execute_migration: removing %d duplicates", len(preview.duplicates))
    for dup in preview.duplicates:
        try:
            # Remove vibeDebrid's symlinks for the existing item from filesystem + DB.
            existing_symlinks_result = await session.execute(
                select(Symlink).where(Symlink.media_item_id == dup.existing_id)
            )
            existing_symlinks = list(existing_symlinks_result.scalars().all())

            for sl in existing_symlinks:
                is_link = await asyncio.to_thread(os.path.islink, sl.target_path)
                if is_link:
                    try:
                        await asyncio.to_thread(os.unlink, sl.target_path)
                        logger.info(
                            "execute_migration: removed duplicate symlink %r",
                            sl.target_path,
                        )
                    except OSError as exc:
                        logger.warning(
                            "execute_migration: could not remove symlink %r: %s",
                            sl.target_path,
                            exc,
                        )
                await session.delete(sl)

            # Delete the MediaItem record.
            existing_mi_result = await session.execute(
                select(MediaItem).where(MediaItem.id == dup.existing_id)
            )
            existing_mi = existing_mi_result.scalar_one_or_none()
            if existing_mi is not None:
                await session.delete(existing_mi)

            await session.flush()
            result.duplicates_removed += 1
            logger.debug(
                "execute_migration: removed duplicate media_item_id=%s (%s)",
                dup.existing_id,
                dup.existing_title,
            )
        except Exception as exc:
            msg = f"Failed to remove duplicate id={dup.existing_id} ({dup.existing_title!r}): {exc}"
            logger.warning("execute_migration: %s", msg)
            result.errors.append(msg)

    # -----------------------------------------------------------------------
    # Step 3: Move vibeDebrid items to new library paths
    # -----------------------------------------------------------------------
    logger.info("execute_migration: moving %d items", len(preview.to_move))
    for item_dict in preview.to_move:
        item_id: int = item_dict["id"]
        try:
            # Fetch current symlinks for this item.
            symlinks_result = await session.execute(
                select(Symlink).where(Symlink.media_item_id == item_id)
            )
            symlinks = list(symlinks_result.scalars().all())

            mi_result = await session.execute(
                select(MediaItem).where(MediaItem.id == item_id)
            )
            mi = mi_result.scalar_one_or_none()
            if mi is None:
                logger.warning(
                    "execute_migration: media_item_id=%s not found, skipping move",
                    item_id,
                )
                continue

            for sl in symlinks:
                old_target = sl.target_path
                filename = os.path.basename(old_target)

                # Build new target directory using vibeDebrid naming convention.
                if mi.media_type == MediaType.MOVIE:
                    new_target_dir = await asyncio.to_thread(
                        build_movie_dir,
                        mi.title,
                        mi.year,
                        mi.requested_resolution,
                    )
                else:
                    season = mi.season if mi.season is not None else 1
                    new_target_dir = await asyncio.to_thread(
                        build_show_dir,
                        mi.title,
                        mi.year,
                        season,
                        mi.requested_resolution,
                    )

                new_target = os.path.join(new_target_dir, filename)

                # Skip if already in the right place.
                if os.path.normpath(new_target) == os.path.normpath(old_target):
                    logger.debug(
                        "execute_migration: symlink already at correct location %r",
                        new_target,
                    )
                    continue

                # Create parent directories.
                try:
                    await asyncio.to_thread(os.makedirs, new_target_dir, exist_ok=True)
                except OSError as exc:
                    msg = f"Could not create dir {new_target_dir!r}: {exc}"
                    logger.warning("execute_migration: %s", msg)
                    result.errors.append(msg)
                    continue

                # Create new symlink at new location.
                try:
                    await asyncio.to_thread(os.symlink, sl.source_path, new_target)
                except FileExistsError:
                    logger.debug(
                        "execute_migration: new symlink already exists at %r", new_target
                    )
                except OSError as exc:
                    msg = f"Could not create symlink {new_target!r} -> {sl.source_path!r}: {exc}"
                    logger.warning("execute_migration: %s", msg)
                    result.errors.append(msg)
                    continue

                # Remove old symlink from filesystem.
                old_is_link = await asyncio.to_thread(os.path.islink, old_target)
                if old_is_link:
                    try:
                        await asyncio.to_thread(os.unlink, old_target)
                        logger.info(
                            "execute_migration: moved %r -> %r",
                            old_target,
                            new_target,
                        )
                    except OSError as exc:
                        logger.warning(
                            "execute_migration: could not remove old symlink %r: %s",
                            old_target,
                            exc,
                        )

                # Update DB record.
                sl.target_path = new_target

            await session.flush()
            result.moved += 1

        except Exception as exc:
            msg = f"Failed to move item id={item_id}: {exc}"
            logger.warning("execute_migration: %s", msg)
            result.errors.append(msg)

    # -----------------------------------------------------------------------
    # Step 4: Update config.json with new library paths
    # -----------------------------------------------------------------------
    async with config_lock:
        try:
            existing_cfg: dict[str, Any] = {}
            cfg_exists = await asyncio.to_thread(CONFIG_FILE.exists)
            if cfg_exists:
                raw = await asyncio.to_thread(CONFIG_FILE.read_text)
                existing_cfg = json.loads(raw)

            existing_cfg.setdefault("paths", {})
            existing_cfg["paths"]["library_movies"] = movies_path
            existing_cfg["paths"]["library_shows"] = shows_path

            await asyncio.to_thread(
                CONFIG_FILE.write_text, json.dumps(existing_cfg, indent=2)
            )

            # Reload in-memory settings singleton (Settings.load reads disk — run in thread).
            reloaded = await asyncio.to_thread(Settings.load)
            for f in reloaded.model_fields:
                setattr(settings, f, getattr(reloaded, f))

            result.config_updated = True
            logger.info(
                "execute_migration: config updated — movies=%r shows=%r",
                movies_path,
                shows_path,
            )
        except Exception as exc:
            msg = f"Failed to update config.json: {exc}"
            logger.error("execute_migration: %s", msg)
            result.errors.append(msg)

    logger.info(
        "execute_migration: complete — imported=%d moved=%d duplicates_removed=%d errors=%d",
        result.imported,
        result.moved,
        result.duplicates_removed,
        len(result.errors),
    )
    return result
