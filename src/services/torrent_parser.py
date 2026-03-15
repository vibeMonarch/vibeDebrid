"""Shared torrent name parsing utilities for vibeDebrid.

This module centralises all regex patterns, constants, and parsing functions
that were previously duplicated across ``torrentio.py``, ``zilean.py``,
``mount_scanner.py``, and ``symlink_manager.py``.

Every function here is pure (no I/O, no external dependencies beyond the
stdlib and PTN) and is safe to call from any async or sync context.

Public API
----------
Regexes (compiled once at import time):
    SEASON_ONLY_RE, COMPLETE_RE, SEASON_DASH_EP_RE, ORDINAL_SEASON_EP_RE,
    ANIME_BARE_DASH_EP_RE, ANIME_EP_RANGE_RE, ANIME_BATCH_RE,
    ANIME_SEASON_KEYWORD_RE, DUB_RE, DUAL_AUDIO_RE,
    BARE_TRAILING_EP_RE

Constants:
    NON_EPISODE_NUMBERS, LANGUAGE_TOKENS, LANGUAGE_ABBREV_TOKENS,
    CYRILLIC_RE, LANGUAGE_ABBREV_RES

Functions:
    parse_languages(title) -> list[str]
    parse_episode_fallbacks(title, season, episode) -> (season, episode, is_anime_batch)
    detect_season_pack(title, episode, is_anime_batch) -> bool
    parse_episode_from_filename(filename) -> int | None
"""

from __future__ import annotations

import logging
import os
import re

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Compiled regexes — torrent title parsing
# ---------------------------------------------------------------------------

# Season-only patterns in release names (S02, S2, Season.2) — no episode part.
# Used for season-pack detection together with PTN's absence of an 'episode' key.
SEASON_ONLY_RE = re.compile(
    r"(?:\b|_)S(\d{1,2})(?!\s*E\d)",  # S02 not followed by E##
    re.IGNORECASE,
)

# Explicitly tagged "complete" season packs
COMPLETE_RE = re.compile(r"\b(?:complete|season\.?\d+)\b", re.IGNORECASE)

# Fallback: anime-style "S2 - 06" or "S02-E06" (PTN doesn't handle dash-separated notation)
SEASON_DASH_EP_RE = re.compile(
    r"(?:\b|_)S(\d{1,2})\s*-\s*E?(\d{1,3})(?:\b|_|\[)",
    re.IGNORECASE,
)

# Fallback: ordinal season + episode, e.g. "2nd Season - 01", "1st Season - 28"
ORDINAL_SEASON_EP_RE = re.compile(
    r"(\d+)(?:st|nd|rd|th)\s+Season\s*[-–]\s*(\d{1,3})\b",
    re.IGNORECASE,
)

# Fallback: anime bare dash notation, e.g. "[Group] Title - 29 [1080p]"
# Also used by mount_scanner under the name _ANIME_DASH_EP_RE.
ANIME_BARE_DASH_EP_RE = re.compile(r"\s-\s(\d{1,3})(?:\s|$|\[|\()")

# Anime batch/season pack patterns — checked BEFORE ANIME_BARE_DASH_EP_RE so that
# episode-range titles (e.g. "- 01 ~ 13") are not mistakenly parsed as episode 1.

# Episode range: "- 01 ~ 13", "- 01~13", "01 - 13" (two distinct episode numbers)
# The range must span at least 2 episodes to avoid matching single-ep "- 01".
ANIME_EP_RANGE_RE = re.compile(
    r"[-–]\s*(\d{1,3})\s*[~–-]\s*(\d{1,3})(?:\s|$|\[|\()",
)

# [BATCH] or BATCH keyword (case-insensitive, bracket-optional)
ANIME_BATCH_RE = re.compile(r"\[BATCH\]|\bBATCH\b", re.IGNORECASE)

# "(Season N)" or "Season N" — extracts the season number (1-based)
ANIME_SEASON_KEYWORD_RE = re.compile(r"\bSeason\s+(\d{1,2})\b", re.IGNORECASE)

# Dub / Dual Audio detection — used to tag results with "Dubbed" or "Dual Audio".
# DUB_RE matches "DUB" and "DUBBED" but not "DUBLIN" (word boundary prevents it).
DUB_RE = re.compile(r"\bDUB(?:BED)?\b", re.IGNORECASE)
# DUAL_AUDIO_RE requires "AUDIO" after "DUAL" (with optional separator) to avoid
# false positives in show titles like "Dual.Survival.S01E01".
DUAL_AUDIO_RE = re.compile(r"\bDUAL[\.\s-]?AUDIO\b", re.IGNORECASE)

# ---------------------------------------------------------------------------
# Compiled regexes — filename episode extraction
# ---------------------------------------------------------------------------

# Last-resort bare trailing number for anime naming conventions, e.g.:
#   "Show Title 01.mkv" → episode 1
# Only matches 1-3 digit numbers at the end of the stem (4-digit years excluded
# by regex length limit; common resolution values excluded by the set below).
BARE_TRAILING_EP_RE = re.compile(r"[\s.](\d{1,3})\s*$")

# ---------------------------------------------------------------------------
# Constants — language detection
# ---------------------------------------------------------------------------

# Known language tokens that appear in torrent names (non-exhaustive but covers
# the common cases we encounter in Torrentio/Zilean output).
LANGUAGE_TOKENS: dict[str, str] = {
    "FRENCH": "French",
    "TRUEFRENCH": "French",
    "VOSTFR": "French",
    "GERMAN": "German",
    "DEUTSCH": "German",
    "SPANISH": "Spanish",
    "SPANISH.DUBBED": "Spanish",
    "PORTUGUESE": "Portuguese",
    "ITALIAN": "Italian",
    "DUTCH": "Dutch",
    "RUSSIAN": "Russian",
    "JAPANESE": "Japanese",
    "KOREAN": "Korean",
    "CHINESE": "Chinese",
    "MULTI": "Multi",
    "MULTi": "Multi",
}

# Cyrillic script detection — any Cyrillic character in the title means Russian.
CYRILLIC_RE = re.compile(r"[\u0400-\u04FF]")

# Short ISO/scene abbreviations that need word-boundary matching to avoid false
# positives (e.g. "RUS" inside "BRUSH").  Compiled once at module level.
LANGUAGE_ABBREV_TOKENS: dict[str, str] = {
    "RUS": "Russian",
    "JAP": "Japanese",
    "JPN": "Japanese",
    "KOR": "Korean",
    "CHI": "Chinese",
    "ITA": "Italian",
    "NLD": "Dutch",
    "DEU": "German",
    "SPA": "Spanish",
    "POR": "Portuguese",
    "FRA": "French",
}
LANGUAGE_ABBREV_RES: dict[str, re.Pattern[str]] = {
    abbrev: re.compile(rf"\b{abbrev}\b", re.IGNORECASE)
    for abbrev in LANGUAGE_ABBREV_TOKENS
}

# ---------------------------------------------------------------------------
# Constants — episode number exclusions
# ---------------------------------------------------------------------------

# Values that look like episode numbers but are resolution/bitrate markers.
# These must NOT be treated as episode numbers when matched by the bare
# trailing pattern above.
NON_EPISODE_NUMBERS: frozenset[int] = frozenset({264, 265, 480, 576, 720, 1080, 2160, 4320})

# ---------------------------------------------------------------------------
# Private regex for parse_episode_from_filename
# ---------------------------------------------------------------------------

_EPISODE_RE = re.compile(r"[Ss]\d{1,2}[Ee](\d{1,3})")
_BARE_EPISODE_RE = re.compile(r"[\s._-][Ee](\d{2,3})(?:\b|[\s._-])")


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------


def parse_languages(title: str) -> list[str]:
    """Extract language tags from a torrent release name.

    PTN does not reliably expose a 'language' key — language tokens often
    end up in the PTN 'title', 'excess', or are simply not extracted.  We
    scan the raw release name for known language tokens instead.

    English is not included — it is assumed when no other language is
    present.

    Args:
        title: The raw torrent title / release name string.

    Returns:
        A deduplicated list of language names found, e.g. ``["French"]``.
        Returns an empty list when no non-English language is detected.
    """
    upper_name = title.upper()
    found: list[str] = []
    seen: set[str] = set()

    for token, lang_name in LANGUAGE_TOKENS.items():
        if token.upper() in upper_name and lang_name not in seen:
            found.append(lang_name)
            seen.add(lang_name)

    # Cyrillic script → Russian (catches titles written in Russian that lack
    # any English-language tag).
    if CYRILLIC_RE.search(title) and "Russian" not in seen:
        found.append("Russian")
        seen.add("Russian")

    # Short ISO/scene abbreviations (word-boundary matched).
    for abbrev, lang_name in LANGUAGE_ABBREV_TOKENS.items():
        if lang_name not in seen and LANGUAGE_ABBREV_RES[abbrev].search(title):
            found.append(lang_name)
            seen.add(lang_name)

    # Dub / Dual Audio detection
    if DUB_RE.search(title) and "Dubbed" not in seen:
        found.append("Dubbed")
        seen.add("Dubbed")
    if DUAL_AUDIO_RE.search(title) and "Dual Audio" not in seen:
        found.append("Dual Audio")
        seen.add("Dual Audio")

    return found


def parse_episode_fallbacks(
    title: str,
    season: int | None,
    episode: int | None,
) -> tuple[int | None, int | None, bool]:
    """Apply anime/non-standard episode fallback parsing to a torrent title.

    PTN misses several anime and regional naming conventions.  This function
    applies the canonical fallback chain in the correct order:

    1. ``S2 - 06`` / ``S02-E06`` dash-separated notation
    2. Ordinal season: ``2nd Season - 01``
    3. Episode range: ``- 01 ~ 13`` → season pack (``is_anime_batch=True``)
    4. ``[BATCH]`` / ``BATCH`` keyword → season pack
    5. ``Season N`` keyword → extract season number
    6. Bare dash: ``- 29`` → single episode (only when not a batch)

    Steps 3–5 are checked only when ``episode`` is still ``None`` at that
    point (i.e. PTN and steps 1–2 all failed).

    Args:
        title:   Raw torrent release name or ``raw_title`` from Zilean.
        season:  Season number already extracted by PTN (or ``None``).
        episode: Episode number already extracted by PTN (or ``None``).

    Returns:
        A 3-tuple ``(season, episode, is_anime_batch)`` where:
        - ``season`` and ``episode`` are the (possibly updated) numbers.
        - ``is_anime_batch`` is ``True`` when the title represents a season
          pack / batch rather than a single episode.
    """
    is_anime_batch = False

    # (1) Dash-separated anime notation: "S2 - 06" or "S02-E06"
    if episode is None:
        m = SEASON_DASH_EP_RE.search(title)
        if m:
            season = int(m.group(1))
            episode = int(m.group(2))

    # (2) Ordinal season notation: "2nd Season - 01"
    if episode is None:
        m = ORDINAL_SEASON_EP_RE.search(title)
        if m:
            season = int(m.group(1))
            episode = int(m.group(2))

    # (3) Episode range: "- 01 ~ 13" → season pack
    if episode is None:
        range_m = ANIME_EP_RANGE_RE.search(title)
        if range_m:
            ep_start = int(range_m.group(1))
            ep_end = int(range_m.group(2))
            if ep_end > ep_start:
                # Genuine range spanning multiple episodes → season pack.
                # Do NOT set episode — leave it None to trigger pack detection.
                is_anime_batch = True

    # (4) BATCH keyword: "[BATCH]" or bare "BATCH"
    if episode is None and not is_anime_batch and ANIME_BATCH_RE.search(title):
        is_anime_batch = True

    # (5) "Season N" keyword — extract season number regardless of pack status.
    #     A bare "(Season N)" title with no episode indicator is itself a
    #     season pack marker, so we also set is_anime_batch when it fires
    #     and episode is still None.
    if season is None:
        season_kw_m = ANIME_SEASON_KEYWORD_RE.search(title)
        if season_kw_m:
            season = int(season_kw_m.group(1))
            if episode is None:
                is_anime_batch = True

    # (6) Bare dash episode fallback — only when NOT an anime batch pack
    if episode is None and not is_anime_batch:
        m = ANIME_BARE_DASH_EP_RE.search(title)
        if m:
            episode = int(m.group(1))
            # No season info from this pattern — leave season as-is

    return season, episode, is_anime_batch


def detect_season_pack(
    title: str,
    episode: int | None,
    is_anime_batch: bool,
) -> bool:
    """Determine whether a torrent title represents a season pack.

    A result is a season pack when ``episode`` is ``None`` AND at least one of:
    - The release name matches the season-only pattern (e.g. ``S02``).
    - The release name contains ``complete`` / ``season`` keywords.
    - ``is_anime_batch`` is ``True`` (set by :func:`parse_episode_fallbacks`).

    Args:
        title:          Raw torrent release name or ``raw_title`` from Zilean.
        episode:        Episode number (``None`` if not a single episode).
        is_anime_batch: Whether the title was identified as an anime batch/range
                        by :func:`parse_episode_fallbacks`.

    Returns:
        ``True`` when the entry should be treated as a season pack.
    """
    if episode is not None:
        return False
    has_season_marker = bool(SEASON_ONLY_RE.search(title))
    has_complete_marker = bool(COMPLETE_RE.search(title))
    return has_season_marker or has_complete_marker or is_anime_batch


def parse_episode_from_filename(filename: str) -> int | None:
    """Extract episode number from a filename using PTN then regex fallback.

    Used for season pack files where the media item has no episode number set,
    so the episode must be inferred from the individual file's name.

    Fallback order:
    1. PTN parse — checks the ``episode`` key.
    2. ``SxxExx`` regex — standard season-episode notation.
    3. Bare ``E``-prefixed number — ``E05``, ``E12``, etc.
    4. Bare trailing number — anime convention, e.g. ``Show Title 01.mkv``.
       Excludes known resolution/bitrate values (480, 720, 1080, …).

    Args:
        filename: Basename of the source file (e.g. ``"Show.S01E05.mkv"``).

    Returns:
        The episode number as an integer, or ``None`` when no episode number
        could be parsed.
    """
    try:
        import PTN  # type: ignore[import-untyped]

        parsed = PTN.parse(filename)
        if parsed and parsed.get("episode") is not None:
            return int(parsed["episode"])
    except Exception:
        pass

    # Regex fallback: SxxExx pattern first, then bare E-prefixed number.
    match = _EPISODE_RE.search(filename)
    if match:
        return int(match.group(1))
    match = _BARE_EPISODE_RE.search(filename)
    if match:
        return int(match.group(1))

    # Last-resort: bare trailing number for anime naming conventions, e.g.
    # "Show Title 01.mkv" → episode 1.  Excludes known resolution values.
    stem = os.path.splitext(filename)[0]
    trailing_match = BARE_TRAILING_EP_RE.search(stem)
    if trailing_match:
        candidate = int(trailing_match.group(1))
        if candidate not in NON_EPISODE_NUMBERS:
            return candidate

    return None
