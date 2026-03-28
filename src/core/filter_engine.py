"""Three-tier filtering engine for vibeDebrid scrape results.

Tier 1 — Hard Reject: instant elimination based on configurable hard rules
(size limits, blocked keywords, blocked release groups, language requirements,
minimum resolution). Rejected results are discarded entirely and never retried
from the filter engine's perspective.

Tier 2 — Quality Scoring: all results that survive Tier 1 receive a composite
score across resolution, codec, audio, source, seeders, RD cache status,
season-pack preference, and language preference. Results are sorted descending
by score. The approximate maximum score is ~120 pts when all categories align
perfectly (resolution 40 + source 15 + codec 15 + language 15 + audio 10 +
cached 10 + seeders 10 + season_pack 5).

Tier 3 (retry/dormant strategy) is handled by queue_manager.py, not here.

Usage::

    from src.core.filter_engine import filter_engine

    ranked = filter_engine.filter_and_rank(results, cached_hashes=rd_hashes)
    best = filter_engine.get_best(results, cached_hashes=rd_hashes)
"""

from __future__ import annotations

import logging
import re
from typing import Any, Protocol

from pydantic import BaseModel

from src.config import QualityProfile, settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Protocol — shared interface for TorrentioResult and ZileanResult
# ---------------------------------------------------------------------------


class ScrapeResult(Protocol):
    """Protocol capturing the shared fields of TorrentioResult and ZileanResult.

    Both concrete result types expose identical field names, so the filter
    engine can handle either without importing either class directly.
    """

    info_hash: str
    title: str
    resolution: str | None
    codec: str | None
    quality: str | None
    release_group: str | None
    size_bytes: int | None
    seeders: int | None
    languages: list[str]
    is_season_pack: bool
    season: int | None
    episode: int | None


# ---------------------------------------------------------------------------
# Output model
# ---------------------------------------------------------------------------


class FilteredResult(BaseModel):
    """A scrape result annotated with a quality score and optional rejection info.

    Attributes:
        result: The original TorrentioResult or ZileanResult object.
        score: Composite quality score (0–100+). Higher is better.
        rejection_reason: Human-readable rejection string set when the result
            was eliminated by Tier 1. None means the result passed.
        score_breakdown: Maps scoring category names to their awarded points,
            useful for debugging and UI display.
    """

    result: Any  # TorrentioResult | ZileanResult — kept as Any to avoid circular imports
    score: float
    rejection_reason: str | None = None
    score_breakdown: dict[str, float] = {}


# ---------------------------------------------------------------------------
# Scoring constants
# ---------------------------------------------------------------------------

# Resolution: points awarded by position in resolution_order list
_RESOLUTION_POINTS: list[float] = [40.0, 30.0, 20.0]
_RESOLUTION_NOT_IN_LIST: float = 5.0
_RESOLUTION_NONE: float = 10.0  # benefit of the doubt

# Codec: points decrease by 3 per position
_CODEC_MAX: float = 15.0
_CODEC_STEP: float = 3.0
_CODEC_NOT_IN_LIST: float = 2.0
_CODEC_NONE: float = 5.0

# Audio: points decrease by 2 per position
_AUDIO_MAX: float = 10.0
_AUDIO_STEP: float = 2.0
_AUDIO_NOT_IN_LIST: float = 1.0
_AUDIO_NONE: float = 3.0

# Source: points decrease by 3 per position
_SOURCE_MAX: float = 15.0
_SOURCE_STEP: float = 3.0
_SOURCE_NOT_IN_LIST: float = 2.0
_SOURCE_NONE: float = 5.0

# Seeders: min(seeders / 100, 1.0) * 10
_SEEDERS_MAX: float = 10.0
_SEEDERS_DIVISOR: float = 100.0

# RD cache bonus (configurable via settings.filters.cached_bonus)

# Season pack bonus
_SEASON_PACK_BONUS: float = 5.0

# Language preference: max 15 pts (enough to counter 10 pts max for seeders)
_LANGUAGE_MAX: float = 15.0
_LANGUAGE_STEP: float = 3.0
_LANGUAGE_MULTI_BONUS: float = 10.0


# ---------------------------------------------------------------------------
# Title similarity helpers
# ---------------------------------------------------------------------------

_RELEASE_NOISE_RE = re.compile(
    r"\b(?:\d{3,4}p|x26[45]|h\.?26[45]|hevc|av1|aac|"
    r"web[.-]?dl|webrip|bluray|hdtv|remux|"
    r"atmos|truehd|dts(?:[- ]hd)?|flac|"
    r"s\d{1,2}(?:e\d{1,3})?|e\d{1,3}|"
    r"complete|batch|season\s*\d*)\b",
    re.IGNORECASE,
)
_RELEASE_GROUP_RE = re.compile(r"-\w+$")
_NON_ALPHA_RE = re.compile(r"[^a-z0-9\s]")


def _normalize_title_tokens(text: str) -> set[str]:
    """Extract content-meaningful tokens from a torrent release name.

    Strips release metadata (resolution, codec, audio, source, season/episode
    markers, release group suffix) so that the remaining tokens reflect the
    actual content title.  Years are intentionally preserved — numeric titles
    like "2012" or "1917" participate in similarity scoring.
    """
    lowered = text.lower()
    lowered = re.sub(r"\[.*?\]", " ", lowered)
    lowered = _RELEASE_GROUP_RE.sub("", lowered)
    lowered = _RELEASE_NOISE_RE.sub(" ", lowered)
    lowered = lowered.replace(".", " ").replace("_", " ").replace("-", " ")
    lowered = _NON_ALPHA_RE.sub(" ", lowered)
    tokens = {t for t in lowered.split() if len(t) > 1}
    return tokens


def _normalize_reference_tokens(text: str) -> set[str]:
    """Extract tokens from a clean TMDB reference title.

    Applies lighter normalization than ``_normalize_title_tokens``: no release
    noise stripping and no release-group suffix removal.  This prevents
    hyphenated words in proper titles (e.g. "Spider-Man") from losing their
    trailing component.

    Args:
        text: A canonical title string from TMDB or a known-titles list.

    Returns:
        Set of lowercase alphanumeric tokens with single-character tokens
        filtered out.
    """
    lowered = text.lower()
    lowered = lowered.replace(".", " ").replace("_", " ").replace("-", " ")
    lowered = _NON_ALPHA_RE.sub(" ", lowered)
    tokens = {t for t in lowered.split() if len(t) > 1}
    return tokens


def _title_similarity(
    result_tokens: set[str],
    ref_token_sets: list[set[str]],
) -> float:
    """Compute best Jaccard similarity between result tokens and any reference title.

    Args:
        result_tokens: Normalized token set from the scrape result title.
        ref_token_sets: List of normalized token sets from known reference titles.

    Returns:
        Best Jaccard similarity score in range [0.0, 1.0].  Returns 1.0 when
        no comparison is possible (benefit of the doubt).
    """
    if not ref_token_sets or not result_tokens:
        return 1.0  # No comparison possible — benefit of the doubt

    best = 0.0
    for ref_tokens in ref_token_sets:
        intersection = len(result_tokens & ref_tokens)
        union = len(result_tokens | ref_tokens)
        if union > 0:
            sim = intersection / union
            if sim > best:
                best = sim
                if best == 1.0:
                    break
    return best


# ---------------------------------------------------------------------------
# FilterEngine
# ---------------------------------------------------------------------------


class FilterEngine:
    """Stateless two-tier filter and ranking engine for scrape results.

    All configuration is read from ``settings`` at call time, so no restart
    is needed after config changes.  The engine does not hold any mutable
    state — the module-level ``filter_engine`` singleton is safe to share
    across concurrent coroutines.
    """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def filter_and_rank(
        self,
        results: list[ScrapeResult],
        profile_name: str | None = None,
        cached_hashes: set[str] | None = None,
        prefer_season_packs: bool = False,
        original_language: str | None = None,
        requested_season: int | None = None,
        requested_episode: int | None = None,
        known_titles: list[str] | None = None,
        expected_episode_count: int | None = None,
    ) -> list[FilteredResult]:
        """Apply Tier 1 hard filters then Tier 2 quality scoring to a result list.

        Tier 1 rejections are excluded from the returned list (but logged at
        DEBUG level).  Surviving results are scored, then sorted descending by
        score.  The top 3 scored results are logged at INFO level.

        Args:
            results: List of TorrentioResult or ZileanResult objects to process.
            profile_name: Quality profile key (e.g. ``"high"``).  Defaults to
                ``settings.quality.default_profile``.
            cached_hashes: Set of info_hash strings known to be cached in
                Real-Debrid.  Matching hashes earn a bonus score.  Pass
                ``None`` or an empty set when cache status is unknown.
            prefer_season_packs: When True, the queue item is requesting a full
                season pack.  Single-episode results (``is_season_pack=False``)
                are hard-rejected by Tier 1 so that a stray episode torrent is
                never downloaded in place of the intended season pack.  Season
                pack results also earn a +5 scoring bonus.  Defaults to False
                so that calls without an explicit value (e.g. tests, search)
                are unaffected.
            original_language: ISO 639-1 code or language name of the content's
                original language (e.g. ``"ja"`` or ``"Japanese"``).  Used to
                score dubs/subs preference when ``prefer_original_language`` is
                enabled.  Defaults to None (no original language scoring).
            requested_season: The season number that was requested (after XEM
                scene mapping, if applicable).  When set together with
                ``requested_episode``, results whose parsed season differs are
                hard-rejected unless ``prefer_season_packs`` is True or the
                result's season is None (PTN parse failure — benefit of doubt).
                Pass None for movies or when episode validation is not needed
                (e.g. search endpoint).  Defaults to None.
            requested_episode: The episode number that was requested (after XEM
                scene mapping, if applicable).  Behaviour mirrors
                ``requested_season``.  Defaults to None.
            known_titles: List of known canonical titles for the content (e.g.
                primary title, original title, alternative titles).  Used for
                title similarity scoring and hard-reject filtering.  When None,
                title similarity features are disabled.  Defaults to None.
            expected_episode_count: Number of episodes in the requested season,
                used to validate season pack size-per-episode against
                ``settings.filters.season_pack_min_size_mb_per_episode``.
                When None or when the setting is 0, the check is skipped.
                Defaults to None.

        Returns:
            List of FilteredResult objects sorted by score descending.
            Rejected results are not included.
        """
        effective_profile = profile_name or settings.quality.default_profile
        profile = settings.quality.profiles.get(effective_profile)
        if profile is None:
            logger.warning(
                "filter_and_rank: unknown profile %r, falling back to 'high'",
                effective_profile,
            )
            profile = settings.quality.profiles.get("high") or QualityProfile()

        resolved_cached: set[str] = cached_hashes or set()

        # Pre-compile blocked keyword patterns once before the result loop.
        # Compiling inside the loop costs O(results × keywords) compilations.
        blocked_patterns: list[tuple[str, re.Pattern[str]]] = [
            (kw, re.compile(rf"\b{re.escape(kw)}\b", re.IGNORECASE))
            for kw in settings.filters.blocked_keywords
        ]

        # Pre-compile audio token patterns once before the result loop.
        # _score_audio is called once per result, so compiling inside the
        # helper would cost O(results × audio_tokens) compilations.
        preferred_audio_lower = [a.lower() for a in profile.preferred_audio]
        audio_patterns: list[tuple[str, re.Pattern[str]]] = [
            (tok, re.compile(rf"\b{re.escape(tok)}\b", re.IGNORECASE))
            for tok in preferred_audio_lower
        ]

        # Pre-compute lowercased preferred languages once before the result loop.
        # _score_language and _apply_hard_filters both lower-case the list per call.
        preferred_lower: list[str] = [
            lang.lower() for lang in settings.filters.preferred_languages
        ]

        # Pre-compute normalized reference token sets for title similarity.
        # Reference titles come from TMDB (clean canonical strings), so we use
        # the lighter _normalize_reference_tokens that does NOT strip hyphenated
        # suffixes — this preserves words like "Man" in "Spider-Man".
        ref_token_sets: list[set[str]] = []
        if known_titles:
            for t in known_titles:
                tokens = _normalize_reference_tokens(t)
                if tokens:
                    ref_token_sets.append(tokens)

        ranked: list[FilteredResult] = []
        rejected_count = 0

        for result in results:
            # Pre-compute result tokens once and share between hard-filter and
            # scoring to avoid calling _normalize_title_tokens twice per result.
            result_tokens: set[str] | None = (
                _normalize_title_tokens(result.title) if ref_token_sets else None
            )

            passed, reason = self._apply_hard_filters(
                result, profile, prefer_season_packs, requested_season, requested_episode,
                blocked_patterns=blocked_patterns,
                preferred_lower=preferred_lower,
                ref_token_sets=ref_token_sets if ref_token_sets else None,
                result_tokens=result_tokens,
                expected_episode_count=expected_episode_count,
            )
            if not passed:
                logger.debug(
                    "filter_engine: REJECTED title=%r reason=%r",
                    result.title,
                    reason,
                )
                rejected_count += 1
                continue

            score, breakdown = self._calculate_score(
                result, profile, resolved_cached, prefer_season_packs, original_language,
                audio_patterns=audio_patterns,
                preferred_lower=preferred_lower,
                ref_token_sets=ref_token_sets if ref_token_sets else None,
                result_tokens=result_tokens,
            )
            ranked.append(
                FilteredResult(
                    result=result,
                    score=score,
                    rejection_reason=None,
                    score_breakdown=breakdown,
                )
            )

        ranked.sort(key=lambda fr: fr.score, reverse=True)

        logger.debug(
            "filter_engine: profile=%r total=%d passed=%d rejected=%d",
            effective_profile,
            len(results),
            len(ranked),
            rejected_count,
        )

        # Log the top 3 results at INFO for operational visibility
        for i, fr in enumerate(ranked[:3], start=1):
            logger.info(
                "filter_engine: rank=%d title=%r score=%.1f breakdown=%s",
                i,
                fr.result.title,
                fr.score,
                fr.score_breakdown,
            )

        return ranked

    def get_best(
        self,
        results: list[ScrapeResult],
        profile_name: str | None = None,
        cached_hashes: set[str] | None = None,
        prefer_season_packs: bool = False,
        original_language: str | None = None,
        requested_season: int | None = None,
        requested_episode: int | None = None,
        known_titles: list[str] | None = None,
        expected_episode_count: int | None = None,
    ) -> FilteredResult | None:
        """Return the highest-scored result, or None if all were rejected.

        Args:
            results: List of TorrentioResult or ZileanResult objects to process.
            profile_name: Quality profile key.  Defaults to
                ``settings.quality.default_profile``.
            cached_hashes: Set of info_hash strings known to be cached in RD.
            prefer_season_packs: When True, single-episode results are
                hard-rejected (Tier 1) and season pack results earn a +5
                scoring bonus.  Defaults to False.
            original_language: ISO 639-1 code or language name for original
                language scoring.  Defaults to None.
            requested_season: The season number that was requested.  When set
                together with ``requested_episode``, results with a mismatched
                season are hard-rejected.  Defaults to None.
            requested_episode: The episode number that was requested.  When set
                together with ``requested_season``, results with a mismatched
                episode are hard-rejected.  Defaults to None.
            known_titles: List of known canonical titles for title similarity
                scoring and hard-reject filtering.  Defaults to None.
            expected_episode_count: Number of episodes in the requested season
                for season pack size validation.  Defaults to None.

        Returns:
            The top-ranked FilteredResult, or None when the list is empty or
            every result was eliminated by Tier 1.
        """
        ranked = self.filter_and_rank(
            results,
            profile_name=profile_name,
            cached_hashes=cached_hashes,
            prefer_season_packs=prefer_season_packs,
            original_language=original_language,
            requested_season=requested_season,
            requested_episode=requested_episode,
            known_titles=known_titles,
            expected_episode_count=expected_episode_count,
        )
        return ranked[0] if ranked else None

    # ------------------------------------------------------------------
    # Tier 1 — Hard Reject
    # ------------------------------------------------------------------

    def _apply_hard_filters(
        self,
        result: ScrapeResult,
        profile: QualityProfile,
        prefer_season_packs: bool = False,
        requested_season: int | None = None,
        requested_episode: int | None = None,
        *,
        blocked_patterns: list[tuple[str, re.Pattern[str]]] | None = None,
        preferred_lower: list[str] | None = None,
        ref_token_sets: list[set[str]] | None = None,
        result_tokens: set[str] | None = None,
        expected_episode_count: int | None = None,
    ) -> tuple[bool, str | None]:
        """Evaluate Tier 1 hard-reject rules against a single result.

        Checks are applied in declaration order; the first failing check
        determines the rejection reason.

        Args:
            result: The scrape result to evaluate.
            profile: The quality profile supplying size limits and resolution
                     ordering.
            prefer_season_packs: When True the queue item is requesting a full
                season pack.  Single-episode results are hard-rejected so that
                a stray episode torrent is never downloaded in place of the
                intended season pack.
            requested_season: The season number that was requested (after XEM
                scene mapping).  When both this and ``requested_episode`` are
                set and the result carries a parsed season, a mismatch causes
                rejection.  Skipped when ``prefer_season_packs`` is True or
                when either value is None.
            requested_episode: The episode number that was requested (after XEM
                scene mapping).  Behaviour mirrors ``requested_season``.
            blocked_patterns: Pre-compiled ``(keyword, pattern)`` pairs for
                blocked keyword matching.  When None the patterns are compiled
                on the fly (fallback for direct callers).
            preferred_lower: Pre-lowercased copy of
                ``settings.filters.preferred_languages``.  When None it is
                computed inline (fallback for direct callers).
            ref_token_sets: Pre-computed normalized token sets from known
                canonical titles.  When provided and
                ``settings.filters.title_similarity_threshold`` is > 0.0,
                results with a Jaccard similarity below the threshold are
                hard-rejected.  When None, title similarity filtering is
                skipped entirely.
            result_tokens: Pre-computed normalized token set for ``result.title``
                from ``_normalize_title_tokens``.  When provided it is used
                directly instead of calling ``_normalize_title_tokens`` again
                (avoids double computation when the caller pre-computes tokens
                for both hard-filter and scoring).  When None the tokens are
                computed inline if ``ref_token_sets`` is set.
            expected_episode_count: Number of episodes expected in the season
                pack.  When set together with a positive
                ``season_pack_min_size_mb_per_episode`` config value, season
                pack results whose size-per-episode falls below the floor are
                hard-rejected.  When None the check is skipped entirely.

        Returns:
            A 2-tuple ``(passed, reason)`` where ``passed`` is True when the
            result survives all checks.  ``reason`` is a human-readable
            string describing the first failing check, or None when passed.
        """
        # 1. Season pack gate: reject single-episode results for season pack requests
        if prefer_season_packs and not result.is_season_pack:
            return False, "single episode result for season pack request"

        # 2. Episode/season mismatch (skip for season packs and when PTN didn't parse)
        if not prefer_season_packs and requested_season is not None and requested_episode is not None:
            if result.season is not None and result.season != requested_season:
                return False, (
                    f"season mismatch: result has S{result.season:02d} "
                    f"but S{requested_season:02d} was requested"
                )
            if result.episode is not None and result.episode != requested_episode:
                return False, (
                    f"episode mismatch: result has E{result.episode:02d} "
                    f"but E{requested_episode:02d} was requested"
                )

        # 3. Minimum file size
        if result.size_bytes is not None:
            min_bytes = profile.min_size_mb * 1024 * 1024
            if result.size_bytes < min_bytes:
                return False, (
                    f"below minimum size ({result.size_bytes} bytes "
                    f"< {min_bytes} bytes)"
                )

        # 4. Maximum file size
        if result.size_bytes is not None:
            max_bytes = profile.max_size_gb * 1024 * 1024 * 1024
            if result.size_bytes > max_bytes:
                return False, (
                    f"above maximum size ({result.size_bytes} bytes "
                    f"> {max_bytes} bytes)"
                )

        # 5. Blocked keywords (whole-word, case-insensitive match against title)
        # Use pre-compiled patterns when provided (hot path); fall back to
        # compiling inline when called directly (e.g. tests or other callers).
        if blocked_patterns is None:
            blocked_patterns = [
                (kw, re.compile(rf"\b{re.escape(kw)}\b", re.IGNORECASE))
                for kw in settings.filters.blocked_keywords
            ]
        for keyword, pattern in blocked_patterns:
            if pattern.search(result.title):
                return False, f"blocked keyword: {keyword!r}"

        # 6. Blocked release groups (case-insensitive exact match)
        if result.release_group is not None:
            release_group_lower = result.release_group.lower()
            for blocked in settings.filters.blocked_release_groups:
                if release_group_lower == blocked.lower():
                    return False, f"blocked release group: {result.release_group!r}"

        # 7. Language filter
        preferred_langs = settings.filters.preferred_languages
        if preferred_langs:
            # New-style: reject anything not in the preferred list.
            # Use the pre-computed list when provided to avoid re-lowercasing
            # on every result; fall back to computing inline for direct callers.
            if preferred_lower is None:
                preferred_lower = [lang.lower() for lang in preferred_langs]
            if result.languages:
                languages_lower = [lang.lower() for lang in result.languages]
                has_preferred = any(lang in preferred_lower for lang in languages_lower)
                has_multi = (
                    settings.filters.allow_multi_audio
                    and "multi" in languages_lower
                )
                if not has_preferred and not has_multi:
                    return False, (
                        f"no preferred language found "
                        f"(detected={result.languages}, preferred={preferred_langs})"
                    )
            else:
                # No language tag detected — assumed English.
                # Only reject when English is NOT in the preferred list.
                if "english" not in preferred_lower:
                    return False, (
                        "no language detected (assumed English) and English not in preferred languages"
                    )
        else:
            # Legacy fallback: required_language (only applied when preferred_languages is empty)
            required_lang = settings.filters.required_language
            if required_lang is not None:
                languages_lower = [lang.lower() for lang in result.languages]
                has_required = required_lang.lower() in languages_lower
                has_multi = (
                    settings.filters.allow_multi_audio
                    and "multi" in languages_lower
                )
                if not has_required and not has_multi:
                    return False, (
                        f"required language {required_lang!r} not present "
                        f"(languages={result.languages})"
                    )

        # 8. Minimum resolution (only enforced when resolution is known)
        if result.resolution is not None:
            resolution_order_lower = [
                r.lower() for r in profile.resolution_order
            ]
            result_res_lower = result.resolution.lower()
            min_res_lower = profile.min_resolution.lower()

            # Position of this result's resolution in the preference order.
            # Lower index = higher quality.  If not in the list, treat as
            # "worse than everything in the list" i.e. index = len(list).
            if result_res_lower in resolution_order_lower:
                result_pos = resolution_order_lower.index(result_res_lower)
            else:
                result_pos = len(resolution_order_lower)

            if min_res_lower in resolution_order_lower:
                min_pos = resolution_order_lower.index(min_res_lower)
            else:
                # min_resolution not in the configured order — be lenient
                min_pos = len(resolution_order_lower)

            # A result is too low if its position is AFTER (greater index
            # than) the minimum resolution's position.
            if result_pos > min_pos:
                return False, (
                    f"resolution {result.resolution!r} is below minimum "
                    f"{profile.min_resolution!r}"
                )

        # 9. Title similarity floor (hard reject for very low similarity)
        if ref_token_sets:
            threshold = settings.filters.title_similarity_threshold
            if threshold > 0.0:
                # Use pre-computed tokens when available to avoid re-normalizing.
                tokens = result_tokens if result_tokens is not None else _normalize_title_tokens(result.title)
                sim = _title_similarity(tokens, ref_token_sets)
                if sim < threshold:
                    return False, f"title similarity {sim:.2f} below threshold {threshold:.2f}"

        # 10. Season pack size-per-episode floor
        min_mb = settings.filters.season_pack_min_size_mb_per_episode
        if (
            min_mb > 0
            and result.is_season_pack
            and result.size_bytes is not None
            and expected_episode_count is not None
            and expected_episode_count > 0
        ):
            size_per_ep_mb = (result.size_bytes / (1024 * 1024)) / expected_episode_count
            if size_per_ep_mb < min_mb:
                return False, (
                    f"season pack too small per episode: "
                    f"{size_per_ep_mb:.1f}MB/ep "
                    f"(need {min_mb}MB/ep for {expected_episode_count} episodes)"
                )

        return True, None

    # ------------------------------------------------------------------
    # Tier 2 — Quality Scoring
    # ------------------------------------------------------------------

    def _calculate_score(
        self,
        result: ScrapeResult,
        profile: QualityProfile,
        cached_hashes: set[str],
        prefer_season_packs: bool = False,
        original_language: str | None = None,
        *,
        audio_patterns: list[tuple[str, re.Pattern[str]]] | None = None,
        preferred_lower: list[str] | None = None,
        ref_token_sets: list[set[str]] | None = None,
        result_tokens: set[str] | None = None,
    ) -> tuple[float, dict[str, float]]:
        """Compute the composite quality score for a result.

        Each scoring category is independently evaluated.  The sum across
        all categories forms the final score.  The per-category breakdown
        is returned alongside the total for debugging and UI display.

        Args:
            result: The scrape result to score.
            profile: The quality profile supplying preference lists.
            cached_hashes: Set of info_hash strings cached in Real-Debrid.
            prefer_season_packs: When True, season pack results receive the
                ``_SEASON_PACK_BONUS`` score increment.  Pass False for
                single-episode requests to avoid inflating season pack ranks.
            original_language: ISO 639-1 code or language name of the
                content's original language.  Used to score dub preference.
            audio_patterns: Pre-compiled ``(token, pattern)`` pairs for audio
                token matching.  When None, patterns are compiled inline
                (fallback for direct callers).
            preferred_lower: Pre-lowercased copy of
                ``settings.filters.preferred_languages``.  When None it is
                computed inline inside ``_score_language`` (fallback for direct
                callers).
            ref_token_sets: Pre-computed normalized token sets from known
                canonical titles.  When provided, title similarity is scored
                and added to the breakdown.  When None, title similarity
                scoring is skipped.
            result_tokens: Pre-computed normalized token set for ``result.title``
                from ``_normalize_title_tokens``.  When provided it is used
                directly instead of re-normalizing (avoids double computation
                when the caller pre-computes tokens for both hard-filter and
                scoring).  When None the tokens are computed inline if
                ``ref_token_sets`` is set.

        Returns:
            A 2-tuple ``(total_score, breakdown)`` where ``breakdown`` maps
            category names to the points awarded in that category.
        """
        breakdown: dict[str, float] = {}

        # --- Resolution (max 40 pts) ---
        breakdown["resolution"] = self._score_resolution(
            result.resolution, profile.resolution_order
        )

        # --- Codec (max 15 pts) ---
        breakdown["codec"] = self._score_by_position(
            value=result.codec,
            preference_list=profile.preferred_codec,
            max_points=_CODEC_MAX,
            step=_CODEC_STEP,
            not_in_list=_CODEC_NOT_IN_LIST,
            none_value=_CODEC_NONE,
        )

        # --- Audio (max 10 pts) ---
        # The `quality` field on a scrape result carries audio information from
        # the release name (e.g. "Atmos", "TrueHD") in addition to source info.
        # We check both the dedicated `quality` field (for source) AND the title
        # itself for audio keywords, since PTN doesn't always expose audio as a
        # separate field.  We score against whichever audio token we find first.
        breakdown["audio"] = self._score_audio(
            result.title, result.quality, profile.preferred_audio,
            audio_patterns=audio_patterns,
        )

        # --- Source (max 15 pts) ---
        breakdown["source"] = self._score_by_position(
            value=result.quality,
            preference_list=profile.preferred_source,
            max_points=_SOURCE_MAX,
            step=_SOURCE_STEP,
            not_in_list=_SOURCE_NOT_IN_LIST,
            none_value=_SOURCE_NONE,
        )

        # --- Seeders (max 10 pts) ---
        if result.seeders is not None:
            seeder_score = min(result.seeders / _SEEDERS_DIVISOR, 1.0) * _SEEDERS_MAX
            breakdown["seeders"] = round(seeder_score, 2)
        else:
            breakdown["seeders"] = 0.0

        # --- Cached in RD (configurable, default 25 pts) ---
        cached_bonus = float(settings.filters.cached_bonus)
        breakdown["cached"] = (
            cached_bonus if result.info_hash in cached_hashes else 0.0
        )

        # --- Season pack (max 5 pts) ---
        breakdown["season_pack"] = _SEASON_PACK_BONUS if (prefer_season_packs and result.is_season_pack) else 0.0

        # --- Language preference (max 15 pts, 0 when preferred_languages unset) ---
        breakdown["language"] = self._score_language(
            result.languages, preferred_lower=preferred_lower
        )

        # --- Original language preference (variable pts, 0 when disabled) ---
        breakdown["original_language"] = self._score_original_language(
            result.languages, original_language
        )

        # --- Title similarity bonus ---
        if ref_token_sets:
            # Use pre-computed tokens when available to avoid re-normalizing.
            tokens = result_tokens if result_tokens is not None else _normalize_title_tokens(result.title)
            sim = _title_similarity(tokens, ref_token_sets)
            bonus = settings.filters.title_similarity_bonus
            breakdown["title_match"] = round(sim * bonus, 2)

        total = sum(breakdown.values())
        return total, breakdown

    # ------------------------------------------------------------------
    # Scoring helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _score_resolution(
        resolution: str | None,
        resolution_order: list[str],
    ) -> float:
        """Award points for resolution based on position in the preference list.

        Args:
            resolution: The result's resolution string (e.g. ``"1080p"``), or
                        None when unknown.
            resolution_order: Ordered list of resolutions, best-first.

        Returns:
            Points awarded: 40 for 1st position, 30 for 2nd, 20 for 3rd,
            5 for any position beyond 3rd, 10 for None.
        """
        if resolution is None:
            return _RESOLUTION_NONE

        res_lower = resolution.lower()
        order_lower = [r.lower() for r in resolution_order]

        try:
            pos = order_lower.index(res_lower)
        except ValueError:
            return _RESOLUTION_NOT_IN_LIST

        if pos < len(_RESOLUTION_POINTS):
            return _RESOLUTION_POINTS[pos]
        return _RESOLUTION_NOT_IN_LIST

    @staticmethod
    def _score_by_position(
        value: str | None,
        preference_list: list[str],
        max_points: float,
        step: float,
        not_in_list: float,
        none_value: float,
    ) -> float:
        """Award points for a quality attribute based on its position in a preference list.

        Points start at ``max_points`` for the first (most preferred) entry and
        decrease by ``step`` for each subsequent position.  Values not found in
        the list receive ``not_in_list`` points.  A None value receives
        ``none_value`` points (benefit of the doubt).

        Args:
            value: The attribute value to look up (e.g. codec string).  The
                   comparison is case-insensitive.
            preference_list: Ordered list of preferred values, best-first.
            max_points: Points for position 0 (most preferred).
            step: Reduction in points per position step.
            not_in_list: Points when value is not None but not in the list.
            none_value: Points when value is None.

        Returns:
            Points awarded as a float.
        """
        if value is None:
            return none_value

        value_lower = value.lower()
        list_lower = [v.lower() for v in preference_list]

        try:
            pos = list_lower.index(value_lower)
        except ValueError:
            return not_in_list

        awarded = max_points - pos * step
        # Clamp to not_in_list as the floor (never award negative points for
        # a value that is technically present but far down the list).
        return max(awarded, not_in_list)

    def _score_language(
        self,
        languages: list[str],
        *,
        preferred_lower: list[str] | None = None,
    ) -> float:
        """Award points for language match against the preferred_languages list.

        When ``preferred_languages`` is empty, returns 0.0 (no effect on
        scoring).  When set, awards points based on the language's position in
        the ordered preference list.  Untagged results (empty ``languages``
        list) are treated as English, since releases without a language token
        are assumed to be English.

        Multi-audio releases receive a fixed bonus when ``allow_multi_audio``
        is enabled, regardless of their position in the preference list.

        Args:
            languages: List of language strings detected from the torrent
                       title (e.g. ``["Russian"]`` or ``[]`` for untagged).
            preferred_lower: Pre-lowercased copy of
                ``settings.filters.preferred_languages``.  When None it is
                computed inline (fallback for direct callers and tests).

        Returns:
            Points awarded: ``_LANGUAGE_MAX`` (15) for 1st preference,
            decreasing by ``_LANGUAGE_STEP`` (3) per position, floored at
            1.0 for any match.  ``_LANGUAGE_MULTI_BONUS`` (10) for multi.
            0.0 when ``preferred_languages`` is not configured.
        """
        preferred_langs = settings.filters.preferred_languages
        if not preferred_langs:
            return 0.0

        # Use pre-computed list when provided to avoid re-lowercasing per call.
        if preferred_lower is None:
            preferred_lower = [lang.lower() for lang in preferred_langs]

        # Determine effective language list (untagged releases = English)
        effective_langs = [lang.lower() for lang in languages] if languages else ["english"]

        # Check position-based matches first — a preferred language may score
        # higher than the fixed multi-audio bonus.
        best_score = 0.0
        for lang in effective_langs:
            if lang in preferred_lower:
                pos = preferred_lower.index(lang)
                score = _LANGUAGE_MAX - pos * _LANGUAGE_STEP
                score = max(score, 1.0)  # floor at 1.0 for any match
                best_score = max(best_score, score)

        # Multi-audio only fills in when no preferred language scored higher.
        if best_score == 0.0 and "multi" in effective_langs and settings.filters.allow_multi_audio:
            return _LANGUAGE_MULTI_BONUS

        return best_score

    def _score_original_language(
        self, languages: list[str], original_language: str | None
    ) -> float:
        """Score based on original language preference.

        Only active when ``prefer_original_language`` is True and
        ``original_language`` is not None and not English.  Penalises dubs
        and rewards dual-audio or original-language releases.

        Args:
            languages: Language tags detected from the torrent title.
            original_language: The content's original language (ISO 639-1
                code or full language name), or None when unknown.

        Returns:
            A float score delta (can be negative for dubs).  0.0 when the
            feature is disabled or original_language is English/None.
        """
        if not settings.filters.prefer_original_language:
            return 0.0
        if original_language is None or original_language.lower() in ("en", "english"):
            return 0.0

        score = 0.0
        effective_langs = {lang.lower() for lang in languages}
        orig_lower = original_language.lower()

        # Penalise detected dubs
        if "dubbed" in effective_langs:
            # Don't penalise if dubbed INTO the original language (edge case)
            if orig_lower not in effective_langs:
                score -= settings.filters.dub_penalty

        # Bonus for dual audio
        if "dual audio" in effective_langs:
            score += settings.filters.dual_audio_bonus

        # Bonus for original language match
        if orig_lower in effective_langs:
            score += 15.0
        elif not effective_langs - {"dubbed", "dual audio"}:
            # No language tags at all (assumed English) — moderate penalty
            # for non-English content whose language is undetected.
            score -= settings.filters.dub_penalty / 2

        return score

    @staticmethod
    def _score_audio(
        title: str,
        quality_field: str | None,
        preferred_audio: list[str],
        *,
        audio_patterns: list[tuple[str, re.Pattern[str]]] | None = None,
    ) -> float:
        """Award points for audio quality by scanning the title and quality field.

        PTN does not reliably extract audio codec as a separate field — it
        often ends up folded into the release name or quality string.  We scan
        both the full title and the quality field for the first matching audio
        token from the preference list.

        Args:
            title: The raw release name / torrent title.
            quality_field: The ``quality`` field from the scrape result (may
                           contain source or audio info, e.g. ``"Atmos"``).
            preferred_audio: Ordered list of preferred audio codecs/formats.
                             Still required so the method signature is stable
                             for direct callers; the list is only used to build
                             ``audio_patterns`` when they are not pre-supplied.
            audio_patterns: Pre-compiled ``(token, pattern)`` pairs ordered to
                            match ``preferred_audio``.  When None the patterns
                            are compiled inline (fallback for direct callers and
                            tests).

        Returns:
            Points awarded: ``_AUDIO_MAX`` decremented by ``_AUDIO_STEP`` per
            position, ``_AUDIO_NOT_IN_LIST`` when a token is found but not
            preferred, ``_AUDIO_NONE`` when no audio token is detected.
        """
        search_text = title.lower()
        if quality_field:
            search_text = search_text + " " + quality_field.lower()

        # Use pre-compiled patterns when provided; fall back to compiling inline
        # so that direct callers (e.g. tests) still work without modification.
        if audio_patterns is None:
            list_lower = [a.lower() for a in preferred_audio]
            audio_patterns = [
                (tok, re.compile(rf"\b{re.escape(tok)}\b", re.IGNORECASE))
                for tok in list_lower
            ]

        # Check each preferred audio token in preference order; return as soon
        # as we find the first match (keeps the first/best hit).
        for pos, (_, pattern) in enumerate(audio_patterns):
            # Word-boundary matching prevents "dts" matching "dts-hd" etc.
            if pattern.search(search_text):
                awarded = _AUDIO_MAX - pos * _AUDIO_STEP
                return max(awarded, _AUDIO_NOT_IN_LIST)

        return _AUDIO_NONE


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

filter_engine = FilterEngine()
