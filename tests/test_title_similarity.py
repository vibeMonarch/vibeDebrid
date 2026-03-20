"""Tests for title similarity scoring in filter engine."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from src.config import FiltersConfig, QualityConfig, QualityProfile
from src.core.filter_engine import (
    FilterEngine,
    _normalize_reference_tokens,
    _normalize_title_tokens,
    _title_similarity,
)
from src.services.torrentio import TorrentioResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DEFAULT_PROFILE = QualityProfile(
    resolution_order=["2160p", "1080p", "720p"],
    min_resolution="720p",
    preferred_codec=["x265", "hevc", "av1", "x264"],
    preferred_audio=["atmos", "truehd", "dts-hd", "dts", "aac"],
    preferred_source=["bluray", "web-dl", "webrip", "hdtv"],
    min_size_mb=0,
    max_size_gb=80,
)

_DEFAULT_QUALITY_CONFIG = QualityConfig(
    default_profile="high",
    profiles={"high": _DEFAULT_PROFILE},
)

_DEFAULT_FILTERS = FiltersConfig(
    blocked_release_groups=[],
    blocked_keywords=[],
    required_language=None,
    allow_multi_audio=True,
)


def _make_result(
    title: str,
    info_hash: str = "a" * 40,
    size: int = 1_000_000_000,
) -> TorrentioResult:
    """Build a minimal TorrentioResult for title similarity tests."""
    return TorrentioResult(
        info_hash=info_hash,
        title=title,
        size_bytes=size,
        seeders=10,
        resolution="1080p",
        codec=None,
        quality=None,
        release_group=None,
        languages=[],
        is_season_pack=False,
    )


# ---------------------------------------------------------------------------
# TestNormalizeTitleTokens
# ---------------------------------------------------------------------------


class TestNormalizeTitleTokens:
    """Test torrent release name normalization."""

    def test_clean_title(self):
        assert _normalize_title_tokens("Movie Title") == {"movie", "title"}

    def test_release_name(self):
        tokens = _normalize_title_tokens("Movie.Title.1080p.WEB-DL.x265-GROUP")
        assert "movie" in tokens
        assert "title" in tokens
        assert "1080p" not in tokens
        assert "group" not in tokens
        assert "x265" not in tokens

    def test_anime_brackets(self):
        tokens = _normalize_title_tokens("[SubGroup] Show Title - 05 [1080p]")
        assert "show" in tokens
        assert "title" in tokens
        assert "subgroup" not in tokens
        assert "1080p" not in tokens

    def test_preserves_year(self):
        """Years should NOT be stripped — they're meaningful for numeric titles."""
        tokens = _normalize_title_tokens("2012.2009.1080p.BluRay")
        assert "2012" in tokens
        assert "2009" in tokens

    def test_strips_resolution(self):
        tokens = _normalize_title_tokens("Title.720p.x264")
        assert "720p" not in tokens
        assert "x264" not in tokens

    def test_strips_season_episode(self):
        tokens = _normalize_title_tokens("Show.S01E05.Title")
        assert "show" in tokens
        assert "title" in tokens
        # s01e05 should be stripped
        assert not any("s01" in t for t in tokens)

    def test_empty_string(self):
        assert _normalize_title_tokens("") == set()

    def test_only_noise(self):
        """String with only release metadata should produce empty or near-empty set."""
        tokens = _normalize_title_tokens("1080p.WEB-DL.x265-GROUP")
        # May have some remnants but shouldn't crash
        assert isinstance(tokens, set)

    def test_underscores(self):
        tokens = _normalize_title_tokens("Movie_Title_Name")
        assert "movie" in tokens
        assert "title" in tokens
        assert "name" in tokens


# ---------------------------------------------------------------------------
# TestNormalizeReferenceTokens
# ---------------------------------------------------------------------------


class TestNormalizeReferenceTokens:
    """Test clean reference title normalization."""

    def test_simple_title(self):
        assert _normalize_reference_tokens("Movie Title") == {"movie", "title"}

    def test_hyphenated_title(self):
        """Hyphenated titles should keep all words."""
        tokens = _normalize_reference_tokens("Spider-Man")
        assert "spider" in tokens
        assert "man" in tokens

    def test_preserves_all_words(self):
        tokens = _normalize_reference_tokens("Re:Zero - Starting Life in Another World")
        assert "world" in tokens
        assert "starting" in tokens
        assert "life" in tokens
        assert "another" in tokens
        assert "zero" in tokens

    def test_numeric_title(self):
        tokens = _normalize_reference_tokens("2012")
        assert "2012" in tokens

    def test_does_not_strip_trailing_word(self):
        """Reference normalization must not strip the last word of a hyphenated title."""
        tokens = _normalize_reference_tokens("Spider-Man")
        # _normalize_title_tokens would strip "Man" via _RELEASE_GROUP_RE
        # _normalize_reference_tokens must not do that
        assert "man" in tokens

    def test_no_noise_stripping(self):
        """Reference normalization does not strip codec/resolution terms
        (those terms are unlikely in clean TMDB titles, but we ensure the
        function doesn't mangle them)."""
        tokens = _normalize_reference_tokens("1080p Movie")
        # The reference function does NOT strip resolution terms
        assert "1080p" in tokens


# ---------------------------------------------------------------------------
# TestTitleSimilarity
# ---------------------------------------------------------------------------


class TestTitleSimilarity:
    """Test Jaccard similarity computation."""

    def test_perfect_match(self):
        tokens = {"movie", "title"}
        refs = [{"movie", "title"}]
        assert _title_similarity(tokens, refs) == 1.0

    def test_partial_match(self):
        tokens = {"great", "show"}
        refs = [{"great", "show", "the"}]
        sim = _title_similarity(tokens, refs)
        assert 0.6 < sim < 0.7  # 2/3 ≈ 0.667

    def test_no_match(self):
        tokens = {"completely", "different"}
        refs = [{"movie", "title"}]
        assert _title_similarity(tokens, refs) == 0.0

    def test_multiple_refs_takes_best(self):
        tokens = {"show", "title"}
        refs = [
            {"completely", "different"},
            {"show", "title"},  # perfect match
        ]
        assert _title_similarity(tokens, refs) == 1.0

    def test_empty_result_tokens(self):
        """Empty result tokens should return 1.0 (benefit of doubt)."""
        assert _title_similarity(set(), [{"movie", "title"}]) == 1.0

    def test_empty_refs(self):
        """Empty reference list should return 1.0 (no comparison possible)."""
        assert _title_similarity({"movie", "title"}, []) == 1.0

    def test_superset_match(self):
        """Result title is superset of reference."""
        tokens = {"show", "title", "season", "three"}
        refs = [{"show", "title"}]
        sim = _title_similarity(tokens, refs)
        assert 0.4 < sim < 0.6  # 2/4 = 0.5


# ---------------------------------------------------------------------------
# TestTitleSimilarityIntegration
# ---------------------------------------------------------------------------


class TestTitleSimilarityIntegration:
    """Test title similarity in filter_and_rank."""

    @pytest.fixture
    def engine(self):
        return FilterEngine()

    def test_known_titles_none_backward_compat(self, engine):
        """When known_titles is None, results should not be affected."""
        results = [_make_result("Some.Movie.1080p.WEB-DL.x265-GROUP")]
        with patch("src.core.filter_engine.settings.quality", _DEFAULT_QUALITY_CONFIG), \
             patch("src.core.filter_engine.settings.filters", _DEFAULT_FILTERS):
            ranked = engine.filter_and_rank(results, known_titles=None)
        assert len(ranked) == 1
        assert "title_match" not in ranked[0].score_breakdown

    def test_scoring_bonus_applied(self, engine):
        """Results matching known titles should get a title_match bonus."""
        results = [_make_result("Some.Movie.1080p.WEB-DL.x265-GROUP")]
        with patch("src.core.filter_engine.settings.quality", _DEFAULT_QUALITY_CONFIG), \
             patch("src.core.filter_engine.settings.filters", _DEFAULT_FILTERS):
            ranked = engine.filter_and_rank(results, known_titles=["Some Movie"])
        assert len(ranked) == 1
        assert ranked[0].score_breakdown.get("title_match", 0) > 0

    def test_wrong_title_gets_lower_bonus(self, engine):
        """Results with wrong titles should get lower bonus than correct ones."""
        results = [
            _make_result("Correct.Movie.1080p.WEB-DL-GROUP", info_hash="a" * 40),
            _make_result("Wrong.Different.Movie.1080p.WEB-DL-GROUP", info_hash="b" * 40),
        ]
        with patch("src.core.filter_engine.settings.quality", _DEFAULT_QUALITY_CONFIG), \
             patch("src.core.filter_engine.settings.filters", _DEFAULT_FILTERS):
            ranked = engine.filter_and_rank(results, known_titles=["Correct Movie"])
        # Find the two results
        correct = next(r for r in ranked if "correct" in r.result.title.lower())
        wrong = next(r for r in ranked if "wrong" in r.result.title.lower())
        assert correct.score_breakdown.get("title_match", 0) > wrong.score_breakdown.get("title_match", 0)

    def test_hard_reject_below_threshold(self, engine, monkeypatch):
        """With threshold > 0, completely different titles should be rejected."""
        from src.config import settings
        original = settings.filters.title_similarity_threshold
        monkeypatch.setattr(settings.filters, "title_similarity_threshold", 0.3)
        try:
            results = [_make_result("Completely.Different.Show.1080p-GROUP")]
            with patch("src.core.filter_engine.settings.quality", _DEFAULT_QUALITY_CONFIG), \
                 patch("src.core.filter_engine.settings.filters", settings.filters):
                ranked = engine.filter_and_rank(results, known_titles=["Target Movie"])
            assert len(ranked) == 0
        finally:
            monkeypatch.setattr(settings.filters, "title_similarity_threshold", original)

    def test_hard_reject_disabled_by_default(self, engine):
        """With default threshold 0.0, nothing should be rejected by title similarity."""
        results = [_make_result("Completely.Different.Show.1080p-GROUP")]
        with patch("src.core.filter_engine.settings.quality", _DEFAULT_QUALITY_CONFIG), \
             patch("src.core.filter_engine.settings.filters", _DEFAULT_FILTERS):
            ranked = engine.filter_and_rank(results, known_titles=["Target Movie"])
        assert len(ranked) == 1

    def test_hyphenated_reference_title_scores_correctly(self, engine):
        """A hyphenated reference title should match correctly via _normalize_reference_tokens."""
        results = [
            _make_result("Spider-Man.No.Way.Home.2021.1080p.WEB-DL-GROUP", info_hash="a" * 40),
        ]
        with patch("src.core.filter_engine.settings.quality", _DEFAULT_QUALITY_CONFIG), \
             patch("src.core.filter_engine.settings.filters", _DEFAULT_FILTERS):
            ranked = engine.filter_and_rank(results, known_titles=["Spider-Man: No Way Home"])
        assert len(ranked) == 1
        # The title_match bonus should be present and meaningful
        assert ranked[0].score_breakdown.get("title_match", 0) > 0

    def test_numeric_title_preserved_in_similarity(self, engine):
        """Numeric titles like '2012' should match when both torrent and reference contain the year."""
        results = [
            _make_result("2012.2009.1080p.BluRay.x265-GROUP", info_hash="a" * 40),
        ]
        with patch("src.core.filter_engine.settings.quality", _DEFAULT_QUALITY_CONFIG), \
             patch("src.core.filter_engine.settings.filters", _DEFAULT_FILTERS):
            ranked = engine.filter_and_rank(results, known_titles=["2012"])
        assert len(ranked) == 1
        # "2012" token must be present in the result tokens (not stripped as a year)
        # and "2012" must be in the reference tokens, so similarity > 0
        assert ranked[0].score_breakdown.get("title_match", 0) > 0
