"""Tests for the "Prefer Original Language" feature (Issue #28).

Covers:
  - DUB / DUBBED / DUAL AUDIO detection in torrentio._parse_languages()
  - DUB / DUBBED / DUAL AUDIO detection in zilean._parse_languages()
  - iso_to_language_name() mapping in tmdb.py
  - TmdbClient._parse_item() extracting original_language
  - TmdbClient.get_show_details() returning original_language
  - FilterEngine._score_original_language() scoring logic
  - filter_and_rank() integration with original_language param
  - AddToQueueRequest.original_language field
  - Season pack split copying original_language from parent
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.deps import get_db
from src.config import FiltersConfig, QualityConfig, QualityProfile
from src.core.filter_engine import FilterEngine
from src.main import app
from src.models.media_item import MediaItem, MediaType, QueueState
from src.services.tmdb import (
    ISO_639_1_TO_LANGUAGE,
    TmdbClient,
    iso_to_language_name,
    language_name_to_iso,
)
from src.services.torrentio import TorrentioClient, TorrentioResult
from src.services.zilean import ZileanClient, ZileanResult

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_torrentio_result(**overrides: Any) -> TorrentioResult:
    """Build a TorrentioResult with sensible defaults."""
    defaults: dict[str, Any] = {
        "info_hash": "a" * 40,
        "title": "Show.S01E01.1080p.WEB-DL.x265-GROUP",
        "resolution": "1080p",
        "codec": "x265",
        "quality": "WEB-DL",
        "release_group": "GROUP",
        "size_bytes": 2 * 1024 * 1024 * 1024,
        "seeders": 50,
        "languages": [],
        "is_season_pack": False,
    }
    defaults.update(overrides)
    return TorrentioResult(**defaults)


def _make_zilean_result(**overrides: Any) -> ZileanResult:
    """Build a ZileanResult with sensible defaults."""
    defaults: dict[str, Any] = {
        "info_hash": "b" * 40,
        "title": "Show.S01E01.1080p.WEB-DL.x265-GROUP",
        "resolution": "1080p",
        "codec": "x265",
        "quality": "WEB-DL",
        "release_group": "GROUP",
        "size_bytes": 2 * 1024 * 1024 * 1024,
        "seeders": None,
        "languages": [],
        "is_season_pack": False,
    }
    defaults.update(overrides)
    return ZileanResult(**defaults)


_DEFAULT_PROFILE = QualityProfile(
    resolution_order=["2160p", "1080p", "720p"],
    min_resolution="720p",
    preferred_codec=["x265", "hevc", "av1", "x264"],
    preferred_audio=["atmos", "truehd", "dts-hd", "dts", "aac"],
    preferred_source=["bluray", "web-dl", "webrip", "hdtv"],
    min_size_mb=200,
    max_size_gb=80,
)

_DEFAULT_QUALITY_CONFIG = QualityConfig(
    default_profile="high",
    profiles={"high": _DEFAULT_PROFILE},
)

_ORIG_LANG_FILTERS = FiltersConfig(
    prefer_original_language=True,
    dub_penalty=20,
    dual_audio_bonus=10,
    preferred_languages=[],
)

_NO_ORIG_LANG_FILTERS = FiltersConfig(
    prefer_original_language=False,
    dub_penalty=20,
    dual_audio_bonus=10,
    preferred_languages=[],
)

ENGINE = FilterEngine()


def _patch_filter_settings(filters: FiltersConfig) -> Any:
    """Return a patch for filter engine settings."""
    return patch("src.core.filter_engine.settings.filters", filters)


# ---------------------------------------------------------------------------
# Helper: mock httpx.Response
# ---------------------------------------------------------------------------


def _make_response(
    status_code: int,
    body: Any = None,
    *,
    content_type: str = "application/json",
) -> httpx.Response:
    """Build a fake httpx.Response."""
    if body is None:
        raw = b""
    elif isinstance(body, (dict, list)):
        raw = json.dumps(body).encode()
    else:
        raw = body if isinstance(body, bytes) else str(body).encode()

    return httpx.Response(
        status_code=status_code,
        headers={"content-type": content_type},
        content=raw,
        request=httpx.Request("GET", "https://api.themoviedb.org/"),
    )


# ===========================================================================
# Section 1: DUB / DUAL AUDIO detection — Torrentio
# ===========================================================================


class TestTorrentioLanguageDubDetection:
    """Test _parse_languages() in TorrentioClient for dub/dual audio tags."""

    def setup_method(self) -> None:
        self.client = TorrentioClient()

    def _parse(self, release_name: str) -> list[str]:
        """Convenience: call _parse_languages with empty ptn_data."""
        return self.client._parse_languages(release_name, {})

    def test_dubbed_keyword_detected(self) -> None:
        """'DUBBED' in release name adds 'Dubbed' to languages."""
        langs = self._parse("Movie.1080p.DUBBED.x264")
        assert "Dubbed" in langs

    def test_dub_keyword_matched_standalone(self) -> None:
        """Bare 'DUB' (without 'BED') IS matched by _DUB_RE = r'\\bDUB(?:BED)?\\b'.

        The updated regex matches both "DUB" and "DUBBED" as separate alternatives,
        so standalone "DUB" in a release name like "Anime.DUB.720p" is now detected.
        """
        langs = self._parse("Movie.1080p.DUB.x264")
        assert "Dubbed" in langs

    def test_english_dub_bare_detected(self) -> None:
        """'ENGLISH.DUB' adds 'Dubbed' because standalone DUB is now matched."""
        langs = self._parse("Movie.ENGLISH.DUB.1080p")
        assert "Dubbed" in langs

    def test_spanish_dubbed_detects_language_and_dub(self) -> None:
        """'SPANISH.DUBBED' release includes both Spanish and Dubbed."""
        langs = self._parse("Movie.SPANISH.DUBBED.1080p")
        assert "Spanish" in langs
        assert "Dubbed" in langs

    def test_dual_audio_space_separated(self) -> None:
        """'Dual Audio' (space) is detected as 'Dual Audio'."""
        langs = self._parse("Movie.Dual.Audio.1080p")
        assert "Dual Audio" in langs

    def test_dual_audio_no_space(self) -> None:
        """'DualAudio' (no separator) is detected as 'Dual Audio'."""
        langs = self._parse("Movie.DualAudio.1080p")
        assert "Dual Audio" in langs

    def test_dual_word_alone_not_detected(self) -> None:
        """'DUAL' as a standalone word (without 'AUDIO') is NOT detected as 'Dual Audio'.

        The updated _DUAL_AUDIO_RE requires 'AUDIO' after 'DUAL' (with optional
        separator) to avoid false positives in titles like 'Dual.Survival.S01E01'.
        """
        langs = self._parse("Movie.DUAL.1080p")
        assert "Dual Audio" not in langs

    def test_plain_release_no_dubbed_or_dual(self) -> None:
        """A plain release name does not add 'Dubbed' or 'Dual Audio'."""
        langs = self._parse("Movie.1080p.x264")
        assert "Dubbed" not in langs
        assert "Dual Audio" not in langs

    def test_dublin_false_positive_check(self) -> None:
        """'Dublin' in a title must NOT be detected as 'Dubbed'."""
        langs = self._parse("Dublin.2024.1080p.WEB-DL")
        assert "Dubbed" not in langs

    def test_dutch_false_positive_check(self) -> None:
        """'DUTCH' in a title must NOT be detected as 'Dual Audio'."""
        langs = self._parse("Movie.DUTCH.1080p")
        assert "Dual Audio" not in langs
        # Dutch should still be detected as the language
        assert "Dutch" in langs

    def test_dubbed_and_dual_audio_can_coexist(self) -> None:
        """A release tagged with both DUBBED and DUAL AUDIO gets both tags."""
        langs = self._parse("Movie.DUBBED.Dual.Audio.1080p")
        assert "Dubbed" in langs
        assert "Dual Audio" in langs

    def test_dubbed_variants_case_insensitive(self) -> None:
        """DUBBED detection is case-insensitive ('dubbed', 'Dubbed', 'DUBBED')."""
        for variant in ("dubbed", "Dubbed", "DUBBED"):
            langs = self._parse(f"Movie.1080p.{variant}.x264")
            assert "Dubbed" in langs, f"Expected 'Dubbed' for variant {variant!r}"

    def test_dual_audio_dash_separated(self) -> None:
        """'DUAL-AUDIO' (dash) is detected as 'Dual Audio'."""
        langs = self._parse("Show.S01.DUAL-AUDIO.1080p")
        assert "Dual Audio" in langs


# ===========================================================================
# Section 1 (continued): DUB / DUAL AUDIO detection — Zilean
# ===========================================================================


class TestZileanLanguageDubDetection:
    """Test _parse_languages() in ZileanClient for dub/dual audio tags."""

    def setup_method(self) -> None:
        self.client = ZileanClient()

    def _parse(self, raw_title: str) -> list[str]:
        return self.client._parse_languages(raw_title)

    def test_dubbed_keyword_detected(self) -> None:
        langs = self._parse("Movie.1080p.DUBBED.x264")
        assert "Dubbed" in langs

    def test_dub_alone_matched(self) -> None:
        """Bare 'DUB' IS matched — _DUB_RE = r'\\bDUB(?:BED)?\\b' matches both DUB and DUBBED."""
        langs = self._parse("Movie.1080p.DUB.x264")
        assert "Dubbed" in langs

    def test_english_dub_bare_detected(self) -> None:
        """'ENGLISH.DUB' adds 'Dubbed' because standalone DUB is now matched."""
        langs = self._parse("Movie.ENGLISH.DUB.1080p")
        assert "Dubbed" in langs

    def test_spanish_dubbed_detects_both(self) -> None:
        langs = self._parse("Movie.SPANISH.DUBBED.1080p")
        assert "Spanish" in langs
        assert "Dubbed" in langs

    def test_dual_audio_space_detected(self) -> None:
        langs = self._parse("Movie.Dual.Audio.1080p")
        assert "Dual Audio" in langs

    def test_dual_audio_nospace_detected(self) -> None:
        langs = self._parse("Movie.DualAudio.1080p")
        assert "Dual Audio" in langs

    def test_dual_standalone_not_detected(self) -> None:
        """'DUAL' without 'AUDIO' is NOT detected — requires DUAL AUDIO to avoid false positives."""
        langs = self._parse("Movie.DUAL.1080p")
        assert "Dual Audio" not in langs

    def test_plain_release_clean(self) -> None:
        langs = self._parse("Movie.1080p.x264")
        assert "Dubbed" not in langs
        assert "Dual Audio" not in langs

    def test_dublin_no_false_positive(self) -> None:
        langs = self._parse("Dublin.2024.1080p.WEB-DL")
        assert "Dubbed" not in langs

    def test_dutch_no_dual_audio_false_positive(self) -> None:
        langs = self._parse("Movie.DUTCH.1080p")
        assert "Dual Audio" not in langs

    def test_japanese_dubbed_detects_all_three(self) -> None:
        """'JAPANESE.DUBBED' release detects Japanese and Dubbed."""
        langs = self._parse("Anime.Show.JAPANESE.DUBBED.1080p")
        assert "Japanese" in langs
        assert "Dubbed" in langs


# ===========================================================================
# Section 2: ISO mapping
# ===========================================================================


class TestIsoToLanguageName:
    """Test iso_to_language_name() for ISO 639-1 code → language name mapping."""

    def test_japanese_iso(self) -> None:
        assert iso_to_language_name("ja") == "Japanese"

    def test_english_iso(self) -> None:
        assert iso_to_language_name("en") == "English"

    def test_korean_iso(self) -> None:
        assert iso_to_language_name("ko") == "Korean"

    def test_none_input_returns_none(self) -> None:
        assert iso_to_language_name(None) is None

    def test_unknown_code_returns_none(self) -> None:
        assert iso_to_language_name("xx") is None

    def test_case_insensitive_uppercase(self) -> None:
        """'JA' (uppercase) should map the same as 'ja'."""
        assert iso_to_language_name("JA") == "Japanese"

    def test_case_insensitive_mixed(self) -> None:
        assert iso_to_language_name("Ko") == "Korean"

    def test_german_iso(self) -> None:
        assert iso_to_language_name("de") == "German"

    def test_french_iso(self) -> None:
        assert iso_to_language_name("fr") == "French"

    def test_spanish_iso(self) -> None:
        assert iso_to_language_name("es") == "Spanish"

    def test_all_mapped_codes_round_trip(self) -> None:
        """Every code in ISO_639_1_TO_LANGUAGE must be retrievable."""
        for code, expected_name in ISO_639_1_TO_LANGUAGE.items():
            assert iso_to_language_name(code) == expected_name

    def test_empty_string_returns_none(self) -> None:
        """Empty string is not a valid ISO code."""
        assert iso_to_language_name("") is None

    def test_chinese_iso(self) -> None:
        assert iso_to_language_name("zh") == "Chinese"

    def test_russian_iso(self) -> None:
        assert iso_to_language_name("ru") == "Russian"


# ===========================================================================
# Section 3: TMDB original_language parsing
# ===========================================================================


class TestTmdbOriginalLanguageParsing:
    """Test that TmdbClient._parse_item() and get_show_details() extract original_language."""

    def setup_method(self) -> None:
        self.client = TmdbClient()

    # ------------------------------------------------------------------
    # _parse_item()
    # ------------------------------------------------------------------

    def test_parse_item_movie_extracts_original_language(self) -> None:
        """_parse_item extracts original_language from a movie raw dict."""
        raw = {
            "id": 12345,
            "title": "Spirited Away",
            "release_date": "2001-07-20",
            "media_type": "movie",
            "overview": "A Japanese animated film",
            "original_language": "ja",
        }
        item = self.client._parse_item(raw, media_type="movie")
        assert item is not None
        assert item.original_language == "ja"

    def test_parse_item_tv_extracts_original_language(self) -> None:
        """_parse_item extracts original_language from a TV raw dict."""
        raw = {
            "id": 67890,
            "name": "Attack on Titan",
            "first_air_date": "2013-04-07",
            "media_type": "tv",
            "overview": "A Japanese anime series",
            "original_language": "ja",
        }
        item = self.client._parse_item(raw, media_type="tv")
        assert item is not None
        assert item.original_language == "ja"

    def test_parse_item_english_content(self) -> None:
        """_parse_item correctly stores 'en' for English content."""
        raw = {
            "id": 99999,
            "title": "The Dark Knight",
            "release_date": "2008-07-18",
            "media_type": "movie",
            "overview": "An English movie",
            "original_language": "en",
        }
        item = self.client._parse_item(raw, media_type="movie")
        assert item is not None
        assert item.original_language == "en"

    def test_parse_item_missing_original_language_is_none(self) -> None:
        """When original_language is absent from response, field is None."""
        raw = {
            "id": 11111,
            "title": "Unknown Lang Movie",
            "release_date": "2020-01-01",
            "media_type": "movie",
            "overview": "",
        }
        item = self.client._parse_item(raw, media_type="movie")
        assert item is not None
        assert item.original_language is None

    def test_parse_item_korean_content(self) -> None:
        """_parse_item extracts Korean original language."""
        raw = {
            "id": 22222,
            "name": "Squid Game",
            "first_air_date": "2021-09-17",
            "media_type": "tv",
            "overview": "A Korean thriller series",
            "original_language": "ko",
        }
        item = self.client._parse_item(raw, media_type="tv")
        assert item is not None
        assert item.original_language == "ko"

    # ------------------------------------------------------------------
    # get_show_details()
    # ------------------------------------------------------------------

    async def test_get_show_details_returns_original_language(self) -> None:
        """get_show_details() populates original_language from TMDB /tv/{id} response."""
        raw_response = {
            "id": 12345,
            "name": "Attack on Titan",
            "first_air_date": "2013-04-07",
            "overview": "Anime about titans",
            "original_language": "ja",
            "status": "Ended",
            "vote_average": 9.0,
            "number_of_seasons": 4,
            "seasons": [],
            "genres": [],
            "next_episode_to_air": None,
            "last_episode_to_air": None,
            "external_ids": {"imdb_id": "tt2560140", "tvdb_id": 267440},
        }

        mock_cfg = MagicMock()
        mock_cfg.enabled = True
        mock_cfg.api_key = "test-key"
        mock_cfg.base_url = "https://api.themoviedb.org/3"
        mock_cfg.timeout_seconds = 10

        # Use httpx mock transport to intercept the HTTP call
        mock_response = _make_response(200, raw_response)

        class _FakeTransport(httpx.AsyncBaseTransport):
            async def handle_async_request(self, request):
                mock_response.request = request
                return mock_response

        async def _fake_get_client() -> httpx.AsyncClient:
            return httpx.AsyncClient(
                base_url="https://api.themoviedb.org/3",
                transport=_FakeTransport(),
            )

        with (
            patch("src.services.tmdb.settings.tmdb", mock_cfg),
            patch("src.services.tmdb.get_circuit_breaker", return_value=MagicMock(
                before_request=AsyncMock(),
                record_success=AsyncMock(),
                record_failure=AsyncMock(),
            )),
        ):
            self.client._get_client = _fake_get_client  # type: ignore[method-assign]
            detail = await self.client.get_show_details(12345)

        assert detail is not None
        assert detail.original_language == "ja"

    async def test_get_show_details_english_original_language(self) -> None:
        """get_show_details() stores 'en' for English shows."""
        raw_response = {
            "id": 99998,
            "name": "Breaking Bad",
            "first_air_date": "2008-01-20",
            "overview": "An English drama",
            "original_language": "en",
            "status": "Ended",
            "vote_average": 9.5,
            "number_of_seasons": 5,
            "seasons": [],
            "genres": [],
            "next_episode_to_air": None,
            "last_episode_to_air": None,
            "external_ids": {"imdb_id": "tt0903747", "tvdb_id": 81189},
        }

        mock_cfg = MagicMock()
        mock_cfg.enabled = True
        mock_cfg.api_key = "test-key"
        mock_cfg.base_url = "https://api.themoviedb.org/3"
        mock_cfg.timeout_seconds = 10

        mock_response = _make_response(200, raw_response)

        class _FakeTransport(httpx.AsyncBaseTransport):
            async def handle_async_request(self, request):
                mock_response.request = request
                return mock_response

        async def _fake_get_client() -> httpx.AsyncClient:
            return httpx.AsyncClient(
                base_url="https://api.themoviedb.org/3",
                transport=_FakeTransport(),
            )

        with (
            patch("src.services.tmdb.settings.tmdb", mock_cfg),
            patch("src.services.tmdb.get_circuit_breaker", return_value=MagicMock(
                before_request=AsyncMock(),
                record_success=AsyncMock(),
                record_failure=AsyncMock(),
            )),
        ):
            self.client._get_client = _fake_get_client  # type: ignore[method-assign]
            detail = await self.client.get_show_details(99998)

        assert detail is not None
        assert detail.original_language == "en"


# ===========================================================================
# Section 4: _score_original_language() in filter engine
# ===========================================================================


class TestScoreOriginalLanguage:
    """Test FilterEngine._score_original_language() directly."""

    def test_feature_disabled_returns_zero(self) -> None:
        """When prefer_original_language=False, score is always 0.0."""
        with _patch_filter_settings(_NO_ORIG_LANG_FILTERS):
            score = ENGINE._score_original_language(["Japanese"], "ja")
        assert score == 0.0

    def test_feature_disabled_with_dubbed_tag_still_zero(self) -> None:
        """Even if content has Dubbed tag, returns 0 when feature is off."""
        with _patch_filter_settings(_NO_ORIG_LANG_FILTERS):
            score = ENGINE._score_original_language(["Dubbed"], "ja")
        assert score == 0.0

    def test_english_original_returns_zero(self) -> None:
        """English-original content is never scored (not applicable)."""
        with _patch_filter_settings(_ORIG_LANG_FILTERS):
            score = ENGINE._score_original_language([], "en")
        assert score == 0.0

    def test_english_full_name_returns_zero(self) -> None:
        """'English' as full name is treated same as 'en'."""
        with _patch_filter_settings(_ORIG_LANG_FILTERS):
            score = ENGINE._score_original_language(["Japanese"], "English")
        assert score == 0.0

    def test_none_original_language_returns_zero(self) -> None:
        """None original_language → no score."""
        with _patch_filter_settings(_ORIG_LANG_FILTERS):
            score = ENGINE._score_original_language(["Japanese"], None)
        assert score == 0.0

    def test_japanese_original_with_japanese_tag_positive(self) -> None:
        """Japanese original + Japanese language tag → +15.0."""
        with _patch_filter_settings(_ORIG_LANG_FILTERS):
            score = ENGINE._score_original_language(["Japanese"], "Japanese")
        assert score == 15.0

    def test_japanese_original_with_dual_audio_only(self) -> None:
        """Japanese original + Dual Audio only → dual_audio_bonus - half_penalty.

        "Dual Audio" alone leaves the effective language set as
        {'dual audio'} — after removing tagged markers the set is empty, so
        the half-penalty for assumed-English applies.  Net = bonus - half_penalty.
        """
        with _patch_filter_settings(_ORIG_LANG_FILTERS):
            score = ENGINE._score_original_language(["Dual Audio"], "Japanese")
        expected = (
            _ORIG_LANG_FILTERS.dual_audio_bonus
            - _ORIG_LANG_FILTERS.dub_penalty / 2
        )
        assert score == expected

    def test_japanese_original_with_dubbed_penalty(self) -> None:
        """Japanese original + Dubbed tag (no JP) → dub_penalty only.

        The "dubbed" explicit penalty and the "no language tags" half-penalty
        are mutually exclusive.  Once the dub penalty fires, the no-tags
        penalty is not additionally applied.
        """
        with _patch_filter_settings(_ORIG_LANG_FILTERS):
            score = ENGINE._score_original_language(["Dubbed"], "Japanese")
        expected = -_ORIG_LANG_FILTERS.dub_penalty
        assert score == expected

    def test_japanese_original_no_tags_half_penalty(self) -> None:
        """Japanese original + no language tags (assumed EN) → -dub_penalty/2."""
        with _patch_filter_settings(_ORIG_LANG_FILTERS):
            score = ENGINE._score_original_language([], "Japanese")
        assert score == -(_ORIG_LANG_FILTERS.dub_penalty / 2)

    def test_japanese_original_with_both_japanese_and_dual_audio(self) -> None:
        """Japanese original + Japanese + Dual Audio → +15 + bonus."""
        with _patch_filter_settings(_ORIG_LANG_FILTERS):
            score = ENGINE._score_original_language(
                ["Japanese", "Dual Audio"], "Japanese"
            )
        expected = 15.0 + _ORIG_LANG_FILTERS.dual_audio_bonus
        assert score == expected

    def test_dubbed_into_original_language_no_penalty(self) -> None:
        """Dubbed tag but original language IS in tags → no dub penalty."""
        # Edge case: "JAPANESE DUBBED" — labelled as dubbed but original lang detected
        with _patch_filter_settings(_ORIG_LANG_FILTERS):
            score = ENGINE._score_original_language(
                ["Japanese", "Dubbed"], "Japanese"
            )
        # Japanese match (+15) but no dub penalty because orig_lower is in effective_langs
        # Dual audio bonus does not apply here
        assert score == 15.0

    def test_korean_original_with_korean_tag(self) -> None:
        """Korean original + Korean tag → +15.0."""
        with _patch_filter_settings(_ORIG_LANG_FILTERS):
            score = ENGINE._score_original_language(["Korean"], "Korean")
        assert score == 15.0

    def test_korean_original_with_dubbed_penalty(self) -> None:
        """Korean original + Dubbed (no Korean) → dub_penalty only.

        Dub penalty and no-tags half-penalty are mutually exclusive.
        """
        with _patch_filter_settings(_ORIG_LANG_FILTERS):
            score = ENGINE._score_original_language(["Dubbed"], "Korean")
        expected = -_ORIG_LANG_FILTERS.dub_penalty
        assert score == expected

    def test_korean_original_no_tags_half_penalty(self) -> None:
        """Korean original, no language tags → -penalty/2."""
        with _patch_filter_settings(_ORIG_LANG_FILTERS):
            score = ENGINE._score_original_language([], "Korean")
        assert score == -(_ORIG_LANG_FILTERS.dub_penalty / 2)

    def test_iso_code_as_original_language_en_returns_zero(self) -> None:
        """ISO code 'en' is also treated as English and returns 0."""
        with _patch_filter_settings(_ORIG_LANG_FILTERS):
            score = ENGINE._score_original_language([], "en")
        assert score == 0.0

    def test_custom_dub_penalty_applied(self) -> None:
        """Custom dub_penalty value is respected.

        With Dubbed + empty remainder: only the explicit dub_penalty fires.
        The no-tags half-penalty is suppressed because dub_penalty_applied=True.
        """
        custom_filters = FiltersConfig(
            prefer_original_language=True,
            dub_penalty=30,
            dual_audio_bonus=5,
        )
        with _patch_filter_settings(custom_filters):
            score = ENGINE._score_original_language(["Dubbed"], "Japanese")
        # -30 (dub only — no double penalty)
        assert score == -30.0

    def test_custom_dual_audio_bonus_applied(self) -> None:
        """Custom dual_audio_bonus value is respected.

        With Dual Audio only: +bonus - half_penalty.
        """
        custom_filters = FiltersConfig(
            prefer_original_language=True,
            dub_penalty=20,
            dual_audio_bonus=15,
        )
        with _patch_filter_settings(custom_filters):
            score = ENGINE._score_original_language(["Dual Audio"], "Japanese")
        # +15 (dual audio bonus) - 10 (half of dub_penalty=20) = 5
        assert score == 15 - 20 / 2


# ===========================================================================
# Section 4b: language_name_to_iso() reverse lookup
# ===========================================================================


class TestLanguageNameToIso:
    """Test language_name_to_iso() for full language name → ISO code mapping."""

    def test_japanese_name(self) -> None:
        assert language_name_to_iso("Japanese") == "ja"

    def test_korean_name(self) -> None:
        assert language_name_to_iso("Korean") == "ko"

    def test_english_name(self) -> None:
        assert language_name_to_iso("English") == "en"

    def test_arabic_name(self) -> None:
        assert language_name_to_iso("Arabic") == "ar"

    def test_hindi_name(self) -> None:
        assert language_name_to_iso("Hindi") == "hi"

    def test_case_insensitive_lowercase(self) -> None:
        assert language_name_to_iso("japanese") == "ja"

    def test_case_insensitive_uppercase(self) -> None:
        assert language_name_to_iso("JAPANESE") == "ja"

    def test_none_input_returns_none(self) -> None:
        assert language_name_to_iso(None) is None

    def test_empty_string_returns_none(self) -> None:
        assert language_name_to_iso("") is None

    def test_unknown_name_returns_none(self) -> None:
        assert language_name_to_iso("Klingon") is None

    def test_round_trip_iso_to_name_to_iso(self) -> None:
        """Every ISO code in the map survives a round-trip via the language name."""
        for code in ISO_639_1_TO_LANGUAGE:
            name = iso_to_language_name(code)
            assert name is not None, f"iso_to_language_name({code!r}) returned None"
            assert language_name_to_iso(name) == code, (
                f"Round-trip failed: {code!r} → {name!r} → {language_name_to_iso(name)!r}"
            )


class TestExpandedLanguageMap:
    """Test that ISO_639_1_TO_LANGUAGE covers the expected set of languages."""

    def test_arabic_present(self) -> None:
        assert iso_to_language_name("ar") == "Arabic"

    def test_hindi_present(self) -> None:
        assert iso_to_language_name("hi") == "Hindi"

    def test_thai_present(self) -> None:
        assert iso_to_language_name("th") == "Thai"

    def test_turkish_present(self) -> None:
        assert iso_to_language_name("tr") == "Turkish"

    def test_polish_present(self) -> None:
        assert iso_to_language_name("pl") == "Polish"

    def test_swedish_present(self) -> None:
        assert iso_to_language_name("sv") == "Swedish"

    def test_danish_present(self) -> None:
        assert iso_to_language_name("da") == "Danish"

    def test_norwegian_no_present(self) -> None:
        assert iso_to_language_name("no") == "Norwegian"

    def test_finnish_present(self) -> None:
        assert iso_to_language_name("fi") == "Finnish"

    def test_czech_present(self) -> None:
        assert iso_to_language_name("cs") == "Czech"

    def test_hungarian_present(self) -> None:
        assert iso_to_language_name("hu") == "Hungarian"

    def test_romanian_present(self) -> None:
        assert iso_to_language_name("ro") == "Romanian"

    def test_greek_present(self) -> None:
        assert iso_to_language_name("el") == "Greek"

    def test_hebrew_present(self) -> None:
        assert iso_to_language_name("he") == "Hebrew"

    def test_indonesian_present(self) -> None:
        assert iso_to_language_name("id") == "Indonesian"

    def test_vietnamese_present(self) -> None:
        assert iso_to_language_name("vi") == "Vietnamese"

    def test_ukrainian_present(self) -> None:
        assert iso_to_language_name("uk") == "Ukrainian"

    def test_persian_present(self) -> None:
        assert iso_to_language_name("fa") == "Persian"

    def test_map_has_at_least_forty_entries(self) -> None:
        """The map must cover at least 40 languages."""
        assert len(ISO_639_1_TO_LANGUAGE) >= 40


# ===========================================================================
# Section 4c: ISO/name normalisation in _score_original_language
# ===========================================================================


class TestScoreOriginalLanguageNormalisation:
    """Test that _score_original_language accepts both ISO codes and full names."""

    def test_iso_code_matches_language_tag(self) -> None:
        """ISO code 'ja' matches a result tagged 'Japanese'."""
        with _patch_filter_settings(_ORIG_LANG_FILTERS):
            score = ENGINE._score_original_language(["Japanese"], "ja")
        assert score == 15.0

    def test_full_name_matches_language_tag(self) -> None:
        """Full name 'Japanese' matches a result tagged 'Japanese'."""
        with _patch_filter_settings(_ORIG_LANG_FILTERS):
            score = ENGINE._score_original_language(["Japanese"], "Japanese")
        assert score == 15.0

    def test_iso_code_ko_matches_korean_tag(self) -> None:
        """ISO code 'ko' matches a result tagged 'Korean'."""
        with _patch_filter_settings(_ORIG_LANG_FILTERS):
            score = ENGINE._score_original_language(["Korean"], "ko")
        assert score == 15.0

    def test_iso_code_applies_dub_penalty(self) -> None:
        """ISO code 'ja' triggers dub penalty when only 'Dubbed' is tagged."""
        with _patch_filter_settings(_ORIG_LANG_FILTERS):
            score = ENGINE._score_original_language(["Dubbed"], "ja")
        assert score == -_ORIG_LANG_FILTERS.dub_penalty

    def test_iso_code_no_tags_applies_half_penalty(self) -> None:
        """ISO code 'ja' with no language tags applies only the half-penalty."""
        with _patch_filter_settings(_ORIG_LANG_FILTERS):
            score = ENGINE._score_original_language([], "ja")
        assert score == -(_ORIG_LANG_FILTERS.dub_penalty / 2)

    def test_iso_en_returns_zero(self) -> None:
        """ISO 'en' is still treated as English (disabled)."""
        with _patch_filter_settings(_ORIG_LANG_FILTERS):
            score = ENGINE._score_original_language(["Japanese"], "en")
        assert score == 0.0

    def test_unknown_iso_code_falls_back_gracefully(self) -> None:
        """An unknown ISO-like code (2 chars) falls back to raw lowercase comparison."""
        # "zz" is not in the map; effective_langs won't contain it,
        # so the result should get the half-penalty (no tags match).
        with _patch_filter_settings(_ORIG_LANG_FILTERS):
            score = ENGINE._score_original_language([], "zz")
        assert score == -(_ORIG_LANG_FILTERS.dub_penalty / 2)

    def test_case_insensitive_full_name(self) -> None:
        """'japanese' (lowercase) matches a 'Japanese' tag just like 'Japanese'."""
        with _patch_filter_settings(_ORIG_LANG_FILTERS):
            score = ENGINE._score_original_language(["Japanese"], "japanese")
        assert score == 15.0


# ===========================================================================
# Section 4d: double-penalty regression tests
# ===========================================================================


class TestScoreOriginalLanguageNoPenaltyStack:
    """Verify that the dub penalty and no-tags penalty never stack."""

    def test_dubbed_only_single_penalty(self) -> None:
        """A result with only 'Dubbed' tag gets exactly -dub_penalty (not 1.5x)."""
        with _patch_filter_settings(_ORIG_LANG_FILTERS):
            score = ENGINE._score_original_language(["Dubbed"], "Japanese")
        assert score == -_ORIG_LANG_FILTERS.dub_penalty

    def test_dubbed_only_not_double_penalised(self) -> None:
        """Score must NOT equal -(dub_penalty + dub_penalty/2) for Dubbed-only."""
        with _patch_filter_settings(_ORIG_LANG_FILTERS):
            score = ENGINE._score_original_language(["Dubbed"], "Japanese")
        double = -(_ORIG_LANG_FILTERS.dub_penalty + _ORIG_LANG_FILTERS.dub_penalty / 2)
        assert score != double

    def test_no_tags_gets_half_penalty_only(self) -> None:
        """A result with no tags gets exactly -dub_penalty/2 (not more)."""
        with _patch_filter_settings(_ORIG_LANG_FILTERS):
            score = ENGINE._score_original_language([], "Japanese")
        assert score == -(_ORIG_LANG_FILTERS.dub_penalty / 2)

    def test_dubbed_penalty_larger_than_no_tags_penalty(self) -> None:
        """Dubbed result is penalised more than a no-tags result."""
        with _patch_filter_settings(_ORIG_LANG_FILTERS):
            dubbed_score = ENGINE._score_original_language(["Dubbed"], "Japanese")
            no_tags_score = ENGINE._score_original_language([], "Japanese")
        assert dubbed_score < no_tags_score

    def test_dubbed_into_original_no_penalty_no_half_penalty(self) -> None:
        """'Japanese Dubbed' — original lang IS in tags — no penalty at all."""
        with _patch_filter_settings(_ORIG_LANG_FILTERS):
            score = ENGINE._score_original_language(["Japanese", "Dubbed"], "Japanese")
        # +15 for JP match, no dub penalty (orig lang in tags), no half-penalty
        assert score == 15.0


# ===========================================================================
# Section 5: Integration — filter_and_rank with original_language
# ===========================================================================


class TestFilterAndRankOriginalLanguage:
    """Integration test: filter_and_rank() with original_language parameter."""

    def _make_results_set(self) -> list[TorrentioResult]:
        """Create three results: JP original, dual audio, and English dub."""
        return [
            _make_torrentio_result(
                info_hash="1" * 40,
                title="Test.Anime.S01E01.JAPANESE.1080p.WEB-DL.x265",
                languages=["Japanese"],
            ),
            _make_torrentio_result(
                info_hash="2" * 40,
                title="Test.Anime.S01E01.Dual.Audio.1080p.WEB-DL.x265",
                languages=["Dual Audio"],
            ),
            _make_torrentio_result(
                info_hash="3" * 40,
                title="Test.Anime.S01E01.DUBBED.1080p.WEB-DL.x265",
                languages=["Dubbed"],
            ),
        ]

    def test_japanese_original_ranks_higher_than_dub(self) -> None:
        """With prefer_original_language=True, JP audio ranks above dubbed."""
        results = self._make_results_set()
        with (
            patch("src.core.filter_engine.settings.quality", _DEFAULT_QUALITY_CONFIG),
            _patch_filter_settings(_ORIG_LANG_FILTERS),
        ):
            ranked = ENGINE.filter_and_rank(
                results, original_language="Japanese"
            )

        assert len(ranked) == 3
        titles = [fr.result.title for fr in ranked]
        # JP original should rank ahead of English dubbed
        jp_idx = next(i for i, t in enumerate(titles) if "JAPANESE" in t)
        dub_idx = next(i for i, t in enumerate(titles) if "DUBBED" in t)
        assert jp_idx < dub_idx

    def test_dual_audio_ranks_above_dub(self) -> None:
        """Dual Audio ranks above pure English dub when feature is enabled."""
        results = self._make_results_set()
        with (
            patch("src.core.filter_engine.settings.quality", _DEFAULT_QUALITY_CONFIG),
            _patch_filter_settings(_ORIG_LANG_FILTERS),
        ):
            ranked = ENGINE.filter_and_rank(
                results, original_language="Japanese"
            )

        titles = [fr.result.title for fr in ranked]
        dual_idx = next(i for i, t in enumerate(titles) if "Dual.Audio" in t)
        dub_idx = next(i for i, t in enumerate(titles) if "DUBBED" in t)
        assert dual_idx < dub_idx

    def test_feature_disabled_ranking_unaffected(self) -> None:
        """Without prefer_original_language, all three results have equal OL score."""
        results = self._make_results_set()
        with (
            patch("src.core.filter_engine.settings.quality", _DEFAULT_QUALITY_CONFIG),
            _patch_filter_settings(_NO_ORIG_LANG_FILTERS),
        ):
            ranked = ENGINE.filter_and_rank(
                results, original_language="Japanese"
            )

        assert len(ranked) == 3
        # All original_language scores should be 0
        for fr in ranked:
            assert fr.score_breakdown.get("original_language", 0.0) == 0.0

    def test_original_language_none_no_scoring(self) -> None:
        """When original_language=None, no OL scoring is applied."""
        results = self._make_results_set()
        with (
            patch("src.core.filter_engine.settings.quality", _DEFAULT_QUALITY_CONFIG),
            _patch_filter_settings(_ORIG_LANG_FILTERS),
        ):
            ranked = ENGINE.filter_and_rank(
                results, original_language=None
            )

        for fr in ranked:
            assert fr.score_breakdown.get("original_language", 0.0) == 0.0

    def test_english_original_no_scoring(self) -> None:
        """English-original content gets 0 OL score even with feature enabled."""
        results = self._make_results_set()
        with (
            patch("src.core.filter_engine.settings.quality", _DEFAULT_QUALITY_CONFIG),
            _patch_filter_settings(_ORIG_LANG_FILTERS),
        ):
            ranked = ENGINE.filter_and_rank(
                results, original_language="English"
            )

        for fr in ranked:
            assert fr.score_breakdown.get("original_language", 0.0) == 0.0

    def test_score_breakdown_contains_original_language_key(self) -> None:
        """After ranking, each result's score_breakdown has an 'original_language' key."""
        results = [
            _make_torrentio_result(
                info_hash="a" * 40,
                languages=["Japanese"],
            )
        ]
        with (
            patch("src.core.filter_engine.settings.quality", _DEFAULT_QUALITY_CONFIG),
            _patch_filter_settings(_ORIG_LANG_FILTERS),
        ):
            ranked = ENGINE.filter_and_rank(
                results, original_language="Japanese"
            )

        assert len(ranked) == 1
        assert "original_language" in ranked[0].score_breakdown

    def test_positive_ol_score_added_to_total(self) -> None:
        """When OL score is positive, total score is higher than without it."""
        result = _make_torrentio_result(
            info_hash="a" * 40,
            languages=["Japanese"],
        )
        with (
            patch("src.core.filter_engine.settings.quality", _DEFAULT_QUALITY_CONFIG),
            _patch_filter_settings(_ORIG_LANG_FILTERS),
        ):
            ranked_with = ENGINE.filter_and_rank([result], original_language="Japanese")

        with (
            patch("src.core.filter_engine.settings.quality", _DEFAULT_QUALITY_CONFIG),
            _patch_filter_settings(_NO_ORIG_LANG_FILTERS),
        ):
            ranked_without = ENGINE.filter_and_rank([result], original_language="Japanese")

        assert len(ranked_with) == 1
        assert len(ranked_without) == 1
        assert ranked_with[0].score > ranked_without[0].score

    def test_negative_ol_score_reduces_total(self) -> None:
        """Dubbed content score is lower with OL feature on vs off."""
        result = _make_torrentio_result(
            info_hash="a" * 40,
            languages=["Dubbed"],
        )
        with (
            patch("src.core.filter_engine.settings.quality", _DEFAULT_QUALITY_CONFIG),
            _patch_filter_settings(_ORIG_LANG_FILTERS),
        ):
            ranked_with = ENGINE.filter_and_rank([result], original_language="Japanese")

        with (
            patch("src.core.filter_engine.settings.quality", _DEFAULT_QUALITY_CONFIG),
            _patch_filter_settings(_NO_ORIG_LANG_FILTERS),
        ):
            ranked_without = ENGINE.filter_and_rank([result], original_language="Japanese")

        assert len(ranked_with) == 1
        assert len(ranked_without) == 1
        assert ranked_with[0].score < ranked_without[0].score


# ===========================================================================
# Section 6: Fixtures and helpers for integration tests
# ===========================================================================


@pytest.fixture
def override_db(session: AsyncSession):
    """Override the FastAPI get_db dependency with the test session."""

    async def _override() -> AsyncSession:
        yield session

    app.dependency_overrides[get_db] = _override
    yield
    app.dependency_overrides.clear()


@pytest.fixture
async def http(override_db) -> AsyncClient:
    """Async HTTP client backed by the FastAPI test app."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def _persist_item(
    session: AsyncSession,
    *,
    title: str = "Spirited Away",
    imdb_id: str = "tt0245429",
    tmdb_id: str | None = "129",
    media_type: MediaType = MediaType.MOVIE,
    state: QueueState = QueueState.COMPLETE,
    original_language: str | None = "ja",
    metadata_json: str | None = None,
) -> MediaItem:
    """Persist a MediaItem and return it."""
    item = MediaItem(
        imdb_id=imdb_id,
        tmdb_id=tmdb_id,
        title=title,
        year=2001,
        media_type=media_type,
        state=state,
        state_changed_at=datetime.now(UTC),
        retry_count=0,
        original_language=original_language,
        metadata_json=metadata_json,
    )
    session.add(item)
    await session.flush()
    return item


# ===========================================================================
# Section 7: Data population — AddToQueueRequest and season pack split
# ===========================================================================


class TestAddToQueueRequestOriginalLanguage:
    """AddToQueueRequest.original_language field validation."""

    def test_accepts_iso_code(self) -> None:
        """AddToQueueRequest accepts ISO 639-1 codes like 'ja'."""
        from src.api.routes.discover import AddToQueueRequest

        req = AddToQueueRequest(
            tmdb_id=12345,
            media_type="movie",
            title="Spirited Away",
            year=2001,
            original_language="ja",
        )
        assert req.original_language == "ja"

    def test_accepts_full_language_name(self) -> None:
        """AddToQueueRequest accepts full language names like 'Japanese'."""
        from src.api.routes.discover import AddToQueueRequest

        req = AddToQueueRequest(
            tmdb_id=12345,
            media_type="movie",
            title="Spirited Away",
            year=2001,
            original_language="Japanese",
        )
        assert req.original_language == "Japanese"

    def test_accepts_none(self) -> None:
        """AddToQueueRequest accepts None for original_language."""
        from src.api.routes.discover import AddToQueueRequest

        req = AddToQueueRequest(
            tmdb_id=12345,
            media_type="movie",
            title="The Dark Knight",
            year=2008,
        )
        assert req.original_language is None

    async def test_add_to_queue_stores_original_language(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """POST /api/discover/add stores original_language on the created item."""
        with (
            patch("src.services.tmdb.settings.tmdb") as mock_tmdb_cfg,
            patch(
                "src.api.routes.discover.tmdb_client.get_external_ids",
                new_callable=AsyncMock,
                return_value=MagicMock(imdb_id="tt0245429", tvdb_id=None),
            ),
        ):
            mock_tmdb_cfg.api_key = "test-key"
            mock_tmdb_cfg.enabled = True
            resp = await http.post(
                "/api/discover/add",
                json={
                    "tmdb_id": 129,
                    "media_type": "movie",
                    "title": "Spirited Away",
                    "year": 2001,
                    "original_language": "ja",
                },
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "created"

        from sqlalchemy import select as _select

        result = await session.execute(
            _select(MediaItem).where(MediaItem.tmdb_id == "129")
        )
        item = result.scalar_one_or_none()
        assert item is not None
        assert item.original_language == "ja"


class TestSeasonPackSplitCopiesOriginalLanguage:
    """Season pack split must copy original_language from parent to child items."""

    async def test_split_copies_original_language(
        self, session: AsyncSession
    ) -> None:
        """_split_season_pack_to_episodes creates items with parent's original_language."""
        from src.core.scrape_pipeline import ScrapePipeline
        from src.services.tmdb import TmdbSeasonInfo, TmdbShowDetail

        # Create a season pack parent item with Japanese original language
        parent = MediaItem(
            imdb_id="tt1234567",
            tmdb_id="67890",
            title="Attack on Titan",
            year=2013,
            media_type=MediaType.SHOW,
            state=QueueState.SCRAPING,
            state_changed_at=datetime.now(UTC),
            retry_count=0,
            season=1,
            is_season_pack=True,
            original_language="ja",
        )
        session.add(parent)
        await session.flush()

        pipeline = ScrapePipeline()

        # _split_season_pack_to_episodes calls get_show_details() and reads
        # show_details.seasons to find the season's episode_count.
        mock_show_detail = TmdbShowDetail(
            tmdb_id=67890,
            title="Attack on Titan",
            year=2013,
            number_of_seasons=1,
            seasons=[
                TmdbSeasonInfo(season_number=1, episode_count=3),
            ],
        )

        with patch(
            "src.services.tmdb.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show_detail,
        ):
            created = await pipeline._split_season_pack_to_episodes(
                session, parent
            )

        assert created > 0

        # Verify all created items have the parent's original_language
        from sqlalchemy import select as _select

        result = await session.execute(
            _select(MediaItem).where(
                MediaItem.title == "Attack on Titan",
                MediaItem.is_season_pack == False,  # noqa: E712
                MediaItem.source == "season_pack_split",
            )
        )
        children = result.scalars().all()
        assert len(children) > 0
        for child in children:
            assert child.original_language == "ja", (
                f"Expected 'ja' but got {child.original_language!r} "
                f"for child episode {child.episode}"
            )

    async def test_split_with_none_original_language_stays_none(
        self, session: AsyncSession
    ) -> None:
        """When parent has no original_language, child items also have None."""
        from src.core.scrape_pipeline import ScrapePipeline
        from src.services.tmdb import TmdbSeasonInfo, TmdbShowDetail

        parent = MediaItem(
            imdb_id="tt1111111",
            tmdb_id="11111",
            title="Unknown Language Show",
            year=2020,
            media_type=MediaType.SHOW,
            state=QueueState.SCRAPING,
            state_changed_at=datetime.now(UTC),
            retry_count=0,
            season=1,
            is_season_pack=True,
            original_language=None,
        )
        session.add(parent)
        await session.flush()

        pipeline = ScrapePipeline()

        mock_show_detail = TmdbShowDetail(
            tmdb_id=11111,
            title="Unknown Language Show",
            year=2020,
            number_of_seasons=1,
            seasons=[
                TmdbSeasonInfo(season_number=1, episode_count=2),
            ],
        )

        with patch(
            "src.services.tmdb.tmdb_client.get_show_details",
            new_callable=AsyncMock,
            return_value=mock_show_detail,
        ):
            created = await pipeline._split_season_pack_to_episodes(
                session, parent
            )

        assert created > 0

        from sqlalchemy import select as _select

        result = await session.execute(
            _select(MediaItem).where(
                MediaItem.title == "Unknown Language Show",
                MediaItem.source == "season_pack_split",
            )
        )
        children = result.scalars().all()
        assert len(children) > 0
        for child in children:
            assert child.original_language is None


# ===========================================================================
# Section 8: MediaItemResponse includes original_language
# ===========================================================================


class TestMediaItemResponseOriginalLanguage:
    """MediaItemResponse.original_language field is included in API responses."""

    async def test_queue_list_includes_original_language(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """GET /api/queue returns original_language for each item."""
        await _persist_item(
            session,
            title="Test Anime",
            imdb_id="tt0000002",
            original_language="ja",
            state=QueueState.WANTED,
        )
        resp = await http.get("/api/queue")
        assert resp.status_code == 200
        items = resp.json()["items"]
        assert len(items) >= 1
        test_anime_items = [i for i in items if i["title"] == "Test Anime"]
        assert len(test_anime_items) == 1
        assert test_anime_items[0]["original_language"] == "ja"

    async def test_queue_item_without_original_language_is_null(
        self, http: AsyncClient, session: AsyncSession
    ) -> None:
        """Items without original_language return null in API response."""
        await _persist_item(
            session,
            title="No Lang Movie",
            imdb_id="tt99990001",
            original_language=None,
            state=QueueState.WANTED,
        )
        resp = await http.get("/api/queue")
        assert resp.status_code == 200
        items = resp.json()["items"]
        no_lang_items = [i for i in items if i["title"] == "No Lang Movie"]
        assert len(no_lang_items) == 1
        assert no_lang_items[0]["original_language"] is None
