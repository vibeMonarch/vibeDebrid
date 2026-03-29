"""Tests for src/services/torrent_parser.py.

Covers the shared torrent name parsing utilities used by torrentio.py,
zilean.py, mount_scanner.py, and symlink_manager.py.

Groups:
  - parse_languages: token detection, Cyrillic, abbreviations, DUB/DUAL,
    word-boundary false-positive prevention, deduplication
  - parse_episode_fallbacks: all six fallback steps (dash-separated, ordinal
    season, episode range, BATCH keyword, Season N keyword, bare dash)
  - detect_season_pack: season-only pattern, complete marker, anime batch,
    single-episode passthrough
  - parse_episode_from_filename: PTN path, SxxExx regex, bare-E regex, bare
    trailing number (anime), NON_EPISODE_NUMBERS exclusion
"""

from __future__ import annotations

from src.services.torrent_parser import (
    ANIME_BARE_DASH_EP_RE,
    ANIME_BATCH_RE,
    ANIME_EP_RANGE_RE,
    ANIME_SEASON_KEYWORD_RE,
    BARE_TRAILING_EP_RE,
    COMPLETE_RE,
    CYRILLIC_RE,
    DUAL_AUDIO_RE,
    DUB_RE,
    LANGUAGE_ABBREV_RES,
    LANGUAGE_ABBREV_TOKENS,
    LANGUAGE_TOKENS,
    NON_EPISODE_NUMBERS,
    ORDINAL_SEASON_EP_RE,
    SEASON_DASH_EP_RE,
    SEASON_ONLY_RE,
    detect_season_pack,
    parse_episode_fallbacks,
    parse_episode_from_filename,
    parse_languages,
)

# ---------------------------------------------------------------------------
# parse_languages
# ---------------------------------------------------------------------------


class TestParseLanguages:
    """Tests for parse_languages()."""

    # --- Long-form token detection ---

    def test_french_token(self) -> None:
        result = parse_languages("Movie.2024.1080p.FRENCH.WEB-DL")
        assert result == ["French"]

    def test_truefrench_token(self) -> None:
        result = parse_languages("Movie.2024.1080p.TRUEFRENCH.WEB-DL")
        assert result == ["French"]

    def test_vostfr_token(self) -> None:
        result = parse_languages("Anime.S01E01.VOSTFR.1080p")
        assert result == ["French"]

    def test_german_token(self) -> None:
        result = parse_languages("Movie.2024.1080p.GERMAN.WEB-DL")
        assert result == ["German"]

    def test_deutsch_token(self) -> None:
        result = parse_languages("Movie.2024.DEUTSCH.1080p.WEB-DL")
        assert result == ["German"]

    def test_spanish_token(self) -> None:
        result = parse_languages("Movie.2024.SPANISH.1080p.WEB-DL")
        assert result == ["Spanish"]

    def test_portuguese_token(self) -> None:
        result = parse_languages("Movie.2024.PORTUGUESE.1080p.WEB-DL")
        assert result == ["Portuguese"]

    def test_italian_token(self) -> None:
        result = parse_languages("Movie.2024.ITALIAN.1080p.WEB-DL")
        assert result == ["Italian"]

    def test_dutch_token(self) -> None:
        result = parse_languages("Movie.2024.DUTCH.1080p.WEB-DL")
        assert result == ["Dutch"]

    def test_russian_token(self) -> None:
        result = parse_languages("Movie.2024.RUSSIAN.1080p.WEB-DL")
        assert result == ["Russian"]

    def test_japanese_token(self) -> None:
        result = parse_languages("Anime.S01E01.JAPANESE.1080p")
        assert result == ["Japanese"]

    def test_korean_token(self) -> None:
        result = parse_languages("Drama.S01E01.KOREAN.1080p")
        assert result == ["Korean"]

    def test_chinese_token(self) -> None:
        result = parse_languages("Movie.2024.CHINESE.1080p")
        assert result == ["Chinese"]

    def test_multi_token(self) -> None:
        result = parse_languages("Movie.2024.1080p.MULTI.WEB-DL")
        assert result == ["Multi"]

    # --- Cyrillic script detection ---

    def test_cyrillic_only(self) -> None:
        title = "Фильм 2024 1080p"
        result = parse_languages(title)
        assert "Russian" in result

    def test_cyrillic_full_russian_title(self) -> None:
        title = "Мстители: Финал (2019) [1080p]"
        result = parse_languages(title)
        assert "Russian" in result

    def test_cyrillic_no_duplicate_with_russian_token(self) -> None:
        title = "RUSSIAN Фильм 2024"
        result = parse_languages(title)
        assert result.count("Russian") == 1

    def test_cyrillic_with_other_language(self) -> None:
        title = "Movie.2024.FRENCH.1080p Описание"
        result = parse_languages(title)
        assert "French" in result
        assert "Russian" in result

    # --- ISO/scene abbreviation tokens ---

    def test_rus_abbreviation(self) -> None:
        result = parse_languages("Movie.2024.1080p.[RUS].WEB-DL")
        assert result == ["Russian"]

    def test_jap_abbreviation(self) -> None:
        result = parse_languages("Anime.S01E01.1080p.[JAP].WEB-DL")
        assert result == ["Japanese"]

    def test_jpn_abbreviation(self) -> None:
        result = parse_languages("Anime.S01E01.1080p.[JPN+ENG].WEB-DL")
        assert result == ["Japanese"]

    def test_kor_abbreviation(self) -> None:
        result = parse_languages("Drama.S01E01.1080p.[KOR].WEB-DL")
        assert result == ["Korean"]

    def test_chi_abbreviation(self) -> None:
        result = parse_languages("Drama.S01E01.1080p.[CHI].WEB-DL")
        assert result == ["Chinese"]

    def test_ita_abbreviation(self) -> None:
        result = parse_languages("Movie.2024.1080p.[ITA+ENG].WEB-DL")
        assert result == ["Italian"]

    def test_nld_abbreviation(self) -> None:
        result = parse_languages("Movie.2024.1080p.[NLD].WEB-DL")
        assert result == ["Dutch"]

    def test_deu_abbreviation(self) -> None:
        result = parse_languages("Movie.2024.1080p.[DEU].WEB-DL")
        assert result == ["German"]

    def test_spa_abbreviation(self) -> None:
        result = parse_languages("Movie.2024.1080p.[SPA].WEB-DL")
        assert result == ["Spanish"]

    def test_por_abbreviation(self) -> None:
        result = parse_languages("Movie.2024.1080p.[POR].WEB-DL")
        assert result == ["Portuguese"]

    def test_fra_abbreviation(self) -> None:
        result = parse_languages("Movie.2024.1080p.[FRA].WEB-DL")
        assert result == ["French"]

    def test_multiple_abbreviations(self) -> None:
        result = parse_languages("Anime.S01E01.1080p.[RUS + JAP].WEB-DL")
        assert "Russian" in result
        assert "Japanese" in result

    def test_abbreviation_no_duplicate_when_long_form_also_present(self) -> None:
        result = parse_languages("Movie.2024.1080p.RUSSIAN.[RUS].WEB-DL")
        assert result.count("Russian") == 1

    # --- Word-boundary false-positive prevention ---

    def test_brush_no_false_positive(self) -> None:
        result = parse_languages("Movie.2024.1080p.BRUSH.WEB-DL")
        assert "Russian" not in result

    def test_trust_no_false_positive(self) -> None:
        result = parse_languages("Movie.2024.TRUST.1080p.WEB-DL")
        assert "Russian" not in result

    def test_japan_full_word_not_confused_with_jap_abbreviation(self) -> None:
        # "JAPAN" is not in LANGUAGE_ABBREV_TOKENS, so it should not match
        # "JAP" as an abbreviation (JAP regex is word-boundary matched).
        result = parse_languages("Movie.2024.Japan.Release.1080p.WEB-DL")
        # JAPAN is not a direct token either, so should be empty
        assert "Japanese" not in result

    # --- DUB / DUAL AUDIO detection ---

    def test_dub_bare(self) -> None:
        result = parse_languages("Anime.S01E01.DUB.1080p")
        assert "Dubbed" in result

    def test_dubbed(self) -> None:
        result = parse_languages("Anime.S01E01.DUBBED.1080p")
        assert "Dubbed" in result

    def test_dub_case_insensitive(self) -> None:
        result = parse_languages("Anime.S01E01.dub.1080p")
        assert "Dubbed" in result

    def test_dub_word_boundary_prevents_dublin(self) -> None:
        result = parse_languages("The.Dublin.Murders.S01E01.1080p")
        assert "Dubbed" not in result

    def test_dual_audio(self) -> None:
        result = parse_languages("Anime.S01E01.DUAL.AUDIO.1080p")
        assert "Dual Audio" in result

    def test_dual_audio_with_dot_separator(self) -> None:
        result = parse_languages("Anime.S01E01.DUAL.AUDIO.1080p")
        assert "Dual Audio" in result

    def test_dual_audio_case_insensitive(self) -> None:
        result = parse_languages("Anime.S01E01.dual audio.1080p")
        assert "Dual Audio" in result

    def test_dual_survival_no_false_positive(self) -> None:
        # "DUAL" alone without "AUDIO" should not match DUAL_AUDIO_RE.
        result = parse_languages("Dual.Survival.S01E01.1080p.WEB-DL")
        assert "Dual Audio" not in result

    # --- Empty / English-only ---

    def test_regular_english_title_returns_empty(self) -> None:
        result = parse_languages("The.Dark.Knight.2008.1080p.BluRay.x264-GROUP")
        assert result == []

    def test_empty_title_returns_empty(self) -> None:
        result = parse_languages("")
        assert result == []

    # --- Deduplication ---

    def test_french_detected_once_from_multiple_french_tokens(self) -> None:
        result = parse_languages("Movie.FRENCH.TRUEFRENCH.VOSTFR.1080p")
        assert result.count("French") == 1


# ---------------------------------------------------------------------------
# parse_episode_fallbacks
# ---------------------------------------------------------------------------


class TestParseEpisodeFallbacks:
    """Tests for parse_episode_fallbacks()."""

    # --- Step 1: dash-separated anime notation ---

    def test_season_dash_ep_s2_dash_06(self) -> None:
        season, episode, is_batch = parse_episode_fallbacks(
            "[Group] Show S2 - 06 [1080p]", None, None
        )
        assert season == 2
        assert episode == 6
        assert is_batch is False

    def test_season_dash_ep_s02e06(self) -> None:
        season, episode, is_batch = parse_episode_fallbacks(
            "Show.S02-E06.1080p", None, None
        )
        assert season == 2
        assert episode == 6
        assert is_batch is False

    def test_season_dash_ep_not_triggered_when_episode_already_set(self) -> None:
        season, episode, is_batch = parse_episode_fallbacks(
            "[Group] Show S2 - 06 [1080p]", 2, 5
        )
        # episode was already set to 5 — fallback should not override it
        assert episode == 5

    # --- Step 2: ordinal season notation ---

    def test_ordinal_season_2nd_season_01(self) -> None:
        season, episode, is_batch = parse_episode_fallbacks(
            "[Group] Title 2nd Season - 01 [1080p]", None, None
        )
        assert season == 2
        assert episode == 1
        assert is_batch is False

    def test_ordinal_season_1st_season_28(self) -> None:
        season, episode, is_batch = parse_episode_fallbacks(
            "Show 1st Season - 28 [720p]", None, None
        )
        assert season == 1
        assert episode == 28
        assert is_batch is False

    def test_ordinal_season_3rd_season_ep(self) -> None:
        season, episode, is_batch = parse_episode_fallbacks(
            "Title 3rd Season - 05", None, None
        )
        assert season == 3
        assert episode == 5

    # --- Step 3: episode range → season pack ---

    def test_episode_range_01_13(self) -> None:
        season, episode, is_batch = parse_episode_fallbacks(
            "[Group] Show - 01 ~ 13 [1080p]", None, None
        )
        assert episode is None
        assert is_batch is True

    def test_episode_range_with_en_dash(self) -> None:
        season, episode, is_batch = parse_episode_fallbacks(
            "[Group] Show – 01 – 13 [1080p]", None, None
        )
        # en-dash range should still be detected
        assert is_batch is True

    def test_episode_range_single_number_not_batch(self) -> None:
        # "- 01 ~ 01" has ep_end == ep_start so should NOT be a batch
        season, episode, is_batch = parse_episode_fallbacks(
            "[Group] Show - 01 ~ 01 [1080p]", None, None
        )
        assert is_batch is False

    # --- Step 4: BATCH keyword ---

    def test_batch_keyword_in_brackets(self) -> None:
        season, episode, is_batch = parse_episode_fallbacks(
            "[Group] Show [BATCH] [1080p]", None, None
        )
        assert is_batch is True
        assert episode is None

    def test_batch_keyword_bare(self) -> None:
        season, episode, is_batch = parse_episode_fallbacks(
            "Show S01 BATCH 1080p", None, None
        )
        assert is_batch is True

    def test_batch_keyword_case_insensitive(self) -> None:
        season, episode, is_batch = parse_episode_fallbacks(
            "Show [batch] [1080p]", None, None
        )
        assert is_batch is True

    # --- Step 5: "Season N" keyword ---

    def test_season_keyword_season_2(self) -> None:
        season, episode, is_batch = parse_episode_fallbacks(
            "[Group] Show (Season 2) [1080p]", None, None
        )
        assert season == 2
        assert is_batch is True  # no episode → season pack

    def test_season_keyword_sets_batch_when_episode_still_none(self) -> None:
        # When "Season N" keyword fires (step 5) and episode is still None,
        # is_anime_batch is set to True even if a bare dash episode follows.
        # The bare dash (step 6) is then suppressed because is_anime_batch=True.
        season, episode, is_batch = parse_episode_fallbacks(
            "[Group] Show (Season 2) - 05 [1080p]", None, None
        )
        # Season keyword fires and sets is_anime_batch=True before step 6.
        assert season == 2
        assert is_batch is True
        # Bare dash is suppressed because is_anime_batch=True, so episode stays None
        assert episode is None

    def test_season_keyword_does_not_override_existing_season(self) -> None:
        # If season is already known, the Season N keyword block is skipped.
        season, episode, is_batch = parse_episode_fallbacks(
            "Show Season 2 [1080p]", 1, None
        )
        # season was already 1, should not be overridden
        assert season == 1

    # --- Step 6: bare dash episode ---

    def test_bare_dash_29(self) -> None:
        season, episode, is_batch = parse_episode_fallbacks(
            "[Group] Title - 29 [1080p]", None, None
        )
        assert episode == 29
        assert is_batch is False

    def test_bare_dash_not_triggered_when_batch(self) -> None:
        season, episode, is_batch = parse_episode_fallbacks(
            "[Group] Title [BATCH] - 01 [1080p]", None, None
        )
        # BATCH is detected first, then bare dash is suppressed
        assert is_batch is True
        assert episode is None

    # --- PTN values passed through unchanged ---

    def test_ptn_values_not_overridden_when_both_present(self) -> None:
        season, episode, is_batch = parse_episode_fallbacks(
            "Show.S02E05.1080p", 2, 5
        )
        assert season == 2
        assert episode == 5
        assert is_batch is False


# ---------------------------------------------------------------------------
# detect_season_pack
# ---------------------------------------------------------------------------


class TestDetectSeasonPack:
    """Tests for detect_season_pack()."""

    def test_season_only_pattern_s02(self) -> None:
        assert detect_season_pack("Show.S02.1080p.WEB-DL", None, False) is True

    def test_season_only_pattern_s1(self) -> None:
        assert detect_season_pack("Show.S1.1080p", None, False) is True

    def test_season_with_episode_not_a_pack(self) -> None:
        # S02E05 is NOT a season-only match (episode follows S02)
        assert detect_season_pack("Show.S02E05.1080p", 5, False) is False

    def test_complete_keyword(self) -> None:
        assert detect_season_pack("Show.Complete.Season.1080p", None, False) is True

    def test_season_keyword_in_title(self) -> None:
        assert detect_season_pack("Show.Season.2.1080p", None, False) is True

    def test_anime_batch_flag(self) -> None:
        assert detect_season_pack("[Group] Show [BATCH] [1080p]", None, True) is True

    def test_episode_set_always_false(self) -> None:
        # When episode is not None, detect_season_pack always returns False,
        # even if other markers are present.
        assert detect_season_pack("Show.S02.COMPLETE.1080p", 5, True) is False

    def test_no_markers_and_no_episode_is_false(self) -> None:
        # A title with no season markers and episode=None is not a season pack
        assert detect_season_pack("The.Dark.Knight.2008.1080p.BluRay", None, False) is False

    def test_regular_single_episode_is_false(self) -> None:
        assert detect_season_pack("Show.S01E01.1080p.WEB-DL", 1, False) is False


# ---------------------------------------------------------------------------
# parse_episode_from_filename
# ---------------------------------------------------------------------------


class TestParseEpisodeFromFilename:
    """Tests for parse_episode_from_filename()."""

    # --- Standard SxxExx notation ---

    def test_standard_s01e05(self) -> None:
        assert parse_episode_from_filename("Show.S01E05.1080p.mkv") == 5

    def test_standard_s02e12(self) -> None:
        assert parse_episode_from_filename("Show.S02E12.1080p.mkv") == 12

    def test_standard_uppercase(self) -> None:
        assert parse_episode_from_filename("Show.S03E07.BluRay.mkv") == 7

    def test_standard_lowercase(self) -> None:
        assert parse_episode_from_filename("show.s01e03.web-dl.mkv") == 3

    # --- Bare E-prefixed notation ---

    def test_bare_e_prefix(self) -> None:
        assert parse_episode_from_filename("Show - E05 - Title.mkv") == 5

    def test_bare_e_prefix_two_digits(self) -> None:
        assert parse_episode_from_filename("Show.E12.mkv") == 12

    # --- Bare trailing number (anime convention) ---

    def test_bare_trailing_number_01(self) -> None:
        assert parse_episode_from_filename("Show Title 01.mkv") == 1

    def test_bare_trailing_number_26(self) -> None:
        assert parse_episode_from_filename("Show Title 26.mkv") == 26

    def test_bare_trailing_with_dot_separator(self) -> None:
        assert parse_episode_from_filename("Show.Title.05.mkv") == 5

    # --- NON_EPISODE_NUMBERS exclusion ---

    def test_resolution_480_excluded(self) -> None:
        # "480" is in NON_EPISODE_NUMBERS — should not be treated as episode
        result = parse_episode_from_filename("Show.480p.mkv")
        assert result != 480

    def test_resolution_720_excluded(self) -> None:
        result = parse_episode_from_filename("Show.Title.720.mkv")
        assert result != 720

    def test_resolution_1080_excluded(self) -> None:
        result = parse_episode_from_filename("Show.Title.1080.mkv")
        assert result != 1080

    def test_resolution_2160_excluded(self) -> None:
        result = parse_episode_from_filename("Show.Title.2160.mkv")
        assert result != 2160

    def test_codec_264_excluded(self) -> None:
        result = parse_episode_from_filename("Show.x264.mkv")
        assert result != 264

    def test_codec_265_excluded(self) -> None:
        result = parse_episode_from_filename("Show.x265.mkv")
        assert result != 265

    # --- No episode found ---

    def test_no_episode_returns_none(self) -> None:
        assert parse_episode_from_filename("Movie.Title.2024.1080p.mkv") is None

    def test_empty_filename_returns_none(self) -> None:
        assert parse_episode_from_filename("") is None

    # --- Anime dash notation via PTN/regex ---

    def test_anime_dash_ep_via_ptn(self) -> None:
        # PTN typically handles "Show - 05.mkv" correctly; if it does not,
        # the bare trailing number fallback catches "05".
        result = parse_episode_from_filename("Attack on Titan - 05.mkv")
        assert result == 5


# ---------------------------------------------------------------------------
# Compiled regex smoke tests
# ---------------------------------------------------------------------------


class TestCompiledRegexes:
    """Spot-check that imported regex patterns compile and match as expected."""

    def test_season_only_re_matches_s02(self) -> None:
        assert SEASON_ONLY_RE.search("Show.S02.1080p") is not None

    def test_season_only_re_matches_s02_alone(self) -> None:
        # S02 with no episode following — clear season pack marker.
        assert SEASON_ONLY_RE.search("Show.S02.1080p.WEB-DL") is not None

    def test_complete_re_matches_complete(self) -> None:
        assert COMPLETE_RE.search("Show.Complete.Season") is not None

    def test_season_dash_ep_re_matches(self) -> None:
        assert SEASON_DASH_EP_RE.search("Show S2 - 06 [1080p]") is not None

    def test_ordinal_season_ep_re_matches(self) -> None:
        assert ORDINAL_SEASON_EP_RE.search("2nd Season - 01") is not None

    def test_anime_bare_dash_ep_re_matches(self) -> None:
        assert ANIME_BARE_DASH_EP_RE.search("[Group] Title - 29 [1080p]") is not None

    def test_anime_ep_range_re_matches(self) -> None:
        assert ANIME_EP_RANGE_RE.search("[Group] Show - 01 ~ 13 [1080p]") is not None

    def test_anime_batch_re_matches_bracket(self) -> None:
        assert ANIME_BATCH_RE.search("[BATCH]") is not None

    def test_anime_batch_re_matches_bare(self) -> None:
        assert ANIME_BATCH_RE.search("Show BATCH 1080p") is not None

    def test_anime_season_keyword_re_matches(self) -> None:
        assert ANIME_SEASON_KEYWORD_RE.search("(Season 2)") is not None

    def test_dub_re_matches_dub(self) -> None:
        assert DUB_RE.search("Anime.DUB.1080p") is not None

    def test_dub_re_matches_dubbed(self) -> None:
        assert DUB_RE.search("Anime.DUBBED.1080p") is not None

    def test_dual_audio_re_matches(self) -> None:
        assert DUAL_AUDIO_RE.search("Show.DUAL.AUDIO.1080p") is not None

    def test_bare_trailing_ep_re_matches(self) -> None:
        assert BARE_TRAILING_EP_RE.search("Show Title 01") is not None

    def test_cyrillic_re_matches(self) -> None:
        assert CYRILLIC_RE.search("Фильм") is not None

    def test_cyrillic_re_no_match_on_ascii(self) -> None:
        assert CYRILLIC_RE.search("Movie 2024") is None

    def test_non_episode_numbers_contains_resolutions(self) -> None:
        for val in (264, 265, 480, 576, 720, 1080, 2160, 4320):
            assert val in NON_EPISODE_NUMBERS

    def test_language_tokens_contains_french(self) -> None:
        assert "FRENCH" in LANGUAGE_TOKENS

    def test_language_abbrev_tokens_contains_rus(self) -> None:
        assert "RUS" in LANGUAGE_ABBREV_TOKENS

    def test_language_abbrev_res_compiled(self) -> None:
        # All abbreviation patterns should compile successfully
        for abbrev in LANGUAGE_ABBREV_TOKENS:
            assert abbrev in LANGUAGE_ABBREV_RES
        # Spot-check one pattern actually matches
        assert LANGUAGE_ABBREV_RES["RUS"].search("[RUS]") is not None
