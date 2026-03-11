"""Tests for HTTP 451 (infringing_file) handling in Real-Debrid and the scrape pipeline.

Covers:
  Section 1 — CacheCheckResult.blocked field semantics
  Section 2 — check_cached returning blocked=True on HTTP 451
  Section 3 — Pipeline fallback loop when add_magnet raises 451
  Section 4 — Edge cases (single candidate, reusable rd_id bypasses fallback)

All external HTTP calls are mocked — no real network traffic is generated.
asyncio_mode = "auto" (set in pyproject.toml), so no @pytest.mark.asyncio needed.
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.media_item import MediaItem, MediaType, QueueState
from src.models.torrent import RdTorrent, TorrentStatus
from src.services.real_debrid import (
    CacheCheckResult,
    RealDebridAuthError,
    RealDebridClient,
    RealDebridError,
    RealDebridRateLimitError,
)
from src.services.torrentio import TorrentioResult


# ---------------------------------------------------------------------------
# Helpers shared across sections
# ---------------------------------------------------------------------------


def _make_torrentio_result(info_hash: str, title: str = "Movie.2024.1080p.WEB-DL.x265-GROUP") -> TorrentioResult:
    """Build a TorrentioResult with a specific info_hash."""
    return TorrentioResult(
        info_hash=info_hash,
        title=title,
        resolution="1080p",
        codec="x265",
        quality="WEB-DL",
        size_bytes=2 * 1024**3,
        seeders=100,
        release_group="GROUP",
        languages=[],
        is_season_pack=False,
    )


def _make_filtered_result(
    info_hash: str,
    score: float = 75.0,
    title: str = "Movie.2024.1080p.WEB-DL.x265-GROUP",
) -> object:
    """Return a mock FilteredResult-like object backed by a real TorrentioResult."""
    fr = MagicMock()
    fr.result = _make_torrentio_result(info_hash, title)
    fr.score = score
    fr.rejection_reason = None
    fr.score_breakdown = {
        "resolution": 40.0,
        "codec": 15.0,
        "source": 12.0,
        "audio": 3.0,
        "seeders": 10.0,
        "cached": 0.0,
        "season_pack": 0.0,
    }
    return fr


_PATCH_TARGETS = {
    "mount_scanner_available": "src.core.scrape_pipeline.mount_scanner.is_mount_available",
    "mount_scanner_lookup": "src.core.scrape_pipeline.mount_scanner.lookup",
    "dedup_check": "src.core.scrape_pipeline.dedup_engine.check_content_duplicate",
    "dedup_local": "src.core.scrape_pipeline.dedup_engine.check_local_duplicate",
    "dedup_register": "src.core.scrape_pipeline.dedup_engine.register_torrent",
    "zilean_search": "src.core.scrape_pipeline.zilean_client.search",
    "torrentio_movie": "src.core.scrape_pipeline.torrentio_client.scrape_movie",
    "torrentio_episode": "src.core.scrape_pipeline.torrentio_client.scrape_episode",
    "rd_add": "src.core.scrape_pipeline.rd_client.add_magnet",
    "rd_select": "src.core.scrape_pipeline.rd_client.select_files",
    "rd_check_cached_batch": "src.core.scrape_pipeline.rd_client.check_cached_batch",
    "rd_delete": "src.core.scrape_pipeline.rd_client.delete_torrent",
    "filter_rank": "src.core.scrape_pipeline.filter_engine.filter_and_rank",
    "queue_transition": "src.core.scrape_pipeline.queue_manager.transition",
}


class _Mocks:
    """Namespace holding all active mock objects for a pipeline test."""

    mount_scanner_available: AsyncMock
    mount_scanner_lookup: AsyncMock
    dedup_check: AsyncMock
    dedup_local: AsyncMock
    dedup_register: AsyncMock
    zilean_search: AsyncMock
    torrentio_movie: AsyncMock
    torrentio_episode: AsyncMock
    rd_add: AsyncMock
    rd_select: AsyncMock
    rd_check_cached_batch: AsyncMock
    rd_delete: AsyncMock
    filter_rank: MagicMock
    queue_transition: AsyncMock


@asynccontextmanager
async def _all_mocks(
    mock_rd_torrent: RdTorrent | None = None,
) -> AsyncGenerator[_Mocks, None]:
    """Patch every ScrapePipeline dependency at once with safe defaults."""
    mocks = _Mocks()
    patchers = {name: patch(target) for name, target in _PATCH_TARGETS.items()}
    started: dict[str, MagicMock] = {}
    try:
        for name, patcher in patchers.items():
            started[name] = patcher.start()
    except Exception:
        for p in patchers.values():
            try:
                p.stop()
            except RuntimeError:
                pass
        raise

    mocks.mount_scanner_available = started["mount_scanner_available"]
    mocks.mount_scanner_available.return_value = True

    mocks.mount_scanner_lookup = started["mount_scanner_lookup"]
    mocks.mount_scanner_lookup.return_value = []

    mocks.dedup_check = started["dedup_check"]
    mocks.dedup_check.return_value = None

    mocks.dedup_local = started["dedup_local"]
    mocks.dedup_local.return_value = None

    mocks.dedup_register = started["dedup_register"]
    mocks.dedup_register.return_value = mock_rd_torrent or MagicMock(spec=RdTorrent)

    mocks.zilean_search = started["zilean_search"]
    mocks.zilean_search.return_value = []

    mocks.torrentio_movie = started["torrentio_movie"]
    mocks.torrentio_movie.return_value = []

    mocks.torrentio_episode = started["torrentio_episode"]
    mocks.torrentio_episode.return_value = []

    mocks.rd_add = started["rd_add"]
    mocks.rd_add.return_value = {"id": "RD123", "uri": "magnet:?xt=urn:btih:" + "a" * 40}

    mocks.rd_select = started["rd_select"]
    mocks.rd_select.return_value = None

    mocks.rd_check_cached_batch = started["rd_check_cached_batch"]
    mocks.rd_check_cached_batch.return_value = {}

    mocks.rd_delete = started["rd_delete"]
    mocks.rd_delete.return_value = None

    mocks.filter_rank = started["filter_rank"]
    mocks.filter_rank.return_value = []

    mocks.queue_transition = started["queue_transition"]
    mocks.queue_transition.side_effect = None
    mocks.queue_transition.return_value = MagicMock(spec=MediaItem)

    try:
        yield mocks
    finally:
        for p in patchers.values():
            p.stop()


def _import_pipeline():
    from src.core.scrape_pipeline import PipelineResult, ScrapePipeline  # noqa: PLC0415

    return ScrapePipeline, PipelineResult


# ---------------------------------------------------------------------------
# Section 1: CacheCheckResult.blocked field
# ---------------------------------------------------------------------------


class TestCacheCheckResultBlockedField:
    """CacheCheckResult.blocked field semantics."""

    def test_default_blocked_is_false(self) -> None:
        """CacheCheckResult constructed without blocked= defaults to False."""
        result = CacheCheckResult(info_hash="a" * 40, cached=False)
        assert result.blocked is False

    def test_blocked_can_be_set_true(self) -> None:
        """CacheCheckResult accepts blocked=True."""
        result = CacheCheckResult(info_hash="a" * 40, cached=False, blocked=True)
        assert result.blocked is True

    def test_blocked_false_explicit(self) -> None:
        """CacheCheckResult accepts blocked=False explicitly."""
        result = CacheCheckResult(info_hash="b" * 40, cached=True, blocked=False)
        assert result.blocked is False

    def test_backward_compatible_no_blocked_arg(self) -> None:
        """Existing callers that omit blocked= still get a valid object."""
        result = CacheCheckResult(info_hash="c" * 40, cached=None)
        assert result.blocked is False
        assert result.info_hash == "c" * 40
        assert result.cached is None

    def test_blocked_true_with_rd_id_none(self) -> None:
        """blocked=True is valid even when rd_id is None."""
        result = CacheCheckResult(info_hash="d" * 40, cached=False, blocked=True, rd_id=None)
        assert result.blocked is True
        assert result.rd_id is None

    def test_blocked_false_with_rd_id_set(self) -> None:
        """A kept (rd_id set) non-blocked result is valid."""
        result = CacheCheckResult(
            info_hash="e" * 40, cached=True, blocked=False, rd_id="RD_KEPT"
        )
        assert result.blocked is False
        assert result.rd_id == "RD_KEPT"


# ---------------------------------------------------------------------------
# Section 2: check_cached 451 handling
# ---------------------------------------------------------------------------


class TestCheckCached451Handling:
    """check_cached returns blocked=True on HTTP 451, with proper cleanup."""

    @pytest.fixture()
    def client(self) -> RealDebridClient:
        """A fresh RealDebridClient instance for each test."""
        return RealDebridClient()

    async def test_451_on_add_magnet_returns_blocked_true(
        self, client: RealDebridClient
    ) -> None:
        """check_cached returns cached=False, blocked=True when add_magnet raises 451."""
        info_hash = "a" * 40
        blocked_exc = RealDebridError("infringing_file", status_code=451, error_code=35)

        with patch.object(client, "add_magnet", new_callable=AsyncMock) as mock_add, \
             patch.object(client, "delete_torrent", new_callable=AsyncMock):
            mock_add.side_effect = blocked_exc
            result = await client.check_cached(info_hash)

        assert result.blocked is True
        assert result.cached is False
        assert result.info_hash == info_hash

    async def test_451_rd_id_is_none(self, client: RealDebridClient) -> None:
        """When 451 fires before an rd_id is obtained, rd_id in the result is None."""
        info_hash = "b" * 40
        blocked_exc = RealDebridError("infringing_file", status_code=451, error_code=35)

        with patch.object(client, "add_magnet", new_callable=AsyncMock) as mock_add, \
             patch.object(client, "delete_torrent", new_callable=AsyncMock):
            mock_add.side_effect = blocked_exc
            result = await client.check_cached(info_hash)

        assert result.rd_id is None

    async def test_451_cleanup_not_called_when_no_rd_id(
        self, client: RealDebridClient
    ) -> None:
        """delete_torrent is NOT called when 451 fires before any rd_id is obtained."""
        info_hash = "c" * 40
        blocked_exc = RealDebridError("infringing_file", status_code=451, error_code=35)

        with patch.object(client, "add_magnet", new_callable=AsyncMock) as mock_add, \
             patch.object(client, "delete_torrent", new_callable=AsyncMock) as mock_del:
            mock_add.side_effect = blocked_exc
            await client.check_cached(info_hash)

        mock_del.assert_not_called()

    async def test_451_cleanup_called_when_rd_id_was_obtained(
        self, client: RealDebridClient
    ) -> None:
        """delete_torrent IS called when 451 fires after add_magnet already returned an rd_id.

        This is an unlikely but theoretically possible scenario: add_magnet succeeds,
        a second call inside the try block raises 451.  The finally block still runs
        cleanup since should_keep remains False.
        """
        info_hash = "d" * 40

        # add_magnet returns an id, but get_torrent_info raises 451
        get_info_exc = RealDebridError("infringing_file", status_code=451, error_code=35)

        with patch.object(
            client, "add_magnet", new_callable=AsyncMock,
            return_value={"id": "RD_X", "uri": ""},
        ), patch.object(
            client, "get_torrent_info", new_callable=AsyncMock,
            side_effect=get_info_exc,
        ), patch.object(
            client, "delete_torrent", new_callable=AsyncMock
        ) as mock_del:
            result = await client.check_cached(info_hash)

        # The 451 propagated from get_torrent_info is caught by the
        # RealDebridError handler, so blocked=True is returned and
        # the finally block deletes the orphaned torrent.
        assert result.blocked is True
        mock_del.assert_called_once_with("RD_X")

    async def test_non_451_error_returns_cached_none_not_blocked(
        self, client: RealDebridClient
    ) -> None:
        """A 503 from add_magnet returns cached=None, blocked=False (transient error)."""
        info_hash = "e" * 40
        transient_exc = RealDebridError("Service Unavailable", status_code=503)

        with patch.object(client, "add_magnet", new_callable=AsyncMock) as mock_add, \
             patch.object(client, "delete_torrent", new_callable=AsyncMock):
            mock_add.side_effect = transient_exc
            result = await client.check_cached(info_hash)

        assert result.blocked is False
        assert result.cached is None

    async def test_auth_error_returns_cached_none_not_blocked(
        self, client: RealDebridClient
    ) -> None:
        """A 401 from add_magnet returns cached=None, blocked=False."""
        info_hash = "f" * 40
        auth_exc = RealDebridAuthError("Bad token", status_code=401, error_code=8)

        with patch.object(client, "add_magnet", new_callable=AsyncMock) as mock_add, \
             patch.object(client, "delete_torrent", new_callable=AsyncMock):
            mock_add.side_effect = auth_exc
            result = await client.check_cached(info_hash)

        assert result.blocked is False
        assert result.cached is None

    async def test_rate_limit_error_is_not_blocked(
        self, client: RealDebridClient
    ) -> None:
        """A 429 that exhausts all retries returns cached=None, blocked=False."""
        info_hash = "g" * 40
        rate_exc = RealDebridRateLimitError("Too many requests", status_code=429)

        with patch.object(client, "add_magnet", new_callable=AsyncMock) as mock_add, \
             patch.object(client, "delete_torrent", new_callable=AsyncMock), \
             patch("src.services.real_debrid.asyncio.sleep", new_callable=AsyncMock):
            mock_add.side_effect = rate_exc
            result = await client.check_cached(info_hash)

        assert result.blocked is False
        assert result.cached is None

    async def test_451_error_code_35_specifically(
        self, client: RealDebridClient
    ) -> None:
        """Verify the specific RD error_code=35 is preserved in the exception."""
        exc = RealDebridError("infringing_file", status_code=451, error_code=35)
        assert exc.status_code == 451
        assert exc.error_code == 35

    async def test_keep_if_cached_true_still_returns_blocked(
        self, client: RealDebridClient
    ) -> None:
        """keep_if_cached=True does not suppress blocked=True on 451."""
        info_hash = "h" * 40
        blocked_exc = RealDebridError("infringing_file", status_code=451, error_code=35)

        with patch.object(client, "add_magnet", new_callable=AsyncMock) as mock_add, \
             patch.object(client, "delete_torrent", new_callable=AsyncMock):
            mock_add.side_effect = blocked_exc
            result = await client.check_cached(info_hash, keep_if_cached=True)

        assert result.blocked is True
        assert result.cached is False


# ---------------------------------------------------------------------------
# Section 3: Pipeline fallback loop on 451
# ---------------------------------------------------------------------------


class TestPipelineFallbackOn451:
    """_step_add_to_rd falls back through ranked candidates on HTTP 451."""

    async def test_first_candidate_451_second_succeeds(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """When add_magnet raises 451 for the first hash, the second candidate is used."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        hash_1 = "1" * 40
        hash_2 = "2" * 40
        fr1 = _make_filtered_result(hash_1, score=90.0)
        fr2 = _make_filtered_result(hash_2, score=80.0)

        def _add_magnet_side_effect(magnet: str):
            if hash_1 in magnet:
                raise RealDebridError("infringing_file", status_code=451, error_code=35)
            return {"id": "RD_2", "uri": ""}

        async with _all_mocks() as m:
            m.torrentio_movie.return_value = [_make_torrentio_result("f" * 40)]
            m.filter_rank.return_value = [fr1, fr2]
            m.rd_check_cached_batch.return_value = {}
            m.rd_add.side_effect = _add_magnet_side_effect

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.action == "added_to_rd"
        assert result.selected_hash == hash_2

    async def test_first_candidate_451_result_uses_second_hash(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """The scrape log selected_result records the fallback winner's hash."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        hash_1 = "1" * 40
        hash_2 = "2" * 40
        fr1 = _make_filtered_result(hash_1, score=90.0)
        fr2 = _make_filtered_result(hash_2, score=80.0)

        def _add_magnet_side_effect(magnet: str):
            if hash_1 in magnet:
                raise RealDebridError("infringing_file", status_code=451, error_code=35)
            return {"id": "RD_WINNER", "uri": ""}

        async with _all_mocks() as m:
            m.torrentio_movie.return_value = [_make_torrentio_result("f" * 40)]
            m.filter_rank.return_value = [fr1, fr2]
            m.rd_check_cached_batch.return_value = {}
            m.rd_add.side_effect = _add_magnet_side_effect

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.selected_hash == hash_2

    async def test_all_candidates_blocked_transitions_to_sleeping(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """When every candidate is 451-blocked, the item transitions to SLEEPING."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        hash_1 = "1" * 40
        hash_2 = "2" * 40
        hash_3 = "3" * 40
        fr1 = _make_filtered_result(hash_1, score=90.0)
        fr2 = _make_filtered_result(hash_2, score=80.0)
        fr3 = _make_filtered_result(hash_3, score=70.0)

        blocked_exc = RealDebridError("infringing_file", status_code=451, error_code=35)

        async with _all_mocks() as m:
            m.torrentio_movie.return_value = [_make_torrentio_result("f" * 40)]
            m.filter_rank.return_value = [fr1, fr2, fr3]
            m.rd_check_cached_batch.return_value = {}
            m.rd_add.side_effect = blocked_exc

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.action == "error"
        sleeping_called = any(
            QueueState.SLEEPING in call_args.args
            or QueueState.SLEEPING in call_args.kwargs.values()
            for call_args in m.queue_transition.call_args_list
        )
        assert sleeping_called

    async def test_all_candidates_blocked_result_message_mentions_451(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """The error message when all candidates are blocked mentions 451."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        hash_1 = "1" * 40
        fr1 = _make_filtered_result(hash_1, score=90.0)

        blocked_exc = RealDebridError("infringing_file", status_code=451, error_code=35)

        async with _all_mocks() as m:
            m.torrentio_movie.return_value = [_make_torrentio_result("f" * 40)]
            m.filter_rank.return_value = [fr1]
            m.rd_check_cached_batch.return_value = {}
            m.rd_add.side_effect = blocked_exc

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert "451" in result.message or "blocked" in result.message.lower()

    async def test_first_451_second_rate_limit_stops_loop(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """A transient error (rate limit) after a 451 stops the loop and sends to SLEEPING."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        hash_1 = "1" * 40
        hash_2 = "2" * 40
        fr1 = _make_filtered_result(hash_1, score=90.0)
        fr2 = _make_filtered_result(hash_2, score=80.0)

        def _add_side_effect(magnet: str):
            if hash_1 in magnet:
                raise RealDebridError("infringing_file", status_code=451, error_code=35)
            raise RealDebridRateLimitError("Too many requests", status_code=429)

        async with _all_mocks() as m:
            m.torrentio_movie.return_value = [_make_torrentio_result("f" * 40)]
            m.filter_rank.return_value = [fr1, fr2]
            m.rd_check_cached_batch.return_value = {}
            m.rd_add.side_effect = _add_side_effect

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.action == "error"
        sleeping_called = any(
            QueueState.SLEEPING in call_args.args
            or QueueState.SLEEPING in call_args.kwargs.values()
            for call_args in m.queue_transition.call_args_list
        )
        assert sleeping_called

    async def test_first_451_second_network_error_stops_loop(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """An unexpected exception (network error) after a 451 stops the loop → SLEEPING."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        hash_1 = "1" * 40
        hash_2 = "2" * 40
        fr1 = _make_filtered_result(hash_1, score=90.0)
        fr2 = _make_filtered_result(hash_2, score=80.0)

        import httpx  # noqa: PLC0415

        def _add_side_effect(magnet: str):
            if hash_1 in magnet:
                raise RealDebridError("infringing_file", status_code=451, error_code=35)
            raise httpx.TimeoutException("connection timed out")

        async with _all_mocks() as m:
            m.torrentio_movie.return_value = [_make_torrentio_result("f" * 40)]
            m.filter_rank.return_value = [fr1, fr2]
            m.rd_check_cached_batch.return_value = {}
            m.rd_add.side_effect = _add_side_effect

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.action == "error"

    async def test_cache_results_blocked_hashes_pre_filtered(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """Hashes flagged blocked=True in cache_results are excluded before the loop."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        hash_blocked = "b" * 40
        hash_ok = "c" * 40
        fr_blocked = _make_filtered_result(hash_blocked, score=95.0)
        fr_ok = _make_filtered_result(hash_ok, score=85.0)

        # cache_results marks the top-scored hash as blocked
        blocked_cache = {
            hash_blocked: CacheCheckResult(
                info_hash=hash_blocked, cached=False, blocked=True
            )
        }

        async with _all_mocks() as m:
            m.torrentio_movie.return_value = [_make_torrentio_result("f" * 40)]
            m.filter_rank.return_value = [fr_blocked, fr_ok]
            m.rd_check_cached_batch.return_value = blocked_cache
            m.rd_add.return_value = {"id": "RD_OK", "uri": ""}

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.action == "added_to_rd"
        # add_magnet must have been called with the OK hash, not the blocked one
        assert m.rd_add.call_count >= 1
        for mock_call in m.rd_add.call_args_list:
            magnet_arg = mock_call.args[0] if mock_call.args else ""
            assert hash_blocked not in magnet_arg, (
                f"Blocked hash {hash_blocked} was submitted to add_magnet"
            )

    async def test_all_cache_blocked_no_candidates_transitions_sleeping(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """When all ranked results are pre-filtered due to blocked cache results → SLEEPING."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        hash_1 = "1" * 40
        hash_2 = "2" * 40
        fr1 = _make_filtered_result(hash_1, score=90.0)
        fr2 = _make_filtered_result(hash_2, score=80.0)

        all_blocked_cache = {
            hash_1: CacheCheckResult(info_hash=hash_1, cached=False, blocked=True),
            hash_2: CacheCheckResult(info_hash=hash_2, cached=False, blocked=True),
        }

        async with _all_mocks() as m:
            m.torrentio_movie.return_value = [_make_torrentio_result("f" * 40)]
            m.filter_rank.return_value = [fr1, fr2]
            m.rd_check_cached_batch.return_value = all_blocked_cache

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.action == "error"
        m.rd_add.assert_not_called()
        sleeping_called = any(
            QueueState.SLEEPING in call_args.args
            or QueueState.SLEEPING in call_args.kwargs.values()
            for call_args in m.queue_transition.call_args_list
        )
        assert sleeping_called

    async def test_three_candidates_middle_one_wins(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """451 on first, success on second — third candidate never attempted."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        hash_1 = "1" * 40
        hash_2 = "2" * 40
        hash_3 = "3" * 40
        fr1 = _make_filtered_result(hash_1, score=90.0)
        fr2 = _make_filtered_result(hash_2, score=80.0)
        fr3 = _make_filtered_result(hash_3, score=70.0)

        call_order: list[str] = []

        def _add_side_effect(magnet: str):
            if hash_1 in magnet:
                call_order.append("hash_1_blocked")
                raise RealDebridError("infringing_file", status_code=451, error_code=35)
            if hash_2 in magnet:
                call_order.append("hash_2_ok")
                return {"id": "RD_2", "uri": ""}
            call_order.append("hash_3_never")
            return {"id": "RD_3", "uri": ""}

        async with _all_mocks() as m:
            m.torrentio_movie.return_value = [_make_torrentio_result("f" * 40)]
            m.filter_rank.return_value = [fr1, fr2, fr3]
            m.rd_check_cached_batch.return_value = {}
            m.rd_add.side_effect = _add_side_effect

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.action == "added_to_rd"
        assert result.selected_hash == hash_2
        assert "hash_3_never" not in call_order

    async def test_transition_to_adding_on_fallback_success(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """On a successful fallback, the item transitions to ADDING (not SLEEPING)."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        hash_1 = "1" * 40
        hash_2 = "2" * 40
        fr1 = _make_filtered_result(hash_1, score=90.0)
        fr2 = _make_filtered_result(hash_2, score=80.0)

        def _add_side_effect(magnet: str):
            if hash_1 in magnet:
                raise RealDebridError("infringing_file", status_code=451, error_code=35)
            return {"id": "RD_GOOD", "uri": ""}

        async with _all_mocks() as m:
            m.torrentio_movie.return_value = [_make_torrentio_result("f" * 40)]
            m.filter_rank.return_value = [fr1, fr2]
            m.rd_check_cached_batch.return_value = {}
            m.rd_add.side_effect = _add_side_effect

            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        adding_called = any(
            QueueState.ADDING in call_args.args
            or QueueState.ADDING in call_args.kwargs.values()
            for call_args in m.queue_transition.call_args_list
        )
        assert adding_called


# ---------------------------------------------------------------------------
# Section 4: Edge cases
# ---------------------------------------------------------------------------


class TestPipeline451EdgeCases:
    """Edge cases for the 451 fallback path."""

    async def test_single_candidate_gets_451_transitions_sleeping(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """A single candidate that gets 451 sends the item to SLEEPING."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        hash_only = "0" * 40
        fr_only = _make_filtered_result(hash_only, score=80.0)

        blocked_exc = RealDebridError("infringing_file", status_code=451, error_code=35)

        async with _all_mocks() as m:
            m.torrentio_movie.return_value = [_make_torrentio_result("f" * 40)]
            m.filter_rank.return_value = [fr_only]
            m.rd_check_cached_batch.return_value = {}
            m.rd_add.side_effect = blocked_exc

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.action == "error"
        sleeping_called = any(
            QueueState.SLEEPING in call_args.args
            or QueueState.SLEEPING in call_args.kwargs.values()
            for call_args in m.queue_transition.call_args_list
        )
        assert sleeping_called

    async def test_reusable_rd_id_from_cache_check_bypasses_add_magnet(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """When cache check kept the torrent (rd_id in cache_results), add_magnet is skipped."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        hash_kept = "k" * 40
        fr_kept = _make_filtered_result(hash_kept, score=90.0)

        # Simulate the cache check having kept the torrent
        kept_cache = {
            hash_kept: CacheCheckResult(
                info_hash=hash_kept, cached=True, rd_id="RD_KEPT"
            )
        }

        async with _all_mocks() as m:
            m.torrentio_movie.return_value = [_make_torrentio_result("f" * 40)]
            m.filter_rank.return_value = [fr_kept]
            m.rd_check_cached_batch.return_value = kept_cache
            # rd_add should never be called since the kept rd_id is reused
            m.rd_add.side_effect = AssertionError("add_magnet must not be called")

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.action == "added_to_rd"
        m.rd_add.assert_not_called()

    async def test_reusable_rd_id_used_even_when_other_candidates_would_be_451(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """The kept rd_id path exits before the loop can encounter any 451 candidates."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        hash_kept = "k" * 40
        hash_would_be_451 = "x" * 40
        fr_kept = _make_filtered_result(hash_kept, score=90.0)
        fr_blocked = _make_filtered_result(hash_would_be_451, score=80.0)

        kept_cache = {
            hash_kept: CacheCheckResult(
                info_hash=hash_kept, cached=True, rd_id="RD_KEPT_REUSE"
            )
        }

        async with _all_mocks() as m:
            m.torrentio_movie.return_value = [_make_torrentio_result("f" * 40)]
            m.filter_rank.return_value = [fr_kept, fr_blocked]
            m.rd_check_cached_batch.return_value = kept_cache
            # Both candidates would 451 if add_magnet were called — but it must not be
            m.rd_add.side_effect = RealDebridError(
                "infringing_file", status_code=451, error_code=35
            )

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.action == "added_to_rd"
        m.rd_add.assert_not_called()

    async def test_451_does_not_increment_add_magnet_call_for_skipped_hash(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """add_magnet is called exactly once per non-blocked attempt (no retry on 451)."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        hash_1 = "1" * 40
        hash_2 = "2" * 40
        fr1 = _make_filtered_result(hash_1, score=90.0)
        fr2 = _make_filtered_result(hash_2, score=80.0)

        call_count_per_hash: dict[str, int] = {}

        def _add_side_effect(magnet: str):
            for h in (hash_1, hash_2):
                if h in magnet:
                    call_count_per_hash[h] = call_count_per_hash.get(h, 0) + 1
            if hash_1 in magnet:
                raise RealDebridError("infringing_file", status_code=451, error_code=35)
            return {"id": "RD_SECOND", "uri": ""}

        async with _all_mocks() as m:
            m.torrentio_movie.return_value = [_make_torrentio_result("f" * 40)]
            m.filter_rank.return_value = [fr1, fr2]
            m.rd_check_cached_batch.return_value = {}
            m.rd_add.side_effect = _add_side_effect

            pipeline = ScrapePipeline()
            await pipeline.run(session, wanted_item)

        # Each hash must be attempted exactly once
        assert call_count_per_hash.get(hash_1, 0) == 1
        assert call_count_per_hash.get(hash_2, 0) == 1

    async def test_451_mixed_with_blocked_cache_results(
        self, session: AsyncSession, wanted_item: MediaItem
    ) -> None:
        """Blocked cache result for hash_1 AND live 451 on hash_2 → SLEEPING."""
        ScrapePipeline, PipelineResult = _import_pipeline()
        hash_1 = "1" * 40
        hash_2 = "2" * 40
        fr1 = _make_filtered_result(hash_1, score=90.0)
        fr2 = _make_filtered_result(hash_2, score=80.0)

        # hash_1 blocked in cache, hash_2 gets a live 451
        mixed_cache = {
            hash_1: CacheCheckResult(info_hash=hash_1, cached=False, blocked=True),
        }
        blocked_exc = RealDebridError("infringing_file", status_code=451, error_code=35)

        async with _all_mocks() as m:
            m.torrentio_movie.return_value = [_make_torrentio_result("f" * 40)]
            m.filter_rank.return_value = [fr1, fr2]
            m.rd_check_cached_batch.return_value = mixed_cache
            m.rd_add.side_effect = blocked_exc  # fires for hash_2

            pipeline = ScrapePipeline()
            result: PipelineResult = await pipeline.run(session, wanted_item)

        assert result.action == "error"
        # hash_1 must never reach add_magnet (pre-filtered)
        for mock_call in m.rd_add.call_args_list:
            magnet_arg = mock_call.args[0] if mock_call.args else ""
            assert hash_1 not in magnet_arg
