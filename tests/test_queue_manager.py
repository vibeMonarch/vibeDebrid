"""Tests for src/core/queue_manager.py.

Covers:
  - Valid and invalid state transitions
  - SLEEPING retry logic (counter, scheduling, DORMANT redirect)
  - DORMANT next_retry_at scheduling and SCRAPING clear
  - force_transition (any-to-any, reset, warning log)
  - process_unreleased (past/future/no air_date)
  - get_ready_for_retry (past/future next_retry_at for SLEEPING and DORMANT)
  - process_queue (combined summary dict)
  - calculate_next_retry (clamping, schedule index)
  - Edge cases: ItemNotFoundError, retry_count reset on WANTED
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.queue_manager import (
    VALID_TRANSITIONS,
    InvalidTransitionError,
    ItemNotFoundError,
    QueueManager,
)
from src.models.media_item import MediaItem, MediaType, QueueState


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def _make_item(
    session: AsyncSession,
    *,
    state: QueueState,
    retry_count: int = 0,
    next_retry_at: datetime | None = None,
    air_date: date | None = None,
    imdb_id: str = "tt0000001",
    title: str = "Test Item",
) -> MediaItem:
    """Helper: persist a MediaItem with the given state to the test session."""
    item = MediaItem(
        imdb_id=imdb_id,
        title=title,
        year=2024,
        media_type=MediaType.MOVIE,
        state=state,
        state_changed_at=_utcnow(),
        retry_count=retry_count,
        next_retry_at=next_retry_at,
        air_date=air_date,
    )
    session.add(item)
    await session.flush()
    return item


# ---------------------------------------------------------------------------
# Group 1: Valid Transitions
# ---------------------------------------------------------------------------


class TestValidTransitions:
    """Every edge in the state diagram should succeed."""

    async def test_unreleased_to_wanted(self, session: AsyncSession) -> None:
        """UNRELEASED → WANTED is a valid transition."""
        item = await _make_item(session, state=QueueState.UNRELEASED)
        qm = QueueManager()
        result = await qm.transition(session, item.id, QueueState.WANTED)
        assert result.state == QueueState.WANTED

    async def test_wanted_to_scraping(self, session: AsyncSession) -> None:
        """WANTED → SCRAPING is the standard happy-path start."""
        item = await _make_item(session, state=QueueState.WANTED)
        qm = QueueManager()
        result = await qm.transition(session, item.id, QueueState.SCRAPING)
        assert result.state == QueueState.SCRAPING

    async def test_scraping_to_adding(self, session: AsyncSession) -> None:
        """SCRAPING → ADDING happens when a torrent is selected."""
        item = await _make_item(session, state=QueueState.SCRAPING)
        qm = QueueManager()
        result = await qm.transition(session, item.id, QueueState.ADDING)
        assert result.state == QueueState.ADDING

    async def test_scraping_to_sleeping(self, session: AsyncSession) -> None:
        """SCRAPING → SLEEPING happens when no results are found."""
        item = await _make_item(session, state=QueueState.SCRAPING)
        qm = QueueManager()
        result = await qm.transition(session, item.id, QueueState.SLEEPING)
        assert result.state == QueueState.SLEEPING

    async def test_adding_to_checking(self, session: AsyncSession) -> None:
        """ADDING → CHECKING happens after the torrent is submitted to RD."""
        item = await _make_item(session, state=QueueState.ADDING)
        qm = QueueManager()
        result = await qm.transition(session, item.id, QueueState.CHECKING)
        assert result.state == QueueState.CHECKING

    async def test_checking_to_complete(self, session: AsyncSession) -> None:
        """CHECKING → COMPLETE is the successful end state."""
        item = await _make_item(session, state=QueueState.CHECKING)
        qm = QueueManager()
        result = await qm.transition(session, item.id, QueueState.COMPLETE)
        assert result.state == QueueState.COMPLETE

    async def test_checking_to_sleeping(self, session: AsyncSession) -> None:
        """CHECKING → SLEEPING happens when the torrent is not yet ready."""
        item = await _make_item(session, state=QueueState.CHECKING)
        qm = QueueManager()
        result = await qm.transition(session, item.id, QueueState.SLEEPING)
        assert result.state == QueueState.SLEEPING

    async def test_sleeping_to_scraping(self, session: AsyncSession) -> None:
        """SLEEPING → SCRAPING is the retry path."""
        item = await _make_item(session, state=QueueState.SLEEPING, retry_count=1)
        qm = QueueManager()
        result = await qm.transition(session, item.id, QueueState.SCRAPING)
        assert result.state == QueueState.SCRAPING

    async def test_sleeping_to_dormant(self, session: AsyncSession) -> None:
        """SLEEPING → DORMANT is an explicit demotion."""
        item = await _make_item(session, state=QueueState.SLEEPING, retry_count=1)
        qm = QueueManager()
        result = await qm.transition(session, item.id, QueueState.DORMANT)
        assert result.state == QueueState.DORMANT

    async def test_dormant_to_scraping(self, session: AsyncSession) -> None:
        """DORMANT → SCRAPING is the weekly-recheck path."""
        item = await _make_item(session, state=QueueState.DORMANT, retry_count=7)
        qm = QueueManager()
        result = await qm.transition(session, item.id, QueueState.SCRAPING)
        assert result.state == QueueState.SCRAPING

    async def test_complete_to_done(self, session: AsyncSession) -> None:
        """COMPLETE → DONE marks an item fully finished."""
        item = await _make_item(session, state=QueueState.COMPLETE)
        qm = QueueManager()
        result = await qm.transition(session, item.id, QueueState.DONE)
        assert result.state == QueueState.DONE

    async def test_state_changed_at_is_updated_on_every_transition(
        self, session: AsyncSession
    ) -> None:
        """state_changed_at is updated to the current time on each transition."""
        original_ts = _utcnow() - timedelta(hours=1)
        item = await _make_item(session, state=QueueState.WANTED)
        item.state_changed_at = original_ts
        await session.flush()

        qm = QueueManager()
        result = await qm.transition(session, item.id, QueueState.SCRAPING)

        assert result.state_changed_at is not None
        # state_changed_at must be after the original timestamp
        changed_at = result.state_changed_at
        if changed_at.tzinfo is None:
            changed_at = changed_at.replace(tzinfo=timezone.utc)
        assert changed_at > original_ts

    async def test_state_matches_expected_new_state(self, session: AsyncSession) -> None:
        """The returned item's state matches the requested new state."""
        item = await _make_item(session, state=QueueState.ADDING)
        qm = QueueManager()
        result = await qm.transition(session, item.id, QueueState.CHECKING)
        assert result.state == QueueState.CHECKING


# ---------------------------------------------------------------------------
# Group 2: Invalid Transitions
# ---------------------------------------------------------------------------


class TestInvalidTransitions:
    """Any transition not in VALID_TRANSITIONS must raise InvalidTransitionError."""

    async def test_wanted_to_done_raises(self, session: AsyncSession) -> None:
        """WANTED → DONE skips many states and is not permitted."""
        item = await _make_item(session, state=QueueState.WANTED)
        qm = QueueManager()
        with pytest.raises(InvalidTransitionError):
            await qm.transition(session, item.id, QueueState.DONE)

    async def test_done_to_wanted_raises(self, session: AsyncSession) -> None:
        """DONE → WANTED is not a valid backward transition."""
        item = await _make_item(session, state=QueueState.DONE)
        qm = QueueManager()
        with pytest.raises(InvalidTransitionError):
            await qm.transition(session, item.id, QueueState.WANTED)

    async def test_sleeping_to_complete_raises(self, session: AsyncSession) -> None:
        """SLEEPING → COMPLETE skips SCRAPING/ADDING/CHECKING and is not allowed."""
        item = await _make_item(session, state=QueueState.SLEEPING, retry_count=1)
        qm = QueueManager()
        with pytest.raises(InvalidTransitionError):
            await qm.transition(session, item.id, QueueState.COMPLETE)

    async def test_unreleased_to_scraping_raises(self, session: AsyncSession) -> None:
        """UNRELEASED → SCRAPING skips WANTED and is not permitted."""
        item = await _make_item(session, state=QueueState.UNRELEASED)
        qm = QueueManager()
        with pytest.raises(InvalidTransitionError):
            await qm.transition(session, item.id, QueueState.SCRAPING)

    async def test_wanted_to_checking_raises(self, session: AsyncSession) -> None:
        """WANTED → CHECKING skips multiple states and is not allowed."""
        item = await _make_item(session, state=QueueState.WANTED)
        qm = QueueManager()
        with pytest.raises(InvalidTransitionError):
            await qm.transition(session, item.id, QueueState.CHECKING)

    async def test_adding_to_done_raises(self, session: AsyncSession) -> None:
        """ADDING → DONE is not in the state diagram."""
        item = await _make_item(session, state=QueueState.ADDING)
        qm = QueueManager()
        with pytest.raises(InvalidTransitionError):
            await qm.transition(session, item.id, QueueState.DONE)

    async def test_complete_to_wanted_raises(self, session: AsyncSession) -> None:
        """COMPLETE → WANTED is a backward transition that is not allowed."""
        item = await _make_item(session, state=QueueState.COMPLETE)
        qm = QueueManager()
        with pytest.raises(InvalidTransitionError):
            await qm.transition(session, item.id, QueueState.WANTED)

    async def test_done_to_scraping_raises(self, session: AsyncSession) -> None:
        """DONE is a terminal state; no transitions are permitted from it."""
        item = await _make_item(session, state=QueueState.DONE)
        qm = QueueManager()
        with pytest.raises(InvalidTransitionError):
            await qm.transition(session, item.id, QueueState.SCRAPING)

    async def test_dormant_to_sleeping_raises(self, session: AsyncSession) -> None:
        """DORMANT → SLEEPING is not in the state diagram."""
        item = await _make_item(session, state=QueueState.DORMANT, retry_count=7)
        qm = QueueManager()
        with pytest.raises(InvalidTransitionError):
            await qm.transition(session, item.id, QueueState.SLEEPING)

    async def test_invalid_transition_error_carries_state_info(
        self, session: AsyncSession
    ) -> None:
        """InvalidTransitionError message identifies both states."""
        item = await _make_item(session, state=QueueState.WANTED)
        qm = QueueManager()
        with pytest.raises(InvalidTransitionError) as exc_info:
            await qm.transition(session, item.id, QueueState.COMPLETE)
        err = exc_info.value
        assert hasattr(err, "from_state")
        assert hasattr(err, "to_state")
        assert err.from_state == QueueState.WANTED
        assert err.to_state == QueueState.COMPLETE

    async def test_invalid_transition_error_message_readable(
        self, session: AsyncSession
    ) -> None:
        """InvalidTransitionError.__str__ is human-readable."""
        item = await _make_item(session, state=QueueState.WANTED)
        qm = QueueManager()
        with pytest.raises(InvalidTransitionError) as exc_info:
            await qm.transition(session, item.id, QueueState.DONE)
        assert "wanted" in str(exc_info.value).lower()
        assert "done" in str(exc_info.value).lower()


# ---------------------------------------------------------------------------
# Group 3: SLEEPING Retry Logic
# ---------------------------------------------------------------------------


class TestSleepingRetryLogic:
    """Transitioning to SLEEPING must increment retry_count and schedule a retry."""

    async def test_sleeping_increments_retry_count(self, session: AsyncSession) -> None:
        """Each SLEEPING transition increments retry_count by 1."""
        item = await _make_item(session, state=QueueState.SCRAPING, retry_count=0)
        qm = QueueManager()
        result = await qm.transition(session, item.id, QueueState.SLEEPING)
        assert result.retry_count == 1

    async def test_sleeping_sets_next_retry_at(self, session: AsyncSession) -> None:
        """SLEEPING sets next_retry_at to a future datetime."""
        item = await _make_item(session, state=QueueState.SCRAPING, retry_count=0)
        qm = QueueManager()
        before = _utcnow()
        result = await qm.transition(session, item.id, QueueState.SLEEPING)
        after = _utcnow()

        assert result.next_retry_at is not None
        nra = result.next_retry_at
        if nra.tzinfo is None:
            nra = nra.replace(tzinfo=timezone.utc)
        assert nra > before
        # next_retry_at must be at least 29 minutes from now (schedule[0] = 30 min)
        assert nra > before + timedelta(minutes=29)

    async def test_sleeping_uses_first_schedule_entry_on_first_retry(
        self, session: AsyncSession
    ) -> None:
        """First SLEEPING uses schedule_minutes[0] = 30 min delay."""
        item = await _make_item(session, state=QueueState.SCRAPING, retry_count=0)
        qm = QueueManager()

        mock_retry_cfg = MagicMock()
        mock_retry_cfg.schedule_minutes = [30, 60, 120, 360, 720, 1440]
        mock_retry_cfg.max_active_retries = 7
        mock_retry_cfg.dormant_recheck_days = 7

        mock_settings = MagicMock()
        mock_settings.retry = mock_retry_cfg

        before = _utcnow()
        with patch("src.core.queue_manager.settings", mock_settings):
            result = await qm.transition(session, item.id, QueueState.SLEEPING)

        nra = result.next_retry_at
        if nra.tzinfo is None:
            nra = nra.replace(tzinfo=timezone.utc)
        # Should be ~30 minutes from before; we check >= 29 to allow for execution time
        assert nra >= before + timedelta(minutes=29)
        assert nra <= before + timedelta(minutes=31)

    async def test_sleeping_uses_second_schedule_entry_on_second_retry(
        self, session: AsyncSession
    ) -> None:
        """Second SLEEPING uses schedule_minutes[1] = 60 min delay."""
        item = await _make_item(session, state=QueueState.SCRAPING, retry_count=1)
        qm = QueueManager()

        mock_retry_cfg = MagicMock()
        mock_retry_cfg.schedule_minutes = [30, 60, 120, 360, 720, 1440]
        mock_retry_cfg.max_active_retries = 7
        mock_retry_cfg.dormant_recheck_days = 7

        mock_settings = MagicMock()
        mock_settings.retry = mock_retry_cfg

        before = _utcnow()
        with patch("src.core.queue_manager.settings", mock_settings):
            result = await qm.transition(session, item.id, QueueState.SLEEPING)

        nra = result.next_retry_at
        if nra.tzinfo is None:
            nra = nra.replace(tzinfo=timezone.utc)
        # retry_count was 1, incremented to 2, index = 2-1 = 1 => 60 minutes
        assert nra >= before + timedelta(minutes=59)
        assert nra <= before + timedelta(minutes=61)

    async def test_sleeping_escalates_delay_on_successive_retries(
        self, session: AsyncSession
    ) -> None:
        """Each subsequent SLEEPING uses a longer delay than the previous one."""
        schedule = [30, 60, 120, 360, 720, 1440]

        mock_retry_cfg = MagicMock()
        mock_retry_cfg.schedule_minutes = schedule
        mock_retry_cfg.max_active_retries = 10  # high enough to not redirect
        mock_retry_cfg.dormant_recheck_days = 7

        mock_settings = MagicMock()
        mock_settings.retry = mock_retry_cfg

        qm = QueueManager()
        delays: list[float] = []

        for i in range(len(schedule)):
            item = await _make_item(
                session,
                state=QueueState.SCRAPING,
                retry_count=i,
                imdb_id=f"tt{i:07d}",
            )
            before = _utcnow()
            with patch("src.core.queue_manager.settings", mock_settings):
                result = await qm.transition(session, item.id, QueueState.SLEEPING)
            nra = result.next_retry_at
            if nra.tzinfo is None:
                nra = nra.replace(tzinfo=timezone.utc)
            delays.append((nra - before).total_seconds())

        # Each delay must be greater than or equal to the previous delay
        for idx in range(1, len(delays)):
            assert delays[idx] >= delays[idx - 1], (
                f"Delay at retry {idx} ({delays[idx]:.0f}s) should be >= "
                f"delay at retry {idx - 1} ({delays[idx - 1]:.0f}s)"
            )

    async def test_sleeping_redirects_to_dormant_at_max_retries(
        self, session: AsyncSession
    ) -> None:
        """When retry_count reaches max_active_retries, SLEEPING auto-promotes to DORMANT."""
        mock_retry_cfg = MagicMock()
        mock_retry_cfg.schedule_minutes = [30, 60, 120, 360, 720, 1440]
        mock_retry_cfg.max_active_retries = 7
        mock_retry_cfg.dormant_recheck_days = 7

        mock_settings = MagicMock()
        mock_settings.retry = mock_retry_cfg

        # retry_count=6 means the next increment (to 7) hits max_active_retries=7
        item = await _make_item(session, state=QueueState.SCRAPING, retry_count=6)
        qm = QueueManager()

        with patch("src.core.queue_manager.settings", mock_settings):
            result = await qm.transition(session, item.id, QueueState.SLEEPING)

        assert result.state == QueueState.DORMANT

    async def test_sleeping_at_exact_max_retries_goes_dormant(
        self, session: AsyncSession
    ) -> None:
        """An item with retry_count already at max_active_retries goes straight to DORMANT."""
        mock_retry_cfg = MagicMock()
        mock_retry_cfg.schedule_minutes = [30, 60, 120]
        mock_retry_cfg.max_active_retries = 3
        mock_retry_cfg.dormant_recheck_days = 7

        mock_settings = MagicMock()
        mock_settings.retry = mock_retry_cfg

        # retry_count=2; after increment becomes 3 == max_active_retries
        item = await _make_item(session, state=QueueState.SCRAPING, retry_count=2)
        qm = QueueManager()

        with patch("src.core.queue_manager.settings", mock_settings):
            result = await qm.transition(session, item.id, QueueState.SLEEPING)

        assert result.state == QueueState.DORMANT

    async def test_sleeping_below_max_retries_stays_sleeping(
        self, session: AsyncSession
    ) -> None:
        """An item below max_active_retries stays SLEEPING (not redirected to DORMANT)."""
        mock_retry_cfg = MagicMock()
        mock_retry_cfg.schedule_minutes = [30, 60, 120, 360, 720, 1440]
        mock_retry_cfg.max_active_retries = 7
        mock_retry_cfg.dormant_recheck_days = 7

        mock_settings = MagicMock()
        mock_settings.retry = mock_retry_cfg

        item = await _make_item(session, state=QueueState.SCRAPING, retry_count=2)
        qm = QueueManager()

        with patch("src.core.queue_manager.settings", mock_settings):
            result = await qm.transition(session, item.id, QueueState.SLEEPING)

        assert result.state == QueueState.SLEEPING


# ---------------------------------------------------------------------------
# Group 4: DORMANT Logic
# ---------------------------------------------------------------------------


class TestDormantLogic:
    """DORMANT state sets next_retry_at and SCRAPING from DORMANT clears it."""

    async def test_dormant_sets_next_retry_at_based_on_config(
        self, session: AsyncSession
    ) -> None:
        """Explicit SLEEPING → DORMANT sets next_retry_at = now + dormant_recheck_days."""
        mock_retry_cfg = MagicMock()
        mock_retry_cfg.schedule_minutes = [30, 60, 120, 360, 720, 1440]
        mock_retry_cfg.max_active_retries = 100  # prevent auto-redirect
        mock_retry_cfg.dormant_recheck_days = 7

        mock_settings = MagicMock()
        mock_settings.retry = mock_retry_cfg

        item = await _make_item(session, state=QueueState.SLEEPING, retry_count=1)
        qm = QueueManager()
        before = _utcnow()

        with patch("src.core.queue_manager.settings", mock_settings):
            result = await qm.transition(session, item.id, QueueState.DORMANT)

        nra = result.next_retry_at
        if nra.tzinfo is None:
            nra = nra.replace(tzinfo=timezone.utc)
        assert nra >= before + timedelta(days=7)
        assert nra <= before + timedelta(days=7, minutes=1)

    async def test_dormant_next_retry_at_uses_dormant_recheck_days_from_config(
        self, session: AsyncSession
    ) -> None:
        """dormant_recheck_days is read from config, not hardcoded."""
        mock_retry_cfg = MagicMock()
        mock_retry_cfg.schedule_minutes = [30]
        mock_retry_cfg.max_active_retries = 100
        mock_retry_cfg.dormant_recheck_days = 14  # 14 days instead of 7

        mock_settings = MagicMock()
        mock_settings.retry = mock_retry_cfg

        item = await _make_item(session, state=QueueState.SLEEPING, retry_count=1)
        qm = QueueManager()
        before = _utcnow()

        with patch("src.core.queue_manager.settings", mock_settings):
            result = await qm.transition(session, item.id, QueueState.DORMANT)

        nra = result.next_retry_at
        if nra.tzinfo is None:
            nra = nra.replace(tzinfo=timezone.utc)
        assert nra >= before + timedelta(days=14)

    async def test_scraping_from_dormant_clears_next_retry_at(
        self, session: AsyncSession
    ) -> None:
        """DORMANT → SCRAPING clears next_retry_at to None."""
        past = _utcnow() - timedelta(days=1)
        item = await _make_item(
            session,
            state=QueueState.DORMANT,
            retry_count=7,
            next_retry_at=past,
        )
        qm = QueueManager()
        result = await qm.transition(session, item.id, QueueState.SCRAPING)
        assert result.next_retry_at is None

    async def test_sleeping_auto_dormant_sets_next_retry_at(
        self, session: AsyncSession
    ) -> None:
        """When SLEEPING is auto-redirected to DORMANT, next_retry_at is still set."""
        mock_retry_cfg = MagicMock()
        mock_retry_cfg.schedule_minutes = [30, 60, 120]
        mock_retry_cfg.max_active_retries = 3
        mock_retry_cfg.dormant_recheck_days = 7

        mock_settings = MagicMock()
        mock_settings.retry = mock_retry_cfg

        item = await _make_item(session, state=QueueState.SCRAPING, retry_count=2)
        qm = QueueManager()
        before = _utcnow()

        with patch("src.core.queue_manager.settings", mock_settings):
            result = await qm.transition(session, item.id, QueueState.SLEEPING)

        assert result.state == QueueState.DORMANT
        assert result.next_retry_at is not None
        nra = result.next_retry_at
        if nra.tzinfo is None:
            nra = nra.replace(tzinfo=timezone.utc)
        assert nra >= before + timedelta(days=7)


# ---------------------------------------------------------------------------
# Group 5: Force Transition
# ---------------------------------------------------------------------------


class TestForceTransition:
    """force_transition allows any-to-any state change and resets retry fields."""

    async def test_force_transition_from_done_to_wanted(
        self, session: AsyncSession
    ) -> None:
        """Force transition allows backward jump DONE → WANTED."""
        item = await _make_item(session, state=QueueState.DONE)
        qm = QueueManager()
        result = await qm.force_transition(session, item.id, QueueState.WANTED)
        assert result.state == QueueState.WANTED

    async def test_force_transition_from_sleeping_to_complete(
        self, session: AsyncSession
    ) -> None:
        """Force transition allows SLEEPING → COMPLETE (normally invalid)."""
        item = await _make_item(session, state=QueueState.SLEEPING, retry_count=3)
        qm = QueueManager()
        result = await qm.force_transition(session, item.id, QueueState.COMPLETE)
        assert result.state == QueueState.COMPLETE

    async def test_force_transition_from_unreleased_to_scraping(
        self, session: AsyncSession
    ) -> None:
        """Force transition allows UNRELEASED → SCRAPING (skips WANTED)."""
        item = await _make_item(session, state=QueueState.UNRELEASED)
        qm = QueueManager()
        result = await qm.force_transition(session, item.id, QueueState.SCRAPING)
        assert result.state == QueueState.SCRAPING

    async def test_force_transition_resets_retry_count(
        self, session: AsyncSession
    ) -> None:
        """force_transition resets retry_count to 0 regardless of prior value."""
        item = await _make_item(session, state=QueueState.SLEEPING, retry_count=5)
        qm = QueueManager()
        result = await qm.force_transition(session, item.id, QueueState.WANTED)
        assert result.retry_count == 0

    async def test_force_transition_clears_next_retry_at(
        self, session: AsyncSession
    ) -> None:
        """force_transition sets next_retry_at to None."""
        future = _utcnow() + timedelta(hours=6)
        item = await _make_item(
            session, state=QueueState.SLEEPING, retry_count=2, next_retry_at=future
        )
        qm = QueueManager()
        result = await qm.force_transition(session, item.id, QueueState.WANTED)
        assert result.next_retry_at is None

    async def test_force_transition_updates_state_changed_at(
        self, session: AsyncSession
    ) -> None:
        """force_transition updates state_changed_at to the current time."""
        old_ts = _utcnow() - timedelta(hours=2)
        item = await _make_item(session, state=QueueState.WANTED)
        item.state_changed_at = old_ts
        await session.flush()

        qm = QueueManager()
        result = await qm.force_transition(session, item.id, QueueState.SCRAPING)

        changed_at = result.state_changed_at
        if changed_at.tzinfo is None:
            changed_at = changed_at.replace(tzinfo=timezone.utc)
        assert changed_at > old_ts

    async def test_force_transition_logs_warning(
        self, session: AsyncSession, caplog: pytest.LogCaptureFixture
    ) -> None:
        """force_transition emits a WARNING-level log entry."""
        item = await _make_item(session, state=QueueState.DONE)
        qm = QueueManager()

        with caplog.at_level(logging.WARNING, logger="src.core.queue_manager"):
            await qm.force_transition(session, item.id, QueueState.WANTED)

        assert any(
            "force" in record.message.lower() or "override" in record.message.lower()
            or "manual" in record.message.lower()
            for record in caplog.records
            if record.levelno >= logging.WARNING
        ), "Expected a WARNING log mentioning force/override/manual"

    async def test_force_transition_to_same_state(self, session: AsyncSession) -> None:
        """Force transition to the current state is accepted without error."""
        item = await _make_item(session, state=QueueState.WANTED)
        qm = QueueManager()
        result = await qm.force_transition(session, item.id, QueueState.WANTED)
        assert result.state == QueueState.WANTED


# ---------------------------------------------------------------------------
# Group 6: process_unreleased
# ---------------------------------------------------------------------------


class TestProcessUnreleased:
    """process_unreleased promotes items whose air_date is in the past."""

    async def test_past_air_date_item_transitions_to_wanted(
        self, session: AsyncSession
    ) -> None:
        """UNRELEASED item with past air_date is promoted to WANTED."""
        item = await _make_item(
            session,
            state=QueueState.UNRELEASED,
            air_date=date(2020, 1, 1),
        )
        qm = QueueManager()
        advanced = await qm.process_unreleased(session)
        assert item.id in advanced
        assert item.state == QueueState.WANTED

    async def test_future_air_date_item_stays_unreleased(
        self, session: AsyncSession
    ) -> None:
        """UNRELEASED item with a future air_date is left unchanged."""
        future_date = date(2099, 12, 31)
        item = await _make_item(
            session,
            state=QueueState.UNRELEASED,
            air_date=future_date,
        )
        qm = QueueManager()
        advanced = await qm.process_unreleased(session)
        assert item.id not in advanced
        assert item.state == QueueState.UNRELEASED

    async def test_no_air_date_item_stays_unreleased(
        self, session: AsyncSession
    ) -> None:
        """UNRELEASED item without an air_date is left unchanged."""
        item = await _make_item(
            session,
            state=QueueState.UNRELEASED,
            air_date=None,
        )
        qm = QueueManager()
        advanced = await qm.process_unreleased(session)
        assert item.id not in advanced
        assert item.state == QueueState.UNRELEASED

    async def test_today_air_date_item_transitions_to_wanted(
        self, session: AsyncSession
    ) -> None:
        """UNRELEASED item with today's air_date is promoted (air_date <= today)."""
        today = date.today()
        item = await _make_item(
            session,
            state=QueueState.UNRELEASED,
            air_date=today,
        )
        qm = QueueManager()
        advanced = await qm.process_unreleased(session)
        assert item.id in advanced
        assert item.state == QueueState.WANTED

    async def test_multiple_past_items_all_promoted(self, session: AsyncSession) -> None:
        """All UNRELEASED items with past air_dates are promoted in one call."""
        items = [
            await _make_item(
                session,
                state=QueueState.UNRELEASED,
                air_date=date(2020, 1, 1),
                imdb_id=f"tt{i:07d}",
            )
            for i in range(3)
        ]
        qm = QueueManager()
        advanced = await qm.process_unreleased(session)
        for item in items:
            assert item.id in advanced
            assert item.state == QueueState.WANTED

    async def test_mixed_items_only_past_promoted(self, session: AsyncSession) -> None:
        """Only items with past air_dates are promoted; future/none are skipped."""
        past_item = await _make_item(
            session,
            state=QueueState.UNRELEASED,
            air_date=date(2020, 6, 15),
            imdb_id="tt0000010",
        )
        future_item = await _make_item(
            session,
            state=QueueState.UNRELEASED,
            air_date=date(2099, 6, 15),
            imdb_id="tt0000011",
        )
        no_date_item = await _make_item(
            session,
            state=QueueState.UNRELEASED,
            air_date=None,
            imdb_id="tt0000012",
        )
        qm = QueueManager()
        advanced = await qm.process_unreleased(session)

        assert past_item.id in advanced
        assert future_item.id not in advanced
        assert no_date_item.id not in advanced
        assert past_item.state == QueueState.WANTED
        assert future_item.state == QueueState.UNRELEASED
        assert no_date_item.state == QueueState.UNRELEASED

    async def test_wanted_item_not_touched_by_process_unreleased(
        self, session: AsyncSession
    ) -> None:
        """WANTED items are not included in unreleased processing."""
        wanted = await _make_item(
            session,
            state=QueueState.WANTED,
            air_date=date(2020, 1, 1),
        )
        qm = QueueManager()
        advanced = await qm.process_unreleased(session)
        assert wanted.id not in advanced

    async def test_process_unreleased_returns_list_of_ids(
        self, session: AsyncSession
    ) -> None:
        """process_unreleased returns a list of integer item IDs."""
        await _make_item(
            session,
            state=QueueState.UNRELEASED,
            air_date=date(2020, 1, 1),
        )
        qm = QueueManager()
        result = await qm.process_unreleased(session)
        assert isinstance(result, list)
        assert all(isinstance(i, int) for i in result)


# ---------------------------------------------------------------------------
# Group 7: get_ready_for_retry
# ---------------------------------------------------------------------------


class TestGetReadyForRetry:
    """get_ready_for_retry finds items whose next_retry_at has elapsed."""

    async def test_sleeping_with_past_next_retry_at_is_returned(
        self, session: AsyncSession
    ) -> None:
        """A SLEEPING item whose next_retry_at is in the past should be returned."""
        past = _utcnow() - timedelta(minutes=5)
        item = await _make_item(
            session, state=QueueState.SLEEPING, retry_count=1, next_retry_at=past
        )
        qm = QueueManager()
        ready = await qm.get_ready_for_retry(session)
        assert any(r.id == item.id for r in ready)

    async def test_sleeping_with_future_next_retry_at_is_not_returned(
        self, session: AsyncSession
    ) -> None:
        """A SLEEPING item with a future next_retry_at is not yet due."""
        future = _utcnow() + timedelta(hours=2)
        item = await _make_item(
            session, state=QueueState.SLEEPING, retry_count=1, next_retry_at=future
        )
        qm = QueueManager()
        ready = await qm.get_ready_for_retry(session)
        assert not any(r.id == item.id for r in ready)

    async def test_dormant_with_past_next_retry_at_is_returned(
        self, session: AsyncSession
    ) -> None:
        """A DORMANT item whose next_retry_at is in the past should be returned."""
        past = _utcnow() - timedelta(days=1)
        item = await _make_item(
            session, state=QueueState.DORMANT, retry_count=7, next_retry_at=past
        )
        qm = QueueManager()
        ready = await qm.get_ready_for_retry(session)
        assert any(r.id == item.id for r in ready)

    async def test_dormant_with_future_next_retry_at_is_not_returned(
        self, session: AsyncSession
    ) -> None:
        """A DORMANT item whose retry is still in the future is not yet due."""
        future = _utcnow() + timedelta(days=3)
        item = await _make_item(
            session, state=QueueState.DORMANT, retry_count=7, next_retry_at=future
        )
        qm = QueueManager()
        ready = await qm.get_ready_for_retry(session)
        assert not any(r.id == item.id for r in ready)

    async def test_wanted_item_not_included_in_retry_results(
        self, session: AsyncSession
    ) -> None:
        """WANTED items are not returned by get_ready_for_retry."""
        past = _utcnow() - timedelta(minutes=5)
        item = await _make_item(
            session, state=QueueState.WANTED, next_retry_at=past
        )
        qm = QueueManager()
        ready = await qm.get_ready_for_retry(session)
        assert not any(r.id == item.id for r in ready)

    async def test_both_sleeping_and_dormant_returned_together(
        self, session: AsyncSession
    ) -> None:
        """Both SLEEPING and DORMANT items past their retry time are returned."""
        past = _utcnow() - timedelta(hours=1)
        sleeping = await _make_item(
            session,
            state=QueueState.SLEEPING,
            retry_count=1,
            next_retry_at=past,
            imdb_id="tt0000020",
        )
        dormant = await _make_item(
            session,
            state=QueueState.DORMANT,
            retry_count=7,
            next_retry_at=past,
            imdb_id="tt0000021",
        )
        qm = QueueManager()
        ready = await qm.get_ready_for_retry(session)
        ids = {r.id for r in ready}
        assert sleeping.id in ids
        assert dormant.id in ids

    async def test_no_items_due_returns_empty_list(self, session: AsyncSession) -> None:
        """When no items are due, get_ready_for_retry returns an empty list."""
        future = _utcnow() + timedelta(hours=10)
        await _make_item(
            session, state=QueueState.SLEEPING, retry_count=1, next_retry_at=future
        )
        qm = QueueManager()
        ready = await qm.get_ready_for_retry(session)
        assert ready == []

    async def test_returns_list_of_media_items(self, session: AsyncSession) -> None:
        """get_ready_for_retry returns MediaItem instances."""
        past = _utcnow() - timedelta(minutes=1)
        await _make_item(
            session, state=QueueState.SLEEPING, retry_count=1, next_retry_at=past
        )
        qm = QueueManager()
        ready = await qm.get_ready_for_retry(session)
        assert isinstance(ready, list)
        assert all(isinstance(r, MediaItem) for r in ready)


# ---------------------------------------------------------------------------
# Group 8: process_queue
# ---------------------------------------------------------------------------


class TestProcessQueue:
    """process_queue combines process_unreleased and get_ready_for_retry."""

    async def test_process_queue_returns_summary_dict(
        self, session: AsyncSession
    ) -> None:
        """process_queue returns a dict with unreleased_advanced and retries_triggered."""
        qm = QueueManager()
        summary = await qm.process_queue(session)
        assert "unreleased_advanced" in summary
        assert "retries_triggered" in summary

    async def test_process_queue_promotes_unreleased_items(
        self, session: AsyncSession
    ) -> None:
        """process_queue advances UNRELEASED items with past air_dates."""
        item = await _make_item(
            session,
            state=QueueState.UNRELEASED,
            air_date=date(2020, 1, 1),
        )
        qm = QueueManager()
        summary = await qm.process_queue(session)
        assert summary["unreleased_advanced"] >= 1
        assert item.state == QueueState.WANTED

    async def test_process_queue_triggers_retries_for_overdue_items(
        self, session: AsyncSession
    ) -> None:
        """process_queue transitions overdue SLEEPING items to SCRAPING."""
        past = _utcnow() - timedelta(hours=1)
        item = await _make_item(
            session,
            state=QueueState.SLEEPING,
            retry_count=1,
            next_retry_at=past,
        )
        qm = QueueManager()
        summary = await qm.process_queue(session)
        assert summary["retries_triggered"] >= 1
        assert item.state == QueueState.SCRAPING

    async def test_process_queue_zero_counts_when_nothing_to_do(
        self, session: AsyncSession
    ) -> None:
        """process_queue returns zeros when no items are ready."""
        qm = QueueManager()
        summary = await qm.process_queue(session)
        assert summary["unreleased_advanced"] == 0
        assert summary["retries_triggered"] == 0

    async def test_process_queue_handles_both_unreleased_and_retries(
        self, session: AsyncSession
    ) -> None:
        """process_queue runs both promotions and retries in a single call."""
        unreleased = await _make_item(
            session,
            state=QueueState.UNRELEASED,
            air_date=date(2020, 6, 1),
            imdb_id="tt0000030",
        )
        past = _utcnow() - timedelta(hours=2)
        sleeping = await _make_item(
            session,
            state=QueueState.SLEEPING,
            retry_count=1,
            next_retry_at=past,
            imdb_id="tt0000031",
        )
        qm = QueueManager()
        summary = await qm.process_queue(session)
        assert summary["unreleased_advanced"] >= 1
        assert summary["retries_triggered"] >= 1
        assert unreleased.state == QueueState.WANTED
        assert sleeping.state == QueueState.SCRAPING

    async def test_process_queue_summary_values_are_ints(
        self, session: AsyncSession
    ) -> None:
        """Summary dict values are integers."""
        qm = QueueManager()
        summary = await qm.process_queue(session)
        assert isinstance(summary["unreleased_advanced"], int)
        assert isinstance(summary["retries_triggered"], int)


# ---------------------------------------------------------------------------
# Group 9: Edge Cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Miscellaneous edge cases and error paths."""

    async def test_item_not_found_raises_on_transition(
        self, session: AsyncSession
    ) -> None:
        """Transitioning a non-existent item ID raises ItemNotFoundError."""
        qm = QueueManager()
        with pytest.raises(ItemNotFoundError):
            await qm.transition(session, 999999, QueueState.SCRAPING)

    async def test_item_not_found_error_carries_item_id(
        self, session: AsyncSession
    ) -> None:
        """ItemNotFoundError stores the missing item_id."""
        qm = QueueManager()
        with pytest.raises(ItemNotFoundError) as exc_info:
            await qm.transition(session, 42, QueueState.SCRAPING)
        assert exc_info.value.item_id == 42

    async def test_item_not_found_on_force_transition(
        self, session: AsyncSession
    ) -> None:
        """force_transition on a non-existent item also raises ItemNotFoundError."""
        qm = QueueManager()
        with pytest.raises(ItemNotFoundError):
            await qm.force_transition(session, 999999, QueueState.WANTED)

    async def test_transition_to_wanted_resets_retry_count(
        self, session: AsyncSession
    ) -> None:
        """Transitioning to WANTED resets retry_count to 0."""
        item = await _make_item(session, state=QueueState.UNRELEASED, retry_count=3)
        qm = QueueManager()
        result = await qm.transition(session, item.id, QueueState.WANTED)
        assert result.retry_count == 0

    async def test_transition_to_wanted_clears_next_retry_at(
        self, session: AsyncSession
    ) -> None:
        """Transitioning to WANTED sets next_retry_at to None."""
        future = _utcnow() + timedelta(hours=1)
        item = await _make_item(
            session, state=QueueState.UNRELEASED, next_retry_at=future
        )
        qm = QueueManager()
        result = await qm.transition(session, item.id, QueueState.WANTED)
        assert result.next_retry_at is None

    async def test_calculate_next_retry_first_index(self) -> None:
        """calculate_next_retry(0) uses schedule[0] = 30 minutes."""
        mock_retry_cfg = MagicMock()
        mock_retry_cfg.schedule_minutes = [30, 60, 120, 360, 720, 1440]

        mock_settings = MagicMock()
        mock_settings.retry = mock_retry_cfg

        before = _utcnow()
        with patch("src.core.queue_manager.settings", mock_settings):
            result = QueueManager.calculate_next_retry(0)

        if result.tzinfo is None:
            result = result.replace(tzinfo=timezone.utc)
        assert result >= before + timedelta(minutes=29)
        assert result <= before + timedelta(minutes=31)

    async def test_calculate_next_retry_clamps_beyond_schedule(self) -> None:
        """calculate_next_retry clamps to the last schedule entry for high counts."""
        mock_retry_cfg = MagicMock()
        mock_retry_cfg.schedule_minutes = [30, 60, 120]  # only 3 entries, last = 120

        mock_settings = MagicMock()
        mock_settings.retry = mock_retry_cfg

        before = _utcnow()
        # retry_count=100 is way beyond the 3-entry schedule
        with patch("src.core.queue_manager.settings", mock_settings):
            result = QueueManager.calculate_next_retry(100)

        if result.tzinfo is None:
            result = result.replace(tzinfo=timezone.utc)
        # Should use 120 minutes (the last entry), not crash or use a wrong index
        assert result >= before + timedelta(minutes=119)
        assert result <= before + timedelta(minutes=121)

    async def test_calculate_next_retry_second_entry(self) -> None:
        """calculate_next_retry(1) uses schedule[1] = 60 minutes."""
        mock_retry_cfg = MagicMock()
        mock_retry_cfg.schedule_minutes = [30, 60, 120, 360, 720, 1440]

        mock_settings = MagicMock()
        mock_settings.retry = mock_retry_cfg

        before = _utcnow()
        with patch("src.core.queue_manager.settings", mock_settings):
            result = QueueManager.calculate_next_retry(1)

        if result.tzinfo is None:
            result = result.replace(tzinfo=timezone.utc)
        assert result >= before + timedelta(minutes=59)
        assert result <= before + timedelta(minutes=61)

    async def test_calculate_next_retry_returns_timezone_aware_datetime(self) -> None:
        """calculate_next_retry always returns a timezone-aware datetime."""
        result = QueueManager.calculate_next_retry(0)
        assert result.tzinfo is not None

    async def test_unicode_title_handled_without_crash(
        self, session: AsyncSession
    ) -> None:
        """Items with unicode titles transition correctly."""
        item = await _make_item(
            session,
            state=QueueState.WANTED,
            title="\u30b6\u30fb\u30dc\u30fc\u30a4\u30ba \u30e2\u30fc\u30d3\u30fc",
        )
        qm = QueueManager()
        result = await qm.transition(session, item.id, QueueState.SCRAPING)
        assert result.state == QueueState.SCRAPING

    async def test_scraping_from_sleeping_clears_next_retry_at(
        self, session: AsyncSession
    ) -> None:
        """SLEEPING → SCRAPING clears next_retry_at per the state machine rules."""
        future = _utcnow() + timedelta(hours=1)
        item = await _make_item(
            session, state=QueueState.SLEEPING, retry_count=1, next_retry_at=future
        )
        qm = QueueManager()
        result = await qm.transition(session, item.id, QueueState.SCRAPING)
        assert result.next_retry_at is None

    async def test_valid_transitions_dict_is_complete(self) -> None:
        """VALID_TRANSITIONS covers all QueueState members as keys."""
        for state in QueueState:
            assert state in VALID_TRANSITIONS, (
                f"QueueState.{state.name} is missing from VALID_TRANSITIONS"
            )

    async def test_valid_transitions_values_are_subsets_of_queue_states(self) -> None:
        """Every target state in VALID_TRANSITIONS is a valid QueueState."""
        all_states = set(QueueState)
        for from_state, targets in VALID_TRANSITIONS.items():
            for target in targets:
                assert target in all_states, (
                    f"Unknown target state {target!r} in VALID_TRANSITIONS[{from_state!r}]"
                )

    async def test_done_has_no_valid_transitions(self) -> None:
        """DONE is a terminal state with an empty target set."""
        assert VALID_TRANSITIONS[QueueState.DONE] == set()

    async def test_process_queue_dormant_item_moves_to_scraping(
        self, session: AsyncSession
    ) -> None:
        """process_queue transitions overdue DORMANT items to SCRAPING."""
        past = _utcnow() - timedelta(days=8)
        item = await _make_item(
            session,
            state=QueueState.DORMANT,
            retry_count=7,
            next_retry_at=past,
        )
        qm = QueueManager()
        summary = await qm.process_queue(session)
        assert summary["retries_triggered"] >= 1
        assert item.state == QueueState.SCRAPING

    async def test_item_not_found_error_message_contains_id(self) -> None:
        """ItemNotFoundError message is informative and includes the item ID."""
        err = ItemNotFoundError(12345)
        assert "12345" in str(err)

    async def test_invalid_transition_error_is_exception_subclass(self) -> None:
        """InvalidTransitionError is a proper Exception subclass."""
        err = InvalidTransitionError(QueueState.WANTED, QueueState.DONE)
        assert isinstance(err, Exception)

    async def test_item_not_found_error_is_exception_subclass(self) -> None:
        """ItemNotFoundError is a proper Exception subclass."""
        err = ItemNotFoundError(1)
        assert isinstance(err, Exception)


# ---------------------------------------------------------------------------
# Event bus integration tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_transition_publishes_event(session: AsyncSession) -> None:
    """transition() publishes a QueueEvent to the event bus."""
    from src.core.event_bus import EventBus

    bus = EventBus()
    client_id, queue = bus.subscribe()

    item = await _make_item(session, state=QueueState.WANTED)
    qm = QueueManager()

    with patch("src.core.event_bus.event_bus", bus):
        await qm.transition(session, item.id, QueueState.SCRAPING)

    event = queue.get_nowait()
    assert event.item_id == item.id
    assert event.old_state == "wanted"
    assert event.new_state == "scraping"
    bus.unsubscribe(client_id)


@pytest.mark.asyncio
async def test_force_transition_publishes_event(session: AsyncSession) -> None:
    """force_transition() publishes a QueueEvent to the event bus."""
    from src.core.event_bus import EventBus

    bus = EventBus()
    client_id, queue = bus.subscribe()

    item = await _make_item(session, state=QueueState.SLEEPING)
    qm = QueueManager()

    with patch("src.core.event_bus.event_bus", bus):
        await qm.force_transition(session, item.id, QueueState.WANTED)

    event = queue.get_nowait()
    assert event.item_id == item.id
    assert event.old_state == "sleeping"
    assert event.new_state == "wanted"
    bus.unsubscribe(client_id)
