"""Queue state machine for vibeDebrid.

Manages all MediaItem state transitions, retry scheduling, and periodic
queue processing. Every transition is validated, logged, and flushed within
the caller-managed transaction.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.models.media_item import MediaItem, QueueState

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Valid transitions map (matches SPEC.md Section 3.1 state diagram exactly)
# ---------------------------------------------------------------------------

VALID_TRANSITIONS: dict[QueueState, set[QueueState]] = {
    QueueState.UNRELEASED: {QueueState.WANTED},
    QueueState.WANTED: {QueueState.SCRAPING},
    QueueState.SCRAPING: {QueueState.ADDING, QueueState.SLEEPING},
    QueueState.ADDING: {QueueState.CHECKING},
    QueueState.CHECKING: {QueueState.COMPLETE, QueueState.SLEEPING},
    QueueState.SLEEPING: {QueueState.SCRAPING, QueueState.DORMANT},
    QueueState.DORMANT: {QueueState.SCRAPING},
    QueueState.COMPLETE: {QueueState.DONE},
    QueueState.DONE: set(),
}

# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------


class InvalidTransitionError(Exception):
    """Raised when a requested state transition is not permitted.

    Args:
        from_state: The current state of the item.
        to_state: The target state that was rejected.
    """

    def __init__(self, from_state: QueueState, to_state: QueueState) -> None:
        self.from_state = from_state
        self.to_state = to_state
        super().__init__(
            f"Transition from {from_state.value!r} to {to_state.value!r} is not allowed. "
            f"Valid targets: {[s.value for s in VALID_TRANSITIONS.get(from_state, set())]}"
        )


class ItemNotFoundError(Exception):
    """Raised when a MediaItem with the given ID does not exist.

    Args:
        item_id: The primary key that was not found.
    """

    def __init__(self, item_id: int) -> None:
        self.item_id = item_id
        super().__init__(f"MediaItem with id={item_id} not found")


# ---------------------------------------------------------------------------
# QueueManager
# ---------------------------------------------------------------------------


class QueueManager:
    """Stateless service that manages queue state transitions and scheduling.

    All public methods accept an ``AsyncSession`` and delegate commit
    responsibility to the caller. Methods only call ``session.flush()``
    to push pending changes to the database connection without committing.
    """

    # ------------------------------------------------------------------
    # Static helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _utcnow() -> datetime:
        """Return the current UTC time as a timezone-aware datetime."""
        return datetime.now(UTC)

    @staticmethod
    def calculate_next_retry(retry_count: int) -> datetime:
        """Calculate the absolute datetime for the next retry attempt.

        Looks up the retry delay in ``settings.retry.schedule_minutes`` by
        index. If ``retry_count`` exceeds the schedule length, the last value
        in the schedule is used (daily retry).

        Args:
            retry_count: The item's current retry count (0-based index into
                the retry schedule).

        Returns:
            A timezone-aware UTC datetime when the item should be retried next.
        """
        schedule = settings.retry.schedule_minutes
        index = min(retry_count, len(schedule) - 1)
        delay_minutes = schedule[index]
        return datetime.now(UTC) + timedelta(minutes=delay_minutes)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _get_item(self, session: AsyncSession, item_id: int) -> MediaItem:
        """Fetch a MediaItem by primary key or raise ItemNotFoundError.

        Args:
            session: Active async SQLAlchemy session.
            item_id: Primary key of the MediaItem to load.

        Returns:
            The loaded MediaItem instance.

        Raises:
            ItemNotFoundError: If no item with that ID exists.
        """
        result = await session.execute(select(MediaItem).where(MediaItem.id == item_id))
        item = result.scalar_one_or_none()
        if item is None:
            raise ItemNotFoundError(item_id)
        return item

    def _apply_transition_side_effects(
        self,
        item: MediaItem,
        from_state: QueueState,
        to_state: QueueState,
    ) -> None:
        """Apply field mutations that accompany a state transition.

        This method encodes the business rules for each destination state:
        - SLEEPING: increment retry_count, schedule next retry, redirect to
          DORMANT if max retries exceeded.
        - DORMANT: schedule next retry using dormant_recheck_days.
        - SCRAPING (from SLEEPING or DORMANT): clear next_retry_at.
        - WANTED: reset retry_count and clear next_retry_at.

        Note: When a SLEEPING transition is redirected to DORMANT, this method
        sets ``item.state = DORMANT`` directly. The caller must account for
        this when logging the effective destination.

        Args:
            item: The MediaItem whose fields will be mutated in place.
            from_state: The state the item is transitioning out of.
            to_state: The initially requested destination state.
        """
        now = self._utcnow()

        if to_state == QueueState.SLEEPING:
            item.retry_count += 1
            if item.retry_count >= settings.retry.max_active_retries:
                # Redirect to DORMANT — max active retries exhausted
                logger.info(
                    "item id=%d retry_count=%d >= max_active_retries=%d, redirecting SLEEPING -> DORMANT",
                    item.id,
                    item.retry_count,
                    settings.retry.max_active_retries,
                )
                item.state = QueueState.DORMANT
                item.next_retry_at = now + timedelta(days=settings.retry.dormant_recheck_days)
            else:
                item.state = QueueState.SLEEPING
                item.next_retry_at = self.calculate_next_retry(item.retry_count - 1)

        elif to_state == QueueState.DORMANT:
            item.state = QueueState.DORMANT
            item.next_retry_at = now + timedelta(days=settings.retry.dormant_recheck_days)

        elif to_state == QueueState.SCRAPING and from_state in (
            QueueState.SLEEPING,
            QueueState.DORMANT,
        ):
            item.state = QueueState.SCRAPING
            item.next_retry_at = None

        elif to_state == QueueState.WANTED:
            item.state = QueueState.WANTED
            item.retry_count = 0
            item.next_retry_at = None

        else:
            item.state = to_state

        item.state_changed_at = now

    # ------------------------------------------------------------------
    # Core transition methods
    # ------------------------------------------------------------------

    async def transition(
        self,
        session: AsyncSession,
        item_id: int,
        new_state: QueueState,
    ) -> MediaItem:
        """Transition a MediaItem to a new state, enforcing the state diagram.

        Validates that the transition is permitted, applies all business-rule
        side effects (retry counter, next_retry_at, dormant redirect), and
        flushes the change within the caller's transaction.

        Args:
            session: Active async SQLAlchemy session. The caller owns the
                transaction and must call ``await session.commit()`` afterward.
            item_id: Primary key of the MediaItem to transition.
            new_state: The target QueueState.

        Returns:
            The updated MediaItem after the transition.

        Raises:
            ItemNotFoundError: If no item with that ID exists.
            InvalidTransitionError: If the transition is not permitted by the
                state diagram.
        """
        item = await self._get_item(session, item_id)
        from_state = item.state

        # Validate transition is legal
        allowed = VALID_TRANSITIONS.get(from_state, set())
        if new_state not in allowed:
            raise InvalidTransitionError(from_state, new_state)

        self._apply_transition_side_effects(item, from_state, new_state)
        effective_state = item.state  # may differ from new_state (e.g. SLEEPING -> DORMANT redirect)

        await session.flush()

        logger.info(
            "Transition: item id=%d %r -> %r (title=%r)",
            item_id,
            from_state.value,
            effective_state.value,
            item.title,
        )
        return item

    async def force_transition(
        self,
        session: AsyncSession,
        item_id: int,
        new_state: QueueState,
    ) -> MediaItem:
        """Manually override an item's state, bypassing transition validation.

        Intended for UI-driven manual overrides. Resets ``retry_count`` to 0
        and clears ``next_retry_at`` regardless of the target state.

        Args:
            session: Active async SQLAlchemy session.
            item_id: Primary key of the MediaItem to override.
            new_state: The target QueueState (any state is accepted).

        Returns:
            The updated MediaItem after the forced transition.

        Raises:
            ItemNotFoundError: If no item with that ID exists.
        """
        item = await self._get_item(session, item_id)
        from_state = item.state

        logger.warning(
            "MANUAL OVERRIDE: item id=%d forcing state %r -> %r (title=%r)",
            item_id,
            from_state.value,
            new_state.value,
            item.title,
        )

        item.state = new_state
        item.state_changed_at = self._utcnow()
        item.retry_count = 0
        item.next_retry_at = None

        await session.flush()

        logger.info(
            "Force transition complete: item id=%d now in %r",
            item_id,
            new_state.value,
        )
        return item

    # ------------------------------------------------------------------
    # Queue processing methods
    # ------------------------------------------------------------------

    async def process_unreleased(self, session: AsyncSession) -> list[int]:
        """Advance UNRELEASED items whose air date has been reached.

        Queries all items in UNRELEASED state and transitions each one to
        WANTED if its ``air_date`` is not None and is on or before today.

        Args:
            session: Active async SQLAlchemy session.

        Returns:
            List of item IDs that were transitioned to WANTED.
        """
        result = await session.execute(
            select(MediaItem).where(MediaItem.state == QueueState.UNRELEASED)
        )
        unreleased_items = result.scalars().all()

        today = date.today()
        advanced_ids: list[int] = []

        for item in unreleased_items:
            if item.air_date is not None and item.air_date <= today:
                logger.info(
                    "process_unreleased: item id=%d air_date=%s reached, transitioning to WANTED",
                    item.id,
                    item.air_date,
                )
                self._apply_transition_side_effects(item, QueueState.UNRELEASED, QueueState.WANTED)
                await session.flush()
                advanced_ids.append(item.id)

        if advanced_ids:
            logger.info("process_unreleased: advanced %d items to WANTED", len(advanced_ids))

        return advanced_ids

    async def get_ready_for_retry(self, session: AsyncSession) -> list[MediaItem]:
        """Find SLEEPING and DORMANT items whose retry timer has elapsed.

        Queries both SLEEPING and DORMANT items where ``next_retry_at`` is at
        or before the current UTC time.

        Args:
            session: Active async SQLAlchemy session.

        Returns:
            List of MediaItem objects ready to be transitioned to SCRAPING.
        """
        now = self._utcnow()

        sleeping_result = await session.execute(
            select(MediaItem).where(
                MediaItem.state == QueueState.SLEEPING,
                MediaItem.next_retry_at <= now,
            )
        )
        dormant_result = await session.execute(
            select(MediaItem).where(
                MediaItem.state == QueueState.DORMANT,
                MediaItem.next_retry_at <= now,
            )
        )

        ready: list[MediaItem] = list(sleeping_result.scalars().all()) + list(
            dormant_result.scalars().all()
        )

        logger.debug("get_ready_for_retry: %d items ready for retry", len(ready))
        return ready

    async def process_queue(self, session: AsyncSession) -> dict[str, int]:
        """Main scheduler entry point: advance the entire queue one step.

        Performs two operations in sequence:
        1. Advance UNRELEASED items whose air date has passed to WANTED.
        2. Transition all SLEEPING/DORMANT items whose retry timer has elapsed
           to SCRAPING.

        Args:
            session: Active async SQLAlchemy session.

        Returns:
            Summary dict with keys:
            - ``unreleased_advanced``: number of UNRELEASED -> WANTED transitions.
            - ``retries_triggered``: number of SLEEPING/DORMANT -> SCRAPING transitions.
        """
        advanced_ids = await self.process_unreleased(session)

        ready_items = await self.get_ready_for_retry(session)
        retries_triggered = 0

        for item in ready_items:
            from_state = item.state
            logger.info(
                "process_queue: triggering retry for item id=%d (state=%r, title=%r)",
                item.id,
                from_state.value,
                item.title,
            )
            self._apply_transition_side_effects(item, from_state, QueueState.SCRAPING)
            await session.flush()
            retries_triggered += 1

        summary: dict[str, int] = {
            "unreleased_advanced": len(advanced_ids),
            "retries_triggered": retries_triggered,
        }
        logger.info(
            "process_queue complete: unreleased_advanced=%d retries_triggered=%d",
            summary["unreleased_advanced"],
            summary["retries_triggered"],
        )
        return summary


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

queue_manager = QueueManager()
