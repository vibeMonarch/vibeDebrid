"""Backtest script: verify lookup_multi coverage for DONE/COMPLETE items.

For each DONE/COMPLETE item that has at least one symlink, runs lookup_multi
using gather_alt_titles and compares the result against the symlink's
source_path.  Reports matches, mismatches, and items where lookup returned
nothing.

Usage:
    .venv/bin/python scripts/backtest_mount_lookup.py
"""

from __future__ import annotations

import asyncio
import os
import sys

# Allow running from the repo root without installing the package.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from src.database import async_session
from src.models.media_item import MediaItem, QueueState
from src.models.symlink import Symlink
from src.core.mount_scanner import gather_alt_titles, mount_scanner


async def run() -> None:
    """Query all DONE/COMPLETE items and backtest mount lookup against symlinks."""
    done_states = {QueueState.DONE, QueueState.COMPLETE}

    matched = 0
    mismatched: list[tuple[MediaItem, str, list[str]]] = []
    no_results: list[tuple[MediaItem, str]] = []
    skipped_no_symlink = 0

    async with async_session() as session:
        # Load all DONE/COMPLETE items with their symlinks eagerly.
        result = await session.execute(
            select(MediaItem)
            .where(MediaItem.state.in_(done_states))
            .options(selectinload(MediaItem.symlinks))
        )
        items = list(result.scalars().all())

    print(f"Found {len(items)} DONE/COMPLETE items.")

    async with async_session() as session:
        for item in items:
            valid_symlinks = [s for s in item.symlinks if s.valid and s.source_path]
            if not valid_symlinks:
                skipped_no_symlink += 1
                continue

            titles = await gather_alt_titles(session, item)
            lookup_results = await mount_scanner.lookup_multi(
                session, titles, season=item.season, episode=item.episode
            )

            found_paths = {m.filepath for m in lookup_results}

            for symlink in valid_symlinks:
                expected = symlink.source_path
                if expected in found_paths:
                    matched += 1
                elif lookup_results:
                    # Lookup returned results but not the expected path
                    mismatched.append((item, expected, [m.filepath for m in lookup_results]))
                else:
                    no_results.append((item, expected))

    total_symlinks = matched + len(mismatched) + len(no_results)
    print(f"\n--- Results ({total_symlinks} symlinks total) ---")
    print(f"  Matched:    {matched} ({matched / total_symlinks * 100:.1f}%)" if total_symlinks else "  Matched:    0")
    print(f"  Mismatched: {len(mismatched)}")
    print(f"  No results: {len(no_results)}")
    print(f"  Skipped (no valid symlink): {skipped_no_symlink}")

    if mismatched:
        print(f"\n--- Mismatches ({len(mismatched)}) ---")
        for item, expected, got in mismatched[:20]:
            print(f"  id={item.id} title={item.title!r} S{item.season}E{item.episode}")
            print(f"    expected: {expected}")
            for g in got[:3]:
                print(f"    got:      {g}")

    if no_results:
        print(f"\n--- No results ({len(no_results)}) ---")
        for item, expected in no_results[:20]:
            print(f"  id={item.id} title={item.title!r} S{item.season}E{item.episode}")
            print(f"    expected: {expected}")


if __name__ == "__main__":
    asyncio.run(run())
