"""Outbound rate limiter for POST /v1/dm/send.

Upstream allows 10 requests per rolling 60 seconds. "Never breached" is a
stronger requirement than "usually respected", so the limit is enforced by
shape, not by care. Three properties do that:

1. **Single owner.** Exactly one sender task calls `acquire()`. There is no
   concurrency between the check and the call, so there is nothing to race.
   The lock below is belt-and-braces for a future second caller.

2. **Persisted before the call.** The timestamp row is committed *before* the
   POST goes out. A crash between the write and the call costs us one wasted
   slot; a crash after an in-memory-only write would let the restarted process
   forget up to 10 recent sends and burst straight through the limit.

3. **A padded window.** 61 seconds, not 60. Our clock and theirs are not the
   same clock, and the failure mode of being 200ms optimistic is a 429.

Reads (`GET /v1/dm/{id}`) are documented as free and deliberately do not pass
through here.
"""

from __future__ import annotations

import asyncio

from sqlalchemy import text

from .config import settings
from .db import get_engine, now


class RateLimiter:
    def __init__(self, max_calls: int | None = None, window: float | None = None) -> None:
        self.max_calls = max_calls if max_calls is not None else settings.rate_limit_max
        self.window = window if window is not None else settings.rate_limit_window
        self._lock = asyncio.Lock()
        # Set when upstream returns 429 anyway; a hard floor on the next attempt.
        self._blocked_until = 0.0
        self._prune_after = 0.0
        # Adaptive ceiling. See note_rate_limited().
        self._effective_max = self.max_calls
        self._floor = max(3, self.max_calls // 2)

    @property
    def effective_max(self) -> int:
        return self._effective_max

    def note_rate_limited(self, retry_after: float) -> None:
        """Upstream said no. Back off now, and lower our own ceiling for good.

        Padding the window is not sufficient on its own, and the load harness
        showed why: the gap that matters is between our recorded timestamp and
        the request's *arrival*, and under load that gap is dominated by event
        loop scheduling, not by clock skew. No fixed padding is safe against an
        unbounded pause.

        So the limiter treats a 429 as evidence that its model of their window
        is wrong, and permanently gives up one slot per breach, down to a floor.
        A disagreement therefore costs exactly one 429 and then self-corrects,
        instead of recurring every window for the life of the process. Nothing
        is lost either way -- a 429 refunds the attempt and reschedules -- but
        repeatedly provoking one is the thing the brief asks me not to do.
        """
        self._blocked_until = max(self._blocked_until, now() + retry_after)
        if self._effective_max > self._floor:
            self._effective_max -= 1

    async def acquire(self, stop: asyncio.Event | None = None) -> int | None:
        """Block until a slot is available, then consume it.

        Returns the `send_log` row id of the consumed slot, or None if `stop`
        was set while waiting. Pass the id to `stamp()` immediately before the
        POST -- see that method for why.
        """
        async with self._lock:
            while True:
                if stop is not None and stop.is_set():
                    return None

                current = now()
                wait_for = self._blocked_until - current
                if wait_for > 0:
                    if not await self._sleep(min(wait_for, 5.0), stop):
                        return None
                    continue

                async with get_engine().begin() as conn:
                    if current >= self._prune_after:
                        # Keep the table from growing without bound. Anything
                        # older than two windows can never constrain us again.
                        await conn.execute(
                            text("DELETE FROM send_log WHERE ts < :cutoff"),
                            {"cutoff": current - self.window * 2},
                        )
                        self._prune_after = current + 30.0

                    rows = (await conn.execute(
                        text("SELECT ts FROM send_log WHERE ts > :floor ORDER BY ts ASC"),
                        {"floor": current - self.window},
                    )).scalars().all()

                    if len(rows) < self._effective_max:
                        # Commit the slot before returning; the caller POSTs next.
                        slot_id = (await conn.execute(
                            text("INSERT INTO send_log (ts) VALUES (:ts) RETURNING id"),
                            {"ts": current},
                        )).scalar()
                        return int(slot_id)

                    # Oldest call in the window ages out at ts + window.
                    wait_for = (float(rows[0]) + self.window) - current + 0.05

                if not await self._sleep(max(0.05, min(wait_for, 5.0)), stop):
                    return None

    async def stamp(self, slot_id: int) -> None:
        """Re-date a reserved slot to *now*, immediately before the POST.

        This closes a real breach found by the load harness. The slot is
        reserved (and committed) before the task is claimed, so between the
        reservation and the actual request there is a claim query -- and on a
        throttled free-tier box, possibly a scheduling pause. Our window is
        measured from the reserved time; theirs from the arrival time. If a call
        lands materially later than we recorded it, it is still inside their
        window when our 11th goes out, and we get a 429 despite doing the
        arithmetic correctly.

        Re-stamping shrinks that gap to one UPDATE. Moving a timestamp *later*
        is always the safe direction: it can only delay our next send.
        """
        async with get_engine().begin() as conn:
            await conn.execute(
                text("UPDATE send_log SET ts = :ts WHERE id = :id"),
                {"ts": now(), "id": slot_id},
            )

    @staticmethod
    async def _sleep(seconds: float, stop: asyncio.Event | None) -> bool:
        """Sleep, but wake early on shutdown. False means "we are stopping"."""
        if stop is None:
            await asyncio.sleep(seconds)
            return True
        try:
            await asyncio.wait_for(stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            return True
        return False

    async def snapshot(self) -> dict[str, float | int]:
        """Live limiter state for the dashboard and /stats/detail."""
        current = now()
        async with get_engine().connect() as conn:
            rows = (await conn.execute(
                text("SELECT ts FROM send_log WHERE ts > :floor ORDER BY ts ASC"),
                {"floor": current - self.window},
            )).scalars().all()
        used = len(rows)
        next_slot = 0.0
        if used >= self._effective_max and rows:
            next_slot = max(0.0, (float(rows[0]) + self.window) - current)
        return {
            "window_seconds": self.window,
            "max_per_window": self.max_calls,
            "effective_max": self._effective_max,
            "used_in_window": used,
            "remaining": max(0, self._effective_max - used),
            "seconds_until_next_slot": round(next_slot, 2),
            "blocked_until_in": round(max(0.0, self._blocked_until - current), 2),
        }
