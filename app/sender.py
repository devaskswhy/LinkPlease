"""Outbound delivery: the sender loop and the delivery reconciler.

Two ideas do most of the work here.

**202 is not delivered.** The API returns `202 {"status": "queued"}` and then
roughly 15% of those quietly become `failed`. So a 202 moves a task to
`accepted`, never to `delivered`, and `sent` in /stats counts only rows the
reconciler has confirmed via `GET /v1/dm/{dm_id}`. Anything still `accepted` is
reported as `queued`, because it is not confirmed.

**Idempotency keys rotate on resend, not on retry.** The distinction is the
subtle part:

  * *Retrying one attempt* (a 500, a timeout) reuses the same key. That is the
    whole point: a 500 that actually landed upstream returns the original
    `dm_id` on retry instead of sending a second DM.
  * *Resending after a confirmed `failed` delivery* rotates the key. Reusing it
    would make the API hand back the same failed `dm_id` without sending
    anything, and the task would loop forever reporting a failure it could have
    fixed.

`generation` is the rotation counter, so the key is a pure function of
(task id, generation) and survives a restart unchanged.
"""

from __future__ import annotations

import asyncio
import logging
import random

from sqlalchemy import text

from .config import settings
from .db import bump_counter, get_engine, now
from .pseudogram import PseudoGramClient, SendOutcome
from .ratelimit import RateLimiter

log = logging.getLogger("linkplease.sender")


def idempotency_key(task_id: int, generation: int) -> str:
    return f"lp-{task_id}-g{generation}"


def backoff_delay(attempts: int) -> float:
    """Exponential with full jitter. Jitter matters because a 500-event burst
    produces a cohort of tasks that would otherwise retry in lockstep."""
    ceiling = min(settings.retry_base_delay * (2 ** max(0, attempts - 1)), settings.retry_max_delay)
    return random.uniform(ceiling / 2, ceiling)


def check_delay(checks: int) -> float:
    """Reconciliation polling backoff. Reads are free, so start tight."""
    if checks >= settings.reconcile_max_checks:
        return 60.0
    return min(2.0 * (1.6 ** checks), 30.0)


# --------------------------------------------------------------------------
# Crash recovery
# --------------------------------------------------------------------------

async def recover_in_flight() -> int:
    """Return tasks stranded mid-POST by a crash to the pending queue.

    Safe without knowing whether the POST landed: the retry reuses the same
    idempotency key, so if it did land we get the original `dm_id` back rather
    than a second DM. `attempts` is intentionally left as-is -- the attempt was
    genuinely spent.
    """
    async with get_engine().begin() as conn:
        result = await conn.execute(
            text("""
                UPDATE dm_tasks SET status = 'pending', next_attempt_at = :t, updated_at = :t
                WHERE status = 'in_flight'
            """),
            {"t": now()},
        )
        return result.rowcount


# --------------------------------------------------------------------------
# Sender
# --------------------------------------------------------------------------

async def _peek_pending() -> bool:
    async with get_engine().connect() as conn:
        found = (await conn.execute(
            text("SELECT 1 FROM dm_tasks WHERE status = 'pending' AND next_attempt_at <= :t LIMIT 1"),
            {"t": now()},
        )).scalar()
    return found is not None


async def _claim_next() -> dict | None:
    """Move one due task from `pending` to `in_flight` and return it.

    The claim is a conditional `UPDATE ... WHERE id = ? AND status = 'pending'`.
    If it affects zero rows, something else changed the task underneath us --
    in practice a `comment.deleted` cancelling it while we were queued behind
    the rate limiter -- so we move on to the next candidate rather than sending
    a DM for a comment that no longer exists.
    """
    for _ in range(10):
        async with get_engine().begin() as conn:
            row = (await conn.execute(
                text("""
                    SELECT id, user_id, comment_id, message, generation, attempts
                    FROM dm_tasks
                    WHERE status = 'pending' AND next_attempt_at <= :t
                    ORDER BY next_attempt_at ASC, id ASC
                    LIMIT 1
                """),
                {"t": now()},
            )).mappings().first()

            if row is None:
                return None

            claimed = (await conn.execute(
                text("""
                    UPDATE dm_tasks
                    SET status = 'in_flight', attempts = attempts + 1, updated_at = :t
                    WHERE id = :id AND status = 'pending'
                """),
                {"id": row["id"], "t": now()},
            )).rowcount

            if claimed == 1:
                return dict(row)
    return None


async def _record_send_result(task: dict, result, limiter: RateLimiter) -> None:
    task_id = task["id"]
    stamp = now()

    async with get_engine().begin() as conn:
        if result.outcome is SendOutcome.ACCEPTED:
            await conn.execute(
                text("""
                    UPDATE dm_tasks
                    SET status = 'accepted', dm_id = :dm, checks = 0,
                        next_check_at = :check_at, last_error = NULL, updated_at = :t
                    WHERE id = :id
                """),
                {"dm": result.dm_id, "check_at": stamp + 2.0, "t": stamp, "id": task_id},
            )
            return

        if result.outcome is SendOutcome.RATE_LIMIT:
            # Our limiter and theirs disagreed. Resync to their answer, and
            # refund the attempt -- being throttled is not the task failing.
            limiter.note_rate_limited(result.retry_after or 5.0)
            await conn.execute(
                text("""
                    UPDATE dm_tasks
                    SET status = 'pending', attempts = CASE WHEN attempts > 0 THEN attempts - 1 ELSE 0 END,
                        next_attempt_at = :next, last_error = '429 rate_limited', updated_at = :t
                    WHERE id = :id
                """),
                {"next": stamp + (result.retry_after or 5.0), "t": stamp, "id": task_id},
            )
            log.warning("429 from upstream on task %s; backing off %.1fs", task_id, result.retry_after or 5.0)
            return

        if result.outcome is SendOutcome.PERMANENT:
            await conn.execute(
                text("""
                    UPDATE dm_tasks
                    SET status = 'failed', last_error = :err, updated_at = :t
                    WHERE id = :id
                """),
                {"err": f"{result.status_code} {result.detail}"[:500], "t": stamp, "id": task_id},
            )
            return

        # TRANSIENT
        attempts = int(task["attempts"]) + 1  # the claim already incremented it
        if attempts >= settings.max_send_attempts:
            await conn.execute(
                text("""
                    UPDATE dm_tasks
                    SET status = 'failed', last_error = :err, updated_at = :t
                    WHERE id = :id
                """),
                {"err": f"gave up after {attempts} attempts: {result.detail}"[:500],
                 "t": stamp, "id": task_id},
            )
        else:
            await conn.execute(
                text("""
                    UPDATE dm_tasks
                    SET status = 'pending', next_attempt_at = :next, last_error = :err, updated_at = :t
                    WHERE id = :id
                """),
                {"next": stamp + backoff_delay(attempts),
                 "err": f"{result.status_code or 'transport'} {result.detail}"[:500],
                 "t": stamp, "id": task_id},
            )


async def sender_loop(client: PseudoGramClient, limiter: RateLimiter, stop: asyncio.Event) -> None:
    """The single owner of outbound sends.

    Order of operations is deliberate: wait for a rate-limit slot *first*, then
    claim a task. The reverse would park a task in `in_flight` for however long
    the limiter makes us wait -- possibly minutes, given 10 sends/60s -- during
    which a `comment.deleted` could not cancel it. The cost of this order is
    that a slot is occasionally spent on a task that got cancelled while we
    waited; `_claim_next` reuses the slot for the next candidate when it can.
    """
    while not stop.is_set():
        try:
            if not await _peek_pending():
                await _sleep(0.5, stop)
                continue

            slot_id = await limiter.acquire(stop)
            if slot_id is None:
                continue

            task = await _claim_next()
            if task is None:
                # Slot spent on a task that vanished (cancelled while we waited).
                # Left consumed on purpose: under-sending is the safe direction.
                continue

            # Re-date the slot to the moment of the actual request, not the
            # moment it was reserved. See RateLimiter.stamp().
            await limiter.stamp(slot_id)

            result = await client.send_dm(
                recipient_user_id=task["user_id"],
                message=task["message"],
                comment_id=task["comment_id"],
                idempotency_key=idempotency_key(task["id"], int(task["generation"])),
            )
            await _record_send_result(task, result, limiter)

        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - the loop must outlive any single failure
            log.exception("sender iteration failed")
            await _sleep(1.0, stop)


# --------------------------------------------------------------------------
# Reconciler
# --------------------------------------------------------------------------

async def reconcile_once(client: PseudoGramClient, limit: int = 50) -> int:
    async with get_engine().connect() as conn:
        rows = (await conn.execute(
            text("""
                SELECT id, dm_id, checks, resends
                FROM dm_tasks
                WHERE status = 'accepted' AND dm_id IS NOT NULL
                  AND (next_check_at IS NULL OR next_check_at <= :t)
                ORDER BY next_check_at ASC
                LIMIT :lim
            """),
            {"t": now(), "lim": limit},
        )).mappings().all()

    if not rows:
        return 0

    semaphore = asyncio.Semaphore(settings.reconcile_concurrency)

    async def check(row):
        async with semaphore:
            return row, await client.get_dm(row["dm_id"])

    results = await asyncio.gather(*(check(row) for row in rows))

    stamp = now()
    async with get_engine().begin() as conn:
        for row, status in results:
            task_id = row["id"]

            if status is None or status.status == "queued":
                # Still in flight upstream, or the status read itself failed.
                # Either way: look again later, and stay in the `queued` bucket.
                checks = int(row["checks"]) + 1
                await conn.execute(
                    text("UPDATE dm_tasks SET checks = :c, next_check_at = :n, updated_at = :t WHERE id = :id"),
                    {"c": checks, "n": stamp + check_delay(checks), "t": stamp, "id": task_id},
                )
                continue

            if status.status == "delivered":
                await conn.execute(
                    text("""
                        UPDATE dm_tasks SET status = 'delivered', next_check_at = NULL, updated_at = :t
                        WHERE id = :id AND status = 'accepted'
                    """),
                    {"t": stamp, "id": task_id},
                )
                continue

            # status == "failed": the API accepted it and then dropped it.
            if int(row["resends"]) < settings.max_resends:
                # Rotate the idempotency key by bumping `generation`; reusing it
                # would return this same failed dm_id and send nothing.
                await conn.execute(
                    text("""
                        UPDATE dm_tasks
                        SET status = 'pending', generation = generation + 1, resends = resends + 1,
                            attempts = 0, dm_id = NULL, checks = 0, next_check_at = NULL,
                            next_attempt_at = :t, last_error = 'delivery_failed; resending',
                            updated_at = :t
                        WHERE id = :id AND status = 'accepted'
                    """),
                    {"t": stamp, "id": task_id},
                )
                await bump_counter(conn, "resends_after_failure")
            else:
                await conn.execute(
                    text("""
                        UPDATE dm_tasks
                        SET status = 'failed', next_check_at = NULL,
                            last_error = 'delivery_failed after resends', updated_at = :t
                        WHERE id = :id AND status = 'accepted'
                    """),
                    {"t": stamp, "id": task_id},
                )

    return len(rows)


async def reconcile_loop(client: PseudoGramClient, stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            done = await reconcile_once(client)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            log.exception("reconcile iteration failed")
            done = 0
        await _sleep(0.5 if done else 1.5, stop)


async def _sleep(seconds: float, stop: asyncio.Event) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass
