"""Drains the durable inbox and turns comment events into outbox tasks.

The webhook route does not run any of this. It writes one row and returns. This
worker is the only thing that interprets events, which means event handling can
be slow, can retry, and can crash, without ever costing us a 200 inside 5s.

**The duplicate-suppression design.** There is no "have we sent this already?"
read anywhere in this file. Suppression is a `UNIQUE (user_id, rule_id)`
constraint plus `INSERT ... ON CONFLICT DO NOTHING RETURNING id`. If the insert
returns no row, the DM was already claimed by someone, and that is a blocked
duplicate. This matters because the read-then-write version is correct in every
single-threaded test and wrong under the 500-events-in-10s run: two copies of
one event interleave between the SELECT and the INSERT and both decide to send.
A constraint cannot interleave.

The same constraint covers three different causes at once:
  * the same `event_id` redelivered (~8% of the stream),
  * the same user commenting the keyword again on another post,
  * two copies of one event racing inside the same process.

Which cause fired is recoverable after the fact by comparing the `comment_id`
on the surviving row, and that split is counted separately -- see stats.py for
why the two definitions of `duplicates_blocked` are both tracked.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from .config import settings
from .db import bump_counter, get_engine, now
from .matching import Rule, match_rules

log = logging.getLogger("linkplease.ingest")

# Set by the webhook route so the processor starts immediately instead of
# waiting out its poll interval. Purely a latency optimisation: correctness
# never depends on this firing, because the loop also polls.
wakeup = asyncio.Event()

EVENT_CREATED = "comment.created"
EVENT_DELETED = "comment.deleted"


async def load_rules(conn: AsyncConnection) -> list[Rule]:
    rows = (await conn.execute(text(
        "SELECT rule_id, keyword, keyword_lc, dm_message FROM rules "
        "WHERE active = 1 ORDER BY created_at, rule_id"
    ))).mappings().all()
    return [Rule(r["rule_id"], r["keyword"], r["keyword_lc"], r["dm_message"]) for r in rows]


async def _pending_rows(conn, limit: int):
    return (await conn.execute(
        text(
            "SELECT id, event_id, event_type, payload FROM deliveries "
            "WHERE processed_at IS NULL ORDER BY id ASC LIMIT :lim"
        ),
        {"lim": limit},
    )).mappings().all()


async def _process_one(conn, row, rules: list[Rule], stamp: float) -> None:
    try:
        payload = json.loads(row["payload"])
    except (ValueError, TypeError):
        await bump_counter(conn, "malformed_rejected")
        payload = None

    if isinstance(payload, dict):
        await _handle_event(conn, payload, row["event_type"], rules, stamp)

    await conn.execute(
        text("UPDATE deliveries SET processed_at = :t WHERE id = :id"),
        {"t": stamp, "id": row["id"]},
    )


async def process_pending(limit: int | None = None) -> int:
    """Process one batch of unprocessed deliveries. Returns how many were done.

    The whole batch runs in a single transaction: the side effects (comment
    rows, outbox inserts, duplicate counters) and the `processed_at` stamps
    commit together or not at all. That is what makes a `SIGKILL` mid-batch
    safe -- on restart the batch is simply redone from scratch, and no counter
    was incremented for work that did not land.
    """
    limit = limit or settings.ingest_batch_size

    async with get_engine().begin() as conn:
        rows = await _pending_rows(conn, limit)
        if not rows:
            return 0

        rules = await load_rules(conn)
        stamp = now()
        for row in rows:
            await _process_one(conn, row, rules, stamp)
        return len(rows)


async def process_pending_isolated(limit: int | None = None) -> int:
    """Poison-pill escape hatch: one transaction per delivery.

    Batching is what makes ingest fast, but it shares fate: one event that
    raises rolls the whole batch back, the loop retries the same batch, it
    raises again, and the entire inbox stalls forever behind a single bad row.
    Every subsequent event stops being processed -- which is the worst outcome
    in the whole system, and it is silent.

    So after a batch fails twice, the loop switches to this: each delivery gets
    its own transaction, and one that raises is marked processed and counted
    rather than retried. That drops the DMs for that one event -- recorded in
    `events_error` and listed in FAILURES.md -- instead of stopping the queue.
    """
    limit = limit or settings.ingest_batch_size

    async with get_engine().connect() as conn:
        rows = await _pending_rows(conn, limit)
    if not rows:
        return 0

    async with get_engine().begin() as conn:
        rules = await load_rules(conn)

    for row in rows:
        stamp = now()
        try:
            async with get_engine().begin() as conn:
                await _process_one(conn, row, rules, stamp)
        except Exception:  # noqa: BLE001
            log.exception("delivery %s could not be processed; quarantining it", row["id"])
            try:
                async with get_engine().begin() as conn:
                    await bump_counter(conn, "events_error")
                    await conn.execute(
                        text("UPDATE deliveries SET processed_at = :t WHERE id = :id"),
                        {"t": now(), "id": row["id"]},
                    )
            except Exception:  # noqa: BLE001
                # The database itself is unhealthy; leave the row for the next
                # pass rather than pretending we handled it.
                log.exception("could not quarantine delivery %s", row["id"])
                return 0

    return len(rows)


async def _handle_event(
    conn: AsyncConnection,
    payload: dict[str, Any],
    event_type: str | None,
    rules: list[Rule],
    stamp: float,
) -> None:
    etype = (event_type or payload.get("event_type") or "").strip()
    data = payload.get("data")
    if not isinstance(data, dict):
        await bump_counter(conn, "events_unusable")
        return

    if etype == EVENT_DELETED:
        await _handle_deleted(conn, data, stamp)
    elif etype == EVENT_CREATED or (not etype and data.get("text") is not None):
        await _handle_created(conn, data, rules, stamp)
    else:
        await bump_counter(conn, "events_unknown_type")


async def _handle_created(
    conn: AsyncConnection, data: dict[str, Any], rules: list[Rule], stamp: float
) -> None:
    comment_id = _s(data.get("comment_id"))
    sender = data.get("from") if isinstance(data.get("from"), dict) else {}
    user_id = _s(sender.get("user_id"))
    username = _s(sender.get("username"))
    body = data.get("text") or ""

    # user_id is the identity, never username -- usernames change, and a DM
    # addressed to a stale username goes to whoever holds it now.
    if not user_id or not comment_id:
        await bump_counter(conn, "events_unusable")
        return

    # Upsert the comment facts. `deleted` is deliberately not in the SET list:
    # if a tombstone got here first, re-learning the comment's content must not
    # resurrect it.
    await conn.execute(
        text("""
            INSERT INTO comments (comment_id, post_id, user_id, username, text, created_at, first_seen_at, deleted)
            VALUES (:cid, :pid, :uid, :uname, :txt, :created, :seen, 0)
            ON CONFLICT (comment_id) DO UPDATE SET
                post_id    = excluded.post_id,
                user_id    = excluded.user_id,
                username   = excluded.username,
                text       = excluded.text,
                created_at = excluded.created_at
        """),
        {
            "cid": comment_id,
            "pid": _s(data.get("post_id")),
            "uid": user_id,
            "uname": username,
            "txt": body,
            "created": _s(data.get("created_at")),
            "seen": stamp,
        },
    )

    deleted = (await conn.execute(
        text("SELECT deleted FROM comments WHERE comment_id = :cid"), {"cid": comment_id}
    )).scalar()
    if deleted:
        # The deletion beat the creation through the network. Honour it.
        await bump_counter(conn, "deleted_skipped")
        return

    for rule in match_rules(body, rules):
        await _claim_dm(conn, user_id, username, rule, comment_id, stamp)


async def _claim_dm(
    conn: AsyncConnection,
    user_id: str,
    username: str,
    rule: Rule,
    comment_id: str,
    stamp: float,
) -> None:
    inserted = (await conn.execute(
        text("""
            INSERT INTO dm_tasks
                (user_id, rule_id, comment_id, username, message, status,
                 next_attempt_at, created_at, updated_at)
            VALUES
                (:uid, :rid, :cid, :uname, :msg, 'pending', :t, :t, :t)
            ON CONFLICT (user_id, rule_id) DO NOTHING
            RETURNING id
        """),
        {
            "uid": user_id, "rid": rule.rule_id, "cid": comment_id,
            "uname": username, "msg": rule.dm_message, "t": stamp,
        },
    )).scalar()

    if inserted is not None:
        return

    # Lost the race / already claimed: this is a DM we correctly chose not to
    # send. Attribute it, so both candidate definitions of duplicates_blocked
    # stay computable.
    existing_comment = (await conn.execute(
        text("SELECT comment_id FROM dm_tasks WHERE user_id = :uid AND rule_id = :rid"),
        {"uid": user_id, "rid": rule.rule_id},
    )).scalar()

    if existing_comment == comment_id:
        # Same comment, same rule: this delivery was a redelivery of an event
        # we have already acted on.
        await bump_counter(conn, "dup_redelivery")
    else:
        # A genuinely different comment from a user we have already DMed for
        # this rule.
        await bump_counter(conn, "dup_repeat_user")


async def _handle_deleted(conn: AsyncConnection, data: dict[str, Any], stamp: float) -> None:
    comment_id = _s(data.get("comment_id"))
    if not comment_id:
        await bump_counter(conn, "events_unusable")
        return

    # Tombstone unconditionally, even for a comment we have never seen. That is
    # what makes arrival order irrelevant rather than merely handled: if the
    # creation shows up later, _handle_created reads deleted=1 and stops.
    await conn.execute(
        text("""
            INSERT INTO comments (comment_id, first_seen_at, deleted, deleted_at)
            VALUES (:cid, :seen, 1, :seen)
            ON CONFLICT (comment_id) DO UPDATE SET deleted = 1, deleted_at = excluded.deleted_at
        """),
        {"cid": comment_id, "seen": stamp},
    )

    # Cancel only what has not yet been handed to the API. `in_flight` means a
    # POST is on the wire right now and `accepted` means it is already theirs --
    # neither can be recalled, so neither is touched.
    #
    # The row is deleted rather than marked 'cancelled' because it holds the
    # UNIQUE (user_id, rule_id) slot. Leaving a tombstone there would mean a
    # user who deletes their comment and posts the keyword again is blocked
    # from ever receiving the DM -- suppressing a message we never actually
    # sent. Deleting frees the slot; no DM went out, so the "never twice"
    # guarantee is untouched. The audit trail survives on `comments`, which
    # keeps the deleted=1 row, and in the counter below.
    cancelled = (await conn.execute(
        text("DELETE FROM dm_tasks WHERE comment_id = :cid AND status = 'pending'"),
        {"cid": comment_id},
    )).rowcount

    if cancelled:
        await bump_counter(conn, "cancelled_pending", cancelled)


def _s(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


async def ingest_loop(stop: asyncio.Event) -> None:
    """Drain continuously. Wakes on `wakeup`, and polls as a safety net so a
    missed signal delays work rather than stranding it."""
    consecutive_failures = 0

    while not stop.is_set():
        try:
            # Two failures in a row means the batch is not going to succeed as a
            # batch. Isolate, so one bad row cannot hold the whole inbox hostage.
            if consecutive_failures >= 2:
                done = await process_pending_isolated()
                consecutive_failures = 0
            else:
                done = await process_pending()
                consecutive_failures = 0
        except Exception:  # noqa: BLE001 - a worker loop must not die
            consecutive_failures += 1
            log.exception("ingest batch failed (%d in a row); retrying", consecutive_failures)
            done = 0
            await asyncio.sleep(min(1.0 * consecutive_failures, 5.0))

        if done:
            # More may be waiting: keep going without sleeping.
            wakeup.clear()
            continue

        wakeup.clear()
        try:
            await asyncio.wait_for(wakeup.wait(), timeout=0.25)
        except asyncio.TimeoutError:
            pass
