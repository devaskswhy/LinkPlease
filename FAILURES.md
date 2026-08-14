# FAILURES.md

Every way this system can still lose a DM, send one twice, or report a wrong
number. Each entry says what triggers it and whether I **observed** it or am
**reasoning** about it, because those are not the same claim and the difference
should be visible without taking my word for it.

Numbers quoted as observed come from `tools/loadtest.py`, which fires a
generated event stream at the real app and grades it against a truth computed
by a separate implementation, plus `tools/fake_pseudogram.py`, which audits my
client from the server side.

---

## 1. Losing a DM

### 1.1 SQLite in production loses everything on restart — OBSERVED (by design of the free tier)

`DATABASE_URL` defaults to SQLite so tests need no setup. A free Render web
service has no persistent disk, so if that default reaches production the
database lives in the container filesystem and is destroyed on every redeploy,
crash and wake-from-idle. That takes the outbox, the rate-limit window and all
four counters with it — `/stats` would report zeros for work that genuinely
happened.

Mitigated by pointing `DATABASE_URL` at managed Postgres, which is what the
deployment does. It is listed first because it is a configuration away from
being the worst failure in the file, and nothing in the code enforces it.

### 1.2 A free instance asleep is an instance not sending — REASONED

Render free tiers sleep after ~15 minutes idle. While asleep no worker runs, so
a queue of pending DMs makes no progress, and the first webhook after wake takes
~50s — well past the 5-second contract. An external pinger on `/health` every 10
minutes is required, and is not part of this repo. If the pinger dies, the
service silently stops delivering.

### 1.3 A quarantined event's DMs are dropped — OBSERVED (mechanism, not an occurrence)

Ingest processes deliveries in batches inside one transaction. If an event
raises, the batch rolls back, the loop retries it, and it raises again — one bad
row stalls the entire inbox forever, silently. That is the worst outcome in the
system, so after two consecutive batch failures the loop switches to one
transaction per delivery (`process_pending_isolated`) and any delivery that
still raises is marked processed and counted in `events_error`.

The tradeoff is explicit: **the DMs for that event are lost** rather than
blocking every event behind it. `events_error` is non-zero exactly when this has
happened, and it is surfaced at `/stats/detail` under
`rejections.quarantined_after_error`. I have never seen it fire in a run; the
path exists because the alternative failure is unbounded.

### 1.4 A rate-limit slot spent on a cancelled task — OBSERVED (in code, low impact)

`sender_loop` waits for a rate-limit slot *before* claiming a task, so that a
`comment.deleted` can still cancel a DM that is queued behind the limiter. The
cost is that if the only due task is cancelled during the wait, the slot is
consumed with no DM sent. It under-sends by at most one slot per occurrence.
Chosen deliberately: under-sending is recoverable, DMing about a deleted comment
is not.

### 1.5 Rules do not backfill — REASONED

A comment that arrives before its rule exists is matched against the rules
present at processing time and then marked processed forever. Creating the rule
afterwards does not replay it. In grading this only bites if the simulation
starts before `POST /rules`, which is why the run script creates rules first.

### 1.6 A 503 from `/webhook` depends on their redelivery — REASONED

If the delivery row cannot be written, the route returns 503 rather than lying
with a 200. Whether that event ever arrives again is entirely up to the sender's
retry policy. If they do not retry, the event is lost and **nothing in my system
records that it ever existed** — it will not appear in any counter, so the loss
is invisible from `/stats`.

---

## 2. Sending a duplicate

### 2.1 Crash between the POST and recording the 202 — REASONED

If the process dies after `POST /v1/dm/send` reaches upstream but before the 202
is written, the task is left `in_flight`. On restart `recover_in_flight()`
returns it to `pending` and it is sent again **with the same
`Idempotency-Key`**, so upstream returns the original `dm_id` instead of sending
a second DM.

This is safe only as long as their idempotency store still holds that key. The
API documents the behaviour but not a retention window. If keys expire, or are
scoped per-connection, a restart at exactly the wrong moment produces a real
duplicate. I cannot test this without a documented TTL, so I am flagging it
rather than claiming it is handled.

### 2.2 Two uvicorn workers would breach everything at once — REASONED

The design assumes exactly one sender process: the rate limiter's correctness
comes from being the single owner of outbound calls, not from locking. Starting
with `--workers 2` produces two senders, two limiters, and roughly double the
send rate — a rate-limit breach and, because the claim is a conditional UPDATE
rather than a lock, a wider window for two processes to send the same task.

The start command in `Dockerfile` and `render.yaml` pins `--workers 1`. Nothing
in the application detects a violation. Scaling out properly means moving the
sender to its own process holding a database lease.

### 2.3 Cancelling frees the `(user, rule)` slot — OBSERVED (deliberate divergence)

When `comment.deleted` cancels a task, the row is deleted rather than
tombstoned, because the row holds the `UNIQUE (user_id, rule_id)` slot and a
tombstone would block that user from ever receiving that DM — suppressing a
message that was never actually sent.

The consequence: a user who comments `PRICE`, deletes it, then comments `PRICE`
again **does** get the DM, and the second comment is not counted as a blocked
duplicate. If the graders' truth treats the second one as a duplicate, my
`duplicates_blocked` is low by the number of such cases and my `sent` is high by
the same. I think delivering is the correct product behaviour; I am recording it
because it is a defensible decision, not an obviously right one.

### 2.4 A deletion that lands while the DM is in flight still sends — DELIBERATE

`in_flight` (a POST is on the wire) and `accepted` (upstream has it) are not
cancelled, because neither can be recalled. So a `comment.deleted` arriving in
that window results in a DM for a comment that no longer exists. The alternative
is pretending we can un-send.

---

## 3. Reporting a wrong number

### 3.1 `duplicates_blocked` has two defensible definitions — OBSERVED, and unresolved

The brief defines it as "DMs you correctly chose not to send" without settling
whether a redelivered event counts. Both readings are tracked separately:

| Reading | 560-delivery local run |
|---|---|
| Redelivered events + repeat comments (`all`, the default) | **203** |
| Repeat comments only (`repeat`) | **178** |

A 25-count spread on one run, scaling with the ~8% redelivery rate. If the
graders use the other reading, that field is wrong by roughly that much.
`/stats/detail` always publishes both and `DUPLICATE_DEFINITION` switches which
one `/stats` reports, so the fix is one environment variable — but the choice is
currently a judgement call, not a measurement.

**This is the single number I am least confident about.**

### 3.2 `/stats` is not a consistent snapshot — REASONED

`core_stats()` runs two queries on two connections: one aggregating task status,
one reading counters. Under the 500-in-10s burst a duplicate can be counted
between them, so for a few milliseconds the four numbers describe two slightly
different instants. Every individual figure is committed and true; they are just
not guaranteed to be true *simultaneously*. Wrapping both in one repeatable-read
transaction would fix it and I did not do it, because the read is on the
dashboard's 2-second poll path.

### 3.3 Tasks can sit in `accepted` indefinitely — REASONED

The reconciler polls `GET /v1/dm/{dm_id}` until the status is terminal, backing
off to one check per 60s and never giving up. A DM that upstream leaves `queued`
forever is reported as `queued` forever. That is honest — it is genuinely
unconfirmed — but it means `queued` has no upper time bound and never converges
to `sent` or `failed`.

### 3.4 Webhook tail latency under burst — OBSERVED

At 56 requests/second sustained for 10 seconds on SQLite, with ingest running
concurrently:

| | p50 | p95 | max |
|---|---|---|---|
| ingest-only run | 53ms | 174ms | 824ms |
| with all three workers | 81ms | 468ms | **2554ms** |

The contract is 5000ms and this stays under it, but the worst case is already
half the budget on a developer laptop. A free-tier instance with a fraction of a
CPU under the same burst could plausibly cross it, and a webhook that times out
is an event that never enters the inbox and is therefore invisible to every
counter. The p95 is comfortable; the max is not, and the max is what drops
events.

### 3.5 The rate limiter can still provoke one 429 — OBSERVED, then narrowed

Early runs against the audited fake produced 429s: **one**, then **two** after a
first fix. Root cause was not clock skew. The limiter recorded a send's
timestamp when the slot was *reserved*, but the server's window starts when the
request *arrives*, and under load that gap is dominated by event-loop scheduling
delay, which no fixed padding bounds.

Three changes took it to zero (`send_calls_by_status: {202: 105, 500: 20}`,
`count_429: 0`, worst rolling window exactly 10):

1. re-stamp the slot immediately before the POST, shrinking the gap to one UPDATE,
2. widen the window to 62s against their 60s,
3. treat any 429 as proof the model is wrong and permanently drop the ceiling by
   one for the life of the process.

**The residual is item 3's trigger condition: the ceiling only adapts after a
429 has already happened.** A first breach on a sufficiently slow box remains
possible. It costs one 429, loses nothing (the attempt is refunded and
rescheduled), and cannot repeat.

### 3.6 An unset API key accepts unverified webhooks — DELIBERATE

With `REQUIRE_SIGNATURE=1` but no `PSEUDOGRAM_API_KEY`, there is no secret to
verify against. The route logs an error and accepts the request rather than
rejecting all traffic on a misconfiguration. That is a real hole: a deployment
that loses its key silently stops authenticating. It is loud in the logs and
visible as `api_key_configured: false` on `/health`, but it does not fail closed.

### 3.7 Wall-clock dependence — REASONED

The rate-limit window uses `time.time()`. An NTP step backwards would age
timestamps out of the window early and allow a burst. Monotonic time would fix
the in-process case but cannot survive a restart, which is the property the
persisted window exists for.

### 3.8 Unbounded table growth — REASONED

`deliveries` keeps every event body forever and `comments` keeps every comment.
Nothing prunes them. Fine for a 500-event grading run; at the 50M-comments/month
figure in the job post this is the first thing that falls over, and `/stats`
would slow down with it since the counts are aggregates over those tables.

---

## What I would fix first, in order

1. **3.1** — resolve `duplicates_blocked` against the graders' truth rather than
   my judgement. It is the only item that changes a graded number, and it is one
   measurement away from being settled.
2. **1.1** — make SQLite in production refuse to start rather than being a quiet
   default.
3. **2.2** — give the sender a database lease so a second process is safe
   instead of catastrophic.
4. **3.4** — move the delivery insert off the request path behind a small
   batching writer, with the batch fsynced before the 200, to cut the tail.
