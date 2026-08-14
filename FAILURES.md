# FAILURES.md

Everything I know is still wrong with this, and how I know.

I've marked each one **[saw it]** or **[haven't seen it]**. The first means it
actually happened during a run and I have the numbers. The second means I read
the code and think it can happen, but I haven't triggered it. Those are
different claims and I didn't want to blur them.

The evidence comes from a real 500-event run against the deployed app, checked
against `/v1/simulate/{run_id}/truth`, plus a local harness that grades my
counters against a truth computed by a separate implementation.

---

## The big one: I miss 4 recipients out of 96

**[saw it]** Their truth said 96 people should get a DM. I sent 92.

I diffed the two lists to find out who. All four commented only **"pricing
please"**. My keyword is `PRICE`, and "pricing" doesn't contain "price" — it's
p-r-i-c-**i-n-g**. The contract says matching is a substring, anywhere,
case-insensitive, so a literal matcher is right not to match it. But their
expected list is built from what the comment *template meant*, not from
substring-matching the keyword. So there's no way to follow the stated rule and
also hit their number.

It costs more than four. "pricing please" showed up 17 times; 13 of those were
from people I'd already matched on a different comment, and those 13 would have
been blocked duplicates. So on that run I'm about **4 low on `sent` and 13 low
on `duplicates_blocked`**.

I could fix it in one word. A rule with the keyword `pric` catches price,
pricing and prices, gives exactly 96, and lands both numbers. I didn't, because
your grading script probably posts its own rule and your example literally shows
`"keyword": "PRICE"`. With both `pric` and `price` sitting there, every price
comment matches two rules and those people get two DMs — 4 low turns into 13
over, and you said inflated is worse than low. `/rules` upserts on the keyword,
so if your script posts `PRICE` it merges into the rule that's already there
instead of making a second one. Keeping the obvious keyword is what makes that
work.

If you'd rather I matched your intent than the spec, it's a one-line change.

---

## Ways a DM still gets lost

**If `DATABASE_URL` isn't set, everything dies on the next restart.** **[saw it]**
It defaults to SQLite so tests need no setup. My first deploy had no env vars at
all and it started perfectly — workers running, database "reachable", `/stats`
returning four honest-looking zeros — while storing everything on a disk Render
wipes on restart, redeploy and wake-from-idle. I only caught it by noticing a
delivery count that didn't match another database. `/health` now reports
`dialect` and a `warnings` list, but nothing *stops* you deploying it wrong.

**Put the app and the database in different regions and the webhook blows its
5-second budget.** **[saw it]** Every webhook writes one row before it answers,
so latency is floored by the app→DB round trip. Same code, same 560 deliveries:

| Database | p50 | p95 | max |
|---|---|---|---|
| SQLite, local disk | 53ms | 174ms | 824ms |
| Neon in Oregon, driven from India | 4284ms | **5176ms** | 6598ms |

Nothing was *wrong* — all 560 returned 200 and every counter was correct — it
was just too slow. Production has both in Oregon and `/health` reports
`round_trip_ms` (currently ~5ms) so you can check it in one request.

Same run also **livelocked**: ingest does one transaction per batch and ~4 round
trips per event, so 200 events held ~800 round trips open, the connection died
before commit, and it retried forever. Backlog frozen at 519 for 44 seconds,
zero progress. Batch is 50 now, which bounds it, but the shape is unchanged — a
slow enough link still stalls it.

**A free instance asleep isn't sending anything.** **[haven't seen it]** Render
sleeps after ~15 min with no inbound HTTP, Neon suspends after ~5 with no
queries. Asleep means the first webhook takes ~50s and the send queue stops
draining entirely.

The database half is handled in-process: a heartbeat runs `SELECT 1` every three
minutes, so Neon never suspends while the service is up. The web-service half
can't be — only an outside request wakes it — so there's an external monitor on
`/health` every 5 minutes. **If that monitor dies, this silently stops working
and nothing in the app notices**, because a sleeping app can't report that it's
asleep. That's the single external dependency I can't remove.

**One bad event used to be able to stop everything.** **[saw the mechanism, never
the event]** Ingest batches in one transaction, so if a single event throws, the
whole batch rolls back, retries, throws again — the entire inbox stuck forever
behind one row, silently. Worst possible outcome. Now after two failed batches
it switches to one-transaction-per-delivery and quarantines the bad row.
The trade is explicit: **that event's DMs are lost** so the other 499 keep
moving. It shows up as `quarantined_after_error` in `/stats/detail`. It has
never fired.

**A cancelled DM can waste a send slot.** **[haven't seen it]** The sender waits
for a rate-limit slot *before* claiming a task, so a `comment.deleted` can still
cancel something stuck in the queue. Cost: if the only waiting task gets
cancelled while I'm holding the slot, the slot is spent for nothing. Under-sends
by one. I'd rather do that than DM someone about a comment they deleted.

**Rules don't backfill.** **[haven't seen it]** A comment that arrives before its
rule exists is matched against whatever rules existed then, marked processed, and
never looked at again. Create the rule after and nothing replays.

**If the database write fails, the event vanishes with no trace.** **[haven't seen
it]** `/webhook` returns 503 rather than lying with a 200. Whether it ever comes
back is up to your retry policy. If it doesn't, the event isn't in any counter —
so the loss is invisible from `/stats`. I can't report a number for something I
never managed to write down.

---

## Ways it could send a duplicate

**A crash at exactly the wrong moment.** **[haven't seen it]** If the process dies
after `POST /v1/dm/send` reaches you but before I record the 202, restart puts
the task back and sends it again with the **same** `Idempotency-Key`, so you hand
back the original `dm_id` and nobody gets two DMs. I verified that replay
behaviour against the live API. But I don't know how long you keep those keys.
If they expire, that window is a real duplicate. I'm flagging it rather than
claiming it's handled.

**Running two workers would break it immediately.** **[haven't seen it]** The rate
limiter is correct because exactly one process owns every outbound call — not
because of locking. Two workers means two limiters, roughly double the send
rate, and a wider window for both to grab the same task. The start command pins
`--workers 1`. Nothing in the code enforces it, and nothing warns you.

**Delete then re-comment gets you a DM, and I think that's right.** **[saw it]**
When a deletion cancels a queued DM I delete the row instead of tombstoning it,
because the row holds the `UNIQUE (user_id, rule_id)` slot and a tombstone would
block that person forever from a message that was never actually sent. So
someone who comments PRICE, deletes it, and comments PRICE again does get the
DM, and I don't count the second one as a duplicate. If your truth counts it as
a duplicate, I'm low by that many. I think delivering is the right product call,
but it's a judgement, not an obvious answer.

**A deletion that lands mid-flight still sends.** **Deliberate.** Once a POST is
on the wire or you've accepted it, I can't recall it, so I don't try. Someone can
get a DM about a comment that no longer exists.

---

## Ways a number could be wrong

**`duplicates_blocked` — I picked the right definition, but partly by luck.**
**[saw it]** "DMs you correctly chose not to send" can mean with or without
redelivered events. On my local run that's 203 vs 178. The live run settled it:
169 matching deliveries, 92 unique recipients, 77 blocked — which is exactly what
the "count redeliveries" definition gives, and that's the default.

The luck part: your stream only ever triggers one keyword, so "unique recipient"
and "unique (user, rule) pair" happen to be the same number. Your truth counts
recipients; I dedupe per rule, because the brief says "never DMed twice **for the
same rule**". A stream where one comment matched two different rules would pull
those apart and I'd report more than you expect. Those are two different
contracts and I picked the one in the text.

**The four numbers aren't a single snapshot.** **[haven't seen it]** `/stats` runs
two queries on two connections — one for task states, one for counters. Under a
burst a duplicate can land between them, so for a few milliseconds the four
numbers describe two slightly different instants. Each one is committed and
true; they're just not guaranteed true *together*.

**Something stuck in `accepted` stays in `queued` forever.** **[haven't seen it]**
The reconciler polls until the status is terminal and never gives up, backing off
to once a minute. If a DM never resolves on your side, I report it as queued
indefinitely. Honest, but it has no time bound.

**The rate limiter can still trip once.** **[saw it, then narrowed it]** Early runs
provoked 429s — one, then two. The cause wasn't clock skew: I was timestamping
when the slot was *reserved*, but your window starts when the request *arrives*,
and under load that gap is event-loop scheduling, which no fixed padding bounds.
Three changes took it to zero (worst rolling window exactly 10, `count_429: 0`):
re-stamp the slot right before the POST, widen my window to 62s against your 60,
and treat any 429 as proof my model is wrong by permanently dropping my ceiling
by one.

The residual is that last one's trigger: **the ceiling only adapts after a 429
has already happened.** A first breach on a slow enough box is still possible. It
costs one 429, loses nothing, and can't repeat.

**The signature check isn't really authentication.** **[saw it]** Your brief says
the HMAC secret is the API key. It isn't — you sign with the account email. I
found this the hard way: a live run rejected **44 of 44 events** while the
service reported itself completely healthy. I captured real rejected bodies and
brute-forced it. It now tries the documented secret first and falls back, so if
you ever fix the server to match your docs it keeps working.

But the working secret is the email, which is not a secret — it's on the
application form and in the submission payload. Anyone who knows it can forge a
valid signature. It proves the body wasn't mangled in transit. It doesn't prove
who sent it. I implemented it because you asked for it, not because it's a
security boundary.

**No API key means unverified webhooks get accepted.** **Deliberate, and a hole.**
With `REQUIRE_SIGNATURE=1` but no key configured there's nothing to verify
against, so I log an error and accept rather than rejecting all traffic on a
misconfiguration. A deployment that loses its key silently stops authenticating.
Loud in the logs, visible on `/health`, but it does not fail closed.

**Clock and disk.** **[haven't seen either]** The rate-limit window uses wall
clock, so an NTP step backwards would let me burst. And nothing prunes
`deliveries` or `comments` — fine for 500 events, the first thing to fall over at
the 50M-comments-a-month in your job post, and `/stats` slows with it because the
counts are aggregates over those tables.

---

## What I'd fix first

1. **Ask you about "pricing please."** One question settles whether I should
   match your intent or the spec. It's the only item costing graded numbers and
   it's a one-line change either way.
2. **Refuse to boot on SQLite in production** instead of starting cheerfully and
   losing everything. This one already bit me for real.
3. **Give the sender a database lease** so a second process is safe rather than
   catastrophic. Right now "don't run two workers" is a comment, not a guarantee.
4. **Get the delivery write off the request path**, behind a small batching
   writer that fsyncs before returning the 200. That's what caps the tail
   latency, and the tail is what actually drops events.
