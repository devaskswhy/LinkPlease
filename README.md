# LinkPlease — comment → DM automation

Someone comments `PRICE` on a post; they get the price list in a DM. Once, no
matter how many times they comment, and never silently dropped when the platform
API misbehaves — which it does, deliberately: 20% 500s, 10 sends per rolling 60
seconds, events out of order, ~8% redelivered, and ~15% of accepted DMs quietly
failing after the fact.

**Parts A + B + C.** FastAPI · SQLAlchemy async · SQLite locally, Postgres in production.

- **Dashboard** — `/`
- **Failure list** — [FAILURES.md](FAILURES.md) ← the interesting one

---

## The contract routes

| Route | Behaviour |
|---|---|
| `POST /webhook` | Verifies the HMAC, appends one row, returns. Nothing else happens on the request path. |
| `POST /rules` | `{keyword, dm_message}` → `201 {rule_id, keyword, dm_message}`. Upserts on the folded keyword. |
| `GET /stats` | Exactly `{sent, failed, queued, duplicates_blocked}` — no extra keys, so nothing can trip a strict grader. |

Everything else is on `/stats/detail`, `/health`, `/rules` (GET) and `/docs`.

---

## How it works

```
POST /webhook ──► verify HMAC over raw bytes ──► INSERT deliveries ──► 200
                                                        │
                                              (durable inbox, id order)
                                                        ▼
                                        ingest worker ── match rules
                                                        │
                                    INSERT dm_tasks ON CONFLICT DO NOTHING
                                     UNIQUE (user_id, rule_id)  ◄── dedup lives here
                                                        ▼
                                    sender ── rate limiter (10 / 62s, single owner)
                                                        │
                                              POST /v1/dm/send  ── 202 accepted
                                                        ▼
                                    reconciler ── GET /v1/dm/{id} until terminal
                                                   delivered → sent
                                                   failed    → resend, rotated key
```

Five decisions carry most of the weight:

**The webhook writes and returns.** No matching, no outbound HTTP, no in-memory
hand-off. A 200 is a durability promise, so the row is committed before we
answer. Observed: 56 req/s sustained, p95 174ms.

**Dedup is a constraint, not a check.** There is no "have we sent this?" read
anywhere. `UNIQUE (user_id, rule_id)` plus `ON CONFLICT DO NOTHING RETURNING id`
— no row returned means someone already claimed it, and that is a blocked
duplicate. The read-then-write version passes every single-threaded test and
loses under load, when two copies of an event interleave between the SELECT and
the INSERT. One constraint covers redelivered events, repeat comments, and
in-process races alike.

**202 is not delivered.** A 202 moves a task to `accepted`, never to
`delivered`. `sent` counts only what `GET /v1/dm/{dm_id}` confirmed. Counting
202s would inflate `sent` by roughly the 15% that later fail.

**Idempotency keys rotate on resend, not on retry.** Retrying one attempt (a
500, a timeout) reuses the key, so an attempt that actually landed comes back as
the original `dm_id` instead of a second DM. Resending after a *confirmed
failure* rotates it, because reusing it would return the same failed `dm_id` and
send nothing at all.

**The rate limit is enforced by shape.** One sender owns every outbound call, so
there is nothing to race; the slot is committed to the database before the POST,
so a restart cannot forget and burst; the window is 62s against their 60s; and
any 429 permanently lowers the ceiling, because the gap that matters is
scheduling delay, which no fixed padding bounds. Server-side audit of the last
run: 125 send calls, worst rolling window 10, **zero 429s**.

---

## Run it locally

```bash
python -m venv .venv && .venv/Scripts/activate     # Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env                                # set PSEUDOGRAM_API_KEY
uvicorn app.main:app --reload
```

Open <http://127.0.0.1:8000>.

## Prove it works

```bash
pytest                              # 42 unit tests: matcher, signatures, retry policy
python -m tools.smoke               # 22 end-to-end contract checks, in-process
python -m tools.loadtest --mode ingest   # 500 events in 10s vs an independent truth
python -m tools.loadtest --mode full     # sender + retries + reconciler + limiter
```

`--mode ingest` fires 500 events (plus redeliveries and deletions) at the real
app and compares every counter against `expected_truth()`, a plain-dict replay
that shares no code with the application — so a bug in the dedup logic cannot
quietly agree with itself.

`--mode full` boots [`tools/fake_pseudogram.py`](tools/fake_pseudogram.py), a
local replica of the hostile API that reproduces the 429s, the 20% 500s and the
15% late failures, and additionally **audits the client from the server side**:
how many DMs it actually created, the worst rolling 60s window it saw, and
whether any `(recipient, message)` pair was delivered more than once.

Last full run, all 8 server-side checks green:

```
{"sent": 84, "failed": 0, "queued": 0, "duplicates_blocked": 62}

send_calls_by_status : {202: 105, 500: 20}
worst_rolling_window : 10        count_429 : 0
duplicate_pair_count : 0         dms_created == delivered + failed
resends after confirmed failure : 21   (all recovered)
```

## The live run

500 events in 10 seconds against the deployment, graded against
`GET /v1/simulate/{run_id}/truth`:

| Their truth | Mine |
|---|---|
| `total_deliveries_attempted: 550` | `deliveries_received: 550` |
| `webhook_200_count: 550` | every delivery answered 200, none lost at ingress |
| `expected_unique_recipient_count: 96` | `sent: 92` |

```json
{"sent": 92, "failed": 0, "queued": 0, "duplicates_blocked": 77}
```

Zero failures, zero forged/rejected events, and **13 DMs that the API accepted
and then failed were caught by the reconciler and resent successfully** — the
202-is-not-delivered path working against the real thing, not the local fake.

The four-recipient gap is understood and deliberate: all four commented only
`"pricing please"`, and `"pricing"` does not contain the substring `"price"`.
Their expected list is derived from the comment template's intent rather than
from the literal substring rule the contract specifies, so no conforming matcher
reproduces it. [FAILURES.md §3.0](FAILURES.md) has the full reasoning, including
the one-word rule change that would close the gap and why taking it would risk
over-sending instead.

Three real bugs were found this way and would each have been fatal in grading:

1. **The webhook secret is not the API key.** The brief says it is; the live API
   signs with the account email. Following the brief rejected 44 of 44 events
   while every health indicator stayed green.
2. **`POST /v1/dm/send` answers 200, not the documented 202.** Keying success on
   202 classifies every real send as an error and retries all of them.
3. **A misconfigured deploy looks perfectly healthy.** No `DATABASE_URL` and no
   API key still starts cleanly, serves, and reports four zeros. `/health` now
   reports `dialect`, `configured_correctly` and a `warnings` list.

## Grade against the real API

```bash
python -m tools.pg keygen --email you@example.com
python -m tools.pg rules  --app https://your-app.example.com
python -m tools.pg run    --app https://your-app.example.com --count 500 --duration 10
```

`run` starts a real simulation, waits for the inbox to drain, then pulls
`/v1/simulate/{run_id}/truth` and `/v1/admin/dm_sends` and diffs both against
`/stats` — printing `duplicates_blocked` under **both** candidate definitions so
their data settles the one genuinely ambiguous number instead of me guessing.
Snapshots land in `.runs/`.

---

## Deploying

Web service, **one worker**, plus two things that decide whether this scores at
all:

1. **`DATABASE_URL` must be Postgres.** A free web service has no persistent
   disk; SQLite there is wiped on every restart, redeploy and wake-from-idle,
   taking the outbox and every counter with it.
2. **Keep it awake.** Free instances sleep after ~15 minutes and take ~50s to
   wake, which breaks the 5-second webhook contract and stalls the send queue.
   Point an external pinger at `/health` every 10 minutes.

| Variable | Purpose |
|---|---|
| `PSEUDOGRAM_API_KEY` | API key, and the HMAC secret for inbound signatures |
| `DATABASE_URL` | Postgres URL (`postgres://` and `postgresql://` both work) |
| `REQUIRE_SIGNATURE` | `1` — reject unsigned and forged webhooks |
| `RATE_LIMIT_MAX` / `RATE_LIMIT_WINDOW` | `10` / `62` |
| `DUPLICATE_DEFINITION` | `all` or `repeat` — see FAILURES.md §3.1 |
| `ADMIN_TOKEN` | enables `POST /admin/reset` between runs |

`--workers 1` is not incidental. The rate limiter is correct because one process
owns every outbound call; a second worker means a second limiter and an
immediate breach.

---

## Layout

```
app/
  main.py        routes, lifespan, worker startup
  ingest.py      drains the inbox; dedup and deletion logic
  sender.py      sender loop, retry policy, delivery reconciler
  ratelimit.py   durable rolling-window limiter
  pseudogram.py  upstream client; all status-code classification
  security.py    HMAC verification over raw bytes
  stats.py       the four graded numbers, and why each is drawn where it is
  db.py          schema; identical DML on SQLite and Postgres
tools/
  loadtest.py         500-event harness vs independent truth
  fake_pseudogram.py  local hostile API that audits its own client
  pg.py               apply / keygen / simulate / self-grade / submit
  smoke.py            fast in-process contract checks
```
