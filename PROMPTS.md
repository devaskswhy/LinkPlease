# PROMPTS.md — the phase-wise prompt playbook

How this system was built with AI, phase by phase. Each phase has the exact prompt used, why
it is written that way, and the acceptance test that closes the phase.

The prompting rule that governs all of them comes from a good piece of advice about AI-built
award sites: **name the exact library, the exact selector, the exact constraint.** "Add retry
logic" produces generic slop. "Retry on 500 and transport errors with exponential backoff +
jitter, cap 6 attempts, reuse the same `Idempotency-Key` across attempts of one task, treat 400
as permanent, and do not count 429 as an attempt" produces the thing you actually shipped.

Four properties every prompt below has:

1. **Names the concrete tech.** `FastAPI`, `SQLAlchemy async text()`, `httpx.AsyncClient`, `hmac.compare_digest`.
2. **States the invariant, not the feature.** "The same user is never DMed twice for the same rule, even if two copies of the event land 5ms apart on two workers."
3. **Names the failure it must survive.** "…survive `SIGKILL` between the DB write and the HTTP POST."
4. **Ends with an acceptance test.** If the prompt cannot be graded, the output cannot be trusted.

---

## Phase 0 — Read the hostile API before writing a line

> Fetch `https://pseudogram-api.onrender.com/openapi.json` and enumerate every path, including
> ones the assignment brief does not mention. For each, list the required headers, the request
> schema, and the documented error codes. Flag anything that exists in the schema but is absent
> from the brief — those are the endpoints I can use to grade myself before submitting.

**Why this shape:** the brief is prose; the OpenAPI doc is truth. This phase found
`GET /v1/admin/dm_sends?email=` — the graders' own server-side send log — which is exactly what
`/stats` gets compared against. Building against the doc instead of the prose means the
self-check harness and the grader read the same source.

**Acceptance:** a written table of all 9 paths with auth requirements. No code yet.

---

## Phase 1 — Decide the storage substrate before the features

> I am building a webhook consumer that must not lose an event across a process restart, and it
> will be deployed on a free tier that sleeps and wipes its local disk. Compare: (a) SQLite on the
> container filesystem, (b) SQLite on a mounted volume, (c) managed Postgres over the network.
> Score each on: survives redeploy, survives 15-minute idle spin-down, write latency at 50
> requests/second, setup cost. Recommend one, and give me a data-access layer where the SQL is
> identical on SQLite and Postgres so local development and production run the same code paths.

**Why this shape:** every "we lost the queue" story in `FAILURES.md` traces back to this
decision, so it is made explicitly and first, not implicitly by whatever the framework defaults
to. The dialect-neutrality constraint is what lets the test suite run against in-process SQLite
while production runs Postgres.

**Acceptance:** `pytest` green against SQLite, same file green against a Postgres URL.

---

## Phase 2 — The inbox, not the handler (Part A)

> Write `POST /webhook` in FastAPI. It must return 200 in under 5 seconds under a load of 50
> requests/second, so it does exactly three things: read the raw body, append one row to a
> `deliveries` table, return. No rule matching, no HTTP calls to anyone, no `BackgroundTasks`
> holding work in memory.
>
> Every physical HTTP delivery gets its own row with its own autoincrement id, even when
> `event_id` repeats — a redelivery is a distinct row, because "how many times were we told this"
> is data I need for the duplicate counter. A separate async task drains unprocessed rows in id
> order and marks `processed_at` in the same transaction that writes its side effects.
>
> Acceptance: kill the process with `SIGKILL` mid-drain, restart, and assert every accepted
> delivery is processed exactly once — no gaps, no double side effects.

**Why this shape:** "do the real work in the background" is the brief's own instruction, and the
naive reading of it (`BackgroundTasks`) keeps the work in RAM, where a restart eats it. Naming
the table, the ordering, and the same-transaction rule forces the durable-inbox pattern instead.
The `SIGKILL` clause is what makes the model write the crash-recovery sweep rather than assume a
graceful shutdown.

**Acceptance:** the kill test above, plus 500 POSTs with p99 latency under 200ms.

---

## Phase 3 — Make the duplicate impossible, not unlikely (Part A)

> Enforce "the same user is never DMed twice for the same rule" with a `UNIQUE (user_id, rule_id)`
> constraint on the outbox table and an `INSERT … ON CONFLICT DO NOTHING`, not with a
> read-then-write check. When the insert affects zero rows, that is a blocked duplicate: increment
> the counter inside the same transaction as the insert attempt.
>
> This one constraint has to cover three different causes at once: the same `event_id` redelivered,
> the same user commenting the keyword twice on different posts, and two copies racing 5ms apart.
> Show me why a `SELECT` followed by an `INSERT` fails the third case and the constraint does not.

**Why this shape:** it forbids the wrong implementation by name. Left alone, a model writes
`if exists(...): return` — which is correct in a single-threaded test and wrong under the 500-in-10s
run, where two copies of an event interleave between the SELECT and the INSERT. Making the
database the arbiter removes the race instead of narrowing it. Asking for the *explanation*
alongside the code is the "explain every line you shipped" rule, enforced at write time.

**Acceptance:** fire the same event 20 times concurrently; exactly one outbox row exists and
`duplicates_blocked == 19`.

---

## Phase 4 — Signatures on the raw bytes (Part B)

> Verify `X-PseudoGram-Signature: sha256=<hex>` as HMAC-SHA256 of the **raw request body** using
> the PseudoGram API key as the secret. Compare with `hmac.compare_digest`, never `==`. Read the
> body with `await request.body()` and hand those exact bytes to both the verifier and the JSON
> parser — never re-serialise the parsed dict to check the signature, because key order and
> whitespace will not survive the round trip and every signature will fail.
>
> Reject with 401 when the header is missing, malformed, or wrong. Make strictness a config flag
> so the local load harness can run unsigned, but default it to strict.

**Why this shape:** the re-serialisation bug is the single most common way this feature is
written wrong, and it fails in a way that looks like "their signatures are broken." Naming it in
the prompt costs one sentence and saves an hour. `compare_digest` gets named explicitly because
`==` short-circuits and leaks timing.

**Acceptance:** a valid signature passes; flipping one byte of the body, one byte of the
signature, or dropping the header all return 401.

---

## Phase 5 — A rate limit you cannot breach, not one you usually respect (Part C)

> `POST /v1/dm/send` allows 10 requests per rolling 60 seconds. Build a limiter that makes
> breaching it structurally impossible: one single sender task owns all outbound sends, so there is
> no concurrency to race; every attempt (including retries) is recorded as a timestamp row **in the
> database before the POST**, so a process restart cannot forget the last 10 sends and burst; and
> the window is enforced at 61 seconds, not 60, so clock skew between me and the server cannot put
> an 11th request inside their window.
>
> When a 429 comes back anyway, honour `Retry-After`, and do not count that rejected attempt
> against the task's retry budget.

**Why this shape:** "add rate limiting" gets you an in-memory token bucket that is correct until
the process restarts, then bursts. The three clauses — single owner, persisted before the call,
window padded — each close a specific hole, and the "structurally impossible" framing pushes
toward a design where the invariant is enforced by shape rather than by care.

**Acceptance:** a 500-event run with zero 429 responses in the outbound log.

---

## Phase 6 — 202 is not delivered (Part C)

> `POST /v1/dm/send` returns 202 Accepted with `status: "queued"`, and roughly 15% of accepted DMs
> later become `failed`. So 202 must **not** count toward `sent`.
>
> Model the task lifecycle as `pending → in_flight → accepted → delivered | failed`, and write a
> reconciler that polls `GET /v1/dm/{dm_id}` — which does not count against the rate limit — until
> the status is terminal. `sent` counts only `delivered`. Anything still `accepted` is reported as
> `queued`, because it is not confirmed.
>
> When a DM comes back `failed`, re-send it with a **new** `Idempotency-Key`, because reusing the
> old key returns the original failed `dm_id` instead of sending anything. Retries of a single
> attempt reuse the key; a resend after confirmed failure rotates it. Explain that distinction in
> a comment.

**Why this shape:** the key-rotation rule is the subtle one — idempotency keys are usually
described as "always reuse", and blindly reusing here produces a retry loop that never sends
anything and reports `failed` forever. Stating both halves of the rule, and asking for the
comment, is what makes it survive the "explain every line" interview.

**Acceptance:** force a failed delivery, observe exactly one resend with a rotated key, and
`sent` incrementing only after `GET /v1/dm/{id}` says `delivered`.

---

## Phase 7 — Deletion that arrives early (Part C)

> Handle `comment.deleted`. Events arrive out of order, so the deletion can land **before** the
> `comment.created` it refers to. Write a tombstone keyed on `comment_id` on every deletion,
> whether or not the comment is known, and check that tombstone at task-creation time. Cancel any
> outbox task for that comment that has not yet been handed to the API; leave already-accepted
> ones alone, because they are gone.
>
> Cancelled tasks are not `sent`, `failed`, or `queued`. Do not fold them into any of those
> buckets to make the arithmetic look tidy — expose them as a separate field.

**Why this shape:** the brief hints at the out-of-order case; the tombstone-always rule is what
makes arrival order irrelevant instead of merely handled. The last sentence exists because the
tempting move is to bucket cancellations into `failed` so the four numbers sum neatly, and the
brief explicitly says inflated numbers are worse than honest ones.

**Acceptance:** deletion 200ms before creation, and deletion 5s after creation with the task
still pending — both end with zero DMs sent and one cancelled task.

---

## Phase 8 — Grade yourself before they grade you

> Write a harness that: starts a 500-event run via `POST /v1/simulate/start`, waits, pulls
> `GET /v1/simulate/{run_id}/truth`, pulls `GET /v1/admin/dm_sends?email=`, and diffs their truth
> against my `/stats`. Report per-metric deltas, and for `duplicates_blocked` show both candidate
> definitions — counting redeliveries as blocked duplicates, and not counting them — so I can see
> which one their number matches instead of guessing.

**Why this shape:** `duplicates_blocked` is the one metric in the contract whose definition is
genuinely ambiguous, and guessing wrong silently costs the whole automated stage. Computing both
and letting their data pick the winner turns a coin flip into a measurement.

**Acceptance:** every delta is zero, or the non-zero ones are understood and written into
`FAILURES.md`.

---

## Phase 9 — Write the failure list from evidence, not imagination

> Read the code and list every path where a DM can still be lost, sent twice, or counted wrong.
> For each, give the precise trigger condition and say whether I have actually observed it or am
> reasoning about it. Do not write a bullet that begins "in rare cases"; give me the window in
> milliseconds and the sequence of events that opens it. If a bullet cannot be tied to a specific
> line of code, cut it.

**Why this shape:** the brief promises to detect a dishonest short list in ninety seconds. The
"observed vs reasoned" split is the honesty mechanism — it makes the difference between the two
visible in the document itself rather than hiding it behind confident prose.

**Acceptance:** every bullet in `FAILURES.md` names a file, a condition, and an evidence status.

---

## The prompts that were wrong, and what replaced them

Kept here because the failed versions are more instructive than the good ones.

| Prompt that produced bad code | What went wrong | What replaced it |
|---|---|---|
| "Make sure we don't send duplicate DMs" | Read-then-write check; correct in tests, raced under load | Phase 3 — the unique constraint, named |
| "Add retry logic with backoff" | Retried 400s forever, counted 429s as attempts, rotated the idempotency key every attempt | Phase 6 — per-code policy, spelled out |
| "Verify the webhook signature" | Hashed `json.dumps(await request.json())` — every signature failed | Phase 4 — raw bytes, named explicitly |
| "Handle rate limiting" | In-memory bucket, burst on every restart | Phase 5 — persisted before the call |
| "Return stats" | Counted 202 as sent, inflating `sent` by ~15% | Phase 6 — 202 is not delivered |
