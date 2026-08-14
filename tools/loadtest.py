"""End-to-end load harness: 500 events in 10 seconds, graded against truth.

Boots the fake PseudoGram API and the real app as subprocesses, fires a
generated event stream at `/webhook`, then checks `/stats` against a ground
truth computed independently -- a plain dict simulation in `expected_truth()`
that shares no code with the app, so a bug in the app's dedup logic cannot
quietly agree with itself.

Two modes:

  --mode ingest   (default) 500 events in 10s with workers disabled. Grades the
                  inbox, matching, dedup accounting and deletion handling at
                  full arrival rate. Takes about 30 seconds.

  --mode full     Fewer events, workers on, pointed at the fake API. Grades the
                  sender, retries, reconciliation and the rate limiter, then
                  reads the fake's /_audit for server-side proof: zero 429s and
                  zero duplicate (recipient, message) pairs.

                  The rate-limit window is compressed to 6s (10 sends per 6s
                  instead of per 60s) so a run finishes in about a minute. It is
                  the same limiter code and the same algorithm -- only the
                  window constant differs, on both the client and the fake
                  server -- so a breach would still show up.

    python -m tools.loadtest --mode ingest
    python -m tools.loadtest --mode full
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import json
import os
import random
import subprocess
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
PYTHON = sys.executable
SECRET = "loadtest-secret"
ADMIN_TOKEN = "loadtest-admin"

APP_PORT = 8000
FAKE_PORT = 8099
APP_URL = f"http://127.0.0.1:{APP_PORT}"
FAKE_URL = f"http://127.0.0.1:{FAKE_PORT}"

KEYWORDS = ["PRICE", "LINK", "COURSE"]
RULES = {
    "PRICE": "Here's the price list: linkplease.example/pricing",
    "LINK": "Here's the link you asked for: linkplease.example/go",
    "COURSE": "Course details: linkplease.example/course",
}

# Comment templates. The keyword lands in different cases and positions on
# purpose -- the contract says matching is case-insensitive and matches
# anywhere, and this is where that gets exercised at volume.
TEMPLATES = [
    "{kw} please 🙏",
    "hey what's the {kw}?",
    "can you DM me the {kw}",
    "#{kw}",
    "{kw}!!!",
    "I need {kw} asap",
    "...{kw}",
]
NO_MATCH = [
    "love this 🔥", "first!", "how long did this take?", "amazing work",
    "what camera is this", "😍😍😍", "commenting for the algorithm",
]


# --------------------------------------------------------------------------
# Event generation
# --------------------------------------------------------------------------

def generate_events(count: int, users: int, seed: int, with_deletions: bool) -> list[dict]:
    """Build a stream shaped like the real one: repeats, redeliveries, deletions,
    and an arrival order that deliberately does not match `sent_at`."""
    rng = random.Random(seed)
    events: list[dict] = []
    created: list[tuple[str, str]] = []  # (comment_id, user_id)

    for i in range(count):
        user_id = f"usr_{rng.randrange(users):04d}"
        comment_id = f"cmt_{i:05d}"
        roll = rng.random()

        if roll < 0.60:
            text = rng.choice(TEMPLATES).format(kw=_cased(rng, rng.choice(KEYWORDS)))
        elif roll < 0.72:
            # Two keywords in one comment -> two separate DM intents.
            first, second = rng.sample(KEYWORDS, 2)
            text = f"{_cased(rng, first)} and also the {_cased(rng, second)}"
        else:
            text = rng.choice(NO_MATCH)

        events.append({
            "event_id": f"evt_{i:05d}",
            "event_type": "comment.created",
            "sent_at": _iso(time.time() + i * 0.01),
            "data": {
                "comment_id": comment_id,
                "post_id": f"post_{rng.randrange(12):02d}",
                "text": text,
                "created_at": _iso(time.time() + i * 0.01),
                "from": {"user_id": user_id, "username": f"user{user_id[-4:]}"},
            },
        })
        created.append((comment_id, user_id))

    # ~8% redeliveries: byte-identical repeats of an event already in the stream.
    for event in rng.sample(events, k=max(1, int(count * 0.08))):
        events.append(json.loads(json.dumps(event)))

    if with_deletions:
        for comment_id, _ in rng.sample(created, k=max(1, int(count * 0.04))):
            events.append({
                "event_id": f"evtdel_{comment_id}",
                "event_type": "comment.deleted",
                "sent_at": _iso(time.time()),
                "data": {"comment_id": comment_id},
            })

    # Order is not guaranteed by the real sender, so do not guarantee it here.
    rng.shuffle(events)
    return events


def _cased(rng: random.Random, keyword: str) -> str:
    return rng.choice([keyword.upper(), keyword.lower(), keyword.capitalize()])


def _iso(ts: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ts)) + ".000Z"


# --------------------------------------------------------------------------
# Independent ground truth
# --------------------------------------------------------------------------

def expected_truth(events: list[dict]) -> dict:
    """Replay the stream with plain dicts. Shares no code with the app.

    Models the app's contract, not its implementation: one DM per
    (user_id, rule) pair, a redelivery of an already-acted-on event is a blocked
    duplicate, a repeat comment from the same user is a blocked duplicate, and a
    deletion removes a not-yet-sent task and frees its slot.
    """
    tasks: dict[tuple[str, str], str] = {}
    deleted: set[str] = set()
    dup_redelivery = 0
    dup_repeat = 0
    skipped_deleted = 0
    cancelled = 0

    for event in events:
        data = event["data"]
        comment_id = data.get("comment_id", "")

        if event["event_type"] == "comment.deleted":
            deleted.add(comment_id)
            for key, owner in list(tasks.items()):
                if owner == comment_id:
                    del tasks[key]
                    cancelled += 1
            continue

        if comment_id in deleted:
            skipped_deleted += 1
            continue

        user_id = data["from"]["user_id"]
        haystack = data["text"].casefold()
        for keyword in KEYWORDS:
            if keyword.casefold() not in haystack:
                continue
            key = (user_id, keyword)
            if key in tasks:
                if tasks[key] == comment_id:
                    dup_redelivery += 1
                else:
                    dup_repeat += 1
            else:
                tasks[key] = comment_id

    return {
        "unique_dms": len(tasks),
        "dup_redelivery": dup_redelivery,
        "dup_repeat": dup_repeat,
        "duplicates_blocked": dup_redelivery + dup_repeat,
        "skipped_deleted": skipped_deleted,
        "cancelled": cancelled,
        "deliveries_sent": len(events),
    }


# --------------------------------------------------------------------------
# Process control
# --------------------------------------------------------------------------

def spawn(module: str, port: int, env_extra: dict[str, str]) -> subprocess.Popen:
    env = {**os.environ, **env_extra, "PYTHONUNBUFFERED": "1"}
    return subprocess.Popen(
        [PYTHON, "-m", "uvicorn", module, "--port", str(port), "--host", "127.0.0.1", "--log-level", "warning"],
        cwd=str(ROOT), env=env,
        stdout=subprocess.DEVNULL if os.getenv("LOADTEST_QUIET") else None,
        stderr=subprocess.STDOUT if os.getenv("LOADTEST_QUIET") else None,
    )


async def wait_healthy(url: str, timeout: float = 60.0) -> bool:
    deadline = time.time() + timeout
    async with httpx.AsyncClient(timeout=5.0) as client:
        while time.time() < deadline:
            try:
                if (await client.get(f"{url}/health")).status_code == 200:
                    return True
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.4)
    return False


# --------------------------------------------------------------------------
# Firing
# --------------------------------------------------------------------------

async def fire(events: list[dict], duration: float, concurrency: int = 40) -> dict:
    """POST every event at `/webhook` spread over `duration` seconds."""
    semaphore = asyncio.Semaphore(concurrency)
    latencies: list[float] = []
    codes: dict[int, int] = {}
    started = time.perf_counter()
    gap = duration / max(1, len(events))

    async with httpx.AsyncClient(timeout=20.0) as client:
        async def send(index: int, event: dict) -> None:
            delay = index * gap - (time.perf_counter() - started)
            if delay > 0:
                await asyncio.sleep(delay)
            body = json.dumps(event).encode()
            signature = hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()
            async with semaphore:
                begin = time.perf_counter()
                try:
                    response = await client.post(
                        f"{APP_URL}/webhook", content=body,
                        headers={"Content-Type": "application/json",
                                 "X-PseudoGram-Signature": f"sha256={signature}"},
                    )
                    code = response.status_code
                except httpx.HTTPError:
                    code = 0
                latencies.append(time.perf_counter() - begin)
                codes[code] = codes.get(code, 0) + 1

        await asyncio.gather(*(send(i, e) for i, e in enumerate(events)))

    latencies.sort()
    wall = time.perf_counter() - started
    return {
        "wall_seconds": round(wall, 2),
        "events_per_second": round(len(events) / wall, 1),
        "status_codes": codes,
        "latency_ms": {
            "p50": round(latencies[len(latencies) // 2] * 1000, 1),
            "p95": round(latencies[int(len(latencies) * 0.95)] * 1000, 1),
            "max": round(latencies[-1] * 1000, 1),
        },
    }


async def drain(url: str, key: str, target: int, timeout: float) -> bool:
    deadline = time.time() + timeout
    async with httpx.AsyncClient(timeout=20.0) as client:
        while time.time() < deadline:
            detail = (await client.get(f"{url}/stats/detail")).json()
            if detail["inbox"][key] == target:
                return True
            await asyncio.sleep(0.5)
    return False


# --------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------

results: list[tuple[str, bool]] = []


def compare(label: str, actual, expected, note: str = "") -> None:
    ok = actual == expected
    results.append((label, ok))
    mark = "PASS" if ok else "FAIL"
    delta = "" if ok else f"   delta={_delta(actual, expected)}"
    print(f"  {mark}  {label:<42} actual={actual!s:<8} expected={expected!s:<8}{delta} {note}")


def _delta(actual, expected):
    try:
        return actual - expected
    except TypeError:
        return "n/a"


# --------------------------------------------------------------------------
# Modes
# --------------------------------------------------------------------------

async def run_ingest_mode(count: int, users: int, duration: float, seed: int) -> None:
    print(f"\n=== INGEST MODE — {count} events over {duration:g}s, workers off ===")
    events = generate_events(count, users, seed, with_deletions=True)
    truth = expected_truth(events)

    app_proc = spawn("app.main:app", APP_PORT, {
        "DATABASE_URL": f"sqlite+aiosqlite:///{ROOT}/.loadtest_ingest.db",
        "PSEUDOGRAM_API_KEY": SECRET,
        "REQUIRE_SIGNATURE": "1",
        "RUN_WORKERS": "1",   # ingest on: we are grading matching and dedup
        "RUN_SENDER": "0",    # delivery off: 229 DMs at 10/min would take 23 minutes
        "ADMIN_TOKEN": ADMIN_TOKEN,
    })
    try:
        if not await wait_healthy(APP_URL):
            raise SystemExit("app did not become healthy")

        async with httpx.AsyncClient(timeout=30.0) as client:
            await client.post(f"{APP_URL}/admin/reset", headers={"X-Admin-Token": ADMIN_TOKEN})
            for keyword, message in RULES.items():
                await client.post(f"{APP_URL}/rules", json={"keyword": keyword, "dm_message": message})

        print(f"\n  firing {len(events)} deliveries ({count} events + redeliveries + deletions)...")
        fired = await fire(events, duration)
        print(f"  wall={fired['wall_seconds']}s  rate={fired['events_per_second']}/s  "
              f"codes={fired['status_codes']}  latency_ms={fired['latency_ms']}")

        print("\n  draining inbox...")
        drained = await drain(APP_URL, "deliveries_backlog", 0, timeout=120)

        async with httpx.AsyncClient(timeout=30.0) as client:
            stats = (await client.get(f"{APP_URL}/stats")).json()
            detail = (await client.get(f"{APP_URL}/stats/detail")).json()

        print("\n  --- webhook contract ---")
        compare("every delivery answered 200", fired["status_codes"].get(200, 0), len(events))
        compare("p95 webhook latency under 5000ms", fired["latency_ms"]["p95"] < 5000, True,
                f"(p95={fired['latency_ms']['p95']}ms)")
        compare("inbox fully drained", drained, True)
        compare("deliveries persisted", detail["inbox"]["deliveries_received"], len(events))

        print("\n  --- dedup accounting vs independent truth ---")
        compare("unique DM tasks", detail["tasks"]["pending"], truth["unique_dms"])
        compare("duplicates: redelivered events", detail["duplicates"]["redelivered_events"], truth["dup_redelivery"])
        compare("duplicates: repeat comments", detail["duplicates"]["repeat_comments"], truth["dup_repeat"])
        compare("duplicates_blocked (/stats)", stats["duplicates_blocked"], truth["duplicates_blocked"])

        print("\n  --- deletion handling ---")
        compare("cancelled before send", detail["deletions"]["cancelled_before_send"], truth["cancelled"])
        compare("deletion-before-creation skips", detail["deletions"]["skipped_deleted_first"], truth["skipped_deleted"])

        print("\n  --- /stats shape ---")
        compare("exactly four keys", sorted(stats.keys()),
                ["duplicates_blocked", "failed", "queued", "sent"])
        compare("queued == unique tasks", stats["queued"], truth["unique_dms"])
        compare("sent == 0 (workers off)", stats["sent"], 0)
    finally:
        app_proc.terminate()
        app_proc.wait(timeout=20)


async def run_full_mode(count: int, users: int, duration: float, seed: int) -> None:
    print(f"\n=== FULL MODE — {count} events, workers on, fake API, 6s rate window ===")
    events = generate_events(count, users, seed, with_deletions=False)
    truth = expected_truth(events)

    fake_proc = spawn("tools.fake_pseudogram:app", FAKE_PORT, {
        "FAKE_RATE_LIMIT": "10",
        "FAKE_RATE_WINDOW": "6",
        "FAKE_SETTLE_SECONDS": "1.5",
    })
    app_proc = spawn("app.main:app", APP_PORT, {
        "DATABASE_URL": f"sqlite+aiosqlite:///{ROOT}/.loadtest_full.db",
        "PSEUDOGRAM_API_KEY": SECRET,
        "PSEUDOGRAM_BASE_URL": FAKE_URL,
        "REQUIRE_SIGNATURE": "1",
        "RUN_WORKERS": "1",
        "RATE_LIMIT_MAX": "10",
        # 3.3% padding over the fake's 6s window -- the same ratio as the
        # production default (62s against their 60s), so this run is an honest
        # scale model of the real thing rather than a stricter one.
        "RATE_LIMIT_WINDOW": "6.2",
        "ADMIN_TOKEN": ADMIN_TOKEN,
    })
    try:
        if not (await wait_healthy(FAKE_URL) and await wait_healthy(APP_URL)):
            raise SystemExit("services did not become healthy")

        async with httpx.AsyncClient(timeout=30.0) as client:
            await client.post(f"{FAKE_URL}/_reset")
            await client.post(f"{APP_URL}/admin/reset", headers={"X-Admin-Token": ADMIN_TOKEN})
            for keyword, message in RULES.items():
                await client.post(f"{APP_URL}/rules", json={"keyword": keyword, "dm_message": message})

        print(f"\n  firing {len(events)} deliveries...")
        fired = await fire(events, duration)
        print(f"  wall={fired['wall_seconds']}s  codes={fired['status_codes']}  latency_ms={fired['latency_ms']}")

        expected_dms = truth["unique_dms"]
        budget = 60 + expected_dms * 0.7
        print(f"\n  waiting up to {budget:.0f}s for {expected_dms} DMs to reach a terminal state...")

        deadline = time.time() + budget
        async with httpx.AsyncClient(timeout=30.0) as client:
            while time.time() < deadline:
                stats = (await client.get(f"{APP_URL}/stats")).json()
                if stats["queued"] == 0:
                    break
                print(f"    sent={stats['sent']:<4} failed={stats['failed']:<4} "
                      f"queued={stats['queued']:<4} dupes={stats['duplicates_blocked']}", end="\r")
                await asyncio.sleep(2.0)

            stats = (await client.get(f"{APP_URL}/stats")).json()
            detail = (await client.get(f"{APP_URL}/stats/detail")).json()
            audit = (await client.get(f"{FAKE_URL}/_audit")).json()

        print("\n\n  --- our numbers ---")
        print(f"  {json.dumps(stats)}")
        print("\n  --- their audit ---")
        print(f"  {json.dumps({k: v for k, v in audit.items() if k != 'duplicate_recipient_message_pairs'})}")

        print("\n  --- rate limit (server-side proof) ---")
        compare("zero 429s provoked", audit["rate_limit"]["count_429"], 0)
        compare("worst rolling window <= 10", audit["rate_limit"]["worst_rolling_window"] <= 10, True,
                f"(worst={audit['rate_limit']['worst_rolling_window']})")

        print("\n  --- duplicates (server-side proof) ---")
        compare("no pair delivered twice", audit["duplicate_pair_count"], 0)
        # Every DM upstream created is either delivered or a failure we resent.
        # If those do not add up, a send escaped without being accounted for.
        compare("dms created == delivered + failed",
                audit["dms_created"], audit["dms_delivered"] + audit["dms_failed"])
        # Every failure upstream was either resent or was the last straw for a
        # task that had exhausted its resend budget (which is what `failed`
        # counts). If these do not balance, a confirmed failure was dropped
        # silently instead of being retried or reported.
        compare("every confirmed failure resent or reported",
                detail["reconciliation"]["resends_after_confirmed_failure"] + stats["failed"],
                audit["dms_failed"])

        print("\n  --- accounting ---")
        compare("duplicates_blocked", stats["duplicates_blocked"], truth["duplicates_blocked"])
        compare("sent + failed + queued == unique intents",
                stats["sent"] + stats["failed"] + stats["queued"], expected_dms)
        compare("sent matches their delivered count", stats["sent"], audit["dms_delivered"])
        print(f"  INFO  resends after confirmed failure: "
              f"{detail['reconciliation']['resends_after_confirmed_failure']}")
        print(f"  INFO  send calls upstream: {audit['send_calls_by_status']}")
    finally:
        app_proc.terminate()
        fake_proc.terminate()
        app_proc.wait(timeout=20)
        fake_proc.wait(timeout=20)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["ingest", "full"], default="ingest")
    parser.add_argument("--count", type=int, default=None)
    parser.add_argument("--users", type=int, default=None)
    parser.add_argument("--duration", type=float, default=None)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    if args.mode == "ingest":
        asyncio.run(run_ingest_mode(
            args.count or 500, args.users or 120, args.duration or 10.0, args.seed))
    else:
        asyncio.run(run_full_mode(
            args.count or 150, args.users or 40, args.duration or 5.0, args.seed))

    failed = [label for label, ok in results if not ok]
    print("\n" + "=" * 76)
    print(f"{len(results) - len(failed)}/{len(results)} checks passed"
          + (f"  —  FAILED: {failed}" if failed else "  —  ALL PASSED"))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
