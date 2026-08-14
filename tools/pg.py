"""CLI for the real PseudoGram API: apply, keygen, simulate, self-grade, submit.

    python -m tools.pg apply --name "..." --email "..." --phone "+91..." --linkedin "..."
    python -m tools.pg keygen --email "..."
    python -m tools.pg rules  --app https://your-app.example.com
    python -m tools.pg run    --app https://your-app.example.com --count 500 --duration 10
    python -m tools.pg truth  --run-id <run_id>
    python -m tools.pg submit --email "..." --repo ... --url ... --loom ... --parts A+B+C --start 2026-08-14

`run` is the self-grading step: it starts a real simulation against the deployed
app, waits for the inbox to drain, pulls their truth and their server-side send
log, and diffs both against /stats -- including both candidate definitions of
`duplicates_blocked`, so their data decides which one is right instead of me
guessing.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

import httpx
from dotenv import load_dotenv

# The app loads .env via app.config, but this CLI reads the environment before
# importing any of that -- without this, --key would be the only way in.
load_dotenv()

BASE = os.getenv("PSEUDOGRAM_BASE_URL", "https://pseudogram-api.onrender.com").rstrip("/")
RUNS_DIR = Path(__file__).resolve().parent.parent / ".runs"

# The rules created for a grading run. Keep this list in sync with whatever the
# graders' stream actually contains -- `truth` prints the keywords it saw.
DEFAULT_RULES = {
    "PRICE": "Here's the price list 💸 linkplease.example/pricing",
    "LINK": "Here's the link you asked for 🔗 linkplease.example/go",
    "COURSE": "Course details are here 🎓 linkplease.example/course",
    "INFO": "All the info you need ℹ️ linkplease.example/info",
}


def pretty(label: str, payload) -> None:
    print(f"\n--- {label} ---")
    print(json.dumps(payload, indent=2, ensure_ascii=False)[:6000])


async def _json(client: httpx.AsyncClient, method: str, url: str, **kwargs):
    response = await client.request(method, url, **kwargs)
    try:
        body = response.json()
    except ValueError:
        body = {"raw": response.text[:2000]}
    return response.status_code, body


# --------------------------------------------------------------------------

async def cmd_apply(args) -> int:
    payload = {
        "name": args.name, "email": args.email,
        "phone": args.phone, "linkedin_url": args.linkedin,
    }
    if args.whatsapp:
        payload["whatsapp"] = args.whatsapp
    async with httpx.AsyncClient(timeout=90) as client:
        code, body = await _json(client, "POST", f"{BASE}/v1/apply", json=payload)
    pretty(f"POST /v1/apply -> {code}", body)
    return 0 if code < 400 else 1


async def cmd_keygen(args) -> int:
    async with httpx.AsyncClient(timeout=90) as client:
        code, body = await _json(client, "POST", f"{BASE}/v1/keygen", json={"email": args.email})
    pretty(f"POST /v1/keygen -> {code}", body)
    if code == 403:
        print("\n403 means the email has not applied yet. Run `apply` first.")
        return 1
    if key := body.get("api_key"):
        print(f"\nSet these, then redeploy:\n  PSEUDOGRAM_API_KEY={key}\n  PSEUDOGRAM_EMAIL={args.email}")
        print("\nThe key is also the HMAC secret for inbound webhook signatures.")
    return 0 if code < 400 else 1


async def cmd_rules(args) -> int:
    """Create the keyword rules on the deployed app."""
    async with httpx.AsyncClient(timeout=60) as client:
        for keyword, message in DEFAULT_RULES.items():
            code, body = await _json(client, "POST", f"{args.app.rstrip('/')}/rules",
                                     json={"keyword": keyword, "dm_message": message})
            print(f"  {code}  {keyword:<8} {body.get('rule_id', body)}")
    return 0


async def cmd_truth(args) -> int:
    key = args.key or os.getenv("PSEUDOGRAM_API_KEY", "")
    async with httpx.AsyncClient(timeout=90, headers={"X-API-Key": key}) as client:
        code, body = await _json(client, "GET", f"{BASE}/v1/simulate/{args.run_id}/truth")
    pretty(f"GET /v1/simulate/{args.run_id}/truth -> {code}", body)
    if isinstance(body, dict):
        print("\n--- truth payload shape ---")
        for field, value in body.items():
            kind = f"list[{len(value)}]" if isinstance(value, list) else type(value).__name__
            print(f"  {field}: {kind}")
        _summarise_truth(body)
    return 0


def _summarise_truth(truth: dict) -> None:
    """Work out the expected numbers from whatever shape they return.

    Their field names are not documented, so this looks for the event list and
    recomputes both duplicate definitions from it. If the shape is unfamiliar it
    says so rather than guessing.
    """
    events = None
    for field in ("events", "sent_events", "deliveries", "payloads"):
        if isinstance(truth.get(field), list):
            events = truth[field]
            break
    if not events:
        print("\n  (no recognisable event list; inspect the payload above by hand)")
        return

    seen: dict[str, int] = {}
    keywords: set[str] = set()
    for event in events:
        data = event.get("data", event) if isinstance(event, dict) else {}
        event_id = str(event.get("event_id", ""))
        seen[event_id] = seen.get(event_id, 0) + 1
        text = str(data.get("text", "")).casefold()
        for keyword in DEFAULT_RULES:
            if keyword.casefold() in text:
                keywords.add(keyword)

    redeliveries = sum(count - 1 for count in seen.values() if count > 1)
    print(f"\n  events in truth      : {len(events)}")
    print(f"  distinct event_ids   : {len(seen)}")
    print(f"  redeliveries         : {redeliveries}")
    print(f"  keywords observed    : {sorted(keywords) or 'none of my rules matched — CHECK DEFAULT_RULES'}")


async def cmd_run(args) -> int:
    key = args.key or os.getenv("PSEUDOGRAM_API_KEY", "")
    if not key:
        print("Need an API key: --key or PSEUDOGRAM_API_KEY")
        return 1

    app = args.app.rstrip("/")
    RUNS_DIR.mkdir(exist_ok=True)

    async with httpx.AsyncClient(timeout=120, headers={"X-API-Key": key}) as api, \
               httpx.AsyncClient(timeout=60) as mine:

        before = (await mine.get(f"{app}/stats")).json()
        print(f"\n/stats before: {json.dumps(before)}")

        code, started = await _json(api, "POST", f"{BASE}/v1/simulate/start", json={
            "webhook_url": f"{app}/webhook", "count": args.count, "duration_seconds": args.duration,
        })
        if code >= 400:
            pretty(f"simulate/start -> {code}", started)
            return 1
        run_id = started.get("run_id")
        print(f"run_id={run_id}  ({args.count} events over {args.duration}s)")

        deadline = time.time() + args.duration + args.wait
        drained_at = None
        while time.time() < deadline:
            await asyncio.sleep(3)
            detail = (await mine.get(f"{app}/stats/detail")).json()
            inbox, stats = detail["inbox"], detail["stats"]
            print(f"  received={inbox['deliveries_received']:<5} backlog={inbox['deliveries_backlog']:<5} "
                  f"sent={stats['sent']:<4} queued={stats['queued']:<4} dupes={stats['duplicates_blocked']}",
                  end="\r")
            if inbox["deliveries_backlog"] == 0 and inbox["deliveries_received"] >= args.count:
                drained_at = time.time()
                break

        print()
        detail = (await mine.get(f"{app}/stats/detail")).json()
        stats = (await mine.get(f"{app}/stats")).json()
        _, truth = await _json(api, "GET", f"{BASE}/v1/simulate/{run_id}/truth")
        sends = {}
        if args.email:
            _, sends = await _json(api, "GET", f"{BASE}/v1/admin/dm_sends",
                                   params={"email": args.email, "since": 0})

        snapshot = {
            "run_id": run_id, "requested": {"count": args.count, "duration": args.duration},
            "drained": drained_at is not None,
            "stats": stats, "detail": detail, "truth": truth, "their_send_log": sends,
        }
        path = RUNS_DIR / f"run_{run_id or int(time.time())}.json"
        path.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8")

        pretty("my /stats", stats)
        pretty("my /stats/detail", {k: v for k, v in detail.items() if k != "config"})
        _summarise_truth(truth if isinstance(truth, dict) else {})
        if sends:
            pretty("their /v1/admin/dm_sends", sends if len(json.dumps(sends)) < 4000 else
                   {"note": "truncated", "keys": list(sends)[:20]})

        print(f"\nFull snapshot written to {path}")
        print("\n--- the number to reconcile ---")
        print(f"  duplicates_blocked reported : {stats['duplicates_blocked']}")
        print(f"    counting redeliveries     : {detail['duplicates']['all']}")
        print(f"    repeat comments only      : {detail['duplicates']['repeat_only']}")
        print("  If their truth says otherwise, flip DUPLICATE_DEFINITION and redeploy.")
    return 0


async def cmd_probe(args) -> int:
    """Validate the client against the *real* API, not just the local fake.

    Sends one DM and polls it to a terminal status. Costs one of the ten sends
    in the current window, and confirms the things the fake can only assume:
    the real status codes, the real response shape, and that a 202 really does
    resolve to delivered/failed on a later read.
    """
    key = args.key or os.getenv("PSEUDOGRAM_API_KEY", "")
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from app.pseudogram import PseudoGramClient
    from app.sender import idempotency_key

    client = PseudoGramClient(api_key=key)
    await client.start()
    try:
        idem = idempotency_key(999_001, 0)
        result = await client.send_dm("usr_probe_001", "probe: contract check", "cmt_probe_001", idem)
        print(f"\nsend      outcome={result.outcome.value} status={result.status_code} "
              f"dm_id={result.dm_id} detail={result.detail[:120]}")

        if result.dm_id:
            # Same key again: must return the original dm_id, not a second DM.
            replay = await client.send_dm("usr_probe_001", "probe: contract check", "cmt_probe_001", idem)
            same = replay.dm_id == result.dm_id
            print(f"replay    dm_id={replay.dm_id}  same_as_original={same}"
                  f"{'' if same else '   <-- IDEMPOTENCY DOES NOT HOLD, retries would duplicate'}")

            for attempt in range(20):
                await asyncio.sleep(2)
                status = await client.get_dm(result.dm_id)
                if status is None:
                    print(f"  poll {attempt + 1}: status read failed")
                    continue
                print(f"  poll {attempt + 1}: {status.status}")
                if status.terminal:
                    print(f"\nterminal after ~{(attempt + 1) * 2}s: {status.status}")
                    break
            else:
                print("\nnever reached a terminal status in 40s")
    finally:
        await client.aclose()
    return 0


async def cmd_submit(args) -> int:
    payload = {
        "github_repo": args.repo, "working_url": args.url,
        "loom_url": args.loom, "parts_completed": args.parts, "start_date": args.start,
    }
    if args.email:
        payload["email"] = args.email
    if args.key:
        payload["api_key"] = args.key

    print("\nAbout to POST this to /v1/submit:")
    print(json.dumps(payload, indent=2))
    if not args.yes:
        print("\nRe-run with --yes to actually submit.")
        return 0

    async with httpx.AsyncClient(timeout=90) as client:
        code, body = await _json(client, "POST", f"{BASE}/v1/submit", json=payload)
    pretty(f"POST /v1/submit -> {code}", body)
    return 0 if code < 400 else 1


def main() -> int:
    parser = argparse.ArgumentParser(prog="tools.pg")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("apply")
    p.add_argument("--name", required=True)
    p.add_argument("--email", required=True)
    p.add_argument("--phone", required=True)
    p.add_argument("--linkedin", required=True)
    p.add_argument("--whatsapp")
    p.set_defaults(fn=cmd_apply)

    p = sub.add_parser("keygen")
    p.add_argument("--email", required=True)
    p.set_defaults(fn=cmd_keygen)

    p = sub.add_parser("rules")
    p.add_argument("--app", required=True)
    p.set_defaults(fn=cmd_rules)

    p = sub.add_parser("run")
    p.add_argument("--app", required=True)
    p.add_argument("--count", type=int, default=500)
    p.add_argument("--duration", type=int, default=10)
    p.add_argument("--wait", type=int, default=120, help="extra seconds to wait for the inbox to drain")
    p.add_argument("--key")
    p.add_argument("--email")
    p.set_defaults(fn=cmd_run)

    p = sub.add_parser("probe")
    p.add_argument("--key")
    p.set_defaults(fn=cmd_probe)

    p = sub.add_parser("truth")
    p.add_argument("--run-id", required=True)
    p.add_argument("--key")
    p.set_defaults(fn=cmd_truth)

    p = sub.add_parser("submit")
    p.add_argument("--email")
    p.add_argument("--key")
    p.add_argument("--repo", required=True)
    p.add_argument("--url", required=True)
    p.add_argument("--loom")
    p.add_argument("--parts", default="A+B+C")
    p.add_argument("--start", required=True)
    p.add_argument("--yes", action="store_true")
    p.set_defaults(fn=cmd_submit)

    args = parser.parse_args()
    return asyncio.run(args.fn(args))


if __name__ == "__main__":
    sys.exit(main())
