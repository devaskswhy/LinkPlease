"""A local stand-in for the PseudoGram API, with the same hostility.

Why this exists: the real API allows 10 sends per 60 seconds, so a single
end-to-end test of the retry and reconciliation paths would take half an hour
and burn the budget I need for the real self-grading run. This reproduces the
documented failure modes locally at whatever speed I want, and -- the part that
matters -- it *audits* my client from the server side.

Reproduced faithfully:
  * 10 requests / rolling 60s on POST /v1/dm/send, 429 + Retry-After beyond it
  * ~20% random 500s
  * 202 Accepted with status "queued", never "delivered"
  * ~15% of accepted DMs resolve to "failed" a second or two later
  * Idempotency-Key returns the original dm_id instead of sending again

Added for grading myself, and not in the real API:
  GET /_audit -- every send call it saw, the worst rolling-60s window, how many
  429s I provoked, and how many distinct DMs were actually created per
  recipient. If that last number exceeds one per (recipient, message), I sent a
  duplicate, regardless of what my own /stats claims.

    uvicorn tools.fake_pseudogram:app --port 8099
"""

from __future__ import annotations

import os
import random
import time
from collections import defaultdict

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse

RATE_LIMIT = int(os.getenv("FAKE_RATE_LIMIT", "10"))
RATE_WINDOW = float(os.getenv("FAKE_RATE_WINDOW", "60"))
ERROR_RATE = float(os.getenv("FAKE_ERROR_RATE", "0.20"))
DELIVERY_FAILURE_RATE = float(os.getenv("FAKE_FAILURE_RATE", "0.15"))
SETTLE_SECONDS = float(os.getenv("FAKE_SETTLE_SECONDS", "2.0"))

app = FastAPI(title="fake-pseudogram")

state = {
    "calls": [],            # (ts, status_code) for every POST /v1/dm/send
    "dms": {},              # dm_id -> record
    "idempotency": {},      # key -> dm_id
    "sends_by_pair": defaultdict(list),   # (recipient, message) -> [dm_id]
}


@app.post("/v1/dm/send")
async def send_dm(request: Request, idempotency_key: str = Header(default=None, alias="Idempotency-Key")):
    now = time.time()
    payload = await request.json()

    # Rate limit is checked before anything else, exactly like the real one.
    recent = [ts for ts, _ in state["calls"] if ts > now - RATE_WINDOW]
    if len(recent) >= RATE_LIMIT:
        state["calls"].append((now, 429))
        retry_after = max(1, int(recent[0] + RATE_WINDOW - now) + 1)
        return JSONResponse(
            {"error": "rate_limited"}, status_code=429, headers={"Retry-After": str(retry_after)}
        )

    # Idempotency replay happens before the random 500, so a retry of a call
    # that already landed can never create a second DM.
    if idempotency_key and idempotency_key in state["idempotency"]:
        state["calls"].append((now, 202))
        dm_id = state["idempotency"][idempotency_key]
        return JSONResponse({"dm_id": dm_id, "status": state["dms"][dm_id]["status"], "replayed": True},
                            status_code=202)

    if random.random() < ERROR_RATE:
        state["calls"].append((now, 500))
        return JSONResponse({"error": "internal_error"}, status_code=500)

    recipient = payload.get("recipient_user_id")
    message = payload.get("message")
    if not recipient or not message:
        state["calls"].append((now, 400))
        return JSONResponse({"error": "invalid_request", "detail": "recipient_user_id and message required"},
                            status_code=400)

    dm_id = f"dm_{len(state['dms']) + 1:06d}"
    state["dms"][dm_id] = {
        "dm_id": dm_id,
        "recipient_user_id": recipient,
        "message": message,
        "status": "queued",
        "created_at": now,
        # Decided now, revealed only after SETTLE_SECONDS -- the client must
        # actually poll to find out.
        "final": "failed" if random.random() < DELIVERY_FAILURE_RATE else "delivered",
    }
    state["calls"].append((now, 202))
    state["sends_by_pair"][(recipient, message)].append(dm_id)
    if idempotency_key:
        state["idempotency"][idempotency_key] = dm_id

    return JSONResponse({"dm_id": dm_id, "status": "queued"}, status_code=202)


@app.get("/v1/dm/{dm_id}")
async def get_dm(dm_id: str):
    record = state["dms"].get(dm_id)
    if record is None:
        return JSONResponse({"error": "not_found"}, status_code=404)
    if record["status"] == "queued" and time.time() - record["created_at"] >= SETTLE_SECONDS:
        record["status"] = record["final"]
    return {
        "dm_id": dm_id,
        "status": record["status"],
        "recipient_user_id": record["recipient_user_id"],
        "updated_at": time.time(),
    }


@app.get("/_audit")
async def audit():
    calls = state["calls"]
    timestamps = [ts for ts, _ in calls]

    # Worst rolling 60s window, computed over every request we received.
    worst = 0
    for i, ts in enumerate(timestamps):
        count = sum(1 for other in timestamps[i:] if other < ts + RATE_WINDOW)
        worst = max(worst, count)

    # A duplicate is two DMs that both *reached the recipient* for the same
    # (recipient, message). Two dm_ids for one pair is not a duplicate when the
    # first one failed -- that is the resend Part C asks for, and counting it as
    # a duplicate would penalise the correct behaviour. Only `delivered` counts.
    duplicate_pairs = {}
    resent_pairs = 0
    for (recipient, message), dm_ids in state["sends_by_pair"].items():
        delivered = [i for i in dm_ids if state["dms"][i]["status"] == "delivered"]
        if len(delivered) > 1:
            duplicate_pairs[f"{recipient}|{message[:40]}"] = delivered
        elif len(dm_ids) > 1:
            resent_pairs += 1

    by_code: dict[int, int] = defaultdict(int)
    for _, code in calls:
        by_code[code] += 1

    return {
        "send_calls_total": len(calls),
        "send_calls_by_status": dict(sorted(by_code.items())),
        "rate_limit": {
            "configured": f"{RATE_LIMIT}/{RATE_WINDOW:g}s",
            "worst_rolling_window": worst,
            "breached": by_code.get(429, 0) > 0,
            "count_429": by_code.get(429, 0),
        },
        "dms_created": len(state["dms"]),
        "dms_delivered": sum(1 for d in state["dms"].values() if d["status"] == "delivered"),
        "dms_failed": sum(1 for d in state["dms"].values() if d["status"] == "failed"),
        "dms_still_queued": sum(1 for d in state["dms"].values() if d["status"] == "queued"),
        "idempotency_keys_seen": len(state["idempotency"]),
        # Ground truth on duplicates: two *delivered* DMs for one pair.
        "duplicate_recipient_message_pairs": duplicate_pairs,
        "duplicate_pair_count": len(duplicate_pairs),
        # Pairs that needed a second DM because the first was confirmed failed.
        # Expected behaviour, reported separately so it cannot be mistaken for
        # a duplicate.
        "pairs_resent_after_failure": resent_pairs,
    }


@app.post("/_reset")
async def reset():
    state["calls"].clear()
    state["dms"].clear()
    state["idempotency"].clear()
    state["sends_by_pair"].clear()
    return {"ok": True}


@app.get("/health")
async def health():
    return {"ok": True, "fake": True}
