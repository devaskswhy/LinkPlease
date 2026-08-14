"""Fast local smoke test: rules -> signed webhook -> dedup -> stats.

Runs the real ASGI app in-process with workers disabled, so it exercises the
contract routes and the ingest path without touching the network.

    python -m tools.smoke
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import sys
import tempfile
import time
import uuid

os.environ.setdefault("DATABASE_URL", f"sqlite+aiosqlite:///{tempfile.gettempdir()}/lp_smoke_{uuid.uuid4().hex}.db")
os.environ["PSEUDOGRAM_API_KEY"] = "smoke-secret"
os.environ["REQUIRE_SIGNATURE"] = "1"
os.environ["RUN_WORKERS"] = "0"          # no outbound sends in the smoke test
os.environ["ADMIN_TOKEN"] = "smoke"

from fastapi.testclient import TestClient  # noqa: E402

from app.ingest import process_pending  # noqa: E402
from app.main import app  # noqa: E402

SECRET = "smoke-secret"
failures: list[str] = []


def check(label: str, actual, expected) -> None:
    ok = actual == expected
    print(f"  {'PASS' if ok else 'FAIL'}  {label}: got {actual!r}, want {expected!r}")
    if not ok:
        failures.append(label)


def signed(client: TestClient, payload: dict, secret: str = SECRET):
    body = json.dumps(payload).encode()
    sig = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return client.post(
        "/webhook",
        content=body,
        headers={"Content-Type": "application/json", "X-PseudoGram-Signature": f"sha256={sig}"},
    )


def comment_event(event_id: str, comment_id: str, user_id: str, text: str) -> dict:
    return {
        "event_id": event_id,
        "event_type": "comment.created",
        "sent_at": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
        "data": {
            "comment_id": comment_id,
            "post_id": "post_smoke",
            "text": text,
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
            "from": {"user_id": user_id, "username": f"{user_id}_handle"},
        },
    }


def main() -> int:
    with TestClient(app) as client:
        print("\n[1] POST /rules — contract shape")
        r = client.post("/rules", json={"keyword": "PRICE", "dm_message": "Here's the price list"})
        check("status", r.status_code, 201)
        body = r.json()
        check("keys", sorted(body.keys()), ["dm_message", "keyword", "rule_id"])
        check("keyword echoed", body["keyword"], "PRICE")

        print("\n[2] duplicate keyword does not create a second rule")
        client.post("/rules", json={"keyword": "price", "dm_message": "Updated price list"})
        check("rule count", len(client.get("/rules").json()["rules"]), 1)

        print("\n[3] forged and unsigned webhooks are rejected (Part B)")
        good = comment_event("evt_sig", "cmt_sig", "usr_sig", "PRICE please")
        raw = json.dumps(good).encode()
        check("no signature", client.post("/webhook", content=raw).status_code, 401)
        check(
            "wrong secret",
            client.post("/webhook", content=raw, headers={
                "X-PseudoGram-Signature": "sha256=" + hmac.new(b"wrong", raw, hashlib.sha256).hexdigest()
            }).status_code,
            401,
        )
        tampered = raw.replace(b"PRICE", b"PRICEX")
        check(
            "body tampered after signing",
            client.post("/webhook", content=tampered, headers={
                "X-PseudoGram-Signature": "sha256=" + hmac.new(SECRET.encode(), raw, hashlib.sha256).hexdigest()
            }).status_code,
            401,
        )

        print("\n[4] matching: case-insensitive, anywhere in the text")
        for i, text in enumerate(["price?", "what is the PrIcE", "#price!!", "no keyword here"]):
            check(f"accepted {text!r}", signed(client, comment_event(f"evt_m{i}", f"cmt_m{i}", f"usr_m{i}", text)).status_code, 200)
        client.portal.call(process_pending)
        detail = client.get("/stats/detail").json()
        check("tasks created for 3 of 4", detail["tasks"]["pending"], 3)

        print("\n[5] the same event redelivered 5 times sends once")
        event = comment_event("evt_dupe", "cmt_dupe", "usr_dupe", "PRICE now")
        for _ in range(5):
            signed(client, event)
        client.portal.call(process_pending)
        detail = client.get("/stats/detail").json()
        check("pending tasks", detail["tasks"]["pending"], 4)
        check("redelivery duplicates", detail["duplicates"]["redelivered_events"], 4)

        print("\n[6] same user, different comment, same rule -> blocked as a repeat")
        signed(client, comment_event("evt_again", "cmt_again", "usr_dupe", "price again"))
        client.portal.call(process_pending)
        detail = client.get("/stats/detail").json()
        check("repeat duplicates", detail["duplicates"]["repeat_comments"], 1)
        check("pending unchanged", detail["tasks"]["pending"], 4)

        print("\n[7] comment.deleted cancels a task that has not been sent")
        signed(client, comment_event("evt_del", "cmt_del", "usr_del", "PRICE?"))
        client.portal.call(process_pending)
        signed(client, {"event_id": "evt_del_2", "event_type": "comment.deleted",
                        "sent_at": "now", "data": {"comment_id": "cmt_del"}})
        client.portal.call(process_pending)
        detail = client.get("/stats/detail").json()
        check("cancelled_before_send", detail["deletions"]["cancelled_before_send"], 1)
        # The task row is removed, not tombstoned, so the queue returns to 4 and
        # the (user, rule) slot is free if that user comments the keyword again.
        check("pending back to 4", detail["tasks"]["pending"], 4)

        print("\n[8] deletion arriving BEFORE creation is still honoured")
        signed(client, {"event_id": "evt_early_del", "event_type": "comment.deleted",
                        "sent_at": "now", "data": {"comment_id": "cmt_early"}})
        client.portal.call(process_pending)
        signed(client, comment_event("evt_early_new", "cmt_early", "usr_early", "PRICE!"))
        client.portal.call(process_pending)
        detail = client.get("/stats/detail").json()
        check("skipped, no task created", detail["deletions"]["skipped_deleted_first"], 1)
        check("pending still 4", detail["tasks"]["pending"], 4)

        print("\n[9] GET /stats — exactly the four contract keys")
        stats = client.get("/stats").json()
        check("keys", sorted(stats.keys()), ["duplicates_blocked", "failed", "queued", "sent"])
        check("sent (nothing delivered yet)", stats["sent"], 0)
        check("queued", stats["queued"], 4)
        check("duplicates_blocked", stats["duplicates_blocked"], 5)
        check("all values are ints", all(isinstance(v, int) for v in stats.values()), True)

    print("\n" + ("ALL SMOKE CHECKS PASSED" if not failures else f"{len(failures)} FAILED: {failures}"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
