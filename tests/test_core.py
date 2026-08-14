"""Unit tests for the pieces whose failure modes are silent.

The end-to-end behaviour is covered by tools/loadtest.py against an independent
truth. These cover the small decisions that a load test would pass over: the
matcher's edge cases, the signature comparison, and the retry policy's mapping
from status code to action.
"""

from __future__ import annotations

import hashlib
import hmac

import pytest

from app.matching import Rule, match_rules, normalise_keyword
from app.pseudogram import SendOutcome, _retry_after
from app.security import compute_signature, verify_signature
from app.sender import backoff_delay, check_delay, idempotency_key

SECRET = "test-secret-key"


def rule(keyword: str) -> Rule:
    return Rule(f"rule_{keyword}", keyword, normalise_keyword(keyword), f"msg for {keyword}")


# --------------------------------------------------------------------------
# Matching
# --------------------------------------------------------------------------

@pytest.mark.parametrize("text", [
    "PRICE", "price", "PrIcE", "price please", "what's the price?",
    "#PRICE", "price!!!", "...price...", "priceless",   # "anywhere", so substrings count
    "🙏 PRICE 🙏", "DM me the\nprice\nplease",
])
def test_matches_anywhere_case_insensitively(text):
    assert match_rules(text, [rule("PRICE")]) != []


@pytest.mark.parametrize("text", ["", "no keyword", "pricy", "p r i c e", "💰", "PRIC"])
def test_non_matches(text):
    assert match_rules(text, [rule("PRICE")]) == []


def test_one_comment_can_match_several_rules():
    rules = [rule("PRICE"), rule("LINK"), rule("COURSE")]
    matched = match_rules("send the PRICE and the link", rules)
    assert [r.keyword for r in matched] == ["PRICE", "LINK"]


def test_match_order_follows_rule_order_not_text_order():
    # Deterministic ordering matters: it decides which DM is queued first.
    rules = [rule("PRICE"), rule("LINK")]
    assert [r.keyword for r in match_rules("link then PRICE", rules)] == ["PRICE", "LINK"]


def test_unicode_case_folding():
    # casefold(), not lower(): the comment stream is full of non-ASCII text.
    assert match_rules("STRASSE", [rule("straße")]) != []


# --------------------------------------------------------------------------
# Signatures (Part B)
# --------------------------------------------------------------------------

def test_valid_signature_accepted():
    body = b'{"event_id":"evt_1","data":{}}'
    sig = compute_signature(body, SECRET)
    assert verify_signature(body, f"sha256={sig}", SECRET).ok


def test_bare_hex_digest_accepted():
    body = b'{"a":1}'
    assert verify_signature(body, compute_signature(body, SECRET), SECRET).ok


def test_uppercase_hex_accepted():
    body = b'{"a":1}'
    assert verify_signature(body, f"sha256={compute_signature(body, SECRET).upper()}", SECRET).ok


@pytest.mark.parametrize("header,reason", [
    (None, "missing_signature"),
    ("", "missing_signature"),
    ("sha256=deadbeef", "malformed_signature"),
    ("sha256=" + "0" * 64, "signature_mismatch"),
])
def test_bad_signatures_rejected(header, reason):
    result = verify_signature(b'{"a":1}', header, SECRET)
    assert not result.ok and result.reason == reason


def test_body_tampering_after_signing_is_caught():
    original = b'{"amount":10}'
    sig = compute_signature(original, SECRET)
    assert not verify_signature(b'{"amount":99}', f"sha256={sig}", SECRET).ok


def test_wrong_secret_rejected():
    body = b'{"a":1}'
    forged = hmac.new(b"attacker-key", body, hashlib.sha256).hexdigest()
    assert not verify_signature(body, f"sha256={forged}", SECRET).ok


def test_signature_is_over_raw_bytes_not_reparsed_json():
    """The classic way to get this wrong: sign the re-serialised dict.

    Same JSON object, different bytes. Only the bytes actually received can
    verify, which is why the route hands `await request.body()` to the verifier
    rather than `json.dumps(await request.json())`.
    """
    received = b'{"b": 2,  "a": 1}'          # sender's spacing and key order
    reserialised = b'{"a": 1, "b": 2}'       # what json.dumps would produce
    sig = compute_signature(received, SECRET)
    assert verify_signature(received, f"sha256={sig}", SECRET).ok
    assert not verify_signature(reserialised, f"sha256={sig}", SECRET).ok


def test_missing_secret_is_reported_distinctly():
    # The route treats this differently from a forgery: it cannot verify at all.
    assert verify_signature(b"{}", "sha256=" + "0" * 64, "").reason == "no_secret_configured"


# --------------------------------------------------------------------------
# Retry and reconciliation policy
# --------------------------------------------------------------------------

def test_idempotency_key_is_stable_per_generation():
    # Retries of one attempt must reuse the key, so a 500 that actually landed
    # returns the original dm_id instead of sending a second DM.
    assert idempotency_key(42, 0) == idempotency_key(42, 0) == "lp-42-g0"


def test_idempotency_key_rotates_with_generation():
    # A resend after a *confirmed failure* must not reuse the key, or the API
    # hands back the same failed dm_id and nothing is ever sent.
    assert idempotency_key(42, 0) != idempotency_key(42, 1)


def test_backoff_grows_and_is_capped():
    from app.config import settings
    previous = 0.0
    for attempt in range(1, 12):
        delay = backoff_delay(attempt)
        assert 0 < delay <= settings.retry_max_delay
        if attempt < 6:
            assert delay >= previous / 2  # jittered, so compare loosely
        previous = delay


def test_backoff_is_jittered():
    # A 500-event burst produces a cohort of tasks; identical backoff would make
    # them retry in lockstep and hammer the same second.
    assert len({round(backoff_delay(4), 6) for _ in range(40)}) > 1


def test_reconcile_check_delay_backs_off_then_settles():
    assert check_delay(0) < check_delay(3) <= 30.0
    from app.config import settings
    assert check_delay(settings.reconcile_max_checks) == 60.0


@pytest.mark.parametrize("header,expected", [
    ("5", 5.0),
    ("0", 1.0),        # never a hot loop against a still-closed limiter
    ("99999", 120.0),  # never park the sender for an hour
    ("banana", 5.0),
    (None, 5.0),
])
def test_retry_after_is_clamped(header, expected):
    class FakeResponse:
        headers = {} if header is None else {"Retry-After": header}
    assert _retry_after(FakeResponse()) == expected


def test_send_outcomes_are_distinct():
    # These four drive every branch in _record_send_result; collapsing any two
    # would silently change the retry policy.
    assert len({o.value for o in SendOutcome}) == 4
