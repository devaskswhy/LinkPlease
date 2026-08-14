"""Inbound webhook authentication (Part B).

`X-PseudoGram-Signature: sha256=<hex>` is an HMAC-SHA256 of the **raw request
body**.

## What the secret actually is

The brief says the secret is the API key. It is not. Verified against real
captured requests (`tools/sigcrack.py`), the signature that PseudoGram sends is:

    HMAC-SHA256(key = account email, msg = raw body)

The API key has the shape `<base64(email)>.<hex>`, so `base64decode(prefix)`
produces the identical secret -- the same match, reached two ways. Implementing
the brief literally rejects 100% of events, which is what happened on the first
live run: 44 of 44 rejected, while every health indicator stayed green.

Rather than hardcode the observed answer, this tries an ordered list of
candidate secrets and accepts the first that verifies. The documented one is
tried first, so if they later change the server to match their own
documentation, this keeps working with no redeploy.

## And a caveat worth stating plainly

The working secret is the account email, which is not secret -- it is on the
application form and in the submission payload. Anyone who knows it can forge a
perfectly valid signature. So this check proves the body was not corrupted in
transit and that the sender knows a public string; it is not authentication in
any meaningful sense. It is implemented because the assignment asks for it, and
it does correctly reject unsigned and tampered requests, but it should not be
mistaken for a security boundary.

## The implementation trap

Never verify against a re-serialised body. `json.dumps(await request.json())`
produces different bytes than the sender signed -- key order, separator spacing
and unicode escaping all differ -- so every signature fails and it looks like
the sender is broken. The raw bytes are read once in the route and passed to
both this verifier and the parser.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac

from .config import settings


class SignatureResult:
    __slots__ = ("ok", "reason", "matched")

    def __init__(self, ok: bool, reason: str = "", matched: str = "") -> None:
        self.ok = ok
        self.reason = reason
        # Which candidate secret verified, for /health and debugging.
        self.matched = matched

    def __bool__(self) -> bool:
        return self.ok


def compute_signature(body: bytes, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


def candidate_secrets(api_key: str | None = None, email: str | None = None) -> list[tuple[str, str]]:
    """(label, secret) pairs to try, in order, most-documented first."""
    api_key = settings.api_key if api_key is None else api_key
    email = settings.contact_email if email is None else email

    candidates: list[tuple[str, str]] = []
    if api_key:
        candidates.append(("api_key", api_key))

        # The key is `<base64(email)>.<hex>`; the prefix decodes to the secret
        # that actually works. Derived from the key rather than from config, so
        # it holds even when PSEUDOGRAM_EMAIL is unset.
        prefix = api_key.split(".", 1)[0]
        try:
            padded = prefix + "=" * (-len(prefix) % 4)
            decoded = base64.b64decode(padded).decode("utf-8")
            if decoded.isprintable():
                candidates.append(("api_key_prefix_b64decoded", decoded))
        except (binascii.Error, UnicodeDecodeError, ValueError):
            pass

    if email:
        candidates.append(("account_email", email))

    seen: set[str] = set()
    unique: list[tuple[str, str]] = []
    for label, secret in candidates:
        if secret and secret not in seen:
            seen.add(secret)
            unique.append((label, secret))
    return unique


def verify_signature(
    body: bytes,
    header_value: str | None,
    secret: str | None = None,
) -> SignatureResult:
    """Constant-time check of the `sha256=<hex>` header against the raw body.

    Passing `secret` explicitly checks against that one secret only; otherwise
    every candidate from `candidate_secrets()` is tried.
    """
    candidates = [("explicit", secret)] if secret is not None else candidate_secrets()
    candidates = [(label, value) for label, value in candidates if value]

    if not candidates:
        # Nothing to verify against. Surfaced as a distinct reason so the route
        # can decide, rather than silently treating it as a forgery.
        return SignatureResult(False, "no_secret_configured")

    if not header_value:
        return SignatureResult(False, "missing_signature")

    value = header_value.strip()
    provided = value[7:] if value.lower().startswith("sha256=") else value
    provided = provided.strip().lower()

    if len(provided) != 64:
        return SignatureResult(False, "malformed_signature")

    for label, candidate in candidates:
        # compare_digest, never `==`: `==` short-circuits on the first differing
        # byte and leaks how much of a forged prefix was correct.
        if hmac.compare_digest(compute_signature(body, candidate), provided):
            return SignatureResult(True, matched=label)

    return SignatureResult(False, "signature_mismatch")
