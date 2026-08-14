"""Inbound webhook authentication (Part B).

`X-PseudoGram-Signature: sha256=<hex>` is an HMAC-SHA256 of the **raw request
body**, keyed with our PseudoGram API key.

The one thing that must never happen here: verifying against a re-serialised
body. `json.dumps(await request.json())` produces different bytes than the
sender signed -- key order, separator spacing and unicode escaping all differ --
so every signature would fail and it would look like the sender was broken. The
raw bytes are read once in the route and passed to both this verifier and the
parser.
"""

from __future__ import annotations

import hashlib
import hmac

from .config import settings


class SignatureResult:
    __slots__ = ("ok", "reason")

    def __init__(self, ok: bool, reason: str = "") -> None:
        self.ok = ok
        self.reason = reason

    def __bool__(self) -> bool:
        return self.ok


def compute_signature(body: bytes, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


def verify_signature(body: bytes, header_value: str | None, secret: str | None = None) -> SignatureResult:
    """Constant-time check of the `sha256=<hex>` header against the raw body."""
    secret = settings.api_key if secret is None else secret

    if not secret:
        # No key configured. Refusing to accept anything would make the service
        # undebuggable before the key is issued, so this is surfaced as an
        # explicit reason and the route decides based on REQUIRE_SIGNATURE.
        return SignatureResult(False, "no_secret_configured")

    if not header_value:
        return SignatureResult(False, "missing_signature")

    value = header_value.strip()
    if value.lower().startswith("sha256="):
        provided = value[7:]
    else:
        # Tolerate a bare hex digest; reject anything else outright.
        provided = value
    provided = provided.strip()

    if len(provided) != 64:
        return SignatureResult(False, "malformed_signature")

    expected = compute_signature(body, secret)

    # compare_digest, never `==`: `==` short-circuits on the first differing
    # byte and leaks how much of a forged prefix was correct.
    if hmac.compare_digest(expected, provided.lower()):
        return SignatureResult(True)
    return SignatureResult(False, "signature_mismatch")
