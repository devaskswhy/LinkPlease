"""Client for the PseudoGram mock API.

Everything hostile about the upstream is classified here, in one place, so the
sender loop reads as policy rather than as a pile of status-code branches.

Response taxonomy for POST /v1/dm/send:
    ACCEPTED   202 -- accepted, *not* delivered. Reconciliation decides that.
    RATE_LIMIT 429 -- back off by Retry-After. Not the task's fault: it does
                      not consume a retry attempt.
    PERMANENT  400 -- malformed payload. Retrying cannot help; fail it now.
    TRANSIENT  5xx, timeouts, connection errors -- safe to retry, and safe
                      *specifically because* every attempt carries the same
                      Idempotency-Key, so a 500 that actually landed does not
                      become a second DM.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

import httpx

from .config import settings


class SendOutcome(str, Enum):
    ACCEPTED = "accepted"
    RATE_LIMIT = "rate_limit"
    PERMANENT = "permanent"
    TRANSIENT = "transient"


@dataclass(slots=True)
class SendResult:
    outcome: SendOutcome
    dm_id: str | None = None
    status_code: int | None = None
    retry_after: float | None = None
    detail: str = ""


@dataclass(slots=True)
class DMStatus:
    dm_id: str
    status: str            # queued | delivered | failed
    raw: dict[str, Any]

    @property
    def terminal(self) -> bool:
        return self.status in {"delivered", "failed"}


class PseudoGramClient:
    def __init__(self, base_url: str | None = None, api_key: str | None = None) -> None:
        self.base_url = (base_url or settings.base_url).rstrip("/")
        self.api_key = api_key if api_key is not None else settings.api_key
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(settings.http_timeout, connect=10.0),
            headers={"X-API-Key": self.api_key, "Content-Type": "application/json"},
            # The upstream is a single free-tier box; a small pool keeps us from
            # opening a connection per in-flight reconciliation check.
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        )

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("PseudoGramClient not started")
        return self._client

    # -- DM send ----------------------------------------------------------

    async def send_dm(
        self,
        recipient_user_id: str,
        message: str,
        comment_id: str | None,
        idempotency_key: str,
    ) -> SendResult:
        payload = {"recipient_user_id": recipient_user_id, "message": message}
        if comment_id:
            payload["comment_id"] = comment_id

        try:
            response = await self.client.post(
                "/v1/dm/send",
                json=payload,
                headers={"Idempotency-Key": idempotency_key},
            )
        except httpx.HTTPError as exc:
            # Includes read timeouts, where the request may well have been
            # accepted upstream. The idempotency key makes the retry safe.
            return SendResult(SendOutcome.TRANSIENT, detail=f"{type(exc).__name__}: {exc}")

        if response.status_code in (200, 201, 202):
            body = _json_or_empty(response)
            dm_id = body.get("dm_id")
            if not dm_id:
                # Accepted but unusable: no handle to reconcile against. Retry
                # with the same key -- upstream will hand back the original id.
                return SendResult(SendOutcome.TRANSIENT, status_code=response.status_code,
                                  detail="202 without dm_id")
            return SendResult(SendOutcome.ACCEPTED, dm_id=dm_id, status_code=response.status_code)

        if response.status_code == 429:
            return SendResult(
                SendOutcome.RATE_LIMIT,
                status_code=429,
                retry_after=_retry_after(response),
                detail="rate_limited",
            )

        if response.status_code in (400, 404, 422):
            body = _json_or_empty(response)
            return SendResult(SendOutcome.PERMANENT, status_code=response.status_code,
                              detail=str(body.get("detail") or body.get("error") or response.text)[:400])

        # 401/403 land here on purpose: an API key can be corrected without a
        # redeploy, so they stay retryable rather than instantly failing the task.
        return SendResult(SendOutcome.TRANSIENT, status_code=response.status_code,
                          detail=response.text[:400])

    # -- DM status (free: does not count against the rate limit) -----------

    async def get_dm(self, dm_id: str) -> DMStatus | None:
        try:
            response = await self.client.get(f"/v1/dm/{dm_id}")
        except httpx.HTTPError:
            return None
        if response.status_code != 200:
            return None
        body = _json_or_empty(response)
        status = str(body.get("status") or "").lower()
        if not status:
            return None
        return DMStatus(dm_id=dm_id, status=status, raw=body)

    # -- Tooling endpoints -------------------------------------------------

    async def start_simulation(self, webhook_url: str, count: int, duration_seconds: int) -> dict[str, Any]:
        response = await self.client.post(
            "/v1/simulate/start",
            json={"webhook_url": webhook_url, "count": count, "duration_seconds": duration_seconds},
        )
        response.raise_for_status()
        return _json_or_empty(response)

    async def truth(self, run_id: str) -> dict[str, Any]:
        response = await self.client.get(f"/v1/simulate/{run_id}/truth")
        response.raise_for_status()
        return _json_or_empty(response)

    async def admin_dm_sends(self, email: str, since: float = 0) -> dict[str, Any]:
        response = await self.client.get("/v1/admin/dm_sends", params={"email": email, "since": since})
        response.raise_for_status()
        return _json_or_empty(response)


def _json_or_empty(response: httpx.Response) -> dict[str, Any]:
    try:
        body = response.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


def _retry_after(response: httpx.Response) -> float:
    raw = response.headers.get("Retry-After", "")
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return 5.0
    # Clamp: a nonsense value must not park the sender for an hour, and 0 must
    # not turn into a hot loop against a limiter that is still closed.
    return max(1.0, min(value, 120.0))
