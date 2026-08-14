"""Durable storage.

One deliberate constraint runs through this file: **the DML is identical on
SQLite and Postgres.** Only the DDL branches. That is what lets the test suite
run against an in-process SQLite file while production runs Neon Postgres, with
no "works locally, races in prod" gap between them.

Timestamps are stored as float epoch seconds. No timezone, no driver-specific
datetime adaptation, and directly comparable in SQL.
"""

from __future__ import annotations

import sqlite3
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Sequence

from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from .config import settings

_engine: AsyncEngine | None = None
_dialect: str = "sqlite"


def now() -> float:
    """Single source of wall-clock time, so tests can reason about it."""
    return time.time()


def normalise_url(url: str) -> str:
    """Accept the URL forms hosting providers actually hand you.

    Render and Neon emit `postgres://` or `postgresql://`; SQLAlchemy's async
    engine needs an explicit async driver. asyncpg also rejects libpq-only query
    parameters like `sslmode`, so those are stripped and re-expressed as connect
    args in `_connect_args()`.
    """
    if url.startswith("postgres://"):
        url = "postgresql+asyncpg://" + url[len("postgres://"):]
    elif url.startswith("postgresql://"):
        url = "postgresql+asyncpg://" + url[len("postgresql://"):]
    if url.startswith("sqlite://") and "+aiosqlite" not in url:
        url = url.replace("sqlite://", "sqlite+aiosqlite://", 1)

    if "postgresql+asyncpg" in url and "?" in url:
        base, _, query = url.partition("?")
        keep = [
            part for part in query.split("&")
            if part and part.split("=")[0] not in {"sslmode", "channel_binding", "options"}
        ]
        url = base + ("?" + "&".join(keep) if keep else "")
    return url


def _connect_args(url: str) -> dict[str, Any]:
    if url.startswith("postgresql+asyncpg"):
        # Neon and Render both require TLS; asyncpg takes the libpq mode name.
        return {"ssl": "require", "server_settings": {"application_name": "linkplease"}}
    return {}


def dialect() -> str:
    return _dialect


def get_engine() -> AsyncEngine:
    if _engine is None:
        raise RuntimeError("database not initialised; call init_db() first")
    return _engine


# --------------------------------------------------------------------------
# Schema
# --------------------------------------------------------------------------

# `{PK}` autoincrementing integer primary key, `{F8}` double precision float.
_DDL_TYPES = {
    "sqlite": {"PK": "INTEGER PRIMARY KEY AUTOINCREMENT", "F8": "REAL", "I8": "INTEGER"},
    "postgresql": {"PK": "BIGSERIAL PRIMARY KEY", "F8": "DOUBLE PRECISION", "I8": "BIGINT"},
}

_SCHEMA = [
    # A rule is "keyword -> dm_message". keyword_lc is the pre-lowered form the
    # matcher compares against, so matching never lowercases inside the hot loop.
    """
    CREATE TABLE IF NOT EXISTS rules (
        rule_id     TEXT PRIMARY KEY,
        keyword     TEXT NOT NULL,
        keyword_lc  TEXT NOT NULL,
        dm_message  TEXT NOT NULL,
        active      {I8} NOT NULL DEFAULT 1,
        created_at  {F8} NOT NULL,
        -- One rule per distinct keyword. Matching is case-insensitive, so
        -- "PRICE" and "price" are the same rule; without this, posting the
        -- keyword twice would create two rules and DM the same user twice for
        -- what is really one intent.
        UNIQUE (keyword_lc)
    )
    """,

    # The durable inbox. One row per *physical HTTP delivery*, not per event_id:
    # a redelivery is a distinct row on purpose, because "how many times were we
    # told this" is the input to the duplicate counter.
    """
    CREATE TABLE IF NOT EXISTS deliveries (
        id           {PK},
        event_id     TEXT NOT NULL,
        event_type   TEXT,
        payload      TEXT NOT NULL,
        received_at  {F8} NOT NULL,
        processed_at {F8}
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_deliveries_pending ON deliveries (id) WHERE processed_at IS NULL",
    "CREATE INDEX IF NOT EXISTS ix_deliveries_event ON deliveries (event_id)",

    # Comment facts, plus the deletion tombstone. A `comment.deleted` for a
    # comment we have never seen still writes a row here with deleted=1, so a
    # deletion that overtakes its creation is still honoured.
    """
    CREATE TABLE IF NOT EXISTS comments (
        comment_id    TEXT PRIMARY KEY,
        post_id       TEXT,
        user_id       TEXT,
        username      TEXT,
        text          TEXT,
        created_at    TEXT,
        first_seen_at {F8} NOT NULL,
        deleted       {I8} NOT NULL DEFAULT 0,
        deleted_at    {F8}
    )
    """,

    # The outbox. UNIQUE(user_id, rule_id) is the entire duplicate-suppression
    # mechanism -- see ingest.py. status is one of:
    #   pending    queued locally, waiting for the rate limiter
    #   in_flight  a POST /v1/dm/send is on the wire right now
    #   accepted   API returned 202; NOT yet confirmed delivered
    #   delivered  GET /v1/dm/{id} confirmed delivery      -> counts as `sent`
    #   failed     retry budget exhausted, or a permanent 400
    # A task cancelled by comment.deleted is deleted outright rather than given
    # a status: the row holds the UNIQUE slot, and a tombstone there would block
    # the user forever. See ingest._handle_deleted.
    """
    CREATE TABLE IF NOT EXISTS dm_tasks (
        id              {PK},
        user_id         TEXT NOT NULL,
        rule_id         TEXT NOT NULL,
        comment_id      TEXT,
        username        TEXT,
        message         TEXT NOT NULL,
        status          TEXT NOT NULL,
        attempts        {I8} NOT NULL DEFAULT 0,
        resends         {I8} NOT NULL DEFAULT 0,
        generation      {I8} NOT NULL DEFAULT 0,
        checks          {I8} NOT NULL DEFAULT 0,
        dm_id           TEXT,
        next_attempt_at {F8} NOT NULL DEFAULT 0,
        next_check_at   {F8},
        last_error      TEXT,
        created_at      {F8} NOT NULL,
        updated_at      {F8} NOT NULL,
        UNIQUE (user_id, rule_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_tasks_status ON dm_tasks (status)",
    "CREATE INDEX IF NOT EXISTS ix_tasks_pending ON dm_tasks (next_attempt_at) WHERE status = 'pending'",
    "CREATE INDEX IF NOT EXISTS ix_tasks_accepted ON dm_tasks (next_check_at) WHERE status = 'accepted'",
    "CREATE INDEX IF NOT EXISTS ix_tasks_comment ON dm_tasks (comment_id)",

    # Rate-limiter window, persisted. One row per outbound POST /v1/dm/send,
    # written *before* the call. A restart therefore cannot forget recent sends
    # and burst through the limit.
    """
    CREATE TABLE IF NOT EXISTS send_log (
        id {PK},
        ts {F8} NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_send_log_ts ON send_log (ts)",

    # Monotonic counters for things not derivable from table state.
    """
    CREATE TABLE IF NOT EXISTS counters (
        name  TEXT PRIMARY KEY,
        value {I8} NOT NULL DEFAULT 0
    )
    """,
]

COUNTERS = (
    "dup_redelivery",      # same comment matched the same rule again (event redelivered)
    "dup_repeat_user",     # different comment, same (user, rule) -> already DMed
    "deleted_skipped",     # comment.deleted arrived before comment.created
    "cancelled_pending",   # comment.deleted cancelled a task still waiting to send
    "signature_rejected",  # webhook rejected: bad or missing HMAC
    "malformed_rejected",  # webhook rejected: unparseable body
    "events_unusable",     # accepted, but no user_id to DM / no comment_id to key on
    "events_unknown_type", # an event_type we have no handler for
    "resends_after_failure",  # deliveries confirmed failed and sent again
    "events_error",        # quarantined: raised twice, dropped to unblock the inbox
)


async def init_db() -> None:
    global _engine, _dialect

    url = normalise_url(settings.database_url)
    is_sqlite = url.startswith("sqlite")
    _dialect = "sqlite" if is_sqlite else "postgresql"

    if is_sqlite and sqlite3.sqlite_version_info < (3, 35, 0):
        raise RuntimeError(
            f"SQLite {sqlite3.sqlite_version} is too old; this app uses "
            "INSERT ... ON CONFLICT DO NOTHING RETURNING (needs 3.35+)."
        )

    engine = create_async_engine(
        url,
        echo=False,
        pool_pre_ping=True,
        connect_args=_connect_args(url),
        **({"pool_size": 10, "max_overflow": 10, "pool_recycle": 280} if not is_sqlite else {}),
    )

    if is_sqlite:
        @event.listens_for(engine.sync_engine, "connect")
        def _sqlite_pragmas(dbapi_conn, _record):  # pragma: no cover - driver glue
            cur = dbapi_conn.cursor()
            # WAL lets the webhook keep inserting while a worker reads.
            cur.execute("PRAGMA journal_mode=WAL")
            # NORMAL still fsyncs the WAL at checkpoints; the durability we care
            # about is process crash, not machine power loss.
            cur.execute("PRAGMA synchronous=NORMAL")
            # Wait rather than raising "database is locked" under the 50 rps burst.
            cur.execute("PRAGMA busy_timeout=10000")
            cur.close()

    _engine = engine

    types = _DDL_TYPES[_dialect]
    async with engine.begin() as conn:
        for stmt in _SCHEMA:
            await conn.execute(text(stmt.format(**types)))
        for name in COUNTERS:
            await conn.execute(
                text("INSERT INTO counters (name, value) VALUES (:n, 0) ON CONFLICT (name) DO NOTHING"),
                {"n": name},
            )


async def close_db() -> None:
    global _engine
    if _engine is not None:
        await _engine.dispose()
        _engine = None


# --------------------------------------------------------------------------
# Query helpers
# --------------------------------------------------------------------------

@asynccontextmanager
async def tx() -> AsyncIterator[AsyncConnection]:
    """A single committed transaction. Rolls back on any exception."""
    async with get_engine().begin() as conn:
        yield conn


async def fetch_all(sql: str, params: dict[str, Any] | None = None) -> Sequence[Any]:
    async with get_engine().connect() as conn:
        result = await conn.execute(text(sql), params or {})
        return result.mappings().all()


async def fetch_one(sql: str, params: dict[str, Any] | None = None) -> Any | None:
    async with get_engine().connect() as conn:
        result = await conn.execute(text(sql), params or {})
        return result.mappings().first()


async def execute(sql: str, params: dict[str, Any] | None = None) -> int:
    async with get_engine().begin() as conn:
        result = await conn.execute(text(sql), params or {})
        return result.rowcount


async def bump_counter(conn: AsyncConnection, name: str, delta: int = 1) -> None:
    """Increment inside the caller's transaction, so a counter can never be
    incremented without the side effect it describes also committing.

    Upsert rather than UPDATE: a counter name added in a later release must not
    silently no-op against a database created by an earlier one.
    """
    if delta == 0:
        return
    await conn.execute(
        text(
            "INSERT INTO counters (name, value) VALUES (:n, :d) "
            "ON CONFLICT (name) DO UPDATE SET value = counters.value + :d"
        ),
        {"d": delta, "n": name},
    )


async def counters() -> dict[str, int]:
    rows = await fetch_all("SELECT name, value FROM counters")
    return {row["name"]: int(row["value"]) for row in rows}
