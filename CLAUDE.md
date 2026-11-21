# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`seven-tv-events` is an async Python client for the 7TV EventAPI v3 over WebSocket, plus a few REST lookups. It targets Python 3.14+ (developed on 3.15) and is managed with uv. Its only runtime dependency is aiohttp.

## Commands

```console
uv run pytest                                                 # all tests (in-process fake server, no network)
uv run pytest tests/test_client.py::test_connect_and_ready    # a single test
uv run ruff check --fix && uv run ruff format
uv run ty check                                               # type checking (also covers examples/)
uv run seven-tv-events emote_set.update:object_id=<id>        # CLI: prints dispatches as JSON lines
```

`uv sync` installs the `examples` group (twitchio) by default so that `ty` can resolve the example imports.

## Architecture

The public API is whatever `src/seven_tv_events/__init__.py` re-exports. Keep `__all__` there and the README in sync when that API changes.

### The connection loop (`client.py`)

All the complicated logic lives in `Client`.

- **Desired state vs. server state.** `_desired` (an insertion-ordered dict used as a set) holds what the user asked for. `_active` holds what the current server session holds. `_sync()` diffs the two and sends SUBSCRIBE/UNSUBSCRIBE one command at a time under `_sync_lock`, waiting for each ack. `subscribe()` and `unsubscribe()` only edit `_desired`. They call `_sync()` only when the client is ready. Before that, the handshake picks them up.
- **Connection lifecycle.** `_run_loop` calls `_run_once`, which always ends by raising `_Disconnected(code, resumable=...)`. `_run_loop` then chooses the next step from `CloseCode.reconnect`, a `ReconnectPolicy` (YES / NO / MAYBE / LATER) defined in `enums.py`:
  - NO raises `ClosedError`.
  - LATER waits at least 30 s.
  - A connection that stayed up for `stable_after` reconnects within a second (a small random delay, so clients of a restarting server don't all reconnect at once). Anything else uses jittered exponential backoff.
  - 4009 and 4010 (already subscribed / not subscribed) are treated as local desync. The loop starts a fresh, non-resumed session instead of giving up.
  - If the same code from `_GIVE_UP_WHEN_REPEATED` (4009, 4010, 4011, 1008) closes 5 unstable connections in a row, the loop raises `ClosedError` instead of retrying forever.
- **Rejected subscriptions.** The live server answers an invalid subscription with an ERROR (op 6), or by ending the connection (END_OF_STREAM 4002 followed by a 1008 close, or 4011). `_reject_pending` attributes the refusal to the oldest pending command, removes a rejected SUBSCRIBE from `_desired`, and records it in `_rejected` so that `subscribe()` can raise `SubscriptionError`. On a close, this runs in the reader before the handshake task is cancelled, and a close that rejected a subscription is reconnected (even 4002) without counting toward the repeat limit.
  - ERRORs don't name the command they answer, so only those whose message mentions "subscription" count as rejections.
  - The live server follows such an ERROR with END_OF_STREAM 4002 and a 1008 close. `_rejected_by_error` (reset per connection) makes that close count as already handled, so a second, valid subscription sent in between isn't blamed too.
  - The live server never acks a duplicate subscription. Don't build logic on `_stale_acks` predicting the next server answer: a silently ignored duplicate also leaves a stale count behind.
- **Handshake.** After HELLO, the reader task and the `_handshake` task run concurrently. `_handshake` tries RESUME with `_resume_session_id`. If the resume fails, it clears `_active` so `_sync()` re-subscribes everything. `_resume_session_id` only moves to the new session after the resume result is known, so a drop mid-handshake still resumes the old session.
- **Ack matching.** The server rewrites IDs in its ack echoes, so `_resolve_ack` matches on content when it can and otherwise in FIFO order. When a command times out, the client assumes it succeeded and increments `_stale_acks`, so the late ack is discarded instead of shifting every later match.
- **Watchdog.** A connection that receives nothing for 3 × `heartbeat_interval` + `heartbeat_grace` seconds is closed as dead.
- **Handler isolation.** Handlers run as separate tasks (`_spawn`), and exceptions inside them are logged. `_handle` errors are caught per message, so a malformed payload never kills the connection. `DispatchStream`s are buffered queues held in a `WeakSet` and ended when the loop stops.

### Models (`models.py`)

These are dataclasses with `from_json` classmethods. Every model keeps the untouched payload in `raw` so that new API fields stay reachable. `Subscription` is hashable and normalizes its conditions: it sorts them and converts a legacy 24-hex ObjectID in `object_id` (and only there) to a ULID via `legacy_id_to_ulid`, because the live server does the same in its acks. Without that conversion, IDs in acks would not match the IDs the client subscribed with. `Emote.from_json` parses both REST emote-set entries and `emote_set.update` dispatch values.

### REST (`rest.py`)

The REST module deliberately covers only two lookups: `fetch_user_connection` and `fetch_emote_set`. Don't grow it into a full API client.

## Testing

- `tests/fake_server.py` (`FakeEventAPI`) is an aiohttp server on a random local port. It mimics the real protocol: sessions, RESUME, 4009/4010 closes, acks. Flags like `allow_resume` and `ack_commands` let tests simulate misbehavior. Client tests must use it, never the live endpoint.
- `pytest-asyncio` runs in `asyncio_mode = "auto"`, so async tests need no decorator. Use the `eventually(predicate)` helper in `test_client.py` to wait on async effects.
- `tests/data/*.jsonl` and `*.json` are payloads captured from the live 7TV API. Tests assert exact values from them, so don't hand-edit these files.

## Conventions

- Ruff runs in preview mode with line length 120. Rules are ignored by name, not by code, in `pyproject.toml`. Keep the explanatory comment next to each ignored rule.
- The code uses modern Python syntax throughout: `type` aliases, PEP 695 generics, `match` statements, `Self`.
- Behaviors of the live server that the official docs don't cover are listed in the README under "Notes on the live server". Add to that section when you find new ones.
