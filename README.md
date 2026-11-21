# seven-tv-events

Unofficial async Python client for the [7TV](https://7tv.app) [EventAPI](https://7tv.app/api/docs#EventAPI) (v3) over WebSocket: get live 7TV emote set, user, and cosmetic updates for Twitch channels and bots.

- Typed models for every payload (`Hello`, `Dispatch`, `ChangeMap`, `ChangeField`, `Ack`, …)
- Subscriptions are remembered and survive reconnects
- Automatic reconnect with backoff, honoring each close code's reconnect policy
- Session **resume** after drops (falls back to re-subscribing if the resume fails)
- Heartbeat watchdog: a connection silent for 3 heartbeat intervals is considered dead
- Local guards against `4009 Already Subscribed`, `4010 Not Subscribed`, and the subscription limit

This project is not affiliated with or endorsed by 7TV.

## 📦 Installation

Requires Python 3.14+. The package is not on PyPI yet, so install it from GitHub:

```console
pip install git+https://github.com/Aluerie/seven-tv-events
uv add git+https://github.com/Aluerie/seven-tv-events
```

## 🚀 Usage

```python
import asyncio
import seven_tv_events


async def main() -> None:
    client = seven_tv_events.Client()

    @client.on_dispatch("emote_set.update")
    async def on_emote_set_update(event: seven_tv_events.Dispatch) -> None:
        for change in event.body.pushed:
            print("emote added:", change.value["name"])
        for change in event.body.pulled:
            print("emote removed:", change.old_value["name"])

    @client.on_event("ready")
    async def on_ready(hello: seven_tv_events.Hello) -> None:
        print("connected, session", hello.session_id)

    async with client:
        await client.subscribe("emote_set.update", object_id="62cdd34e72a832540de95857")
        await client.wait_closed()  # runs until client.close() or a fatal error


asyncio.run(main())
```

### 📡 Subscriptions

```python
await client.subscribe(seven_tv_events.SubscriptionType.EMOTE_SET_UPDATE, object_id="...")
await client.subscribe("entitlement.create", ctx="channel", platform="TWITCH", id="<twitch user id>")
await client.subscribe("emote.update", {"object_id": "..."})  # conditions as a mapping
await client.unsubscribe("emote.update", object_id="...")
```

`subscribe()` can be called before connecting. Queued subscriptions are sent once the session is ready. When connected, it waits for the server's ack, and raises `SubscriptionError` if the server rejects the subscription. A queued subscription that the server rejects is dropped and logged.

### 👂 Listeners

- `@client.on_dispatch(pattern)` handles dispatches whose type matches a shell-style wildcard (`"emote_set.*"`, `"*"`).
- `@client.on_event(name)` handles lifecycle events: `ready`, `resumed`, `heartbeat`, `ack`, `error`, `reconnect`, `end_of_stream`, `disconnect`.

Handlers run as separate tasks, so a slow or failing handler never blocks the connection. Exceptions are logged to the `seven_tv_events.client` logger.

### 🔁 Iterating instead of callbacks

```python
async with seven_tv_events.Client() as client:
    stream = client.dispatches("emote_set.*")  # create it first: it buffers from now on
    await client.subscribe("emote_set.*", object_id="...")
    async for event in stream:
        actor = event.body.actor  # typed seven_tv_events.User
        print(event.type, "by", actor.display_name if actor else "?")
```

Each `dispatches()` call gets its own buffer, filled from the moment it's created. Iteration ends when the client stops. Pass `maxsize=` to bound the buffer; the oldest events are dropped when the consumer falls behind.

### ▶️ Running the client

| Style | Use |
| --- | --- |
| `async with Client() as c:` | connects, waits until ready, closes on exit |
| `await c.start()` / `await c.close()` | the same, run in the background |
| `await c.run()` | run in the foreground until `close()` |

Pass `session=` to share an existing `aiohttp.ClientSession` (for example a bot's).

`ClosedError` is raised when the server closes with a code that must not be retried (`4001`–`4004`). Those codes indicate a client bug, except for a `4002` caused by an invalid subscription: the client drops that subscription and reconnects. `ClosedError` is also raised when the server closes the connection with the same subscription-related code (`4009`, `4010`, `4011` or `1008`) 5 times in a row without the connection becoming stable in between. A close that the client can attribute to one rejected subscription drops that subscription and doesn't count toward this limit.

If `start()` is cancelled while it waits (for example by `asyncio.timeout`), the client is closed.

### 🌐 REST helpers

`seven_tv_events.rest` covers the few REST API lookups you usually need alongside the EventAPI. It isn't a full API client.

```python
from seven_tv_events import rest

# Twitch user ID -> 7TV user and the emote set active on their channel
conn = await rest.fetch_user_connection("twitch", 1277023540)
await client.subscribe("emote_set.update", object_id=conn.emote_set_id)

# The full set, to build the state that dispatches then update
emote_set = await rest.fetch_emote_set(conn.emote_set_id)
for emote in emote_set.emotes:
    print(emote.name, emote.host_url, "(alias of %s)" % emote.original_name if emote.aliased else "")
```

Both accept `session=` to reuse an `aiohttp.ClientSession`. They raise `NotFoundError` for unknown IDs and `RESTError` for other API errors. `Emote.from_json()` also parses the emote values in `emote_set.update` dispatches.

More complete, runnable examples are in [examples/](examples/).

## 💻 CLI

```console
seven-tv-events emote_set.update:object_id=62cdd34e72a832540de95857 "entitlement.create:ctx=channel,platform=TWITCH,id=..."
```

Prints every dispatch as a JSON line on stdout. Exits with status 1 on a fatal error.

## 📝 Notes on the live server

These behaviors are not in the official docs:

- `Hello` also carries `instance` (`{"name": ..., "population": ...}`).
- Ack echoes for `SUBSCRIBE` add a subscription `id` and rewrite a legacy hex ObjectID in `object_id` into a ULID. The client applies the same conversion when you subscribe (`seven_tv_events.legacy_id_to_ulid`), so the IDs in acks and events always match the ones you subscribed with. Other conditions are not converted (e.g. `entitlement.create`'s `id` is echoed unchanged).
- Next to `object_id`, unknown condition keys are silently dropped from the ack echo.
- The server silently ignores a subscription it already has (no ack, and no `4009 Already Subscribed`), including one that only differs in keys it drops. The client never sends an exact duplicate, but a `subscribe()` that differs from an existing subscription only in such keys waits for `ack_timeout` and then assumes it succeeded.
- An invalid condition value (e.g. a hex ID as `entitlement.create`'s `id`) gets an `ERROR` (op 6, `"Invalid Subscription Condition"`) instead of an ack, followed by `END_OF_STREAM` with `4002 Invalid Payload` and a `1008` close. The client drops that subscription and reconnects.
- Some conditions (e.g. `host_id`) make the server end the connection: it sends `END_OF_STREAM` with `4002 Invalid Payload`, then a WebSocket close with the standard code `1008`. The client drops the offending subscription and reconnects instead of treating `4002` as fatal.
- Wildcard types such as `cosmetic.*` and `emote_set.*` are accepted.

## 🛠️ Development

```console
uv run pytest      # tests run against an in-process fake EventAPI server
uv run ruff check
uv run ty check    # type checking
```

## 🤖 AI disclosure

This repository is a personal experiment. Until recently I hadn't used AI for coding *at all*. In October 2026 I started using it to review my code, but I never let it write anything from scratch. This project (first commit: November 2026) is the opposite: its architecture, code, tests, examples, and documentation were all produced by [Claude Code](https://claude.com/claude-code), with me directing the work.

The code is tested against an in-process fake server, and its handling of undocumented server behavior was checked against the live 7TV EventAPI. Even so, review it before you rely on it, and please report anything that looks wrong.
