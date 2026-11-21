import asyncio
from collections.abc import AsyncIterator, Callable

import pytest

from seven_tv_events import (
    Client,
    CloseCode,
    ClosedError,
    Dispatch,
    Hello,
    Opcode,
    Subscription,
    SubscriptionError,
    SubscriptionLimitError,
)

from .fake_server import FakeEventAPI

EMOTE_SET = "01G7T02T5G00075A1JAG6YJP2Q"


@pytest.fixture
async def server() -> AsyncIterator[FakeEventAPI]:
    srv = FakeEventAPI()
    await srv.start()
    yield srv
    await srv.stop()


@pytest.fixture
async def client(server: FakeEventAPI) -> AsyncIterator[Client]:
    c = Client(server.url, ack_timeout=2)
    yield c
    await c.close()


async def eventually(predicate: Callable[[], bool], timeout: float = 5) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


def sent(server: FakeEventAPI, op: Opcode) -> list[dict]:
    return [m["d"] for m in server.received if m["op"] == op]


async def test_connect_and_ready(server: FakeEventAPI, client: Client) -> None:
    ready: list[Hello] = []
    client.add_event_handler("ready", lambda h: _append(ready, h))
    await client.start()
    assert client.is_ready
    assert client.hello is not None and client.hello.session_id == "session-1"
    await eventually(lambda: len(ready) == 1)


async def _append(lst: list, item: object) -> None:
    lst.append(item)


async def test_subscribe_before_and_after_connect(server: FakeEventAPI, client: Client) -> None:
    await client.subscribe("emote_set.update", object_id=EMOTE_SET)  # queued while offline
    assert server.received == []
    await client.start()
    await client.subscribe("user.update", object_id="u1")  # sent and acked immediately
    assert sent(server, Opcode.SUBSCRIBE) == [
        {"type": "emote_set.update", "condition": {"object_id": EMOTE_SET}},
        {"type": "user.update", "condition": {"object_id": "u1"}},
    ]
    assert client.subscriptions == [
        Subscription("emote_set.update", object_id=EMOTE_SET),
        Subscription("user.update", object_id="u1"),
    ]


async def test_dispatch_routing_with_wildcards(server: FakeEventAPI, client: Client) -> None:
    exact: list[Dispatch] = []
    wildcard: list[Dispatch] = []
    other: list[Dispatch] = []

    @client.on_dispatch("emote_set.update")
    async def on_exact(e: Dispatch) -> None:
        exact.append(e)

    @client.on_dispatch("emote_set.*")
    async def on_wildcard(e: Dispatch) -> None:
        wildcard.append(e)

    @client.on_dispatch("user.*")
    async def on_other(e: Dispatch) -> None:
        other.append(e)

    await client.start()
    await client.subscribe("emote_set.update", object_id=EMOTE_SET)
    await server.dispatch(
        "emote_set.update",
        {
            "id": EMOTE_SET,
            "kind": 3,
            "pushed": [{"key": "emotes", "index": 0, "value": {"id": "e1", "name": "Pog"}}],
        },
    )
    await eventually(lambda: len(exact) == 1 and len(wildcard) == 1)
    assert other == []
    assert exact[0].body.pushed[0].value["name"] == "Pog"
    assert exact[0].timestamp is not None


async def test_handler_exceptions_do_not_kill_client(server: FakeEventAPI, client: Client) -> None:
    got: list[Dispatch] = []

    @client.on_dispatch()
    async def broken(e: Dispatch) -> None:
        raise RuntimeError("boom")

    @client.on_dispatch()
    async def fine(e: Dispatch) -> None:
        got.append(e)

    await client.start()
    await client.subscribe("emote.update")
    await server.dispatch("emote.update", {"id": "1"})
    await server.dispatch("emote.update", {"id": "2"})
    await eventually(lambda: len(got) == 2)
    assert client.is_ready


async def test_duplicate_and_unknown_subscriptions_raise_locally(server: FakeEventAPI, client: Client) -> None:
    await client.start()
    await client.subscribe("emote.update", object_id="a")
    with pytest.raises(SubscriptionError):
        await client.subscribe("emote.update", {"object_id": "a"})
    with pytest.raises(SubscriptionError):
        await client.unsubscribe("emote.update", object_id="nope")
    assert client.is_ready


async def test_unsubscribe(server: FakeEventAPI, client: Client) -> None:
    await client.start()
    sub = await client.subscribe("emote.update", object_id="a")
    await client.unsubscribe(sub)
    assert sent(server, Opcode.UNSUBSCRIBE) == [{"type": "emote.update", "condition": {"object_id": "a"}}]
    assert client.subscriptions == []


async def test_subscription_limit(server: FakeEventAPI, client: Client) -> None:
    server.subscription_limit = 1
    await client.start()
    await client.subscribe("emote.update", object_id="a")
    with pytest.raises(SubscriptionLimitError):
        await client.subscribe("emote.update", object_id="b")


async def test_resume_after_restart(server: FakeEventAPI, client: Client) -> None:
    resumed: list[Hello] = []
    disconnects: list[object] = []
    client.add_event_handler("resumed", lambda h: _append(resumed, h))
    client.add_event_handler("disconnect", lambda c: _append(disconnects, c))
    await client.start()
    await client.subscribe("emote_set.update", object_id=EMOTE_SET)

    await server.close_all(CloseCode.RESTART, "restarting")
    await server.wait_for_connections(2)
    await eventually(lambda: len(resumed) == 1)

    assert disconnects == [CloseCode.RESTART]
    assert sent(server, Opcode.RESUME) == [{"session_id": "session-1"}]
    # The resumed session restored the subscription, so it must not be sent again.
    assert len(sent(server, Opcode.SUBSCRIBE)) == 1
    assert client.session_id == "session-2"

    got: list[Dispatch] = []
    client.add_dispatch_handler("*", lambda e: _append(got, e))
    await server.dispatch("emote_set.update", {"id": EMOTE_SET})
    await eventually(lambda: len(got) == 1)


async def test_failed_resume_resubscribes(server: FakeEventAPI, client: Client) -> None:
    ready: list[Hello] = []
    client.add_event_handler("ready", lambda h: _append(ready, h))
    await client.start()
    await client.subscribe("emote_set.update", object_id=EMOTE_SET)
    server.allow_resume = False

    await server.close_all(CloseCode.RESTART)
    await eventually(lambda: len(ready) == 2)
    assert len(sent(server, Opcode.RESUME)) == 1
    assert len(sent(server, Opcode.SUBSCRIBE)) == 2
    assert server.connections[0].session.subscriptions == [
        {"type": "emote_set.update", "condition": {"object_id": EMOTE_SET}}
    ]


async def test_server_reconnect_op(server: FakeEventAPI, client: Client) -> None:
    resumed: list[Hello] = []
    client.add_event_handler("resumed", lambda h: _append(resumed, h))
    await client.start()
    await server.broadcast(Opcode.RECONNECT, {"reason": "scaling"})
    await eventually(lambda: len(resumed) == 1)
    assert server.connect_count == 2


async def test_end_of_stream_reports_its_code(server: FakeEventAPI, client: Client) -> None:
    disconnects: list[object] = []
    client.add_event_handler("disconnect", lambda c: _append(disconnects, c))
    await client.start()
    await server.broadcast(Opcode.END_OF_STREAM, {"code": 4006, "message": "restart"})
    await server.close_all(1000)
    await server.wait_for_connections(2)
    assert disconnects == [CloseCode.RESTART]


async def test_fatal_close_code_stops_client(server: FakeEventAPI, client: Client) -> None:
    await client.start()
    await server.close_all(CloseCode.INVALID_PAYLOAD, "bad payload")
    with pytest.raises(ClosedError) as info:
        await client.wait_closed()
    assert info.value.code is CloseCode.INVALID_PAYLOAD
    assert server.connect_count == 1


async def test_desync_recovers_with_fresh_session(server: FakeEventAPI, client: Client) -> None:
    ready: list[Hello] = []
    client.add_event_handler("ready", lambda h: _append(ready, h))
    await client.start()
    await client.subscribe("emote.update", object_id="a")
    await server.close_all(CloseCode.ALREADY_SUBSCRIBED)
    await eventually(lambda: len(ready) == 2)
    assert sent(server, Opcode.RESUME) == []  # no resume after a desync
    assert len(sent(server, Opcode.SUBSCRIBE)) == 2


async def test_heartbeat_watchdog_reconnects(server: FakeEventAPI) -> None:
    server.heartbeat_interval = 50  # ms -> dead after 150ms of silence (+ grace)
    client = Client(server.url, ack_timeout=2)
    client.heartbeat_grace = 0.1
    try:
        await client.start()
        await server.wait_for_connections(2, timeout=3)
        await eventually(lambda: bool(sent(server, Opcode.RESUME)))
        assert sent(server, Opcode.RESUME)[0] == {"session_id": "session-1"}
    finally:
        await client.close()


async def test_missing_ack_does_not_block_forever(server: FakeEventAPI) -> None:
    server.ack_commands = False
    client = Client(server.url, ack_timeout=0.2)
    try:
        await client.start()
        await client.subscribe("emote.update", object_id="a")
        assert client.is_ready
    finally:
        await client.close()


async def test_close_during_backoff_is_prompt(server: FakeEventAPI) -> None:
    client = Client("ws://127.0.0.1:1/v3", max_backoff=60)  # nothing listens there
    task = asyncio.create_task(client.run())
    await asyncio.sleep(0.3)
    async with asyncio.timeout(2):
        await client.close()
        await task


async def test_context_manager(server: FakeEventAPI) -> None:
    async with Client(server.url) as client:
        assert client.is_ready
    assert not client.is_ready
    await eventually(lambda: not server.connections)


async def test_dispatch_stream(server: FakeEventAPI, client: Client) -> None:
    stream = client.dispatches("emote_set.*")  # created before start: buffers from now on
    await client.start()
    await client.subscribe("emote_set.update", object_id=EMOTE_SET)
    await client.subscribe("user.update", object_id="u1")
    await server.dispatch("user.update", {"id": "u1"})  # filtered out by the pattern
    await server.dispatch("emote_set.update", {"id": "a"})
    await server.dispatch("emote_set.update", {"id": "b"})

    got = []
    async with asyncio.timeout(5):
        async for event in stream:
            got.append(event.body.id)
            if len(got) == 2:
                break
    assert got == ["a", "b"]


async def test_dispatch_stream_ends_when_client_closes(server: FakeEventAPI, client: Client) -> None:
    await client.start()

    async def consume() -> list[Dispatch]:
        return [e async for e in client.dispatches()]

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.05)
    await client.close()
    async with asyncio.timeout(2):
        assert await task == []
    # A stream created after the client stopped ends immediately.
    async with asyncio.timeout(2):
        assert [e async for e in client.dispatches()] == []


async def test_dispatch_stream_context_manager_and_gc(server: FakeEventAPI, client: Client) -> None:
    await client.start()
    async with client.dispatches() as stream:
        assert stream in client._streams
    assert stream not in client._streams

    client.dispatches()  # dropped right away
    import gc

    gc.collect()
    assert len(client._streams) == 0


async def test_dispatch_stream_maxsize_drops_oldest(server: FakeEventAPI, client: Client) -> None:
    stream = client.dispatches(maxsize=2)
    seen: list[Dispatch] = []
    client.add_dispatch_handler("*", lambda e: _append(seen, e))
    await client.start()
    await client.subscribe("emote.update")
    for i in range(4):
        await server.dispatch("emote.update", {"id": str(i)})
    # Streams are fed before handler tasks run, so once the handler saw all 4 the stream did too.
    await eventually(lambda: len(seen) == 4)
    await client.close()
    assert [e.body.id async for e in stream] == ["2", "3"]
