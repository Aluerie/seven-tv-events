"""WebSocket client for the 7TV EventAPI (v3)."""

import asyncio
import contextlib
import fnmatch
import inspect
import json
import logging
import random
import time
import weakref
from collections import Counter, defaultdict
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, Literal, NoReturn, Self

import aiohttp

from .enums import CloseCode, Opcode, ReconnectPolicy
from .errors import ClosedError, NotConnectedError, SubscriptionError, SubscriptionLimitError
from .models import (
    Ack,
    Dispatch,
    EndOfStream,
    Error,
    Heartbeat,
    Hello,
    Message,
    Reconnect,
    Subscription,
)

__all__ = ("DEFAULT_URL", "Client", "DispatchStream", "LifecycleEvent")

log = logging.getLogger(__name__)

DEFAULT_URL = "wss://events.7tv.io/v3"

type LifecycleEvent = Literal[
    "ready", "resumed", "heartbeat", "ack", "error", "reconnect", "end_of_stream", "disconnect"
]
"""Names accepted by :meth:`Client.on_event`.

=================  =================================  ==================================================
Event              Handler argument                   When
=================  =================================  ==================================================
``ready``          :class:`Hello`                     A new session is ready and subscriptions are sent.
``resumed``        :class:`Hello`                     A previous session was resumed.
``heartbeat``      :class:`Heartbeat`                 The server sent a heartbeat.
``ack``            :class:`Ack`                       The server acknowledged a command.
``error``          :class:`Error`                     The server reported an error.
``reconnect``      :class:`Reconnect`                 The server asked the client to reconnect.
``end_of_stream``  :class:`EndOfStream`               The server is about to close the connection.
``disconnect``     ``CloseCode | int | None``         The connection was lost (close code if known).
=================  =================================  ==================================================
"""

type DispatchHandler = Callable[[Dispatch], Awaitable[Any]]
type EventHandler = Callable[[Any], Awaitable[Any]]

# Close codes after which the previous session should not be resumed.
_NO_RESUME = frozenset({
    CloseCode.SERVER_ERROR,
    CloseCode.RATE_LIMITED,
    CloseCode.INSUFFICIENT_PRIVILEGE,
})
# Close codes that mean the server's subscriptions differ from what the client tracked.
_DESYNC = frozenset({CloseCode.ALREADY_SUBSCRIBED, CloseCode.NOT_SUBSCRIBED})
# WebSocket "policy violation": the live server's close frame after an END_OF_STREAM with 4002.
_POLICY_VIOLATION = 1008
# Close codes with which the live server answers a subscription it won't accept. For some
# invalid conditions (e.g. `host_id`) it sends END_OF_STREAM 4002 "Invalid Payload".
_REJECTING = frozenset({CloseCode.INVALID_PAYLOAD, _POLICY_VIOLATION, CloseCode.INSUFFICIENT_PRIVILEGE})
# Close codes that would otherwise be retried forever if their cause persists.
_GIVE_UP_WHEN_REPEATED = _DESYNC | frozenset({_POLICY_VIOLATION, CloseCode.INSUFFICIENT_PRIVILEGE})
_MAX_REPEATED_CLOSES = 5
# Upper bound of the random delay before reconnecting after a stable connection, so that
# every client of a restarting server doesn't reconnect in the same instant.
_STABLE_RECONNECT_JITTER = 1.0


class _Rejected(Exception):
    """Internal: the server refused a subscription command (with an error or a close)."""


@dataclass(slots=True)
class _PendingAck:
    command: str
    key: Subscription | None
    future: asyncio.Future[Ack | None] = field(default_factory=lambda: asyncio.get_running_loop().create_future())


class _Backoff:
    """Exponential backoff with full jitter."""

    def __init__(self, base: float, cap: float) -> None:
        self.base = base
        self.cap = cap
        self.attempt = 0

    def next(self) -> float:
        # Clamp the exponent so a very long outage can't overflow the float conversion.
        delay = min(self.cap, self.base * 2 ** min(self.attempt, 32))
        self.attempt += 1
        return random.uniform(delay / 2, delay)

    def reset(self) -> None:
        self.attempt = 0


class DispatchStream:
    """Async iterator over dispatches; create one with :meth:`Client.dispatches`.

    Events are buffered from the moment the stream is created. Iteration ends
    when the client stops, or when :meth:`close` is called. The client holds the
    stream only weakly, so a stream that is no longer referenced (e.g. after
    ``break``-ing out of the loop) stops receiving events on its own.
    """

    def __init__(self, client: Client, pattern: str, maxsize: int) -> None:
        self.pattern = pattern
        self._client = client
        # Unbounded on purpose: ``maxsize`` is enforced in _put so the end marker (None) never displaces an event.
        self._queue: asyncio.Queue[Dispatch | None] = asyncio.Queue()  # None ends the stream
        self._maxsize = maxsize
        self._ended = False
        self._dropped = 0  # events dropped in the current overflow episode

    def _put(self, item: Dispatch | None) -> None:
        if self._ended:
            return
        if item is None:
            self._ended = True
        elif self._maxsize > 0 and self._queue.qsize() >= self._maxsize:
            self._queue.get_nowait()
            if not self._dropped:
                log.warning("Dispatch stream %r is full; dropping the oldest events", self.pattern)
            self._dropped += 1
        self._queue.put_nowait(item)

    def close(self) -> None:
        """Stop receiving events. Buffered events can still be read before iteration ends."""
        self._client._streams.discard(self)
        self._put(None)

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> Dispatch:
        item = await self._queue.get()
        if self._dropped and self._queue.qsize() <= self._maxsize // 2:
            log.warning("Dispatch stream %r caught up after dropping %d events", self.pattern, self._dropped)
            self._dropped = 0
        if item is None:
            self._queue.put_nowait(None)  # keep ending on later calls too
            raise StopAsyncIteration
        return item

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.close()


class _Disconnected(Exception):
    """Internal: the current connection ended and the run loop should decide what to do."""

    def __init__(
        self, code: CloseCode | int | None, message: str = "", *, resumable: bool = True, rejected: bool = False
    ) -> None:
        super().__init__(code, message)
        self.code = code
        self.message = message
        self.resumable = resumable
        self.rejected = rejected  # the close was the server refusing a subscription, which was dropped


class Client:
    """A self-healing client for the 7TV EventAPI WebSocket.

    Subscriptions are remembered by the client: they may be added before
    connecting, and are restored automatically after a reconnect (by resuming
    the previous session when possible, or by re-subscribing otherwise).

    Example::

        client = seven_tv_events.Client()

        @client.on_dispatch("emote_set.update")
        async def on_emote_set_update(event: seven_tv_events.Dispatch) -> None:
            for change_type, change in event.body.changes():
                print(change_type, change.key, change.value)

        async with client:
            await client.subscribe("emote_set.update", object_id="62cdd34e72a832540de95857")
            await client.wait_closed()

    Parameters
    ----------
    url:
        The WebSocket endpoint.
    session:
        An existing :class:`aiohttp.ClientSession` to use. If omitted, the client
        creates (and closes) its own.
    reconnect:
        Whether to reconnect automatically when the connection drops.
    ack_timeout:
        Seconds to wait for the server to acknowledge a command.
    max_backoff:
        Upper bound, in seconds, for the delay between reconnect attempts.
    """

    heartbeat_grace: float = 5.0
    """Extra seconds, on top of 3 missed heartbeat intervals, before a silent connection is considered dead."""
    stable_after: float = 30.0
    """Seconds a connection must stay ready before a drop is retried at once (within a second, not with backoff)."""

    def __init__(
        self,
        url: str = DEFAULT_URL,
        *,
        session: aiohttp.ClientSession | None = None,
        reconnect: bool = True,
        ack_timeout: float = 10.0,
        max_backoff: float = 60.0,
    ) -> None:
        self.url = url
        self.reconnect = reconnect
        self.ack_timeout = ack_timeout
        self.max_backoff = max_backoff

        self._session = session
        self._owns_session = session is None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._send_lock = asyncio.Lock()

        self._hello: Hello | None = None
        self._resume_session_id: str | None = None

        self._desired: dict[Subscription, None] = {}  # insertion-ordered set
        self._active: set[Subscription] = set()
        self._rejected: dict[Subscription, str] = {}  # subscriptions the server refused -> reason
        self._sync_lock = asyncio.Lock()

        self._pending_acks: list[_PendingAck] = []
        self._stale_acks: Counter[str] = Counter()  # per command: acks still owed to timed-out commands
        self._dispatch_handlers: list[tuple[str, DispatchHandler]] = []
        self._event_handlers: defaultdict[str, list[EventHandler]] = defaultdict(list)
        self._handler_tasks: set[asyncio.Task[Any]] = set()
        self._streams: weakref.WeakSet[DispatchStream] = weakref.WeakSet()

        self._ready = asyncio.Event()
        self._close_event = asyncio.Event()
        self._closing = False
        self._running = False  # True from start()/run() until the run loop has fully finished
        self._run_done = asyncio.Event()  # set once the client has stopped (or close() was called)
        self._run_error: BaseException | None = None  # what stopped the run loop, if anything
        self._loop_task: asyncio.Task[Any] | None = None  # the task executing the run loop
        self._connecting = False  # True while a connection attempt is in progress
        self._cancelled_by_close = False
        self._ready_since = 0.0
        self._run_task: asyncio.Task[None] | None = None

    # ------------------------------------------------------------------ state

    @property
    def is_ready(self) -> bool:
        """Whether the client is connected and its subscriptions are in sync."""
        return self._ready.is_set()

    @property
    def hello(self) -> Hello | None:
        """The :class:`Hello` payload of the current session, if connected."""
        return self._hello

    @property
    def session_id(self) -> str | None:
        """ID of the current (or most recent) session."""
        return self._hello.session_id if self._hello else self._resume_session_id

    @property
    def subscriptions(self) -> list[Subscription]:
        """Every subscription the client wants, in the order they were added."""
        return list(self._desired)

    # -------------------------------------------------------------- listeners

    def on_dispatch(self, pattern: str = "*") -> Callable[[DispatchHandler], DispatchHandler]:
        """Decorator registering a coroutine for dispatches whose type matches ``pattern``.

        ``pattern`` is a shell-style wildcard, so ``"emote_set.*"`` matches
        every emote set event and ``"*"`` matches everything.
        """

        def decorator(func: DispatchHandler) -> DispatchHandler:
            self.add_dispatch_handler(pattern, func)
            return func

        return decorator

    def add_dispatch_handler(self, pattern: str, func: DispatchHandler) -> None:
        """Register ``func`` for dispatches whose type matches ``pattern``."""
        self._dispatch_handlers.append((str(pattern), func))

    def remove_dispatch_handler(self, pattern: str, func: DispatchHandler) -> None:
        """Remove a handler registered with :meth:`add_dispatch_handler`."""
        with contextlib.suppress(ValueError):
            self._dispatch_handlers.remove((str(pattern), func))

    def on_event(self, name: LifecycleEvent) -> Callable[[EventHandler], EventHandler]:
        """Decorator registering a coroutine for a lifecycle event (see :data:`LifecycleEvent`)."""

        def decorator(func: EventHandler) -> EventHandler:
            self.add_event_handler(name, func)
            return func

        return decorator

    def add_event_handler(self, name: LifecycleEvent, func: EventHandler) -> None:
        """Register ``func`` for a lifecycle event."""
        self._event_handlers[name].append(func)

    def remove_event_handler(self, name: LifecycleEvent, func: EventHandler) -> None:
        """Remove a handler registered with :meth:`add_event_handler`."""
        with contextlib.suppress(ValueError):
            self._event_handlers[name].remove(func)

    def _spawn(self, func: Callable[[Any], Any], arg: object, name: str) -> None:
        # The handler is called inside the task, so even a plain (non-async) function
        # that raises is contained here instead of breaking the read loop.
        async def runner() -> None:
            try:
                result = func(arg)
                if inspect.isawaitable(result):
                    await result
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Unhandled exception in %s handler", name)

        task = asyncio.create_task(runner(), name=f"seven_tv_events:{name}")
        self._handler_tasks.add(task)
        task.add_done_callback(self._handler_tasks.discard)

    def _emit(self, name: LifecycleEvent, arg: object) -> None:
        for func in self._event_handlers.get(name, ()):
            self._spawn(func, arg, name)

    def _emit_dispatch(self, event: Dispatch) -> None:
        for pattern, func in self._dispatch_handlers:
            if fnmatch.fnmatchcase(event.type, pattern):
                self._spawn(func, event, event.type)
        for stream in list(self._streams):
            if fnmatch.fnmatchcase(event.type, stream.pattern):
                stream._put(event)

    def dispatches(self, pattern: str = "*", *, maxsize: int = 0) -> DispatchStream:
        """Iterate over dispatches whose type matches ``pattern`` (a shell-style wildcard).

        Example::

            async for event in client.dispatches("emote_set.*"):
                print(event.type, event.body.id)

        The stream buffers events from the moment it is created and ends when the
        client stops. With ``maxsize`` > 0 the buffer is bounded, and the oldest
        events are dropped when a slow consumer falls behind.
        Use ``async with client.dispatches(...) as stream:`` to stop it deterministically.
        """
        stream = DispatchStream(self, str(pattern), maxsize)
        if self._run_done.is_set():
            stream._put(None)
        else:
            self._streams.add(stream)
        return stream

    def _end_streams(self) -> None:
        for stream in list(self._streams):
            stream.close()

    # ---------------------------------------------------------- subscriptions

    async def subscribe(
        self,
        type: str | Subscription,
        condition: Mapping[str, str] | None = None,
        /,
        **conditions: str,
    ) -> Subscription:
        """Subscribe to an event type, optionally filtered by conditions.

        If the client is connected this waits until the server acknowledges the
        subscription; otherwise it is queued and sent once the client connects.
        Either way, it is restored automatically after reconnects.

        Raises
        ------
        SubscriptionError
            The same subscription (type and conditions) already exists. The server
            would otherwise close the connection with ``4009 Already Subscribed``.
            Also raised when the server rejects the subscription (e.g. an invalid
            condition); it is then dropped. A queued subscription that the server
            rejects later is dropped and logged instead.
        SubscriptionLimitError
            The session's ``subscription_limit`` would be exceeded.
        """
        sub = type if isinstance(type, Subscription) else Subscription(type, condition, **conditions)
        if sub in self._desired:
            raise SubscriptionError(f"already subscribed to {sub!r}")
        limit = self._hello.subscription_limit if self._hello else -1
        if limit >= 0 and len(self._desired) >= limit:
            raise SubscriptionLimitError(f"subscription limit of {limit} reached")

        self._rejected.pop(sub, None)
        self._desired[sub] = None
        if self.is_ready:
            await self._sync()
            if (reason := self._rejected.pop(sub, None)) is not None:
                raise SubscriptionError(f"the server rejected {sub!r}: {reason}")
        return sub

    async def unsubscribe(
        self,
        type: str | Subscription,
        condition: Mapping[str, str] | None = None,
        /,
        **conditions: str,
    ) -> None:
        """Remove a subscription previously added with :meth:`subscribe`.

        Raises :class:`SubscriptionError` if no such subscription exists (the
        server would otherwise close the connection with ``4010 Not Subscribed``).
        """
        sub = type if isinstance(type, Subscription) else Subscription(type, condition, **conditions)
        if sub not in self._desired:
            raise SubscriptionError(f"not subscribed to {sub!r}")
        del self._desired[sub]
        if self.is_ready:
            await self._sync()

    async def _sync(self) -> None:
        """Bring the server's subscriptions in line with the desired ones."""
        async with self._sync_lock:
            # Loop until nothing is left to do: subscribe()/unsubscribe() calls made while
            # commands were in flight (e.g. during a reconnect handshake) are picked up too.
            while True:
                if to_remove := next((s for s in self._active if s not in self._desired), None):
                    with contextlib.suppress(_Rejected):  # logged by _reject_pending
                        if await self._command(Opcode.UNSUBSCRIBE, to_remove) is None:
                            return
                    self._active.discard(to_remove)
                elif to_add := next((s for s in self._desired if s not in self._active), None):
                    try:
                        if await self._command(Opcode.SUBSCRIBE, to_add) is None:
                            return
                    except _Rejected:
                        continue  # _reject_pending already dropped it from _desired
                    self._active.add(to_add)
                else:
                    return

    async def _command(self, op: Opcode, sub: Subscription) -> Ack | None:
        """Send a subscription command and wait for its ack.

        Returns ``None`` if the connection dropped before the ack arrived; the
        command will then be retried by the next sync after reconnecting.
        Raises :class:`_Rejected` if the server refused it.
        """
        pending = _PendingAck(op.command, sub)
        self._pending_acks.append(pending)
        try:
            await self.send(op, sub.to_json())
            async with asyncio.timeout(self.ack_timeout):
                return await pending.future
        except NotConnectedError:
            return None
        except TimeoutError:
            log.warning("No ack for %s %r within %ss; assuming it succeeded", op.command, sub, self.ack_timeout)
            self._stale_acks[op.command] += 1
            return Ack(command=op.command, data=sub.to_json(), raw={})
        finally:
            with contextlib.suppress(ValueError):
                self._pending_acks.remove(pending)

    def _resolve_ack(self, ack: Ack) -> None:
        key: Subscription | None = None
        if isinstance(ack.data, dict) and "type" in ack.data:
            with contextlib.suppress(TypeError, ValueError):
                key = Subscription(ack.data["type"], ack.data.get("condition") or {})
        candidates = [p for p in self._pending_acks if p.command == ack.command and not p.future.done()]
        exact = next((p for p in candidates if key is not None and p.key == key), None)
        if exact is not None:
            # The server answers in order, so any late acks we were waiting to discard are gone.
            self._stale_acks[ack.command] = 0
            exact.future.set_result(ack)
            return
        if self._stale_acks[ack.command] > 0:
            # The server rewrites IDs in its echo, so acks can't always be matched by content.
            # Commands are answered in order, so this one belongs to a command that already
            # timed out; handing it to the next pending command would shift every later ack.
            self._stale_acks[ack.command] -= 1
            log.debug("Discarding late ack for an earlier timed-out %s", ack.command)
            return
        if candidates:
            candidates[0].future.set_result(ack)

    def _reject_pending(self, reason: str) -> bool:
        """Mark the oldest pending subscription command as refused by the server.

        The server answers an unacceptable subscription with an ERROR instead of an
        ack, or by closing the connection. Commands are sent one at a time, so the
        refusal belongs to the oldest one still waiting. A refused SUBSCRIBE is
        dropped right here, because a close also cancels the task awaiting it.
        Returns whether there was such a command.
        """
        pending = next((p for p in self._pending_acks if p.key is not None and not p.future.done()), None)
        if pending is None or pending.key is None:
            return False
        log.error("The server rejected %s %r: %s", pending.command, pending.key, reason)
        if pending.command == Opcode.SUBSCRIBE.command and pending.key in self._desired:
            del self._desired[pending.key]
            self._rejected[pending.key] = reason
        pending.future.set_exception(_Rejected(reason))
        return True

    def _fail_pending_acks(self) -> None:
        for p in self._pending_acks:
            if not p.future.done():
                p.future.set_result(None)
        self._pending_acks.clear()
        self._stale_acks.clear()

    # ---------------------------------------------------------------- sending

    async def send(self, op: Opcode | int, data: Mapping[str, Any] | None = None) -> None:
        """Send a raw message. Most users should use :meth:`subscribe` instead."""
        ws = self._ws
        if ws is None or ws.closed:
            raise NotConnectedError("not connected to the EventAPI")
        async with self._send_lock:
            try:
                await ws.send_json({"op": int(op), "d": dict(data or {})})
            except (ConnectionError, aiohttp.ClientError, RuntimeError) as exc:
                raise NotConnectedError("connection lost while sending") from exc

    # -------------------------------------------------------------- lifecycle

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def start(self, *, wait: bool = True) -> None:
        """Run the client in a background task, by default waiting until it is ready.

        Raises whatever :meth:`run` raised if it stops before becoming ready. If the
        wait is cancelled (e.g. by a timeout), the client is closed before re-raising.
        """
        # Reset state here, before the task exists, so a close() issued before the task
        # first runs is not undone by it.
        self._prepare_run()
        self._run_task = asyncio.create_task(self._run(), name="seven_tv_events:run")
        if wait:
            try:
                await self.wait_until_ready()
            except BaseException:
                # Otherwise a cancelled start() would leave the client reconnecting in the background.
                await self.close()
                raise

    async def wait_until_ready(self) -> None:
        """Wait until the client is connected and its subscriptions are sent.

        Works with both :meth:`start` and :meth:`run`. Raises what stopped the client
        if it stops before becoming ready.
        """
        if self._ready.is_set():
            return
        waiters = {asyncio.create_task(self._ready.wait()), asyncio.create_task(self._run_done.wait())}
        try:
            await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for w in waiters:
                w.cancel()
        if not self._ready.is_set():
            self._raise_run_error()
            raise NotConnectedError("client stopped before becoming ready")

    async def wait_closed(self) -> None:
        """Wait until the client stops (after :meth:`close`, or a fatal error).

        Works with both :meth:`start` and :meth:`run`; returns at once if the client
        was never started. Re-raises the exception that stopped the client, if any.
        """
        if not self._running and not self._run_done.is_set():
            return  # never started
        await self._run_done.wait()
        self._raise_run_error()

    def _raise_run_error(self) -> None:
        task = self._run_task
        if task is not None and task.done() and not task.cancelled():
            task.exception()  # mark it retrieved, so asyncio doesn't warn about it
        if self._run_error is not None:
            raise self._run_error

    async def close(self) -> None:
        """Close the connection and stop reconnecting."""
        self._closing = True
        self._close_event.set()
        self._ready.clear()
        loop_task = self._loop_task
        if self._connecting and loop_task is not None and not loop_task.done():
            # A connection attempt can hang until aiohttp's connect timeout; interrupt it.
            self._cancelled_by_close = True
            loop_task.cancel()
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()
        if self._running and asyncio.current_task() is not loop_task:
            await self._run_done.wait()
        self._run_done.set()  # also marks a never-started client as stopped
        with contextlib.suppress(Exception):
            self._raise_run_error()  # retrieve it; close() itself doesn't raise
        self._end_streams()
        current = asyncio.current_task()
        for t in list(self._handler_tasks):
            if t is not current:  # a handler that calls close() must be able to finish
                t.cancel()

    async def run(self) -> None:
        """Connect and process events until :meth:`close` is called.

        Reconnects automatically (if enabled). Raises :class:`ClosedError` when
        the server closes the connection with a code that must not be retried, or
        keeps closing it with the same subscription-related code (4009, 4010, 4011
        or 1008) without the connection ever becoming stable.
        """
        self._prepare_run()
        await self._run()

    def _prepare_run(self) -> None:
        if self._running:
            raise RuntimeError("client is already running")
        self._running = True
        self._run_error = None
        self._run_done.clear()
        self._closing = False
        self._cancelled_by_close = False
        self._close_event.clear()

    async def _run(self) -> None:
        self._loop_task = asyncio.current_task()
        try:
            await self._run_loop()
        except asyncio.CancelledError:
            if not self._cancelled_by_close:
                raise
            # Cancelled by close() to interrupt a pending connect: finish normally.
            if self._loop_task is not None:
                self._loop_task.uncancel()
        except BaseException as exc:
            self._run_error = exc
            raise
        finally:
            self._running = False
            self._loop_task = None
            self._run_done.set()

    async def _run_loop(self) -> None:
        session = self._session
        if session is None or session.closed:
            session = aiohttp.ClientSession()
            self._owns_session = True
        self._session = session

        backoff = _Backoff(base=1.0, cap=self.max_backoff)
        last_code: CloseCode | int | None = None
        repeats = 0  # consecutive unstable disconnects with `last_code`
        try:
            while not self._closing:
                uptime = 0.0
                try:
                    await self._run_once(session)
                except _Disconnected as exc:
                    disconnect = exc
                except (aiohttp.ClientError, OSError, TimeoutError) as exc:
                    log.warning("Could not connect to %s: %r", self.url, exc)
                    disconnect = _Disconnected(None, str(exc) or type(exc).__name__)
                finally:
                    self._connecting = False
                    if self._ready.is_set():
                        uptime = time.monotonic() - self._ready_since
                    self._on_connection_lost()

                if self._closing:
                    break
                self._emit("disconnect", disconnect.code)

                code = disconnect.code
                policy = code.reconnect if isinstance(code, CloseCode) else ReconnectPolicy.YES
                if code in _DESYNC:
                    # The client dedupes subscriptions locally, so these mean our view of
                    # the session drifted from the server's (e.g. around a resume).
                    # Recover with a fresh session instead of giving up.
                    log.error("Subscription state out of sync (%s); starting a fresh session", code)
                    policy = ReconnectPolicy.MAYBE
                    disconnect.resumable = False
                if disconnect.rejected and policy is ReconnectPolicy.NO:
                    # 4002 caused by one invalid subscription, which is dropped now: not a client bug.
                    policy = ReconnectPolicy.MAYBE
                if policy is ReconnectPolicy.NO:
                    raise ClosedError(code, disconnect.message)
                if not disconnect.resumable or code in _NO_RESUME:
                    self._resume_session_id = None
                if not self.reconnect:
                    break

                stable = uptime >= self.stable_after
                if stable:
                    # A connection that stayed up for a while ends any earlier outage, whatever
                    # the close code, so start counting backoff attempts from scratch.
                    backoff.reset()
                if disconnect.rejected:
                    # The offending subscription was dropped, so this is progress, not a loop.
                    repeats, last_code = 0, None
                else:
                    repeats = repeats + 1 if code == last_code and not stable else 1
                    last_code = code
                if code in _GIVE_UP_WHEN_REPEATED and repeats >= _MAX_REPEATED_CLOSES:
                    # Whatever causes these did not go away with a fresh session; retrying
                    # forever would only hide the problem.
                    raise ClosedError(code, f"{disconnect.message} ({repeats} times in a row)")
                if stable and policy is ReconnectPolicy.YES:
                    # Only a stable connection earns a quick reconnect; otherwise a server
                    # that accepts and immediately drops us would be hammered.
                    delay = random.uniform(0, _STABLE_RECONNECT_JITTER)
                else:
                    delay = backoff.next()
                    if policy is ReconnectPolicy.LATER:
                        delay = max(delay, 30.0)
                log.info("Disconnected (%s %s); reconnecting in %.1fs", code, disconnect.message, delay)
                if delay:
                    with contextlib.suppress(TimeoutError):
                        async with asyncio.timeout(delay):
                            await self._close_event.wait()
        finally:
            self._closing = True
            self._end_streams()
            if self._owns_session:
                await session.close()
                self._session = None

    def _on_connection_lost(self) -> None:
        self._ready.clear()
        self._fail_pending_acks()
        self._hello = None
        self._ws = None

    async def _run_once(self, session: aiohttp.ClientSession) -> NoReturn:
        """Run a single connection until it ends, then raise :class:`_Disconnected`."""
        log.debug("Connecting to %s", self.url)
        self._connecting = True
        async with session.ws_connect(self.url, autoping=True, heartbeat=None) as ws:
            self._ws = ws
            if self._closing:
                raise _Disconnected(None, "client closing")
            try:
                hello_msg = await self._receive(ws, timeout=15)
                if hello_msg is None or hello_msg.op != Opcode.HELLO:
                    raise _Disconnected(_close_code(ws.close_code), "expected HELLO as the first message")
                hello = Hello.from_json(hello_msg.d)
            except (ValueError, KeyError, TypeError) as exc:
                await ws.close()
                raise _Disconnected(None, f"malformed HELLO: {exc!r}") from exc
            self._connecting = False
            self._hello = hello
            log.debug("Hello: session=%s heartbeat=%sms", hello.session_id, hello.heartbeat_interval)

            # Heartbeats missed for 3 cycles mean the connection is dead.
            watchdog = hello.heartbeat_interval / 1000 * 3 + self.heartbeat_grace
            reader = asyncio.create_task(self._read_loop(ws, watchdog), name="seven_tv_events:reader")
            handshake = asyncio.create_task(self._handshake(hello), name="seven_tv_events:handshake")
            try:
                await asyncio.wait({reader, handshake}, return_when=asyncio.FIRST_COMPLETED)
                if handshake.done():
                    handshake.result()  # propagate unexpected handshake errors
                await reader
                raise AssertionError("unreachable")  # _read_loop only exits by raising
            finally:
                for task in (handshake, reader):
                    if not task.done():
                        task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await task

    async def _handshake(self, hello: Hello) -> None:
        """Resume the previous session if possible, then sync subscriptions."""
        # `_resume_session_id` is the session that `_active` describes. It may only move to
        # the new session once we know what the new session holds; if the connection drops
        # before the resume result arrives, the next attempt must resume `previous` again.
        previous = self._resume_session_id
        resumed = False
        if previous and previous != hello.session_id:
            pending = _PendingAck(Opcode.RESUME.command, None)
            self._pending_acks.append(pending)
            try:
                await self.send(Opcode.RESUME, {"session_id": previous})
                async with asyncio.timeout(self.ack_timeout):
                    ack = await pending.future
            except NotConnectedError:
                return  # connection lost mid-handshake; `previous` stays resumable
            except TimeoutError:
                ack = Ack(command=Opcode.RESUME.command, data={"success": False}, raw={})
                self._stale_acks[Opcode.RESUME.command] += 1
            finally:
                with contextlib.suppress(ValueError):
                    self._pending_acks.remove(pending)
            if ack is None:
                return  # connection lost mid-handshake; `previous` stays resumable
            resumed = isinstance(ack.data, dict) and bool(ack.data.get("success"))
            log.debug("Resume of session %s %s", previous, "succeeded" if resumed else "failed")
        if not resumed:
            self._active.clear()
        self._resume_session_id = hello.session_id
        await self._sync()
        if self._ws is None or self._ws.closed:
            return
        self._ready_since = time.monotonic()
        self._ready.set()
        self._emit("resumed" if resumed else "ready", hello)

    async def _receive(self, ws: aiohttp.ClientWebSocketResponse, timeout: float | None) -> Message | None:
        msg = await ws.receive(timeout=timeout)
        match msg.type:
            case aiohttp.WSMsgType.TEXT:
                return Message.from_json(msg.json())
            case aiohttp.WSMsgType.BINARY:
                return Message.from_json(json.loads(msg.data))
            case aiohttp.WSMsgType.ERROR:
                raise _Disconnected(
                    _close_code(ws.close_code), f"websocket error: {ws.exception()!r}"
                ) from ws.exception()
            case _:  # CLOSE / CLOSING / CLOSED
                return None

    async def _read_loop(self, ws: aiohttp.ClientWebSocketResponse, watchdog: float) -> NoReturn:
        end_of_stream: EndOfStream | None = None
        while True:
            try:
                message = await self._receive(ws, timeout=watchdog)
            except TimeoutError:
                log.warning("No message for %.1fs; connection considered dead", watchdog)
                await ws.close()
                raise _Disconnected(None, "heartbeat timeout") from None
            except ValueError as exc:
                log.error("Received a malformed message: %r", exc)
                continue

            if message is None:
                code = _close_code(ws.close_code)
                if end_of_stream is not None:
                    code = end_of_stream.code
                text = end_of_stream.message if end_of_stream else ""
                # Runs before the handshake task is cancelled, so that a queued subscription
                # it was sending is dropped too instead of being retried forever.
                rejected = code in _REJECTING and self._reject_pending(
                    f"connection closed ({code}{': ' + text if text else ''})"
                )
                raise _Disconnected(code, text or "connection closed", resumable=code != 1000, rejected=rejected)

            # A payload with an unexpected shape must not take the whole client down.
            try:
                self._handle(message)
                if message.op == Opcode.END_OF_STREAM:
                    end_of_stream = EndOfStream.from_json(message.d)
            except Exception:
                log.exception("Failed to handle message (op %s): %s", message.op, message.d)

            if message.op == Opcode.RECONNECT:
                await ws.close()
                raise _Disconnected(None, "server requested reconnect")

    def _handle(self, message: Message) -> None:
        d = message.d
        match message.op:
            case Opcode.DISPATCH:
                event = Dispatch.from_json(d, message.t)
                if event.type:
                    self._emit_dispatch(event)
                else:
                    log.warning("Ignoring dispatch without a type: %s", d)
            case Opcode.HEARTBEAT:
                self._emit("heartbeat", Heartbeat.from_json(d))
            case Opcode.ACK:
                ack = Ack.from_json(d)
                self._resolve_ack(ack)
                self._emit("ack", ack)
            case Opcode.ERROR:
                error = Error.from_json(d)
                log.error("EventAPI error: %s %s", error.message, error.raw)
                # The server answers an invalid subscription with an ERROR instead of an ack.
                self._reject_pending(error.message or "server error")
                self._emit("error", error)
            case Opcode.RECONNECT:
                self._emit("reconnect", Reconnect.from_json(d))
            case Opcode.END_OF_STREAM:
                eos = EndOfStream.from_json(d)
                log.info("End of stream: %s %s", eos.code, eos.message)
                self._emit("end_of_stream", eos)
            case Opcode.HELLO:
                log.debug("Unexpected HELLO mid-session: %s", d)
            case _:
                log.debug("Unhandled opcode %s: %s", message.op, d)


def _close_code(code: int | None) -> CloseCode | int | None:
    if code is None:
        return None
    try:
        return CloseCode(code)
    except ValueError:
        return code
