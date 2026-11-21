"""An in-process fake of the 7TV EventAPI WebSocket server, for tests."""

import asyncio
import itertools
import time
from dataclasses import dataclass, field
from typing import Any

from aiohttp import WSMsgType, web


@dataclass
class Session:
    id: str
    subscriptions: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class Connection:
    ws: web.WebSocketResponse
    session: Session


class FakeEventAPI:
    def __init__(self, *, heartbeat_interval: int = 45_000, subscription_limit: int = 500) -> None:
        self.heartbeat_interval = heartbeat_interval
        self.subscription_limit = subscription_limit
        self.sessions: dict[str, Session] = {}
        self.connections: list[Connection] = []
        self.received: list[dict[str, Any]] = []
        self.connect_count = 0
        self.allow_resume = True
        self.ack_commands = True
        self._ids = itertools.count(1)
        self._connected = asyncio.Condition()
        self.app = web.Application()
        self.app.router.add_get("/v3", self.handler)
        self._runner: web.AppRunner | None = None
        self.url = ""

    async def start(self) -> None:
        self._runner = web.AppRunner(self.app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        port = self._runner.addresses[0][1]
        self.url = f"ws://127.0.0.1:{port}/v3"

    async def stop(self) -> None:
        for conn in list(self.connections):
            await conn.ws.close()
        if self._runner:
            await self._runner.cleanup()

    @staticmethod
    def msg(op: int, d: dict[str, Any]) -> dict[str, Any]:
        return {"op": op, "t": int(time.time() * 1000), "d": d}

    async def handler(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        session = Session(id=f"session-{next(self._ids)}")
        self.sessions[session.id] = session
        conn = Connection(ws, session)
        self.connections.append(conn)
        self.connect_count += 1
        await ws.send_json(
            self.msg(
                1,
                {
                    "heartbeat_interval": self.heartbeat_interval,
                    "session_id": session.id,
                    "subscription_limit": self.subscription_limit,
                },
            )
        )
        async with self._connected:
            self._connected.notify_all()
        try:
            async for raw in ws:
                if raw.type != WSMsgType.TEXT:
                    continue
                data = raw.json()
                self.received.append(data)
                await self._on_message(conn, data["op"], data.get("d") or {})
        finally:
            if conn in self.connections:
                self.connections.remove(conn)
        return ws

    async def _on_message(self, conn: Connection, op: int, d: dict[str, Any]) -> None:
        match op:
            case 35:  # SUBSCRIBE
                if d in conn.session.subscriptions:
                    await conn.ws.close(code=4009, message=b"Already Subscribed")
                    return
                conn.session.subscriptions.append(d)
                if self.ack_commands:
                    await conn.ws.send_json(self.msg(5, {"command": "SUBSCRIBE", "data": d}))
            case 36:  # UNSUBSCRIBE
                if d not in conn.session.subscriptions:
                    await conn.ws.close(code=4010, message=b"Not Subscribed")
                    return
                conn.session.subscriptions.remove(d)
                await conn.ws.send_json(self.msg(5, {"command": "UNSUBSCRIBE", "data": d}))
            case 34:  # RESUME
                old = self.sessions.get(d.get("session_id", ""))
                success = self.allow_resume and old is not None
                if success:
                    conn.session.subscriptions = old.subscriptions
                await conn.ws.send_json(
                    self.msg(
                        5,
                        {
                            "command": "RESUME",
                            "data": {"success": success, "subscriptions_restored": len(conn.session.subscriptions)},
                        },
                    )
                )
            case _:
                await conn.ws.close(code=4001, message=b"Unknown Operation")

    async def wait_for_connections(self, count: int, timeout: float = 5) -> None:
        async with asyncio.timeout(timeout):
            async with self._connected:
                await self._connected.wait_for(lambda: self.connect_count >= count)

    async def dispatch(self, type: str, body: dict[str, Any]) -> None:
        for conn in self.connections:
            if any(s["type"] == type for s in conn.session.subscriptions):
                await conn.ws.send_json(self.msg(0, {"type": type, "body": body}))

    async def broadcast(self, op: int, d: dict[str, Any]) -> None:
        for conn in self.connections:
            await conn.ws.send_json(self.msg(op, d))

    async def close_all(self, code: int, message: str = "") -> None:
        for conn in list(self.connections):
            await conn.ws.close(code=code, message=message.encode())
