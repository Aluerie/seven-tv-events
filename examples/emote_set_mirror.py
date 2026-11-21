"""Keep an up-to-date local copy of an emote set.

This is the usual pattern for chat bots: load the full set once over the REST
API (``seven_tv_events.rest.fetch_emote_set``), then apply EventAPI changes to it.
It also shows two less obvious things:

* sharing one ``aiohttp.ClientSession`` between the REST calls and the client;
* re-fetching the set after a reconnect that could NOT resume the session
  (``"ready"`` fires again), since events may have been missed in between.
  After a successful resume (``"resumed"``) the server replays missed events,
  so no re-fetch is needed.

    uv run python examples/emote_set_mirror.py EMOTE_SET_ID
"""

import asyncio
import contextlib
import sys

import aiohttp

import seven_tv_events

EMOTE_SET_ID = sys.argv[1] if len(sys.argv) > 1 else sys.exit(__doc__)


class EmoteSetMirror:
    def __init__(self, session: aiohttp.ClientSession, emote_set_id: str) -> None:
        self.session = session
        self.emote_set_id = emote_set_id
        self.name = ""
        self.emotes: dict[str, str] = {}  # emote id -> name used in this set (aliases included)
        self._buffer: list[seven_tv_events.Dispatch] | None = None  # events that arrive while load() runs

    async def load(self) -> None:
        # Events can arrive while the REST request is in flight, and the snapshot may
        # or may not include them. Hold them back and replay them on top of the
        # snapshot: re-applying a change it already has is harmless, since each change
        # sets or removes one emote by ID.
        self._buffer = []
        try:
            emote_set = await seven_tv_events.rest.fetch_emote_set(self.emote_set_id, session=self.session)
            self.name = emote_set.name
            self.emotes = {e.id: e.name for e in emote_set.emotes}
        finally:
            buffered, self._buffer = self._buffer, None
        print(f"Loaded {self.name!r}: {len(self.emotes)} emotes")
        for event in buffered:
            self.apply(event)

    def apply(self, event: seven_tv_events.Dispatch) -> None:
        if self._buffer is not None:
            self._buffer.append(event)
            return
        for change_type, change in event.body.changes():
            match change_type, change.key:
                case "pushed", "emotes":
                    self.emotes[change.value["id"]] = change.value["name"]
                    print(f"+ {change.value['name']}")
                case "pulled", "emotes":
                    self.emotes.pop(change.old_value["id"], None)
                    print(f"- {change.old_value['name']}")
                case "updated", "emotes":
                    self.emotes.pop(change.old_value["id"], None)
                    self.emotes[change.value["id"]] = change.value["name"]
                    print(f"~ {change.old_value['name']} -> {change.value['name']}")
                case "updated", "name":
                    self.name = change.value
                    print(f"~ set renamed to {self.name!r}")
        print(f"  now {len(self.emotes)} emotes in {self.name!r}")


async def main() -> None:
    async with aiohttp.ClientSession() as session:
        mirror = EmoteSetMirror(session, EMOTE_SET_ID)
        client = seven_tv_events.Client(session=session)  # reuse the same HTTP session

        @client.on_event("ready")
        async def on_ready(hello: seven_tv_events.Hello) -> None:
            # Fires on the first connect and after every reconnect that started a
            # fresh session, which is exactly when our copy may be stale.
            await mirror.load()

        @client.on_dispatch(seven_tv_events.SubscriptionType.EMOTE_SET_UPDATE)
        async def on_update(event: seven_tv_events.Dispatch) -> None:
            mirror.apply(event)

        await client.subscribe(seven_tv_events.SubscriptionType.EMOTE_SET_UPDATE, object_id=EMOTE_SET_ID)
        async with client:
            await client.wait_closed()


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
