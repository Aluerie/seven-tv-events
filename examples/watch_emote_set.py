"""Print emote changes in a 7TV emote set as they happen, using decorator handlers.

Run it, then add, rename or remove an emote in the set on https://7tv.app:

    uv run python examples/watch_emote_set.py EMOTE_SET_ID

Stop with Ctrl+C.
"""

import asyncio
import contextlib
import logging
import sys

import seven_tv_events

EMOTE_SET_ID = sys.argv[1] if len(sys.argv) > 1 else sys.exit(__doc__)


async def main() -> None:
    client = seven_tv_events.Client()

    # Lifecycle events: useful for logging and for reacting to reconnects.
    @client.on_event("ready")
    async def on_ready(hello: seven_tv_events.Hello) -> None:
        print(f"Connected (session {hello.session_id}), watching emote set {EMOTE_SET_ID}")

    @client.on_event("resumed")
    async def on_resumed(hello: seven_tv_events.Hello) -> None:
        print("Reconnected and resumed the previous session; missed events are replayed")

    @client.on_event("disconnect")
    async def on_disconnect(code: seven_tv_events.CloseCode | int | None) -> None:
        print(f"Disconnected ({code}); reconnecting automatically")

    # Dispatches: one handler per event type (wildcards like "emote_set.*" work too).
    @client.on_dispatch(seven_tv_events.SubscriptionType.EMOTE_SET_UPDATE)
    async def on_emote_set_update(event: seven_tv_events.Dispatch) -> None:
        who = event.body.actor.display_name if event.body.actor else "someone"

        # An emote set's emotes live in the "emotes" array:
        #   pushed  -> an emote was added       (value     = the new emote)
        #   pulled  -> an emote was removed     (old_value = the removed emote)
        #   updated -> an emote was renamed     (old_value / value = before / after)
        for change in event.body.pushed:
            if change.key == "emotes":
                print(f"+ {change.value['name']} added by {who}")
        for change in event.body.pulled:
            if change.key == "emotes":
                print(f"- {change.old_value['name']} removed by {who}")
        for change in event.body.updated:
            if change.key == "emotes":
                print(f"~ {change.old_value['name']} renamed to {change.value['name']} by {who}")
            elif change.key == "name":
                print(f"~ emote set renamed from {change.old_value!r} to {change.value!r} by {who}")

    # Subscribing before connecting is fine: it is sent as soon as the session is ready.
    await client.subscribe(seven_tv_events.SubscriptionType.EMOTE_SET_UPDATE, object_id=EMOTE_SET_ID)

    async with client:  # connects, waits until ready, and closes on exit
        await client.wait_closed()  # runs until Ctrl+C (or a fatal error)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
