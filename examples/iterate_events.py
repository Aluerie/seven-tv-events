"""Consume events with ``async for`` instead of callbacks.

Prints every event about an emote set and its owner, then stops after a few
events (or Ctrl+C):

    uv run python examples/iterate_events.py EMOTE_SET_ID [MAX_EVENTS]

Tip: a user's default emote set often shares its ID with the user, so the same
ID usually works for ``user.*`` too.
"""

import asyncio
import contextlib
import sys

import seven_tv_events

OBJECT_ID = sys.argv[1] if len(sys.argv) > 1 else sys.exit(__doc__)
MAX_EVENTS = int(sys.argv[2]) if len(sys.argv) > 2 else 5


async def main() -> None:
    async with seven_tv_events.Client() as client:
        # Create the stream before subscribing: it buffers from the moment it exists, so
        # events arriving while the subscriptions are being acked aren't missed.
        # Each dispatches() stream has its own buffer and its own filter pattern, and
        # `async with` makes sure it is closed when we leave the block.
        async with client.dispatches("*") as stream:
            # Wildcard subscriptions: every emote_set.* and user.* event for this object.
            await client.subscribe(seven_tv_events.SubscriptionType.EMOTE_SET_ALL, object_id=OBJECT_ID)
            await client.subscribe(seven_tv_events.SubscriptionType.USER_ALL, object_id=OBJECT_ID)
            print(f"Listening for {MAX_EVENTS} events on {OBJECT_ID}...")

            count = 0
            async for event in stream:
                count += 1
                actor = event.body.actor.display_name if event.body.actor else "?"
                print(f"[{count}] {event.type} on {event.body.id} by {actor}")
                for change_type, change in event.body.changes():
                    print(
                        f"      {change_type:<8} {change.key}"
                        + (f"[{change.index}]" if change.index is not None else "")
                    )
                if count >= MAX_EVENTS:
                    break
    # Leaving `async with seven_tv_events.Client()` closes the connection.


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
