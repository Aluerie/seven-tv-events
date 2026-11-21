"""Embed the client in a long-running app, such as a Twitch or Discord bot.

The bot below is a stand-in for whatever framework you use: what matters is
where the client is started, how channels are added and removed at runtime, and
how errors are handled. The demo "joins" one channel, then shuts down after
a while:

    uv run python examples/bot_integration.py EMOTE_SET_ID [SECONDS]
"""

import asyncio
import logging
import sys

import seven_tv_events

log = logging.getLogger("bot")


class EmoteNotifier:
    """Announces 7TV emote changes for the channels the bot is in."""

    def __init__(self) -> None:
        self.client = seven_tv_events.Client()
        self.client.add_dispatch_handler(seven_tv_events.SubscriptionType.EMOTE_SET_UPDATE, self.on_emote_set_update)
        self.channels: dict[str, set[str]] = {}  # emote set id -> channels using it
        self._supervisor: asyncio.Task[None] | None = None

    # -- lifecycle: call these from your bot's setup / shutdown hooks ---------

    async def start(self) -> None:
        # wait=False: don't block bot startup on 7TV being reachable. Subscriptions
        # added before the connection is ready are queued and sent automatically.
        await self.client.start(wait=False)
        self._supervisor = asyncio.create_task(self._watch())

    async def stop(self) -> None:
        await self.client.close()
        if self._supervisor:
            await self._supervisor

    async def _watch(self) -> None:
        # The client reconnects by itself; it only stops on its own for close codes
        # that mean a bug in the client (ClosedError). Surface those loudly.
        try:
            await self.client.wait_closed()
        except seven_tv_events.ClosedError as exc:
            log.critical("7TV EventAPI stopped for good: %s", exc)

    # -- runtime subscription management --------------------------------------

    async def join(self, channel: str, emote_set_id: str) -> None:
        # Normalize legacy hex IDs to ULIDs, the form event IDs arrive in.
        emote_set_id = seven_tv_events.legacy_id_to_ulid(emote_set_id)
        if emote_set_id not in self.channels:  # several channels can share one set
            # Record the set before awaiting, so a concurrent join() for the same set
            # doesn't subscribe a second time.
            self.channels[emote_set_id] = set()
            try:
                await self.client.subscribe(seven_tv_events.SubscriptionType.EMOTE_SET_UPDATE, object_id=emote_set_id)
            except seven_tv_events.SubscriptionError as exc:  # e.g. the subscription limit, or a rejected ID
                del self.channels[emote_set_id]
                log.error("Can't watch 7TV emotes for #%s: %s", channel, exc)
                return
        self.channels[emote_set_id].add(channel)
        log.info("Watching 7TV emotes for #%s", channel)

    async def leave(self, channel: str, emote_set_id: str) -> None:
        emote_set_id = seven_tv_events.legacy_id_to_ulid(emote_set_id)
        channels = self.channels.get(emote_set_id)
        if channels is None or channel not in channels:
            return
        channels.discard(channel)
        if not channels:  # last channel using this set: stop watching it
            del self.channels[emote_set_id]
            await self.client.unsubscribe(seven_tv_events.SubscriptionType.EMOTE_SET_UPDATE, object_id=emote_set_id)

    # -- event handling --------------------------------------------------------

    async def on_emote_set_update(self, event: seven_tv_events.Dispatch) -> None:
        who = event.body.actor.display_name if event.body.actor else "someone"
        for channel in self.channels.get(event.body.id, ()):
            for change in event.body.pushed:
                if change.key == "emotes":
                    await self.send_chat(channel, f"{who} added {change.value['name']}")
            for change in event.body.pulled:
                if change.key == "emotes":
                    await self.send_chat(channel, f"{who} removed {change.old_value['name']}")

    async def send_chat(self, channel: str, message: str) -> None:
        print(f"[#{channel}] {message}")  # replace with your bot's send-message call


async def main(emote_set_id: str, seconds: float) -> None:
    notifier = EmoteNotifier()
    await notifier.start()  # e.g. in your bot's setup_hook / event_ready
    await notifier.join("demo_channel", emote_set_id)
    await notifier.join("other_channel", emote_set_id)  # both channels get the announcements
    try:
        await asyncio.sleep(seconds)  # your bot runs here
    finally:
        await notifier.stop()  # e.g. in your bot's close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    asyncio.run(
        main(
            emote_set_id=sys.argv[1] if len(sys.argv) > 1 else sys.exit(__doc__),
            seconds=float(sys.argv[2]) if len(sys.argv) > 2 else 60,
        )
    )
