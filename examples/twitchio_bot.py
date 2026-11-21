"""Announce 7TV emote changes in a streamer's Twitch chat, with TwitchIO 3.

When an emote is added, removed or renamed in the streamer's active 7TV emote
set, the bot posts e.g. "7TV: Irene added apuBand" in chat.

Setup (once)
------------
1. Create an application at https://dev.twitch.tv/console/apps with the OAuth
   redirect URL ``http://localhost:4343/oauth/callback``.
2. Set these environment variables:

       TWITCH_CLIENT_ID      the application's client ID
       TWITCH_CLIENT_SECRET  the application's client secret
       BOT_ID                numeric Twitch user ID of the bot account
       STREAMER_ID           numeric Twitch user ID of the streamer's channel

3. Install the example dependencies and start the bot:

       uv sync --group examples
       uv run python examples/twitchio_bot.py

4. With the bot running, authorize it once. Tokens are saved to
   ``.tio.tokens.json`` and reused on later runs, so keep that file private.

   - Logged in to Twitch as the **bot** account, open:
     http://localhost:4343/oauth?scopes=user:read:chat%20user:write:chat%20user:bot&force_verify=true
   - Logged in as the **streamer**, open (not needed if the bot is a moderator there):
     http://localhost:4343/oauth?scopes=channel:bot&force_verify=true
"""

import logging
import os

from twitchio.ext import commands

import seven_tv_events

log = logging.getLogger("twitchio_bot")


class Bot(commands.Bot):
    def __init__(self) -> None:
        self.streamer_id = os.environ["STREAMER_ID"]
        super().__init__(
            client_id=os.environ["TWITCH_CLIENT_ID"],
            client_secret=os.environ["TWITCH_CLIENT_SECRET"],
            bot_id=os.environ["BOT_ID"],
            owner_id=self.streamer_id,
            prefix="!",
        )
        self.stv = seven_tv_events.Client()
        self.stv.add_dispatch_handler(seven_tv_events.SubscriptionType.EMOTE_SET_UPDATE, self.on_emote_set_update)

    async def setup_hook(self) -> None:
        emote_set_id = await self.fetch_7tv_emote_set_id(self.streamer_id)
        await self.stv.subscribe(seven_tv_events.SubscriptionType.EMOTE_SET_UPDATE, object_id=emote_set_id)
        # Don't block bot startup on 7TV: the client connects and reconnects in the background.
        await self.stv.start(wait=False)
        log.info("Watching 7TV emote set %s", emote_set_id)

    async def close(self, **options: object) -> None:
        await self.stv.close()
        await super().close(**options)

    @staticmethod
    async def fetch_7tv_emote_set_id(twitch_id: str) -> str:
        """The emote set currently active on this Twitch channel, according to 7TV."""
        try:
            connection = await seven_tv_events.rest.fetch_user_connection("twitch", twitch_id)
        except seven_tv_events.NotFoundError:
            raise RuntimeError(f"Twitch user {twitch_id} has no 7TV account") from None
        if not connection.emote_set_id:
            raise RuntimeError(f"Twitch user {twitch_id} has no active 7TV emote set")
        return connection.emote_set_id

    async def on_emote_set_update(self, event: seven_tv_events.Dispatch) -> None:
        who = event.body.actor.display_name if event.body.actor else "Someone"
        added = [c.value["name"] for c in event.body.pushed if c.key == "emotes"]
        removed = [c.old_value["name"] for c in event.body.pulled if c.key == "emotes"]
        renamed = [f"{c.old_value['name']} → {c.value['name']}" for c in event.body.updated if c.key == "emotes"]

        streamer = self.create_partialuser(self.streamer_id)
        for verb, names in (("added", added), ("removed", removed), ("renamed", renamed)):
            if names:
                # Twitch chat messages are capped at 500 characters.
                message = f"7TV: {who} {verb} {', '.join(names)}"[:500]
                await streamer.send_message(sender=self.bot_id, message=message)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    Bot().run()
