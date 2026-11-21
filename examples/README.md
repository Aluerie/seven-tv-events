# Examples

Each example takes an emote set ID as its first argument:

```console
uv run python examples/watch_emote_set.py <EMOTE_SET_ID>
```

To find an emote set ID, open the set on [7tv.app](https://7tv.app) and copy the ID from the URL. A user's default emote set often has the same ID as the user.

| Example | Shows |
| --- | --- |
| [watch_emote_set.py](watch_emote_set.py) | The basics: decorator handlers, lifecycle events, and reading added, removed and renamed emotes. |
| [iterate_events.py](iterate_events.py) | `async for` over `client.dispatches()`, wildcard subscriptions, and walking through every change. |
| [emote_set_mirror.py](emote_set_mirror.py) | A local copy of a set kept in sync: initial load with `seven_tv_events.rest.fetch_emote_set`, applying changes, re-fetching after a non-resumed reconnect, and a shared `aiohttp` session. |
| [twitchio_bot.py](twitchio_bot.py) | A real [TwitchIO 3](https://twitchio.dev) bot that posts emote changes in the streamer's chat. It finds the streamer's active set from their Twitch ID with `seven_tv_events.rest.fetch_user_connection`. It's configured with environment variables instead of an emote set ID argument; setup steps are at the top of the file. |
| [bot_integration.py](bot_integration.py) | Embedding the client in a bot: background start, joining and leaving channels at runtime, and handling `ClosedError` and subscription errors. |

To try them, run one and then add, rename or remove an emote in that set on 7tv.app.
