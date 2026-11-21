# Contributing

Thanks for helping out! Bug reports, fixes, and notes about how the live 7TV server behaves are all welcome.

## Setup

The project uses [uv](https://docs.astral.sh/uv/) and Python 3.14+.

```console
uv sync
```

## Before you open a pull request

```console
uv run pytest                                  # tests run against an in-process fake server, no network
uv run ruff check --fix && uv run ruff format
uv run ty check                                # type checking (also covers examples/)
```

CI runs the same checks on Linux, Windows, and macOS with Python 3.14 and 3.15.

- **Tests:** client tests use the fake server in `tests/fake_server.py`, never the live endpoint. The files in `tests/data/` were captured from the live 7TV API and tests assert exact values from them, so don't edit them by hand.
- **Live server behavior:** if you find something the live server does that the [official docs](https://7tv.app/api/docs#EventAPI) don't mention, add it to "Notes on the live server" in the README.
- **Scope:** the REST helpers intentionally cover only the two lookups EventAPI users need. Please don't grow them into a full 7TV API client.

## Commit messages

Commits follow [gitmoji](https://gitmoji.dev): start the subject line with the emoji (the character itself, not a `:shortcode:`) that matches the main intent of the change.

```text
🐛 Fix ack matching after a timeout
✨ Add maxsize to dispatch streams
📝 Document the 1008 close in the README
```

The ones you'll need most often:

| Emoji | Use for |
| --- | --- |
| ✨ | a new feature |
| 🐛 | a bug fix |
| 🚑️ | a critical hotfix |
| 📝 | documentation |
| ✅ | adding or updating tests |
| ♻️ | refactoring |
| 🎨 | code structure or formatting |
| ⚡️ | performance |
| 🏷️ | type annotations |
| 🔥 | removing code or files |
| 🔧 | configuration files |
| 👷 | CI |
| ⬆️ | upgrading dependencies |
| 🔖 | a release or version tag |

The full list is at [gitmoji.dev](https://gitmoji.dev). If a commit mixes several kinds of change, use the emoji for the main one, or split the commit.
