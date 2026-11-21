"""``seven-tv-events``: print 7TV EventAPI events to stdout as JSON lines."""

import argparse
import asyncio
import json
import logging
import sys

from .client import DEFAULT_URL, Client
from .errors import EventAPIError
from .models import Dispatch, Hello, Subscription


def parse_subscription(spec: str) -> Subscription:
    """Parse ``type[:key=value[,key=value...]]`` into a :class:`Subscription`."""
    type_, _, conds = spec.partition(":")
    condition: dict[str, str] = {}
    for pair in filter(None, conds.split(",")):
        key, sep, value = pair.partition("=")
        if not sep:
            raise argparse.ArgumentTypeError(f"condition {pair!r} must look like key=value")
        condition[key.strip()] = value.strip()
    if not type_:
        raise argparse.ArgumentTypeError("subscription type is empty")
    return Subscription(type_, condition)


async def _run(url: str, subscriptions: list[Subscription]) -> None:
    client = Client(url)

    @client.on_event("ready")
    async def on_ready(hello: Hello) -> None:
        print(f"# ready: session={hello.session_id} limit={hello.subscription_limit}", file=sys.stderr)

    @client.on_event("resumed")
    async def on_resumed(hello: Hello) -> None:
        print(f"# resumed: session={hello.session_id}", file=sys.stderr)

    @client.on_dispatch("*")
    async def on_dispatch(event: Dispatch) -> None:
        print(json.dumps(event.raw, ensure_ascii=False), flush=True)

    for sub in dict.fromkeys(subscriptions):  # the same subscription given twice is harmless here
        await client.subscribe(sub)
    await client.start()
    try:
        await client.wait_closed()
    finally:
        await client.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="seven-tv-events",
        description="Subscribe to 7TV EventAPI events and print each dispatch as a JSON line.",
        epilog="example: seven-tv-events emote_set.update:object_id=62cdd34e72a832540de95857",
    )
    parser.add_argument(
        "subscriptions",
        nargs="+",
        type=parse_subscription,
        metavar="TYPE[:KEY=VALUE,...]",
        help="subscription type with optional conditions, e.g. 'emote_set.*:object_id=...'",
    )
    parser.add_argument("--url", default=DEFAULT_URL, help=f"EventAPI endpoint (default: {DEFAULT_URL})")
    parser.add_argument("-v", "--verbose", action="store_true", help="enable debug logging")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    try:
        asyncio.run(_run(args.url, args.subscriptions))
    except KeyboardInterrupt:
        pass
    except EventAPIError as exc:  # e.g. a rejected subscription or a fatal close code
        sys.exit(f"seven-tv-events: error: {exc}")
