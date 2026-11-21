"""Helpers for the few 7TV REST API (v3) lookups that EventAPI users usually need.

These are not a full API client. They cover finding what to subscribe to and
loading the initial state that dispatches then update:

* :func:`fetch_user_connection` turns a Twitch (or other platform) account ID
  into its active emote set;
* :func:`fetch_emote_set` loads a whole emote set.

Every helper takes an optional ``session`` to reuse an existing
:class:`aiohttp.ClientSession`. Without one, a session is created for the call.
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any
from urllib.parse import quote

import aiohttp

from .errors import NotFoundError, RESTError
from .models import EmoteSet, UserConnection

__all__ = ("DEFAULT_REST_URL", "fetch_emote_set", "fetch_user_connection")

DEFAULT_REST_URL = "https://7tv.io/v3"
DEFAULT_TIMEOUT = 15.0


@asynccontextmanager
async def _session_or_new(session: aiohttp.ClientSession | None) -> AsyncGenerator[aiohttp.ClientSession]:
    if session is not None:
        yield session
        return
    async with aiohttp.ClientSession() as new:
        yield new


async def _get_json(session: aiohttp.ClientSession | None, url: str, timeout: float) -> Any:
    async with _session_or_new(session) as s, s.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
        if resp.status < 400:
            return await resp.json()
        try:
            body = await resp.json(content_type=None)
        except ValueError:
            body = None
        message, code = resp.reason or "", None
        if isinstance(body, dict):
            message = body.get("error") or message
            code = body.get("error_code")
        error = NotFoundError if resp.status == 404 else RESTError
        raise error(resp.status, message, code)


async def fetch_emote_set(
    emote_set_id: str,
    *,
    session: aiohttp.ClientSession | None = None,
    base_url: str = DEFAULT_REST_URL,
    timeout: float = DEFAULT_TIMEOUT,
) -> EmoteSet:
    """Fetch an emote set with all of its emotes.

    Raises :class:`~seven_tv_events.NotFoundError` if the set does not exist, and
    :class:`~seven_tv_events.RESTError` for other API errors (e.g. a malformed ID).
    """
    data = await _get_json(session, f"{base_url}/emote-sets/{quote(emote_set_id, safe='')}", timeout)
    return EmoteSet.from_json(data)


async def fetch_user_connection(
    platform: str,
    platform_id: str | int,
    *,
    session: aiohttp.ClientSession | None = None,
    base_url: str = DEFAULT_REST_URL,
    timeout: float = DEFAULT_TIMEOUT,
) -> UserConnection:
    """Look up a 7TV user by their account on another platform.

    ``platform`` is ``"twitch"``, ``"youtube"``, ``"kick"`` or ``"discord"``
    (case-insensitive), and ``platform_id`` the account's ID there, e.g. a
    numeric Twitch user ID. The result's :attr:`~seven_tv_events.UserConnection.emote_set_id`
    is the emote set active on that channel, and the full set and 7TV user are
    embedded as :attr:`~seven_tv_events.UserConnection.emote_set` and
    :attr:`~seven_tv_events.UserConnection.user`.

    Raises :class:`~seven_tv_events.NotFoundError` if no 7TV user has linked that account.

    >>> conn = await fetch_user_connection("twitch", 1277023540)  # doctest: +SKIP
    >>> await client.subscribe("emote_set.update", object_id=conn.emote_set_id)  # doctest: +SKIP
    """
    path = f"users/{quote(platform.lower(), safe='')}/{quote(str(platform_id), safe='')}"
    data = await _get_json(session, f"{base_url}/{path}", timeout)
    return UserConnection.from_json(data)
