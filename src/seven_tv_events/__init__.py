"""Unofficial asynchronous Python client for the 7TV EventAPI (v3) over WebSocket.

See https://7tv.app/api/docs#EventAPI for the protocol documentation.
:mod:`seven_tv_events.rest` has helpers for the few REST lookups EventAPI users usually need.
"""

from importlib import metadata as _metadata

from . import rest
from .client import DEFAULT_URL, Client, DispatchStream, LifecycleEvent
from .enums import CloseCode, ObjectKind, Opcode, ReconnectPolicy, SubscriptionType
from .errors import (
    ClosedError,
    EventAPIError,
    NotConnectedError,
    NotFoundError,
    RESTError,
    SubscriptionError,
    SubscriptionLimitError,
)
from .models import (
    Ack,
    ChangeField,
    ChangeMap,
    Dispatch,
    Emote,
    EmoteSet,
    EndOfStream,
    Error,
    Heartbeat,
    Hello,
    Message,
    Reconnect,
    Subscription,
    User,
    UserConnection,
    UserStyle,
    legacy_id_to_ulid,
)

__all__ = (
    "DEFAULT_URL",
    "Ack",
    "ChangeField",
    "ChangeMap",
    "Client",
    "CloseCode",
    "ClosedError",
    "Dispatch",
    "DispatchStream",
    "Emote",
    "EmoteSet",
    "EndOfStream",
    "Error",
    "EventAPIError",
    "Heartbeat",
    "Hello",
    "LifecycleEvent",
    "Message",
    "NotConnectedError",
    "NotFoundError",
    "ObjectKind",
    "Opcode",
    "RESTError",
    "Reconnect",
    "ReconnectPolicy",
    "Subscription",
    "SubscriptionError",
    "SubscriptionLimitError",
    "SubscriptionType",
    "User",
    "UserConnection",
    "UserStyle",
    "legacy_id_to_ulid",
    "rest",
)

__version__ = _metadata.version("seven-tv-events")  # single source of truth: pyproject.toml
