"""Typed payload models for the 7TV EventAPI (v3) and the REST objects it refers to.

Every model keeps the untouched JSON it was built from in ``raw`` so fields
added to the API later stay reachable without a library update.
"""

import datetime
import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .enums import CloseCode, ObjectKind, Opcode

__all__ = (
    "Ack",
    "ChangeField",
    "ChangeMap",
    "Dispatch",
    "Emote",
    "EmoteSet",
    "EndOfStream",
    "Error",
    "Heartbeat",
    "Hello",
    "Message",
    "Reconnect",
    "Subscription",
    "User",
    "UserConnection",
    "UserStyle",
    "legacy_id_to_ulid",
)

type JSON = dict[str, Any]

_LEGACY_ID = re.compile(r"[0-9a-fA-F]{24}")
_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def legacy_id_to_ulid(value: str) -> str:
    """Convert a legacy 24-character hex 7TV ID (a MongoDB ObjectID) to its ULID form.

    Any other value is returned unchanged. This is the same conversion the server
    applies: the ObjectID's seconds become the ULID's millisecond timestamp, and its
    remaining 8 bytes become the ULID's random part.

    >>> legacy_id_to_ulid("62cdd34e72a832540de95857")
    '01G7T02T5G00075A1JAG6YJP2Q'
    """
    if not _LEGACY_ID.fullmatch(value):
        return value
    raw = bytes.fromhex(value)
    n = (int.from_bytes(raw[:4]) * 1000) << 80 | int.from_bytes(raw[4:])
    return "".join(_CROCKFORD[(n >> shift) & 31] for shift in range(125, -1, -5))


def _try_enum[E: Enum](enum: type[E], value: Any) -> E | Any:
    try:
        return enum(value)
    except ValueError:
        return value


@dataclass(frozen=True, slots=True)
class Message:
    """The envelope every WebSocket message is wrapped in."""

    op: Opcode | int
    """Message operation code."""
    t: datetime.datetime | None
    """Timestamp of the message's formation."""
    d: JSON
    """Generic data payload."""

    @classmethod
    def from_json(cls, data: Any) -> Message:
        """Parse a message envelope. Raises :class:`ValueError` if it is malformed."""
        if not isinstance(data, dict) or not isinstance(data.get("op"), int):
            raise ValueError(f"not a valid EventAPI message: {data!r:.200}")
        d = data.get("d")
        return cls(op=_try_enum(Opcode, data["op"]), t=_millis(data.get("t")), d=d if isinstance(d, dict) else {})


@dataclass(frozen=True, slots=True)
class Hello:
    """Op 1. Received upon connecting, presents info about the session."""

    heartbeat_interval: int
    """Interval in milliseconds between each heartbeat."""
    session_id: str
    """Unique token for this session, used for resuming and mutating the session."""
    subscription_limit: int
    """The maximum amount of subscriptions this connection can initiate (``-1`` if unlimited)."""
    instance: JSON | None
    """Info about the server instance, e.g. ``{"name": "event-api-7", "population": 97621}``."""
    raw: JSON = field(repr=False)

    @classmethod
    def from_json(cls, data: JSON) -> Hello:
        return cls(
            heartbeat_interval=data["heartbeat_interval"],
            session_id=data["session_id"],
            subscription_limit=data.get("subscription_limit", -1),
            instance=data.get("instance"),
            raw=data,
        )


@dataclass(frozen=True, slots=True)
class Heartbeat:
    """Op 2. Ensures the connection is still alive."""

    count: int
    """The amount of heartbeats so far."""
    raw: JSON = field(repr=False)

    @classmethod
    def from_json(cls, data: JSON) -> Heartbeat:
        return cls(count=data.get("count", 0), raw=data)


@dataclass(frozen=True, slots=True)
class Reconnect:
    """Op 4. The server wants the client to reconnect."""

    reason: str | None
    raw: JSON = field(repr=False)

    @classmethod
    def from_json(cls, data: JSON) -> Reconnect:
        return cls(reason=data.get("reason"), raw=data)


@dataclass(frozen=True, slots=True)
class Ack:
    """Op 5. The server acknowledges an action by the client."""

    command: str
    """The acknowledged opcode in text form, e.g. ``"SUBSCRIBE"``."""
    data: Any
    """Echo of the data sent by the client (for ``RESUME``, the outcome of the resume)."""
    raw: JSON = field(repr=False)

    @classmethod
    def from_json(cls, data: JSON) -> Ack:
        return cls(command=data.get("command", ""), data=data.get("data"), raw=data)


@dataclass(frozen=True, slots=True)
class Error:
    """Op 6. An error occurred."""

    message: str
    fields: dict[str, Any]
    """Extra details about the error, if the server sent any."""
    raw: JSON = field(repr=False)

    @classmethod
    def from_json(cls, data: JSON) -> Error:
        return cls(message=data.get("message", ""), fields=data.get("fields") or {}, raw=data)


def _millis(value: Any) -> datetime.datetime | None:
    if not isinstance(value, int | float) or value <= 0:
        return None
    return datetime.datetime.fromtimestamp(value / 1000, tz=datetime.UTC)


@dataclass(frozen=True, slots=True)
class UserStyle:
    """A user's cosmetics, as embedded in user objects."""

    color: int | None
    """Name color as a signed 32-bit RGBA integer (``None`` if unset)."""
    paint_id: str | None
    badge_id: str | None
    raw: JSON = field(repr=False)

    @classmethod
    def from_json(cls, data: JSON) -> UserStyle:
        return cls(
            color=data.get("color") or None,
            paint_id=data.get("paint_id"),
            badge_id=data.get("badge_id"),
            raw=data,
        )


@dataclass(frozen=True, slots=True)
class UserConnection:
    """A user's account on a linked platform (Twitch, YouTube, Discord, …)."""

    id: str
    """The account's ID on that platform (e.g. a Twitch user ID)."""
    platform: str
    """``"TWITCH"``, ``"YOUTUBE"``, ``"DISCORD"``, or ``"KICK"``."""
    username: str
    display_name: str
    linked_at: datetime.datetime | None
    emote_capacity: int
    emote_set_id: str | None
    """The emote set active on this connection."""
    emote_set: EmoteSet | None = field(repr=False)
    """The full active emote set, when the API embeds it (e.g. :func:`seven_tv_events.rest.fetch_user_connection`)."""
    user: User | None = field(repr=False)
    """The 7TV user owning this connection, when embedded (e.g. :func:`seven_tv_events.rest.fetch_user_connection`)."""
    raw: JSON = field(repr=False)

    @classmethod
    def from_json(cls, data: JSON) -> UserConnection:
        return cls(
            id=str(data.get("id", "")),
            platform=data.get("platform", ""),
            username=data.get("username", ""),
            display_name=data.get("display_name", ""),
            linked_at=_millis(data.get("linked_at")),
            emote_capacity=data.get("emote_capacity", 0),
            emote_set_id=data.get("emote_set_id"),
            emote_set=EmoteSet.from_json(emote_set) if (emote_set := data.get("emote_set")) else None,
            user=User.from_json(user) if (user := data.get("user")) else None,
            raw=data,
        )


@dataclass(frozen=True, slots=True)
class User:
    """A 7TV user, as embedded in events (e.g. :attr:`ChangeMap.actor`)."""

    id: str
    username: str
    display_name: str
    avatar_url: str | None
    style: UserStyle
    role_ids: list[str]
    connections: list[UserConnection]
    raw: JSON = field(repr=False)

    @classmethod
    def from_json(cls, data: JSON) -> User:
        return cls(
            id=data.get("id", ""),
            username=data.get("username", ""),
            display_name=data.get("display_name", ""),
            avatar_url=data.get("avatar_url") or None,
            style=UserStyle.from_json(data.get("style") or {}),
            role_ids=list(data.get("role_ids") or ()),
            connections=[UserConnection.from_json(c) for c in data.get("connections") or ()],
            raw=data,
        )

    def connection(self, platform: str) -> UserConnection | None:
        """The user's first connection on ``platform`` (case-insensitive), if any."""
        platform = platform.upper()
        return next((c for c in self.connections if c.platform.upper() == platform), None)


@dataclass(frozen=True, slots=True)
class Emote:
    """An emote as it is enabled in an emote set.

    This is the shape of :attr:`EmoteSet.emotes` items, and also of the values of
    ``"emotes"`` changes in ``emote_set.update`` dispatches, so it can parse those too::

        for change in event.body.pushed:
            emote = Emote.from_json(change.value)
    """

    id: str
    name: str
    """The name the emote is used by in this set. Differs from :attr:`original_name` if aliased."""
    original_name: str
    """The emote's own name."""
    flags: int
    """Flags of this emote within the set (``1`` = zero-width)."""
    added_at: datetime.datetime | None
    """When the emote was added to the set."""
    actor_id: str | None
    """The user who added the emote to the set."""
    animated: bool
    listed: bool
    owner: User | None
    """The emote's author."""
    host_url: str | None
    """Base CDN URL of the emote's images, e.g. ``"https://cdn.7tv.app/emote/<id>"``; append ``"/1x.webp"``."""
    raw: JSON = field(repr=False)

    @classmethod
    def from_json(cls, data: JSON) -> Emote:
        info = data.get("data") or {}
        host_url = (info.get("host") or {}).get("url") or None
        if host_url and host_url.startswith("//"):
            host_url = "https:" + host_url
        return cls(
            id=data.get("id", ""),
            name=data.get("name", ""),
            original_name=info.get("name") or data.get("name", ""),
            flags=data.get("flags", 0),
            added_at=_millis(data.get("timestamp")),
            actor_id=data.get("actor_id"),
            animated=bool(info.get("animated", False)),
            listed=bool(info.get("listed", False)),
            owner=User.from_json(owner) if (owner := info.get("owner")) else None,
            host_url=host_url,
            raw=data,
        )

    @property
    def aliased(self) -> bool:
        """Whether the emote is used under a different name in this set."""
        return self.name != self.original_name


@dataclass(frozen=True, slots=True)
class EmoteSet:
    """A 7TV emote set, as returned by :func:`seven_tv_events.rest.fetch_emote_set`."""

    id: str
    name: str
    flags: int
    tags: list[str]
    immutable: bool
    privileged: bool
    emotes: list[Emote]
    emote_count: int
    capacity: int
    owner: User | None
    raw: JSON = field(repr=False)

    @classmethod
    def from_json(cls, data: JSON) -> EmoteSet:
        emotes = [Emote.from_json(e) for e in data.get("emotes") or ()]
        return cls(
            id=data.get("id", ""),
            name=data.get("name", ""),
            flags=data.get("flags", 0),
            tags=list(data.get("tags") or ()),
            immutable=bool(data.get("immutable", False)),
            privileged=bool(data.get("privileged", False)),
            emotes=emotes,
            emote_count=data.get("emote_count", len(emotes)),
            capacity=data.get("capacity", 0),
            owner=User.from_json(owner) if (owner := data.get("owner")) else None,
            raw=data,
        )

    def get(self, name: str) -> Emote | None:
        """The emote used by ``name`` in this set (case-sensitive), if any."""
        return next((e for e in self.emotes if e.name == name), None)


@dataclass(frozen=True, slots=True)
class EndOfStream:
    """Op 7. The server will send no further data and imminently end the connection."""

    code: CloseCode | int
    """Close code; see :attr:`CloseCode.reconnect` for whether to reconnect."""
    message: str
    """A text message about the closure."""
    raw: JSON = field(repr=False)

    @classmethod
    def from_json(cls, data: JSON) -> EndOfStream:
        return cls(
            code=_try_enum(CloseCode, data.get("code", 0)),
            message=data.get("message", ""),
            raw=data,
        )


@dataclass(frozen=True, slots=True)
class ChangeField:
    """A single field change inside a :class:`ChangeMap`."""

    key: str
    """The key in context."""
    index: int | None
    """If the field is an array, the index of the item within the array that was updated."""
    type: str | None
    """Value type hint sent by the server, e.g. ``"object"`` (not in the official docs)."""
    nested: bool
    """If true, :attr:`value` is a list of nested :class:`ChangeField`."""
    old_value: Any
    """The previous value."""
    value: Any
    """The new value; always ``list[ChangeField]`` when :attr:`nested` is true."""
    raw: JSON = field(repr=False)

    @classmethod
    def from_json(cls, data: JSON) -> ChangeField:
        nested = bool(data.get("nested", False))
        value = data.get("value")
        if nested and isinstance(value, list):
            value = [cls.from_json(v) for v in value]
        return cls(
            key=data.get("key", ""),
            index=data.get("index"),
            type=data.get("type"),
            nested=nested,
            old_value=data.get("old_value"),
            value=value,
            raw=data,
        )


def _fields(data: JSON, key: str) -> list[ChangeField]:
    return [ChangeField.from_json(f) for f in data.get(key) or ()]


@dataclass(frozen=True, slots=True)
class ChangeMap:
    """Detailed changes carried by a :class:`Dispatch`."""

    id: str
    """The object's ID."""
    kind: ObjectKind | int
    """The object kind."""
    contextual: bool
    """If true, this event represents a change local only to the current session."""
    actor: User | None
    """The user responsible for these changes."""
    added: list[ChangeField]
    updated: list[ChangeField]
    removed: list[ChangeField]
    pushed: list[ChangeField]
    """Items pushed to an array."""
    pulled: list[ChangeField]
    """Items pulled from an array."""
    object: JSON | None
    """The full object, sent with some events (e.g. ``cosmetic.create``, ``entitlement.create``)."""
    raw: JSON = field(repr=False)

    @classmethod
    def from_json(cls, data: JSON) -> ChangeMap:
        return cls(
            id=data.get("id", ""),
            kind=_try_enum(ObjectKind, data.get("kind", 0)),
            contextual=bool(data.get("contextual", False)),
            actor=User.from_json(actor) if (actor := data.get("actor")) else None,
            added=_fields(data, "added"),
            updated=_fields(data, "updated"),
            removed=_fields(data, "removed"),
            pushed=_fields(data, "pushed"),
            pulled=_fields(data, "pulled"),
            object=data.get("object"),
            raw=data,
        )

    def changes(self) -> Iterator[tuple[str, ChangeField]]:
        """Iterate over every change as ``(change_type, field)`` pairs.

        ``change_type`` is one of ``"added"``, ``"updated"``, ``"removed"``,
        ``"pushed"`` or ``"pulled"``.
        """
        for name in ("added", "updated", "removed", "pushed", "pulled"):
            for f in getattr(self, name):
                yield name, f


@dataclass(frozen=True, slots=True)
class Dispatch:
    """Op 0. A standard event message, sent when a subscribed event is emitted."""

    type: str
    """The event type, e.g. ``"emote_set.update"``."""
    body: ChangeMap
    """Detailed changes."""
    timestamp: datetime.datetime | None
    """When the server formed the message."""
    raw: JSON = field(repr=False)

    @classmethod
    def from_json(cls, data: JSON, timestamp: datetime.datetime | None = None) -> Dispatch:
        return cls(
            type=data.get("type", ""),
            body=ChangeMap.from_json(data.get("body") or {}),
            timestamp=timestamp,
            raw=data,
        )


@dataclass(frozen=True, slots=True, init=False)
class Subscription:
    """A subscription: an event type plus conditions that filter it.

    Hashable, so two subscriptions with the same type and conditions compare
    equal regardless of condition order.

    A legacy 24-character hex ID in the ``object_id`` condition is converted to a
    ULID, as the server does, so it matches the IDs in acks and events. Other
    conditions are kept as given (the server doesn't convert them either):

    >>> Subscription("emote_set.update", object_id="62cdd34e72a832540de95857")
    Subscription('emote_set.update', {'object_id': '01G7T02T5G00075A1JAG6YJP2Q'})
    """

    type: str
    _condition: tuple[tuple[str, str], ...]

    def __init__(
        self,
        type: str,
        condition: Mapping[str, str] | None = None,
        /,
        **conditions: str,
    ) -> None:
        merged = {**(condition or {}), **conditions}
        object.__setattr__(self, "type", str(type))
        normalized = ((k, legacy_id_to_ulid(str(v)) if k == "object_id" else str(v)) for k, v in merged.items())
        object.__setattr__(self, "_condition", tuple(sorted(normalized)))

    @property
    def condition(self) -> dict[str, str]:
        """Conditions that filter the events of this subscription."""
        return dict(self._condition)

    def to_json(self) -> JSON:
        return {"type": self.type, "condition": self.condition}

    def __repr__(self) -> str:
        return f"Subscription({self.type!r}, {self.condition!r})"
