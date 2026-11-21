"""Enumerations for the 7TV EventAPI (v3)."""

from enum import IntEnum, StrEnum

__all__ = (
    "CloseCode",
    "ObjectKind",
    "Opcode",
    "ReconnectPolicy",
    "SubscriptionType",
)


class Opcode(IntEnum):
    """Operation codes identifying what kind of payload a message carries."""

    # Sent by the server
    DISPATCH = 0
    """A standard event message, sent when a subscribed event is emitted."""
    HELLO = 1
    """Received upon connecting, presents info about the session."""
    HEARTBEAT = 2
    """Ensures the connection is still alive."""
    RECONNECT = 4
    """Server wants the client to reconnect."""
    ACK = 5
    """Server acknowledges an action by the client."""
    ERROR = 6
    """An error occurred, you should log this."""
    END_OF_STREAM = 7
    """The server will send no further data and imminently end the connection."""

    # Sent by the client
    IDENTIFY = 33
    """Authenticate with an account."""
    RESUME = 34
    """Try to resume a previous session."""
    SUBSCRIBE = 35
    """Watch for changes on specific objects or sources."""
    UNSUBSCRIBE = 36
    """Stop listening for changes."""
    SIGNAL = 37
    BRIDGE = 38

    @property
    def command(self) -> str:
        """Text form of the opcode, as used in ``Ack.command`` (e.g. ``"SUBSCRIBE"``)."""
        return self.name


class ReconnectPolicy(StrEnum):
    """Whether a client should reconnect after a given close code."""

    YES = "yes"
    """Reconnect (resuming the session if possible)."""
    NO = "no"
    """Do not reconnect: the client did something wrong and must be fixed."""
    LATER = "later"
    """Reconnect, but only after waiting a while (e.g. maintenance)."""
    MAYBE = "maybe"
    """Reconnecting may work, but only with a backoff."""


class CloseCode(IntEnum):
    """Close codes the server uses to explain why a connection ended."""

    SERVER_ERROR = 4000
    """An error occurred on the server's end."""
    UNKNOWN_OPERATION = 4001
    """The client sent an unexpected opcode."""
    INVALID_PAYLOAD = 4002
    """The client sent a payload that couldn't be decoded."""
    AUTH_FAILURE = 4003
    """The client unsuccessfully tried to identify."""
    ALREADY_IDENTIFIED = 4004
    """The client wanted to identify again."""
    RATE_LIMITED = 4005
    """The client is being rate-limited."""
    RESTART = 4006
    """The server is restarting and the client should reconnect."""
    MAINTENANCE = 4007
    """The server is in maintenance mode and not accepting connections."""
    TIMEOUT = 4008
    """The client was idle for too long."""
    ALREADY_SUBSCRIBED = 4009
    """The client tried to subscribe to an event twice."""
    NOT_SUBSCRIBED = 4010
    """The client tried to unsubscribe from an event they weren't subscribing to."""
    INSUFFICIENT_PRIVILEGE = 4011
    """The client did something that they did not have permission for."""

    @property
    def reconnect(self) -> ReconnectPolicy:
        """What a client should do after the server closed with this code."""
        return _RECONNECT_POLICY[self]


_RECONNECT_POLICY: dict[CloseCode, ReconnectPolicy] = {
    CloseCode.SERVER_ERROR: ReconnectPolicy.YES,
    CloseCode.UNKNOWN_OPERATION: ReconnectPolicy.NO,
    CloseCode.INVALID_PAYLOAD: ReconnectPolicy.NO,
    CloseCode.AUTH_FAILURE: ReconnectPolicy.NO,
    CloseCode.ALREADY_IDENTIFIED: ReconnectPolicy.NO,
    CloseCode.RATE_LIMITED: ReconnectPolicy.MAYBE,
    CloseCode.RESTART: ReconnectPolicy.YES,
    CloseCode.MAINTENANCE: ReconnectPolicy.LATER,
    CloseCode.TIMEOUT: ReconnectPolicy.YES,
    CloseCode.ALREADY_SUBSCRIBED: ReconnectPolicy.NO,
    CloseCode.NOT_SUBSCRIBED: ReconnectPolicy.NO,
    CloseCode.INSUFFICIENT_PRIVILEGE: ReconnectPolicy.MAYBE,
}


class ObjectKind(IntEnum):
    """Kind of object a :class:`~seven_tv_events.models.ChangeMap` refers to."""

    USER = 1
    EMOTE = 2
    EMOTE_SET = 3
    ROLE = 4
    ENTITLEMENT = 5
    BAN = 6
    MESSAGE = 7
    REPORT = 8
    PRESENCE = 9
    COSMETIC = 10


class SubscriptionType(StrEnum):
    """Common subscription (event) types.

    Any string is accepted wherever a subscription type is expected, so types
    not listed here (and wildcards like ``"emote_set.*"``) can still be used.
    """

    SYSTEM_ANNOUNCEMENT = "system.announcement"
    SYSTEM_ALL = "system.*"

    EMOTE_CREATE = "emote.create"
    EMOTE_UPDATE = "emote.update"
    EMOTE_DELETE = "emote.delete"
    EMOTE_ALL = "emote.*"

    EMOTE_SET_CREATE = "emote_set.create"
    EMOTE_SET_UPDATE = "emote_set.update"
    EMOTE_SET_DELETE = "emote_set.delete"
    EMOTE_SET_ALL = "emote_set.*"

    USER_CREATE = "user.create"
    USER_UPDATE = "user.update"
    USER_DELETE = "user.delete"
    USER_ADD_CONNECTION = "user.add_connection"
    USER_UPDATE_CONNECTION = "user.update_connection"
    USER_DELETE_CONNECTION = "user.delete_connection"
    USER_ALL = "user.*"

    COSMETIC_CREATE = "cosmetic.create"
    COSMETIC_UPDATE = "cosmetic.update"
    COSMETIC_DELETE = "cosmetic.delete"
    COSMETIC_ALL = "cosmetic.*"

    ENTITLEMENT_CREATE = "entitlement.create"
    ENTITLEMENT_UPDATE = "entitlement.update"
    ENTITLEMENT_DELETE = "entitlement.delete"
    ENTITLEMENT_ALL = "entitlement.*"
