"""Exceptions raised by seven_tv_events."""

from .enums import CloseCode

__all__ = (
    "ClosedError",
    "EventAPIError",
    "NotConnectedError",
    "NotFoundError",
    "RESTError",
    "SubscriptionError",
    "SubscriptionLimitError",
)


class EventAPIError(Exception):
    """Base class for every exception raised by this library."""


class NotConnectedError(EventAPIError):
    """The operation requires a live, ready connection."""


class SubscriptionError(EventAPIError):
    """A subscription operation was invalid (e.g. duplicate or unknown subscription)."""


class SubscriptionLimitError(SubscriptionError):
    """Subscribing would exceed the session's ``subscription_limit``."""


class ClosedError(EventAPIError):
    """The server closed the connection in a way the client must not retry.

    Raised by :meth:`seven_tv_events.Client.run` for close codes whose reconnect policy is
    :attr:`~seven_tv_events.ReconnectPolicy.NO`. These indicate a client bug.
    """

    def __init__(self, code: CloseCode | int | None, message: str = "") -> None:
        self.code = code
        self.message = message
        name = code.name if isinstance(code, CloseCode) else code
        super().__init__(f"connection closed ({name}): {message}" if message else f"connection closed ({name})")


class RESTError(EventAPIError):
    """A 7TV REST API request failed with an error status.

    Network failures are not wrapped: they surface as :class:`aiohttp.ClientError`.
    """

    def __init__(self, status: int, message: str = "", error_code: int | None = None) -> None:
        self.status = status
        """The HTTP status code."""
        self.message = message
        """The API's error message, e.g. ``"emote set not found"``."""
        self.error_code = error_code
        """The API's numeric error code, if it sent one."""
        super().__init__(f"HTTP {status}: {message}" if message else f"HTTP {status}")


class NotFoundError(RESTError):
    """The requested object does not exist (HTTP 404)."""
