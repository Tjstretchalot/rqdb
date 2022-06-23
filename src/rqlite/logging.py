from typing import Any, Callable, Optional, TypedDict
import dataclasses
import logging


class LogMessageConfig(TypedDict):
    """Configures a single log message within rqlite."""

    enabled: bool
    """True if the message should be logged, False otherwise. If not
    present, assumed to be True
    """

    method: Callable[[str], Any]
    """The function to call to log the message. If not present,
    then this will be set based on the level of the message.
    For example, a level of DEBUG implies that the method is
    effectively logging.debug.

    The method should support "exc_info=True" as a keyword argument.
    """

    level: int
    """The level of the message. If not present, assumed to be
    logging.DEBUG. The level does not have to be one of the default
    logging levels - the method will be a partial variant of logging.log
    with the level as the first argument.

    The level is ignored if the method is set.
    """

    max_length: Optional[int]
    """The approximate maximum length of the message. This may be
    implemented differently depending on which message is being
    configured. If not present assumed to be None, for no maximum
    length.
    """


@dataclasses.dataclass(frozen=True)
class LogConfig:
    """Describes the configuration of rqlite's logging."""

    read_start: LogMessageConfig = dataclasses.field(
        default_factory=lambda: LogMessageConfig(enabled=True, level=logging.DEBUG)
    )
    """Configures the message to log when cursor.execute
    is called with a SELECT query.
    """

    read_response: LogMessageConfig = dataclasses.field(
        default_factory=lambda: LogMessageConfig(enabled=True, level=logging.DEBUG)
    )
    """Configures the message to log when we get the response
    from the server for a SELECT query.
    """

    read_stale: LogMessageConfig = dataclasses.field(
        default_factory=lambda: LogMessageConfig(enabled=True, level=logging.DEBUG)
    )
    """Configures the message to log when we get a response from
    the server for a SELECT query, but the response indicates we
    must retry because the data is not sufficiently fresh. This
    occurs only on reads with the read consistency level "none".
    """

    write_start: LogMessageConfig = dataclasses.field(
        default_factory=lambda: LogMessageConfig(enabled=True, level=logging.DEBUG)
    )
    """Configures the message to log when cursor.execute
    is called with a non-SELECT query.
    """

    write_response: LogMessageConfig = dataclasses.field(
        default_factory=lambda: LogMessageConfig(enabled=True, level=logging.DEBUG)
    )
    """Configures the message to log when we get the response
    from the server for a non-SELECT query.
    """

    connect_timeout: LogMessageConfig = dataclasses.field(
        default_factory=lambda: LogMessageConfig(enabled=True, level=logging.WARNING)
    )
    """Configures the message to log when a connection attempt
    to one of the host nodes fails.
    """

    hosts_exhausted: LogMessageConfig = dataclasses.field(
        default_factory=lambda: LogMessageConfig(enabled=True, level=logging.CRITICAL)
    )
    """Configures the message to log when we are going to give
    up on a given query because we have exhausted all attempts on
    all nodes. This implies the cluster is unresponsive or we cannot
    reach the cluster.
    """

    non_ok_response: LogMessageConfig = dataclasses.field(
        default_factory=lambda: LogMessageConfig(enabled=True, level=logging.WARNING)
    )
    """Configures the message to log when we get a response from
    the server that is not OK or is a redirect when one is not
    expected, such as when we have exceeded the maximum number of
    redirects.
    """

    backup_start: LogMessageConfig = dataclasses.field(
        default_factory=lambda: LogMessageConfig(enabled=True, level=logging.INFO)
    )
    """Configures the message to log when we start attempting a backup."""

    backup_end: LogMessageConfig = dataclasses.field(
        default_factory=lambda: LogMessageConfig(enabled=True, level=logging.INFO)
    )
    """Configures the message to log when we finish attempting a backup."""


DISABLED_LOG_CONFIG = LogConfig(
    read_start=LogMessageConfig(enabled=False),
    read_response=LogMessageConfig(enabled=False),
    read_stale=LogMessageConfig(enabled=False),
    write_start=LogMessageConfig(enabled=False),
    write_response=LogMessageConfig(enabled=False),
    connect_timeout=LogMessageConfig(enabled=False),
    hosts_exhausted=LogMessageConfig(enabled=False),
    non_ok_response=LogMessageConfig(enabled=False),
    backup_start=LogMessageConfig(enabled=False),
    backup_end=LogMessageConfig(enabled=False),
)
"""The log configuration which disables all logging."""


def log(
    config: LogMessageConfig,
    msg_supplier: Callable[[Optional[int]], str],
    exc_info: bool = False,
) -> None:
    """Logs a message if the config is enabled.

    Args:
        config: The configuration of the message to log.
        msg_supplier: A function which returns the message to log.
            Passed the approximate length of the message if there is
            one.
        exc_info: True to pass the current exception to the logger,
            False not to.
    """
    if not config.get("enabled", True):
        return

    max_length = config.get("max_length", None)
    message = msg_supplier(max_length)

    method = config.get("method", None)
    if method is not None:
        if not exc_info:
            method(message)
        else:
            method(message, exc_info=True)
        return

    level = config.get("level", logging.DEBUG)
    logging.log(level, message, exc_info=exc_info)
