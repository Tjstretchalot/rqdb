from typing import (
    Any,
    Callable,
    Iterable,
    List,
    Literal,
    Optional,
    TypedDict,
    Union,
    Protocol,
    TYPE_CHECKING,
)
import dataclasses
import logging

if TYPE_CHECKING:
    from rqdb.result import BulkResult


class LogMethod(Protocol):
    def __call__(self, msg: str, *, exc_info: bool = False) -> Any:
        ...


QueryInfoRequestType = Literal[
    "execute-read",
    "execute-write",
    "executemany",
    "executeunified-readonly",
    "executeunified-write",
]


@dataclasses.dataclass
class QueryInfoLazy:
    """
    Potentially lazily initialized query information.
    """

    operations: Union[Iterable[str], Callable[[], Iterable[str]]]
    """The operations as a list of SQL queries. May be built lazily."""
    params: Union[Iterable[Iterable[Any]], Callable[[], Iterable[Iterable[Any]]]]
    """The parameters for each query in operations. May be built lazily."""

    request_type: Union[QueryInfoRequestType, Callable[[], QueryInfoRequestType]]
    """
    The type of request that was made, may be determined lazily. This is one of
    - "execute-read": A single SELECT or EXPLAIN query via execute() or explain()
    - "execute-write": A single non-SELECT query via execute()
    - "executemany": A single execute_many() request, which is always treated as write
    - "executeunified-readonly": A single execute_unified() request where all of the
        queries are SELECT or EXPLAIN
    - "executeunified-write": A single execute_unified() request where at least one
        of the queries is not SELECT or EXPLAIN
    """
    consistency: Literal["none", "weak", "strong"]
    """
    The consistency level that was used for the request. Only matters if the
    request is a read (execute-read or executeunified-readonly).
    """
    freshness: str
    """
    The minimum freshness to use, e.g., 5m. Only relevant for read queries at the
    none consistency level.
    """


@dataclasses.dataclass
class QueryInfo:
    """
    The information about a query as it is passed to the slow query log
    method, before augmenting information that can only be known when the
    request is made
    """

    operations: Iterable[str]
    """The operations as a list of SQL queries. May be built lazily."""
    params: Iterable[Iterable[Any]]
    """The parameters for each query in operations. May be built lazily."""

    request_type: Literal[
        "execute-read",
        "execute-write",
        "executemany",
        "executeunified-readonly",
        "executeunified-write",
    ]
    """
    The type of request that was made. This is one of
    - "execute-read": A single SELECT or EXPLAIN query via execute() or explain()
    - "execute-write": A single non-SELECT query via execute()
    - "executemany": A single execute_many() request, which is always treated as write
    - "executeunified-readonly": A single execute_unified() request where all of the
        queries are SELECT or EXPLAIN
    - "executeunified-write": A single execute_unified() request where at least one
        of the queries is not SELECT or EXPLAIN
    """
    consistency: Literal["none", "weak", "strong"]
    """
    The consistency level that was used for the request. Only matters if the
    request is a read (execute-read or executeunified-readonly).
    """
    freshness: str
    """
    The minimum freshness to use, e.g., 5m. Only relevant for read queries at the
    none consistency level.
    """


class SlowQueryLogMethod(Protocol):
    def __call__(
        self,
        info: QueryInfo,
        /,
        *,
        duration_seconds: float,
        host: str,
        response_size_bytes: int,
        started_at: float,
        ended_at: float,
    ) -> None:
        """Called to log a slow query. Provided the operations and parameters in the
        same format as executemany2, then all the relevant context about the query
        that exceeded the threshold.

        Args:
            info (QueryInfo): The information about the query.
            duration_seconds (float): How long it took between us initiating the
                request and us receiving the response, in seconds
            host (str): The host that the request was made to.
            response_size_bytes (int): The size of the response in bytes, as reported by the
                content-length header. 0 if the content-length header was not present in the
                response.
            started_at (float): The time that the request was initiated, in seconds since the epoch.
            ended_at (float): The time that the response was received, in seconds since the epoch.
        """
        ...


class LogMessageConfig(TypedDict):
    """Configures a single log message within rqlite."""

    enabled: bool
    """True if the message should be logged, False otherwise. If not
    present, assumed to be True
    """

    method: LogMethod
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


class LevelOnlyMessageConfig(TypedDict):
    """Used to appease the type system when initializing a log message config
    using only a debug level.
    """

    enabled: bool
    """See LogMessageConfig"""
    level: int
    """See LogMessageConfig"""


class DisabledMessageConfig(TypedDict):
    """Used to appease the type system when initializing a log message config
    which is disabled, since the other arguments are not needed.
    """

    enabled: Literal[False]
    """See LogMessageConfig"""


class SlowQueryLogMessageConfig(TypedDict):
    """The configuration available for the slow query log message, which
    is a special case because it's expected that these messages will be
    sent to a more visible place and formatted in a special way.
    """

    enabled: bool
    """True if the message should be logged, False otherwise. If not
    present, assumed to be True
    """

    threshold_seconds: float
    """The threshold in seconds for a query to be considered slow. Can
    be set to for detailed timing information on every query.
    """

    method: SlowQueryLogMethod
    """The function to call to log the message. If not present,
    then this will be set based on the level of the message.
    For example, a level of DEBUG implies that the method is
    effectively logging.debug.

    The method should support "exc_info=True" as a keyword argument.
    """


ForgivingLogMessageConfigT = Union[
    LogMessageConfig,
    LevelOnlyMessageConfig,
    DisabledMessageConfig,
]


@dataclasses.dataclass(frozen=True)
class LogConfig:
    """Describes the configuration of rqlite's logging."""

    read_start: ForgivingLogMessageConfigT = dataclasses.field(
        default_factory=lambda: LevelOnlyMessageConfig(
            enabled=True, level=logging.DEBUG
        )
    )
    """Configures the message to log when cursor.execute
    is called with a SELECT query.
    """

    read_response: ForgivingLogMessageConfigT = dataclasses.field(
        default_factory=lambda: LevelOnlyMessageConfig(
            enabled=True, level=logging.DEBUG
        )
    )
    """Configures the message to log when we get the response
    from the server for a SELECT query.
    """

    read_stale: ForgivingLogMessageConfigT = dataclasses.field(
        default_factory=lambda: LevelOnlyMessageConfig(
            enabled=True, level=logging.DEBUG
        )
    )
    """Configures the message to log when we get a response from
    the server for a SELECT query, but the response indicates we
    must retry because the data is not sufficiently fresh. This
    occurs only on reads with the read consistency level "none".
    """

    write_start: ForgivingLogMessageConfigT = dataclasses.field(
        default_factory=lambda: LevelOnlyMessageConfig(
            enabled=True, level=logging.DEBUG
        )
    )
    """Configures the message to log when cursor.execute
    is called with a non-SELECT query.
    """

    write_response: ForgivingLogMessageConfigT = dataclasses.field(
        default_factory=lambda: LevelOnlyMessageConfig(
            enabled=True, level=logging.DEBUG
        )
    )
    """Configures the message to log when we get the response
    from the server for a non-SELECT query.
    """

    connect_timeout: ForgivingLogMessageConfigT = dataclasses.field(
        default_factory=lambda: LevelOnlyMessageConfig(
            enabled=True, level=logging.WARNING
        )
    )
    """Configures the message to log when a connection attempt
    to one of the host nodes fails.
    """

    hosts_exhausted: ForgivingLogMessageConfigT = dataclasses.field(
        default_factory=lambda: LevelOnlyMessageConfig(
            enabled=True, level=logging.CRITICAL
        )
    )
    """Configures the message to log when we are going to give
    up on a given query because we have exhausted all attempts on
    all nodes. This implies the cluster is unresponsive or we cannot
    reach the cluster.
    """

    non_ok_response: ForgivingLogMessageConfigT = dataclasses.field(
        default_factory=lambda: LevelOnlyMessageConfig(
            enabled=True, level=logging.WARNING
        )
    )
    """Configures the message to log when we get a response from
    the server that is not OK or is a redirect when one is not
    expected, such as when we have exceeded the maximum number of
    redirects.
    """

    slow_query: Union[
        SlowQueryLogMessageConfig, DisabledMessageConfig
    ] = dataclasses.field(default_factory=lambda: DisabledMessageConfig(enabled=False))
    """Configures the message to log when we get a response from
    the server, but that response takes longer than a certain
    threshold to arrive.
    """

    backup_start: ForgivingLogMessageConfigT = dataclasses.field(
        default_factory=lambda: LevelOnlyMessageConfig(enabled=True, level=logging.INFO)
    )
    """Configures the message to log when we start attempting a backup."""

    backup_end: ForgivingLogMessageConfigT = dataclasses.field(
        default_factory=lambda: LevelOnlyMessageConfig(enabled=True, level=logging.INFO)
    )
    """Configures the message to log when we finish attempting a backup."""


DISABLED_LOG_CONFIG = LogConfig(
    read_start=DisabledMessageConfig(enabled=False),
    read_response=DisabledMessageConfig(enabled=False),
    read_stale=DisabledMessageConfig(enabled=False),
    write_start=DisabledMessageConfig(enabled=False),
    write_response=DisabledMessageConfig(enabled=False),
    connect_timeout=DisabledMessageConfig(enabled=False),
    hosts_exhausted=DisabledMessageConfig(enabled=False),
    non_ok_response=DisabledMessageConfig(enabled=False),
    slow_query=DisabledMessageConfig(enabled=False),
    backup_start=DisabledMessageConfig(enabled=False),
    backup_end=DisabledMessageConfig(enabled=False),
)
"""The log configuration which disables all logging."""


def log(
    config: ForgivingLogMessageConfigT,
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
