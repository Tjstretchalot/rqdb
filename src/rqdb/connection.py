from typing import (
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    TypeVar,
    Union,
    Protocol,
    cast,
    TYPE_CHECKING,
)
from rqdb.errors import (
    ConnectError,
    MaxAttemptsError,
    MaxRedirectsError,
    UnexpectedResponse,
)
import rqdb.logging
import random
import requests
from rqdb.cursor import Cursor
from rqdb.types import ReadConsistency, DEFAULT_READ_CONSISTENCY
import secrets
import time
import urllib.parse
import inspect
from dataclasses import dataclass

if TYPE_CHECKING:
    from rqdb.result import BulkResult


class SyncWritableIO(Protocol):
    def write(self, data: bytes, /) -> Any:
        ...


@dataclass(frozen=True)
class FetchResponseFullResult:
    response: requests.Response
    """The underlying response"""

    method: Literal["GET", "POST"]
    """The method attempted"""

    uri: str
    """The uri attempted"""

    json: Any
    """The json data sent with the request or None"""

    headers: Optional[Dict[str, Any]]
    """The headers sent with the request"""

    stream: bool
    """If streaming was requested"""

    requested_initial_host: Optional[Tuple[str, int]]
    """The value provided for initial_host to the request; if None, we
    selected a random host, otherwise, we tried the provided host first
    """

    node_path: List[Tuple[str, Exception]]
    """The nodes that failed before we got to the final host"""

    lazy_query_info: Optional[rqdb.logging.QueryInfoLazy]
    """The query info provided, which may require calling to get the actual
    values
    """

    slow_query_handled_on_success: bool
    """If we were told that slow queries on successful (200-299) status codes
    would be handled by the caller (True) or us (False)
    """

    host: Tuple[str, int]
    """The final host attempted as a (hostname, port) pair"""

    started_at_wall: float
    """The time we started the final host attempt in seconds since the epoch. This
    is useful primarily if you are interested in understanding if the request
    took a long time because of retries (usually not that interesting) or
    because the actual request took a long time for us to process (usually more
    interesting).
    """

    ended_at_wall: float
    """The time we ended the final host attempt in seconds since the epoch. This
    is useful primarily if you are interested in understanding if the request
    took a long time because of retries (usually not that interesting) or
    because the actual request took a long time for us to process (usually more
    interesting).
    """

    request_time_perf: float
    """The time it took to make the request in fractional seconds on our
    performance clock. This is not affected by changes to the local clock
    but is still affected by network latency or local cpu contention
    """

    response_size_bytes: int
    """The size of the response in bytes; generally for checking if the
    request time could reasonably be explained by transfer speed because of
    a query without a limit. For this to be accurate the server needs to
    give us an accurate Content-Length header, since we don't read the response
    eagerly.
    """


T = TypeVar("T")


class Connection:
    """Describes a synchronous description of a pool of nodes which
    make up a rqlite cluster. There is no actual connection in this
    class, however the name is chosed to signify its similarity to
    a Connection in a DBAPI 2.0 style api.
    """

    def __init__(
        self,
        hosts: List[str],
        timeout: int = 5,
        max_redirects: int = 2,
        max_attempts_per_host: int = 2,
        read_consistency: ReadConsistency = DEFAULT_READ_CONSISTENCY,
        freshness: str = "0",
        log: Union[rqdb.logging.LogConfig, bool] = True,
    ):
        """Initializes a new synchronous Connection. This is typically
        called with the alias rqlite.connect

        Args:
            hosts (list[str]): A list of hostnames or ip addresses where
                the rqlite cluster is running. Each address can be a
                host:port pair - if the port is omitted, the default
                port of 4001 is used. If the host is specified in ipv6
                format, then it should be surrounded in square brackets
                if the port is specified, i.e., [::1]:4001.

            timeout (int): The number of seconds to wait for a response
                from a host before giving up.

            max_redirects (int): The maximum number of redirects to follow
                when executing a query.

            max_attempts_per_host (int): The maximum number of attempts to
                make to a given host before giving up.

            read_consistency (str): The read consistency to use for cursors
                by default.

            freshness (str): Default freshness value for none-consistency reads.
                Guarrantees the response is no more stale than the given duration,
                specified as a golang Duration string, e.g., "5m" for 5 minutes

            log (bool or LogConfig): If True, logs will have the default settings.
                If False, logs will be disabled. If a LogConfig, the
                configuration of the logs.
        """
        if log is True:
            log_config = rqdb.logging.LogConfig()
        elif log is False:
            log_config = rqdb.logging.DISABLED_LOG_CONFIG
        else:
            log_config = log

        self.hosts: List[Tuple[str, int]] = [parse_host(h) for h in hosts]
        """The host addresses to connect to as a list of (hostname, port) pairs."""

        self.timeout: int = timeout
        """The number of seconds to wait for a response from a node."""

        self.max_redirects: int = max_redirects
        """The maximum number of redirects to follow when executing a query."""

        self.max_attempts_per_host: int = max_attempts_per_host
        """The maximum number of attempts to make to a node before giving up. We
        will attempt all nodes in a random order this many times per node before
        giving up.
        """

        self.read_consistency: ReadConsistency = read_consistency
        """The default read consistency when initializing cursors."""

        self.freshness: str = freshness
        """Default freshness value for none-consistency reads."""

        self.log_config = log_config
        """The log configuration for this connection and all cursors it creates."""

    def cursor(
        self,
        read_consistency: Optional[ReadConsistency] = None,
        freshness: Optional[str] = None,
    ) -> "Cursor":
        """Creates a new cursor for this connection.

        Args:
            read_consistency: The read consistency to use for this cursor. If
                None, the default read consistency for this connection will be
                used.
            freshness: The freshness value to use for this cursor. If None, the
                default freshness value for this connection will be used.

        Returns:
            A new cursor for this connection.
        """
        if read_consistency is None:
            read_consistency = self.read_consistency
        if freshness is None:
            freshness = self.freshness
        return Cursor(self, read_consistency, freshness)

    def try_hosts(
        self,
        attempt_host: Callable[
            [Tuple[str, int], List[Tuple[str, Exception]]], Optional[T]
        ],
        /,
        *,
        initial_host: Optional[Tuple[str, int]] = None,
    ) -> T:
        """Given a function like

        ```py
        def attempt_host(host: Tuple[str, int], node_path: List[Tuple[str, Exception]]) -> Optional[str]:
            try:
                # do something with the host
                return 'result'
            except Exception as e: # only catch exceptions that should continue to the next host
                node_path.append((host, e))
                return None
        ```

        This will call `attempt_host` on each host in the connection in a random
        order until a non-None value is returned or the maximum number of attempts
        per host is reached.

        Args:
            attempt_host: The function to call on each host. It should return None
                if the host failed but we should continue to the next host, or a
                non-None value if the host succeeded. It should raise an exception if
                the host failed and we should not continue to the next host.
            initial_host: The host to try first. If None, a random host will be chosen.
                This does not need to be a host in self.hosts, but if it is not, it
                will not be included in the count for the maximum number of attempts
        Raises:
            MaxAttemptsError: If the maximum number of attempts is reached before
                we get a successful response.
            Exception: any errors raised by `attempt_host`
        """
        node_path: List[Tuple[str, Exception]] = []

        first_node_idx: Optional[int] = None
        if initial_host is not None:
            try:
                first_node_idx = self.hosts.index(initial_host)
            except ValueError:
                if resp := attempt_host(initial_host, node_path):
                    return resp

        if first_node_idx is None:
            first_node_idx = random.randrange(0, len(self.hosts))

        if resp := attempt_host(self.hosts[first_node_idx], node_path):
            return resp

        remaining_nodes = [
            h for idx, h in enumerate(self.hosts) if idx != first_node_idx
        ]
        random.shuffle(remaining_nodes)
        node_ordering = [self.hosts[first_node_idx]] + remaining_nodes
        index = 0
        attempt = 1

        while index < len(self.hosts) or attempt < self.max_attempts_per_host:
            if index + 1 < len(self.hosts):
                index += 1
            else:
                index = 0
                attempt += 1

            if resp := attempt_host(node_ordering[index], node_path):
                return resp

        raise MaxAttemptsError(node_path)

    def fetch_response(
        self,
        method: Literal["GET", "POST"],
        uri: str,
        json: Any = None,
        headers: Optional[Dict[str, Any]] = None,
        stream: bool = False,
        initial_host: Optional[Tuple[str, int]] = None,
        query_info: Optional[rqdb.logging.QueryInfoLazy] = None,
        slow_query_handled_on_success: bool = False,
    ) -> requests.Response:
        """Fetches a response from the server by requesting it from a random node. If
        a connection error occurs, this method will retry the request on a different
        node until it succeeds or the maximum number of attempts is reached.

        Prefer fetch_response_full in most circumstances, which has the same signature
        but provides more information in the response.

        Args:
            method (str): The HTTP method to use for the request.
            uri (str): The URI of the request.
            json (dict): The json data to send with the request.
            headers (dict): The headers to send with the request.
            stream (bool): If True, the response will be streamed.
            initial_host (Optional[Tuple[str, int]]): The host to try first. If None,
                a random host will be chosen.
            query_info (Optional[rqdb.logging.QueryInfo]): The query info for slow
                query logging, or None to disable slow query logging regardless of
                the log configuration.
            slow_query_handled_on_success (bool): If true, the caller assumes responsibility
                for slow query logging if the response is "successful" (i.e., status code
                200-299, which could still mean the SQL query failed). This is desirable if
                more granular timing information is available in the response body, such that
                we can discount the effect of the async thread being blocked on the response
                time, or we can more accurately determine which part of the request took time.

        Returns:
            requests.Response: If a successful response is received, it is returned.

        Raises:
            MaxAttemptsError: If the maximum number of attempts is reached before
                we get a successful response.
            UnexpectedResponse: If one of the rqlite nodes returns a response
                we didn't expect
        """
        return self.fetch_response_full(
            method,
            uri,
            json,
            headers,
            stream,
            initial_host,
            query_info,
            slow_query_handled_on_success,
        ).response

    def fetch_response_full(
        self,
        method: Literal["GET", "POST"],
        uri: str,
        json: Any = None,
        headers: Optional[Dict[str, Any]] = None,
        stream: bool = False,
        initial_host: Optional[Tuple[str, int]] = None,
        query_info: Optional[rqdb.logging.QueryInfoLazy] = None,
        slow_query_handled_on_success: bool = False,
    ) -> FetchResponseFullResult:
        """Fetches a response from the server by requesting it from a random node. If
        a connection error occurs, this method will retry the request on a different
        node until it succeeds or the maximum number of attempts is reached.

        Args:
            method (str): The HTTP method to use for the request.
            uri (str): The URI of the request.
            json (dict): The json data to send with the request.
            headers (dict): The headers to send with the request.
            stream (bool): If True, the response will be streamed.
            initial_host (Optional[Tuple[str, int]]): The host to try first. If None,
                a random host will be chosen.
            query_info (Optional[rqdb.logging.QueryInfo]): The query info for slow
                query logging, or None to disable slow query logging regardless of
                the log configuration.
            slow_query_handled_on_success (bool): If true, the caller assumes responsibility
                for slow query logging if the response is "successful" (i.e., status code
                200-299, which could still mean the SQL query failed). This is desirable if
                more granular timing information is available in the response body, such that
                we can discount the effect of the async thread being blocked on the response
                time, or we can more accurately determine which part of the request took time.

        Returns:
            FetchResponseFullResult: If a successful response is received, it is returned.

        Raises:
            MaxAttemptsError: If the maximum number of attempts is reached before
                we get a successful response.
            UnexpectedResponse: If one of the rqlite nodes returns a response
                we didn't expect
        """

        def attempt_host(
            host: Tuple[str, int], node_path: List[Tuple[str, Exception]]
        ) -> Optional[FetchResponseFullResult]:
            try:
                started_at_wall = time.time()
                started_at_perf = time.perf_counter()
                result = self.fetch_response_with_host(
                    *host, method, uri, json, headers, stream
                )
                request_time_perf = time.perf_counter() - started_at_perf
                ended_at_wall = time.time()

                try:
                    response_size_bytes = int(result.headers.get("Content-Length", "0"))
                except ValueError:
                    response_size_bytes = 0

                response = FetchResponseFullResult(
                    response=result,
                    method=method,
                    uri=uri,
                    json=json,
                    headers=headers,
                    stream=stream,
                    requested_initial_host=initial_host,
                    node_path=node_path,
                    lazy_query_info=query_info,
                    slow_query_handled_on_success=slow_query_handled_on_success,
                    host=host,
                    started_at_wall=started_at_wall,
                    ended_at_wall=ended_at_wall,
                    request_time_perf=request_time_perf,
                    response_size_bytes=response_size_bytes,
                )

                if (
                    not slow_query_handled_on_success
                    or result.status_code < 200
                    or result.status_code >= 300
                ):
                    self.process_slow_query_if_slow(response, None)

                return response
            except ConnectError as e:

                def msg_supplier(max_length: Optional[int]) -> str:
                    str_error = str(e)
                    if max_length is not None and len(str_error) > max_length:
                        str_error = str_error[:max_length] + "..."

                    return f"Failed to connect to node {e.host} - {str_error}"

                rqdb.logging.log(
                    self.log_config.connect_timeout, msg_supplier, exc_info=True
                )
                node_path.append((e.host, e))
            except MaxRedirectsError as e:

                def msg_supplier(max_length: Optional[int]) -> str:
                    str_redirect_path = str(e.redirect_path)
                    if max_length is not None and len(str_redirect_path) > max_length:
                        str_redirect_path = str_redirect_path[:max_length] + "..."

                    return f"Max redirects reached for node {e.host} (redirect path: {str_redirect_path})"

                rqdb.logging.log(
                    self.log_config.non_ok_response, msg_supplier, exc_info=True
                )
                node_path.append((e.host, e))
            except UnexpectedResponse as e:

                def msg_supplier(max_length: Optional[int]) -> str:
                    str_error = str(e)
                    if max_length is not None and len(str_error) > max_length:
                        str_error = str_error[:max_length] + "..."

                    return f"Unexpected response from node {e.host}: {str_error}"

                rqdb.logging.log(
                    self.log_config.non_ok_response, msg_supplier, exc_info=True
                )
                raise

        return self.try_hosts(attempt_host, initial_host=initial_host)

    def fetch_response_with_host(
        self,
        host: str,
        port: int,
        method: Literal["GET", "POST"],
        uri: str,
        json: Any = None,
        headers={},
        stream: bool = False,
    ) -> requests.Response:
        """Fetches a response from a particular host, and returns the status
        code and headers. The response body is written to the given destination.

        This will follow up to the maximum number of redirects.

        Args:
            host (str): The host to fetch the response from.
            port (int): The port to use when connecting to the host.
            method (str): The HTTP method to use.
            uri (str): The URI to fetch.
            json (dict): The JSON to send in the request body.
            headers (dict): The headers to send in the request in addition to the content type.
                The dictionary should be accessed only using lowercase keys.

        Returns:
            requests.Response: The response from the server.

        Raises:
            ConnectTimeout: If the connection times out.
            MaxRedirectsError: If the maximum number of redirects is exceeded.
            UnexpectedResponse: If the server returns a response we didn't expect.
        """
        redirect_path = []
        original_host = f"http://{host}:{port}{uri}"
        current_host = original_host
        while len(redirect_path) < self.max_redirects:
            try:
                response = requests.request(
                    method,
                    current_host,
                    json=json,
                    headers=headers,
                    timeout=self.timeout,
                    allow_redirects=False,
                    stream=stream,
                )

                if response.is_redirect:
                    redirected_to = response.headers["Location"]
                    if stream:
                        response.close()
                    current_host = redirected_to
                    redirect_path.append(redirected_to)
                    continue

                if response.status_code < 200 or response.status_code > 299:
                    raise UnexpectedResponse(
                        current_host,
                        f"Unexpected response from {current_host}: {response.status_code} {response.reason}",
                    )

                return response
            except requests.exceptions.ConnectTimeout:
                raise ConnectError(
                    f"Connection to {host}:{port} timed out", current_host
                )
            except requests.exceptions.ConnectionError:
                raise ConnectError(
                    f"Connection to {host}:{port} was refused", current_host
                )

        raise MaxRedirectsError(original_host, redirect_path)

    def discover_leader(self) -> Tuple[str, int]:
        """Discovers the current leader for the cluster

        Returns:
            A tuple of (leader_host, leader_port)

        Raises:
            MaxAttemptsError: If the maximum number of attempts is reached before
                we get a successful response.
            UnexpectedResponse: If one of the rqlite nodes returns a response
                we didn't expect
        """

        def attempt_host(
            host: Tuple[str, int], node_path: List[Tuple[str, Exception]]
        ) -> Optional[Tuple[str, int]]:
            try:
                return self.discover_leader_with_host(*host)
            except ConnectError as e:

                def msg_supplier(max_length: Optional[int]) -> str:
                    str_error = str(e)
                    if max_length is not None and len(str_error) > max_length:
                        str_error = str_error[:max_length] + "..."

                    return f"Failed to connect to node {e.host} - {str_error}"

                rqdb.logging.log(
                    self.log_config.connect_timeout, msg_supplier, exc_info=True
                )
                node_path.append((e.host, e))
            except UnexpectedResponse as e:

                def msg_supplier(max_length: Optional[int]) -> str:
                    str_error = str(e)
                    if max_length is not None and len(str_error) > max_length:
                        str_error = str_error[:max_length] + "..."

                    return f"Unexpected response from node {e.host}: {str_error}"

                rqdb.logging.log(
                    self.log_config.non_ok_response, msg_supplier, exc_info=True
                )
                raise

        return self.try_hosts(attempt_host)

    def discover_leader_with_host(self, host: str, port: int) -> Tuple[str, int]:
        """Uses the given node in the cluster to discover the current leader
        for the cluster.

        Returns:
            A tuple of (leader_host, leader_port)

        Raises:
            ConnectTimeout: If the connection times out.
            UnexpectedResponse: If the server returns a response we didn't expect.
        """
        response = None
        try:
            response = requests.request(
                "POST",
                f"http://{host}:{port}/db/query?level=weak&redirect",
                json=[["SELECT 1"]],
                headers={"Content-Type": "application/json; charset=UTF-8"},
                timeout=self.timeout,
                allow_redirects=False,
            )

            if response.is_redirect:
                redirected_to = response.headers["Location"]
                parsed_url = urllib.parse.urlparse(redirected_to)
                return parse_host(
                    parsed_url.netloc,
                    default_port=443 if parsed_url.scheme == "https" else 80,
                )

            if response.status_code < 200 or response.status_code > 299:
                raise UnexpectedResponse(
                    host,
                    f"Unexpected response from {host}:{port}: {response.status_code} {response.reason}",
                )

            return (host, port)
        except requests.exceptions.ConnectTimeout:
            raise ConnectError(
                f"Connection to {host}:{port} timed out", f"{host}:{port}"
            )
        except requests.exceptions.ConnectionError:
            raise ConnectError(
                f"Connection to {host}:{port} was refused", f"{host}:{port}"
            )

    def backup(self, file: SyncWritableIO, /, raw: bool = False) -> None:
        """Backup the database to a file.

        Args:
            file (file-like): The file to write the backup to.
            raw (bool): If true, the backup will be in raw SQL format. If false, the
                backup will be in the smaller sqlite format.
        """
        request_id = secrets.token_hex(4)
        rqdb.logging.log(
            self.log_config.backup_start,
            lambda _: f"  [RQLITE BACKUP {{{request_id}}}] raw={raw}",
        )
        request_started_at = time.perf_counter()
        # It is much faster for us to discover the leader and fetch the backup
        # from the leader right now: https://github.com/rqlite/rqlite/issues/1551
        leader = self.discover_leader()

        if raw:
            resp = self.fetch_response(
                "GET", "/db/backup?fmt=sql", stream=True, initial_host=leader
            )
        else:
            resp = self.fetch_response(
                "GET", "/db/backup", stream=True, initial_host=leader
            )

        for chunk in resp.iter_content(chunk_size=4096):
            file.write(chunk)

        resp.close()
        time_taken = time.perf_counter() - request_started_at
        rqdb.logging.log(
            self.log_config.backup_end,
            lambda _: f"    {{{request_id}}} in {time_taken:.3f}s ->> backup fully written",
        )

    def process_slow_query_if_slow(
        self, response: FetchResponseFullResult, result: Optional["BulkResult"]
    ) -> None:
        """Checks if the given query was slow based on the response and result. If it was
        slow and slow query logging is enabled, the slow query log message will be created
        and sent to the log method.

        Args:
            response (FetchResponseFullResult): The response to check for slowness.
            result (BulkResult): The result of the query, if available. If available
                and it has timing information, we use that instead of the local timing
                to avoid confusing DB time with e.g. network time
        """
        if not self.log_config.slow_query or not self.log_config.slow_query.get(
            "enabled", True
        ):
            return
        if response.lazy_query_info is None:
            return
        config = cast(
            rqdb.logging.SlowQueryLogMessageConfig,
            self.log_config.slow_query,
        )

        slow_threshold = config.get("threshold_seconds", 5)

        if result is None:
            if response.request_time_perf < slow_threshold:
                return
        elif result.time is not None:
            if result.time < slow_threshold:
                return
        else:
            if all(r.time is not None for r in result.items):
                if sum((r.time or 0) for r in result.items) < slow_threshold:
                    return
            elif response.request_time_perf < slow_threshold:
                return

        method_argspec = inspect.getfullargspec(config["method"])
        accepts_result_kwarg = (
            "result" in method_argspec.kwonlyargs or method_argspec.varkw is not None
        )
        config["method"](
            rqdb.logging.QueryInfo(
                operations=(
                    response.lazy_query_info.operations
                    if not callable(response.lazy_query_info.operations)
                    else response.lazy_query_info.operations()
                ),
                params=(
                    response.lazy_query_info.params
                    if not callable(response.lazy_query_info.params)
                    else response.lazy_query_info.params()
                ),
                request_type=(
                    response.lazy_query_info.request_type
                    if not callable(response.lazy_query_info.request_type)
                    else response.lazy_query_info.request_type()
                ),
                consistency=response.lazy_query_info.consistency,
                freshness=response.lazy_query_info.freshness,
            ),
            duration_seconds=response.request_time_perf,
            host=f"{response.host[0]}:{response.host[1]}",
            response_size_bytes=response.response_size_bytes,
            started_at=response.started_at_wall,
            ended_at=response.ended_at_wall,
            **({"result": result} if accepts_result_kwarg else {}),
        )


def parse_host(host: str, /, *, default_port: int = 4001) -> Tuple[str, int]:
    """Parses a host:port pair into an ip address and port.

    Args:
        host (str): A host:port pair, or just a host
        default_port (int): the port to assume if not specified; for configuration,
            this is 4001, for loading from the Location header, this depends on the
            protocol

    Returns:
        A tuple of (ip, port)
    """
    if not host:
        raise ValueError("host must not be empty")

    num_colons = host.count(":")
    if num_colons > 1:
        # ipv6; must be of the form [host]:port if port is specified
        if host[0] != "[":
            return host, 4001

        close_square_bracket_idx = host.find("]")
        return host[1:close_square_bracket_idx], int(
            host[close_square_bracket_idx + 1 :]
        )

    if num_colons == 0:
        return host, default_port

    hostname, port_str = host.split(":")
    return hostname, int(port_str)
