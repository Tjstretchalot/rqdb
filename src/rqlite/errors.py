from typing import List, Optional, Tuple


class DBError(Exception):
    """Describes an error returned by rqlite"""

    def __init__(self, msg: str, raw_message: Optional[str] = None) -> None:
        super().__init__(msg)
        self.message = msg
        """Our text for the error"""

        self.raw_message = raw_message or msg
        """The raw message returned by rqlite"""

    @property
    def is_foreign_key(self) -> bool:
        """True if this error is a foreign key error, false otherwise"""
        return self.raw_message == "FOREIGN KEY constraint failed"

    @property
    def is_unique(self) -> bool:
        """True if this error is a unique constraint error, false otherwise"""
        return "unique" in self.raw_message.lower()

    @property
    def is_syntax(self) -> bool:
        """True if this error is a syntax error, false otherwise"""
        return self.raw_message.endswith("syntax error")

    @property
    def is_stale(self) -> bool:
        """True if this error indicates a stale read occurred, false otherwise"""
        return self.raw_message == "stale read"


class ConnectError(Exception):
    """Describes a timeout or other failure when connecting to a rqlite node"""

    def __init__(self, msg: str, host: str) -> None:
        super().__init__(msg)
        self.host = host
        """The host we tried to connect to"""


class UnexpectedResponse(Exception):
    """Describes an unexpected response from rqlite"""

    def __init__(self, host: str, msg: str) -> None:
        super().__init__(msg)
        self.host = host
        """The host which returned the unexpected response"""


class MaxAttemptsError(Exception):
    """Describes exhausting all attempts on all nodes in a rqlite cluster"""

    def __init__(self, node_path: List[Tuple[str, Exception]]) -> None:
        super().__init__(f"Exhausted all attempts on all nodes; {node_path=}")
        self.node_path = node_path
        """A list of (host, error) tuples for each node we tried to connect to"""


class MaxRedirectsError(Exception):
    """Describes too many redirects when connecting to a rqlite node"""

    def __init__(self, host: str, redirect_path: List[str]) -> None:
        super().__init__(f"Too many redirects connecting to {host}: {redirect_path}")
        self.host = host
        """The host we tried to connect to"""

        self.redirect_path = redirect_path
        """The path we followed to get to the final host"""
