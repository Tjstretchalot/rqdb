"""This module describes a result for a query or bulk query"""
from rqdb.errors import DBError as Error
from typing import Any, Iterator, List, Optional


class ResultItem:
    """The result of a single query, either a standalone query or
    a member of a bulk query.
    """

    def __init__(
        self,
        results: Optional[List[list]] = None,
        last_insert_id: Optional[int] = None,
        rows_affected: Optional[int] = None,
        error: Optional[str] = None,
    ):
        self.results = results
        """The rows returned by the query. This is the "values" part of the result
        item returned by the RQLite API. This is the ordered list of values that
        were selected in this row, so, e.g.,

        "SELECT 1, 2, 3"

        would lead to the results list being [1, 2, 3].

        This is None if either the query was not a SELECT query or if the query
        failed.
        """

        self.last_insert_id = last_insert_id
        """The ID of the last inserted row after this query. This is None if
        the query was a SELECT query or if the query failed. This is typically
        only meaningful for INSERT queries.

        Note that the value of the last insert id may change arbitrarily between
        non-insert queries due to request interleaving on the underlying cursor
        on the actual rqlite nodes.
        """

        self.rows_affected = rows_affected
        """The number of rows affected by this query. This is None if the query
        was a SELECT query or if the query failed. This is typically only
        meaningful for INSERT, UPDATE, and DELETE queries.
        """

        self.error = error
        """The actual error text returned by the RQLite server. This is None if
        the query succeeded. It is helpful to use the Error object to determine
        the type of error.
        """

    @property
    def rowcount(self) -> int:
        """The number of rows returned by this query. 0 if the query was
        not a select query or if the query failed.
        """
        if self.results is None:
            return 0
        return len(self.results)

    def raise_on_error(self, hint=None) -> "ResultItem":
        """Raises an error if this result item has an error. Otherwise,
        returns self.

        Args:
            hint (str): If specified, used as part of the error message to
                provide additional context.

        Returns:
            self

        Raises:
            Error: If this result item has an error.
        """
        if self.error is not None:
            if hint is None:
                raise Error(self.error)
            raise Error(f"{hint}: {self.error}", raw_message=self.error)

        return self

    def cursor(self) -> "ResultItemCursor":
        """Returns a cursor-like object that allows paginating over the results
        using fetchone() style methods. Raises an error if this is not the result
        of a successful SELECT query.
        """
        if self.results is None:
            raise Error("Cannot get cursor for non-SELECT query")
        return ResultItemCursor(self.results)

    def __repr__(self) -> str:
        return f"ResultItem(results={repr(self.results)}, last_insert_id={repr(self.last_insert_id)}, rows_affected={repr(self.rows_affected)}, error={repr(self.error)})"

    @classmethod
    def parse(cls, result: dict):
        """Parses a result from the RQLite API into a ResultItem object."""
        if "error" in result:
            return ResultItem(error=result["error"])

        if "values" in result:
            return ResultItem(results=result["values"])

        return ResultItem(
            last_insert_id=result.get("last_insert_id"),
            rows_affected=result.get("rows_affected"),
        )


class ResultItemCursor:
    """Describes a cursor-like object for a result item, which allows paginating
    the values with the familiar fetchone(), fetchmany(), and fetchall() methods.
    This only applies to successful SELECT queries.
    """

    def __init__(self, results: List[list]):
        self.results = results
        """The list of result items returned by the query"""

        self.index = 0
        """The index of the current result item"""

    def fetchone(self) -> Optional[List[Any]]:
        """Returns the next row in the result set. Returns None if there are no
        more rows.
        """
        if self.index >= len(self.results):
            return None
        result = self.results[self.index]
        self.index += 1
        return result

    def fetchmany(self, size: Optional[int] = None) -> List[List[Any]]:
        """Returns the next `size` rows in the result set. Returns an empty
        list if there are no more rows.
        """
        if self.index >= len(self.results):
            return []
        if size is None:
            size = len(self.results) - self.index
        result = self.results[self.index : self.index + size]
        self.index += len(result)
        return result

    def fetchall(self) -> List[List[Any]]:
        """Returns all remaining rows in the result set. Returns an empty
        list if there are no more rows.
        """
        if self.index >= len(self.results):
            return []
        result = self.results[self.index :]
        self.index = len(self.results)
        return result

    @property
    def rowcount(self) -> int:
        """Returns the total number of rows in the result set."""
        return len(self.results)


class BulkResult:
    """Describes the result of a bulk query. This is the result of many
    individual queries, potentially in the same transaction.

    This can be indexed by the index of the query in the bulk query.
    """

    def __init__(self, items: List[ResultItem]) -> None:
        self.items = items
        """The individual result items for the bulk query."""

    def raise_if_error_before(self, idx: int) -> "BulkResult":
        """Raises an error if any queries before the given index have errors.
        Otherwise, returns self.

        Args:
            idx (int): The index of the query to check for errors.

        Returns:
            self

        Raises:
            Error: If any queries before the given index have errors.
        """
        if idx < 0:
            raise ValueError("idx must be >= 0")

        for i in range(min(len(self.items), idx)):
            self.items[i].raise_on_error(f"query idx={idx}")

        return self

    def raise_on_error(self, hint: Optional[str] = None) -> "BulkResult":
        """Raises an error if any of the queries in this bulk query have errors.
        Otherwise, returns self.

        Returns:
            self

        Raises:
            Error: If any of the queries in this bulk query have errors.
        """
        hint = "" if hint is None else f"; {hint}"
        for idx, item in enumerate(self.items):
            item.raise_on_error(f"query idx={idx}{hint}")

        return self

    def __getitem__(self, idx: int) -> ResultItem:
        return self.items[idx]

    def __len__(self) -> int:
        return len(self.items)

    def __iter__(self) -> Iterator[ResultItem]:
        return iter(self.items)

    def __repr__(self) -> str:
        return f"BulkResult(items={repr(self.items)})"

    def __str__(self) -> str:
        return str(self.items)

    @classmethod
    def parse(cls, payload: dict) -> "BulkResult":
        """Parses a result from the RQLite API into a BulkResult object."""
        return BulkResult(
            [ResultItem.parse(item) for item in payload.get("results", [])]
        )
