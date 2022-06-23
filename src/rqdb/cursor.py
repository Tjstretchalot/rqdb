from typing import Any, Literal, Optional, Tuple, TYPE_CHECKING
from rqdb.errors import DBError
from rqdb.logging import log
from rqdb.result import BulkResult, ResultItem, ResultItemCursor
from rqdb.preprocessing import get_sql_command, clean_nulls
import time
import secrets

if TYPE_CHECKING:
    from rqdb.connection import Connection


class Cursor:
    """A synchronous cursor for executing queries on a rqlite cluster."""

    def __init__(
        self,
        connection: "Connection",
        read_consistency: Literal["none", "weak", "strong"],
        freshness: str,
    ):
        self.connection = connection
        """The underlying connection to the rqlite cluster."""

        self.read_consistency = read_consistency
        """The read consistency to use when executing queries."""

        self.freshness = freshness
        """The freshness to use when executing none read consistency queries."""

        self.cursor: Optional[ResultItemCursor] = None
        """If we have a cursor object we are passing through to, this is it."""

        self.rows_affected: Optional[int] = None
        """The number of rows affected by the last query."""

        self.last_insert_id: Optional[int] = None
        """The last insert id after the last query"""

    @property
    def rowcount(self) -> int:
        """Returns the number of rows in the result set."""
        if self.cursor is None:
            return 0
        return self.cursor.rowcount

    def execute(
        self,
        operation: str,
        parameters: Optional[tuple] = None,
        raise_on_error: bool = True,
        read_consistency: Optional[Literal["none", "weak", "strong"]] = None,
        freshness: Optional[str] = None,
    ) -> ResultItem:
        """Executes a single query and returns the result. This will also
        update this object so that fetchone() and related functions can be used
        to fetch the results instead.

        Args:
            operation (str): The query to execute.
            parameters (tuple): The parameters to pass to the query.
            raise_on_error (bool): If True, raise an error if the query fails. If
                False, you can check the result item's error property to see if
                the query failed.
            read_consistency (Optional[Literal["none", "weak", "strong"]]):
                The read consistency to use when executing the query. If None,
                use the default read consistency for this cursor.
            freshness (Optional[str]): The freshness to use when executing
                none read consistency queries. If None, use the default freshness
                for this cursor.

        Returns:
            ResultItem: The result of the query.
        """
        if parameters is None:
            parameters = tuple()
        if read_consistency is None:
            read_consistency = self.read_consistency
        if freshness is None:
            freshness = self.freshness

        self.cursor = None
        self.rows_affected = None
        self.last_insert_id = None

        command = get_sql_command(operation)
        cleaned_query, parameters = clean_nulls(operation, parameters)
        is_read = command == "SELECT"
        request_id = secrets.token_hex(4)

        if is_read:

            def msg_supplier(max_length: Optional[int]) -> str:
                abridged_query = operation
                if max_length is not None and len(abridged_query) > max_length:
                    abridged_query = abridged_query[:max_length] + "..."

                abridged_parameters = repr(parameters)
                if max_length is not None and len(abridged_parameters) > max_length:
                    abridged_parameters = abridged_parameters[:max_length] + "..."

                freshness_str = ""
                if read_consistency == "none":
                    freshness_str = f", {freshness=}"

                return f"  [RQLITE {command} @ {read_consistency}{freshness_str} {{{request_id}}}] - {repr(abridged_query)}; {abridged_parameters}"

            log(self.connection.log_config.read_start, msg_supplier)

            path = f"/db/query?level={read_consistency}"
            if read_consistency == "none":
                path += f"&freshness={freshness}"

            request_started_at = time.perf_counter()
            response = self.connection.fetch_response(
                "POST",
                path,
                json=[[cleaned_query, *parameters]],
                headers={"Content-Type": "application/json; charset=UTF-8"},
            )
        else:

            def msg_supplier(max_length: Optional[int]) -> str:
                abridged_query = operation
                if max_length is not None and len(abridged_query) > max_length:
                    abridged_query = abridged_query[:max_length] + "..."

                abridged_parameters = repr(parameters)
                if max_length is not None and len(abridged_parameters) > max_length:
                    abridged_parameters = abridged_parameters[:max_length] + "..."

                return f"  [RQLITE {command} {{{request_id}}}] - {repr(abridged_query)}; {abridged_parameters}"

            log(self.connection.log_config.write_start, msg_supplier)

            request_started_at = time.perf_counter()
            response = self.connection.fetch_response(
                "POST",
                "/db/execute",
                json=[[cleaned_query, *parameters]],
                headers={"Content-Type": "application/json; charset=UTF-8"},
            )

        payload = response.json()
        request_time = time.perf_counter() - request_started_at

        def msg_supplier(max_length: Optional[int]) -> str:
            abridged_payload = response.text
            if max_length is not None and len(abridged_payload) > max_length:
                abridged_payload = abridged_payload[:max_length] + "..."

            return f"    {{{request_id}}} in {request_time:.3f}s -> {repr(abridged_payload)}"

        log(
            self.connection.log_config.read_response
            if is_read
            else self.connection.log_config.write_response,
            msg_supplier,
        )

        if "error" in payload:
            error = DBError(
                f'error outside of result: {payload["error"]}', payload["error"]
            )
            if error.is_stale:
                assert read_consistency == "none"

                def msg_supplier(max_length: Optional[int]) -> str:
                    return f"    {{{request_id}}} ->> stale read, retrying with weak consistency"

                log(self.connection.log_config.read_stale, msg_supplier)
                return self.execute(
                    operation,
                    parameters,
                    raise_on_error=raise_on_error,
                    read_consistency="weak",
                    freshness=freshness,
                )
            raise error

        results = payload.get("results")
        result = ResultItem.parse(results[0]) if results else ResultItem()
        if raise_on_error:
            result.raise_on_error(f"{request_id=}; {command=}; {parameters=}")

        if is_read and result.rowcount > 0:
            self.cursor = result.cursor()

        self.rows_affected = result.rows_affected
        self.last_insert_id = result.last_insert_id

        return result

    def executemany2(
        self,
        operations: Tuple[str],
        seq_of_parameters: Optional[Tuple[Tuple[Any]]] = None,
        transaction: bool = True,
        raise_on_error: bool = True,
    ) -> BulkResult:
        """Executes multiple operations within a single request and, by default, within
        a transaction.

        Unlike the standard DB-API executemany(), this method accepts different
        operations and parameters for each operation.

        Regardless of what type of operations are passed in, they will executed
        as if they are updates, i.e., no result rows will be returned.

        Args:
            operations (Tuple[str]): The operations to execute.
            seq_of_parameters (Tuple[Tuple[Any]]): The parameters to pass to each operation.
            transaction (bool): If True, execute the operations within a transaction.
            raise_on_error (bool): If True, raise an error if any of the operations fail. If
                False, you can check the result item's error property to see if the
                operation failed.

        Returns:
            BulkResult: The result of the query.

        Raises:
            ValueError: If the number of operations and parameters do not match.
        """
        if seq_of_parameters is None:
            seq_of_parameters = tuple(tuple() for _ in operations)

        if len(operations) != len(seq_of_parameters):
            raise ValueError(
                f"Number of operations ({len(operations)}) does not match number of parameters ({len(seq_of_parameters)})"
            )

        path = "/db/execute"
        if transaction:
            path += "?transaction"

        cleaned_request = []
        for operation, parameters in zip(operations, seq_of_parameters):
            cleaned_query, parameters = clean_nulls(operation, parameters)
            cleaned_request.append([cleaned_query, *parameters])

        request_id = secrets.token_hex(4)

        def msg_supplier(max_length: Optional[int]) -> str:
            abridged_request = repr(cleaned_request)
            if max_length is not None and len(abridged_request) > max_length:
                abridged_request = abridged_request[:max_length] + "..."

            return f"  [RQLITE BULK {path} {{{request_id}}}] - {abridged_request}"

        log(self.connection.log_config.write_start, msg_supplier)

        request_started_at = time.perf_counter()
        response = self.connection.fetch_response(
            "POST",
            path,
            json=cleaned_request,
            headers={"Content-Type": "application/json; charset=UTF-8"},
        )
        payload = response.json()
        request_time = time.perf_counter() - request_started_at

        def msg_supplier(max_length: Optional[int]) -> str:
            abridged_payload = response.text
            if max_length is not None and len(abridged_payload) > max_length:
                abridged_payload = abridged_payload[:max_length] + "..."

            return f"    {{{request_id}}} in {request_time:.3f}s -> {abridged_payload}"

        log(
            self.connection.log_config.write_response,
            msg_supplier,
        )

        result = BulkResult.parse(payload)
        if raise_on_error:
            result.raise_on_error(f"{request_id=}; {operations=}; {seq_of_parameters=}")

        return result

    def executemany3(
        self,
        operation_and_parameters: Tuple[Tuple[str, Tuple[Any]]],
        transaction: bool = True,
        raise_on_error: bool = True,
    ) -> BulkResult:
        """Executes multiple operations within a single request and, by default, within
        a transaction.

        Unlike the standard DB-API executemany(), this method accepts different
        operations and parameters for each operation.

        Regardless of what type of operations are passed in, they will executed
        as if they are updates, i.e., no result rows will be returned.

        Args:
            operations_and_parameters (Tuple[Tuple[str, Tuple[Any]]]):
                The operations and corresponding parameters to execute.
            transaction (bool): If True, execute the operations within a transaction.
            raise_on_error (bool): If True, raise an error if any of the operations fail. If
                False, you can check the result item's error property to see if the
                operation failed.

        Returns:
            BulkResult: The result of the query.

        Raises:
            ValueError: If the number of operations and parameters do not match.
        """
        path = "/db/execute"
        if transaction:
            path += "?transaction"

        cleaned_request = []
        for operation, parameters in operation_and_parameters:
            cleaned_query, parameters = clean_nulls(operation, parameters)
            cleaned_request.append([cleaned_query, *parameters])

        request_id = secrets.token_hex(4)

        def msg_supplier(max_length: Optional[int]) -> str:
            abridged_request = repr(cleaned_request)
            if max_length is not None and len(abridged_request) > max_length:
                abridged_request = abridged_request[:max_length] + "..."

            return f"  [RQLITE BULK {path} {{{request_id}}}] - {abridged_request}"

        log(self.connection.log_config.write_start, msg_supplier)

        request_started_at = time.perf_counter()
        response = self.connection.fetch_response(
            "POST",
            path,
            json=cleaned_request,
            headers={"Content-Type": "application/json; charset=UTF-8"},
        )
        payload = response.json()
        request_time = time.perf_counter() - request_started_at

        def msg_supplier(max_length: Optional[int]) -> str:
            abridged_payload = response.text
            if max_length is not None and len(abridged_payload) > max_length:
                abridged_payload = abridged_payload[:max_length] + "..."

            return f"    {{{request_id}}} in {request_time:.3f}s -> {abridged_payload}"

        log(
            self.connection.log_config.write_response,
            msg_supplier,
        )

        result = BulkResult.parse(payload)
        if raise_on_error:
            result.raise_on_error(f"{request_id=}; {operation_and_parameters=}")

        return result

    def fetchone(self) -> Optional[list]:
        """Fetches the next row from the cursor. If there are no more rows,
        returns None.
        """
        if self.cursor is None:
            return None
        result = self.cursor.fetchone()
        if result is None:
            self.cursor = None
        return result

    def fetchall(self) -> list:
        """Fetches all rows from the cursor. If there are no more rows,
        returns an empty list.
        """
        if self.cursor is None:
            return []
        result = self.cursor.fetchall()
        self.cursor = None
        return result

    def fetchmany(self, size: int) -> list:
        """Fetches the next `size` rows from the cursor. If there are no more rows,
        returns an empty list.
        """
        if self.cursor is None:
            return []
        result = self.cursor.fetchmany(size)
        if len(result) < size:
            self.cursor = None
        return result

    def close(self):
        """No-op"""
        pass
