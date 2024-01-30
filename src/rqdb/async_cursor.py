from functools import partial
from typing import (
    Any,
    Callable,
    Iterable,
    List,
    Literal,
    Optional,
    Tuple,
    TYPE_CHECKING,
    Union,
    overload,
)
from rqdb.errors import DBError
from rqdb.explain import (
    ExplainQueryPlan,
    async_write_explain_query_plan,
    format_explain_query_plan_result,
    parse_explain_query_plan,
    print_explain_query_plan_result,
    write_explain_query_plan,
)
from rqdb.logging import QueryInfoLazy, QueryInfoRequestType, log
from rqdb.result import BulkResult, ResultItem, ResultItemCursor
from rqdb.preprocessing import (
    determine_unified_request_type,
    get_sql_command,
    clean_nulls,
)
import inspect
import time
import secrets

if TYPE_CHECKING:
    from rqdb.async_connection import AsyncConnection, SyncWritableIO, AsyncWritableIO


class AsyncCursor:
    """A synchronous cursor for executing queries on a rqlite cluster."""

    def __init__(
        self,
        connection: "AsyncConnection",
        read_consistency: Literal["none", "weak", "strong"],
        freshness: str,
    ):
        self.connection = connection
        """The underlying connection to the rqlite cluster."""

        self.read_consistency: Literal["none", "weak", "strong"] = read_consistency
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

    async def execute(
        self,
        operation: str,
        parameters: Optional[Iterable[Any]] = None,
        raise_on_error: bool = True,
        read_consistency: Optional[Literal["none", "weak", "strong"]] = None,
        freshness: Optional[str] = None,
    ) -> ResultItem:
        """Executes a single query and returns the result. This will also
        update this object so that fetchone() and related functions can be used
        to fetch the results instead.

        Args:
            operation (str): The query to execute.
            parameters (iterable): The parameters to pass to the query.
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
        is_read = command in ("SELECT", "EXPLAIN")
        request_id = secrets.token_hex(4)

        if is_read:

            def msg_supplier_1(max_length: Optional[int]) -> str:
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

            log(self.connection.log_config.read_start, msg_supplier_1)

            path = f"/db/query?level={read_consistency}"
            if self.read_consistency == "none":
                path += f"&freshness={freshness}"
            else:
                path += "&redirect"

            request_started_at = time.perf_counter()
            response = await self.connection.fetch_response(
                "POST",
                path,
                json=[[cleaned_query, *parameters]],
                headers={"Content-Type": "application/json; charset=UTF-8"},
                query_info=QueryInfoLazy(
                    operations=[operation],
                    params=[parameters],
                    request_type="execute-read",
                    consistency=read_consistency,
                    freshness=freshness,
                ),
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
            response = await self.connection.fetch_response(
                "POST",
                "/db/execute",
                json=[[cleaned_query, *parameters]],
                headers={"Content-Type": "application/json; charset=UTF-8"},
                query_info=QueryInfoLazy(
                    operations=[operation],
                    params=[parameters],
                    request_type="execute-write",
                    consistency=read_consistency,
                    freshness=freshness,
                ),
            )

        payload: dict = await response.json()
        await response.__aexit__(None, None, None)
        request_time = time.perf_counter() - request_started_at

        def msg_supplier(max_length: Optional[int]) -> str:
            abridged_payload = repr(payload)
            if max_length is not None and len(abridged_payload) > max_length:
                abridged_payload = abridged_payload[:max_length] + "..."

            return f"    {{{request_id}}} in {request_time:.3f}s -> {abridged_payload}"

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
                return await self.execute(
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

    async def _executemany2(
        self,
        base_path: str,
        operations: Iterable[str],
        /,
        *,
        seq_of_parameters: Optional[Iterable[Iterable[Any]]] = None,
        transaction: bool = True,
        raise_on_error: bool = True,
        request_type: Union[QueryInfoRequestType, Callable[[], QueryInfoRequestType]],
        read_consistency: Optional[str],
        freshness: Optional[str],
    ) -> BulkResult:
        if read_consistency is None:
            read_consistency = self.read_consistency

        if freshness is None:
            freshness = self.freshness

        if seq_of_parameters is None:
            seq_of_parameters = tuple(tuple() for _ in operations)

        qargs: List[str] = []

        if transaction:
            qargs.append("transaction")

        if request_type != "executemany":
            qargs.append("level=" + read_consistency)
            if read_consistency == "none":
                qargs.append("freshness=" + freshness)

        qargs.append("redirect")

        path = base_path + "?" + "&".join(qargs)

        cleaned_request = []

        operations_iter = iter(operations)
        seq_of_parameters_iter = iter(seq_of_parameters)

        next_operation: str = ""
        next_parameters: Iterable[Any] = tuple()

        while True:
            try:
                next_operation = next(operations_iter)
            except StopIteration:
                operations_iter = None
            try:
                next_parameters = next(seq_of_parameters_iter)
            except StopIteration:
                seq_of_parameters_iter = None

            if (operations_iter is None) is not (seq_of_parameters_iter is None):
                raise ValueError(
                    "operations and seq_of_parameters must be the same length"
                )

            if operations_iter is None or seq_of_parameters_iter is None:
                break

            operation: str = next_operation
            parameters: Iterable[Any] = next_parameters
            cleaned_query, parameters = clean_nulls(operation, parameters)
            cleaned_request.append([cleaned_query, *parameters])

        request_id = secrets.token_hex(4)

        def msg_supplier_1(max_length: Optional[int]) -> str:
            abridged_request = repr(cleaned_request)
            if max_length is not None and len(abridged_request) > max_length:
                abridged_request = abridged_request[:max_length] + "..."

            return f"  [RQLITE BULK {path} {{{request_id}}}] - {abridged_request}"

        log(self.connection.log_config.write_start, msg_supplier_1)

        request_started_at = time.perf_counter()
        response = await self.connection.fetch_response(
            "POST",
            path,
            json=cleaned_request,
            headers={"Content-Type": "application/json; charset=UTF-8"},
            query_info=QueryInfoLazy(
                operations=operations,
                params=seq_of_parameters,
                request_type=request_type,
                consistency="strong",
                freshness="",
            ),
        )
        payload: dict = await response.json()
        await response.__aexit__(None, None, None)
        request_time = time.perf_counter() - request_started_at

        def msg_supplier_2(max_length: Optional[int]) -> str:
            abridged_payload = repr(payload)
            if max_length is not None and len(abridged_payload) > max_length:
                abridged_payload = abridged_payload[:max_length] + "..."

            return f"    {{{request_id}}} in {request_time:.3f}s -> {abridged_payload}"

        log(
            self.connection.log_config.write_response,
            msg_supplier_2,
        )

        result = BulkResult.parse(payload)
        if raise_on_error:
            result.raise_on_error(f"{request_id=}; {operations=}; {seq_of_parameters=}")

        return result

    async def executemany2(
        self,
        operations: Iterable[str],
        seq_of_parameters: Optional[Iterable[Iterable[Any]]] = None,
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
            operations (iterable[str]): The operations to execute.
            seq_of_parameters (iterable[iterable[Any]]): The parameters to pass to each operation.
            transaction (bool): If True, execute the operations within a transaction.
            raise_on_error (bool): If True, raise an error if any of the operations fail. If
                False, you can check the result item's error property to see if the
                operation failed.

        Returns:
            BulkResult: The result of the query.

        Raises:
            ValueError: If the number of operations and parameters do not match.
        """
        return await self._executemany2(
            "/db/execute",
            operations,
            seq_of_parameters=seq_of_parameters,
            transaction=transaction,
            raise_on_error=raise_on_error,
            request_type="executemany",
            read_consistency="strong",
            freshness="",
        )

    async def _executemany3(
        self,
        base_path: str,
        operation_and_parameters: Iterable[Tuple[str, Iterable[Any]]],
        /,
        *,
        transaction: bool = True,
        raise_on_error: bool = True,
        request_type: Union[QueryInfoRequestType, Callable[[], QueryInfoRequestType]],
        read_consistency: Optional[Literal["none", "weak", "strong"]],
        freshness: Optional[str],
    ) -> BulkResult:
        if read_consistency is None:
            read_consistency = self.read_consistency

        if freshness is None:
            freshness = self.freshness

        qargs: List[str] = []
        if transaction:
            qargs.append("transaction")

        if request_type != "executemany":
            qargs.append("level=" + read_consistency)
            if read_consistency == "none":
                qargs.append("freshness=" + freshness)

        qargs.append("redirect")

        path = base_path + "?" + "&".join(qargs)

        cleaned_request = []
        for operation, parameters in operation_and_parameters:
            cleaned_query, parameters = clean_nulls(operation, parameters)
            cleaned_request.append([cleaned_query, *parameters])

        request_id = secrets.token_hex(4)

        def msg_supplier_1(max_length: Optional[int]) -> str:
            abridged_request = repr(cleaned_request)
            if max_length is not None and len(abridged_request) > max_length:
                abridged_request = abridged_request[:max_length] + "..."

            return f"  [RQLITE BULK {path} {{{request_id}}}] - {abridged_request}"

        log(self.connection.log_config.write_start, msg_supplier_1)

        request_started_at = time.perf_counter()
        response = await self.connection.fetch_response(
            "POST",
            path,
            json=cleaned_request,
            headers={"Content-Type": "application/json; charset=UTF-8"},
            query_info=QueryInfoLazy(
                operations=lambda: [
                    operation for operation, _ in operation_and_parameters
                ],
                params=lambda: [
                    parameters for _, parameters in operation_and_parameters
                ],
                request_type=request_type,
                consistency=read_consistency,
                freshness=freshness,
            ),
        )
        payload: dict = await response.json()
        await response.__aexit__(None, None, None)
        request_time = time.perf_counter() - request_started_at

        def msg_supplier_2(max_length: Optional[int]) -> str:
            abridged_payload = repr(payload)
            if max_length is not None and len(abridged_payload) > max_length:
                abridged_payload = abridged_payload[:max_length] + "..."

            return f"    {{{request_id}}} in {request_time:.3f}s -> {abridged_payload}"

        log(
            self.connection.log_config.write_response,
            msg_supplier_2,
        )

        result = BulkResult.parse(payload)
        if raise_on_error:
            result.raise_on_error(f"{request_id=}; {operation_and_parameters=}")

        return result

    async def executemany3(
        self,
        operation_and_parameters: Iterable[Tuple[str, Iterable[Any]]],
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
        return await self._executemany3(
            "/db/execute",
            operation_and_parameters,
            transaction=transaction,
            raise_on_error=raise_on_error,
            request_type="executemany",
            read_consistency="strong",
            freshness="",
        )

    async def executeunified2(
        self,
        operations: Iterable[str],
        seq_of_parameters: Optional[Iterable[Iterable[Any]]] = None,
        transaction: bool = True,
        raise_on_error: bool = True,
        read_consistency: Optional[Literal["none", "weak", "strong"]] = None,
        freshness: Optional[str] = None,
    ) -> BulkResult:
        """Equivalent to executemany2(), except adds support for queries. Be
        aware that this will interpret control statements as queries, meaning if you
        have all readonly queries with control statements it will be executed using
        the read consistency. This may or may not be desirable. Use `strong` read
        consistency if the queries should be executed within the raft log regardless
        of the result of `sqlite3_stmt_readonly()`.

        Args:
            operations (Iterable[str]): The operations to execute.
            seq_of_parameters (Iterable[Iterable[Any]]): The parameters to pass to each option.
            transaction (bool): If True, execute the operations within a transaction.
            raise_on_error (bool): If True, raise an error if any of the operations fail. If
                False, you can check the result item's error property to see if the
                operation failed.
            read_consistency (Optional[Literal["none", "weak", "strong"]]):
                The read consistency to use when executing the query. If None,
                use the default read consistency for this cursor. Ignored if any of
                the operations write according to the sqlite3_stmt_readonly() function.
            freshness (Optional[str]): The freshness to use when executing
        """
        return await self._executemany2(
            "/db/request",
            operations,
            seq_of_parameters=seq_of_parameters,
            transaction=transaction,
            raise_on_error=raise_on_error,
            request_type=partial(determine_unified_request_type, operations),
            read_consistency=read_consistency,
            freshness=freshness,
        )

    async def executeunified3(
        self,
        operation_and_parameters: Iterable[Tuple[str, Iterable[Any]]],
        transaction: bool = True,
        raise_on_error: bool = True,
        read_consistency: Optional[Literal["none", "weak", "strong"]] = None,
        freshness: Optional[str] = None,
    ) -> BulkResult:
        """Equivalent to executemany3(), except adds support for queries. Be
        aware that this will interpret control statements as queries, meaning if you
        have all readonly queries with control statements it will be executed using
        the read consistency. This may or may not be desirable. Use `strong` read
        consistency if the queries should be executed within the raft log regardless
        of the result of `sqlite3_stmt_readonly()`.

        Args:
            operations_and_parameters (Tuple[Tuple[str, Tuple[Any]]]):
                The operations and corresponding parameters to execute.
            transaction (bool): If True, execute the operations within a transaction.
            raise_on_error (bool): If True, raise an error if any of the operations fail. If
                False, you can check the result item's error property to see if the
                operation failed.
            read_consistency (Optional[Literal["none", "weak", "strong"]]):
                The read consistency to use when executing the query. If None,
                use the default read consistency for this cursor. Ignored if any of
                the operations write according to the sqlite3_stmt_readonly() function.
            freshness (Optional[str]): The freshness to use when executing
        """
        return await self._executemany3(
            "/db/request",
            operation_and_parameters,
            transaction=transaction,
            raise_on_error=raise_on_error,
            request_type=lambda: determine_unified_request_type(
                [op for (op, _) in operation_and_parameters]
            ),
            read_consistency=read_consistency,
            freshness=freshness,
        )

    @overload
    async def explain(
        self,
        operation: str,
        parameters: Optional[Iterable[Any]] = None,
        *,
        out: Literal["str"] = "str",
        read_consistency: Optional[Literal["none", "weak"]] = None,
        freshness: Optional[str] = None,
        indent: int = 3,
        include_raw: bool = False,
    ) -> str:
        ...

    @overload
    async def explain(
        self,
        operation: str,
        parameters: Optional[Iterable[Any]] = None,
        *,
        out: Literal["print"],
        read_consistency: Optional[Literal["none", "weak"]] = None,
        freshness: Optional[str] = None,
        indent: int = 3,
        include_raw: bool = False,
    ) -> None:
        ...

    @overload
    async def explain(
        self,
        operation: str,
        parameters: Optional[Iterable[Any]] = None,
        *,
        out: Literal["plan"],
        read_consistency: Optional[Literal["none", "weak"]] = None,
        freshness: Optional[str] = None,
    ) -> ExplainQueryPlan:
        ...

    @overload
    async def explain(
        self,
        operation: str,
        parameters: Optional[Iterable[Any]] = None,
        *,
        out: "SyncWritableIO",
        read_consistency: Optional[Literal["none", "weak"]] = None,
        freshness: Optional[str] = None,
        indent: int = 3,
        include_raw: bool = False,
        sync: Optional[Literal[True]] = None,
    ) -> None:
        ...

    @overload
    async def explain(
        self,
        operation: str,
        parameters: Optional[Iterable[Any]] = None,
        *,
        out: "AsyncWritableIO",
        read_consistency: Optional[Literal["none", "weak"]] = None,
        freshness: Optional[str] = None,
        indent: int = 3,
        include_raw: bool = False,
        sync: Optional[Literal[False]] = None,
    ) -> None:
        ...

    async def explain(
        self,
        operation: str,
        parameters: Optional[Iterable[Any]] = None,
        *,
        out: Union[
            Literal["str", "print", "plan"], "SyncWritableIO", "AsyncWritableIO"
        ] = "str",
        read_consistency: Optional[Literal["none", "weak"]] = None,
        freshness: Optional[str] = None,
        indent: int = 3,
        include_raw: bool = False,
        sync: Optional[bool] = None,
    ) -> Union[ExplainQueryPlan, str, None]:
        """Accepts any query; if it is not prefixed with EXPLAIN then
        it will be prefixed with EXPLAIN QUERY PLAN. The result will
        then be parsed into the corresponding tree structure and either
        written to the designated output or returned.

        This always raises if there is an error, and can only operate
        at none/weak level. If the read consistency is not specified
        and the cursor read consistency is strong, weak consistency
        is used instead.

        Args:
            operation (str): The query to execute.
            out (Union[Literal["str", "print", "plan"], SyncWritableIO, AsyncWritableIO]):
                The output to write the explain query plan to. If "str",
                return the explain query plan as a string. If "print",
                print the explain query plan to stdout. If "plan", return
                the explain query plan as a tree structure. If a writable
                stream, write the explain query plan to the stream.

                Async vs sync is detected via if the function is coroutine or
                not unless specified via "sync"
            parameters (Optional[Iterable[Any]]): The parameters to pass to the query.
                Parameters must be specified if the operation is a parameterized query,
                though the values generally only need to be of the correct shape for
                the appropriate plan to be returned
            read_consistency ("none", "weak", None): The read consistency to use,
                None for a random node, weak for the current leader, or None for the
                cursor default (downgrading strong to weak as query plans are not
                necessarily deterministic)
            freshness (Optional[str]): The freshness to use when executing
                none read consistency queries. If None, use the default freshness
                for this cursor.
            indent (int): The number of characters to indent each level when
                formatting the plan. Ignored for "plan" output.
            include_raw (bool): If True, include the ids that made each row in the
                formatted output. Ignored for "plan" output.
            sync (Optional[bool]): Ignored unless the output is a writable stream.
                If None, we check if the `write` function is a coroutine function,
                i.e., is defined via `async def`. If True, we assume it's synchronous,
                and if False, we assume it's asynchronous. Generally you don't need
                to specify this unless the stream returns an awaitable but isn't a
                typical coroutine function.

        Returns:
            Depends on the value of out. If "str", return the explain query plan as a
            string. If "print", print the explain query plan to stdout. If "plan",
            return the explain query plan as a tree structure. If a writable stream,
            write the explain query plan to the bytes stream and return None
        """
        command = get_sql_command(operation)
        if command != "EXPLAIN":
            operation = f"EXPLAIN QUERY PLAN {operation}"

        if read_consistency is None:
            read_consistency = (
                self.read_consistency if self.read_consistency != "strong" else "weak"
            )

        result = await self.execute(
            operation,
            parameters,
            read_consistency=read_consistency,
            freshness=freshness,
        )
        if out == "plan":
            return parse_explain_query_plan(result)
        elif out == "str":
            return format_explain_query_plan_result(
                result, indent=indent, include_raw=include_raw
            )
        elif out == "print":
            print_explain_query_plan_result(
                result, indent=indent, include_raw=include_raw
            )
            return
        else:
            assert not isinstance(out, str), f"unrecognized out value: {out}"

            plan = parse_explain_query_plan(result)

            if sync is False or (
                sync is None and inspect.iscoroutinefunction(out.write)
            ):
                await async_write_explain_query_plan(
                    plan, out, indent=indent, include_raw=include_raw
                )
            else:
                write_explain_query_plan(
                    plan, out, indent=indent, include_raw=include_raw
                )
            return

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
