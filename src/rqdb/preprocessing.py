"""This module is for any necessary SQL preprocessing required for the
rqlite client.
"""
import re
from typing import Any, Iterable, Literal, Tuple


WITH_MATCHER = re.compile(
    r"WITH( RECURSIVE)?\s+(,?\s*\S+(\s?\([^\)]+\))?\s+AS\s+((NOT\s+)?MATERIALIZED\s+)?\(.+?\))+\s+(?P<cmd>INSERT|UPDATE|DELETE|SELECT)",
    re.IGNORECASE | re.DOTALL,
)
"""The matcher to use for determing the sql command for a SQL string with a WITH clause"""


def get_sql_command(sql_str: str) -> str:
    """Determines which sql command is being used in the given SQL string.

    Args:
        sql_str (str): The SQL string to parse.

    Returns:
        The corresponding command (SELECT, INSERT, etc.)

    Raises:
        Exception: If the command could not be determined
    """
    sql_str = sql_str.lstrip()
    if sql_str[:4].upper() == "WITH":
        match = WITH_MATCHER.match(sql_str)
        if match is None:
            raise Exception(f"could not determine SQL command for {sql_str=}")
        return match.group("cmd").upper()

    whitespace_idx = -1
    for i, c in enumerate(sql_str):
        if c.isspace():
            whitespace_idx = i
            break
    else:
        raise Exception(f"could not determine SQL command for {sql_str=}")
    return sql_str[:whitespace_idx].upper()


def clean_nulls(sql_str: str, args: Iterable[Any]) -> Tuple[str, Iterable[Any]]:
    """Currently RQLite does not handle NULL-arguments. We have to
    do our best to manipulate the SQL-string to replace the appropriate
    ? with NULLs. We will assume there are no non-parameter ?-arguments --
    but to avoid confusion, we try to raise an exception if we suspect
    there are any.

    Arguments:
        sql_str (str): The SQL string to clean
        args (tuple[any]): The arguments to clean

    Returns:
        cleaned_sql_str (str): The SQL string with null parameters replaced
        cleaned_args (tuple[any]): The cleaned arguments with nulls replaced
    """
    if None not in args:
        return (sql_str, args)

    quote_char = None
    is_escaped = False

    result = []
    result_args = []
    current_start_index = 0
    arg_iter = iter(args)

    for i, c in enumerate(sql_str):
        if c == "?":
            if is_escaped:
                raise ValueError(
                    f"{sql_str=} appears to have an escaped ? - this is not supported with None-arguments"
                )

            if quote_char is not None:
                raise ValueError(
                    f"{sql_str=} appears to have a quoted ? - this is not supported with None-arguments"
                )

            try:
                next_arg = next(arg_iter)
            except StopIteration:
                raise ValueError(
                    f"{sql_str=} has a ? without a matching argument (args={args})"
                )

            if next_arg is None:
                result.append(sql_str[current_start_index:i])
                result.append("NULL")
                current_start_index = i + 1
            else:
                result_args.append(next_arg)
            continue

        if is_escaped:
            is_escaped = False
            continue

        if c == "\\":
            is_escaped = True
            continue

        if quote_char is not None:
            if c == quote_char:
                quote_char = None
            continue

        if c == "'" or c == '"':
            quote_char = c

    try:
        next(arg_iter)
    except StopIteration:
        pass
    else:
        raise ValueError(f"{sql_str=} has an argument without a matching ? ({args=})")

    result.append(sql_str[current_start_index:])
    return "".join(result), tuple(result_args)


def determine_unified_request_type(
    sql_strs: Iterable[str],
) -> Literal["executeunified-readonly", "executeunified-write"]:
    """Determines the request type for a unified request, which consists of
    multiple sql commands that mix read and write operations. This is a best-effort
    function to approximate sqlite3_stmt_readonly()
    """
    for sql_str in sql_strs:
        cmd = get_sql_command(sql_str)
        if cmd not in ("SELECT", "EXPLAIN"):
            return "executeunified-write"
    return "executeunified-readonly"
