"""This module is for any necessary SQL preprocessing required for the
rqlite client.
"""
import re
from typing import Tuple


WITH_MATCHER = re.compile(
    r"WITH( RECURSIVE)?\s+(,?\s*\S+\s+AS\s+\(.+?\))+\s+(?P<cmd>INSERT|UPDATE|DELETE|SELECT)",
    re.IGNORECASE,
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
    if sql_str[:4].upper() == "WITH":
        match = WITH_MATCHER.match(sql_str)
        if match is None:
            raise Exception(f"could not determine SQL command for {sql_str=}")
        return match.group("cmd").upper()

    space_idx = sql_str.find(" ")
    if space_idx == -1:
        raise Exception(f"could not determine SQL command for {sql_str=}")
    return sql_str[:space_idx].upper()


def clean_nulls(sql_str: str, args: tuple) -> Tuple[str, tuple]:
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
    next_arg_index = 0

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

            if next_arg_index >= len(args):
                raise ValueError(
                    f"{sql_str=} has a ? without a matching argument (args={args})"
                )

            if args[next_arg_index] is None:
                result.append(sql_str[current_start_index:i])
                result.append("NULL")
                current_start_index = i + 1
            else:
                result_args.append(args[next_arg_index])

            next_arg_index += 1
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

    if next_arg_index != len(args):
        raise ValueError(f"{sql_str=} has an argument without a matching ? ({args=})")

    result.append(sql_str[current_start_index:])
    return "".join(result), tuple(result_args)
