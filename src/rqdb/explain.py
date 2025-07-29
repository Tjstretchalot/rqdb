"""This module is intended to take the result from EXPLAIN QUERY PLAN and
format in a more human-readable way.
"""

from typing import Dict, List, Optional, TYPE_CHECKING
from rqdb.result import ResultItem
from dataclasses import dataclass
import io

if TYPE_CHECKING:
    from rqdb.connection import SyncWritableIO
    from rqdb.async_connection import AsyncWritableIO


@dataclass
class ExplainQueryNode:
    id: int
    detail: str
    parent: Optional["ExplainQueryNode"]
    children: List["ExplainQueryNode"]


@dataclass
class ExplainQueryPlan:
    roots: List[ExplainQueryNode]
    largest_id: int


def parse_explain_query_plan(result: ResultItem) -> ExplainQueryPlan:
    """Parses the given result from EXPLAIN QUERY PLAN into the
    tree structure it represents.
    """
    assert result.results, "Result must have at least one row."
    assert len(result.results[0]) == 4, "Result must have four columns."

    roots: List[ExplainQueryNode] = []
    node_by_id: Dict[int, ExplainQueryNode] = {}
    largest_id = 0

    for row in result.results:
        row_id = row[0]
        detail = row[3]
        parent_id = row[1]

        assert isinstance(row_id, int), "Row ID must be an integer."
        assert isinstance(parent_id, int), "Parent ID must be an integer."
        assert isinstance(detail, str), "Detail must be a string."

        node = ExplainQueryNode(
            id=row_id,
            detail=detail,
            parent=None,
            children=[],
        )
        node_by_id[row_id] = node
        largest_id = max(largest_id, row_id)

        if parent_id == 0:
            roots.append(node)
        else:
            parent_node = node_by_id[parent_id]
            node.parent = parent_node
            parent_node.children.append(node)
    return ExplainQueryPlan(roots=roots, largest_id=largest_id)


def write_explain_query_plan(
    eqp: ExplainQueryPlan,
    out: "SyncWritableIO",
    *,
    indent: int = 3,
    include_raw: bool = False,
) -> None:
    """Given the raw result from EXPLAIN QUERY PLAN, formats it into a
    tree format similar to eqp in the sqlite3 shell.

    Arguments:
        eqp (ExplainQueryPlan): The parsed result from
            parse_explain_query_plan.
        out (SyncWritableIO): The output stream to write to. A buffered
            writer is strongly recommended.
        indent (int): The number of spaces to indent when indicating
            that a line is nested. Must be at least 1
        include_raw (bool): If true, each line is prepended with the
            raw row from the result that was used to generate it.
    """
    assert indent > 0, "Indent must be at least 1."
    largest_id_length = len(str(eqp.largest_id))
    for node in eqp.roots:
        write_explain_query_node(
            node,
            out,
            indent=indent,
            include_raw=include_raw,
            largest_id_length=largest_id_length,
        )


async def async_write_explain_query_plan(
    eqp: ExplainQueryPlan,
    out: "AsyncWritableIO",
    *,
    indent: int = 3,
    include_raw: bool = False,
) -> None:
    """Given the raw result from EXPLAIN QUERY PLAN, formats it into a
    tree format similar to eqp in the sqlite3 shell.

    Arguments:
        eqp (ExplainQueryPlan): The parsed result from
            parse_explain_query_plan.
        out (AsyncWritableIO): The output stream to write to. A buffered
            writer is strongly recommended.
        indent (int): The number of spaces to indent when indicating
            that a line is nested. Must be at least 1
        include_raw (bool): If true, each line is prepended with the
            raw row from the result that was used to generate it.
    """
    assert indent > 0, "Indent must be at least 1."
    largest_id_length = len(str(eqp.largest_id))
    for node in eqp.roots:
        await async_write_explain_query_node(
            node,
            out,
            indent=indent,
            include_raw=include_raw,
            largest_id_length=largest_id_length,
        )


def write_explain_query_node(
    node: ExplainQueryNode,
    out: "SyncWritableIO",
    *,
    level: int = 0,
    indent: int = 3,
    include_raw: bool = False,
    largest_id_length: int = 0,
) -> None:
    """Writes a single explain query node to the given output stream.

    Args:
        node (ExplainQueryNode): The node to write.
        out (SyncWritableIO): The output stream to write to.
        level (int): The current level of indentation, where 0 is no
            indentation, 1 is one level of indentation, and so on.
        indent (int): The number of spaces to indent per level
            of indentation.
        include_raw (bool): If true, each line is prepended with the
            raw row from the result that was used to generate it.
        largest_id_length (int): The length when printed in decimal
            of the largest id in the result, used for padding.
    """
    if include_raw:
        parent_id = node.parent.id if node.parent else 0
        out.write(
            f"[id: {node.id:>{largest_id_length}}, par: {parent_id:>{largest_id_length}}] ".encode(
                "ascii"
            )
        )

    for _ in range(level - 1):
        for _ in range(indent):
            out.write(b" ")

    if level > 0:
        for _ in range(indent - 1):
            out.write(b" ")

    if level > 0:
        out.write(b"|")

    for _ in range(indent - 1):
        out.write(b"-")

    out.write(node.detail.encode("utf-8"))
    out.write(b"\n")

    for child in node.children:
        write_explain_query_node(
            child,
            out,
            level=level + 1,
            indent=indent,
            include_raw=include_raw,
            largest_id_length=largest_id_length,
        )


async def async_write_explain_query_node(
    node: ExplainQueryNode,
    out: "AsyncWritableIO",
    *,
    level: int = 0,
    indent: int = 3,
    include_raw: bool = False,
    largest_id_length: int = 0,
) -> None:
    """Writes a single explain query node to the given output stream.

    Args:
        node (ExplainQueryNode): The node to write.
        out (SyncWritableIO): The output stream to write to.
        level (int): The current level of indentation, where 0 is no
            indentation, 1 is one level of indentation, and so on.
        indent (int): The number of spaces to indent per level
            of indentation.
        include_raw (bool): If true, each line is prepended with the
            raw row from the result that was used to generate it.
        largest_id_length (int): The length when printed in decimal
            of the largest id in the result, used for padding.
    """
    if include_raw:
        parent_id = node.parent.id if node.parent else 0
        await out.write(
            f"[id: {node.id:>{largest_id_length}}, par: {parent_id:>{largest_id_length}}] ".encode(
                "ascii"
            )
        )

    await out.write(b" " * indent * max(level - 1, 0))

    if level > 0:
        await out.write(b" " * (indent - 1))

    if level > 0:
        await out.write(b"|")

    await out.write(b"-" * (indent - 1))

    await out.write(node.detail.encode("utf-8"))
    await out.write(b"\n")

    for child in node.children:
        await async_write_explain_query_node(
            child,
            out,
            level=level + 1,
            indent=indent,
            include_raw=include_raw,
            largest_id_length=largest_id_length,
        )


def format_explain_query_plan_result(
    result: ResultItem, *, indent: int = 3, include_raw: bool = False
) -> str:
    """
    A convenience function that parses the given result from
    EXPLAIN QUERY PLAN and returns a string representation of it.
    """
    eqp = parse_explain_query_plan(result)
    raw_out = io.BytesIO()
    write_explain_query_plan(eqp, raw_out, indent=indent, include_raw=include_raw)
    return raw_out.getvalue().decode("utf-8")


def print_explain_query_plan_result(
    result: ResultItem, *, indent: int = 3, include_raw: bool = False
) -> None:
    """A convenience function that parses the given result from
    EXPLAIN QUERY PLAN and writes it to stdout.
    """
    print(
        format_explain_query_plan_result(result, indent=indent, include_raw=include_raw)
    )
