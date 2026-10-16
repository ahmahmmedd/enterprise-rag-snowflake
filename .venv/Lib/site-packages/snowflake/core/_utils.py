from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re

from collections.abc import Coroutine
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Callable, Generator, Optional, Protocol, TypeVar

from packaging import version
from pydantic import StrictStr

from snowflake.core import _thread_pool
from snowflake.core.exceptions import InvalidResultError


if TYPE_CHECKING:
    from snowflake.connector import SnowflakeConnection
    from snowflake.core import Root
    from snowflake.core.function import Function
    from snowflake.core.procedure import Procedure
    from snowflake.core.tag import TagResource, TagValue
    from snowflake.core.user_defined_function import UserDefinedFunction

logger = logging.getLogger(__name__)


def get_function_name_with_args(function: Function | Procedure | UserDefinedFunction) -> str:
    return f"{function.name}({','.join([str(argument.datatype) for argument in function.arguments])})"


FUNCTION_WITH_ARGS_PATTERN = re.compile(r"""^(\"([^\"]|\"\")+\"|[a-zA-Z_][a-zA-Z0-9_$]*)\(([A-Za-z,0-9_]*)\)$""")


def replace_function_name_in_name_with_args(name_with_args: str, new_name: str) -> str:
    matcher = FUNCTION_WITH_ARGS_PATTERN.match(name_with_args)
    if not matcher:
        raise ValueError("Invalid function name with arguments")

    args = matcher.group(3)
    return f"{new_name}({args})"


def check_version_gte(version_to_check: str, reference_version: str) -> bool:
    return version.parse(version_to_check) >= version.parse(reference_version)


def check_version_lte(version_to_check: str, reference_version: str) -> bool:
    return version.parse(version_to_check) <= version.parse(reference_version)


def fix_hostname(hostname: str) -> str:
    """Perform automatic hostname fixes.

    When a legacy format hostname is used to connect to Snowflake SSL certificates might not work as expected.
    For example if the legacy url format is used to connect to Snowflake (see
    https://docs.snowflake.com/en/user-guide/organizations-connect for more documentation) _ (underscores) should
    be replaced with - (dashes).
    """
    first_part, _, rest = hostname.partition(".")
    if not rest:
        raise Exception(f"provided hostname '{hostname}' is invalid")
    first_part = first_part.replace("_", "-")
    new_hostname = f"{first_part}.{rest}"
    if hostname != new_hostname:
        logger.info(f"Hostname automatically fixed from '{hostname}' to '{new_hostname}'")
    return new_hostname


def cast_result(result: str, datatype: str | None) -> Any:
    if datatype in ["INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT"]:
        return int(result)
    if datatype in ["NUMBER", "DECIMAL", "NUMERIC"]:
        try:
            return int(result)
        except ValueError:
            return float(result)
    if datatype in ["FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL"]:
        return float(result)
    if datatype == "DECFLOAT":
        return Decimal(result)
    if datatype in ["VARCHAR", "STRING", "TEXT"]:
        return str(result)
    if datatype in ["CHAR", "CHARACTER"]:
        return result
    if datatype == "ARRAY":
        return list(result)
    if datatype in ["GEOMETRY", "GEOGRAPHY", "OBJECT", "VARIANT"]:
        return json.loads(result)
    if datatype == "BOOLEAN":
        return result.lower() in ("yes", "y", "true", "t", "1")
    return result


def map_result(procedure: Procedure, raw_result: Any, extract: bool = False) -> Any:
    """Map result from API to native Python types."""
    from snowflake.core.procedure import ReturnDataType, ReturnTable

    if not isinstance(raw_result, list):
        raise InvalidResultError(f"Procedure result {str(raw_result)} is invalid or empty")

    rt_type = procedure.return_type
    if isinstance(rt_type, ReturnTable):
        if rt_type.column_list is None:
            return raw_result
        processed_rows = []
        columns_mapping = {c.name.lower(): c.datatype for c in rt_type.column_list}
        for row in raw_result:
            processed_rows.append({k: cast_result(v, columns_mapping.get(k.lower())) for k, v in row.items()})
        return processed_rows

    if isinstance(rt_type, ReturnDataType):
        payload = raw_result[0]
        if not isinstance(payload, dict):
            raise TypeError(f"Expected first item to be of type dict but got {type(payload)}")

        if not extract:
            return [{k: cast_result(v, rt_type.datatype) for k, v in payload.items()}]

        # Unpack the result of [{sproc_name: RESULT}]
        result = payload[next(iter(payload.keys()))]
        return cast_result(result, rt_type.datatype)
    return raw_result


class TagAssignmentLike(Protocol):
    tag_value: StrictStr
    level: Optional[StrictStr]
    tag_database: Optional[str]
    tag_name: str
    tag_schema: Optional[str]


TTagAssignment = TypeVar("TTagAssignment", bound=TagAssignmentLike)
TTagReference = TypeVar("TTagReference")


def tag_tuple_to_tag_assignment(
    tag_assignment_cls: Callable[..., TTagAssignment],
    tag_resource: TagResource,
    tag_value: TagValue,
) -> TTagAssignment:
    return tag_assignment_cls(
        tag_name=tag_resource.name,
        tag_schema=tag_resource.schema.name,
        tag_database=tag_resource.database.name,
        tag_value=tag_value.value,
    )


def tag_resource_to_tag_reference(
    tag_reference_cls: Callable[..., TTagReference], resource: TagResource
) -> TTagReference:
    return tag_reference_cls(
        tag_name=resource.name,
        tag_schema=resource.schema.name,
        tag_database=resource.database.name,
    )


def tag_assignment_to_tag_tuple(ta: TagAssignmentLike, root: Root) -> tuple[TagResource, TagValue]:
    from snowflake.core.tag import TagValue

    db = ta.tag_database
    schema = ta.tag_schema

    if db is None:
        raise ValueError("TagAssignment must have tag_database set.")

    if schema is None:
        raise ValueError("TagAssignment must have tag_schema set.")

    tag_resource = root.databases[db].schemas[schema].tags[ta.tag_name]
    tag_value = TagValue(ta.tag_value, ta.level)
    return tag_resource, tag_value


@contextlib.contextmanager
def temporary_paramstyle(paramstyle: str, conn: SnowflakeConnection) -> Generator[None, None, None]:
    original_paramstyle = conn._paramstyle
    conn._paramstyle = paramstyle
    try:
        yield
    finally:
        conn._paramstyle = original_paramstyle


_TCoroResult = TypeVar("_TCoroResult")


def run_coroutine_blocking(coro: Coroutine[Any, Any, _TCoroResult]) -> _TCoroResult:
    """Run *coro* to completion from a synchronous caller, even when an event loop is active.

    :func:`asyncio.run` cannot be invoked from a thread that already has a running event loop, so
    when one is detected the coroutine is dispatched to a worker thread (which has its own
    thread-local loop state) and the calling thread blocks on the result. Otherwise the coroutine
    runs inline via :func:`asyncio.run`.

    Parameters
    ----------
    coro : Coroutine
        The coroutine object to execute. Must not have been awaited.

    Returns
    -------
    Any
        Whatever *coro* returns. Exceptions raised by *coro* propagate to the caller.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    return _thread_pool.get_thread_pool().submit(asyncio.run, coro).result()
