# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

"""Manages Snowflake code bundle executions.

Example:
    >>> code_bundle_execution: CodeBundleExecutionCollection = root.code_bundle_execution
    >>> code_bundle_execution.execute(
    ...     ExecuteCodeBundleRequest(from_location="@mydb.myschema.mystage/src", entrypoint="main.py")
    ... )
    >>> execution = code_bundle_execution["<execution_id>"]
    >>> status = execution.fetch_status()

Refer to :class:`snowflake.core.Root` to create the ``root``.
"""

from ._code_bundle_execution import CodeBundleExecutionCollection, CodeBundleExecutionResource
from ._generated.models import (
    BundleSpec,
    CodeBundleExecution,
    CodeBundleSpecification,
    ExecuteCodeBundleRequest,
    StageMount,
    SuccessAcceptedResponse,
    SuccessResponse,
)


__all__ = [
    "CodeBundleExecution",
    "CodeBundleExecutionResource",
    "CodeBundleExecutionCollection",
    "BundleSpec",
    "CodeBundleSpecification",
    "ExecuteCodeBundleRequest",
    "StageMount",
    "SuccessAcceptedResponse",
    "SuccessResponse",
]
