# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

"""Manages Snowflake code bundles.

Example:
    >>> code_bundles: CodeBundleCollection = root.databases["mydb"].schemas["myschema"].code_bundles
    >>> mybundle = code_bundles.create(
    ...     CodeBundle(name="mybundle", from_location="@mydb.myschema.mystage/src")
    ... )
    >>> bundle_iter = code_bundles.iter(like="my%")
    >>> mybundle = code_bundles["mybundle"]
    >>> an_existing_bundle = code_bundles["an_existing_bundle"]

Refer to :class:`snowflake.core.Root` to create the ``root``.
"""

from ._code_bundle import CodeBundleCollection, CodeBundleResource
from ._generated.models import (
    AddVersionCodeBundleRequest,
    AddVersionCodeBundleRequestVersion,
    BundleSpec,
    CodeBundle,
    CodeBundleSpecification,
    ExecuteCodeBundleRequest,
    StageMount,
    SuccessAcceptedResponse,
    SuccessResponse,
    VersionDetails,
)


__all__ = [
    "CodeBundle",
    "CodeBundleResource",
    "CodeBundleCollection",
    "AddVersionCodeBundleRequest",
    "AddVersionCodeBundleRequestVersion",
    "BundleSpec",
    "CodeBundleSpecification",
    "ExecuteCodeBundleRequest",
    "StageMount",
    "SuccessAcceptedResponse",
    "SuccessResponse",
    "VersionDetails",
]
