from typing import TYPE_CHECKING, Optional, Union

from snowflake.core._internal.telemetry import api_telemetry
from snowflake.core._operation import PollingOperation, PollingOperations
from snowflake.core.code_bundle._generated.api.code_bundle_api_base import (
    CodeBundleCollectionBase,
    CodeBundleResourceBase,
)
from snowflake.core.code_bundle._generated.models import (
    ExecuteCodeBundleRequest,
    SuccessAcceptedResponse,
    SuccessResponse,
)


if TYPE_CHECKING:
    from snowflake.core.schema import SchemaResource


class CodeBundleCollection(CodeBundleCollectionBase):
    """Represents the collection operations on the Snowflake code bundle resource.

    With this collection, you can create, iterate through, and fetch code bundles
    that you have access to in the current context.

    Examples
    ________
    Creating a code bundle instance:

    >>> code_bundles = root.databases["my_db"].schemas["my_schema"].code_bundles
    >>> new_code_bundle = CodeBundle(name="my_code_bundle", from_location="@my_db.my_schema.my_stage/src")
    >>> code_bundles.create(new_code_bundle)
    """

    def __init__(self, schema: "SchemaResource") -> None:
        super().__init__(schema, CodeBundleResource)


class CodeBundleResource(CodeBundleResourceBase):
    """Represents a reference to a Snowflake code bundle.

    With this code bundle reference, you can fetch information about a code bundle, as well as
    perform certain actions on it, such as adding a version, executing it, or dropping it.
    """

    _plural_name = "code_bundles"

    def __init__(self, name: str, collection: CodeBundleCollection) -> None:
        super().__init__(name, collection)

    @api_telemetry
    def execute(  # type: ignore[override]
        self,
        execute_code_bundle_request: ExecuteCodeBundleRequest,
        async_exec: Optional[bool] = None,
    ) -> Union[SuccessResponse, SuccessAcceptedResponse]:
        """Execute this code bundle.

        Parameters
        __________
        execute_code_bundle_request : ExecuteCodeBundleRequest
            The execution specification, including the entry point to run (``entrypoint``). (required) The
            bundle configuration captured when the bundle was created is used by default; it can be
            overridden inline through the request's ``specification`` field, a typed
            :class:`~snowflake.core.code_bundle.CodeBundleSpecification` wrapping a
            :class:`~snowflake.core.code_bundle.BundleSpec` (a plain ``dict`` of the same shape is still
            accepted).
        async_exec : bool, optional
            Whether the code bundle should be executed asynchronously on the server. When ``True``, the
            server accepts the execution and returns a ``SuccessAcceptedResponse`` carrying the job id.
            Defaults to ``None`` (synchronous execution on the server).

        Returns
        _______
        Union[SuccessResponse, SuccessAcceptedResponse]
            A ``SuccessResponse`` when the execution completes synchronously, or a
            ``SuccessAcceptedResponse`` (with the ``job_id``) when the server accepts it for asynchronous
            processing.

        Examples
        ________
        Executing a code bundle:

        >>> code_bundles["my_code_bundle"].execute(ExecuteCodeBundleRequest(entrypoint="main.py"))

        Overriding the bundle configuration with a typed, inline specification:

        >>> from snowflake.core.code_bundle import BundleSpec, CodeBundleSpecification
        >>> code_bundles["my_code_bundle"].execute(
        ...     ExecuteCodeBundleRequest(
        ...         entrypoint="main.py",
        ...         specification=CodeBundleSpecification(
        ...             bundle=BundleSpec(type="custom", compute_type="warehouse", language="python")
        ...         ),
        ...     )
        ... )
        """
        return self.collection._api.execute_code_bundle(
            self.database.name,
            self.schema.name,
            self._identifier,
            execute_code_bundle_request=execute_code_bundle_request,
            async_exec=async_exec,
            async_req=False,
        )

    @api_telemetry
    def execute_async(  # type: ignore[override]
        self,
        execute_code_bundle_request: ExecuteCodeBundleRequest,
        async_exec: Optional[bool] = None,
    ) -> PollingOperation[Union[SuccessResponse, SuccessAcceptedResponse]]:
        """An asynchronous version of :func:`execute`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self.collection._api.execute_code_bundle(
            self.database.name,
            self.schema.name,
            self._identifier,
            execute_code_bundle_request=execute_code_bundle_request,
            async_exec=async_exec,
            async_req=True,
        )
        # The generated client types the future as Future[SuccessResponse], but a 202 response
        # (async_exec=True) is deserialized as SuccessAcceptedResponse at runtime.
        return PollingOperations.identity(future)  # type: ignore[arg-type]
