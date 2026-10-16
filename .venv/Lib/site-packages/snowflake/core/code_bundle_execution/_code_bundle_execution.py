from collections.abc import Iterable
from typing import TYPE_CHECKING, Optional, Union

from snowflake.core._internal.telemetry import api_telemetry
from snowflake.core._operation import PollingOperation, PollingOperations
from snowflake.core.code_bundle_execution._generated.api.code_bundle_execution_api_base import (
    CodeBundleExecutionCollectionBase,
    CodeBundleExecutionResourceBase,
)
from snowflake.core.code_bundle_execution._generated.models import (
    CodeBundleExecution,
    ExecuteCodeBundleRequest,
    SuccessAcceptedResponse,
    SuccessResponse,
)
from snowflake.core.exceptions import NotFoundError


if TYPE_CHECKING:
    from snowflake.core import Root


class CodeBundleExecutionCollection(CodeBundleExecutionCollectionBase):
    """Represents the collection operations on the Snowflake code bundle execution resource.

    A code bundle execution runs a code bundle directly from a stage location, without first creating a
    named code bundle. Executions are not scoped to a database or schema, so this resource is exposed at
    the account level. With this collection you can start an execution, and obtain a reference to an
    existing execution (by its execution id) to fetch its status or cancel it.

    Examples
    ________
    Executing a code bundle:

    >>> root.code_bundle_execution.execute(
    ...     ExecuteCodeBundleRequest(from_location="@my_db.my_schema.my_stage/src", entrypoint="main.py")
    ... )

    Supplying the bundle configuration inline with a typed specification:

    >>> from snowflake.core.code_bundle_execution import BundleSpec, CodeBundleSpecification
    >>> root.code_bundle_execution.execute(
    ...     ExecuteCodeBundleRequest(
    ...         from_location="@my_db.my_schema.my_stage/src",
    ...         entrypoint="main.py",
    ...         specification=CodeBundleSpecification(
    ...             bundle=BundleSpec(type="custom", compute_type="warehouse", language="python")
    ...         ),
    ...     )
    ... )

    Getting a reference to an existing execution:

    >>> execution = root.code_bundle_execution["<execution_id>"]
    """

    def __init__(self, root: "Root") -> None:
        super().__init__(root, CodeBundleExecutionResource)

    @api_telemetry
    def execute(
        self,
        execute_code_bundle_request: ExecuteCodeBundleRequest,
        async_exec: Optional[bool] = True,
    ) -> Union[SuccessResponse, SuccessAcceptedResponse]:
        """Execute a code bundle directly from a stage location.

        Parameters
        __________
        execute_code_bundle_request: ExecuteCodeBundleRequest
            The specification of the code bundle to execute, including the stage location to run from
            (``from_location``) and the entry point (``entrypoint``). (required) The bundle configuration
            can be supplied inline through the request's ``specification`` field, a typed
            :class:`~snowflake.core.code_bundle_execution.CodeBundleSpecification` wrapping a
            :class:`~snowflake.core.code_bundle_execution.BundleSpec`; a plain ``dict`` matching the same
            shape is still accepted. When omitted, the configuration is read from the stage.
        async_exec: bool, optional
            Whether the code bundle should be executed asynchronously on the server. Defaults to ``True``,
            which submits the execution asynchronously and returns once the server has accepted it. Pass
            ``False`` to run the code bundle synchronously on the server.

        Returns
        _______
        Union[SuccessResponse, SuccessAcceptedResponse]
            A ``SuccessResponse`` when the execution completes synchronously, or a
            ``SuccessAcceptedResponse`` when ``async_exec`` is ``True`` and the execution was accepted
            for asynchronous processing.

        Examples
        ________
        Executing a code bundle with the configuration read from the stage:

        >>> root.code_bundle_execution.execute(
        ...     ExecuteCodeBundleRequest(from_location="@my_db.my_schema.my_stage/src", entrypoint="main.py")
        ... )

        Executing a code bundle with a typed, inline bundle specification:

        >>> from snowflake.core.code_bundle_execution import BundleSpec, CodeBundleSpecification
        >>> root.code_bundle_execution.execute(
        ...     ExecuteCodeBundleRequest(
        ...         from_location="@my_db.my_schema.my_stage/src",
        ...         entrypoint="main.py",
        ...         specification=CodeBundleSpecification(
        ...             bundle=BundleSpec(
        ...                 type="custom",
        ...                 compute_type="warehouse",
        ...                 language="python",
        ...                 compute_options={"runtime_version": "3.11"},
        ...             )
        ...         ),
        ...     )
        ... )
        """
        return self._api.execute_code_bundle(
            execute_code_bundle_request=execute_code_bundle_request,
            async_exec=async_exec,
            async_req=False,
        )

    @api_telemetry
    def execute_async(
        self,
        execute_code_bundle_request: ExecuteCodeBundleRequest,
        async_exec: Optional[bool] = True,
    ) -> PollingOperation[Union[SuccessResponse, SuccessAcceptedResponse]]:
        """An asynchronous version of :func:`execute`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self._api.execute_code_bundle(
            execute_code_bundle_request=execute_code_bundle_request,
            async_exec=async_exec,
            async_req=True,
        )
        # The generated client types the future as Future[SuccessResponse], but a 202 response
        # (async_exec=True) is deserialized as SuccessAcceptedResponse at runtime.
        return PollingOperations.identity(future)  # type: ignore[arg-type]


class CodeBundleExecutionResource(CodeBundleExecutionResourceBase):
    """Represents a reference to a Snowflake code bundle execution.

    A code bundle execution reference is identified by its execution id (the query id of the execution).
    With this reference you can fetch the execution's status or cancel it.
    """

    _plural_name = "code_bundle_executions"

    def __init__(self, name: str, collection: CodeBundleExecutionCollection) -> None:
        super().__init__(name, collection)

    @api_telemetry
    def fetch_status(self) -> CodeBundleExecution:  # type: ignore[override]
        """Fetch the status of this code bundle execution.

        Returns
        _______
        CodeBundleExecution
            The status of the execution. If the server returns more than one matching record, the first
            one is returned.

        Raises
        ______
        NotFoundError
            If no execution with this execution id exists.

        Examples
        ________
        Fetching the status of an execution:

        >>> root.code_bundle_execution["<execution_id>"].fetch_status()
        """
        result = self.collection._api.fetch_code_bundle_execution_status(
            execution_id=self._identifier,
            async_req=False,
        )
        return self._first_or_not_found(result)

    @api_telemetry
    def fetch_status_async(self) -> PollingOperation[CodeBundleExecution]:  # type: ignore[override]
        """An asynchronous version of :func:`fetch_status`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self.collection._api.fetch_code_bundle_execution_status(
            execution_id=self._identifier,
            async_req=True,
        )
        return PollingOperation(future, self._first_or_not_found)

    def _first_or_not_found(self, executions: Iterable[CodeBundleExecution]) -> CodeBundleExecution:
        """Return the first execution record, or raise :class:`NotFoundError` if there are none."""
        for execution in executions:
            return execution
        raise NotFoundError(
            self.collection.root,
            status=404,
            reason=f"Code bundle execution '{self._identifier}' not found.",
        )

    @api_telemetry
    def cancel(self) -> Union[SuccessResponse, SuccessAcceptedResponse]:  # type: ignore[override]
        """Cancel this code bundle execution.

        Returns
        _______
        Union[SuccessResponse, SuccessAcceptedResponse]
            The server's response to the cancel request (a ``SuccessResponse`` carrying a status message).

        Examples
        ________
        Cancelling an execution:

        >>> root.code_bundle_execution["<execution_id>"].cancel()
        """
        return self.collection._api.cancel_code_bundle_execution(
            execution_id=self._identifier,
            async_req=False,
        )

    @api_telemetry
    def cancel_async(self) -> PollingOperation[Union[SuccessResponse, SuccessAcceptedResponse]]:  # type: ignore[override]
        """An asynchronous version of :func:`cancel`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self.collection._api.cancel_code_bundle_execution(
            execution_id=self._identifier,
            async_req=True,
        )
        return PollingOperations.identity(future)  # type: ignore[arg-type]
