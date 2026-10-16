# mypy: ignore-errors
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, Literal, Optional, Union, overload

from snowflake.core import PollingOperation

from .._internal.telemetry import api_telemetry
from .._utils import cast_result
from ..exceptions import InvalidArgumentsError, InvalidResultError
from . import ReturnTable
from ._generated import UserDefinedFunctionArgument
from ._generated.api.user_defined_function_api_base import (
    UserDefinedFunctionCollectionBase,
    UserDefinedFunctionResourceBase,
)


if TYPE_CHECKING:
    from snowflake.core.schema import SchemaResource


class UserDefinedFunctionCollection(UserDefinedFunctionCollectionBase):
    """Represents the collection operations on the Snowflake User Defined Function resource.

    With this collection, you can create, iterate through, and fetch user defined functions
    that you have access to in the current context.

    Examples
    ________
    Creating a user defined function instance of python language:

    >>> user_defined_functions.create(
    ...     UserDefinedFunction(
    ...         name="my_python_function",
    ...         arguments=[],
    ...         return_type=ReturnDataType(datatype="VARIANT"),
    ...         language_config=PythonFunction(runtime_version="3.13", packages=[], handler="udf"),
    ...         body='''
    ... def udf():
    ...     return {"key": "value"}
    ...             ''',
    ...     )
    ... )
    """

    _identifier_requires_args = True

    def __init__(self, schema: "SchemaResource") -> None:
        super().__init__(schema, UserDefinedFunctionResource)


class UserDefinedFunctionResource(UserDefinedFunctionResourceBase):
    """Represents a reference to a Snowflake user defined function.

    With this user defined function reference, you can fetch information about a user defined
    function, as well as perform certain actions on it.
    """

    _identifier_requires_args = True
    _plural_name = "user_defined_functions"

    def __init__(self, name_with_args: str, collection_class: UserDefinedFunctionCollection) -> None:
        super().__init__(name_with_args, collection_class)

    @api_telemetry
    def rename(
        self,
        target_name: str,
        target_database: Optional[str] = None,
        target_schema: Optional[str] = None,
        if_exists: Optional[bool] = None,
    ) -> None:
        """Rename this user defined function.

        Parameters
        __________
        target_name: str
            The new name of the user defined function
        target_database: str, optional
            The database where the user defined function will be located
        target_schema: str, optional
            The schema where the user defined function will be located
        if_exists: bool, optional
            Check the existence of user defined function before rename

        Examples
        ________
        Renaming this user defined function using its reference:

        >>> user_defined_function_reference.rename("my_other_user_defined_function")

        Renaming this user defined function if it exists:

        >>> user_defined_function_reference.rename("my_other_user_defined_function", if_exists=True)

        Renaming this user defined function and relocating it to another schema within same database:

        >>> user_defined_function_reference.rename(
        ...     "my_other_user_defined_function", target_schema="my_other_schema", if_exists=True
        ... )

        Renaming this user defined function and relocating it to another database and schema:

        >>> user_defined_function_reference.rename(
        ...     "my_other_user_defined_function",
        ...     target_database="my_other_database",
        ...     target_schema="my_other_schema",
        ...     if_exists=True,
        ... )
        """
        if target_database is None:
            target_database = self.database.name
        if target_schema is None:
            target_schema = self.schema.name

        super().rename(
            target_name=target_name,
            target_database=target_database,
            target_schema=target_schema,
            if_exists=if_exists,
        )

    @api_telemetry
    def rename_async(
        self,
        target_name: str,
        target_database: Optional[str] = None,
        target_schema: Optional[str] = None,
        if_exists: Optional[bool] = None,
    ) -> PollingOperation[None]:
        """An asynchronous version of :func:`rename`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        if target_database is None:
            target_database = self.database.name
        if target_schema is None:
            target_schema = self.schema.name

        return super().rename_async(
            target_name=target_name,
            target_database=target_database,
            target_schema=target_schema,
            if_exists=if_exists,
        )

    @api_telemetry
    def execute(self, input_args: Optional[list[UserDefinedFunctionArgument]] = None) -> Any:
        """Execute this user defined function.

        Parameters
        __________
        input_args: list[UserDefinedFunctionArgument], optional
            A list of arguments to pass to the function. The number of arguments must match the number of arguments
            the user defined function expects. Name, datatype and value fields of UserDefinedFunctionArgument are
            required.

        Examples
        ________
        Executing a user defined function using its reference:

        >>> user_defined_function_reference.execute(
        ...     [
        ...         UserDefinedFunctionArgument(name="id", datatype="INT", value=42),
        ...         UserDefinedFunctionArgument(name="tableName", datatype="TEXT", value="my_table_name"),
        ...     ]
        ... )
        """
        return self._execute(input_args, async_req=False)

    @api_telemetry
    def execute_async(self, input_args: Optional[list[UserDefinedFunctionArgument]] = None) -> PollingOperation[Any]:
        """An asynchronous version of :func:`execute`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        return self._execute(input_args=input_args, async_req=True)

    @overload
    def _execute(
        self, input_args: Optional[list[UserDefinedFunctionArgument]], async_req: Literal[True]
    ) -> PollingOperation[Any]: ...

    @overload
    def _execute(self, input_args: Optional[list[UserDefinedFunctionArgument]], async_req: Literal[False]) -> Any: ...

    def _execute(
        self, input_args: Optional[list[UserDefinedFunctionArgument]], async_req: bool
    ) -> Union[Any, PollingOperation[Any]]:
        user_defined_function = self.fetch()
        if isinstance(user_defined_function.return_type, ReturnTable):
            raise NotImplementedError("Executing User Defined Table Functions (UDTFs) is not supported.")

        if input_args is None:
            input_args = []

        for arg in user_defined_function.arguments:
            if arg.default_value is None:
                if not any(arg.name.upper() == input_arg.name.upper() for input_arg in input_args):
                    raise InvalidArgumentsError(f"Required argument '{arg.name}' not provided")

        result_or_future = self.collection._api.execute_user_defined_function(
            self.database.name,
            self.schema.name,
            user_defined_function.name,
            user_defined_function_argument=input_args,
            async_req=async_req,
        )

        def map_result(result: object) -> Any:
            if not isinstance(result, dict) or len(result.values()) != 1:
                raise InvalidResultError(f"User Defined Function result {str(result)} is invalid or empty")

            result = list(result.values())[0]
            return cast_result(result, str(user_defined_function.return_type.to_dict().get("datatype")))

        if isinstance(result_or_future, Future):
            return PollingOperation(result_or_future, map_result)
        return map_result(result_or_future)
