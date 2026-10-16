from collections.abc import Iterator
from concurrent.futures import Future
from functools import cached_property
from typing import TYPE_CHECKING, Literal, Optional, Union, overload

from pydantic import StrictStr

from snowflake.connector import SnowflakeConnection
from snowflake.core import PollingOperation
from snowflake.core._generated.api_client import StoredProcApiClient
from snowflake.core._operation import PollingOperations
from snowflake.core.pipe import PipeCollection
from snowflake.core.schema._generated import SuccessResponse, TagAssignment, TagReference

from .._common import Clone, CreateMode, DatabaseObjectCollectionParent, DatabaseObjectReferenceMixin, PointOfTime
from .._internal.telemetry import api_telemetry
from .._internal.utils import deprecated
from .._utils import tag_assignment_to_tag_tuple, tag_resource_to_tag_reference, tag_tuple_to_tag_assignment
from ..alert import AlertCollection
from ..artifact_repository import ArtifactRepositoryCollection
from ..code_bundle import CodeBundleCollection
from ..cortex.search_service import CortexSearchServiceCollection
from ..dynamic_table import DynamicTableCollection
from ..event_table import EventTableCollection
from ..exceptions import NotFoundError
from ..function import FunctionCollection
from ..iceberg_table import IcebergTableCollection
from ..image_repository import ImageRepositoryCollection
from ..network_rule import NetworkRuleCollection
from ..notebook import NotebookCollection
from ..password_policy import PasswordPolicyCollection
from ..procedure import ProcedureCollection
from ..secret import SecretCollection
from ..sequence import SequenceCollection
from ..service import ServiceCollection
from ..stage import StageCollection
from ..stream import StreamCollection
from ..streamlit import StreamlitCollection
from ..table import TableCollection
from ..tag import TagCollection, TagResource, TagValue
from ..task import TaskCollection
from ..user_defined_function import UserDefinedFunctionCollection
from ..view import ViewCollection
from ._generated.api.schema_api import SchemaApi
from ._generated.models.point_of_time import PointOfTime as SchemaPointOfTime
from ._generated.models.schema import SchemaModel as Schema
from ._generated.models.schema_clone import SchemaClone


if TYPE_CHECKING:
    from .. import Root
    from ..database import DatabaseResource


class SchemaCollection(DatabaseObjectCollectionParent["SchemaResource"]):
    """Represents the collection operations on the Snowflake schema resource.

    With this collection, you can create, iterate through, and search for schemas that you have access to in the
    current context.

    Examples
    ________
    Creating a schema instance:

    >>> schemas = root.databases["my_db"].schemas
    >>> new_schema = Schema("my_schema")
    >>> schemas.create(new_schema)
    """

    def __init__(self, database: "DatabaseResource", root: "Root") -> None:
        super().__init__(database, SchemaResource)
        self._api = SchemaApi(
            root=root, resource_class=self._ref_class, sproc_client=StoredProcApiClient(root=self.root)
        )

    @api_telemetry
    def create(
        self,
        schema: Schema,
        *,
        clone: Optional[Union[str, Clone]] = None,
        mode: CreateMode = CreateMode.error_if_exists,
    ) -> "SchemaResource":
        """Create a schema in Snowflake.

        Parameters
        __________
        schema: SchemaResource
            The ``Schema`` object, together with the ``Schema``'s properties: name; kind, comment, managed_access,
            retention_time, budget, data_retention_time_in_days, default_ddl_colaltion, log_level,
            pipe_execution_paused, max_data_extension_time_in_days, suspend_task_after_num_failures, trace_level,
            user_task_managed_initial_warehouse_size, user_task_timeout_ms, serverless_task_min_statement_size
            and serverless_task_max_statement_size are optional.
        clone: str, or Clone, optional
            Whether to clone an existing schema. An instance of :class:`Clone`, or str of the name, ``None``
            if no cloning is necessary.
        mode: CreateMode, optional
            One of the following enum values.

            ``CreateMode.error_if_exists``: Throw an :class:`snowflake.core.exceptions.ConflictError`
            if the schema already exists in Snowflake.  Equivalent to SQL ``create schema <name> ...``.

            ``CreateMode.or_replace``: Replace if the schema already exists in Snowflake. Equivalent to SQL
            ``create or replace schema <name> ...``.

            ``CreateMode.if_not_exists``: Do nothing if the schema already exists in Snowflake.
            Equivalent to SQL ``create schema <name> if not exists...``

            Default is ``CreateMode.error_if_exists``.

        Examples
        ________
        Creating a new schema called ``new_schema`` in ``my_db``:

        >>> schemas = root.databases["my_db"].schemas
        >>> new_schema_ref = schemas.create(Schema("new_schema"))

        Creating a new schema called ``new_schema`` in ``my_db`` by cloning an existing schema:

        >>> schemas = root.databases["my_db"].schemas
        >>> new_schema_ref = schemas.create(
        ...     "new_schema",
        ...     clone=Clone(
        ...         source="original_schema", point_of_time=PointOfTimeOffset(reference="at", when="-5")
        ...     ),
        ...     mode=CreateMode.or_replace,
        ... )

        Creating a new schema called ``new_schema`` in ``my_db`` by cloning an existing schema in another database:

        >>> schemas = root.databases["my_db"].schemas
        >>> new_schema_ref = schemas.create(
        ...     "new_schema",
        ...     clone=Clone(
        ...         source="another_database.original_schema",
        ...         point_of_time=PointOfTimeOffset(reference="at", when="-5"),
        ...     ),
        ...     mode=CreateMode.or_replace,
        ... )
        """
        self._create(schema=schema, clone=clone, mode=mode, async_req=False)
        return self[schema.name]

    @api_telemetry
    def create_async(
        self,
        schema: Schema,
        *,
        clone: Optional[Union[str, Clone]] = None,
        mode: CreateMode = CreateMode.error_if_exists,
    ) -> PollingOperation["SchemaResource"]:
        """An asynchronous version of :func:`create`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self._create(schema=schema, clone=clone, mode=mode, async_req=True)
        return PollingOperation(future, lambda _: self[schema.name])

    @api_telemetry
    def iter(
        self,
        *,
        like: Optional[str] = None,
        starts_with: Optional[str] = None,
        limit: Optional[int] = None,
        from_name: Optional[str] = None,
    ) -> Iterator[Schema]:
        """Iterate through ``Schema`` objects from Snowflake, filtering on any optional 'like' pattern.

        Parameters
        __________
        like: str, optional
            A case-insensitive string functioning as a filter, with support for SQL
            wildcard characters (``%`` and ``_``).
        starts_with: str, optional
            String used to filter the command output based on the string of characters that appear
            at the beginning of the object name. Uses case-sensitive pattern matching.
        limit: int, optional
            Limit of the maximum number of rows returned by iter(). The default is ``None``, which behaves equivalently
            to show_limit=10000. This value must be between ``1`` and ``10000``.
        from_name: str, optional
            Fetch rows only following the first row whose object name matches
            the specified string. This is case-sensitive and does not have to be the full name.

        Examples
        ________
        Showing all schemas that you have access to see:

        >>> schemas = db_ref.schemas.iter()

        Showing information of the exact schema you want to see:

        >>> schemas = db_ref.schemas.iter(like="your-schema-name")

        Showing schemas starting with 'your-schema-name-':

        >>> schemas = db_ref.schemas.iter(like="your-schema-name-%")

        Using a for loop to retrieve information from iterator:

        >>> for schema in schemas:
        >>>     print(schema.name, schema.query)
        """
        schemas = self._api.list_schemas(
            self.database.name,
            StrictStr(like) if like is not None else None,
            StrictStr(starts_with) if starts_with else None,
            limit,
            from_name=from_name,
            async_req=False,
        )

        return map(Schema._from_model, iter(schemas))

    @api_telemetry
    def iter_async(
        self,
        *,
        like: Optional[str] = None,
        starts_with: Optional[str] = None,
        limit: Optional[int] = None,
        from_name: Optional[str] = None,
    ) -> PollingOperation[Iterator[Schema]]:
        """An asynchronous version of :func:`iter`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self._api.list_schemas(
            self.database.name,
            StrictStr(like) if like is not None else None,
            StrictStr(starts_with) if starts_with else None,
            limit,
            from_name=from_name,
            async_req=True,
        )
        return PollingOperation(future, lambda rest_models: map(Schema._from_model, iter(rest_models)))

    @property
    def _connection(self) -> SnowflakeConnection:
        return self.database._connection

    @overload
    def _create(
        self, schema: Schema, clone: Optional[Union[str, Clone]], mode: CreateMode, async_req: Literal[True]
    ) -> Future[SuccessResponse]: ...

    @overload
    def _create(
        self, schema: Schema, clone: Optional[Union[str, Clone]], mode: CreateMode, async_req: Literal[False]
    ) -> SuccessResponse: ...

    def _create(
        self, schema: Schema, clone: Optional[Union[str, Clone]], mode: CreateMode, async_req: bool
    ) -> Union[SuccessResponse, Future[SuccessResponse]]:
        real_mode = CreateMode[mode].value
        model = schema._to_model()

        if clone is not None:
            pot: Optional[SchemaPointOfTime] = None
            if isinstance(clone, Clone) and isinstance(clone.point_of_time, PointOfTime):
                pot = SchemaPointOfTime.from_dict(clone.point_of_time.to_dict())
            real_clone = Clone(source=clone) if isinstance(clone, str) else clone
            req = SchemaClone(point_of_time=pot, **model.to_dict())

            source_schema_path = real_clone.source.split(".")
            source_database = self.database.name
            source_schema = source_schema_path[-1]
            if len(source_schema_path) == 2:
                source_database = source_schema_path[0]
            elif len(source_schema_path) != 1:
                raise NotFoundError(reason=f"{real_clone.source} does not exists", root=self.root)

            return self._api.clone_schema(
                database=source_database,
                name=source_schema,
                schema_clone=req,
                create_mode=StrictStr(real_mode),
                async_req=async_req,
                target_database=self.database.name,
            )

        return self._api.create_schema(
            database=self.database.name, var_schema=model, create_mode=StrictStr(real_mode), async_req=async_req
        )


class SchemaResource(DatabaseObjectReferenceMixin[SchemaCollection]):
    """Represents a reference to a Snowflake schema.

    With this schema reference, you can fetch information about a schema, as well as perform
    certain actions on it.
    """

    def __init__(self, name: str, collection: SchemaCollection) -> None:
        self.name: str = name
        self.collection: SchemaCollection = collection

    @property
    def _api(self) -> SchemaApi:
        return self.collection._api

    @api_telemetry
    @deprecated("create_or_alter")
    def create_or_update(self, schema: Schema) -> "SchemaResource":
        """Create, or update-in-place a schema in Snowflake.

        Parameters
        __________
        schema: SchemaResource
            An instance of :class:`Schema`, the definition of schema we should create.

        Examples
        ________
        Create a schema from a reference:

        >>> my_db.schemas["my_new_schema"].create_or_update(Schema("my_new_schema"))
        """
        return self.create_or_alter(schema=schema)

    @api_telemetry
    def create_or_alter(self, schema: Schema) -> "SchemaResource":
        """Create, or alter-in-place a schema in Snowflake.

        Parameters
        __________
        schema: SchemaResource
            An instance of :class:`Schema`, the definition of schema we should create.

        Examples
        ________
        Create a schema from a reference:

        >>> my_db.schemas["my_new_schema"].create_or_alter(Schema("my_new_schema"))
        """
        self._api.create_or_alter_schema(self.database.name, schema.name, schema._to_model(), async_req=False)
        return self

    @api_telemetry
    def create_or_alter_async(self, schema: Schema) -> PollingOperation["SchemaResource"]:
        """An asynchronous version of :func:`create_or_alter`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self._api.create_or_alter_schema(self.database.name, schema.name, schema._to_model(), async_req=True)
        return PollingOperation(future, lambda _: self)

    @api_telemetry
    def fetch(self) -> Schema:
        """Fetch the details of a schema.

        Examples
        ________
        Fetching a reference to a schema to print its name and whether it's our current one.

        >>> my_schema = my_db.schemas["my_schema"].fetch()
        >>> print(my_schema.name, my_schema.is_current)
        """
        return Schema._from_model(self.collection._api.fetch_schema(self.database.name, self.name, async_req=False))

    @api_telemetry
    def fetch_async(self) -> PollingOperation[Schema]:
        """An asynchronous version of :func:`fetch`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self.collection._api.fetch_schema(self.database.name, self.name, async_req=True)
        return PollingOperation(future, lambda rest_model: Schema._from_model(rest_model))

    @api_telemetry
    @deprecated("drop")
    def delete(self, if_exists: Optional[bool] = None) -> None:
        """Delete this schema.

        Parameters
        __________
        if_exists: bool, optional
            Check the existence of this schema before dropping it.
            Default is ``None``, which is equivalent to ``False``.

        Examples
        ________
        Deleting a schema using its reference, erroring if it doesn't exist:

        >>> schema_reference.delete()

        Deleting a schema using its reference, if it exists:

        >>> schema_reference.delete(if_exists=True)
        """
        self.drop(if_exists=if_exists)

    @api_telemetry
    def drop(self, if_exists: Optional[bool] = None) -> None:
        """Drop this schema.

        Parameters
        __________
        if_exists: bool, optional
            Check the existence of this schema before dropping it.
            Default is ``None``, which is equivalent to ``False``.

        Examples
        ________
        Dropping a schema using its reference, erroring if it doesn't exist:

        >>> schema_reference.drop()

        Dropping a schema using its reference, if it exists:

        >>> schema_reference.drop(if_exists=True)
        """
        self.collection._api.delete_schema(self.database.name, name=self.name, if_exists=if_exists, async_req=False)

    @api_telemetry
    def drop_async(self, if_exists: Optional[bool] = None) -> PollingOperation[None]:
        """An asynchronous version of :func:`drop`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self.collection._api.delete_schema(
            self.database.name, name=self.name, if_exists=if_exists, async_req=True
        )
        return PollingOperations.empty(future)

    @api_telemetry
    def undrop(self) -> None:
        """Undrop this previously dropped schema if it hasn't been purged yet.

        Examples
        ________
        Undropping a schema using its reference:

        >>> schema_reference.drop()
        >>> schema_reference.undrop()
        """
        self.collection._api.undrop_schema(self.database.name, name=self.name, async_req=False)

    @api_telemetry
    def undrop_async(self) -> PollingOperation[None]:
        """An asynchronous version of :func:`undrop`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self.collection._api.undrop_schema(self.database.name, name=self.name, async_req=True)
        return PollingOperations.empty(future)

    @api_telemetry
    def set_tags(self, tags: dict[TagResource, TagValue], if_exists: Optional[bool] = None) -> None:
        """Set tags on a schema.

        Parameters
        __________
        tags: dict[TagResource, TagValue]
             (required)
        if_exists: bool
             Parameter that specifies how to handle the request for a resource that does not exist: - `true`:
             The endpoint does not throw an error if the resource does not exist. It returns a 200 success response,
             but does not take any action on the resource. - `false`: The endpoint throws an error if the resource
             doesn't exist.
        """
        self.collection._api.set_tags(
            database=self.database.name,
            name=self.name,
            tag_assignment=[
                tag_tuple_to_tag_assignment(TagAssignment, tag_resource, tag_value)
                for [tag_resource, tag_value] in tags.items()
            ],
            if_exists=if_exists,
        )

    @api_telemetry
    def set_tags_async(
        self, tags: dict[TagResource, TagValue], if_exists: Optional[bool] = None
    ) -> PollingOperation[None]:
        """An asynchronous version of :func:`set_tags`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self.collection._api.set_tags(
            database=self.database.name,
            name=self.name,
            tag_assignment=[
                tag_tuple_to_tag_assignment(TagAssignment, tag_resource, tag_value)
                for [tag_resource, tag_value] in tags.items()
            ],
            if_exists=if_exists,
            async_req=True,
        )
        return PollingOperations.empty(future)

    @api_telemetry
    def unset_tags(self, tag_resources: set[TagResource], if_exists: Optional[bool] = None) -> None:
        """Unset tags from a schema.

        Parameters
        __________
        tag_resources: set[TagResource]
             (required)
        if_exists: bool
             Parameter that specifies how to handle the request for a resource that does not exist: - `true`:
             The endpoint does not throw an error if the resource does not exist. It returns a 200 success response,
             but does not take any action on the resource. - `false`: The endpoint throws an error if the resource
             doesn't exist.
        """
        self.collection._api.unset_tags(
            database=self.database.name,
            name=self.name,
            tag_reference=[tag_resource_to_tag_reference(TagReference, tag_resource) for tag_resource in tag_resources],
            if_exists=if_exists,
        )

    @api_telemetry
    def unset_tags_async(
        self, tag_resources: set[TagResource], if_exists: Optional[bool] = None
    ) -> PollingOperation[None]:
        """An asynchronous version of :func:`unset_tags`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self.collection._api.unset_tags(
            database=self.database.name,
            name=self.name,
            tag_reference=[tag_resource_to_tag_reference(TagReference, tag_resource) for tag_resource in tag_resources],
            if_exists=if_exists,
            async_req=True,
        )
        return PollingOperations.empty(future)

    @api_telemetry
    def get_tags(self, with_lineage: Optional[bool] = None) -> dict[TagResource, TagValue]:
        """Get the tag assignments for a schema.

        Returns all tags assigned to a schema. This operation requires an active warehouse.

        Parameters
        __________
        with_lineage: bool, optional
            Parameter that specifies whether tag assignments inherited by the object from its ancestors in securable
            object hierarchy should be returned as well: - `true`: All tags assigned to this object should be returned,
            inheritance included. - `false`: Only tags explicitly assigned to this object should be returned.
        """
        tag_assignments = self.collection._api.get_tags(
            database=self.database.name,
            name=self.name,
            with_lineage=with_lineage,
        )
        return dict(tag_assignment_to_tag_tuple(ta, self.root) for ta in tag_assignments)

    @api_telemetry
    def get_tags_async(self, with_lineage: Optional[bool] = None) -> PollingOperation[dict[TagResource, TagValue]]:
        """An asynchronous version of :func:`get_tags`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self.collection._api.get_tags(
            database=self.database.name,
            name=self.name,
            with_lineage=with_lineage,
            async_req=True,
        )
        return PollingOperation(
            future, lambda tag_assignments: dict(tag_assignment_to_tag_tuple(ta, self.root) for ta in tag_assignments)
        )

    @cached_property
    def alerts(self) -> AlertCollection:
        """The AlertCollection of all alerts contained in this schema.

        Examples
        ________
        Getting all alerts in ``my_schema``:

        >>> my_db.schemas["my_schema"].alerts
        """
        return AlertCollection(self)

    @cached_property
    def event_tables(self) -> EventTableCollection:
        """The EventCollection of all events contained in this schema.

        Examples
        ________
        Getting all events in ``my_schema``:

        >>> my_db.schemas["my_schema"].events
        """
        return EventTableCollection(self)

    @cached_property
    def tasks(self) -> TaskCollection:
        """The TaskCollection of all tasks contained in this schema.

        Examples
        ________
        Getting all tasks in ``my_schema``:

        >>> my_db.schemas["my_schema"].tasks
        """
        return TaskCollection(self)

    @cached_property
    def pipes(self) -> PipeCollection:
        """The PipesCollection of all pipes in this schema.

        Examples
        ________
        Getting all pipes in ``my_schema``:

        >>> my_db.schemas["my_schema"].pipes
        """
        return PipeCollection(self)

    @cached_property
    def services(self) -> ServiceCollection:
        """The ServiceCollection of all services contained in this schema.

        Examples
        ________
        Getting all services in ``my_schema``:

        >>> my_db.schemas["my_schema"].services
        """
        return ServiceCollection(self)

    @cached_property
    def notebooks(self) -> NotebookCollection:
        """The NotebookCollection of all notebooks contained in this schema.

        Examples
        ________
        Getting all notebooks in ``my_schema``:

        >>> my_db.schemas["my_schema"].notebooks
        """
        return NotebookCollection(self)

    @cached_property
    def streamlits(self) -> StreamlitCollection:
        """The StreamlitCollection of all Streamlits contained in this schema.

        Examples
        ________
        Getting all streamlits in ``my_schema``:

        >>> my_db.schemas["my_schema"].streamlits
        """
        return StreamlitCollection(self)

    @cached_property
    def artifact_repositories(self) -> ArtifactRepositoryCollection:
        """The ArtifactRepositoryCollection of all artifact repositories in this schema.

        Examples
        ________
        Getting all artifact repositories in ``my_schema``:

        >>> my_db.schemas["my_schema"].artifact_repositories
        """
        return ArtifactRepositoryCollection(self)

    @cached_property
    def image_repositories(self) -> ImageRepositoryCollection:
        """The ImageRepositoryCollection of all image repositories in this schema.

        Examples
        ________
        Getting all image repositories in ``my_schema``:

        >>> my_db.schemas["my_schema"].image_repositories
        """
        return ImageRepositoryCollection(self)

    @cached_property
    def tables(self) -> TableCollection:
        """The TableCollection of all tables contained in this schema.

        Examples
        ________
        Getting all tables in ``my_schema``:

        >>> my_db.schemas["my_schema"].tables
        """
        return TableCollection(self)

    @cached_property
    def dynamic_tables(self) -> DynamicTableCollection:
        """The DynamicTableCollection of all dynamic tables contained in this schema.

        Examples
        ________
        Getting all dynamic tables in ``my_schema``:

        >>> my_db.schemas["my_schema"].dynamic_tables
        """
        return DynamicTableCollection(self)

    @cached_property
    def iceberg_tables(self) -> IcebergTableCollection:
        """The IcebergTableCollection of all dynamic tables contained in this schema.

        Examples
        ________
        Getting all Iceberg tables in ``my_schema``:

        >>> my_db.schemas["my_schema"].iceberg_tables
        """
        return IcebergTableCollection(self)

    @cached_property
    def code_bundles(self) -> CodeBundleCollection:
        """The CodeBundleCollection of all code bundles contained in this schema.

        Examples
        ________
        Getting all code bundles in ``my_schema``:

        >>> my_db.schemas["my_schema"].code_bundles
        """
        return CodeBundleCollection(self)

    @cached_property
    def stages(self) -> StageCollection:
        """The StageCollection of all stages contained in this schema.

        Examples
        ________
        Getting all stages in ``my_schema``:

        >>> my_db.schemas["my_schema"].stages
        """
        return StageCollection(self)

    @cached_property
    def streams(self) -> StreamCollection:
        """The StreamCollection of all streams contained in this schema.

        Examples
        ________
        Getting all streams in ``my_schema``:

        >>> my_db.schemas["my_schema"].streams
        """
        return StreamCollection(self)

    @cached_property
    def cortex_search_services(self) -> CortexSearchServiceCollection:
        """The CortexSearchServiceCollection of all cortex services contained in this schema.

        Examples
        ________
        Getting all cortex search services in ``my_schema``:

        >>> my_db.schemas["my_schema"].cortex_search_service
        """
        return CortexSearchServiceCollection(self)

    @cached_property
    def functions(self) -> FunctionCollection:
        """The FunctionCollection of all functions contained in this schema.

        Examples
        ________
        Getting all functions in ``my_schema``:

        >>> my_db.schemas["my_schema"].functions
        """
        return FunctionCollection(self)

    @cached_property
    def procedures(self) -> ProcedureCollection:
        """Returns the ``ProcedureCollection`` that represents the visible procedures.

        Examples
        ________
        Getting a specific procedure resource:

        >>> root = Root(session)
        >>> procedure = root.databases["my_db"].schemas["my_schema"].procedures["my_procedure"]
        """
        return ProcedureCollection(self)

    @cached_property
    def views(self) -> ViewCollection:
        """The ViewCollection of all views contained in this schema.

        Examples
        ________
        Getting all views in ``my_schema``:

        >>> my_db.schemas["my_schema"].views
        """
        return ViewCollection(self)

    @cached_property
    def user_defined_functions(self) -> UserDefinedFunctionCollection:
        """The UserDefinedFunctionCollection of all user defined functions contained in this schema.

        Examples
        ________
        Get all user defined functions in ``my_schema``:

        >>> my_db.schemas["my_schema"].user_defined_functions
        """
        return UserDefinedFunctionCollection(self)

    @cached_property
    def network_rules(self) -> NetworkRuleCollection:
        """The NetworkRuleCollection of all network rules contained in this schema.

        Examples
        ________
        Get all network rules in ``my_schema``:

        >>> my_db.schemas["my_schema"].network_rules
        """
        return NetworkRuleCollection(self)

    @cached_property
    def password_policies(self) -> PasswordPolicyCollection:
        """The PasswordPolicyCollection of all password policies contained in this schema.

        Examples
        ________
        Get all password policies in ``my_schema``:

        >>> my_db.schemas["my_schema"].password_policies
        """
        return PasswordPolicyCollection(self)

    @cached_property
    def secrets(self) -> SecretCollection:
        """The SecretCollection of all secrets contained in this schema.

        Examples
        ________
        Get all secrets in ``my_schema``:

        >>> my_db.schemas["my_schema"].secrets
        """
        return SecretCollection(self)

    @cached_property
    def sequences(self) -> SequenceCollection:
        """The SequenceCollection of all sequences contained in this schema.

        Examples
        ________
        Get all sequences in ``my_schema``:

        >>> my_db.schemas["my_schema"].sequences
        """
        return SequenceCollection(self)

    @cached_property
    def tags(self) -> TagCollection:
        """The TagCollection of all tags contained in this schema.

        Examples
        ________
        Get all tags in ``my_schema``:

        >>> my_db.schemas["my_schema"].tags
        """
        return TagCollection(self)
