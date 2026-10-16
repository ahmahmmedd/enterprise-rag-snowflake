from collections.abc import Iterator
from typing import TYPE_CHECKING, Optional

from pydantic import StrictStr

from snowflake.core import PollingOperation
from snowflake.core._common import AccountObjectCollectionParent, CreateMode, ObjectReferenceMixin
from snowflake.core._generated.api_client import StoredProcApiClient
from snowflake.core._internal.telemetry import api_telemetry
from snowflake.core._internal.utils import deprecated
from snowflake.core._operation import PollingOperations
from snowflake.core._utils import (
    tag_assignment_to_tag_tuple,
    tag_resource_to_tag_reference,
    tag_tuple_to_tag_assignment,
)
from snowflake.core.tag import TagResource, TagValue
from snowflake.core.warehouse._generated import TagAssignment, TagReference
from snowflake.core.warehouse._generated.api import WarehouseApi
from snowflake.core.warehouse._generated.models.warehouse import WarehouseModel as Warehouse


if TYPE_CHECKING:
    from snowflake.core import Root


class WarehouseCollection(AccountObjectCollectionParent["WarehouseResource"]):
    """Represents the collection operations of the Snowflake Warehouse resource.

    With this collection, you can create, iterate through, and search for warehouses that you have access to
    in the current context.

    Examples
    ________
    Creating a warehouse instance:

    >>> warehouses = root.warehouses
    >>> new_wh = Warehouse(name="my_wh", warehouse_size="XSMALL")
    >>> warehouses.create(new_wh)
    """

    def __init__(self, root: "Root") -> None:
        super().__init__(root, ref_class=WarehouseResource)
        self._api = WarehouseApi(
            root=self.root, resource_class=self._ref_class, sproc_client=StoredProcApiClient(root=self.root)
        )

    @api_telemetry
    def create(self, warehouse: Warehouse, *, mode: CreateMode = CreateMode.error_if_exists) -> "WarehouseResource":
        """Create a warehouse in Snowflake.

        Parameters
        __________
        warehouse: Warehouse
        mode: CreateMode, optional
            One of the below enum values.

            ``CreateMode.error_if_exists``: Throw an :class:`snowflake.core.exceptions.ConflictError` if the
            warehouse already exists in Snowflake. Equivalent to SQL ``create warehouse <name> ...``.

            ``CreateMode.or_replace``: Replace if the warehouse already exists in Snowflake. Equivalent to SQL
            ``create or replace warehouse <name> ...``.

            ``CreateMode.if_not_exists``: Do nothing if the warehouse already exists in Snowflake. Equivalent to SQL
            ``create warehouse <name> if not exists...``

            Default value is ``CreateMode.error_if_exists``.

        Examples
        ________
        Creating a warehouse in Snowflake and getting the reference to it:

        >>> warehouse_parameters = Warehouse(
        ...     name="your-warehouse-name",
        ...     warehouse_size="SMALL",
        ...     auto_suspend=500,
        ...)

        >>> # Use the warehouse collection created before to create a reference to a warehouse resource
        >>> # in Snowflake.
        >>> warehouse_reference = warehouse_collection.create(warehouse_parameters)
        """
        real_mode = CreateMode[mode].value
        self._api.create_warehouse(warehouse._to_model(), StrictStr(real_mode), async_req=False)
        return self[warehouse.name]

    @api_telemetry
    def create_async(
        self, warehouse: Warehouse, *, mode: CreateMode = CreateMode.error_if_exists
    ) -> PollingOperation["WarehouseResource"]:
        """An asynchronous version of :func:`create`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        real_mode = CreateMode[mode].value
        future = self._api.create_warehouse(warehouse._to_model(), StrictStr(real_mode), async_req=True)
        return PollingOperation(future, lambda _: self[warehouse.name])

    @api_telemetry
    def iter(self, *, like: Optional[str] = None) -> Iterator[Warehouse]:
        """Iterate through ``Warehouse`` objects in Snowflake, filtering on any optional `like` pattern.

        Parameters
        __________
        like: str, optional
            A case-insensitive string functioning as a filter, with support for SQL
            wildcard characters (% and _).

        Examples
        ________
        Showing all warehouses that you have access to see:

        >>> warehouses = warehouse_collection.iter()

        Showing information of the exact warehouse you want to see:

        >>> warehouses = warehouse_collection.iter(like="your-warehouse-name")

        Showing warehouses starting with 'your-warehouse-name-':

        >>> warehouses = warehouse_collection.iter(like="your-warehouse-name-%")

        Using a for loop to retrieve information from iterator:

        >>> for warehouse in warehouses:
        >>>     print(warehouse.name, warehouse.warehouse_size)
        """
        warehouses = self._api.list_warehouses(StrictStr(like) if like is not None else None, async_req=False)

        return map(Warehouse._from_model, iter(warehouses))

    @api_telemetry
    def iter_async(self, *, like: Optional[str] = None) -> PollingOperation[Iterator[Warehouse]]:
        """An asynchronous version of :func:`iter`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self._api.list_warehouses(StrictStr(like) if like is not None else None, async_req=True)
        return PollingOperation(future, lambda rest_models: map(Warehouse._from_model, iter(rest_models)))


class WarehouseResource(ObjectReferenceMixin[WarehouseCollection]):
    """Represents a reference to a Snowflake warehouse.

    With this warehouse reference, you can fetch information about a warehouse, as well as perform
    certain actions on it.
    """

    def __init__(self, name: str, collection: WarehouseCollection):
        self.name = name
        self.collection = collection

    @api_telemetry
    @deprecated("create_or_alter")
    def create_or_update(self, warehouse: Warehouse) -> None:
        self.create_or_alter(warehouse=warehouse)

    @api_telemetry
    def create_or_alter(self, warehouse: Warehouse) -> None:
        """Create a warehouse in Snowflake or alter one if it already exists.

        Parameters
        __________
        warehouse: Warehouse
            An instance of :class:`Warehouse`.

        Examples
        ________
        Creating or updating a warehouse in Snowflake:

        >>> warehouse_parameters = Warehouse(
        ...     name="your-warehouse-name",
        ...     warehouse_size="SMALL",
        ...     auto_suspend=500,
        ...)

        # Using a ``WarehouseCollection`` to create or update a warehouse in Snowflake:
        >>> root.warehouses["your-warehouse-name"].create_or_alter(warehouse_parameters)
        """
        self.collection._api.create_or_alter_warehouse(warehouse.name, warehouse._to_model(), async_req=False)

    @api_telemetry
    def create_or_alter_async(self, warehouse: Warehouse) -> PollingOperation[None]:
        """An asynchronous version of :func:`create_or_alter`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self.collection._api.create_or_alter_warehouse(warehouse.name, warehouse._to_model(), async_req=True)
        return PollingOperations.empty(future)

    @api_telemetry
    def suspend(self, if_exists: Optional[bool] = None) -> None:
        """Suspend the warehouse.

        Parameters
        __________
        if_exists: bool, optional
            Check the existence of this warehouse before suspending it.
            Default is ``None``, which is equivalent to ``False``.

        Examples
        ________
        Using a warehouse reference to suspend a warehouse, erroring if it does not exist:

        >>> warehouse_reference.suspend()

        Using a warehouse reference to suspend a warehouse if it exists:

        >>> warehouse_reference.suspend(if_exists=True)
        """
        self.collection._api.suspend_warehouse(self.name, if_exists=if_exists, async_req=False)

    @api_telemetry
    def suspend_async(self, if_exists: Optional[bool] = None) -> PollingOperation[None]:
        """An asynchronous version of :func:`suspend`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self.collection._api.suspend_warehouse(self.name, if_exists=if_exists, async_req=True)
        return PollingOperations.empty(future)

    @api_telemetry
    def resume(self, if_exists: Optional[bool] = None) -> None:
        """Resume the warehouse.

        Parameters
        __________
        if_exists: bool, optional
            If set to True, the function will not throw an error if the warehouse does not exist.
            The default is ``None``, which is equivalent to False.

        Examples
        ________
        Using a warehouse reference to resume a warehouse, erroring if it does not exist:

        >>> warehouse_reference.resume()

        Using a warehouse reference to resume a warehouse if it exists:

        >>> warehouse_reference.resume(if_exists=True)
        """
        self.collection._api.resume_warehouse(self.name, if_exists=if_exists, async_req=False)

    @api_telemetry
    def resume_async(self, if_exists: Optional[bool] = None) -> PollingOperation[None]:
        """An asynchronous version of :func:`resume`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self.collection._api.resume_warehouse(self.name, if_exists=if_exists, async_req=True)
        return PollingOperations.empty(future)

    @api_telemetry
    @deprecated("drop")
    def delete(self, if_exists: Optional[bool] = None) -> None:
        self.drop(if_exists=if_exists)

    @api_telemetry
    def drop(self, if_exists: Optional[bool] = None) -> None:
        """Drop this warehouse.

        Parameters
        __________
        if_exists: bool, optional
            If set to True, the function will not throw an error if the warehouse does not exist.
            The default is ``None``, which is equivalent to False.

        Examples
        ________
        Dropping a warehouse using its reference, erroring if it does not exist:

        >>> warehouse_reference.drop()

        Dropping a warehouse using its reference, if it exists:

        >>> warehouse_reference.drop(if_exists=True)
        """
        self.collection._api.delete_warehouse(self.name, if_exists=if_exists, async_req=False)

    @api_telemetry
    def drop_async(self, if_exists: Optional[bool] = None) -> PollingOperation[None]:
        """An asynchronous version of :func:`drop`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self.collection._api.delete_warehouse(self.name, if_exists=if_exists, async_req=True)
        return PollingOperations.empty(future)

    @api_telemetry
    def fetch(self) -> Warehouse:
        """Fetch the details of a warehouse.

        Examples
        ________
        Fetching a warehouse using its reference:

        >>> warehouse = warehouse_reference.fetch()

        # Accessing information of the warehouse with warehouse instance.
        >>> print(warehouse.name, warehouse.warehouse_size)
        """
        return Warehouse._from_model(self.collection._api.fetch_warehouse(self.name, async_req=False))

    @api_telemetry
    def fetch_async(self) -> PollingOperation[Warehouse]:
        """An asynchronous version of :func:`fetch`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self.collection._api.fetch_warehouse(self.name, async_req=True)
        return PollingOperation(future, lambda rest_model: Warehouse._from_model(rest_model))

    @api_telemetry
    def rename(self, new_name: str, if_exists: Optional[bool] = None) -> None:
        """Rename this warehouse.

        This function will ignore other parameters in warehouse instance; use `create_or_update()` to update parameters.

        Parameters
        __________
        new_name: Warehouse
            An instance of :class:`Warehouse`.
        if_exists: bool, optional
            If set to ``True``, the function will not throw an error if the warehouse does not exist.
            The default is ``None``, which is equivalent to ``False``.

        Examples
        ________
        Using a warehouse reference to rename a warehouse in Snowflake, erroring if it does not exist:
        >>>  warehouse_reference.rename("new_wh_name")

        Using a warehouse reference to rename a warehouse in Snowflake, if it exists:
        >>>  warehouse_reference.rename("new_wh_name", if_exists=True)
        """
        self.collection._api.rename_warehouse(
            self.name, Warehouse(new_name)._to_model(), if_exists=if_exists, async_req=False
        )

    @api_telemetry
    def rename_async(self, new_name: str, if_exists: Optional[bool] = None) -> PollingOperation[None]:
        """An asynchronous version of :func:`rename`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self.collection._api.rename_warehouse(
            self.name, Warehouse(new_name)._to_model(), if_exists=if_exists, async_req=True
        )
        return PollingOperations.empty(future)

    @api_telemetry
    def abort_all_queries(self, if_exists: Optional[bool] = None) -> None:
        """Abort all queries running or queueing on this warehouse.

        Parameters
        __________
        if_exists: bool, optional
            If set to True, the function will not throw an error if the warehouse does not exist.
            The default is ``None``, which is equivalent to False.

        Examples
        ________
        Using a warehouse reference to abort all queries, erroring if it does not exist:

        >>> warehouse = warehouse_reference.abort_all_queries()

        Using a warehouse reference to abort all queries, if it exists:
        >>> warehouse = warehouse_reference.abort_all_queries(if_exists=True)
        """
        self.collection._api.abort_all_queries_on_warehouse(self.name, if_exists=if_exists, async_req=False)

    @api_telemetry
    def abort_all_queries_async(self, if_exists: Optional[bool] = None) -> PollingOperation[None]:
        """An asynchronous version of :func:`abort_all_queries`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self.collection._api.abort_all_queries_on_warehouse(self.name, if_exists=if_exists, async_req=True)
        return PollingOperations.empty(future)

    @api_telemetry
    def use_warehouse(self) -> None:
        """Use this warehouse as the current warehouse.

        Examples
        ________
        Using a warehouse reference to set the current active warehouse:

        >>> warehouse_reference.use_warehouse()
        """
        self.collection._api.use_warehouse(self.name, async_req=False)

    @api_telemetry
    def use_warehouse_async(self) -> PollingOperation[None]:
        """An asynchronous version of :func:`use_warehouse`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self.collection._api.use_warehouse(self.name, async_req=True)
        return PollingOperations.empty(future)

    @api_telemetry
    def set_tags(self, tags: dict[TagResource, TagValue], if_exists: Optional[bool] = None) -> None:
        """Set tags on a warehouse.

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
        """Unset tags from a warehouse.

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
            name=self.name,
            tag_reference=[tag_resource_to_tag_reference(TagReference, tag_resource) for tag_resource in tag_resources],
            if_exists=if_exists,
            async_req=True,
        )
        return PollingOperations.empty(future)

    @api_telemetry
    def get_tags(self, with_lineage: Optional[bool] = None) -> dict[TagResource, TagValue]:
        """Get the tag assignments for a warehouse.

        Returns all tags assigned to a warehouse. This operation requires an active warehouse.

        Parameters
        __________
        with_lineage: bool, optional
            Parameter that specifies whether tag assignments inherited by the object from its ancestors in securable
            object hierarchy should be returned as well: - `true`: All tags assigned to this object should be returned,
            inheritance included. - `false`: Only tags explicitly assigned to this object should be returned.
        """
        tag_assignments = self.collection._api.get_tags(
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
        future = self.collection._api.get_tags(name=self.name, with_lineage=with_lineage, async_req=True)
        return PollingOperation(
            future, lambda tag_assignments: dict(tag_assignment_to_tag_tuple(ta, self.root) for ta in tag_assignments)
        )


__all__ = ["Warehouse", "WarehouseCollection", "WarehouseResource"]
