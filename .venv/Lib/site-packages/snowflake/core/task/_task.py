# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

import typing

from collections.abc import Iterable, Iterator
from concurrent.futures import Future
from datetime import datetime, timedelta
from logging import getLogger
from types import ModuleType
from typing import TYPE_CHECKING, Any, Callable, Literal, NamedTuple, Optional, Union, overload

import typing_extensions

from pydantic import StrictStr

from snowflake.core import PollingOperation
from snowflake.core._common import CreateMode, SchemaObjectCollectionParent, SchemaObjectReferenceMixin
from snowflake.core._generated.api_client import StoredProcApiClient
from snowflake.core._internal.telemetry import api_telemetry
from snowflake.core._internal.utils import StrEnum, deprecated
from snowflake.core._operation import PollingOperations
from snowflake.core._options import require_snowpark
from snowflake.core.task._generated import (
    CronSchedule,
    MinutesSchedule,
    SuccessResponse,
    TagAssignment,
    TagReference,
    TaskApi,
    TaskRun,
    TaskSchedule,
)
from snowflake.core.task._generated.models import Task as TaskModel

from .._options import snowpark
from .._utils import tag_assignment_to_tag_tuple, tag_resource_to_tag_reference, tag_tuple_to_tag_assignment
from ..tag import TagResource
from ..tag._tag import TagValue


if TYPE_CHECKING:
    from snowflake.core import Root
    from snowflake.core.schema._schema import SchemaResource
    from snowflake.snowpark.stored_procedure import StoredProcedure
    from snowflake.snowpark.types import DataType


_logger = getLogger(__name__)


class Cron(NamedTuple):
    """Specifies a cron expression and time zone for periodically running the task.

    Supports a subset of standard cron utility syntax.

    Examples
    ________

    >>> cron1 = Cron("0 0 10-20 * TUE,THU", "America/Los_Angeles")
    """

    expr: str
    """The cron expression. The minimum interval is 1 minute.

     It consists of the following fields:

    .. code-block::

        # __________ minute (0-59)
        # | ________ hour (0-23)
        # | | ______ day of month (1-31, or L)
        # | | | ____ month (1-12, JAN-DEC)
        # | | | | _ day of week (0-6, SUN-SAT, or L)
        # | | | | |
        # | | | | |
          * * * * *

    The following special characters are supported:

        - ``*`` Wildcard. Specifies any occurrence of the field.

        - ``L`` Stands for “last”. When used in the day-of-week field, it allows you to specify constructs such as
          “the last Friday” (“5L”) of a given month. In the day-of-month field, it specifies the last day of the month.

        - ``/n`` Indicates the nth instance of a given unit of time. Each quanta of time is computed independently.
          For example, if 4/3 is specified in the month field, then the task is scheduled for April, July and October
          (i.e. every 3 months, starting with the 4th month of the year). The same schedule is maintained in subsequent
          years. That is, the task is not scheduled to run in January (3 months after the October run).

    Tasks scheduled during specific times on days when the transition from standard time to daylight saving time
    (or the reverse) occurs can have unexpected behaviors. For example:

      - During the autumn change from daylight saving time to standard time, a task scheduled to start at 1 AM in
        the America/Los_Angeles time zone (i.e. 0 1 * * * America/Los_Angeles) would run twice: once at 1 AM and then
        again when 1:59:59 AM shifts to 1:00:00 AM local time. That is, there are two points in time when the local
        time is 1 AM.
      - During the spring change from standard time to daylight saving time, a task scheduled to start at 2 AM in
        the America/Los_Angeles time zone (i.e. 0 2 * * * America/Los_Angeles) would not run at all because the
        local time shifts from 1:59:59 AM to 3:00:00 AM. That is, there is no point during that day when the local
        time is 2 AM.

    To avoid unexpected task executions due to daylight saving time, use one of the following:
      - Do not schedule tasks to run at a specific time between 1 AM and 3 AM (daily, or on days of the week that
        include Sundays), or
      - Manually adjust the cron expression for tasks scheduled during those hours twice each year to compensate for
        the time change due to daylight saving time, or
      - Use a time format that does not apply daylight savings time, such as UTC.
    """
    timezone: str
    """The timezone for the cron expression.

    For a list of time zones, see the `list of tz database time zones in Wikipedia <https://en.wikipedia.org/wiki/List_of_tz_database_time_zones>`_.
    """

    def __eq__(self, other: "Cron") -> bool:  # type: ignore[override]
        return self.expr.lower() == other.expr.lower() and self.timezone.lower() == other.timezone.lower()


class OverlapPolicy(StrEnum):
    """Policy for whether multiple runs of a task graph may overlap.

    Controls whether a new run may start while a previous run is still executing.
    """

    NO_OVERLAP = "NO_OVERLAP"
    """Only one run at a time; the next run waits for the previous to complete."""
    ALLOW_CHILD_OVERLAP = "ALLOW_CHILD_OVERLAP"
    """Child tasks may overlap across runs; the root task does not overlap."""
    ALLOW_ALL_OVERLAP = "ALLOW_ALL_OVERLAP"
    """Runs may overlap; a new run may start regardless of in-progress runs."""

    def _equivalent_allow_overlapping_execution_value(self) -> bool | None:
        """Return the legacy ``allow_overlapping_execution`` flag equivalent to this policy.

        Returns
        -------
        bool or None
            ``False`` for ``NO_OVERLAP``, ``True`` for ``ALLOW_CHILD_OVERLAP``, and ``None``
            for ``ALLOW_ALL_OVERLAP`` (no single boolean equivalent).

        Raises
        ------
        ValueError
            If this member is not a known overlap policy (should not occur in normal use).
        """
        match self:
            case self.NO_OVERLAP:
                return False
            case self.ALLOW_CHILD_OVERLAP:
                return True
            case self.ALLOW_ALL_OVERLAP:
                return None
            case _:
                raise ValueError(f"Invalid overlap policy: {self}")

    @classmethod
    def _from_allow_overlapping_execution(
        cls, allow_overlapping_execution: bool | None
    ) -> typing_extensions.Self | None:
        """Build an overlap policy from the legacy ``allow_overlapping_execution`` flag.

        Parameters
        ----------
        allow_overlapping_execution : bool or None
            ``False`` maps to ``NO_OVERLAP``, ``True`` maps to ``ALLOW_CHILD_OVERLAP``, and
            ``None`` means unset and is returned as ``None``.

        Returns
        -------
        OverlapPolicy or None
            Matching policy, or ``None`` when ``allow_overlapping_execution`` is ``None``.
        """
        match allow_overlapping_execution:
            case False:
                return cls.NO_OVERLAP
            case True:
                return cls.ALLOW_CHILD_OVERLAP
            case None:
                return None

    @classmethod
    def _effective_value(cls, value: Optional[typing_extensions.Self]) -> typing_extensions.Self:
        """Return the effective overlap policy for this overlap policy."""
        return value if value is not None else cls.NO_OVERLAP


def _timedelta_to_minutes_schedule(value: timedelta) -> MinutesSchedule:
    """Convert a ``timedelta`` to a :class:`MinutesSchedule`.

    The total interval is split into whole minutes plus a ``seconds`` remainder in
    ``[0, 59]``. Sub-second precision is rejected.
    """
    total_seconds = value.total_seconds()
    if not total_seconds.is_integer():
        raise ValueError(f"The interval must be a whole number of seconds but got {total_seconds} second(s).")
    int_total_seconds = int(total_seconds)
    minutes, seconds = divmod(int_total_seconds, 60)
    return MinutesSchedule(minutes=minutes, seconds=seconds)


def _minutes_schedule_to_timedelta(schedule: MinutesSchedule) -> timedelta:
    return timedelta(minutes=schedule.minutes, seconds=schedule.seconds or 0)


def _to_model_schedule(schedule: Optional[Union[Cron, timedelta]]) -> Optional[TaskSchedule]:
    if schedule is None:
        return None
    if isinstance(schedule, Cron):
        return CronSchedule(cron_expr=schedule.expr, timezone=schedule.timezone)
    elif isinstance(schedule, timedelta):
        return _timedelta_to_minutes_schedule(schedule)
    raise TypeError("schedule should be either Cron or timedelta value")


def _to_model_target_completion_interval(target_completion_interval: Optional[timedelta]) -> Optional[MinutesSchedule]:
    if target_completion_interval is None:
        return None
    return _timedelta_to_minutes_schedule(target_completion_interval)


def _from_model_schedule(schedule: Optional[TaskSchedule]) -> Optional[Union[timedelta, Cron]]:
    if schedule is None:
        return None
    if isinstance(schedule, MinutesSchedule):
        return _minutes_schedule_to_timedelta(schedule)
    elif isinstance(schedule, CronSchedule):
        return Cron(schedule.cron_expr, schedule.timezone)
    raise TypeError("schedule must be either a MinutesSchedule or CronSchedule. ")  # won't happen in reality.


def _from_model_target_completion_interval(
    target_completion_interval: Optional[MinutesSchedule],
) -> Optional[timedelta]:
    if target_completion_interval is None:
        return None
    return _minutes_schedule_to_timedelta(target_completion_interval)


class StoredProcedureCall:
    """Represents a procedure call used as a task's ``definition``.

    Parameters
    __________
    func: Union[Callable[..., Any], StoredProcedure]
        When it's a ``Callable``, typically a function, an anonymous stored procedure will be created as
        the Task's definition by using this ``Callable``.
        Note that the first parameter of your function should be a snowpark Session.

        When it's a ``StoredProcedure``, it will be converted to a SQL to call an existing stored procedure.
        The ``StoredProcedure`` must be a permanent one instead of a temporary one because a Task will run
        in a different session than the session that creates the Task. A temporary one won't be accessible
        from that session that runs the Task.
    args: list[Any], optional
        The arguments to call the stored procedure when ``func`` is a ``StoredProcedure``.
    return_type: DataType, optional
        A :class:`~snowflake.snowpark.types.DataType` representing the return data
        type of the stored procedure. Optional if type hints are provided.
    input_types: list[DataType], optional
        A list of :class:`~snowflake.snowpark.types.DataType`
        representing the input data types of the stored procedure. Optional if
        type hints are provided.
    stage_location: str, optional
        The stage location where the Python file for the anonymous stored procedure
        and its dependencies should be uploaded.
        It must be a permanent location because a Task will run in a different session than the session that
        creates the Task. A temporary one won't be accessible from that session that runs the Task.
    imports: list[Union[str, Tuple[str, str]]], optional
        A list of imports that only apply to this stored procedure. You can use a string to
        represent a file path (similar to the ``path`` argument in
        :meth:`~snowflake.snowpark.Session.add_import`) in this list, or a tuple of two
        strings to represent a file path and an import path (similar to the ``import_path``
        argument in :meth:`~snowflake.snowpark.Session.add_import`). These stored procedure-level imports
        will override the session-level imports added by
        :meth:`~snowflake.snowpark.Session.add_import`.
    packages: list[Union[str, ModuleType]], optional
        A list of packages that only apply to this stored procedure.
        These stored procedure-level packages will override the session-level packages added by
        :meth:`~snowflake.snowpark.Session.add_packages` and
        :meth:`~snowflake.snowpark.Session.add_requirements`.
    """

    def __init__(
        self,
        func: Union[Callable[..., Any], "StoredProcedure"],
        *,
        args: Optional[list[Any]] = None,
        return_type: Optional["DataType"] = None,
        input_types: Optional[list["DataType"]] = None,
        stage_location: Optional[str] = None,
        imports: Optional[list[Union[str, tuple[str, str]]]] = None,
        packages: Optional[list[Union[str, ModuleType]]] = None,
    ) -> None:
        require_snowpark()
        from snowflake.snowpark.stored_procedure import StoredProcedure

        if (not isinstance(func, StoredProcedure)) and stage_location is None:
            raise ValueError(
                "stage_location has to be specified when func is a Python function. And it must NOT be a temp location."
            )
        self.func: Union[Callable[..., Any], StoredProcedure] = func
        self._args = args if args else []
        self._return_type = return_type
        self._input_types = input_types
        self._stage_location = stage_location
        self._imports = imports
        self._packages = packages
        self._sql: Optional[str] = None


class Task:
    """Represents a Snowflake Task."""

    def __init__(
        self,
        name: str,
        definition: Union[str, StoredProcedureCall],
        *,
        warehouse: Optional[str] = None,
        user_task_managed_initial_warehouse_size: Optional[str] = None,
        target_completion_interval: Optional[timedelta] = None,
        serverless_task_min_statement_size: Optional[str] = None,
        serverless_task_max_statement_size: Optional[str] = None,
        suspend_task_after_num_failures: Optional[int] = None,
        user_task_timeout_ms: Optional[int] = None,
        schedule: Optional[Union[Cron, timedelta]] = None,
        allow_overlapping_execution: Optional[bool] = None,
        error_integration: Optional[str] = None,
        success_integration: Optional[str] = None,
        overlap_policy: Optional[OverlapPolicy] = None,
        execute_as_user: Optional[str] = None,
        comment: Optional[str] = None,
        finalize: Optional[str] = None,
        task_auto_retry_attempts: Optional[int] = None,
        task_relations: Optional[str] = None,
        predecessors: Optional[list[str]] = None,
        condition: Optional[str] = None,
        config: Optional[dict[str, Any]] = None,
        session_parameters: Optional[dict[str, Any]] = None,
        id: Optional[str] = None,
        created_on: Optional[datetime] = None,
        last_committed_on: Optional[datetime] = None,
        last_suspended_on: Optional[datetime] = None,
        state: Optional[str] = None,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        owner: Optional[str] = None,
        owner_role_type: Optional[str] = None,
    ) -> None:
        self.name: str = name  #: Name of the task.
        self.definition: Union[str, StoredProcedureCall] = definition
        """Definition of the task.

        Any one of the following:
          - A SQL Statement. It can be a single SQL statement, or a procedural logic using Snowflake Scripting.
          - A :class:`StoredProcedureCall` instance. This provides a Pythonic way to call an existing stored procedure,
            or use a Snowflake annonymous stored procedure in Python.
        """
        #: The virtual warehouse that provides compute resources for task runs. Omit this parameter if you want to use
        #: the serverless Snowflake-managed compute resources for runs of this task.
        self.warehouse = warehouse
        #: The size of the compute resources to provision for the first run of the task, before a task history is
        #: available for Snowflake to determine an ideal size. Once a task has successfully completed a few runs,
        #: Snowflake ignores this parameter setting. Note that if the task history is unavailable for a given task, the
        #: compute resources revert to this initial size.
        #:
        #: A warehouse size is the same as in `creating a virtual warehouse
        #: <https://docs.snowflake.com/en/sql-reference/sql/create-warehouse>`_.
        #:
        #: If ``warehouse`` is specified for this task, then setting this parameter produces an error.
        self.user_task_managed_initial_warehouse_size = user_task_managed_initial_warehouse_size
        #: Specifies the desired task completion time. This parameter only applies to serverless tasks.
        #: This property is only set on a Task.
        self.target_completion_interval = target_completion_interval
        #: Specifies the minimum allowed warehouse size for the serverless task. This parameter only applies to
        #: serverless tasks. This parameter can be specified on the Task, Schema, Database, or Account.
        #: Precedence follows the standard parameter hierarchy
        self.serverless_task_min_statement_size = serverless_task_min_statement_size
        #: Specifies the maximum allowed warehouse size for the serverless task. This parameter only applies to
        #: serverless tasks. This parameter can be specified on the Task, Schema, Database, or Account.
        #: Precedence follows the standard parameter hierarchy
        self.serverless_task_max_statement_size = serverless_task_max_statement_size
        #: The schedule for periodically running the task. The minimum schedule is 1 minute.
        self.schedule = schedule
        #: Specifies the number of consecutive failed task runs after which the current task is suspended
        #: automatically. Failed task runs include runs in which task body either produces a user error or times
        #: out. Task runs that are skipped, canceled, or that fail due to a system error are considered indeterminate
        #: and are not included in the count of failed task runs.
        #:
        #: Set the parameter on a standalone task or the root task in a DAG. When the parameter is set to a value
        #: greater than 0, the following behavior applies to runs of the standalone task or DAG:
        #:
        #: Standalone tasks are automatically suspended after the specified number of consecutive task runs either fail
        #: or time out.
        #:
        #: The root task is automatically suspended after the run of any single task in a DAG fails or times out the
        #: specified number of times in consecutive runs.
        self.suspend_task_after_num_failures = suspend_task_after_num_failures
        #: Specifies the time limit on a single run of the task before it times out (in milliseconds).
        self.user_task_timeout_ms = user_task_timeout_ms

        # Switch used to determine whether to use the overlap_policy or allow_overlapping_execution
        # property when creating the task. Set within the property setters for these two properties.
        self._use_overlap_policy = True
        # Overlap policy is a newer field with 3 possible values used to specify the overlap policy which was previously
        # specified using the allow_overlapping_execution Boolean field.
        if overlap_policy is not None:
            if allow_overlapping_execution is not None and (
                (equivalent_value := overlap_policy._equivalent_allow_overlapping_execution_value()) is None
                or equivalent_value != allow_overlapping_execution
            ):
                raise ValueError(
                    "allow_overlapping_execution and overlap_policy are both set and inconsistent. "
                    f"overlap_policy={overlap_policy} maps to allow_overlapping_execution={equivalent_value} "
                    f"but allow_overlapping_execution={allow_overlapping_execution} was provided."
                )
            #: Overlap policy for the task graph. Only applicable to root tasks.
            self.overlap_policy = overlap_policy
        else:
            #: Whether to allow multiple instances of the DAG to run concurrently.
            self.allow_overlapping_execution = allow_overlapping_execution

        #: Specifies the name of the notification integration used to communicate with Amazon SNS, MS Azure Event Grid,
        #: or Google Pub/Sub.
        #:
        #: For more information, refer to `Enabling Error Notifications for Tasks
        #: <https://docs.snowflake.com/en/user-guide/tasks-errors.html>`_.
        #:
        #: Required only when configuring a task to send error notifications using Amazon Simple Notification Service
        #: (SNS), Microsoft Azure Event Grid, or Google Pub/Sub.
        self.error_integration = error_integration
        #: Specifies the name of the notification integration used for success notifications when the task graph
        #: completes successfully.
        self.success_integration = success_integration
        #: Specifies the name of the user whose privileges are used to run the task.
        self.execute_as_user = execute_as_user
        #: Specifies a comment for the task.
        self.comment = comment
        #: Specifies the finalizer task, use this to add a finalizer task for the DAG. For more info
        # https://docs.snowflake.com/en/user-guide/tasks-intro#finalizer-task
        self.finalize = finalize
        #: Specifies the number of automatic task graph retry attempts. If any task graphs complete in a FAILED state,
        #: Snowflake can automatically retry the task graphs from the last task in the graph that failed.
        #:
        #: The automatic task graph retry is disabled by default. To enable this feature, set TASK_AUTO_RETRY_ATTEMPTS
        #: to a value greater than 0 on the root task of a task graph.
        #:
        #: Note that this parameter must be set to the root task of a task graph. If it’s set to a child task, an error
        #: will be returned.
        self.task_auto_retry_attempts = task_auto_retry_attempts
        #: Specifies the relationship between different task in the graph
        self.task_relations = task_relations
        #: Specifies one or more predecessor tasks for the current task. Use this option to create a DAG of tasks or add
        #: this task to an existing DAG.
        #:
        #: `A DAG <https://docs.snowflake.com/en/user-guide/tasks-intro#label-task-dag>`_ is a series of tasks that
        #: starts with a scheduled root task and is linked together by dependencies.
        self.predecessors = predecessors
        #: Specifies a Boolean SQL expression; multiple conditions joined with AND/OR are supported. When a task is
        #: triggered (based on its SCHEDULE or AFTER setting), it validates the conditions of the expression to
        #: determine whether to execute. If the conditions of the expression are not met, then the task skips the
        #: current run. Any tasks that identify this task as a predecessor also do not run.
        #:
        #: SYSTEM$STREAM_HAS_DATA is the only function supported for evaluation in the SQL expression. This function
        #: indicates whether a specified stream contains change tracking data.
        self.condition: Optional[str] = condition
        self.config = config
        """Set the configuration for the task. It can only be set on a root task then it applies to all tasks
        in the DAG.
        The parameter can be set on standalone tasks but does not affect the task behavior.
        Snowflake ensures only one instance of a standalone task is running at a given time.
        """

        self.session_parameters = session_parameters
        """Set the session parameters for the task at runtime."""

        # read-only properties

        #: Unique identifier for each task. Note that recreating a task essentially creates a new task, which has a new
        #  ID.
        self.id = id
        #: Date and time when the task was created.
        self.created_on = created_on
        #: Timestamp when a version of the task was last set. If no version has been set (i.e. if the task has not been
        #: resumed or manually executed after it was created), the value is NULL.
        self.last_committed_on = last_committed_on
        #: Timestamp when the task was last suspended. If the task has not been suspended yet, the value is NULL.
        self.last_suspended_on = last_suspended_on
        #: `"started"` or `"suspended"` based on the current state of the task.
        self.state = state
        #: Database in which the task is stored.
        self.database_name = database_name
        #: Schema in which the task is stored.
        self.schema_name = schema_name
        #: Role that owns the task (i.e. has the OWNERSHIP privilege on the task)
        self.owner = owner
        #: The type of role that owns the object, either ROLE or DATABASE_ROLE. Note that Snowflake returns NULL if you
        #: delete the object because there is no owner role for a deleted object.
        self.owner_role_type = owner_role_type

    def __repr__(self) -> str:
        return repr(self._to_rest_model())

    @property
    def sql_definition(self) -> str:
        """The definition of the task in SQL text.

        It's a readonly property. To set the ``definition`` of the ``Task``, use :attr:`definition`.

        If :attr:`definition` is a :class:`StoredProcedureCall`, the SQL that calls the stored procedure, or the
        anonymous stored procedure definition will be returned.
        """
        if isinstance(self.definition, str):
            return self.definition
        else:
            if self.definition._sql:
                return self.definition._sql
            else:
                raise ValueError("definition of this task can only be retrieved after creating the task")

    @classmethod
    def _from_rest_model(cls, model: TaskModel) -> "Task":
        if model.overlap_policy is not None:
            overlap_policy = OverlapPolicy(model.overlap_policy)
            # Ignore the value of allow_overlapping_execution.
            allow_overlapping_execution = None
        else:
            overlap_policy = None
            allow_overlapping_execution = model.allow_overlapping_execution

        return Task(
            name=model.name,
            definition=model.definition,
            warehouse=model.warehouse,
            suspend_task_after_num_failures=model.suspend_task_after_num_failures,
            user_task_managed_initial_warehouse_size=model.user_task_managed_initial_warehouse_size,
            target_completion_interval=_from_model_target_completion_interval(model.target_completion_interval),
            serverless_task_min_statement_size=model.serverless_task_min_statement_size,
            serverless_task_max_statement_size=model.serverless_task_max_statement_size,
            user_task_timeout_ms=model.user_task_timeout_ms,
            schedule=_from_model_schedule(model.schedule),
            allow_overlapping_execution=allow_overlapping_execution,
            error_integration=model.error_integration,
            success_integration=model.success_integration,
            overlap_policy=overlap_policy,
            execute_as_user=model.execute_as_user,
            comment=model.comment,
            finalize=model.finalize,
            task_auto_retry_attempts=model.task_auto_retry_attempts,
            task_relations=model.task_relations,
            predecessors=model.predecessors,
            condition=model.condition,
            config=model.config,
            session_parameters=model.session_parameters,
            id=model.id,
            created_on=model.created_on,
            last_committed_on=model.last_committed_on,
            last_suspended_on=model.last_suspended_on,
            state=model.state,
            database_name=model.database_name,
            schema_name=model.schema_name,
            owner=model.owner,
            owner_role_type=model.owner_role_type,
        )

    def _to_rest_model(self) -> TaskModel:
        for prop in ("config", "session_parameters"):
            attr_value = getattr(self, prop, None)
            if attr_value:
                for k, v in attr_value.items():
                    if not isinstance(v, (str, int, float, bool)):
                        raise TypeError(
                            f"Task.{prop} is a dict. The value of this dict must be one of str, int, float, or bool."
                            f"Found value type {type(v)} for key {k}"
                        )

        # Avoid setting both overlap_policy and allow_overlapping_execution
        # to remain consistent with what the user set on this instance.
        if self._use_overlap_policy:
            overlap_policy = str(self.overlap_policy) if self.overlap_policy is not None else None
            allow_overlapping_execution = None
        else:
            overlap_policy = None
            allow_overlapping_execution = self.allow_overlapping_execution

        model = TaskModel(
            name=self.name,
            definition=self.sql_definition,
            warehouse=self.warehouse,
            user_task_managed_initial_warehouse_size=self.user_task_managed_initial_warehouse_size,
            target_completion_interval=_to_model_target_completion_interval(self.target_completion_interval),
            serverless_task_min_statement_size=self.serverless_task_min_statement_size,
            serverless_task_max_statement_size=self.serverless_task_max_statement_size,
            suspend_task_after_num_failures=self.suspend_task_after_num_failures,
            user_task_timeout_ms=self.user_task_timeout_ms,
            schedule=_to_model_schedule(self.schedule),
            allow_overlapping_execution=allow_overlapping_execution,
            error_integration=self.error_integration,
            success_integration=self.success_integration,
            overlap_policy=overlap_policy,
            execute_as_user=self.execute_as_user,
            comment=self.comment,
            finalize=self.finalize,
            task_auto_retry_attempts=self.task_auto_retry_attempts,
            task_relations=self.task_relations,
            predecessors=self.predecessors,
            condition=self.condition,
            config=self.config,
            session_parameters=self.session_parameters,
            database_name=self.database_name,
            schema_name=self.schema_name,
            owner=self.owner,
            owner_role_type=self.owner_role_type,
            id=self.id,
            state=self.state,
            created_on=self.created_on,
            last_committed_on=self.last_committed_on,
            last_suspended_on=self.last_suspended_on,
        )
        return model

    def to_dict(self, hide_readonly_properties: bool = False) -> dict[str, Any]:
        return self._to_rest_model().to_dict(hide_readonly_properties=hide_readonly_properties)

    def _extract_definition(self, root: "Root") -> None:
        definition = self.definition
        if not isinstance(definition, str):
            _register_task_definition_stored_procedure(definition, root)

    @property
    def allow_overlapping_execution(self) -> bool | None:
        """Legacy boolean view of whether overlapping runs are allowed.

        The configured :attr:`overlap_policy` (when present) is the source of truth for
        this property.

        Returns
        -------
        bool or None
            * ``False`` — overlapping runs are not allowed (``NO_OVERLAP``).
            * ``True`` — child tasks may overlap across runs (``ALLOW_CHILD_OVERLAP``).
            * ``None`` — unset, or overlap behavior has no legacy boolean equivalent
              (for example ``ALLOW_ALL_OVERLAP``), or :attr:`overlap_policy` was never set.
        """
        if (value := self.overlap_policy) is None:
            return None
        return value._equivalent_allow_overlapping_execution_value()

    @allow_overlapping_execution.setter
    def allow_overlapping_execution(self, value: bool | None) -> None:
        """Set overlap behavior using the legacy ``allow_overlapping_execution`` field.

        Parameters
        ----------
        value : bool or None
            * ``False`` — configure the task not to allow overlapping runs.
            * ``True`` — configure the task to allow overlapping runs of child tasks.
            * ``None`` — clear overlap configuration on this object.

        Notes
        -----
        ``True`` maps only to ``OverlapPolicy.ALLOW_CHILD_OVERLAP``. If the task was
        previously configured with ``OverlapPolicy.ALLOW_ALL_OVERLAP``, assigning
        ``True`` replaces that policy because it has no equivalent legacy boolean value.
        """
        self._use_overlap_policy = False
        self._overlap_policy = OverlapPolicy._from_allow_overlapping_execution(value)

    @property
    def overlap_policy(self) -> OverlapPolicy | None:
        """Snowflake overlap policy configured for this task.

        When you need a non-optional policy, use :attr:`effective_overlap_policy`, which
        treats an unset :attr:`overlap_policy` as ``OverlapPolicy.NO_OVERLAP`` on this object.

        Returns
        -------
        OverlapPolicy or None
            The configured policy, or ``None`` if unset on this object.
        """
        return self._overlap_policy

    @overlap_policy.setter
    def overlap_policy(self, value: OverlapPolicy | None) -> None:
        """Set the Snowflake overlap policy for this task.

        Parameters
        ----------
        value : OverlapPolicy or None
            Policy to store on this object, or ``None`` to clear :attr:`overlap_policy`.
        """
        self._use_overlap_policy = True
        self._overlap_policy = value

    @property
    def effective_overlap_policy(self) -> OverlapPolicy:
        """Effective overlap policy for this task after applying client-side defaults.

        Returns
        -------
        OverlapPolicy
            :attr:`overlap_policy` when set; otherwise ``OverlapPolicy.NO_OVERLAP``.
        """
        return OverlapPolicy._effective_value(self.overlap_policy)


class _FetchTaskDependentsParams(typing.TypedDict, total=False):
    recursive: bool
    """Whether to fetch the dependents recursively.

    If not provided, the server-side default is used.
    """


def _register_task_definition_stored_procedure(definition: StoredProcedureCall, root: "Root") -> None:
    require_snowpark()
    from snowflake.snowpark.stored_procedure import StoredProcedure

    if isinstance(definition.func, StoredProcedure):
        sproc_obj = definition.func
    else:
        imports = definition._imports if definition._imports else []
        sproc_obj = root.session.sproc.register(
            definition.func,
            name="task_handler_sp",
            return_type=definition._return_type,
            input_types=definition._input_types,
            stage_location=definition._stage_location,
            imports=imports,
            packages=definition._packages,
            anonymous=True,
            is_permanent=True,
        )
    sp_sql = snowpark._internal.udf_utils.generate_call_python_sp_sql(root.session, sproc_obj.name, *definition._args)
    if sproc_obj._anonymous_sp_sql:
        sp_sql = f"{sproc_obj._anonymous_sp_sql}{sp_sql}"
    definition._sql = sp_sql


class TaskResource(SchemaObjectReferenceMixin["TaskCollection"]):
    """Represents a reference to a Snowflake Task resource.

    With this task reference, you can fetch information about a task, as well as perform certain
    actions on it.
    """

    def __init__(self, name: str, collection: "TaskCollection") -> None:
        self.collection = collection
        self.name = name

    @api_telemetry
    @deprecated("create_or_alter")
    def create_or_update(self, task: Task) -> None:
        self.create_or_alter(task=task)

    @api_telemetry
    def create_or_alter(self, task: Task) -> None:
        """Create a task in Snowflake or alter one if it already exists.

        The Snowflake task's properties will be updated to the properties of the input ``task`` if the task already
        exists.  Note that the full picture of a task is expected. If a property isn't set a value in the input
        ``task``, the property will be set to ``NULL`` in Snowflake too because it's regarded as the expected value.

        Parameters
        __________
        task: Task
            An instance of :class:`Task`.

        Examples
        ________
        >>> task_parameters = Task(name="your-task-name", definition="select 1")

        # Using a ``TaskCollection`` to create a reference to task in Snowflake server:

        >>> root.warehouses["your-task-name"].create_or_alter(task_parameters)
        """
        self._create_or_alter(task=task, async_req=False)

    @api_telemetry
    def create_or_alter_async(self, task: Task) -> PollingOperation[None]:
        """An asynchronous version of :func:`create_or_alter`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self._create_or_alter(task=task, async_req=True)
        return PollingOperations.empty(future)

    @api_telemetry
    @deprecated("drop")
    def delete(self) -> None:
        """Delete this task."""
        self.drop()

    @api_telemetry
    def drop(self, if_exists: Optional[bool] = None) -> None:
        """Drop this task.

        Parameters
        __________
        if_exists: bool, optional
            Check the existence of this task before dropping it.
            Default is ``None``, which is equivalent to ``False``.

        Examples
        ________
        Deleting a task using its reference:

        >>> task_reference.drop()
        """
        self.collection._api.delete_task(
            self.database.name, self.schema.name, self.name, if_exists=if_exists, async_req=False
        )

    @api_telemetry
    def drop_async(self, if_exists: Optional[bool] = None) -> PollingOperation[None]:
        """An asynchronous version of :func:`drop`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self.collection._api.delete_task(
            self.database.name, self.schema.name, self.name, if_exists=if_exists, async_req=True
        )
        return PollingOperations.empty(future)

    @api_telemetry
    def fetch(self) -> Task:
        """Fetch the task resource.

        Examples
        ________
        Fetching a task using its reference:

        >>> task = task_reference.fetch()

        Accessing information of the task with task instance:

        >>> print(task.name, task.comment)
        """
        rest_model = self.collection._api.fetch_task(self.database.name, self.schema.name, self.name, async_req=False)
        return Task._from_rest_model(rest_model)

    @api_telemetry
    def fetch_async(self) -> PollingOperation[Task]:
        """An asynchronous version of :func:`fetch`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self.collection._api.fetch_task(self.database.name, self.schema.name, self.name, async_req=True)
        return PollingOperation(future, lambda rest_model: Task._from_rest_model(rest_model))

    @api_telemetry
    def execute(self, *, retry_last: bool = False) -> None:
        """Execute the task immediately without waiting for the schedule.

        Parameters
        __________
        retry_last: bool, optional
            Re-execute the last failed task of the DAG. Default is ``False``.

        Examples
        ________
        Execute a task using its reference:

        >>> task_reference.execute()
        """
        self.collection._api.execute_task(
            self.database.name, self.schema.name, self.name, retry_last=retry_last, async_req=False
        )

    @api_telemetry
    def execute_async(self, *, retry_last: bool = False) -> PollingOperation[None]:
        """An asynchronous version of :func:`execute`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self.collection._api.execute_task(
            self.database.name, self.schema.name, self.name, retry_last=retry_last, async_req=True
        )
        return PollingOperations.empty(future)

    @api_telemetry
    def resume(self) -> None:
        """Resume the task then it will run on the schedule.

        Examples
        ________
        Resume a task using its reference:

        >>> task_reference.resume()
        """
        self.collection._api.resume_task(self.database.name, self.schema.name, self.name, async_req=False)

    @api_telemetry
    def resume_async(self) -> PollingOperation[None]:
        """An asynchronous version of :func:`resume`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self.collection._api.resume_task(self.database.name, self.schema.name, self.name, async_req=True)
        return PollingOperations.empty(future)

    @api_telemetry
    def suspend(self) -> None:
        """Suspend the task so it won't run again on the schedule.

        Examples
        ________
        Suspend a task using its reference:

        >>> task_reference.suspend()
        """
        self.collection._api.suspend_task(self.database.name, self.schema.name, self.name, async_req=False)

    @api_telemetry
    def suspend_async(self) -> PollingOperation[None]:
        """An asynchronous version of :func:`suspend`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self.collection._api.suspend_task(self.database.name, self.schema.name, self.name, async_req=True)
        return PollingOperations.empty(future)

    @api_telemetry
    def fetch_task_dependents(self, **kwargs: typing_extensions.Unpack[_FetchTaskDependentsParams]) -> list[Task]:
        """Return the list of child tasks that use this task as the root in a DAG.

        Parameters
        ----------
        recursive: bool
            Whether to fetch the dependents recursively. If not provided, the server-side default is used.

        Examples
        ________
        Fetching the child tasks of a task using its reference:

        >>> child_tasks = task_reference.fetch_task_dependents()
        """
        return [
            Task._from_rest_model(x)
            for x in self.collection._api.fetch_task_dependents(
                self.database.name, self.schema.name, self.name, async_req=False, **kwargs
            )
        ]

    @api_telemetry
    def fetch_task_dependents_async(
        self, **kwargs: typing_extensions.Unpack[_FetchTaskDependentsParams]
    ) -> PollingOperation[list[Task]]:
        """An asynchronous version of :func:`fetch_task_dependents`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.

        Parameters
        ----------
        recursive: bool
            Whether to fetch the dependents recursively. If not provided, the server-side default is used.
        """  # noqa: D401
        future = self.collection._api.fetch_task_dependents(
            self.database.name, self.schema.name, self.name, async_req=True, **kwargs
        )
        return PollingOperation(future, lambda rest_models: [Task._from_rest_model(x) for x in rest_models])

    @api_telemetry
    def get_complete_graphs(self, *, error_only: bool = True) -> Iterable[TaskRun]:
        """Return the status of a completed graph run.

        It returns details for runs that executed successfully, failed, or were cancelled in the past 60 minutes.

        To retrieve the details for graph runs that are currently executing, or are next scheduled to run within the
        next 8 days, use :meth:`get_current_graphs`.

        Parameters
        __________
        error_only: bool, optional
            Return only the graph runs that have failed. Default is ``True``.

        Examples
        ________
        Getting the completed graph runs of a task using its reference:

        >>> completed_graphs = task_reference.get_complete_graphs()
        """
        return self.collection._api.get_complete_graphs(
            self.database.name, self.schema.name, self.name, error_only=error_only, async_req=False
        )

    @api_telemetry
    def get_complete_graphs_async(self, *, error_only: bool = True) -> PollingOperation[Iterable[TaskRun]]:
        """An asynchronous version of :func:`get_complete_graphs`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self.collection._api.get_complete_graphs(
            self.database.name, self.schema.name, self.name, error_only=error_only, async_req=True
        )
        return PollingOperations.identity(future)

    @api_telemetry
    def get_current_graphs(self) -> Iterable[TaskRun]:
        """Return the status of a graph run that is currently scheduled or is executing.

        It returns details for graph runs that are currently executing or are next scheduled to run within the next 8
        days.  To retrieve the details for graph runs that have completed in the past 60 minutes, use
        :meth:`get_complete_graphs`.

        Examples
        ________
        Getting the current graph runs of a task using its reference:

        >>> current_graphs = task_reference.get_current_graphs()
        """
        return self.collection._api.get_current_graphs(self.database.name, self.schema.name, self.name, async_req=False)

    @api_telemetry
    def get_current_graphs_async(self) -> PollingOperation[Iterable[TaskRun]]:
        """An asynchronous version of :func:`get_current_graphs`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self.collection._api.get_current_graphs(
            self.database.name, self.schema.name, self.name, async_req=True
        )
        return PollingOperations.identity(future)

    @api_telemetry
    def set_tags(self, tags: dict[TagResource, TagValue], if_exists: Optional[bool] = None) -> None:
        """Set tags on a task.

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
        tag_assignments = [
            tag_tuple_to_tag_assignment(TagAssignment, tag_resource, tag_value)
            for [tag_resource, tag_value] in tags.items()
        ]

        self.collection._api.set_tags(
            database=self.database.name,
            var_schema=self.schema.name,
            name=self.name,
            tag_assignment=tag_assignments,
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
        tag_assignments = [
            tag_tuple_to_tag_assignment(TagAssignment, tag_resource, tag_value)
            for [tag_resource, tag_value] in tags.items()
        ]

        future = self.collection._api.set_tags(
            database=self.database.name,
            var_schema=self.schema.name,
            name=self.name,
            tag_assignment=tag_assignments,
            if_exists=if_exists,
            async_req=True,
        )
        return PollingOperations.empty(future)

    @api_telemetry
    def unset_tags(self, tag_resources: set[TagResource], if_exists: Optional[bool] = None) -> None:
        """Unset tags from a task.

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
        tag_reference = [tag_resource_to_tag_reference(TagReference, tag_resource) for tag_resource in tag_resources]

        self.collection._api.unset_tags(
            database=self.database.name,
            var_schema=self.schema.name,
            name=self.name,
            tag_reference=tag_reference,
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
        tag_reference = [tag_resource_to_tag_reference(TagReference, tag_resource) for tag_resource in tag_resources]

        future = self.collection._api.unset_tags(
            database=self.database.name,
            var_schema=self.schema.name,
            name=self.name,
            tag_reference=tag_reference,
            if_exists=if_exists,
            async_req=True,
        )
        return PollingOperations.empty(future)

    @api_telemetry
    def get_tags(self, with_lineage: Optional[bool] = None) -> dict[TagResource, TagValue]:
        """Get the tag assignments for a task.

        Returns all tags assigned to a task. This operation requires an active warehouse.

        Parameters
        __________
        with_lineage: bool, optional
            Parameter that specifies whether tag assignments inherited by the object from its ancestors in securable
            object hierarchy should be returned as well: - `true`: All tags assigned to this object should be returned,
            inheritance included. - `false`: Only tags explicitly assigned to this object should be returned.
        """
        tag_assignments = self.collection._api.get_tags(
            database=self.database.name,
            var_schema=self.schema.name,
            name=self.name,
            with_lineage=with_lineage,
        )

        get_tags_dict = dict(tag_assignment_to_tag_tuple(ta, self.root) for ta in tag_assignments)
        return get_tags_dict

    @api_telemetry
    def get_tags_async(self, with_lineage: Optional[bool] = None) -> PollingOperation[dict[TagResource, TagValue]]:
        """An asynchronous version of :func:`get_tags`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self.collection._api.get_tags(
            database=self.database.name,
            var_schema=self.schema.name,
            name=self.name,
            with_lineage=with_lineage,
            async_req=True,
        )
        return PollingOperation(
            future, lambda tag_assignments: dict(tag_assignment_to_tag_tuple(ta, self.root) for ta in tag_assignments)
        )

    @overload
    def _create_or_alter(self, task: Task, async_req: Literal[True]) -> Future[SuccessResponse]: ...

    @overload
    def _create_or_alter(self, task: Task, async_req: Literal[False]) -> SuccessResponse: ...

    def _create_or_alter(self, task: Task, async_req: bool) -> Union[SuccessResponse, Future[SuccessResponse]]:
        self.collection._extract_definition(task)
        task_model = task._to_rest_model()
        return self.collection._api.create_or_alter_task(
            database=self.database.name,
            var_schema=self.schema.name,
            task=task_model,
            name=task.name,
            async_req=async_req,
        )


class TaskCollection(SchemaObjectCollectionParent[TaskResource]):
    """Represents the collection operations of the Snowflake Task resource.

    With this collection, you can create, iterate through, and search for task that you have access to
    in the current context.

    Examples
    ________
    >>> task_collection = root.databases["mydb"].schemas["myschema"].tasks
    >>> task = Task(name="mytask", definition="select 1")
    >>> task_collection.create(task)
    """

    def __init__(self, schema: "SchemaResource") -> None:
        super().__init__(schema, TaskResource)
        self._api = TaskApi(
            root=self.root, resource_class=self._ref_class, sproc_client=StoredProcApiClient(root=self.root)
        )

    @api_telemetry
    def create(self, task: Task, *, mode: CreateMode = CreateMode.error_if_exists) -> TaskResource:
        """Create a task in Snowflake.

        Parameters
        __________
        task: an instance of :class:`Task`.
        mode: CreateMode, optional
            One of the following strings.

            ``CreateMode.error_if_exists``: Throw an :class:`snowflake.core.exceptions.ConflictError`
            if the task already exists in Snowflake. Equivalent to SQL ``create task <name> ...``.

            ``CreateMode.or_replace``: Replace if the task already exists in Snowflake. Equivalent to SQL
            ``create or replace task <name> ...``.

            ``CreateMode.if_not_exists``: Do nothing if the task already exists in Snowflake.
            Equivalent to SQL ``create task <name> if not exists...``

            Default value is ``CreateMode.error_if_exists``.

        Examples
        ________
        Creating a task in Snowflake and getting a reference to it:

        >>> task_parameters = Task(name="mytask", definition="select 1")
        >>> # Use the task collection created before to create a reference to the task resource
        >>> # in Snowflake.
        >>> task_reference = task_collection.create(task_parameters)
        """
        self._extract_definition(task)
        task_model = task._to_rest_model()
        real_mode = CreateMode[mode].value
        self._api.create_task(self.database.name, self.schema.name, task_model, real_mode, async_req=False)
        return self[task.name]

    @api_telemetry
    def create_async(
        self, task: Task, *, mode: CreateMode = CreateMode.error_if_exists
    ) -> PollingOperation[TaskResource]:
        """An asynchronous version of :func:`create`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        self._extract_definition(task)
        task_model = task._to_rest_model()
        real_mode = CreateMode[mode].value
        future = self._api.create_task(self.database.name, self.schema.name, task_model, real_mode, async_req=True)
        return PollingOperation(future, lambda _: self[task.name])

    @api_telemetry
    def iter(
        self,
        *,
        like: Optional[str] = None,
        starts_with: Optional[str] = None,
        limit: Optional[int] = None,
        from_name: Optional[str] = None,
        root_only: bool = False,
    ) -> Iterator[Task]:
        """Iterate through ``Task`` objects in Snowflake, filtering on any optional ``like`` pattern.

        Parameters
        __________
        like: str, optional
            A case-insensitive string functioning as a filter, with support for SQL
            wildcard characters (% and _).
        starts_with: str, optional
            String used to filter the command output based on the string of characters t
        limit: int, optional
            Limit of the maximum number of rows returned by iter(). The default is ``None``, which behaves equivalently
            to show_limit=10000. This value must be between ``1`` and ``10000``.
        from_name: str, optional
            Fetch rows only following the first row whose object name matches
            the specified string. This is case-sensitive and does not have to be the full name.
        root_only: bool, optional
            Look for root tasks only. Default is ``False``.

        Examples
        ________
        Showing all tasks that you have access to see:

        >>> tasks = task_collection.iter()

        Showing information of the exact task you want to see:

        >>> tasks = task_collection.iter(like="your-task-name")

        Showing tasks starting with 'your-task-name-':

        >>> tasks = task_collection.iter(like="your-task-name-%")

        Using a for loop to retrieve information from iterator:

        >>> for task in tasks:
        ...     print(task.name, task.comment)
        """
        tasks = self._api.list_tasks(
            self.database.name,
            self.schema.name,
            root_only,
            StrictStr(like) if like is not None else None,
            StrictStr(starts_with) if starts_with else None,
            show_limit=limit,
            from_name=from_name,
            async_req=False,
        )

        return map(Task._from_rest_model, iter(tasks))

    @api_telemetry
    def iter_async(
        self,
        *,
        like: Optional[str] = None,
        starts_with: Optional[str] = None,
        limit: Optional[int] = None,
        from_name: Optional[str] = None,
        root_only: bool = False,
    ) -> PollingOperation[Iterator[Task]]:
        """An asynchronous version of :func:`iter`.

        Refer to :class:`~snowflake.core.PollingOperation` for more information on asynchronous execution and
        the return type.
        """  # noqa: D401
        future = self._api.list_tasks(
            self.database.name,
            self.schema.name,
            root_only,
            StrictStr(like) if like is not None else None,
            StrictStr(starts_with) if starts_with else None,
            show_limit=limit,
            from_name=from_name,
            async_req=True,
        )
        return PollingOperation(future, lambda tasks: map(Task._from_rest_model, iter(tasks)))

    def _extract_definition(self, task: Task) -> None:
        task._extract_definition(self.root)
