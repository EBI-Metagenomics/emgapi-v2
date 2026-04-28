import functools
import inspect
from functools import partial
from typing import (
    Any,
    Callable,
    Coroutine,
    Optional,
    ParamSpec,
    TypeVar,
    cast,
    overload,
)

from django.db import connections
from prefect.flows import Flow
from prefect.tasks import Task

P = ParamSpec("P")
R = TypeVar("R")


def close_stale_connections() -> None:
    """
    Close Django DB connections that are unusable or past their max age,
    skipping any connection that is currently inside an atomic block.

    Connections inside an atomic block are skipped because closing them would
    corrupt the transaction, and because Django's AUTOCOMMIT setting (True by
    default) mismatches the live autocommit state (False inside atomic()),
    causing close_if_unusable_or_obsolete() to treat it as a state change and
    close the connection. Transactions should never span multiple tasks or
    flows, so in normal operation this guard only triggers in pytest-django,
    which wraps each test in transaction.atomic() to allow rollback.
    """
    for conn in connections.all():
        if not conn.in_atomic_block:
            conn.close_if_unusable_or_obsolete()


@overload
def _refresh_connections_before_call(
    fn: Callable[P, R],
) -> Callable[P, R]: ...


@overload
def _refresh_connections_before_call(
    fn: Callable[P, Coroutine[Any, Any, R]],
) -> Callable[P, Coroutine[Any, Any, R]]: ...


def _refresh_connections_before_call(fn: Callable[P, Any]) -> Callable[P, Any]:
    if inspect.iscoroutinefunction(fn):
        async_fn = cast(Callable[P, Coroutine[Any, Any, Any]], fn)

        # This repository does not currently define async tasks or flows, so this
        # branch hasn't been tested with real workflows. The unit tests cover it, and
        # we expect it to behave correctly if async workflows are introduced later.
        @functools.wraps(fn)
        async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
            close_stale_connections()
            return await async_fn(*args, **kwargs)

        return async_wrapper

    sync_fn = cast(Callable[P, Any], fn)

    @functools.wraps(fn)
    def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
        close_stale_connections()
        return sync_fn(*args, **kwargs)

    return sync_wrapper


@overload
def django_db_flow(_fn: Callable[P, R]) -> Flow[P, R]: ...


@overload
def django_db_flow(
    _fn: Callable[P, Coroutine[Any, Any, R]],
) -> Flow[P, Coroutine[Any, Any, R]]: ...


@overload
def django_db_flow(
    _fn: None = None, **flow_kwargs: Any
) -> Callable[[Callable[P, R]], Flow[P, R]]: ...


def django_db_flow(
    _fn: Optional[Callable[P, Any]] = None, **flow_kwargs: Any
) -> Flow[P, Any] | Callable[[Callable[P, Any]], Flow[P, Any]]:
    """
    Drop-in replacement for prefect ``@flow`` that automatically closes stale Django DB
    connections before the flow body runs.

    Usage is identical to ``@flow`` — just change the import::

        from workflows.prefect_utils.flows_utils import django_db_flow

        @django_db_flow(name="my flow", log_prints=True)
        def my_flow():
            ...

    :param flow_kwargs: Keyword arguments forwarded verbatim to ``prefect.flow``.
    :return: Decorator that produces a Prefect Flow with connection refresh.
    """
    if _fn is not None:
        return Flow(fn=_refresh_connections_before_call(_fn), **flow_kwargs)

    return cast(
        Callable[[Callable[P, Any]], Flow[P, Any]],
        partial(django_db_flow, **flow_kwargs),
    )


@overload
def django_db_task(_fn: Callable[P, R]) -> Task[P, R]: ...


@overload
def django_db_task(
    _fn: Callable[P, Coroutine[Any, Any, R]],
) -> Task[P, Coroutine[Any, Any, R]]: ...


@overload
def django_db_task(
    _fn: None = None, **task_kwargs: Any
) -> Callable[[Callable[P, R]], Task[P, R]]: ...


def django_db_task(
    _fn: Optional[Callable[P, Any]] = None, **task_kwargs: Any
) -> Task[P, Any] | Callable[[Callable[P, Any]], Task[P, Any]]:
    """
    Drop-in replacement for prefect ``@task`` that automatically closes stale Django DB
    connections before the task body runs.

    Usage is identical to ``@task`` — just change the import::

        from workflows.prefect_utils.flows_utils import django_db_task

        @django_db_task(name="my task", retries=2)
        def my_task():
            ...

    :param task_kwargs: Keyword arguments forwarded verbatim to ``prefect.task``.
    :return: Decorator that produces a Prefect Task with connection refresh.
    """
    if _fn is not None:
        return Task(fn=_refresh_connections_before_call(_fn), **task_kwargs)

    return cast(
        Callable[[Callable[P, Any]], Task[P, Any]],
        partial(django_db_task, **task_kwargs),
    )
