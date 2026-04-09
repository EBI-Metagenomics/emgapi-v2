import functools
from typing import Callable

import prefect
from django.db import connections


def _close_stale_connections() -> None:
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


def django_flow(**flow_kwargs) -> Callable:
    """
    Drop-in replacement for prefect ``@flow`` that automatically closes stale Django DB
    connections before the flow body runs.

    Usage is identical to ``@flow`` — just change the import::

        from workflows.prefect_utils.flows_utils import django_flow

        @django_flow(name="my flow", log_prints=True)
        def my_flow():
            ...

    :param flow_kwargs: Keyword arguments forwarded verbatim to ``prefect.flow``.
    :return: Decorator that produces a Prefect Flow with connection refresh.
    """
    prefect_decorator = prefect.flow(**flow_kwargs)

    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            _close_stale_connections()
            return fn(*args, **kwargs)

        return prefect_decorator(wrapper)

    return decorator


def django_task(**task_kwargs) -> Callable:
    """
    Drop-in replacement for prefect ``@task`` that automatically closes stale Django DB
    connections before the task body runs.

    Usage is identical to ``@task`` — just change the import::

        from workflows.prefect_utils.flows_utils import django_task

        @django_task(name="my task", retries=2)
        def my_task():
            ...

    :param task_kwargs: Keyword arguments forwarded verbatim to ``prefect.task``.
    :return: Decorator that produces a Prefect Task with connection refresh.
    """
    prefect_decorator = prefect.task(**task_kwargs)

    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            _close_stale_connections()
            return fn(*args, **kwargs)

        return prefect_decorator(wrapper)

    return decorator
