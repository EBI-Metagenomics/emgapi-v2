import time
from unittest.mock import MagicMock, patch

import pytest
from django.db import connection, OperationalError
from prefect import task
from prefect.flows import Flow
from prefect.tasks import Task

from ena.models import Study
from workflows.prefect_utils.flows_utils import (
    close_stale_connections,
    django_db_flow,
    django_db_task,
)


@django_db_task()
def count_ena_studies() -> int:
    return Study.objects.count()


@task
def count_ena_studies_prefect() -> int:
    return Study.objects.count()


@django_db_task
def add_one(value: int) -> int:
    return value + 1


@django_db_task(name="add two with kwargs")
def add_two(value: int) -> int:
    return value + 2


@django_db_flow
def bare_django_db_flow(value: str) -> str:
    return value.upper()


@django_db_flow(name="django db flow with kwargs")
def keyword_django_db_flow(value: str) -> str:
    return value.upper()


@django_db_task
async def async_add_one(value: int) -> int:
    return value + 1


@django_db_flow
async def async_uppercase(value: str) -> str:
    return value.upper()


@patch("workflows.prefect_utils.flows_utils.connections.all")
def test_close_stale_connections_skips_atomic_connections(mock_connections_all):
    healthy_conn = MagicMock(in_atomic_block=False)
    atomic_conn = MagicMock(in_atomic_block=True)
    mock_connections_all.return_value = [healthy_conn, atomic_conn]

    close_stale_connections()

    healthy_conn.close_if_unusable_or_obsolete.assert_called_once_with()
    atomic_conn.close_if_unusable_or_obsolete.assert_not_called()


@patch("workflows.prefect_utils.flows_utils.close_stale_connections")
def test_django_db_task_supports_bare_decorator(
    mock_close_stale_connections, prefect_harness
):
    assert isinstance(add_one, Task)
    assert add_one(4) == 5

    mock_close_stale_connections.assert_called_once_with()


@patch("workflows.prefect_utils.flows_utils.close_stale_connections")
def test_django_db_flow_supports_bare_decorator(
    mock_close_stale_connections, prefect_harness
):
    assert isinstance(bare_django_db_flow, Flow)
    assert bare_django_db_flow("blue") == "BLUE"

    mock_close_stale_connections.assert_called_once_with()


@patch("workflows.prefect_utils.flows_utils.close_stale_connections")
def test_django_db_flow_supports_keyword_decorator(
    mock_close_stale_connections, prefect_harness
):
    assert isinstance(keyword_django_db_flow, Flow)
    assert keyword_django_db_flow("green") == "GREEN"

    mock_close_stale_connections.assert_called_once_with()


@patch("workflows.prefect_utils.flows_utils.close_stale_connections")
def test_django_db_task_supports_keyword_decorator(
    mock_close_stale_connections, prefect_harness
):
    assert isinstance(add_two, Task)
    result = add_two(4)

    assert result == 6
    mock_close_stale_connections.assert_called_once_with()


@patch("workflows.prefect_utils.flows_utils.close_stale_connections")
@pytest.mark.asyncio
async def test_django_db_task_preserves_async_callables(
    mock_close_stale_connections, prefect_harness
):
    assert isinstance(async_add_one, Task)
    assert async_add_one.isasync
    assert await async_add_one(4) == 5

    mock_close_stale_connections.assert_called_once_with()


@patch("workflows.prefect_utils.flows_utils.close_stale_connections")
@pytest.mark.asyncio
async def test_django_db_flow_preserves_async_callables(
    mock_close_stale_connections, prefect_harness
):
    assert isinstance(async_uppercase, Flow)
    assert async_uppercase.isasync
    assert await async_uppercase("red") == "RED"

    mock_close_stale_connections.assert_called_once_with()


@pytest.mark.django_db(transaction=True)
def test_django_task_recovers_stale_connection(prefect_harness):
    """
    Verify that django_task recovers from a connection closed server-side due
    to idle_session_timeout. This simulates the long idle periods that occur in
    Prefect flows while waiting for e.g. a SLURM job to finish.
    """
    # Establish a connection, then shorten the idle timeout for this session
    with connection.cursor() as cursor:
        cursor.execute("SET SESSION idle_session_timeout = '200ms'")

    # Sleep beyond the timeout so the server closes the connection
    time.sleep(0.5)

    # _close_stale_connections() inside django_task detects the bad connection
    # via the health check and closes it, allowing Django to reconnect cleanly.
    result = count_ena_studies()
    assert isinstance(result, int)


@pytest.mark.django_db(transaction=True)
def test_task_fails_stale_connection(prefect_harness):
    """
    Verify that django_task recovers from a connection closed server-side due
    to idle_session_timeout. This simulates the long idle periods that occur in
    Prefect flows while waiting for e.g. a SLURM job to finish.
    """
    # Establish a connection, then shorten the idle timeout for this session
    with connection.cursor() as cursor:
        cursor.execute("SET SESSION idle_session_timeout = '200ms'")

    # Sleep beyond the timeout so the server closes the connection
    time.sleep(0.5)

    # _close_stale_connections() inside django_task detects the bad connection
    # via the health check and closes it, allowing Django to reconnect cleanly.
    with pytest.raises(OperationalError):
        count_ena_studies_prefect()

    # Close the broken connection so the test teardown can reconnect cleanly.
    connection.close()
