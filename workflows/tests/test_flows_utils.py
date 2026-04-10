import time

import pytest
from django.db import connection, OperationalError
from prefect import task

from ena.models import Study
from workflows.prefect_utils.flows_utils import django_db_task


@django_db_task()
def count_ena_studies() -> int:
    return Study.objects.count()


@task
def count_ena_studies_prefect() -> int:
    return Study.objects.count()


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
