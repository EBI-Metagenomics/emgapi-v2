import os
from unittest.mock import patch, Mock

import django
import pytest
from ninja.testing import TestClient
from prefect.testing.utilities import prefect_test_harness

django.setup()

from emgapiv2.api import api
from django.db import connections

# model fixtures
pytest_plugins = [
    "ena.fixtures.sample.conftest",
    "ena.fixtures.study.conftest",
    "analyses.fixtures.analysis.conftest",
    "analyses.fixtures.assembler.conftest",
    "analyses.fixtures.assembly.conftest",
    "analyses.fixtures.biome.conftest",
    "analyses.fixtures.run.conftest",
    "analyses.fixtures.sample.conftest",
    "analyses.fixtures.study.conftest",
    "workflows.fixtures.legacy_emg_dbs.conftest",
    "workflows.fixtures.slurm.conftest",
    "workflows.nextflow_utils.fixtures.conftest",
]


@pytest.fixture(scope="session")
def prefect_harness():
    with prefect_test_harness():
        yield


@pytest.fixture
def mock_suspend_flow_run(request):
    namespace = request.param
    with patch(f"{namespace}.suspend_flow_run", new_callable=Mock) as mock_suspend:
        yield mock_suspend


@pytest.fixture(scope="session")
def ninja_api_client():
    yield TestClient(api)


@pytest.fixture(scope="session", autouse=True)
def ninja_namespace_workaround():
    # https://github.com/vitalik/django-ninja/issues/1195#issuecomment-2307007575
    os.environ["NINJA_SKIP_REGISTRY"] = "yes"


@pytest.fixture(scope="session", autouse=True)
def django_db_modify_db_settings_for_xdist():
    """
    Configure the database for parallel test execution with pytest-xdist.
    This fixture ensures each worker gets its own isolated database.
    """
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "")
    if not worker_id:
        # not running with xdist
        return

    # Modify database name for each worker
    for db_name in connections.databases:
        db_settings = connections.databases[db_name]
        if "NAME" in db_settings:
            db_settings["NAME"] = f"{db_settings['NAME']}_{worker_id}"


@pytest.fixture(scope="function", autouse=True)
def user(django_user_model):
    return django_user_model.objects.create_user(
        username="testuser", password="password123"
    )
