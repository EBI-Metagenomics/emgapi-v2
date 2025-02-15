import os
from unittest.mock import AsyncMock, patch

import django
import pytest
from ninja.testing import TestClient
from prefect.testing.utilities import prefect_test_harness

django.setup()

from emgapiv2.api import api

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
]


@pytest.fixture(scope="session")
def prefect_harness():
    with prefect_test_harness():
        yield


@pytest.fixture
def mock_suspend_flow_run(request):
    namespace = request.param
    with patch(f"{namespace}.suspend_flow_run", new_callable=AsyncMock) as mock_suspend:
        yield mock_suspend


@pytest.fixture(scope="session")
def ninja_api_client():
    yield TestClient(api)


@pytest.fixture(scope="session", autouse=True)
def ninja_namespace_workaround():
    # https://github.com/vitalik/django-ninja/issues/1195#issuecomment-2307007575
    os.environ["NINJA_SKIP_REGISTRY"] = "yes"
