import os

from workflows.prefect_utils.env_context import TemporaryEnv


def test_env_context():
    assert "SPECIAL_ENV" not in os.environ
    assert "PYTEST_CURRENT_TEST" in os.environ
    test = os.environ.get("PYTEST_CURRENT_TEST")

    with TemporaryEnv(SPECIAL_ENV="42"):
        assert "SPECIAL_ENV" in os.environ
        assert os.environ["SPECIAL_ENV"] == "42"
        assert "PYTEST_CURRENT_TEST" in os.environ
        assert os.environ["PYTEST_CURRENT_TEST"] == test

    assert "SPECIAL_ENV" not in os.environ
    assert "PYTEST_CURRENT_TEST" in os.environ
    assert os.environ["PYTEST_CURRENT_TEST"] == test
