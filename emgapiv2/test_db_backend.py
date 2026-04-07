from unittest.mock import patch

from emgapiv2.db_backend.postgresql.base import DatabaseWrapper


def make_wrapper():
    return DatabaseWrapper.__new__(DatabaseWrapper)


def test_ensure_connection_calls_health_check_before_super():
    wrapper = make_wrapper()
    call_order = []

    with (
        patch.object(
            DatabaseWrapper,
            "close_if_unusable_or_obsolete",
            side_effect=lambda: call_order.append("health_check"),
        ),
        patch(
            "django.db.backends.postgresql.base.DatabaseWrapper.ensure_connection",
            side_effect=lambda: call_order.append("super_ensure"),
        ),
    ):
        wrapper.ensure_connection()

    assert call_order == ["health_check", "super_ensure"]


def test_ensure_connection_checks_health_even_with_existing_connection():
    wrapper = make_wrapper()
    wrapper.connection = "existing"  # any non-None value simulates a live connection
    health_check_called = []

    with (
        patch.object(
            DatabaseWrapper,
            "close_if_unusable_or_obsolete",
            side_effect=lambda: health_check_called.append(True),
        ),
        patch(
            "django.db.backends.postgresql.base.DatabaseWrapper.ensure_connection",
        ),
    ):
        wrapper.ensure_connection()

    assert health_check_called


def test_worker_settings_uses_custom_backend_engine():
    from emgapiv2 import settings_prefect

    assert (
        settings_prefect.DATABASES["default"]["ENGINE"]
        == "emgapiv2.db_backend.postgresql"
    )
