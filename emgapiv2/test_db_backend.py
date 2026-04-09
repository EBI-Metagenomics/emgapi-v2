from unittest.mock import patch

from emgapiv2.db_backend.postgresql.base import DatabaseWrapper


def make_wrapper():
    return DatabaseWrapper.__new__(DatabaseWrapper)


def test_ensure_connection_skips_health_check_when_no_connection():
    """When self.connection is None there is nothing stale to close, and
    calling close_if_unusable_or_obsolete would recurse via get_autocommit."""
    wrapper = make_wrapper()
    wrapper.connection = None
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

    assert call_order == ["super_ensure"]


def test_ensure_connection_runs_health_check_before_super_when_connection_exists():
    wrapper = make_wrapper()
    wrapper.connection = "existing"  # any non-None value simulates a live connection
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


def test_ensure_connection_does_not_recurse_like_django_get_autocommit():
    """Regression: Django's close_if_unusable_or_obsolete calls get_autocommit,
    which calls ensure_connection when no connection is open. The wrapper must
    not recurse into itself in that scenario."""
    wrapper = make_wrapper()
    wrapper.connection = None
    ensure_calls = []

    def fake_close_if_unusable_or_obsolete():
        # Mimic Django: get_autocommit() -> ensure_connection() when conn is None
        wrapper.ensure_connection()

    with (
        patch.object(
            DatabaseWrapper,
            "close_if_unusable_or_obsolete",
            side_effect=fake_close_if_unusable_or_obsolete,
        ),
        patch(
            "django.db.backends.postgresql.base.DatabaseWrapper.ensure_connection",
            side_effect=lambda: ensure_calls.append(True),
        ),
    ):
        wrapper.ensure_connection()  # must not raise RecursionError

    assert ensure_calls == [True]


def test_worker_settings_uses_custom_backend_engine():
    from emgapiv2 import settings_prefect

    assert (
        settings_prefect.DATABASES["default"]["ENGINE"]
        == "emgapiv2.db_backend.postgresql"
    )
