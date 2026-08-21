from django.conf import settings

import emgapiv2.settings as base_settings


def test_base_settings_use_database_task_backend():
    assert "django_tasks" in base_settings.INSTALLED_APPS
    assert "django_tasks_db" in base_settings.INSTALLED_APPS
    assert base_settings.TASKS == {
        "default": {
            "BACKEND": "django_tasks_db.DatabaseBackend",
            "QUEUES": ["default"],
        }
    }


def test_test_settings_use_immediate_task_backend():
    assert settings.TASKS == {
        "default": {
            "BACKEND": "django_tasks.backends.immediate.ImmediateBackend",
            "QUEUES": ["default"],
        }
    }
