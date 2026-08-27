"""Django application configuration for curations."""

from django.apps import AppConfig


class CurationsConfig(AppConfig):
    """Configure the curations Django application."""

    default_auto_field = "django.db.models.BigAutoField"
    name = "curations"
