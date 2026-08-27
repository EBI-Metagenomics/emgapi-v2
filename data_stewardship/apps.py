"""Django application configuration for data stewardship."""

from django.apps import AppConfig


class DataStewardshipConfig(AppConfig):
    """Configure the data stewardship Django application."""

    default_auto_field = "django.db.models.BigAutoField"
    name = "data_stewardship"
