from django.apps import AppConfig


class VirusesConfig(AppConfig):
    """Configure the viral catalogue application."""

    default_auto_field = "django.db.models.BigAutoField"
    name = "viruses"
    verbose_name = "MGnify Viruses"
