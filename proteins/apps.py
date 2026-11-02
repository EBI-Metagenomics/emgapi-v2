from django.apps import AppConfig
from django.conf import settings
from django.core import checks


def check_mgyp_database(app_configs, **kwargs) -> list[checks.CheckMessage]:
    """Warns when the mgyp alias is not set. A warning, so that environments without the MGYP database still start."""
    from proteins.routers import MGYP

    if MGYP in settings.DATABASES:
        return []
    return [
        checks.Warning(
            f"The {MGYP} database alias is not set. Set MGYP_DATABASE_URL to use the proteins flows.",
            id="proteins.W001",
        )
    ]


class ProteinsConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "proteins"
    verbose_name = "MGnify Proteins"

    def ready(self):
        # No database tag: Django runs those checks only with --database.
        checks.register(check_mgyp_database)
