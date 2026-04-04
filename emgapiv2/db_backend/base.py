from django.db.backends.postgresql import base


class DatabaseWrapper(base.DatabaseWrapper):
    """
    Custom PostgreSQL database wrapper that ensures connection health before use.

    Overrides ``ensure_connection()`` to call ``close_if_unusable_or_obsolete()`` first,
    making ``CONN_MAX_AGE`` and ``CONN_HEALTH_CHECKS`` effective outside HTTP request
    cycles (e.g. Prefect flows that pause for hours/days waiting on cluster jobs).

    :note: Django's default behaviour only triggers connection health checks during
        HTTP requests via middleware. This backend extends that to all contexts.
    """

    def ensure_connection(self) -> None:
        """
        Guarantee a live connection, closing stale or obsolete connections first.
        """
        self.close_if_unusable_or_obsolete()
        super().ensure_connection()
