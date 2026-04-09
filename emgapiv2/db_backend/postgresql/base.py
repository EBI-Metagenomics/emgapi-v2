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

        Only checks for stale/obsolete connections when one already exists, to avoid
        a recursion: ``get_autocommit()`` (called inside ``close_if_unusable_or_obsolete``)
        would otherwise call ``ensure_connection()`` again when no connection is open.
        """
        if self.connection is not None:
            self.close_if_unusable_or_obsolete()
        super().ensure_connection()
