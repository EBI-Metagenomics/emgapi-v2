import logging
import re
import threading
import time
from contextlib import contextmanager
from typing import Iterator

import psycopg
from django.conf import settings
from django.db import OperationalError, connections

logger = logging.getLogger(__name__)

MGYP = "mgyp"
# The messages of SQLSTATE 53300: the role's or the database's connection limit, or max_connections.
# psycopg gives no SQLSTATE for an error while connecting, so the message is matched.
TOO_MANY_CONNECTIONS = re.compile(
    r"too many connections for|too many clients already|remaining connection slots are reserved"
)
RETRY_SECONDS = 5


class MGYPRouter:
    """Sends the proteins models to the MGYP database. Django never migrates it: sql/mgyp_schema.sql creates it."""

    def db_for_read(self, model, **hints):
        return MGYP if model._meta.app_label == "proteins" else None

    db_for_write = db_for_read

    def allow_relation(self, obj1, obj2, **hints):
        if MGYP in {obj1._state.db, obj2._state.db}:
            return obj1._state.db == obj2._state.db
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        if db == MGYP or app_label == "proteins":
            return False
        return None


@contextmanager
def mgyp_connection() -> Iterator[psycopg.Connection]:
    """The psycopg connection of the Django mgyp alias, for the current thread.

    For the accessioning core, which uses psycopg directly: binary COPY, temporary tables and
    parallel lookups. It is Django's own connection, in autocommit, with the settings of the alias.

    Django keeps one connection for each thread. The lookups run in short-lived threads, and Django
    never closes the connections of those threads, so this closes them at the end of the with block.

    When the connections are all taken, it tries again every RETRY_SECONDS, for up to
    EMG_CONFIG.proteins.connection_wait_seconds. Any other error fails at once.
    """
    wrapper = connections[MGYP]
    deadline = time.monotonic() + settings.EMG_CONFIG.proteins.connection_wait_seconds
    while True:
        try:
            wrapper.ensure_connection()
            break
        except OperationalError as e:
            if not TOO_MANY_CONNECTIONS.search(str(e)) or time.monotonic() > deadline:
                raise
            logger.warning(f"No free connection to the MGYP database, waiting: {e}")
            time.sleep(RETRY_SECONDS)
    try:
        yield wrapper.connection
    finally:
        if threading.current_thread() is not threading.main_thread():
            wrapper.close()
