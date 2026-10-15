import threading
from contextlib import contextmanager
from typing import Iterator

import psycopg
from django.db import connections

MGYP = "mgyp"


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
    """
    wrapper = connections[MGYP]
    wrapper.ensure_connection()
    try:
        yield wrapper.connection
    finally:
        if threading.current_thread() is not threading.main_thread():
            wrapper.close()
