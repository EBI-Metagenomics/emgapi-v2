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
