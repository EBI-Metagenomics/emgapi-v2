PROTEINDB = "proteindb"


class ProteinDBRouter:
    """Sends the proteins app's models to the Protein DB, and no other app's models there.

    Django runs no migrations on the Protein DB: its schema is owned by the team and
    applied outside Django, so the proteins app's models are unmanaged.
    """

    def db_for_read(self, model, **hints):
        return PROTEINDB if model._meta.app_label == "proteins" else None

    db_for_write = db_for_read

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        if db == PROTEINDB or app_label == "proteins":
            return False
        return None
