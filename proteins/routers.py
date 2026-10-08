PROTEINDB = "proteindb"


class ProteinDBRouter:
    """Keeps every Django migration off the Protein DB, whose schema is applied from sql/ outside Django, and the proteins app's off every database."""

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        if db == PROTEINDB or app_label == "proteins":
            return False
        return None
