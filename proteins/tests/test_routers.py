from types import SimpleNamespace

import pytest
from django.db import connections, router

from analyses.models import Study


def model_of(app_label):
    # The proteins app has no models yet.
    return SimpleNamespace(_meta=SimpleNamespace(app_label=app_label))


def test_proteins_models_go_to_the_protein_db():
    assert router.db_for_read(model_of("proteins")) == "proteindb"
    assert router.db_for_write(model_of("proteins")) == "proteindb"


def test_other_apps_models_stay_on_default():
    assert router.db_for_read(Study) == "default"
    assert router.db_for_write(Study) == "default"


def test_no_migrations_run_on_the_protein_db():
    assert router.allow_migrate("proteindb", "analyses") is False
    assert router.allow_migrate("proteindb", "proteins") is False
    assert router.allow_migrate("default", "proteins") is False
    assert router.allow_migrate("default", "analyses") is True


# "default" too: the root conftest creates a user there for every database test.
@pytest.mark.django_db(databases=["default", "proteindb"])
def test_protein_db_test_database_is_created_without_django_tables():
    with connections["proteindb"].cursor() as cursor:
        tables = connections["proteindb"].introspection.table_names(cursor)
    # Django records migrations even where the router allows none.
    assert tables == ["django_migrations"]
