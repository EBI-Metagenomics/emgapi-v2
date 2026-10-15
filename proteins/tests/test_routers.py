import pytest
from django.conf import settings
from django.db import connections, router

from analyses.models import Analysis
from proteins.apps import check_mgyp_database
from proteins.models import AssemblyProteins, Protein
from proteins.routers import MGYP


def test_proteins_models_are_read_and_written_on_the_mgyp_database():
    for model in (Protein, AssemblyProteins):
        assert router.db_for_read(model) == MGYP
        assert router.db_for_write(model) == MGYP
    assert router.db_for_read(Analysis) == "default"
    assert router.db_for_write(Analysis) == "default"


def test_no_migrations_run_on_the_mgyp_database():
    assert router.allow_migrate(MGYP, "analyses") is False
    assert router.allow_migrate(MGYP, "proteins") is False
    assert router.allow_migrate("default", "proteins") is False
    assert router.allow_migrate("default", "analyses") is True


def on(db, obj):
    obj._state.db = db
    return obj


def test_no_relations_between_the_mgyp_database_and_others():
    assert not router.allow_relation(on(MGYP, Protein()), on("default", Analysis()))
    assert router.allow_relation(on(MGYP, Protein()), on(MGYP, AssemblyProteins()))


# "default" too: the root conftest creates a user there for every database test.
@pytest.mark.django_db(databases=["default", MGYP])
def test_mgyp_test_database_is_created_without_django_tables():
    with connections[MGYP].cursor() as cursor:
        tables = connections[MGYP].introspection.table_names(cursor)
    # Django records migrations even where the router allows none.
    assert tables == ["django_migrations"]


def test_warns_only_without_the_mgyp_alias(monkeypatch):
    assert check_mgyp_database(None) == []
    monkeypatch.delitem(settings.DATABASES, MGYP)
    assert [message.id for message in check_mgyp_database(None)] == ["proteins.W001"]
