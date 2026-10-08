import pytest
from django.db import connections, router


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
