from pathlib import Path

import pytest
from django.db import connections

TIER1_SQL = Path(__file__).parent.parent / "sql" / "tier1.sql"


@pytest.fixture(scope="session")
def tier1(django_db_setup, django_db_blocker):
    """The Tier 1 schema, applied once to the proteindb test database."""
    with django_db_blocker.unblock(), connections["proteindb"].cursor() as cursor:
        cursor.execute(TIER1_SQL.read_text())
        cursor.execute("RESET search_path")
