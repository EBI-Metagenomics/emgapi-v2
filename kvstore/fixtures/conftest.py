import pytest
from django.core.management import call_command


@pytest.fixture
def cog_categories_fixture():
    call_command("loaddata", "kvstore/fixtures/cog_categories.yaml")


@pytest.fixture
def kegg_classes_fixture():
    call_command("loaddata", "kvstore/fixtures/kegg_classes.yaml")
