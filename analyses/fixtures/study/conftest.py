import django
import pytest

django.setup()

import analyses.models as mg_models


@pytest.fixture
def raw_reads_mgnify_study(raw_read_ena_study):
    study = mg_models.Study.objects.get_or_create(
        ena_study=raw_read_ena_study, title=raw_read_ena_study.title
    )[0]
    study.inherit_accessions_from_related_ena_object("ena_study")
    return study
