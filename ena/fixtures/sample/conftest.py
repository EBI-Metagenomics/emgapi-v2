import django
import pytest

django.setup()

import ena.models as ena_models


@pytest.fixture
def raw_read_ena_sample(raw_read_ena_study):
    samples = [
        {"accession": "SAMN07793787", "description": "Sample 1"},
        {"accession": "SAMN07793788", "description": "Sample 2"},
        {"accession": "SAMN08514017", "description": "Sample 3"},
    ]
    sample_objects = []
    for sample in samples:
        sample_obj, _ = ena_models.Sample.objects.get_or_create(
            study=raw_read_ena_study, accession=sample["accession"]
        )
        sample_objects.append(sample_obj)
    return sample_objects


@pytest.fixture
def private_ena_sample(webin_private_ena_study):
    sample_obj, _ = ena_models.Sample.objects.get_or_create(
        study=webin_private_ena_study, accession="SAMN00000001"
    )
    return sample_obj
