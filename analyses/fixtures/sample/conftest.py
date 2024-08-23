import pytest

import django
django.setup()

import analyses.models as mg_models

@pytest.fixture
def raw_reads_mgnify_sample(raw_reads_mgnify_study, raw_read_ena_sample):
    sample_objects = []
    for sample in raw_read_ena_sample:
        sample_obj, _ = mg_models.Sample.objects.get_or_create(
            ena_sample=sample,
            ena_study=raw_reads_mgnify_study.ena_study,
        )
        sample_objects.append(sample_obj)
    return sample_objects