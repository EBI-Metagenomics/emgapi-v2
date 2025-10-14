import django
import pytest

from workflows.ena_utils.sample import ENASampleFields

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
        sample_obj.studies.add(raw_reads_mgnify_study)
        sample_obj.inherit_accessions_from_related_ena_object("ena_sample")
        sample_objects.append(sample_obj)
    return sample_objects


@pytest.fixture
def private_mgnify_sample(webin_private_study, private_ena_sample):
    sample_obj, _ = mg_models.Sample.objects.get_or_create(
        ena_sample=private_ena_sample,
        ena_study=webin_private_study.ena_study,
        is_private=True,
        webin_submitter=webin_private_study.webin_submitter,
    )
    sample_obj.inherit_accessions_from_related_ena_object("ena_sample")
    return sample_obj


@pytest.fixture
def mgnify_sample_lots_of_metadata(raw_reads_mgnify_sample):
    sample = raw_reads_mgnify_sample[0]
    _ = ENASampleFields
    sample.metadata = {
        _.SAMPLE_TITLE: "Space station dust",
        _.SAMPLE_DESCRIPTION: "Microbiome sampling of a vacuum cleaner on the ISS",
        _.LAT: "19.456",
        _.LON: "-155.123",
        _.ALTITUDE: 402317,
        _.TEMPERATURE: 19.1,
        _.ENVIRONMENT_FEATURE: "indoor",
        _.ENVIRONMENT_BIOME: "ISS",
        _.ENVIRONMENT_MATERIAL: "dust",
        _.COLLECTION_DATE: "2023-01-01",
        _.CHECKLIST: "ERC000031",
        _.SAMPLE_ALIAS: "ISS-D1",
    }
