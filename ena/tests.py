import re

import pytest
from django.core.management import call_command

import analyses.models
import ena.models


@pytest.mark.django_db(transaction=True)
def test_ena_suppression_and_privacy_propagation(mgnify_assemblies, raw_read_analyses):
    assert analyses.models.Study.objects.count() == 1

    assert ena.models.Study.objects.count() == 1

    ena_study: ena.models.Study = ena.models.Study.objects.first()

    assert not analyses.models.Study.objects.filter(is_suppressed=True).exists()
    assert not analyses.models.Sample.objects.filter(is_suppressed=True).exists()
    assert not analyses.models.Run.objects.filter(is_suppressed=True).exists()
    assert not analyses.models.Analysis.objects.filter(is_suppressed=True).exists()
    assert not analyses.models.Assembly.objects.filter(is_suppressed=True).exists()

    ena_study.is_suppressed = True
    ena_study.save()

    # everything derived should be suppressed
    assert analyses.models.Study.objects.filter(is_suppressed=True).exists()
    assert analyses.models.Sample.objects.filter(is_suppressed=True).exists()
    assert analyses.models.Run.objects.filter(is_suppressed=True).exists()
    assert analyses.models.Analysis.objects.filter(is_suppressed=True).exists()
    assert analyses.models.Assembly.objects.filter(is_suppressed=True).exists()

    assert not analyses.models.Study.objects.filter(is_suppressed=False).exists()
    assert not analyses.models.Sample.objects.filter(is_suppressed=False).exists()
    assert not analyses.models.Run.objects.filter(is_suppressed=False).exists()
    assert not analyses.models.Analysis.objects.filter(is_suppressed=False).exists()
    assert not analyses.models.Assembly.objects.filter(is_suppressed=False).exists()

    ena_study.is_suppressed = False
    ena_study.save()
    # everything should be unsuppressed now
    assert not analyses.models.Study.objects.filter(is_suppressed=True).exists()
    assert not analyses.models.Sample.objects.filter(is_suppressed=True).exists()
    assert not analyses.models.Run.objects.filter(is_suppressed=True).exists()
    assert not analyses.models.Analysis.objects.filter(is_suppressed=True).exists()
    assert not analyses.models.Assembly.objects.filter(is_suppressed=True).exists()

    # everything should have been public so far
    assert analyses.models.Study.objects.count() == 1
    assert analyses.models.Study.objects.count() == 1
    assert not analyses.models.Study.objects.filter(is_private=True).exists()
    assert not analyses.models.Sample.objects.filter(is_private=True).exists()
    assert not analyses.models.Run.objects.filter(is_private=True).exists()
    assert not analyses.models.Analysis.objects.filter(is_private=True).exists()
    assert not analyses.models.Assembly.objects.filter(is_private=True).exists()

    ena_study.is_private = True
    ena_study.save()

    # everything derived should be private now
    assert analyses.models.Study.objects.filter(is_private=True).exists()
    assert analyses.models.Sample.objects.filter(is_private=True).exists()
    assert analyses.models.Run.objects.filter(is_private=True).exists()
    assert analyses.models.Analysis.objects.filter(is_private=True).exists()
    assert analyses.models.Assembly.objects.filter(is_private=True).exists()

    assert not analyses.models.Study.objects.filter(is_private=False).exists()
    assert not analyses.models.Sample.objects.filter(is_private=False).exists()
    assert not analyses.models.Run.objects.filter(is_private=False).exists()
    assert not analyses.models.Analysis.objects.filter(is_private=False).exists()
    assert not analyses.models.Assembly.objects.filter(is_private=False).exists()

    assert analyses.models.Study.public_objects.count() == 0
    assert analyses.models.Study.objects.count() == 1


@pytest.mark.django_db(transaction=True)
def test_sync_samples_with_ena(raw_read_analyses, httpx_mock):
    httpx_mock.add_response(
        url=re.compile(r".*result=sample.*"),
        json=[{"sample_title": "from tromso", "lat": "69.6"}],
        is_reusable=True,
    )

    mgnify_sample: analyses.models.Sample = analyses.models.Sample.objects.first()
    ena_sample: ena.models.Sample = ena.models.Sample.objects.first()

    mgnify_sample.metadata = {"lat": "69.6", "inferred_lat": 70}
    mgnify_sample.save()

    call_command("sync_samples_with_ena", "--accessions", mgnify_sample.first_accession)
    ena_sample.refresh_from_db()
    assert ena_sample.metadata == {"sample_title": "from tromso", "lat": "69.6"}

    mgnify_sample.refresh_from_db()
    assert mgnify_sample.metadata == {
        "sample_title": "from tromso",
        "lat": "69.6",
        "inferred_lat": 70,
    }

    mgnify_sample.metadata = {"lat": "69.6", "inferred_lat": 70}
    mgnify_sample.save()

    call_command("sync_samples_with_ena", "--all")
    mgnify_sample.refresh_from_db()
    assert mgnify_sample.metadata == {
        "sample_title": "from tromso",
        "lat": "69.6",
        "inferred_lat": 70,
    }
