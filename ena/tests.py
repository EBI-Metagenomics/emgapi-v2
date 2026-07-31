import re

import pytest
from django.apps import apps
from django.core.management import call_command

import analyses.models
import ena.models


def test_models_with_is_suppressed_have_explicit_manager():
    suppressed_models = [
        model
        for model in apps.get_models()
        if any(field.name == "is_suppressed" for field in model._meta.fields)
    ]

    assert suppressed_models
    for model in suppressed_models:
        assert hasattr(model, "objects_not_suppressed")
        assert model._default_manager.name == "objects"


@pytest.mark.django_db(transaction=True)
def test_ena_suppression_and_privacy_propagation(mgnify_assemblies, raw_read_analyses):
    models_with_suppression_managers = (
        ena.models.Study,
        analyses.models.Study,
        analyses.models.Sample,
        analyses.models.Run,
        analyses.models.Analysis,
        analyses.models.Assembly,
    )

    assert analyses.models.Study.objects.count() == 1

    assert ena.models.Study.objects.count() == 1

    ena_study: ena.models.Study = ena.models.Study.objects.first()

    assert not analyses.models.Study.objects.filter(is_suppressed=True).exists()
    assert not analyses.models.Sample.objects.filter(is_suppressed=True).exists()
    assert not analyses.models.Run.objects.filter(is_suppressed=True).exists()
    assert not analyses.models.Analysis.objects.filter(is_suppressed=True).exists()
    assert not analyses.models.Assembly.objects.filter(is_suppressed=True).exists()
    assert all(
        model.objects_not_suppressed.exists()
        for model in models_with_suppression_managers
    )

    ena_study.is_suppressed = True
    ena_study.save()

    # everything derived should be suppressed
    assert analyses.models.Study.objects.filter(is_suppressed=True).exists()
    assert analyses.models.Sample.objects.filter(is_suppressed=True).exists()
    assert analyses.models.Run.objects.filter(is_suppressed=True).exists()
    assert analyses.models.Analysis.objects.filter(is_suppressed=True).exists()
    assert analyses.models.Assembly.objects.filter(is_suppressed=True).exists()
    assert all(
        model.objects.filter(is_suppressed=True).exists()
        for model in models_with_suppression_managers
    )
    assert all(
        not model.objects_not_suppressed.exists()
        for model in models_with_suppression_managers
    )

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
    assert all(
        model.objects_not_suppressed.exists()
        for model in models_with_suppression_managers
    )

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


@pytest.mark.django_db(transaction=True)
def test_sync_studies_with_ena(raw_reads_mgnify_study, httpx_mock):
    httpx_mock.add_response(
        url=re.compile(r".*result=study.*"),
        json=[
            {
                "study_title": "from tromso",
                "study_description": "we looked deep into the fjords",
            }
        ],
        is_reusable=True,
    )

    mgnify_study: analyses.models.Study = analyses.models.Study.objects.first()
    ena_study: ena.models.Study = ena.models.Study.objects.first()

    call_command("sync_studies_with_ena", "--accessions", ena_study.accession)
    mgnify_study.refresh_from_db()
    assert mgnify_study.metadata == {
        "study_title": "from tromso",
        "study_description": "we looked deep into the fjords",
    }


@pytest.mark.django_db(transaction=True)
def test_ena_study_accession_lookups_and_updating():
    primary_accession = "PRJNA1"
    secondary_accession = "ERP1"

    # at first, we only know about the primary accession (for any reason)
    ena_study = ena.models.Study.objects.create(accession="PRJNA1", title="Project 1")
    ena_study.save()

    # later, we know about the secondary accession because we got both from ENA portal API
    additional_accessions = [secondary_accession]  # e.g. from ena_utils
    study_got_later, created_again = (
        ena.models.Study.objects.update_or_create_by_accession(
            accession=primary_accession,
            defaults={
                "title": "Project 1",
                "additional_accessions": additional_accessions,
            },
        )
    )
    assert not created_again
    assert study_got_later.accession == primary_accession
    assert study_got_later.additional_accessions == additional_accessions

    # later again, we know only about the secondary accession
    study_got_by_secondary, created_by_secondary = (
        ena.models.Study.objects.update_or_create_by_accession(
            accession=secondary_accession,
            defaults={"webin_submitter": "Webin-newlyknown"},
        )
    )
    assert not created_by_secondary
    assert study_got_by_secondary == study_got_later
    assert study_got_by_secondary.webin_submitter == "Webin-newlyknown"
