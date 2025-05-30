import pytest
from django.core.management import call_command
from django.db.models import Count
import logging

from analyses.models import Study as MGnifyStudy
from analyses.models import Assembly, Run, Sample
from ena.models import Study as ENAStudy
from ena.models import Sample as ENASample


@pytest.fixture
def setup_duplicate_studies():

    ena_dup_a = ENAStudy.objects.create(
        accession="PRJEB11111",
        is_private=False,
        is_suppressed=False,
        webin_submitter="Webin-460",
    )
    ena_dup_b = ENAStudy.objects.create(
        accession="PRJEB22222",
        is_private=False,
        is_suppressed=False,
        webin_submitter="Webin-460",
    )
    ena_single = ENAStudy.objects.create(
        accession="PRJEB33333",
        is_private=False,
        is_suppressed=False,
        webin_submitter="Webin-460",
    )

    # Create two MGnify studies linked to the same ENA study
    mg_dup_a_new = MGnifyStudy.objects.create(
        id=10001, ena_study=ena_dup_a, title="Test Study 1 new"
    )
    mg_dup_a_old = MGnifyStudy.objects.create(
        id=6001, ena_study=ena_dup_a, title="Test Study 1 old"
    )
    mg_dup_b_new = MGnifyStudy.objects.create(
        id=10002, ena_study=ena_dup_b, title="Test Study 2 new"
    )
    mg_dup_b_old = MGnifyStudy.objects.create(
        id=6002, ena_study=ena_dup_b, title="Test Study 2 old"
    )

    # Create a single MGnify study linked to a different ENA study
    mg_single = MGnifyStudy.objects.create(
        id=10003, ena_study=ena_single, title="Test Study single"
    )

    # ena sample --> ena study --> run
    ena_sample_a = ENASample.objects.create(
        accession="ERS1111111", study=ena_dup_a, metadata={}
    )
    ena_sample_b = ENASample.objects.create(
        accession="ERS2222222", study=ena_dup_b, metadata={}
    )
    sample_a = Sample.objects.create(ena_sample=ena_sample_a, ena_study=ena_dup_a)
    sample_b = Sample.objects.create(ena_sample=ena_sample_b, ena_study=ena_dup_b)

    # public assembly (TPA) in different studies, usually >10,000 acc
    assembly_public = Assembly.objects.create(
        assembly_study=mg_single, ena_study=ena_dup_a
    )
    run_public = Run.objects.create(
        ena_accessions=["ERR100000"],
        study=mg_dup_a_new,
        ena_study=ena_dup_a,
        sample=sample_a,
    )

    # private assembly with run and assembly in same study, usually >10,000 acc
    assembly_private = Assembly.objects.create(
        assembly_study=mg_dup_b_new, ena_study=ena_dup_b
    )
    run_private = Run.objects.create(
        ena_accessions=["ERR200000"],
        study=mg_dup_b_new,
        ena_study=ena_dup_b,
        sample=sample_b,
    )

    return {
        "ena_dup_a": ena_dup_a,
        "ena_dup_b": ena_dup_b,
        "ena_single": ena_single,
        "mg_dup_a_new": mg_dup_a_new,
        "mg_dup_a_old": mg_dup_a_old,
        "mg_dup_b_new": mg_dup_b_new,
        "mg_dup_b_old": mg_dup_b_old,
        "mg_single": mg_single,
        "assembly_public": assembly_public,
        "run_public": run_public,
        "assembly_private": assembly_private,
        "run_private": run_private,
        "sample_a": sample_a,
        "sample_b": sample_b,
    }


@pytest.mark.django_db(transaction=True)
def test_deduplicate_mgys_studies(setup_duplicate_studies, caplog):
    dup_studies = setup_duplicate_studies

    duplicates = (
        MGnifyStudy.objects.values("ena_study")
        .annotate(study_count=Count("accession"))
        .filter(study_count=2)
        .values_list("ena_study", flat=True)
    )

    assert len(duplicates) == 2
    assert dup_studies["ena_dup_a"].accession in duplicates
    assert dup_studies["ena_dup_b"].accession in duplicates
    assert dup_studies["ena_single"].accession not in duplicates

    with caplog.at_level(logging.INFO):
        call_command("merge_mgys_duplicates")

        assert (
            f"ENA accession {dup_studies['ena_dup_a'].accession} is linked to multiple MGnify Studies:"
            in caplog.text
        )
        assert (
            f"ENA accession {dup_studies['ena_dup_b'].accession} is linked to multiple MGnify Studies:"
            in caplog.text
        )
        assert (
            f"ENA accession {dup_studies['ena_single']} is linked to multiple MGnify Studies:"
            not in caplog.text
        )


@pytest.mark.django_db(transaction=True)
def test_reassign_runs_and_assemblies(setup_duplicate_studies, caplog):
    setup_duplicate_studies

    start_count = MGnifyStudy.objects.count()

    with caplog.at_level(logging.INFO):
        call_command("merge_mgys_duplicates")

        # public: run and assembly in different studies
        assert "Moving 0 assemblies from MGYS00010001 to MGYS00006001" in caplog.text
        assert "Moving 1 runs from MGYS00010001 to MGYS00006001" in caplog.text
        # private: run and assembly in same study
        assert "Moving 1 assemblies from MGYS00010002 to MGYS00006002" in caplog.text
        assert "Moving 1 runs from MGYS00010002 to MGYS00006002" in caplog.text

        # Check that 2 new duplicate studies are deleted
        assert MGnifyStudy.objects.count() == start_count - 2


@pytest.mark.django_db(transaction=True)
def test_clashing_runs_gives_warning(setup_duplicate_studies, caplog):
    dup_studies = setup_duplicate_studies

    start_count = MGnifyStudy.objects.count()

    # Create duplicate clashing runs in old and new MGnify studies
    Run.objects.create(
        ena_accessions=["ERR300000"],
        study=dup_studies["mg_dup_a_new"],
        ena_study=dup_studies["ena_dup_a"],
        sample=dup_studies["sample_a"],
    )
    Run.objects.create(
        ena_accessions=["ERR300000"],
        study=dup_studies["mg_dup_a_old"],
        ena_study=dup_studies["ena_dup_a"],
        sample=dup_studies["sample_a"],
    )
    with caplog.at_level(logging.WARNING):
        call_command("merge_mgys_duplicates")

        assert (
            "DUPLICATE RUNS FOUND IN BOTH STUDIES: old MGYS00006001 and new MGYS00010001. No further action performed."
            in caplog.text
        )
        # this time MGYS00010001 is skipped and not deleted
        assert MGnifyStudy.objects.count() == start_count - 1
