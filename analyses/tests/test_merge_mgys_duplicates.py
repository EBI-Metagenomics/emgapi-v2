import csv

import pytest
from django.core.management import call_command

import ena.models
from analyses.management.commands.merge_mgys_duplicates import Command
from analyses.models import (
    Analysis,
    Assembly,
    Publication,
    Run,
    Sample,
    Study,
    StudyPublication,
    SuperStudy,
    SuperStudyStudy,
)
from workflows.models import AssemblyAnalysisBatch


@pytest.fixture
def study_pair():
    ena_study = ena.models.Study.objects.create(
        accession="PRJEB10002",
        additional_accessions=["ERP100002"],
        title="Canonical ENA",
    )
    study_old = Study.objects.create(
        ena_accessions=["ERP100002", "PRJEB10002"],
        ena_study=ena_study,
        title="Canonical study",
        metadata={"keep": "me"},
        results_dir="/work/original",
    )
    study_new = Study.objects.create(
        ena_accessions=["ERP100002", "PRJEB10002"],
        ena_study=ena_study,
        title="",
        metadata={"duplicate": "value"},
        external_results_dir="/work/duplicate",
    )
    # Extra one -- just to make sure
    extra_ena_study = ena.models.Study.objects.create(
        accession="PRJEB10003",
        additional_accessions=["ERP100003"],
        title="A different study",
    )
    extra_ena_study.save()
    extra_study = Study.objects.create(
        ena_accessions=["ERP100003", "PRJEB10003"],
        ena_study=ena_study,
        title="Just a random study",
        metadata={"duplicate": "value"},
        external_results_dir="/work/duplicate",
    )
    extra_study.save()
    study_old.save()
    study_new.save()
    return study_old, study_new, ena_study


@pytest.mark.django_db
def test_find_duplicated_studies_returns_matching_pair(study_pair):
    study_old, study_new, _ = study_pair

    groups = Command().find_duplicated_studies()

    assert len(groups) == 1
    assert [study.accession for study in groups[0]] == sorted(
        [study_old.accession, study_new.accession]
    )


@pytest.mark.django_db
def test_find_duplicated_studies_returns_overlap_pair():
    ena_study = ena.models.Study.objects.create(
        accession="PRJEB10001",
        additional_accessions=["ERP100001"],
        title="Overlap ENA",
    )
    study_old = Study.objects.create(ena_study=ena_study, title="Old")
    study_new = Study.objects.create(ena_study=ena_study, title="New")
    study_old.ena_accessions = ["PRJEB10001", "ERP100001"]
    study_new.ena_accessions = ["PRJEB10001"]
    study_old.save()
    study_new.save()

    groups = Command().find_duplicated_studies()

    assert len(groups) == 1
    assert {study.id for study in groups[0]} == {study_old.id, study_new.id}


@pytest.mark.django_db
def test_find_duplicated_studies_ignores_studies_without_ena_accessions():
    ena_study = ena.models.Study.objects.create(
        accession="PRJEB19999",
        additional_accessions=["ERP19999"],
        title="No accession links",
    )
    Study.objects.create(
        ena_study=ena_study, title="Missing accessions", ena_accessions=[]
    )
    Study.objects.create(
        ena_study=ena_study, title="Missing accessions too", ena_accessions=[]
    )

    groups = Command().find_duplicated_studies()

    assert groups == []


@pytest.mark.django_db
def test_merge_mgys_duplicates_dry_run_leaves_data_unchanged(tmp_path, study_pair):
    study_old, study_new, _ = study_pair
    sample = Sample.objects.create(
        ena_sample=ena.models.Sample.objects.create(
            accession="SAMEA-DRY",
            study=study_new.ena_study,
        ),
        ena_study=study_new.ena_study,
        ena_accessions=["SAMEA-DRY"],
    )
    sample.studies.add(study_new)
    Run.objects.create(
        ena_accessions=["ERR-DRY"],
        study=study_new,
        ena_study=study_new.ena_study,
        sample=sample,
    )
    report_path = tmp_path / "dry-run-report.csv"

    call_command(
        "merge_mgys_duplicates",
        "--output-csv",
        str(report_path),
    )

    assert Study.objects.filter(id=study_old.id).exists()
    assert Study.objects.filter(id=study_new.id).exists()
    assert (
        Run.objects.get(ena_accessions__contains=["ERR-DRY"]).study_id == study_new.id
    )
    with report_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["status"] == "dry_run"
    assert rows[0]["canonical_study"] == study_old.accession


@pytest.mark.django_db
def test_merge_mgys_duplicates_rewires_relations(tmp_path, study_pair):
    study_old, study_new, ena_study = study_pair
    study_old.title = ""
    study_old.metadata = {"keep": "me"}
    study_old.results_dir = None
    study_old.external_results_dir = None
    study_old.features = {"has_prev6_analyses": False, "has_v6_analyses": False}
    study_old.save()

    study_new.title = "Preferred title"
    study_new.metadata = {"duplicate": "value"}
    study_new.results_dir = "/work/results"
    study_new.external_results_dir = "/work/external"
    study_new.features = {"has_prev6_analyses": True, "has_v6_analyses": False}
    study_new.save()

    ena_sample_old = ena.models.Sample.objects.create(
        accession="SAMEA1", study=ena_study
    )
    mg_sample = Sample.objects.create(
        ena_sample=ena_sample_old,
        ena_study=ena_study,
        ena_accessions=["SAMEA1"],
    )
    mg_sample.studies.add(study_new)

    run = Run.objects.create(
        ena_accessions=["ERR1"],
        study=study_new,
        ena_study=ena_study,
        sample=mg_sample,
    )
    assembly = Assembly.objects.create(
        ena_accessions=["ERZ1"],
        ena_study=ena_study,
        sample=mg_sample,
        reads_study=study_new,
        assembly_study=study_new,
    )
    analysis = Analysis.objects.create(
        study=study_new,
        sample=mg_sample,
        run=run,
        assembly=assembly,
        ena_study=ena_study,
    )
    AssemblyAnalysisBatch.objects.create(
        study=study_new,
        batch_type="assembly_analysis",
        workspace_dir="/tmp/study-batch",
        total_analyses=1,
    )
    publication = Publication.objects.create(
        pubmed_id=123456,
        title="Study publication",
        metadata={},
    )
    StudyPublication.objects.create(study=study_new, publication=publication)
    super_study = SuperStudy.objects.create(
        slug="merged-super-study", title="Super Study"
    )
    SuperStudyStudy.objects.create(study=study_new, super_study=super_study)
    report_path = tmp_path / "applied-report.csv"

    call_command("merge_mgys_duplicates", "--apply", "--output-csv", str(report_path))

    study_old.refresh_from_db()
    mg_sample.refresh_from_db()
    run.refresh_from_db()
    assembly.refresh_from_db()
    analysis.refresh_from_db()

    assert not Study.objects.filter(id=study_new.id).exists()
    assert Study.objects.count() == 2  # there is an extra one - not related to the dups
    assert mg_sample.studies.filter(id=study_old.id).exists()
    assert run.study_id == study_old.id
    assert assembly.reads_study_id == study_old.id
    assert assembly.assembly_study_id == study_old.id
    assert analysis.study_id == study_old.accession
    assert study_old.title == "Preferred title"
    assert study_old.metadata == {"duplicate": "value", "keep": "me"}
    assert study_old.results_dir == "/work/results"
    assert study_old.external_results_dir == "/work/external"
    assert study_old.features.has_prev6_analyses is True
    assert StudyPublication.objects.filter(
        study=study_old, publication=publication
    ).exists()
    assert SuperStudyStudy.objects.filter(
        study=study_old, super_study=super_study
    ).exists()
    with report_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["status"] == "applied"
    assert rows[0]["deleted_studies"] == study_new.accession


@pytest.mark.django_db
def test_merge_mgys_duplicates_accepts_partial_ena_accession_aliases(tmp_path):
    canonical_ena_study = ena.models.Study.objects.create(
        accession="PRJEB20002",
        additional_accessions=["ERP200002"],
        title="Canonical ENA",
    )
    duplicate_ena_study = ena.models.Study.objects.create(
        accession="PRJEB20002A",
        additional_accessions=["ERP200002"],
        title="Duplicate ENA with partial aliases",
    )
    canonical_study = Study.objects.create(
        ena_accessions=["PRJEB20002", "ERP200002"],
        ena_study=canonical_ena_study,
        title="Canonical study",
    )
    duplicate_study = Study.objects.create(
        ena_accessions=["ERP200002"],
        ena_study=duplicate_ena_study,
        title="Duplicate study",
    )

    call_command(
        "merge_mgys_duplicates",
        "--apply",
        "--output-csv",
        str(tmp_path / "partial-alias-report.csv"),
    )

    canonical_study.refresh_from_db()
    assert not Study.objects.filter(id=duplicate_study.id).exists()
    assert sorted(canonical_study.ena_accessions) == ["ERP200002", "PRJEB20002"]


@pytest.mark.django_db
def test_merge_mgys_duplicates_accession_filter_limits_groups(tmp_path, study_pair):
    target_old, target_new, _ = study_pair
    other_ena_study = ena.models.Study.objects.create(
        accession="PRJEB10004",
        additional_accessions=["ERP100004"],
    )
    other_old = Study.objects.create(ena_study=other_ena_study, title="Other old")
    other_new = Study.objects.create(ena_study=other_ena_study, title="Other new")
    other_old.ena_accessions = ["PRJEB10004", "ERP100004"]
    other_new.ena_accessions = ["PRJEB10004", "ERP100004"]
    other_old.save()
    other_new.save()

    call_command(
        "merge_mgys_duplicates",
        "--apply",
        "--accession",
        target_old.first_accession,
        "--output-csv",
        str(tmp_path / "filtered-report.csv"),
    )

    assert Study.objects.filter(id=target_new.id).exists() is False
    assert Study.objects.filter(id=other_new.id).exists() is True


@pytest.mark.django_db
def test_merge_mgys_duplicates_reconciles_overlapping_runs(tmp_path, study_pair):
    study_old, study_new, ena_study = study_pair
    ena_sample = ena.models.Sample.objects.create(
        accession="SAMEA-RUNS", study=ena_study
    )
    mg_sample = Sample.objects.create(
        ena_sample=ena_sample,
        ena_study=ena_study,
        ena_accessions=["SAMEA-RUNS"],
    )
    mg_sample.studies.add(study_old, study_new)

    run_old = Run.objects.create(
        ena_accessions=["ERR-DUPLICATE"],
        study=study_old,
        ena_study=ena_study,
        sample=mg_sample,
    )
    run_new = Run.objects.create(
        ena_accessions=["ERR-DUPLICATE"],
        study=study_new,
        ena_study=ena_study,
        sample=mg_sample,
    )

    call_command(
        "merge_mgys_duplicates",
        "--apply",
        "--output-csv",
        str(tmp_path / "overlapping-runs-report.csv"),
    )

    remaining_runs = list(
        Run.objects.filter(ena_accessions__contains=["ERR-DUPLICATE"])
    )
    assert len(remaining_runs) == 1
    assert remaining_runs[0].id == run_new.id
    assert remaining_runs[0].study_id == study_old.id
    assert Run.objects.filter(id=run_old.id).exists() is False
