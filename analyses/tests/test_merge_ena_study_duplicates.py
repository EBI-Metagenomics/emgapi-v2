import csv

import pytest
from django.core.management import call_command

import ena.models
from analyses.management.commands.merge_ena_study_duplicates import Command
from analyses.models import Analysis, Assembly, Run, Sample, Study


@pytest.fixture
def duplicated_ena_study_pair():
    canonical_ena_study = ena.models.Study.objects.create(
        accession="ERP130001",
        additional_accessions=["PRJEB13001"],
        title="Canonical ENA",
        metadata={"keep": "me"},
    )
    duplicate_ena_study = ena.models.Study.objects.create(
        accession="PRJEB13001",
        additional_accessions=["ERP130001"],
        title="Duplicate ENA",
        metadata={"duplicate": "value"},
        is_private=True,
        webin_submitter="Webin-1",
    )
    return canonical_ena_study, duplicate_ena_study


@pytest.mark.django_db
def test_find_duplicated_ena_studies_returns_matching_pair(
    duplicated_ena_study_pair,
):
    canonical_ena_study, duplicate_ena_study = duplicated_ena_study_pair

    groups = Command().find_duplicated_ena_studies()

    assert len(groups) == 1
    assert {study.accession for study in groups[0]} == {
        canonical_ena_study.accession,
        duplicate_ena_study.accession,
    }


@pytest.mark.django_db
def test_merge_ena_study_duplicates_dry_run_leaves_data_unchanged(
    duplicated_ena_study_pair,
    tmp_path,
):
    canonical_ena_study, duplicate_ena_study = duplicated_ena_study_pair
    report_path = tmp_path / "dry-run-report.csv"

    call_command(
        "merge_ena_study_duplicates",
        "--dry-run",
        "--output-csv",
        str(report_path),
    )

    canonical_ena_study.refresh_from_db()
    duplicate_ena_study.refresh_from_db()
    assert canonical_ena_study.additional_accessions == ["PRJEB13001"]
    assert duplicate_ena_study.additional_accessions == ["ERP130001"]
    with report_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["status"] == "dry_run"
    assert rows[0]["canonical_ena_study"] == canonical_ena_study.accession


@pytest.mark.django_db
def test_merge_ena_study_duplicates_rewires_relations_and_deletes_duplicate(
    duplicated_ena_study_pair,
    tmp_path,
):
    canonical_ena_study, duplicate_ena_study = duplicated_ena_study_pair
    mg_study = Study.objects.create(
        ena_study=duplicate_ena_study,
        ena_accessions=["PRJEB13001"],
        title="Duplicate MGYS",
    )
    duplicate_ena_sample = ena.models.Sample.objects.create(
        accession="SAMEA-ENA-DUP",
        study=duplicate_ena_study,
    )
    mg_sample = Sample.objects.create(
        ena_sample=duplicate_ena_sample,
        ena_study=duplicate_ena_study,
        ena_accessions=["SAMEA-ENA-DUP"],
    )
    mg_sample.studies.add(mg_study)
    run = Run.objects.create(
        ena_accessions=["ERR-ENA-DUP"],
        study=mg_study,
        ena_study=duplicate_ena_study,
        sample=mg_sample,
    )
    assembly = Assembly.objects.create(
        ena_accessions=["ERZ-ENA-DUP"],
        ena_study=duplicate_ena_study,
        sample=mg_sample,
        reads_study=mg_study,
        assembly_study=mg_study,
    )
    analysis = Analysis.objects.create(
        study=mg_study,
        sample=mg_sample,
        run=run,
        assembly=assembly,
        ena_study=duplicate_ena_study,
    )
    report_path = tmp_path / "applied-report.csv"

    call_command("merge_ena_study_duplicates", "--output-csv", str(report_path))

    canonical_ena_study.refresh_from_db()
    mg_study.refresh_from_db()
    mg_sample.refresh_from_db()
    run.refresh_from_db()
    assembly.refresh_from_db()
    analysis.refresh_from_db()
    duplicate_ena_sample.refresh_from_db()

    assert not ena.models.Study.objects.filter(
        accession=duplicate_ena_study.accession
    ).exists()
    assert canonical_ena_study.additional_accessions == ["PRJEB13001"]
    assert canonical_ena_study.metadata == {"keep": "me"}
    assert canonical_ena_study.is_private is False
    assert canonical_ena_study.webin_submitter is None
    assert mg_study.ena_study_id == canonical_ena_study.accession
    assert set(mg_study.ena_accessions) == {"ERP130001", "PRJEB13001"}
    assert mg_study.is_private is False
    assert mg_sample.ena_study_id == canonical_ena_study.accession
    assert mg_sample.is_private is False
    assert run.ena_study_id == canonical_ena_study.accession
    assert run.is_private is False
    assert assembly.ena_study_id == canonical_ena_study.accession
    assert assembly.is_private is False
    assert analysis.ena_study_id == canonical_ena_study.accession
    assert analysis.is_private is False
    assert duplicate_ena_sample.study_id == canonical_ena_study.accession
    with report_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["status"] == "applied"
    assert rows[0]["deleted_ena_studies"] == duplicate_ena_study.accession
