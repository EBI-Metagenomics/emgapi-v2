import csv

import pytest
from django.core.management import call_command

from analyses.management.commands.merge_runs_duplicates import Command
from analyses.models import Analysis, Assembly, Run


@pytest.fixture
def run_pair(raw_reads_mgnify_study, raw_reads_mgnify_sample):
    raw_reads_sample, *_ = raw_reads_mgnify_sample
    run_kwargs = {
        "ena_study": raw_reads_mgnify_study.ena_study,
        "study": raw_reads_mgnify_study,
        "sample": raw_reads_sample,
        "ena_accessions": ["ERR100001", "SRR100001"],
    }
    return Run.objects.create(**run_kwargs), Run.objects.create(**run_kwargs)


@pytest.mark.django_db
def test_find_duplicated_runs_returns_matching_pair(run_pair):
    """Finds runs with the same full accession set."""
    run_old, run_new = run_pair

    groups = Command().find_duplicated_runs()

    assert len(groups) == 1
    assert [run.id for run in groups[0]] == [run_old.id, run_new.id]


@pytest.mark.django_db
def test_find_duplicated_runs_returns_overlap_pair(
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
):
    """Finds runs when one accession set overlaps another."""
    raw_reads_sample, *_ = raw_reads_mgnify_sample
    run_old = Run.objects.create(
        ena_study=raw_reads_mgnify_study.ena_study,
        study=raw_reads_mgnify_study,
        sample=raw_reads_sample,
        ena_accessions=["ERR100101", "SRR100101"],
    )
    run_new = Run.objects.create(
        ena_study=raw_reads_mgnify_study.ena_study,
        study=raw_reads_mgnify_study,
        sample=raw_reads_sample,
        ena_accessions=["ERR100101"],
    )
    Run.objects.create(
        ena_study=raw_reads_mgnify_study.ena_study,
        study=raw_reads_mgnify_study,
        sample=raw_reads_sample,
        ena_accessions=["ERR100102", "SRR100102"],
    )

    groups = Command().find_duplicated_runs()

    assert len(groups) == 1
    assert [run.id for run in groups[0]] == [run_old.id, run_new.id]


@pytest.mark.django_db
def test_merge_runs_duplicates_dry_run_leaves_data_unchanged(
    run_pair,
    tmp_path,
):
    """Reports duplicate runs without changing data in dry-run mode."""
    run_old, run_new = run_pair
    report_path = tmp_path / "dry-run-report.csv"

    call_command(
        "merge_runs_duplicates",
        "--output-csv",
        str(report_path),
    )

    assert Run.objects.filter(id=run_old.id).exists()
    assert Run.objects.filter(id=run_new.id).exists()
    with report_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["status"] == "dry_run"
    assert rows[0]["canonical_run"] == (run_old.first_accession or str(run_old.id))


@pytest.mark.django_db
def test_merge_runs_duplicates_rewires_relations(
    run_pair,
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
    tmp_path,
):
    """Reassigns related analyses and assemblies to the canonical run."""
    raw_reads_sample, *_ = raw_reads_mgnify_sample
    run_old, run_new = run_pair

    assembly = Assembly.objects.create(
        ena_study=raw_reads_mgnify_study.ena_study,
        sample=raw_reads_sample,
        reads_study=raw_reads_mgnify_study,
    )
    assembly.runs.add(run_new)

    analysis = Analysis.objects.create(
        study=raw_reads_mgnify_study,
        sample=raw_reads_sample,
        run=run_new,
        ena_study=raw_reads_mgnify_study.ena_study,
    )
    report_path = tmp_path / "applied-report.csv"

    call_command("merge_runs_duplicates", "--apply", "--output-csv", str(report_path))

    analysis.refresh_from_db()
    assembly.refresh_from_db()

    assert not Run.objects.filter(id=run_new.id).exists()
    assert analysis.run_id == run_old.id
    assert assembly.runs.filter(id=run_old.id).exists()
    assert not assembly.runs.filter(id=run_new.id).exists()
    with report_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["status"] == "applied"
    assert rows[0]["deleted_runs"] == (run_new.first_accession or str(run_new.id))
