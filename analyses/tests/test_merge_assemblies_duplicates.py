import csv

import pytest
from django.core.management import call_command

from analyses.management.commands.merge_assemblies_duplicates import Command
from analyses.models import Analysis, Assembly, Run


@pytest.fixture
def assembly_pair(raw_reads_mgnify_study, raw_reads_mgnify_sample):
    raw_reads_sample, *_ = raw_reads_mgnify_sample
    assembly_kwargs = {
        "ena_study": raw_reads_mgnify_study.ena_study,
        "sample": raw_reads_sample,
        "reads_study": raw_reads_mgnify_study,
        # At the moment of writing this, there are no assemblies with more than one accession in ena_accesions
        # but the code should cope with that anyway.
        "ena_accessions": ["ERZ100001", "GCA100101"],
    }
    return Assembly.objects.create(**assembly_kwargs), Assembly.objects.create(
        **assembly_kwargs
    )


@pytest.mark.django_db
def test_find_duplicated_assemblies_returns_matching_pair(assembly_pair):
    """Finds assemblies with the same full accession set."""
    assembly_old, assembly_new = assembly_pair

    groups = Command().find_duplicated_assemblies()

    assert len(groups) == 1
    assert [assembly.id for assembly in groups[0]] == [
        assembly_old.id,
        assembly_new.id,
    ]


@pytest.mark.django_db
def test_find_duplicated_assemblies_returns_overlap_pair(
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
):
    """Finds assemblies when one accession set overlaps another."""
    raw_reads_sample, *_ = raw_reads_mgnify_sample
    assembly_old = Assembly.objects.create(
        ena_study=raw_reads_mgnify_study.ena_study,
        sample=raw_reads_sample,
        reads_study=raw_reads_mgnify_study,
        ena_accessions=["ERZ100101", "GCA100101"],
    )
    assembly_new = Assembly.objects.create(
        ena_study=raw_reads_mgnify_study.ena_study,
        sample=raw_reads_sample,
        reads_study=raw_reads_mgnify_study,
        ena_accessions=["ERZ100101"],
    )
    Assembly.objects.create(
        ena_study=raw_reads_mgnify_study.ena_study,
        sample=raw_reads_sample,
        reads_study=raw_reads_mgnify_study,
        ena_accessions=["ERZ100102"],
    )

    groups = Command().find_duplicated_assemblies()

    assert len(groups) == 1
    assert [assembly.id for assembly in groups[0]] == [
        assembly_old.id,
        assembly_new.id,
    ]


@pytest.mark.django_db
def test_find_duplicated_assemblies_ignores_assemblies_without_ena_accessions(
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
):
    """Ignores unaccessioned assemblies instead of grouping all empty arrays."""
    raw_reads_sample, *_ = raw_reads_mgnify_sample
    Assembly.objects.create(
        ena_study=raw_reads_mgnify_study.ena_study,
        sample=raw_reads_sample,
        reads_study=raw_reads_mgnify_study,
        ena_accessions=[],
    )
    Assembly.objects.create(
        ena_study=raw_reads_mgnify_study.ena_study,
        sample=raw_reads_sample,
        reads_study=raw_reads_mgnify_study,
        ena_accessions=[],
    )

    groups = Command().find_duplicated_assemblies()

    assert groups == []


@pytest.mark.django_db
def test_merge_assemblies_duplicates_dry_run_leaves_data_unchanged(
    assembly_pair,
    tmp_path,
):
    """Reports duplicate assemblies without changing data in dry-run mode."""
    assembly_old, assembly_new = assembly_pair
    report_path = tmp_path / "dry-run-report.csv"

    call_command(
        "merge_assemblies_duplicates",
        "--dry-run",
        "--output-csv",
        str(report_path),
    )

    assert Assembly.objects.filter(id=assembly_old.id).exists()
    assert Assembly.objects.filter(id=assembly_new.id).exists()
    with report_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["status"] == "dry_run"
    assert rows[0]["canonical_assembly"] == (
        assembly_old.first_accession or str(assembly_old.id)
    )


@pytest.mark.django_db
def test_merge_assemblies_duplicates_rewires_relations_and_merges_fields(
    assembly_pair,
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
    tmp_path,
):
    """Reassigns related analyses and runs to the canonical assembly."""
    raw_reads_sample, *_ = raw_reads_mgnify_sample

    assembly_old, assembly_new = assembly_pair

    assembly_old.metadata = {"keep": "me"}
    assembly_old.status = {"assembly_started": False}
    assembly_old.save()

    assembly_new.metadata = {"duplicate": "value"}
    assembly_new.status = {"assembly_started": True, "assembly_completed": True}
    assembly_new.save()

    run = Run.objects.create(
        ena_study=raw_reads_mgnify_study.ena_study,
        study=raw_reads_mgnify_study,
        sample=raw_reads_sample,
        ena_accessions=["ERR-ASSEMBLY-DUP"],
    )

    assembly_new.runs.add(run)
    analysis = Analysis.objects.create(
        study=raw_reads_mgnify_study,
        sample=raw_reads_sample,
        assembly=assembly_new,
        ena_study=raw_reads_mgnify_study.ena_study,
    )
    report_path = tmp_path / "applied-report.csv"

    call_command("merge_assemblies_duplicates", "--output-csv", str(report_path))

    assembly_old.refresh_from_db()
    analysis.refresh_from_db()

    assert not Assembly.objects.filter(id=assembly_new.id).exists()

    assert analysis.assembly_id == assembly_old.id
    assert assembly_old.runs.filter(id=run.id).exists()

    assert sorted(assembly_old.ena_accessions) == ["ERZ100001", "GCA100101"]

    assert assembly_old.metadata == {"duplicate": "value", "keep": "me"}
    assert assembly_old.status["assembly_started"] is True
    assert assembly_old.status["assembly_completed"] is True

    with report_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["status"] == "applied"
    assert rows[0]["deleted_assemblies"] == (
        assembly_new.first_accession or str(assembly_new.id)
    )


@pytest.mark.django_db
def test_merge_assemblies_duplicates_accession_filter_limits_groups(
    assembly_pair,
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
    tmp_path,
):
    """
    Tests that the command handles accessions filtering correctly.
    """
    target_old, target_new = assembly_pair
    raw_reads_sample, *_ = raw_reads_mgnify_sample
    other_old = Assembly.objects.create(
        ena_study=raw_reads_mgnify_study.ena_study,
        sample=raw_reads_sample,
        reads_study=raw_reads_mgnify_study,
        ena_accessions=["ERZ100201"],
    )
    other_new = Assembly.objects.create(
        ena_study=raw_reads_mgnify_study.ena_study,
        sample=raw_reads_sample,
        reads_study=raw_reads_mgnify_study,
        ena_accessions=["ERZ100201"],
    )

    call_command(
        "merge_assemblies_duplicates",
        "--accession",
        target_old.first_accession,
        "--output-csv",
        str(tmp_path / "filtered-report.csv"),
    )

    assert Assembly.objects.filter(id=target_new.id).exists() is False
    assert Assembly.objects.filter(id=other_old.id).exists() is True
    assert Assembly.objects.filter(id=other_new.id).exists() is True
