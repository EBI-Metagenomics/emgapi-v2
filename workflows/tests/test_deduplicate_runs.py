import json

import pytest
from prefect.artifacts import Artifact

from analyses.models import Analysis, Assembly, Run
from workflows.flows.housekeeping.deduplicate_runs import (
    deduplicate_runs,
    find_duplicated_runs,
)


def _create_run_pair(raw_reads_mgnify_study, raw_reads_mgnify_sample):
    run_old = Run.objects.create(
        ena_study=raw_reads_mgnify_study.ena_study,
        study=raw_reads_mgnify_study,
        sample=raw_reads_mgnify_sample[0],
        ena_accessions=["ERR000001", "SRR000001"],
    )
    run_new = Run.objects.create(
        ena_study=raw_reads_mgnify_study.ena_study,
        study=raw_reads_mgnify_study,
        sample=raw_reads_mgnify_sample[0],
        ena_accessions=["ERR000001", "SRR000001"],
    )
    return run_old, run_new


@pytest.mark.django_db
def test_find_duplicated_runs_returns_matching_pair(
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
):
    run_old, run_new = _create_run_pair(
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
    )

    groups = find_duplicated_runs()

    assert len(groups) == 1
    assert [run.id for run in groups[0]] == [run_old.id, run_new.id]


@pytest.mark.django_db
def test_find_duplicated_runs_returns_overlap_pair(
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
):
    run_old = Run.objects.create(
        ena_study=raw_reads_mgnify_study.ena_study,
        study=raw_reads_mgnify_study,
        sample=raw_reads_mgnify_sample[0],
        ena_accessions=["ERR000101", "SRR000101"],
    )
    run_new = Run.objects.create(
        ena_study=raw_reads_mgnify_study.ena_study,
        study=raw_reads_mgnify_study,
        sample=raw_reads_mgnify_sample[0],
        ena_accessions=["ERR000101"],
    )

    groups = find_duplicated_runs()

    assert len(groups) == 1
    assert [run.id for run in groups[0]] == [run_old.id, run_new.id]


@pytest.mark.django_db
def test_deduplicate_runs_dry_run_leaves_data_unchanged(
    prefect_harness,
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
):
    run_old, run_new = _create_run_pair(
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
    )

    results = deduplicate_runs(dry_run=True)

    assert Run.objects.filter(id=run_old.id).exists()
    assert Run.objects.filter(id=run_new.id).exists()
    artifact = Artifact.get("run-deduplication-summary")
    assert artifact.type == "table"
    assert len(json.loads(artifact.data)) == len(results) == 1


@pytest.mark.django_db
def test_deduplicate_runs_rewires_relations(
    prefect_harness,
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
):
    run_old, run_new = _create_run_pair(
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
    )

    assembly = Assembly.objects.create(
        ena_study=raw_reads_mgnify_study.ena_study,
        sample=raw_reads_mgnify_sample[0],
        reads_study=raw_reads_mgnify_study,
    )
    assembly.runs.add(run_new)

    analysis = Analysis.objects.create(
        study=raw_reads_mgnify_study,
        sample=raw_reads_mgnify_sample[0],
        run=run_new,
        ena_study=raw_reads_mgnify_study.ena_study,
    )

    deduplicate_runs(dry_run=False)

    analysis.refresh_from_db()
    assembly.refresh_from_db()

    assert not Run.objects.filter(id=run_new.id).exists()
    assert analysis.run_id == run_old.id
    assert assembly.runs.filter(id=run_old.id).exists()
    assert not assembly.runs.filter(id=run_new.id).exists()
