from io import StringIO

import pytest
from django.core.management import call_command

from analyses.models import Analysis, Study
from ena.models import Study as ENAStudy


@pytest.mark.django_db
def test_reconcile_study_samples_links_analysis_samples(
    raw_reads_mgnify_sample,
):
    assembly_ena_study = ENAStudy.objects.create(accession="PRJEB999991")
    assembly_study = Study.objects.create(
        ena_study=assembly_ena_study,
        title="Assembly study",
    )
    study_without_analyses = Study.objects.create(
        ena_study=ENAStudy.objects.create(accession="PRJEB999992"),
        title="Study without analyses",
    )

    for sample in raw_reads_mgnify_sample[:2]:
        Analysis.objects.create(
            study=assembly_study,
            sample=sample,
            ena_study=assembly_ena_study,
            experiment_type=Analysis.ExperimentTypes.ASSEMBLY,
        )

    dry_run_stdout = StringIO()
    call_command("reconcile_study_samples", "--dry-run", stdout=dry_run_stdout)

    assert not assembly_study.samples.exists()
    assert "Would link 2 samples across 1 studies." in dry_run_stdout.getvalue()

    apply_stdout = StringIO()
    call_command("reconcile_study_samples", stdout=apply_stdout)

    assert set(assembly_study.samples.all()) == set(raw_reads_mgnify_sample[:2])
    assert not study_without_analyses.samples.exists()
    assert "Linked 2 samples across 1 studies." in apply_stdout.getvalue()
