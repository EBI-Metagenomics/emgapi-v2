from datetime import timedelta
from pathlib import Path

import pytest
from django.conf import settings
from django.core.management import call_command
from django.utils.timezone import now

from analyses.models import Study, Assembly
import ena.models as ena_models
from workflows.flows.assemble_study_tasks.archive_assembly_dirs import (
    archive_assembly_dirs,
)

EMG_CONFIG = settings.EMG_CONFIG


@pytest.mark.django_db
def test_clean_assembly_workdirs(prefect_harness, mgnify_assemblies_completed, caplog):
    workdir_root = Path(EMG_CONFIG.slurm.default_workdir)

    study: Study = mgnify_assemblies_completed.first().reads_study

    unrelated_ena_study, _ = ena_models.Study.objects.get_or_create(
        accession="PRJunrelated", title="An unrelated study"
    )
    unrelated_study, _ = Study.objects.get_or_create(
        ena_study=unrelated_ena_study, title=unrelated_ena_study.title
    )

    top_level_workdir_for_study = (
        workdir_root / f"{study.ena_study.accession}_miassembler" / "work"
    )
    samplesheet_workdir_for_study = (
        workdir_root
        / f"{study.ena_study.accession}_miassembler"
        / "samplesheet"
        / "work"
    )

    def setup_dirs():
        # make some dummy workdirs
        for assembly in mgnify_assemblies_completed:
            top_level_workdir_for_study.mkdir(exist_ok=True, parents=True)
            (top_level_workdir_for_study / "file.tsv").touch()
            (
                workdir_root
                / f"{study.ena_study.accession}_miassembler"
                / "samplesheet"
                / f"{study.first_accession}"
                / f"{assembly.run.first_accession}"
            ).mkdir(exist_ok=True, parents=True)
            samplesheet_workdir_for_study.mkdir(exist_ok=True, parents=True)
            (samplesheet_workdir_for_study / "file.tsv").touch()
            assembly.dir = str(
                workdir_root
                / f"{study.ena_study.accession}_miassembler"
                / "samplesheet"
                / f"{study.first_accession}"
                / f"{assembly.run.first_accession}"
            )
            assembly.save()

        (workdir_root / "work").mkdir(exist_ok=True, parents=True)
        (workdir_root / "work" / "file.tsv").touch()

        # make some workdirs that should not be touched
        (workdir_root / "PRJunrelated" / "work").mkdir(exist_ok=True, parents=True)
        (workdir_root / "PRJunrelated" / "work" / "file.tsv").touch()

    # call prefect flow for the study to be cleaned
    setup_dirs()

    # should not archive/clear study yet because there are started but not terminal assemblies
    archive_assembly_dirs(study.accession, dry_run=False)
    assert top_level_workdir_for_study.exists()
    assert samplesheet_workdir_for_study.exists()

    # put started assemblies in terminal state
    for assembly in study.assemblies_reads.filter_by_statuses(
        [Assembly.AssemblyStates.ASSEMBLY_STARTED]
    ):
        assembly.mark_status(Assembly.AssemblyStates.ASSEMBLY_UPLOADED)

    archive_assembly_dirs(study.accession, dry_run=False)
    assert not top_level_workdir_for_study.exists()
    assert not samplesheet_workdir_for_study.exists()

    # put started assemblies in non-terminal state
    for assembly in study.assemblies_reads.filter_by_statuses(
        [Assembly.AssemblyStates.ASSEMBLY_STARTED]
    ):
        assembly.mark_status(
            Assembly.AssemblyStates.ASSEMBLY_UPLOADED, set_status_as=False
        )

    first_assembly = mgnify_assemblies_completed.first()
    assert Path(first_assembly.dir).exists()

    # call flow via management command for an e2e test
    setup_dirs()
    caplog.clear()
    with caplog.at_level("INFO"):
        call_command("clear_workdirs", tolerance_days=1)

    ## should not have cleaned anything because a day has not passed
    assert top_level_workdir_for_study.exists()
    assert samplesheet_workdir_for_study.exists()

    # make study + assemblies be updated in the past
    study.updated_at = now() - timedelta(days=3)
    Study.objects.bulk_update([study], ["updated_at"])

    study.assemblies_reads.update(updated_at=now() - timedelta(days=3))

    study.refresh_from_db()

    caplog.clear()
    with caplog.at_level("INFO"):
        call_command("clear_workdirs", tolerance_days=1)

    ## should still not have cleaned, because there were assemblies in non-terminal state
    assert study.assemblies_reads.filter(
        status__assembly_started=True, status__assembly_uploaded=False
    ).exists()
    assert top_level_workdir_for_study.exists()
    assert samplesheet_workdir_for_study.exists()

    # put started assemblies in terminal state
    for assembly in study.assemblies_reads.filter_by_statuses(
        [Assembly.AssemblyStates.ASSEMBLY_STARTED]
    ):
        assembly.mark_status(Assembly.AssemblyStates.ASSEMBLY_UPLOADED)
    study.assemblies_reads.update(updated_at=now() - timedelta(days=3))

    study.refresh_from_db()

    caplog.clear()
    with caplog.at_level("INFO"):
        call_command("clear_workdirs", tolerance_days=1)

    ## should now have cleaned only first study, since unrelated one was edited today
    assert not top_level_workdir_for_study.exists()
    assert not samplesheet_workdir_for_study.exists()

    assert (workdir_root / "PRJunrelated" / "work").exists()
