from unittest.mock import patch

import pytest
from django.conf import settings
from django.core.management import call_command

from analyses.models import Assembly, Run


@pytest.mark.django_db
def test_repair_semicolon_run_accessions(
    raw_reads_mgnify_study, raw_reads_mgnify_sample
):
    sample = raw_reads_mgnify_sample[0]
    malformed_run = Run.objects.create(
        ena_accessions=["ERR12945506;ERR12954"],
        ena_study=raw_reads_mgnify_study.ena_study,
        study=raw_reads_mgnify_study,
        sample=sample,
    )
    replacement_runs = [
        Run.objects.create(
            ena_accessions=[accession],
            ena_study=raw_reads_mgnify_study.ena_study,
            study=raw_reads_mgnify_study,
            sample=sample,
        )
        for accession in ("ERR12945506", "ERR12954000")
    ]
    assembly = Assembly.objects.create(
        ena_accessions=["ERZ000001"],
        ena_study=raw_reads_mgnify_study.ena_study,
        assembly_study=raw_reads_mgnify_study,
        sample=sample,
    )
    assembly.runs.add(malformed_run)

    with patch(
        "analyses.management.commands.repair_semicolon_run_accessions.get_study_assemblies_from_ena"
    ) as refresh_task:
        refresh = refresh_task.with_options.return_value
        refresh.side_effect = lambda *args, **kwargs: assembly.runs.set(
            replacement_runs
        )
        call_command("repair_semicolon_run_accessions")

    refresh_task.with_options.assert_called_once_with(refresh_cache=True)
    refresh.assert_called_once_with(
        raw_reads_mgnify_study.ena_study.accession,
        limit=settings.EMG_CONFIG.ena.portal_max_readruns_to_fetch,
    )
    assert not Run.objects.filter(id=malformed_run.id).exists()
    assert set(assembly.runs.all()) == set(replacement_runs)
