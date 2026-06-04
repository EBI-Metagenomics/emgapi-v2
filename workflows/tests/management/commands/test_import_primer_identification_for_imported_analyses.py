from io import StringIO
from unittest.mock import patch

import pytest
from django.core.management import call_command

from analyses.models import Analysis, Run


@pytest.fixture
def analyses_for_primer_identification_import(
    raw_reads_mgnify_study, raw_reads_mgnify_sample
):
    analyses = []
    analysis_data = [
        {
            "accession": "MGYA00000001",
            "run_accession": "SRR00000001",
            "experiment_type": Analysis.ExperimentTypes.AMPLICON,
            "annotations_imported": True,
            "results_dir": "/tmp/r1",
        },
        {
            "accession": "MGYA00000002",
            "run_accession": "SRR00000002",
            "experiment_type": Analysis.ExperimentTypes.AMPLICON,
            "annotations_imported": True,
            "results_dir": "/tmp/r2",
        },
        {
            "accession": "MGYA00000003",
            "run_accession": "SRR00000003",
            "experiment_type": Analysis.ExperimentTypes.METAGENOMIC,
            "annotations_imported": True,
            "results_dir": "/tmp/r3",
        },
        {
            "accession": "MGYA00000004",
            "run_accession": "SRR00000004",
            "experiment_type": Analysis.ExperimentTypes.AMPLICON,
            "annotations_imported": False,
            "results_dir": "/tmp/r4",
        },
        {
            "accession": "MGYA00000005",
            "run_accession": "SRR00000005",
            "experiment_type": Analysis.ExperimentTypes.AMPLICON,
            "annotations_imported": True,
            "results_dir": None,
        },
    ]

    for index, analysis_kwargs in enumerate(analysis_data):
        run = Run.objects.create(
            ena_accessions=[analysis_kwargs["run_accession"]],
            study=raw_reads_mgnify_study,
            ena_study=raw_reads_mgnify_study.ena_study,
            sample=raw_reads_mgnify_sample[index % len(raw_reads_mgnify_sample)],
            experiment_type=analysis_kwargs["experiment_type"],
            metadata={
                Run.CommonMetadataKeys.FASTQ_FTPS: [
                    f"ftp://example.org/{analysis_kwargs['run_accession']}.fq"
                ]
            },
        )
        analysis = Analysis.objects.create(
            accession=analysis_kwargs["accession"],
            ena_study=raw_reads_mgnify_study.ena_study,
            study=raw_reads_mgnify_study,
            experiment_type=analysis_kwargs["experiment_type"],
            sample=run.sample,
            run=run,
            results_dir=analysis_kwargs["results_dir"],
        )

        if analysis_kwargs["annotations_imported"]:
            analysis.mark_status(Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED)

        analyses.append(analysis)

    return analyses


@patch(
    "workflows.management.commands.import_primer_identification_for_imported_analyses.import_primer_identification"
)
@pytest.mark.django_db(transaction=True)
def test_command_processes_only_valid_amplicon_analyses_and_logs(
    mock_import_primer_identification, analyses_for_primer_identification_import
):
    stdout = StringIO()
    expected_imported_accessions = [
        analyses_for_primer_identification_import[0].accession,
        analyses_for_primer_identification_import[1].accession,
    ]
    expected_skipped_accession = analyses_for_primer_identification_import[4].accession

    call_command("import_primer_identification_for_imported_analyses", stdout=stdout)

    logs = stdout.getvalue()
    assert "Found 3 amplicon analyses with ANALYSIS_ANNOTATIONS_IMPORTED" in logs
    assert logs.count("primer-identification import done (if files present)") == 2
    assert (
        f"[{expected_skipped_accession}] skipped/failed: "
        "results_dir is not set for this analysis" in logs
    )
    assert "Successes=2, Failures/Skipped=1" in logs
    assert mock_import_primer_identification.call_count == 2
    assert [
        call.args[0].accession
        for call in mock_import_primer_identification.call_args_list
    ] == expected_imported_accessions
    assert all(
        call.kwargs["allow_non_exist"] is True
        for call in mock_import_primer_identification.call_args_list
    )


@patch(
    "workflows.management.commands.import_primer_identification_for_imported_analyses.import_primer_identification"
)
@pytest.mark.django_db(transaction=True)
def test_command_respects_max_count(
    mock_import_primer_identification, analyses_for_primer_identification_import
):
    stdout = StringIO()
    expected_imported_accession = analyses_for_primer_identification_import[0].accession
    excluded_accession = analyses_for_primer_identification_import[1].accession

    call_command(
        "import_primer_identification_for_imported_analyses",
        max_count=1,
        stdout=stdout,
    )

    logs = stdout.getvalue()
    assert "Found 1 amplicon analyses with ANALYSIS_ANNOTATIONS_IMPORTED" in logs
    assert logs.count("primer-identification import done (if files present)") == 1
    assert excluded_accession not in logs
    assert mock_import_primer_identification.call_count == 1
    assert (
        mock_import_primer_identification.call_args_list[0].args[0].accession
        == expected_imported_accession
    )
