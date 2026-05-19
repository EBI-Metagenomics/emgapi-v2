from unittest.mock import patch

import pytest

from analyses.models import Analysis
from workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results import (
    copy_single_analysis_results,
)
from workflows.models import (
    AssemblyAnalysisBatch,
    AssemblyAnalysisBatchAnalysis,
    AssemblyAnalysisPipeline,
    AssemblyAnalysisPipelineStatus,
)


@pytest.mark.django_db
@patch(
    "workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results.copy_schema_directories"
)
def test_copy_single_analysis_results_skips_missing_optional_outputs(
    mock_copy_schema_directories,
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
    mgnify_assemblies,
    tmp_path,
    prefect_harness,
):
    batch = AssemblyAnalysisBatch.objects.create(
        study=raw_reads_mgnify_study,
        workspace_dir=str(tmp_path),
        batch_type="assembly_analysis",
    )
    analysis = Analysis.objects.create(
        study=raw_reads_mgnify_study,
        sample=raw_reads_mgnify_sample[0],
        ena_study=raw_reads_mgnify_study.ena_study,
        assembly=mgnify_assemblies[0],
        pipeline_version="6.0",
    )
    batch_analysis_job = AssemblyAnalysisBatchAnalysis.objects.create(
        batch=batch,
        analysis=analysis,
        asa_status=AssemblyAnalysisPipelineStatus.COMPLETED,
        virify_status=AssemblyAnalysisPipelineStatus.COMPLETED,
        map_status=AssemblyAnalysisPipelineStatus.COMPLETED,
    )
    assembly_accession = analysis.assembly_or_run.first_accession

    asa_source_base = (
        batch.get_pipeline_workspace(AssemblyAnalysisPipeline.ASA) / assembly_accession
    )
    asa_source_base.mkdir(parents=True)

    mock_copy_schema_directories.return_value = (True, [])

    result = copy_single_analysis_results(
        analysis=analysis,
        batch_analysis_job=batch_analysis_job,
        batch=batch,
        destination_root=tmp_path / "ftp",
    )

    assert result.success is True
    assert result.errors == []
    assert mock_copy_schema_directories.call_count == 1
    assert (
        mock_copy_schema_directories.call_args.kwargs["source_base"] == asa_source_base
    )
