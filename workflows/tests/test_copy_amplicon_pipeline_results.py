from unittest.mock import Mock, patch

import pytest

from workflows.data_io_utils.filenames import trailing_slash_ensured_dir
from workflows.data_io_utils.mgnify_v6_utils.amplicon import EMG_CONFIG
from workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results import (
    copy_v6_pipeline_results,
)


@pytest.mark.django_db(transaction=True)
def test_copy_amplicon_pipeline_results(raw_read_analyses, prefect_harness):
    """Test copying amplicon pipeline results with a real Analysis fixture"""
    analysis = raw_read_analyses[0]  # Get the first analysis that has results

    # Mock run_deployment instead of move_data
    mock_run_deployment = Mock(return_value=Mock(id="mock-flow-run-id"))

    with patch(
        "workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results.run_deployment",
        mock_run_deployment,
    ):
        # Call the function synchronously
        copy_v6_pipeline_results(analysis.accession)

        # Verify run_deployment was called
        mock_run_deployment.assert_called_once()

        # Get the call arguments
        call_kwargs = mock_run_deployment.call_args.kwargs

        # Verify deployment name
        assert call_kwargs["name"] == "move-data/move_data_deployment"

        # Check parameters
        parameters = call_kwargs["parameters"]

        # Check source path
        expected_source = trailing_slash_ensured_dir(analysis.results_dir)
        assert parameters["source"] == expected_source

        # Check target path structure
        expected_target_parts = [
            EMG_CONFIG.slurm.ftp_results_dir,
            analysis.study.first_accession[:-3],
            analysis.study.first_accession,
            analysis.run.first_accession[:-3],
            analysis.run.first_accession,
            analysis.pipeline_version,
            analysis.experiment_type.lower(),
        ]
        expected_target = "/".join(str(part) for part in expected_target_parts)
        assert expected_target in parameters["target"]

        # Verify command structure
        command: str = parameters["command"]

        # Check basic command structure
        assert "rsync" in command

        # Check all extensions are included
        expected_extensions = {
            "yml",
            "yaml",
            "txt",
            "tsv",
            "mseq",
            "html",
            "fa",
            "json",
            "gz",
            "fasta",
            "csv",
        }
        for ext in expected_extensions:
            assert f"--include=*.{ext}" in command
        assert command.endswith(
            "'--exclude=*'"
        )  # excludes anything not explicitly included

        # Verify job_variables for partition
        job_variables = call_kwargs["job_variables"]
        assert "partition" in job_variables
        assert job_variables["partition"] == EMG_CONFIG.slurm.datamover_paritition


@pytest.mark.django_db(transaction=True)
def test_copy_amplicon_pipeline_results_disallowed_extensions(
    raw_read_analyses, prefect_harness
):
    """Test that files with disallowed extensions are not included in the copy command"""
    analysis = raw_read_analyses[0]

    # Mock run_deployment instead of move_data
    mock_run_deployment = Mock(return_value=Mock(id="mock-flow-run-id"))

    with patch(
        "workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results.run_deployment",
        mock_run_deployment,
    ):
        copy_v6_pipeline_results(analysis.accession)

        # Verify run_deployment was called
        mock_run_deployment.assert_called_once()

        # Get the command from the parameters
        parameters = mock_run_deployment.call_args.kwargs["parameters"]
        command = parameters["command"]

        # List of sample extensions that should NOT be included
        disallowed_extensions = {
            "exe",
            "sh",
            "py",
            "tmp",
            "bak",
            "log",
            "err",
            "out",
            "xlsx",
            "doc",
            "pdf",
            "png",
            "jpg",
            "jpeg",
        }

        # Check that none of the disallowed extensions are in the find command
        for ext in disallowed_extensions:
            assert (
                f"--include='*.{ext}'" not in command
            ), f"Found disallowed extension: {ext}"
