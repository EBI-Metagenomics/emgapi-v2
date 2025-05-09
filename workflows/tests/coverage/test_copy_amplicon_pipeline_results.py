from unittest.mock import Mock, patch

import pytest

from workflows.data_io_utils.filenames import trailing_slash_ensured_dir
from workflows.data_io_utils.mgnify_v6_utils.amplicon import EMG_CONFIG
from workflows.flows.analyse_study_tasks.copy_v6_pipeline_results import (
    copy_v6_pipeline_results,
)


@pytest.mark.django_db(transaction=True)
def test_copy_amplicon_pipeline_results(raw_read_analyses):
    """Test copying amplicon pipeline results with a real Analysis fixture"""
    analysis = raw_read_analyses[0]  # Get the first analysis that has results

    # Create a mock that returns a synchronous result
    mock_move_data = Mock(return_value="mock_job_id")

    # Make sure we're patching the correct path
    # You might need to adjust this path based on your actual import structure
    with patch(
        "workflows.flows.analyse_study_tasks.copy_v6_pipeline_results.move_data",
        mock_move_data,
    ):
        # Call the function synchronously using .fn()
        copy_v6_pipeline_results.fn(analysis.accession)

        # Verify move_data was called
        mock_move_data.assert_called_once()

        # Get the arguments that move_data was called with
        call_args = mock_move_data.call_args[0]
        print("calling args")
        print(call_args)

        # Check source path
        expected_source = trailing_slash_ensured_dir(analysis.results_dir)
        assert call_args[0] == expected_source

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
        assert expected_target in call_args[1]

        # Verify command structure
        command: str = call_args[2]

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


@pytest.mark.django_db(transaction=True)
def test_copy_amplicon_pipeline_results_disallowed_extensions(raw_read_analyses):
    """Test that files with disallowed extensions are not included in the copy command"""
    analysis = raw_read_analyses[0]
    mock_move_data = Mock(return_value="mock_job_id")

    with patch(
        "workflows.flows.analyse_study_tasks.copy_v6_pipeline_results.move_data",
        mock_move_data,
    ):
        copy_v6_pipeline_results.fn(analysis.accession)

        # Verify move_data was called
        mock_move_data.assert_called_once()

        # Get the command from the call arguments
        command = mock_move_data.call_args[0][2]

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
