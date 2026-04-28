from unittest.mock import patch, Mock

import pytest

from analyses.models import Study
from ena.models import Study as ENAStudy
from workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results import (
    copy_v6_study_summaries,
)
from workflows.flows.analysis import AnalysisType


@pytest.mark.django_db
class TestCopyV6StudySummaries:
    """Test that study summaries are copied from the correct pipeline subdirectory."""

    @pytest.mark.parametrize(
        "analysis_type,expected_subdir",
        [
            (AnalysisType.AMPLICON, "amplicon_v6"),
            (AnalysisType.RAWREADS, "rawreads_v6"),
            (AnalysisType.ASSEMBLY, "asa_v6"),
        ],
    )
    @patch(
        "workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results.run_deployment"
    )
    def test_copies_from_pipeline_subdir_to_study_summaries(
        self,
        mock_run_deployment,
        analysis_type,
        expected_subdir,
        prefect_harness,
        tmp_path,
    ):
        """
        Test that copy_v6_study_summaries sources files from the pipeline-specific
        subdirectory (e.g. amplicon_v6/) rather than the study results_dir root.

        This matches the directory layout produced by merge_study_summaries.
        """
        mock_run_deployment.return_value = Mock(id="mock-flow-run-id")

        ena_study = ENAStudy.objects.create(accession="PRJEB12345", title="Test Study")
        study = Study.objects.create(
            ena_study=ena_study,
            title="Test Study",
            results_dir=str(tmp_path / "results"),
        )
        study.inherit_accessions_from_related_ena_object("ena_study")

        # Create the pipeline subdir with summary files,
        # matching the structure produced by merge_study_summaries
        pipeline_dir = study.results_dir_path / expected_subdir
        pipeline_dir.mkdir(parents=True)
        (
            pipeline_dir / f"{study.first_accession}_taxonomy_study_summary.tsv"
        ).write_text("data\n")

        copy_v6_study_summaries(study.accession, analysis_type=analysis_type)

        assert mock_run_deployment.call_count == 1

        call_kwargs = mock_run_deployment.call_args.kwargs
        assert call_kwargs["name"] == "move-data/move_data_deployment"

        parameters = call_kwargs["parameters"]

        # Verify the source points to the pipeline subdir, not the results_dir root
        source_path = parameters["source"]
        assert source_path.rstrip("/") == str(
            pipeline_dir
        ), f"Expected source to be {pipeline_dir} but got {source_path}"

        # Verify the target path ends with /study-summaries/
        target_path = parameters["target"]
        assert target_path.endswith(
            "/study-summaries/"
        ), f"Expected target to end with /study-summaries/ but got {target_path}"

        assert call_kwargs["timeout"] == 14400

        study.refresh_from_db()
        assert study.external_results_dir is not None
        assert "study-summaries" not in str(
            study.external_results_dir
        ), "external_results_dir should point to parent, not include study-summaries"

    @patch(
        "workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results.run_deployment"
    )
    def test_skips_when_no_results_dir(self, mock_run_deployment, prefect_harness):
        """Test that copy_v6_study_summaries handles missing results_dir gracefully."""

        # I'm not sure if this whole thing is a good idea, as not having a results_dir would be a problem
        # TODO: maybe we should just crash the thing instead of gracefully this

        mock_run_deployment.return_value = Mock(id="mock-flow-run-id")

        ena_study = ENAStudy.objects.create(accession="PRJEB99999", title="No Results")
        study = Study.objects.create(
            ena_study=ena_study, title="No Results", results_dir=None
        )
        study.inherit_accessions_from_related_ena_object("ena_study")

        copy_v6_study_summaries(study.accession, analysis_type=AnalysisType.ASSEMBLY)

        mock_run_deployment.assert_not_called()

        study.refresh_from_db()
        assert study.external_results_dir is None

    @patch(
        "workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results.run_deployment"
    )
    def test_amplicon_uses_configured_pipeline_version_subdir(
        self, mock_run_deployment, prefect_harness, tmp_path, monkeypatch
    ):
        mock_run_deployment.return_value = Mock(id="mock-flow-run-id")
        monkeypatch.setattr(
            "workflows.flows.analyse_study_tasks.shared.study_summary.EMG_CONFIG.amplicon_pipeline.pipeline_version",
            "v6.1",
        )

        ena_study = ENAStudy.objects.create(accession="PRJEB12346", title="Test Study")
        study = Study.objects.create(
            ena_study=ena_study,
            title="Test Study",
            results_dir=str(tmp_path / "results"),
        )
        study.inherit_accessions_from_related_ena_object("ena_study")

        pipeline_dir = study.results_dir_path / "amplicon_v6.1"
        pipeline_dir.mkdir(parents=True)
        (
            pipeline_dir / f"{study.first_accession}_taxonomy_study_summary.tsv"
        ).write_text("data\n")

        copy_v6_study_summaries(study.accession, analysis_type=AnalysisType.AMPLICON)

        source_path = mock_run_deployment.call_args.kwargs["parameters"]["source"]
        assert source_path.rstrip("/") == str(pipeline_dir)
