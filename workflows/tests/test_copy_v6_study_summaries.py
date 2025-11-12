from pathlib import Path
from unittest.mock import patch

import pytest

from analyses.models import Study
from ena.models import Study as ENAStudy
from workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results import (
    copy_v6_study_summaries,
)


@pytest.mark.django_db
class TestCopyV6StudySummaries:
    """Test that study summaries are copied to the correct subdirectory."""

    @patch(
        "workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results.move_data"
    )
    def test_copies_files_to_study_summaries_subdirectory(
        self, mock_move_data, prefect_harness, tmp_path
    ):
        """
        Test that copy_v6_study_summaries copies files from results_dir root
        to external_results/study-summaries/ subdirectory.

        This is to prevent regressions in this behavior as the code is a bit fragile (scatter in different places)
        """
        ena_study = ENAStudy.objects.create(accession="PRJEB12345", title="Test Study")
        study = Study.objects.create(
            ena_study=ena_study,
            title="Test Study",
            results_dir=str(tmp_path / "results"),
        )
        study.inherit_accessions_from_related_ena_object("ena_study")

        # Create results_dir and add study summary files (at root level)
        results_dir = Path(study.results_dir)
        results_dir.mkdir()
        (
            results_dir / f"{study.first_accession}_taxonomy_study_summary.tsv"
        ).write_text("data\n")
        (results_dir / f"{study.first_accession}_ko_study_summary.tsv").write_text(
            "data\n"
        )

        # Go Go Task
        copy_v6_study_summaries(study.accession)

        assert mock_move_data.call_count == 1

        # Verify the target path ends with /study-summaries/
        call_args = mock_move_data.call_args
        target_path = call_args[0][1]  # The second positional arg is the target path
        assert target_path.endswith(
            "/study-summaries/"
        ), f"Expected target to end with /study-summaries/ but got {target_path}"

        study.refresh_from_db()
        assert study.external_results_dir is not None
        assert "study-summaries" not in str(
            study.external_results_dir
        ), "external_results_dir should point to parent, not include study-summaries"

    def test_skips_when_no_results_dir(self, prefect_harness):
        """Test that copy_v6_study_summaries handles missing results_dir gracefully."""

        # I'm not sure if this whole thing is a good idea, as not having a results_dir would be a problem
        # TODO: maybe we should just crash the thing instead of gracefully this

        ena_study = ENAStudy.objects.create(accession="PRJEB99999", title="No Results")
        study = Study.objects.create(
            ena_study=ena_study, title="No Results", results_dir=None
        )
        study.inherit_accessions_from_related_ena_object("ena_study")

        # Should not raise an exception
        copy_v6_study_summaries(study.accession)

        # Verify external_results_dir was not set
        study.refresh_from_db()
        assert study.external_results_dir is None
