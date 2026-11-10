from unittest.mock import patch

import pytest

from analyses.models import Analysis
from workflows.flows.analysis.pipeline_versions import (
    get_current_pipeline_version_for_experiment_type,
)

AMPLICON_CONFIG = (
    "workflows.flows.analysis.pipeline_versions.EMG_CONFIG.amplicon_pipeline"
)


@pytest.mark.parametrize(
    "pipeline_version, git_revision, expected",
    [
        ("v6.1", "v6.1.4", Analysis.PipelineVersions.v6_1),
        # Values set at runtime skip the config validator, so are not yet normalised
        ("V6.1", "v6.1.4", Analysis.PipelineVersions.v6_1),
        ("v6.0", "v6.0.6", Analysis.PipelineVersions.v6),
    ],
)
def test_get_current_amplicon_pipeline_version_matching_revision(
    pipeline_version, git_revision, expected
):
    with (
        patch(f"{AMPLICON_CONFIG}.pipeline_version", pipeline_version),
        patch(f"{AMPLICON_CONFIG}.pipeline_git_revision", git_revision),
    ):
        assert (
            get_current_pipeline_version_for_experiment_type(
                Analysis.ExperimentTypes.AMPLICON
            )
            == expected
        )


@patch(f"{AMPLICON_CONFIG}.pipeline_git_revision", "v6.2.0")
@patch(f"{AMPLICON_CONFIG}.pipeline_version", "v6")
def test_get_current_amplicon_pipeline_version_mismatched_revision():
    # Config is mutable at runtime, so the guard must not rely only on config validation
    with pytest.raises(ValueError, match="does not match"):
        get_current_pipeline_version_for_experiment_type(
            Analysis.ExperimentTypes.AMPLICON
        )
