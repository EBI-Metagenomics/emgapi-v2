from unittest.mock import patch

import pytest

from analyses.models import Analysis
from workflows.flows.analysis.pipeline_versions import (
    get_current_pipeline_version_for_experiment_type,
)

AMPLICON_CONFIG = (
    "workflows.flows.analysis.pipeline_versions.EMG_CONFIG.amplicon_pipeline"
)


@patch(f"{AMPLICON_CONFIG}.pipeline_git_revision", "v6.1.4")
@patch(f"{AMPLICON_CONFIG}.pipeline_version", "v6.1")
def test_get_current_amplicon_pipeline_version_matching_revision():
    assert (
        get_current_pipeline_version_for_experiment_type(
            Analysis.ExperimentTypes.AMPLICON
        )
        == Analysis.PipelineVersions.v6_1
    )


@patch(f"{AMPLICON_CONFIG}.pipeline_git_revision", "v6.2.0")
@patch(f"{AMPLICON_CONFIG}.pipeline_version", "v6")
def test_get_current_amplicon_pipeline_version_mismatched_revision():
    # Config is mutable at runtime, so the guard must not rely only on config validation
    with pytest.raises(ValueError, match="does not match"):
        get_current_pipeline_version_for_experiment_type(
            Analysis.ExperimentTypes.AMPLICON
        )
