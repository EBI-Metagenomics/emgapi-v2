import pytest
from pydantic import ValidationError

from emgapiv2.config import (
    AmpliconPipelineConfig,
    mgnify_pipeline_version_for_nextflow_revision,
    normalise_mgnify_pipeline_version,
)


@pytest.mark.parametrize(
    "pipeline_version, expected",
    [
        ("v6", "v6"),
        ("V6.0", "v6"),
        ("6.0", "v6"),
        ("v6.1", "v6.1"),
        ("v6.10", "v6.10"),
        ("v10", "v10"),
    ],
)
def test_normalise_mgnify_pipeline_version(pipeline_version, expected):
    assert normalise_mgnify_pipeline_version(pipeline_version) == expected


@pytest.mark.parametrize("pipeline_version", ["", "latest", "v6.x", "v6.1.0"])
def test_normalise_mgnify_pipeline_version_invalid(pipeline_version):
    with pytest.raises(ValueError):
        normalise_mgnify_pipeline_version(pipeline_version)


@pytest.mark.parametrize(
    "git_revision, expected",
    [
        ("v6.0.6", "v6"),
        ("6.0.12", "v6"),
        ("v6.1.3", "v6.1"),
        ("V6.2.10", "v6.2"),
    ],
)
def test_mgnify_pipeline_version_for_nextflow_revision(git_revision, expected):
    assert mgnify_pipeline_version_for_nextflow_revision(git_revision) == expected


@pytest.mark.parametrize("git_revision", ["master", "dev", "v6.1", "abc123"])
def test_mgnify_pipeline_version_for_nextflow_revision_not_a_release_tag(
    git_revision,
):
    with pytest.raises(ValueError):
        mgnify_pipeline_version_for_nextflow_revision(git_revision)


@pytest.mark.parametrize(
    "pipeline_version, git_revision",
    [("v6", "v6.0.6"), ("v6.1", "v6.1.2"), ("v6.2", "v6.2.0")],
)
def test_amplicon_config_accepts_matching_versions(pipeline_version, git_revision):
    AmpliconPipelineConfig(
        pipeline_version=pipeline_version, pipeline_git_revision=git_revision
    )


@pytest.mark.parametrize(
    "pipeline_version, git_revision",
    [
        ("v6", "v6.1.0"),
        ("v6", "v6.2.0"),
        ("v6.1", "v6.0.6"),
        ("v6.1", "v6.2.0"),
        ("v6.2", "v6.1.0"),
        ("v6.1", "dev"),
    ],
)
def test_amplicon_config_rejects_mismatched_versions(pipeline_version, git_revision):
    with pytest.raises(ValidationError):
        AmpliconPipelineConfig(
            pipeline_version=pipeline_version, pipeline_git_revision=git_revision
        )


def test_pipeline_config_normalises_pipeline_version():
    config = AmpliconPipelineConfig(
        pipeline_version="V6.1", pipeline_git_revision="v6.1.2"
    )
    assert config.pipeline_version == "v6.1"
