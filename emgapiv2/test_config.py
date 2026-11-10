import os
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from emgapiv2.config import (
    AmpliconPipelineConfig,
    AssemblerConfig,
    EMGConfig,
    RawReadsPipelineConfig,
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
        ("v6.1.0", "v6.1"),
        ("v6.10", "v6.10"),
        ("v10", "v10"),
    ],
)
def test_normalise_mgnify_pipeline_version(pipeline_version, expected):
    assert normalise_mgnify_pipeline_version(pipeline_version) == expected


@pytest.mark.parametrize(
    "pipeline_version", ["", "latest", "v6.x", "v6.1.1", "v6.1-rc1"]
)
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


@pytest.mark.parametrize(
    "git_revision", ["master", "dev", "v6.1", "abc123", "v6.1.0-rc1"]
)
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


@pytest.mark.parametrize(
    "config_class, git_revision",
    [
        (RawReadsPipelineConfig, "master"),
        (RawReadsPipelineConfig, "v6.2.0"),
        (AssemblerConfig, "v3.0.3"),
    ],
)
def test_pipeline_config_without_version_check_accepts_any_revision(
    config_class, git_revision
):
    # The flag is off, so the revision is not checked against pipeline_version (v6)
    config = config_class(pipeline_git_revision=git_revision)
    config.check_pipeline_version()


@patch.dict(
    os.environ,
    {
        "EMG_AMPLICON_PIPELINE__PIPELINE_VERSION": "V6.1",
        "EMG_AMPLICON_PIPELINE__PIPELINE_GIT_REVISION": "v6.1.2",
    },
)
def test_emg_config_reads_amplicon_versions_from_env():
    config = EMGConfig()
    assert config.amplicon_pipeline.pipeline_version == "v6.1"
    assert config.amplicon_pipeline.pipeline_git_revision == "v6.1.2"


@patch.dict(
    os.environ,
    {
        "EMG_AMPLICON_PIPELINE__PIPELINE_VERSION": "v6",
        "EMG_AMPLICON_PIPELINE__PIPELINE_GIT_REVISION": "v6.1.2",
    },
)
def test_emg_config_rejects_mismatched_amplicon_versions_from_env():
    with pytest.raises(ValidationError, match="does not match"):
        EMGConfig()


@patch.dict(
    os.environ,
    {
        "EMG_AMPLICON_PIPELINE__PIPELINE_VERSION": "v6",
        "EMG_AMPLICON_PIPELINE__PIPELINE_GIT_REVISION": "dev",
        "EMG_AMPLICON_PIPELINE__GIT_REVISION_DETERMINES_PIPELINE_VERSION": "false",
    },
)
def test_emg_config_version_check_can_be_disabled_from_env():
    config = EMGConfig()
    assert not config.amplicon_pipeline.git_revision_determines_pipeline_version
    assert config.amplicon_pipeline.pipeline_git_revision == "dev"
