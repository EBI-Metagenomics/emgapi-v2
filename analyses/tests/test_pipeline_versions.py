from unittest.mock import Mock

import pytest

from analyses.management.commands.add_pipeline_to_study_downloads_download_groups import (
    Command,
)
from analyses.pipeline_versions import is_v6_family


@pytest.mark.parametrize(
    "version, expected",
    [
        ("V6", True),
        ("v6.2", True),
        ("V6.99", True),
        ("V5", False),
        ("V60", False),
        ("V6x", False),
    ],
)
def test_v6_family_membership(version, expected):
    assert is_v6_family(version) is expected


@pytest.mark.parametrize(
    "versions, expected",
    [
        (["V6", "V6.2", "V6.9", "V6.10", "V60"], "v6.10"),
        ([], "v6"),
    ],
)
def test_infer_amplicon_summary_pipeline_version(versions, expected):
    analyses = Mock()
    analyses.values_list.return_value = versions
    assert Command._infer_pipeline_version(analyses, "amplicon") == expected
