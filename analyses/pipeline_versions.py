"""Shared rules for MGnify pipeline version families."""

import re

V6_FAMILY_PATTERN = r"^v6(\.[0-9]+)*$"


def is_v6_family(pipeline_version: str) -> bool:
    """Check whether a pipeline version belongs to the v6 family.

    :param pipeline_version: MGnify pipeline version.
    :return: Whether the version is V6 or a numeric V6 subversion.
    """
    return re.fullmatch(V6_FAMILY_PATTERN, pipeline_version, re.IGNORECASE) is not None
