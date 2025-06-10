from typing import Optional

import ena.models


def validate_and_set_webin_owner(
    ena_study: ena.models.Study, webin_owner: Optional[str]
) -> (ena.models.Study, bool):
    """
    Validate and set the webin owner on the provided ENA study.

    Args:
        ena_study: The ENA study object to set the webin owner on (if valid)
        webin_owner: The webin owner string to validate and set

    Returns:
        The updated ENA study with potentially validated webin owner set, and a boolean indicating whether the webin owner was set

    Raises:
        AssertionError: If the webin owner doesn't start with 'Webin-'
    """
    if webin_owner:
        webin_owner = webin_owner.title()
        assert webin_owner.startswith("Webin-"), "Webin owner must start with 'Webin-'"
        ena_study.webin_submitter = webin_owner
        ena_study.save()  # fyi hooks propagate this to dependent models
        return ena_study, True
    return ena_study, False
