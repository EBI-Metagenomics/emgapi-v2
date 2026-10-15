"""Ownership of the protein dataset: the dataset update and the merge write it one at a time."""

from contextlib import contextmanager
from typing import Iterator

from prefect.client.orchestration import get_client
from prefect.client.schemas.responses import GlobalConcurrencyLimitResponse
from prefect.concurrency.sync import concurrency


class OwnershipError(Exception):
    """This flow run cannot safely own the protein dataset."""


@contextmanager
def owner(limit: str) -> Iterator[None]:
    """Holds the slot of the Prefect global concurrency limit that the dataset update and the merge share.

    KNOWN RISK: Prefect frees the slot when it marks a run Crashed (for example, when its heartbeats
    stop), but the SLURM job of that run can still be alive. If that process resumes, it can write
    to the protein dataset at the same time as the next run, and nothing stops it. After a crash,
    check with sacct that the old job has ended before you start the update or the merge again.

    :param limit: The name of the limit.
    :raises OwnershipError: When the limit is missing, inactive, or not 1.
    """
    slot = find_limit(limit)
    if slot is None:
        raise OwnershipError(f"The concurrency limit {limit} does not exist")
    # Prefect does not enforce an inactive limit.
    if not slot.active or slot.limit != 1:
        raise OwnershipError(
            f"The concurrency limit {limit} must be active with a limit of 1,"
            f" not {'active' if slot.active else 'inactive'} with {slot.limit}"
        )
    # strict: a limit deleted since, or a lost lease, is an error rather than a warning.
    with concurrency(limit, occupy=1, strict=True):
        yield


def find_limit(name: str) -> GlobalConcurrencyLimitResponse | None:
    """The Prefect global concurrency limit with this name, if it exists.

    :param name: The limit's name.
    """
    # Read by name, a missing limit is a 404, which emgapi-v2's client settings retry for an hour.
    with get_client(sync_client=True) as client:
        offset = 0
        while page := client.read_global_concurrency_limits(limit=100, offset=offset):
            for limit in page:
                if limit.name == name:
                    return limit
            offset += len(page)
    return None
