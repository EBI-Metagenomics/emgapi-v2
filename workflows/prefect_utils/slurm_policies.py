from datetime import timedelta
from operator import truth
from typing import Callable, List, Optional, Union

from pydantic import BaseModel, Field

from workflows.prefect_utils.slurm_status import (
    SlurmStatus,
    slurm_status_is_finished_unsuccessfully,
)

ANYTHING = truth


class _SlurmResubmitPolicy(BaseModel):
    policy_name: str = Field(..., description="Pretty name of this policy.")
    given_previous_job_matches: Optional[List[str]] = Field(
        default=["script", "memory_per_node", "working_directory"],
        description="List of fields in SlurmJobSubmitDescription which deem a previous job to be identical to the one under consideration.",
    )
    given_previous_job_submitted_before: Optional[timedelta] = Field(
        timedelta(seconds=0),  # now
        description="If a job was submitted earlier than this timedelta before now, its resubmission will be controlled by this policy.",
    )
    given_previous_job_submitted_after: Optional[timedelta] = Field(
        timedelta(weeks=-520),  # 10 years ago
        description="If a job was submitted later than this timedelta before now, its resubmission will be controlled by this policy.",
    )
    considering_input_file_changes: Optional[bool] = Field(
        default=True,
        description="If True, the policy considers a previously submitted job to match the one under consideration if the input files are unchanged.",
    )
    if_status_matches: Optional[
        Union[List[SlurmStatus], Callable[[SlurmStatus], bool]]
    ] = Field(
        default_factory=lambda: truth,
        description="If a job is submitted and an identical job exists in one of these statues, its resubmission will be controlled by this policy.",
    )
    then_resubmit: bool = Field(
        default=True,
        description="If True, jobs submitting following this policy will be resubmitted even if identical jobs already exist that match the policy criteria.",
    )
    resubmit_needs_preparation_command: Optional[str] = Field(
        default=None,
        description="If set, this command will be run before resubmitting a job.",
        examples=["nextflow clean -f"],
    )


ResubmitIfFailedPolicy = _SlurmResubmitPolicy(
    policy_name="Resubmit if identical job previously failed",
    if_status_matches=slurm_status_is_finished_unsuccessfully,
    then_resubmit=True,
)

ResubmitIfOlderThanAWeekPolicy = _SlurmResubmitPolicy(
    policy_name="Resubmit if identical job was submitted a week or more ago",
    given_previous_job_submitted_before=timedelta(weeks=-1),
    if_status_matches=ANYTHING,
    then_resubmit=True,
)

ResubmitAlwaysPolicy = _SlurmResubmitPolicy(
    policy_name="Resubmit regardless of any previous jobs",
    if_status_matches=ANYTHING,
    then_resubmit=True,
)

NeverResubmitPolicy = _SlurmResubmitPolicy(
    policy_name="Never resubmit if a previous identical job exists",
    if_status_matches=ANYTHING,
    then_resubmit=False,
)

DontResubmitIfOnlyInputFilesChangePolicy = _SlurmResubmitPolicy(
    policy_name="Do not resubmit if the job is identical other than input file changes",
    if_status_matches=ANYTHING,
    considering_input_file_changes=False,
    then_resubmit=False,
)

ResubmitWithCleanedNextflowIfFailedPolicy = _SlurmResubmitPolicy(
    policy_name="Resubmit if identical job previously failed",
    if_status_matches=slurm_status_is_finished_unsuccessfully,
    then_resubmit=True,
    resubmit_needs_preparation_command="nextflow clean -f",
)
