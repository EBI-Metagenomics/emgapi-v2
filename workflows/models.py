import uuid
from pathlib import Path
from typing import List, Optional, Union

from django.contrib.postgres.indexes import GinIndex
from django.db import models
from django.db.models.signals import pre_save
from django.dispatch import receiver
from django.utils.timezone import now
from pydantic import BaseModel, ConfigDict, Field, field_validator

from emgapiv2.model_utils import JSONFieldWithSchema

from .prefect_utils.slurm_policies import _SlurmResubmitPolicy
from .prefect_utils.slurm_status import SlurmStatus
from .signals import ready


class OrchestratedClusterJobManager(models.Manager):
    def filter_similar_to_by_policy(
        self,
        policy: _SlurmResubmitPolicy,
        job: "OrchestratedClusterJob.SlurmJobSubmitDescription",
        input_file_hashes: List["OrchestratedClusterJob.JobInputFile"] = None,
    ):
        """
        Filter for jobs with a similar job description, where similarity is determined
        by the policy's list of field keys in `given_previous_job_matches`.
        :param policy: A SlurmResubmitPolicy which defines the fields for matching.
        :param job: A SlurmJobDescription to search for matches of.
        :param input_file_hashes: A list of InputFileHash objects to match (if required by the policy).
        :return: Filtered queryset.
        """
        filters = {
            "created_at__lte": now() + policy.given_previous_job_submitted_before,
            "created_at__gte": now() + policy.given_previous_job_submitted_after,
        }
        filters.update(
            **{
                f"job_submit_description__{field}": getattr(job, field)
                for field in policy.given_previous_job_matches
            }
        )
        if policy.considering_input_file_changes and input_file_hashes:
            filters["input_files_hashes"] = [
                ifh.model_dump() for ifh in input_file_hashes
            ]

        return self.get_queryset().filter(**filters).order_by("-created_at")

    def get_previous_job(
        self,
        policy: _SlurmResubmitPolicy,
        job: "OrchestratedClusterJob.SlurmJobSubmitDescription",
        input_file_hashes: List["OrchestratedClusterJob.JobInputFile"],
    ):
        similar_past_jobs = self.filter_similar_to_by_policy(
            policy, job, input_file_hashes
        )

        if similar_past_jobs.exists():
            return similar_past_jobs.first()

        return None


class OrchestratedClusterJob(models.Model):
    objects = OrchestratedClusterJobManager()

    class SlurmJobSubmitDescription(BaseModel):
        """
        Pydantic schema for storing a slurm JobSubmitDescription object as json.
        """

        model_config = ConfigDict(extra="allow")

        name: str = Field(..., description="Name of the job")
        script: str = Field(..., description="Content of the job script/command")
        memory_per_node: Optional[Union[str, int]] = Field(
            None, description="Memory required for the job"
        )  # TODO: normalise to a less ambiguous form
        time_limit: Optional[str] = Field(
            None, description="Wall time required for the job in HH:MM:SS format"
        )  # TODO: store as a timedelta-derived type?
        working_directory: Optional[str] = Field(
            None, description="Working directory for the job"
        )

    class JobInputFile(BaseModel):
        """
        Pydantic schema for storing the presence and content hash of an input file for the cluster job.
        Useful for determining if an input file has changed (hash diff).
        """

        path: str = Field(..., description="Path to the input file")
        hash: str = Field(..., description="Hash of the input file contents")
        hash_alg: str = Field(
            "blake2b", description="Hash algorithm used to hash the input file contents"
        )

        @field_validator("path", mode="before")
        def coerce_path(cls, value):
            if isinstance(value, Path):
                return str(value)
            return value

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    cluster_job_id = models.IntegerField(
        db_index=True
    )  # not pk because slurm recycles job IDs beyond a max int
    # N.B. this DOES NOT support array job IDs. Reasonable assumption for now since we do not have logic to launch these.
    flow_run_id = models.UUIDField(db_index=True)
    job_submit_description: SlurmJobSubmitDescription = JSONFieldWithSchema(
        schema=SlurmJobSubmitDescription
    )
    input_files_hashes: List[JobInputFile] = JSONFieldWithSchema(
        schema=JobInputFile, is_list=True, default=list, blank=True
    )

    last_known_state = models.TextField(
        db_index=True,
        choices=SlurmStatus.as_choices(),
        null=True,
        blank=True,
        default=None,
    )
    state_checked_at = models.DateTimeField(null=True, blank=True, default=None)

    nextflow_trace = models.JSONField(default=list, null=True, blank=True)
    cluster_log = models.TextField(null=True, blank=True, default=None)

    @property
    def name(self):
        return self.job_submit_description.name if self.job_submit_description else ""

    class Meta:
        indexes = [
            GinIndex(fields=["job_submit_description"]),
        ]

    def should_resubmit_according_to_policy(self, policy: _SlurmResubmitPolicy) -> bool:
        if type(policy.if_status_matches) is list:
            if self.last_known_state in policy.if_status_matches:
                # previous job not in policy scope due to job state
                return policy.then_resubmit
            else:
                return not policy.then_resubmit
        elif callable(policy.if_status_matches):
            if policy.if_status_matches(
                SlurmStatus.get_member_by_value(self.last_known_state)
            ):
                # previous job not in policy scope due to job state
                return policy.then_resubmit
            else:
                return not policy.then_resubmit

    def __str__(self):
        return f"{self.__class__.__name__} {self.pk} (Slurm: {self.cluster_job_id})"


@receiver(pre_save, sender=OrchestratedClusterJob)
def ensure_orchestrated_cluster_job_has_state(
    sender, instance: OrchestratedClusterJob, **kwargs
):
    if not instance.last_known_state:
        instance.last_known_state = SlurmStatus.unknown


ready()
