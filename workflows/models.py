import logging
import uuid
from pathlib import Path
from typing import List, Optional, Union

from django.conf import settings
from django.contrib.postgres.indexes import GinIndex
from django.db import models
from django.db.models.signals import pre_save
from django.dispatch import receiver
from django.utils.timezone import now
from pydantic import BaseModel, ConfigDict, Field, field_validator

from analyses.models import TimeStampedModel, Analysis, Study
from emgapiv2.model_utils import JSONFieldWithSchema
from .prefect_utils.slurm_policies import _SlurmResubmitPolicy
from .prefect_utils.slurm_status import SlurmStatus
from .signals import ready

logger = logging.getLogger(__name__)


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


#############################################################
## == The following models are for the Assembly Analysis == #
#############################################################


class StudyAnalysisBatch(TimeStampedModel):
    """
    Base model for batches of analyses processed together.
    Tracks pipeline milestones - detailed execution state is in Prefect.

    Note: This is kept as a concrete model (not abstract) for database compatibility.
    Do not instantiate directly - use specific batch types instead. For example:
    - AssemblyAnalysisBatch (ASA -> VIRify -> MAP)
    - AmpliconAnalysisBatch (planned)
    - RawReadsAnalysisBatch (planned)
    """

    class BatchType(models.TextChoices):
        """Batch types for different analysis workflows."""

        ASSEMBLY_ANALYSIS = "assembly_analysis", "Assembly Analysis"
        # The following 2 are not implemented yet
        AMPLICON_ANALYSIS = "amplicon_analysis", "Amplicon Analysis"
        RAWREADS_ANALYSIS = "rawreads_analysis", "Raw Reads Analysis"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    study = models.ForeignKey(
        Study, on_delete=models.CASCADE, related_name="analysis_batches"
    )
    batch_type = models.CharField(max_length=50, choices=BatchType.choices)
    workspace_dir = models.CharField(
        max_length=500, help_text="This is the base workspace root dir for this batch"
    )
    total_analyses = models.IntegerField(default=0)

    prefect_flow_run_id = models.CharField(max_length=100, null=True, blank=True)

    class Meta:
        abstract = True

    def __str__(self):
        return f"{self.batch_type.title()}Batch {str(self.id)[:8]}: {self.total_analyses} analyses"

    def get_pipeline_workspace(self, pipeline_name: str) -> Path:
        """Get a workspace directory for a pipeline."""
        return Path(self.workspace_dir) / pipeline_name


class AssemblyAnalysisPipeline(models.TextChoices):
    """Assembly analysis pipelines."""

    ASA = "asa", "Assembly Analysis"
    VIRIFY = "virify", "VIRify"
    MAP = "map", "MAP"


class AssemblyAnalysisPipelineStatus(models.TextChoices):
    """
    Pipeline execution states (synced from Prefect).
    Maps Prefect flow states to batch-level pipeline states.
    """

    PENDING = "pending", "Pending"
    RUNNING = "running", "Running"
    COMPLETED = "completed", "Completed"
    FAILED = "failed", "Failed"


class AssemblyAnalysisPipelineBaseModel(models.Model):
    """Base models with the basic fields to track the Assembly Analysis pipelines"""

    asa_status = models.CharField(
        max_length=20,
        choices=AssemblyAnalysisPipelineStatus.choices,
        default=AssemblyAnalysisPipelineStatus.PENDING,
        help_text="Assembly Analysis status for the batch",
    )

    virify_status = models.CharField(
        max_length=20,
        choices=AssemblyAnalysisPipelineStatus.choices,
        default=AssemblyAnalysisPipelineStatus.PENDING,
        help_text="VIRIfy status for the batch",
    )
    map_status = models.CharField(
        max_length=20,
        choices=AssemblyAnalysisPipelineStatus.choices,
        default=AssemblyAnalysisPipelineStatus.PENDING,
        help_text="Mobilome Annotation Pipeline status for the batch",
    )

    class Meta:
        abstract = True


class AssemblyAnalysisBatchAnalysisManager(models.Manager):
    """Manager for AssemblyAnalysisBatchAnalysis that filters out disabled records by default."""

    def get_queryset(self):
        """
        Return queryset excluding disabled batch assembly analyses.

        :return: QuerySet of enabled AssemblyAnalysisBatchAnalysis records
        :rtype: QuerySet
        """
        return super().get_queryset().filter(disabled=False)


class AssemblyAnalysisBatchAnalysis(
    TimeStampedModel, AssemblyAnalysisPipelineBaseModel
):
    """
    Through table for the ManyToMany relationship between AssemblyAnalysisBatch and Analysis.

    This explicit through model allows for:
    - Admin interface customization
    - Additional metadata about the batch assembly analysis relationship
    - Ordering of analyses within a batch
    """

    objects = AssemblyAnalysisBatchAnalysisManager()
    all_objects = models.Manager()  # Unfiltered manager for admin/debugging

    batch = models.ForeignKey(
        "AssemblyAnalysisBatch",
        on_delete=models.CASCADE,
        related_name="batch_analyses",
    )
    analysis = models.ForeignKey(
        Analysis,
        on_delete=models.CASCADE,
        related_name="analysis_batches",
    )
    order = models.PositiveIntegerField(
        default=0, help_text="Order of this analysis within the batch"
    )
    disabled = models.BooleanField(
        default=False,
        help_text="Whether this analysis should be excluded from the batch",
    )
    disabled_reason = models.TextField(
        null=True, blank=True, help_text="Reason for disabling this analysis"
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Batch Assembly Analysis"
        verbose_name_plural = "Batch Assembly Analyses"
        ordering = ["order", "created_at"]
        unique_together = [["batch", "analysis"]]
        indexes = [
            models.Index(fields=["batch", "order"]),
        ]

    def __str__(self):
        return f"{self.batch.id} - {self.analysis.accession}"


class AssemblyAnalysisBatchManager(models.Manager):

    def get_or_create_batches_for_study(
        self,
        study: "Study",
        pipeline: Analysis.PipelineVersions = None,
        chunk_size: int = None,
        max_analyses: int = None,
        workspace_dir: Path = None,
        skip_completed: bool = True,
    ) -> list["AssemblyAnalysisBatch"]:
        """
        Create batches for all pending analyses in a study.

        This is the primary entry point for batch creation. It handles:
        - Querying analyses from the study (to be used as the source of truth)
        - Filtering out already-completed analyses
        - Validating analyses (warns about blocked/failed)
        - Enforcing maximum analysis limits
        - Chunking analyses into manageable batch sizes
        - Creating batches

        :param study: The study
        :param pipeline: Pipeline version filter (default: v6)
        :param chunk_size: Maximum analyses per batch (default: from config)
        :param max_analyses: Maximum total analyses to process (safety cap)
        :param workspace_dir: The workspace directory (default: working dir, each pipeline will have its own subdir)
        :param skip_completed: Skip already-completed analyses (default True)
        :return: List of created batches
        :raises ValueError: If no valid analyses remain after filtering
        """
        # Apply defaults
        if pipeline is None:
            pipeline = Analysis.PipelineVersions.v6

        if chunk_size is None:
            chunk_size = (
                settings.EMG_CONFIG.assembly_analysis_pipeline.samplesheet_chunk_size
            )

        # Query analyses from study (single source of truth)
        analyses_qs = study.analyses.filter(pipeline_version=pipeline)

        # Filter out completed analyses
        if skip_completed:
            analyses_qs = analyses_qs.exclude_by_statuses(
                [Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED]
            )

        # Validate analyses - warns about blocked/failed but doesn't fail
        blocked_or_failed = analyses_qs.filter_by_statuses(
            [
                Analysis.AnalysisStates.ANALYSIS_BLOCKED,
                Analysis.AnalysisStates.ANALYSIS_QC_FAILED,
            ]
        )

        if blocked_or_failed.exists():
            blocked_accessions = list(
                blocked_or_failed.values_list("accession", flat=True)
            )
            logger.warning(
                f"Including {len(blocked_accessions)} blocked/qc-failed analyses in batches: "
                f"{', '.join(blocked_accessions[:5])}{'...' if len(blocked_accessions) > 5 else ''}"
            )

        analysis_ids = list(analyses_qs.values_list("id", flat=True))

        if not analysis_ids:
            return []
            # TODO: should this be an error?
            # raise ValueError(f"No pending analyses found for study {study.accession}")

        # Apply safety cap if specified
        if max_analyses and len(analysis_ids) > max_analyses:
            logger.warning(
                f"Study {study.accession} has {len(analysis_ids)} analyses, "
                f"but max_analyses={max_analyses}. Processing first {max_analyses} only."
            )
            analysis_ids = analysis_ids[:max_analyses]

        logger.info(
            f"Creating batches for {len(analysis_ids)} analyses in study {study.accession} "
            f"(chunk_size={chunk_size})"
        )

        # Chunk analyses
        chunks = [
            analysis_ids[i : i + chunk_size]
            for i in range(0, len(analysis_ids), chunk_size)
        ]

        logger.info(f"Creating {len(chunks)} batches")

        # Create batches for each chunk
        batches = []
        for chunk in chunks:
            batch = self._create_for_analyses(
                study=study, analysis_ids=chunk, workspace_dir=workspace_dir
            )
            batches.append(batch)

        return batches

    def _create_for_analyses(
        self, study: "Study", analysis_ids: list[int], workspace_dir: Path = None
    ) -> "AssemblyAnalysisBatch":
        """
        Internal method to create a batch for a list of analysis IDs.

        This is a simple, "dumb" method that just creates the batch object
        without validation. All validations should be done in create_batches_for_study.

        :param study: The study
        :param analysis_ids: List of analysis IDs
        :param workspace_dir: Optional base workspace directory
        :return: Created batch
        """
        logger.info(f"Creating batch for {len(analysis_ids)} analyses")

        batch, _ = AssemblyAnalysisBatch.objects.get_or_create(
            study=study,
            batch_type=StudyAnalysisBatch.BatchType.ASSEMBLY_ANALYSIS,
        )
        # Save to generate the id (which is a UUID)
        batch.save()

        # It would be also possible to move this into the save method (but I rather not... I think).
        dir_name = f"assembly_v6_batch_{str(batch.id)[:8]}"

        if workspace_dir:
            results_dir = Path(
                f"{workspace_dir}/{study.ena_study.accession}/{dir_name}"
            )
        else:
            results_dir = Path(
                f"{settings.EMG_CONFIG.slurm.default_workdir}/{study.ena_study.accession}/{dir_name}"
            )

        results_dir.mkdir(parents=True, exist_ok=True)

        # Update the batch with actual workspace_dir
        batch.workspace_dir = str(results_dir)
        batch.total_analyses = len(analysis_ids)
        batch.save()

        logger.info(f"Created batch {batch.id} for {len(analysis_ids)} analyses")

        # Link analysis to batch (bulk update for efficiency)
        AssemblyAnalysisBatchAnalysis.objects.bulk_create(
            [
                AssemblyAnalysisBatchAnalysis(analysis_id=aid, batch=batch)
                for aid in analysis_ids
            ]
        )

        logger.info(f"Linked {len(analysis_ids)} analyses to batch {batch.id}")

        return batch


class AssemblyAnalysisBatch(StudyAnalysisBatch, AssemblyAnalysisPipelineBaseModel):
    """Batch for assembly analyses (ASA -> VIRify -> MAP)."""

    objects = AssemblyAnalysisBatchManager()

    analyses = models.ManyToManyField(
        Analysis,
        through="AssemblyAnalysisBatchAnalysis",
        related_name="assembly_analysis_batches",
        blank=True,
    )

    # Samplesheets
    asa_samplesheet_path = models.CharField(max_length=500, null=True, blank=True)
    virify_samplesheet_path = models.CharField(max_length=500, null=True, blank=True)
    map_samplesheet_path = models.CharField(max_length=500, null=True, blank=True)

    # Prefect flow run IDs
    asa_flow_run_id = models.CharField(
        max_length=100, null=True, blank=True, db_index=True
    )
    virify_flow_run_id = models.CharField(
        max_length=100, null=True, blank=True, db_index=True
    )
    map_flow_run_id = models.CharField(
        max_length=100, null=True, blank=True, db_index=True
    )

    # Error tracking
    last_error = models.TextField(null=True, blank=True)

    # Pipeline versions
    pipeline_versions = models.JSONField(default=dict, blank=True)

    class Meta:
        verbose_name = "Assembly Analysis Batch"
        verbose_name_plural = "Assembly Analysis Batches"
        indexes = [
            models.Index(fields=["asa_status"]),
            models.Index(fields=["virify_status"]),
            models.Index(fields=["map_status"]),
        ]

    def __str__(self):
        return f"Assembly_AnalysisBatch {self.id}: {self.total_analyses} analyses"

    def set_pipeline_status(
        self,
        pipeline_type: AssemblyAnalysisPipeline,
        status: AssemblyAnalysisPipelineStatus,
    ) -> "AssemblyAnalysisBatch":
        """
        Update the pipeline status for a batch and optionally propagate to batch-analysis relations.

        This method:
        1. Updates the batch-level pipeline status field (asa_status, virify_status, map_status)
        2. Validates state transitions
        3. Optionally updates batch-analysis relations (with filtering to avoid overriding)

        :param batch: The AssemblyAnalysisBatch to update
        :param pipeline_type: The pipeline (ASA, VIRify, MAP)
        :param status: The new status (PENDING, RUNNING, COMPLETED, FAILED)
        :return: The updated batch
        :raises ValueError: If the state transition is invalid
        """
        # Get the current status for validation
        current_status = None
        if pipeline_type == AssemblyAnalysisPipeline.ASA:
            current_status = self.asa_status
            self.asa_status = status
        elif pipeline_type == AssemblyAnalysisPipeline.VIRIFY:
            current_status = self.virify_status
            self.virify_status = status
        elif pipeline_type == AssemblyAnalysisPipeline.MAP:
            current_status = self.map_status
            self.map_status = status
        else:
            raise ValueError(f"Unknown pipeline type: {pipeline_type}")

        # Validate state transition
        self._validate_state_transition(pipeline_type, current_status, status)

        self.save()

        return self

    def _validate_state_transition(
        self,
        pipeline_type: AssemblyAnalysisPipeline,
        from_status: str,
        to_status: str,
    ):
        """
        Validate that a state transition is allowed.

        Terminal states (COMPLETED, FAILED, PARTIAL_RESULTS, IMPORTED)
        cannot transition to active states (RUNNING, SCHEDULED).

        :param pipeline_type: The pipeline being transitioned
        :param from_status: Current state
        :param to_status: Desired new state
        :raises ValueError: If transition is not allowed
        """

        # Terminal states that should not transition to active states
        terminal_states = {
            AssemblyAnalysisPipelineStatus.COMPLETED,
            AssemblyAnalysisPipelineStatus.FAILED,
        }

        # Active states
        active_states = {
            AssemblyAnalysisPipelineStatus.RUNNING,
        }

        # Allow same-state transitions (idempotent)
        if from_status == to_status:
            return

        # Allow terminal -> terminal transitions (e.g., COMPLETED -> PARTIAL_RESULTS)
        # Allow active -> terminal transitions (normal flow)
        # Allow active -> active transitions (e.g., SCHEDULED -> RUNNING)

        # Prevent terminal -> active transitions
        if from_status in terminal_states and to_status in active_states:
            raise ValueError(
                f"Invalid state transition for {pipeline_type.value}: "
                f"Cannot transition from terminal state '{from_status}' to active state '{to_status}'"
            )

    def set_pipeline_version(
        self, pipeline_type: AssemblyAnalysisPipeline, version: str, save: bool = True
    ):
        """
        Record pipeline version.

        :param pipeline_type: The pipeline type
        :param version: Version string (e.g., "v6.0.0", "dev", "main")
        :param save: Save the batch after updating (default: True)
        """
        if not self.pipeline_versions:
            self.pipeline_versions = {}
        self.pipeline_versions[pipeline_type.value] = version
        self.save()
        return self

    def get_pipeline_version(
        self, pipeline_type: AssemblyAnalysisPipeline
    ) -> Optional[str]:
        """
        Get the pipeline version.

        :param pipeline_type: The pipeline type
        :return: Version string or None
        """
        return (
            self.pipeline_versions.get(pipeline_type.value)
            if self.pipeline_versions
            else None
        )

    def get_pipeline_workspace(
        self, pipeline_type: Union[AssemblyAnalysisPipeline, str]
    ) -> Path:
        """
        Get workspace for a pipeline.

        :param pipeline_type: PipelineType enum or string pipeline name
        :return: Path to pipeline workspace
        """
        return Path(self.workspace_dir) / str(pipeline_type)

    def refresh_analysis_count(self):
        """
        Update the total_analyses count based on linked analyses.
        Call this after adding/removing analyses from the batch.
        """
        self.total_analyses = self.analyses.count()
        self.save()
