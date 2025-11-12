import logging
import uuid
from pathlib import Path
from typing import List, Optional, Union

from django.conf import settings
from django.contrib.postgres.indexes import GinIndex
from django.db import models
from django.utils import timezone
from django.db.models import Count, Q, Exists, OuterRef
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
## This is the first implementation of multi-step pipeline  #
## and batching of analyses.                                #
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
    TODO: We should revisit all the different pipeline status and centralise them
    """

    PENDING = "pending", "Pending"
    RUNNING = "running", "Running"
    COMPLETED = "completed", "Completed"
    FAILED = "failed", "Failed"


class PipelineStatusCounts(BaseModel):
    """Status counts for a single pipeline, with fields matching AssemblyAnalysisPipelineStatus."""

    model_config = ConfigDict(extra="forbid")

    # TODO: I need to remove total and disabled
    total: int = Field(0, ge=0)
    disabled: int = Field(0, ge=0)
    pending: int = Field(0, ge=0)
    running: int = Field(0, ge=0)
    completed: int = Field(0, ge=0)
    failed: int = Field(0, ge=0)


class AssemblyAnalysisBatchStatusCounts(BaseModel):
    """
    Status counts for all pipelines in a batch.

    Fields are named after AssemblyAnalysisPipeline enum values (asa, virify, map).
    """

    model_config = ConfigDict(extra="forbid")

    asa: PipelineStatusCounts = Field(default_factory=PipelineStatusCounts)
    virify: PipelineStatusCounts = Field(default_factory=PipelineStatusCounts)
    map: PipelineStatusCounts = Field(default_factory=PipelineStatusCounts)


class AssemblyAnalysisPipelineBaseModel(models.Model):
    """
    Base model with status fields to track individual analysis progress through pipelines.

    Note: The batch-level aggregate status is now tracked via pipeline_status_counts JSONField.
    These fields track per-analysis status in the through table (AssemblyAnalysisBatchAnalysis).
    """

    asa_status = models.CharField(
        max_length=20,
        choices=AssemblyAnalysisPipelineStatus,
        default=AssemblyAnalysisPipelineStatus.PENDING,
        help_text="Assembly Analysis status for this individual analysis",
    )

    virify_status = models.CharField(
        max_length=20,
        choices=AssemblyAnalysisPipelineStatus,
        default=AssemblyAnalysisPipelineStatus.PENDING,
        help_text="VIRIfy status for this individual analysis",
    )

    map_status = models.CharField(
        max_length=20,
        choices=AssemblyAnalysisPipelineStatus,
        default=AssemblyAnalysisPipelineStatus.PENDING,
        help_text="Mobilome Annotation Pipeline status for this individual analysis",
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
        - Filtering out analyses already in batches (prevents duplicates on re-runs)
        - Filtering out already-completed analyses
        - Filtering out blocked/failed analyses (with warning)
        - Enforcing maximum analysis limits
        - Chunking analyses into manageable batch sizes
        - Creating batches

        :param study: The study
        :param pipeline: Pipeline version filter (default: v6)
        :param chunk_size: Maximum analyses per batch (default: from config)
        :param max_analyses: Maximum total analyses to process (safety cap)
        :param workspace_dir: The workspace directory (default: working dir, each pipeline will have its own subdir)
        :param skip_completed: Skip already-completed analyses (default True)
        :return: List of created batches (or existing batches if no new analyses to batch)
        :raises ValueError: If no valid analyses remain after filtering
        """
        # Apply defaults
        if pipeline is None:
            pipeline = Analysis.PipelineVersions.v6

        if chunk_size is None:
            chunk_size = (
                settings.EMG_CONFIG.assembly_analysis_pipeline.samplesheet_chunk_size
            )

        # Query analyses from the study (single source of truth)
        analyses_qs = study.analyses.filter(pipeline_version=pipeline)

        # Filter out analyses already in batches for this study
        # This allows re-running flows without creating duplicate batch-analysis relationships
        # Using Exists subquery instead of __in to prevent DB performance issues with large studies (>1000 assemblies)
        # Note: all_objects manager includes disabled relationships to avoid constraint violations
        batched_analyses_subquery = AssemblyAnalysisBatchAnalysis.all_objects.filter(
            analysis_id=OuterRef("pk"),
            batch__study=study,
            batch__batch_type=StudyAnalysisBatch.BatchType.ASSEMBLY_ANALYSIS,
        )

        analyses_qs = analyses_qs.exclude(Exists(batched_analyses_subquery))

        # Filter out completed analyses
        if skip_completed:
            analyses_qs = analyses_qs.exclude_by_statuses(
                [Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED]
            )

        # Filter out blocked/failed analyses and warn
        analyses_qs = analyses_qs.exclude_by_statuses(
            [
                Analysis.AnalysisStates.ANALYSIS_BLOCKED,
                Analysis.AnalysisStates.ANALYSIS_QC_FAILED,  # TODO: we may remove this state from the Analysis
            ]
        )

        analysis_ids = list(analyses_qs.values_list("id", flat=True))

        if not analysis_ids:
            # Check if there are existing batches for this study
            existing_batches = list(
                AssemblyAnalysisBatch.objects.filter(
                    study=study,
                    batch_type=StudyAnalysisBatch.BatchType.ASSEMBLY_ANALYSIS,
                )
            )

            if existing_batches:
                logger.info(
                    f"No new analyses to batch for study {study.accession}. "
                    f"Returning {len(existing_batches)} existing batches."
                )
                return existing_batches
            else:
                logger.error(f"No pending analyses found for study {study.accession}")
                return []

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

        # Filter out analyses already linked to this batch (use all_objects to include disabled ones)
        existing_analysis_ids = set(
            AssemblyAnalysisBatchAnalysis.all_objects.filter(
                batch=batch, analysis_id__in=analysis_ids
            ).values_list("analysis_id", flat=True)
        )

        new_analysis_ids = [
            aid for aid in analysis_ids if aid not in existing_analysis_ids
        ]

        if existing_analysis_ids:
            logger.info(
                f"Skipping {len(existing_analysis_ids)} analyses already linked to batch {batch.id}"
            )

        # Link analysis to batch (bulk update for efficiency)
        if new_analysis_ids:
            AssemblyAnalysisBatchAnalysis.objects.bulk_create(
                [
                    AssemblyAnalysisBatchAnalysis(analysis_id=aid, batch=batch)
                    for aid in new_analysis_ids
                ]
            )
            logger.info(
                f"Linked {len(new_analysis_ids)} new analyses to batch {batch.id}"
            )
        else:
            logger.info(f"No new analyses to link to batch {batch.id}")

        return batch


class AssemblyAnalysisBatch(StudyAnalysisBatch):
    """
    Batch for assembly analyses (ASA -> VIRify -> MAP).

    Status tracking is done via pipeline_status_counts JSONField which aggregates
    the individual analysis statuses from the AssemblyAnalysisBatchAnalysis through table.
    """

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

    last_error = models.TextField(null=True, blank=True)

    # Error log - it records of all (or as many as possible) errors
    error_log = models.JSONField(
        default=list,
        blank=True,
        help_text="Chronological log of errors encountered during processing. "
        "Each entry contains: timestamp, pipeline, analysis_id, error_type, message",
    )

    # Pipeline versions
    pipeline_versions = models.JSONField(default=dict, blank=True)

    # Pipeline status counts
    pipeline_status_counts: AssemblyAnalysisBatchStatusCounts = JSONFieldWithSchema(
        schema=AssemblyAnalysisBatchStatusCounts,
        default=dict,
        blank=True,
        help_text="Count of analyses in each status per pipeline (asa/virify/map)",
    )

    class Meta:
        verbose_name = "Assembly Analysis Batch"
        verbose_name_plural = "Assembly Analysis Batches"

    def __str__(self):
        return f"Assembly_AnalysisBatch {self.id}: {self.total_analyses} analyses"

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

    def update_pipeline_status_counts(
        self, pipeline_type: AssemblyAnalysisPipeline
    ) -> "AssemblyAnalysisBatch":
        """
        Update status counts for a specific pipeline by aggregating from batch_analyses.

        Uses a single optimized query with conditional aggregation for efficiency.
        This should be called at the end of each pipeline execution to reflect current state.

        TODO: this method should be called from a Django model hook... I'm still experimenting (mbc)

        :param pipeline_type: The pipeline (ASA, VIRify, MAP)
        :return: The updated batch
        """
        pipeline_key = pipeline_type.value  # "asa", "virify", "map"
        status_field = f"{pipeline_key}_status"

        # Single query with conditional aggregation - counts all statuses at once
        result = AssemblyAnalysisBatchAnalysis.all_objects.filter(batch=self).aggregate(
            pending_count=Count(
                "id",
                filter=Q(
                    **{
                        status_field: AssemblyAnalysisPipelineStatus.PENDING,
                        "disabled": False,
                    }
                ),
            ),
            running_count=Count(
                "id",
                filter=Q(
                    **{
                        status_field: AssemblyAnalysisPipelineStatus.RUNNING,
                        "disabled": False,
                    }
                ),
            ),
            completed_count=Count(
                "id",
                filter=Q(
                    **{
                        status_field: AssemblyAnalysisPipelineStatus.COMPLETED,
                        "disabled": False,
                    }
                ),
            ),
            failed_count=Count(
                "id",
                filter=Q(
                    **{
                        status_field: AssemblyAnalysisPipelineStatus.FAILED,
                        "disabled": False,
                    }
                ),
            ),
        )

        # Initialize if needed
        if not self.pipeline_status_counts:
            self.pipeline_status_counts = AssemblyAnalysisBatchStatusCounts()

        # Map the query result to PipelineStatusCounts field names
        counts = PipelineStatusCounts(
            pending=result["pending_count"],
            running=result["running_count"],
            completed=result["completed_count"],
            failed=result["failed_count"],
        )
        setattr(self.pipeline_status_counts, pipeline_key, counts)
        self.save()
        return self

    def log_error(
        self,
        pipeline_type: AssemblyAnalysisPipeline,
        error_type: str,
        message: str,
        analysis_id: Optional[int] = None,
        save: bool = True,
    ) -> "AssemblyAnalysisBatch":
        """
        Add an error entry to the error log.

        Creates a timestamped error record that can be reviewed in the admin interface.
        This provides a complete audit trail of all errors encountered during batch processing.

        # TODO: this one could use some more types.. for example in error_type.

        :param pipeline_type: The pipeline where the error occurred (ASA, VIRify, MAP)
        :param error_type: Type of error (e.g., "validation", "import", "pipeline_execution")
        :param message: Error message (will be truncated to 500 chars if longer)
        :param analysis_id: ID of the affected analysis (optional for batch-level errors)
        :param save: Save the batch after logging (default: True)
        :return: The updated batch
        """
        if not isinstance(self.error_log, list):
            self.error_log = []

        # Truncate long messages
        truncated_message = message[:500] + "..." if len(message) > 500 else message

        error_entry = {
            "timestamp": timezone.now().isoformat(),
            "pipeline": pipeline_type.value,
            "error_type": error_type,
            "message": truncated_message,
        }

        if analysis_id:
            error_entry["analysis_id"] = analysis_id

        self.error_log.append(error_entry)

        # Keep only last 100 errors to prevent unbounded growth
        if len(self.error_log) > 100:
            self.error_log = self.error_log[-100:]

        if save:
            self.save()

        return self
