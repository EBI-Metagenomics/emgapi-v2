from __future__ import annotations

import logging
import os
import re
import uuid
from pathlib import Path
from typing import ClassVar, Union, Optional

import pandas as pd
from aenum import extend_enum
from db_file_storage.model_utils import delete_file, delete_file_if_needed
from django.conf import settings
from django.contrib.postgres.indexes import GinIndex
from django.core.exceptions import MultipleObjectsReturned, ObjectDoesNotExist
from django.core.files.storage import storages
from django.db import models
from django.db.models import JSONField, Q, Func, Value, QuerySet
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils import timezone
from django.utils.text import Truncator
from django_ltree.models import TreeModel
from pydantic import BaseModel, ConfigDict, Field

import ena.models
from analyses.base_models.base_models import (
    ENADerivedManager,
    ENADerivedModel,
    PrivacyFilterManagerMixin,
    TimeStampedModel,
    VisibilityControlledModel,
    InferredMetadataMixin,
    DbStoredFileField,
)
from analyses.base_models.mgnify_accessioned_models import MGnifyAccessionField
from analyses.base_models.with_downloads_models import WithDownloadsModel, DownloadFile
from analyses.base_models.with_status_models import SelectByStatusManagerMixin
from analyses.base_models.with_watchers_models import WithWatchersModel
from emgapiv2.async_utils import anysync_property
from emgapiv2.enum_utils import FutureStrEnum
from emgapiv2.model_utils import JSONFieldWithSchema
from workflows.data_io_utils.csv.csv_comment_handler import CSVDelimiter
from workflows.data_io_utils.file_rules.common_rules import (
    DirectoryExistsRule,
    FileExistsRule,
)
from workflows.ena_utils.read_run import ENAReadRunFields
from workflows.ena_utils.sample import ENASampleFields

# Some models associated with MGnify Analyses (MGYS, MGYA etc).


logger = logging.getLogger(__name__)


class Biome(TreeModel):
    biome_name = models.CharField(max_length=255)

    def __str__(self):
        return self.pretty_lineage

    @property
    def pretty_lineage(self):
        return ":".join(self.ancestors().values_list("biome_name", flat=True))

    @property
    def descendants_count(self):
        return self.descendants().count()

    @staticmethod
    def lineage_to_path(lineage: str) -> str:
        """
        E.g. "root:Host-associated:Human:Digestive system:estómago" -> root.host-associated.human.digestive_system:estmago
        :param lineage: Lineage string in colon-separated form.
        :return: Lineage as a dot-separated path suitable for a postgres ltree field (alphanumeric and _ only, nospaced)
        """
        ascii_lower = lineage.encode("ascii", "ignore").decode("ascii").lower()
        dot_separated = ascii_lower.replace(":", ".")
        underscore_punctuated = (
            dot_separated.replace(" ", "_")
            .replace("(", "_")
            .replace(")", "_")
            .replace("-", "_")
            .replace("__", "_")
            .strip("_.")
        )
        return re.sub(r"[^a-zA-Z0-9._]", "", underscore_punctuated)


class StudyManager(ENADerivedManager):
    def get_or_create_for_ena_study(self, ena_study_accession):
        logger.info(f"Will get/create MGnify study for {ena_study_accession}")
        try:
            ena_study = ena.models.Study.objects.filter(
                Q(accession=ena_study_accession)
                | Q(additional_accessions__icontains=ena_study_accession)
            ).first()
            logger.debug(f"Got {ena_study}")
        except (MultipleObjectsReturned, ObjectDoesNotExist):
            logger.error(
                f"Problem getting ENA study {ena_study_accession} from ENA models DB. "
                f"The ENA Study needs to have been fetched from ENA APIs first."
            )
            raise
        study, _ = Study.objects.get_or_create(
            ena_study=ena_study,
            title=ena_study.title,
            defaults={"is_private": ena_study.is_private},
        )
        study.inherit_accessions_from_related_ena_object("ena_study")
        return study


class PublicStudyManager(PrivacyFilterManagerMixin, StudyManager):
    """
    A custom manager that filters out private studies by default.
    """

    pass


class Study(
    ENADerivedModel,
    WithDownloadsModel,
    TimeStampedModel,
    WithWatchersModel,
):
    objects = StudyManager()
    public_objects = PublicStudyManager()

    DOWNLOAD_PARENT_IDENTIFIER_ATTR = "accession"
    ALLOWED_DOWNLOAD_GROUP_PREFIXES = ["study_summary"]

    id = models.AutoField(primary_key=True)
    accession = MGnifyAccessionField(
        accession_prefix="MGYS", accession_length=8, db_index=True
    )
    ena_study = models.ForeignKey(
        ena.models.Study, on_delete=models.CASCADE, null=True, blank=True
    )
    biome = models.ForeignKey(Biome, on_delete=models.CASCADE, null=True, blank=True)

    class StudyFeatures(BaseModel):
        """
        Pydantic schema for storing a feature set of a study in JSON.
        """

        model_config = ConfigDict(extra="allow")

        has_prev6_analyses: bool = Field(False)
        has_v6_analyses: bool = Field(False)

    features: StudyFeatures = JSONFieldWithSchema(
        schema=StudyFeatures, default=StudyFeatures
    )

    title = models.CharField(max_length=4000)  # same max as ENA DB
    results_dir = models.CharField(max_length=256, null=True, blank=True)
    external_results_dir = models.CharField(max_length=256, null=True, blank=True)

    def __str__(self):
        return self.accession

    class Meta:
        verbose_name_plural = "studies"

        indexes = [
            GinIndex(fields=["features"]),
        ]

    @property
    def first_accession(self):
        # Prefer ERP--,SRP--,DRP-- etc style accessions over PRJ--, if available
        return next(
            (
                accession
                for accession in self.ena_accessions
                if not accession.upper().startswith("PRJ")
            ),
            super().first_accession,
        )


class PublicSampleManager(PrivacyFilterManagerMixin, ENADerivedManager): ...


class Sample(ENADerivedModel, TimeStampedModel):
    CommonMetadataKeys = ENASampleFields

    objects = ENADerivedManager()
    public_objects = PublicSampleManager()

    id = models.AutoField(primary_key=True)
    ena_sample = models.ForeignKey(ena.models.Sample, on_delete=models.CASCADE)
    studies = models.ManyToManyField(Study)

    metadata = models.JSONField(default=dict, blank=True)

    def __str__(self):
        return f"Sample {self.id}: {self.ena_sample}"


class WithExperimentTypeModel(models.Model):
    class ExperimentTypes(models.TextChoices):
        METATRANSCRIPTOMIC = "METAT", "Metatranscriptomic"
        METAGENOMIC = "METAG", "Metagenomic"
        AMPLICON = "AMPLI", "Amplicon"
        ASSEMBLY = "ASSEM", "Assembly"
        HYBRID_ASSEMBLY = "HYASS", "Hybrid assembly"
        LONG_READ_ASSEMBLY = "LRASS", "Long-read assembly"

        # legacy
        METABARCODING = "METAB", "Metabarcoding"
        UNKNOWN = "UNKNO", "Unknown"

    experiment_type = models.CharField(
        choices=ExperimentTypes, max_length=5, default=ExperimentTypes.UNKNOWN
    )

    class Meta:
        abstract = True


class PublicRunManager(PrivacyFilterManagerMixin, models.Manager): ...


class Run(
    InferredMetadataMixin, TimeStampedModel, ENADerivedModel, WithExperimentTypeModel
):
    CommonMetadataKeys = ENAReadRunFields
    extend_enum(
        CommonMetadataKeys, "FASTQ_FTPS", "fastq_ftps"
    ),  # plural convention mismatch to ENA; TODO
    extend_enum(
        CommonMetadataKeys, "INFERRED_LIBRARY_LAYOUT", "inferred_library_layout"
    )

    class InstrumentPlatformKeys:
        BGISEQ = "BGISEQ"
        DNBSEQ = "DNBSEQ"
        ILLUMINA = "ILLUMINA"
        OXFORD_NANOPORE = "OXFORD_NANOPORE"
        PACBIO_SMRT = "PACBIO_SMRT"
        ION_TORRENT = "ION_TORRENT"

    objects = ENADerivedManager()
    public_objects = PublicRunManager()

    id = models.AutoField(primary_key=True)
    instrument_platform = models.CharField(
        db_column="instrument_platform", max_length=100, blank=True, null=True
    )
    instrument_model = models.CharField(
        db_column="instrument_model", max_length=100, blank=True, null=True
    )

    metadata = models.JSONField(default=dict, blank=True)
    study = models.ForeignKey(Study, on_delete=models.CASCADE, related_name="runs")
    sample = models.ForeignKey(Sample, on_delete=models.CASCADE, related_name="runs")

    @property
    def latest_analysis(self) -> "Analysis":
        latest_analysis: Analysis = self.analyses.order_by("-updated_at").first()
        return latest_analysis

    @property
    def latest_analysis_status(self) -> dict["Analysis.AnalysisStates", bool]:
        latest_analysis: Analysis = self.latest_analysis
        return latest_analysis.status

    def set_experiment_type_by_metadata(
        self, ena_library_strategy: str, ena_library_source: str
    ):
        ALLOWED_WHOLE_GENOME_LIBRARY_STRATEGIES = ["wgs", "wga"]
        ALLOWED_AMPLICON_LIBRARY_STRATEGIES = ["amplicon"]

        if ena_library_strategy.lower() == "rna-seq" and (
            ena_library_source.lower() == "metagenomic"
            or ena_library_source.lower() == "metatranscriptomic"
        ):
            self.experiment_type = Run.ExperimentTypes.METATRANSCRIPTOMIC
        elif (
            ena_library_strategy.lower() in ALLOWED_WHOLE_GENOME_LIBRARY_STRATEGIES
            and ena_library_source.lower() == "metatranscriptomic"
        ):
            self.experiment_type = Run.ExperimentTypes.METATRANSCRIPTOMIC
        elif (
            ena_library_strategy.lower() in ALLOWED_WHOLE_GENOME_LIBRARY_STRATEGIES
            and ena_library_source.lower() == "metagenomic"
        ):
            self.experiment_type = Run.ExperimentTypes.METAGENOMIC
        elif (
            ena_library_strategy.lower() in ALLOWED_AMPLICON_LIBRARY_STRATEGIES
            and ena_library_source.lower() == "metagenomic"
        ):
            self.experiment_type = Run.ExperimentTypes.AMPLICON
        else:
            self.experiment_type = Run.ExperimentTypes.UNKNOWN
        self.save()

    def __str__(self):
        return f"Run {self.id}: {self.first_accession}"


class Assembler(TimeStampedModel):
    METASPADES = "metaspades"
    MEGAHIT = "megahit"
    SPADES = "spades"
    FLYE = "flye"

    NAME_CHOICES = [
        (METASPADES, "MetaSPAdes"),
        (MEGAHIT, "MEGAHIT"),
        (SPADES, "SPAdes"),
        (FLYE, "Flye"),
    ]

    assembler_default: ClassVar[str] = METASPADES

    name = models.CharField(max_length=20, null=True, blank=True, choices=NAME_CHOICES)
    version = models.CharField(max_length=20)

    def save(self, *args, **kwargs):
        self.clean()
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.name} {self.version}" if self.version is not None else self.name


class AssemblyManager(SelectByStatusManagerMixin, ENADerivedManager):
    def get_queryset(self):
        return super().get_queryset().select_related("run")


class PublicAssemblyManager(PrivacyFilterManagerMixin, AssemblyManager): ...


class Assembly(InferredMetadataMixin, TimeStampedModel, ENADerivedModel):
    objects = AssemblyManager()
    public_objects = PublicAssemblyManager()

    dir = models.CharField(max_length=200, null=True, blank=True)
    run = models.ForeignKey(
        Run, on_delete=models.CASCADE, related_name="assemblies", null=True, blank=True
    )  # TODO: coassembly
    sample = models.ForeignKey(
        Sample,
        on_delete=models.CASCADE,
        related_name="assemblies",
        null=True,
        blank=True,
    )  # TODO: coassembly
    # raw reads study that was used as resource for assembly
    reads_study = models.ForeignKey(
        Study,
        on_delete=models.CASCADE,
        related_name="assemblies_reads",
        null=True,
        blank=True,
    )
    # TPA study that was created to submit assemblies
    assembly_study = models.ForeignKey(
        Study,
        on_delete=models.CASCADE,
        related_name="assemblies_assembly",
        null=True,
        blank=True,
    )
    assembler = models.ForeignKey(
        Assembler,
        on_delete=models.CASCADE,
        related_name="assemblies",
        null=True,
        blank=True,
    )

    class CommonMetadataKeys:
        COVERAGE = "coverage"
        COVERAGE_DEPTH = "coverage_depth"
        N_CONTIGS = "n_contigs"
        CONTAMINANT_REFERENCE = "contaminant_reference"

    metadata = JSONField(default=dict, db_index=True, blank=True)

    class AssemblyStates(FutureStrEnum):
        ENA_METADATA_SANITY_CHECK_FAILED = "ena_metadata_sanity_check_failed"
        ENA_DATA_QC_CHECK_FAILED = "ena_data_qc_check_failed"
        ASSEMBLY_STARTED = "assembly_started"
        PRE_ASSEMBLY_QC_FAILED = "pre_assembly_qc_failed"
        POST_ASSEMBLY_QC_FAILED = "post_assembly_qc_failed"
        ASSEMBLY_FAILED = "assembly_failed"
        ASSEMBLY_COMPLETED = "assembly_completed"
        ASSEMBLY_BLOCKED = "assembly_blocked"
        ASSEMBLY_UPLOADED = "assembly_uploaded"
        ASSEMBLY_UPLOAD_FAILED = "assembly_upload_failed"
        ASSEMBLY_UPLOAD_BLOCKED = "assembly_upload_blocked"

        @classmethod
        def default_status(cls):
            return {
                cls.ASSEMBLY_STARTED: False,
                cls.PRE_ASSEMBLY_QC_FAILED: False,
                cls.ASSEMBLY_FAILED: False,
                cls.ASSEMBLY_COMPLETED: False,
                cls.ASSEMBLY_BLOCKED: False,
                cls.ASSEMBLY_UPLOADED: False,
                cls.ASSEMBLY_UPLOAD_FAILED: False,
                cls.ASSEMBLY_UPLOAD_BLOCKED: False,
            }

    status = models.JSONField(
        default=AssemblyStates.default_status, null=True, blank=True
    )

    def mark_status(
        self, status: AssemblyStates, set_status_as: bool = True, reason: str = None
    ):
        """Updates the assembly's status. If a reason is provided, it will be saved as '{status}_reason'."""
        self.status[status] = set_status_as
        if reason:
            self.status[f"{status}_reason"] = reason
        return self.save()

    def add_erz_accession(self, erz_accession):
        if erz_accession not in self.ena_accessions:
            self.ena_accessions.append(erz_accession)
            return self.save()

    @anysync_property
    def dir_with_miassembler_suffix(self):
        # MIAssembler outputs to a specific dir pattern inside the run's assembly/ies folder.
        assembler = self.assembler
        return (
            Path(self.dir)
            / Path("assembly")
            / Path(assembler.name.lower())
            / Path(assembler.version)
        )

    class Meta:
        verbose_name_plural = "Assemblies"
        constraints = [
            models.CheckConstraint(
                condition=Q(reads_study__isnull=False)
                | Q(assembly_study__isnull=False),
                name="at_least_one_study_present",
            )
        ]
        ordering = ["id"]

    def __str__(self):
        return f"Assembly {self.id} | {self.first_accession or 'unaccessioned'} (Run {self.run.first_accession})"


class AssemblyAnalysisRequest(TimeStampedModel):
    class AssemblyAnalysisStates:
        REQUEST_RECEIVED = "request_received"
        STUDY_FETCHED = "study_fetched"
        ASSEMBLY_STARTED = "assembly_started"
        ASSEMBLY_COMPLETED = "assembly_completed"
        ANALYSIS_STARTED = "analysis_started"
        ANALYSIS_COMPLETED = "analysis_completed"

        @classmethod
        def default_status(cls):
            return {
                cls.REQUEST_RECEIVED: True,
                cls.STUDY_FETCHED: False,
                cls.ASSEMBLY_STARTED: False,
                cls.ASSEMBLY_COMPLETED: False,
                cls.ANALYSIS_STARTED: False,
                cls.ANALYSIS_COMPLETED: False,
            }

    class RequestMetadata:
        STUDY_ACCESSION = "study_accession"
        FLOW_RUN_ID = "flow_run_id"

        @classmethod
        def default_metadata(cls):
            return {cls.STUDY_ACCESSION: None, cls.FLOW_RUN_ID: None}

    requestor = models.CharField(max_length=20)
    status = models.JSONField(
        default=AssemblyAnalysisStates.default_status, null=True, blank=True
    )
    study = models.ForeignKey(Study, on_delete=models.CASCADE, null=True, blank=True)
    request_metadata = models.JSONField(
        default=RequestMetadata.default_metadata, null=True, blank=True
    )

    @property
    def requested_study(self):
        return self.request_metadata.get(self.RequestMetadata.STUDY_ACCESSION)

    @property
    def flow_run_link(self):
        return f"{os.getenv('PREFECT_API_URL')}/flow-runs/flow-run/{self.request_metadata.get(self.RequestMetadata.FLOW_RUN_ID)}"

    def __str__(self):
        return f"AssemblyAnalysisRequest {self.pk}: {self.requested_study}"

    def mark_status(self, status: AssemblyAnalysisStates, set_status_as: bool = True):
        self.status[status] = set_status_as
        return self.save()


class ComputeResourceHeuristic(TimeStampedModel):
    """
    Model for heuristics like how much memory is needed to assemble a certain biome with a certain assembler.
    """

    # process type for when the heuristic should be used
    class ProcessTypes(models.TextChoices):
        ASSEMBLY = "ASSEM", "Assembly"

    process = models.CharField(
        choices=ProcessTypes, max_length=5, null=True, blank=True
    )

    # relationships used for selecting heuristic value
    biome = models.ForeignKey(Biome, on_delete=models.CASCADE, null=True, blank=True)
    assembler = models.ForeignKey(
        Assembler, on_delete=models.CASCADE, null=True, blank=True
    )

    # heuristic values
    memory_gb = models.FloatField(null=True, blank=True)

    def __str__(self):
        if self.process == self.ProcessTypes.ASSEMBLY:
            return f"ComputeResourceHeuristic {self.id} (Use {self.memory_gb:.0f} GB to assemble {self.biome} with {self.assembler})"
        else:
            return f"ComputeResourceHeuristic {self.id} ({self.process})"


class PipelineState(models.TextChoices):
    """Business-level pipeline states."""

    PENDING = "pending", "Pending"
    IN_PROGRESS = "in_progress", "In Progress"
    READY = "ready", "Ready"
    FAILED = "failed", "Failed"


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

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    study = models.ForeignKey(
        Study, on_delete=models.CASCADE, related_name="analysis_batches"
    )
    batch_type = models.CharField(max_length=50)
    results_dir = models.CharField(max_length=500)
    total_analyses = models.IntegerField()
    prefect_flow_run_id = models.CharField(max_length=100, null=True, blank=True)

    class Meta:
        verbose_name = "Study Analysis Batch"
        verbose_name_plural = "Study Analysis Batches"

    def __str__(self):
        return f"{self.batch_type.title()}Batch {str(self.id)[:8]}: {self.total_analyses} analyses"

    def get_pipeline_workspace(self, pipeline_name: str) -> Path:
        """Get workspace directory for a pipeline."""
        return Path(self.results_dir) / pipeline_name


class AssemblyAnalysisBatchManager(models.Manager):

    def create_batches_for_study(
        self,
        study: "Study",
        pipeline: "Analysis.PipelineVersions" = None,
        chunk_size: int = None,
        max_analyses: int = None,
        base_results_dir: Path = None,
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
        :param base_results_dir: Optional base results directory
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
            raise ValueError(f"No pending analyses found for study {study.accession}")

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
                study=study, analysis_ids=chunk, base_results_dir=base_results_dir
            )
            batches.append(batch)

        return batches

    def _create_for_analyses(
        self, study: "Study", analysis_ids: list[int], base_results_dir: Path = None
    ) -> "AssemblyAnalysisBatch":
        """
        Internal method to create a batch for a list of analysis IDs.

        This is a simple, "dumb" method that just creates the batch object
        without validation. All validations should be done in create_batches_for_study.

        :param study: The study
        :param analysis_ids: List of analysis IDs
        :param base_results_dir: Optional base results directory
        :return: Created batch
        """
        batch = AssemblyAnalysisBatch(
            study=study,
            batch_type="assembly_analysis",
            results_dir="",
            total_analyses=len(analysis_ids),
        )
        # Save to generate the id (which is a UUID)
        batch.save()

        # It would be also possible to move this into the save method (but I rather not... I think).
        dir_name = f"{study.accession}_assembly_analysis_{str(batch.id)[:8]}"

        if base_results_dir:
            results_dir = base_results_dir / dir_name
        else:
            results_dir = Path(
                f"{settings.EMG_CONFIG.results.base_dir}/{study.accession}/{dir_name}"
            )

        results_dir.mkdir(parents=True, exist_ok=True)

        # Update the batch with actual results_dir
        batch.results_dir = str(results_dir)
        batch.save()

        # Link analysis to batch (bulk update for efficiency)
        Analysis.objects.filter(id__in=analysis_ids).update(
            assembly_analysis_batch=batch, results_dir=str(results_dir)
        )

        return batch


class AssemblyAnalysisBatch(StudyAnalysisBatch):
    """Batch for assembly analyses (ASA -> VIRify -> MAP)."""

    objects = AssemblyAnalysisBatchManager()

    class PipelineType(models.TextChoices):
        """Assembly analysis pipelines."""

        ASA = "asa", "Assembly Analysis"
        VIRIFY = "virify", "VIRify"
        MAP = "map", "MAP"

    # Samplesheets
    asa_samplesheet_path = models.CharField(max_length=500, null=True, blank=True)
    virify_samplesheet_path = models.CharField(max_length=500, null=True, blank=True)
    map_samplesheet_path = models.CharField(max_length=500, null=True, blank=True)

    # Business states
    asa_state = models.CharField(
        max_length=20, choices=PipelineState.choices, default=PipelineState.PENDING
    )
    virify_state = models.CharField(
        max_length=20, choices=PipelineState.choices, default=PipelineState.PENDING
    )
    map_state = models.CharField(
        max_length=20, choices=PipelineState.choices, default=PipelineState.PENDING
    )

    # Prefect flow run IDs
    asa_flow_run_id = models.CharField(max_length=100, null=True, blank=True)
    virify_flow_run_id = models.CharField(max_length=100, null=True, blank=True)
    map_flow_run_id = models.CharField(max_length=100, null=True, blank=True)

    # Timestamps
    asa_started_at = models.DateTimeField(null=True, blank=True)
    asa_completed_at = models.DateTimeField(null=True, blank=True)
    virify_started_at = models.DateTimeField(null=True, blank=True)
    virify_completed_at = models.DateTimeField(null=True, blank=True)
    map_started_at = models.DateTimeField(null=True, blank=True)
    map_completed_at = models.DateTimeField(null=True, blank=True)

    # Error tracking
    last_error = models.TextField(null=True, blank=True)

    # Pipeline versions
    pipeline_versions = JSONField(default=dict, blank=True)

    class Meta:
        verbose_name = "Assembly Analysis Batch"
        verbose_name_plural = "Assembly Analysis Batches"

    def __str__(self):
        return f"Assembly_AnalysisBatch {self.id}: {self.total_analyses} analyses"

    @property
    def analyses(self) -> QuerySet[Analysis]:
        """Get all analyses in this batch.
        This is not for free; it will query the db.
        :return: QuerySet[Analysis]
        """
        return self.assembly_analysis_set.all()

    def generate_asa_samplesheet(self) -> Path:
        """
        Generate ASA samplesheet for this batch (lazy, idempotent).

        The samplesheet is stored in the batch working directory:
        {batch.results_dir}/samplesheets/

        :return: Path to generated samplesheet
        """
        # Import here to avoid circular dependency
        from workflows.flows.analyse_study_tasks.assembly.make_samplesheet_assembly import (
            make_samplesheet_assembly,
        )

        # Return existing samplesheet if already generated
        if self.asa_samplesheet_path and Path(self.asa_samplesheet_path).exists():
            return Path(self.asa_samplesheet_path)

        # Create samplesheets directory in batch workspace
        samplesheets_dir = Path(self.results_dir) / "samplesheets"
        samplesheets_dir.mkdir(parents=True, exist_ok=True)

        # Generate a new samplesheet for this batch
        samplesheet_path, _ = make_samplesheet_assembly(
            self.study, self.assembly_analysis_set.all(), output_dir=samplesheets_dir
        )

        # Store path
        self.asa_samplesheet_path = str(samplesheet_path)
        self.save()

        return samplesheet_path

    def set_pipeline_state(self, pipeline_type: PipelineType, state: PipelineState):
        """
        Update the pipeline state and propagate to linked analyses.

        :param pipeline_type: The pipeline type
        :param state: The pipeline state
        """
        # TODO: have a think about this... too many assumptions here
        if pipeline_type == self.PipelineType.ASA:
            self.asa_state = state
            if state == PipelineState.IN_PROGRESS and not self.asa_started_at:
                self.asa_started_at = timezone.now()
            elif state == PipelineState.READY:
                self.asa_completed_at = timezone.now()
                # ASA READY: analyses should already be marked as imported by the import flow
                # No need to propagate - just confirm the batch is ready
            elif state == PipelineState.FAILED:
                # Propagate failure to analyses
                self._sync_analyses_state(
                    Analysis.AnalysisStates.ANALYSIS_FAILED, reason=self.last_error
                )
        elif pipeline_type == self.PipelineType.VIRIFY:
            self.virify_state = state
            if state == PipelineState.IN_PROGRESS and not self.virify_started_at:
                self.virify_started_at = timezone.now()
            elif state == PipelineState.READY:
                self.virify_completed_at = timezone.now()
                # VIRify completion doesn't change core analysis status
                # Downloads are added separately in the flow
            elif state == PipelineState.FAILED:
                # VIRify failure doesn't fail the whole analysis
                # Just recorded in batch
                pass
        elif pipeline_type == self.PipelineType.MAP:
            self.map_state = state
            if state == PipelineState.IN_PROGRESS and not self.map_started_at:
                self.map_started_at = timezone.now()
            elif state == PipelineState.READY:
                self.map_completed_at = timezone.now()
                # MAP completion doesn't change core analysis status
                # Downloads are added separately in the flow
            elif state == PipelineState.FAILED:
                # MAP failure doesn't fail the whole analysis
                # Just recorded in batch
                pass
        self.save()

    def _sync_analyses_state(
        self, analysis_state: "Analysis.AnalysisStates", reason: str = None
    ):
        """
        Synchronize analysis states with batch state.
        Called automatically when the batch state changes.

        :param analysis_state: The AnalysisStates enum value to set
        :param reason:  an Optional reason for state change (e.g., error message)
        """
        for analysis in self.assembly_analysis_set.all():
            analysis.mark_status(analysis_state, reason=reason)

    def set_pipeline_version(self, pipeline_type: PipelineType, version: str):
        """
        Record pipeline version.

        :param pipeline_type: The pipeline type
        :param version: Version string (e.g., "v6.0.0", "dev", "main")
        """
        if not self.pipeline_versions:
            self.pipeline_versions = {}
        self.pipeline_versions[pipeline_type.value] = version
        self.save()

    def get_pipeline_version(self, pipeline_type: PipelineType) -> Optional[str]:
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

    def get_pipeline_workspace(self, pipeline_type: Union[PipelineType, str]) -> Path:
        """
        Get workspace for a pipeline.

        :param pipeline_type: PipelineType enum or string pipeline name
        :return: Path to pipeline workspace
        """
        if isinstance(pipeline_type, str):
            pipeline_name = pipeline_type
        else:
            pipeline_name = pipeline_type.value
        return Path(self.results_dir) / pipeline_name

    @property
    def is_ready_for_virify(self) -> bool:
        """Check if ready for VIRify."""
        return self.asa_state == PipelineState.READY

    @property
    def is_ready_for_map(self) -> bool:
        """Check if ready for MAP."""
        return (
            self.asa_state == PipelineState.READY
            and self.virify_state == PipelineState.READY
        )

    def refresh_analysis_count(self):
        """
        Update the total_analyses count based on linked analyses.
        Call this after adding/removing analyses from the batch.
        """
        self.total_analyses = self.assembly_analysis_set.count()
        self.save()


class AnalysisManagerDeferringAnnotations(SelectByStatusManagerMixin, models.Manager):
    """
    The annotations field is a potentially large JSONB field.
    Defer it by default, since most queries don't need to transfer this large dataset.
    """

    def get_queryset(self):
        return super().get_queryset().defer("annotations")


class AnalysisManagerIncludingAnnotations(SelectByStatusManagerMixin, models.Manager):
    def get_queryset(self):
        return super().get_queryset()


class PublicAnalysisManager(
    PrivacyFilterManagerMixin, AnalysisManagerDeferringAnnotations
):
    """
    A custom manager that filters out private analyses by default.
    """

    def get_queryset(self, *args, **kwargs):
        return super().get_queryset(*args, **kwargs).filter(is_ready=True)


class PublicAnalysisManagerIncludingAnnotations(
    PrivacyFilterManagerMixin, AnalysisManagerIncludingAnnotations
):
    """
    A custom manager that includes annotations but still filters out private analyses by default.
    """

    pass


class Analysis(
    InferredMetadataMixin,
    TimeStampedModel,
    VisibilityControlledModel,
    WithDownloadsModel,
    WithExperimentTypeModel,
):
    objects = AnalysisManagerDeferringAnnotations()
    objects_and_annotations = AnalysisManagerIncludingAnnotations()

    public_objects = PublicAnalysisManager()
    public_objects_and_annotations = PublicAnalysisManagerIncludingAnnotations()

    DOWNLOAD_PARENT_IDENTIFIER_ATTR = "accession"

    id = models.AutoField(primary_key=True)
    is_ready = models.GeneratedField(
        expression=Func(
            Value("analysis_annotations_imported"),
            function="jsonb_extract_path_text",
            template="(jsonb_extract_path_text(status, %(expressions)s)::boolean)",
        ),
        output_field=models.BooleanField(),
        db_persist=True,
    )

    accession = MGnifyAccessionField(accession_prefix="MGYA", accession_length=8)

    suppression_following_fields = ["sample"]  # TODO
    study = models.ForeignKey(
        Study, on_delete=models.CASCADE, to_field="accession", related_name="analyses"
    )
    results_dir = models.CharField(max_length=256, null=True, blank=True)
    external_results_dir = models.CharField(max_length=256, null=True, blank=True)
    sample = models.ForeignKey(
        Sample, on_delete=models.CASCADE, related_name="analyses"
    )
    run = models.ForeignKey(
        Run, on_delete=models.CASCADE, null=True, blank=True, related_name="analyses"
    )
    assembly = models.ForeignKey(
        Assembly,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="analyses",
    )
    assembly_analysis_batch = models.ForeignKey(
        AssemblyAnalysisBatch,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="assembly_analysis_set",
    )
    is_suppressed = models.BooleanField(default=False)

    GENOME_PROPERTIES = "genome_properties"
    GO_TERMS = "go_terms"
    GO_SLIMS = "go_slims"
    INTERPRO_IDENTIFIERS = "interpro_identifiers"
    KEGG_MODULES = "kegg_modules"
    KEGG_ORTHOLOGS = "kegg_orthologs"
    ANTISMASH_GENE_CLUSTERS = "antismash_gene_clusters"
    SANNTIS_GENE_CLUSTERS = "sanntis_gene_clusters"
    PFAMS = "pfams"
    RHEA_REACTIONS = "rhea_reactions"

    TAXONOMIES = "taxonomies"
    CLOSED_REFERENCE = "closed_reference"
    ASV = "asv"
    CODING_SEQUENCES = "coding_sequences"
    FUNCTIONAL_ANNOTATION = "functional_annotation"

    VIRIFY = "virify"

    class TaxonomySources(FutureStrEnum):
        SSU: str = "ssu"
        LSU: str = "lsu"
        MOTUS: str = "motus"
        ITS_ONE_DB: str = "its_one_db"
        UNITE: str = "unite"
        PR2: str = "pr2"
        DADA2_SILVA: str = "dada2_silva"
        DADA2_PR2: str = "dada2_pr2"
        UNIREF: str = "uniref"

    TAXONOMIES_SSU = f"{TAXONOMIES}__{TaxonomySources.SSU}"
    TAXONOMIES_LSU = f"{TAXONOMIES}__{TaxonomySources.LSU}"
    TAXONOMIES_ITS_ONE_DB = f"{TAXONOMIES}__{TaxonomySources.ITS_ONE_DB}"
    TAXONOMIES_UNITE = f"{TAXONOMIES}__{TaxonomySources.UNITE}"
    TAXONOMIES_PR2 = f"{TAXONOMIES}__{TaxonomySources.PR2}"
    TAXONOMIES_DADA2_SILVA = f"{TAXONOMIES}__{TaxonomySources.DADA2_SILVA}"
    TAXONOMIES_DADA2_PR2 = f"{TAXONOMIES}__{TaxonomySources.DADA2_PR2}"
    TAXONOMIES_UNIREF = f"{TAXONOMIES}__{TaxonomySources.UNIREF}"

    # TODO: check this... not sure if this is clear enough
    ALLOWED_DOWNLOAD_GROUP_PREFIXES = [
        "all",  # catch-all for legacy
        f"{TAXONOMIES}.closed_reference.",
        f"{TAXONOMIES}.asv.",
        "quality_control",
        "primer_identification",
        "taxonomy",
        "annotation_summary",
        ASV,
        CODING_SEQUENCES,
        FUNCTIONAL_ANNOTATION,
        GENOME_PROPERTIES,
        GO_TERMS,
        GO_SLIMS,
        INTERPRO_IDENTIFIERS,
        KEGG_MODULES,
        KEGG_ORTHOLOGS,
        ANTISMASH_GENE_CLUSTERS,
        SANNTIS_GENE_CLUSTERS,
        PFAMS,
        VIRIFY,
    ]

    class FunctionalSources(FutureStrEnum):
        PFAM: str = "pfam"

    FUNCTIONAL_PFAM = f"{FUNCTIONAL_ANNOTATION}__{FunctionalSources.PFAM}"

    @staticmethod
    def default_annotations():
        return {
            Analysis.GENOME_PROPERTIES: [],
            Analysis.GO_TERMS: [],
            Analysis.GO_SLIMS: [],
            Analysis.INTERPRO_IDENTIFIERS: [],
            Analysis.KEGG_MODULES: [],
            Analysis.KEGG_ORTHOLOGS: [],
            Analysis.TAXONOMIES: [],
            Analysis.ANTISMASH_GENE_CLUSTERS: [],
            Analysis.PFAMS: [],
        }

    annotations = models.JSONField(default=default_annotations.__func__)
    quality_control = models.JSONField(default=dict, blank=True)

    class KnownMetadataKeys:
        MARKER_GENE_SUMMARY = "marker_gene_summary"  # for amplicon analyses

    metadata = models.JSONField(default=dict, blank=True)

    class PipelineVersions(models.TextChoices):
        v5 = "V5", "v5.0"
        v6 = "V6", "v6.0"

    pipeline_version = models.CharField(
        choices=PipelineVersions, max_length=5, default=PipelineVersions.v6
    )

    class AnalysisStates(FutureStrEnum):
        ANALYSIS_STARTED = "analysis_started"
        ANALYSIS_COMPLETED = "analysis_completed"
        ANALYSIS_BLOCKED = "analysis_blocked"
        ANALYSIS_FAILED = "analysis_failed"
        ANALYSIS_QC_FAILED = "analysis_qc_failed"
        ANALYSIS_POST_SANITY_CHECK_FAILED = "analysis_post_sanity_check_failed"
        ANALYSIS_ANNOTATIONS_IMPORTED = "analysis_annotations_imported"

        @classmethod
        def default_status(cls):
            return {
                cls.ANALYSIS_STARTED: False,
                cls.ANALYSIS_QC_FAILED: False,
                cls.ANALYSIS_COMPLETED: False,
                cls.ANALYSIS_BLOCKED: False,
                cls.ANALYSIS_FAILED: False,
                cls.ANALYSIS_ANNOTATIONS_IMPORTED: False,
            }

    status = models.JSONField(
        default=AnalysisStates.default_status, null=True, blank=True
    )

    def mark_status(
        self, status: AnalysisStates, set_status_as: bool = True, reason: str = None
    ):
        self.status[status] = set_status_as
        if reason:
            self.status[f"{status}_reason"] = reason
        return self.save()

    @property
    def assembly_or_run(self) -> Union[Assembly, Run]:
        return self.assembly or self.run

    @property
    def raw_run(self) -> Run:
        return self.assembly.run if self.assembly else self.run

    def inherit_experiment_type(self):
        prev_experiment_type = f"{self.experiment_type}"
        if self.assembly:
            self.experiment_type = self.ExperimentTypes.ASSEMBLY
            # TODO: long reads and hybrids
        if self.run:
            self.experiment_type = (
                self.run.experiment_type or self.ExperimentTypes.UNKNOWN
            )
        if prev_experiment_type != self.experiment_type:
            self.save()

    def import_from_pipeline_file_schema(
        self,
        schema,  # Type: PipelineFileSchema - no type hint due to circular import with workflows.data_io_utils.schemas.base
        file_path: Path,
    ) -> None:
        """
        Import data from a pipeline file schema into this analysis.

        :param schema: The pipeline file schema with import configuration (PipelineFileSchema)
        :param file_path: Path to the file to import
        """
        df = pd.read_csv(file_path, sep=CSVDelimiter.TAB)

        if schema.import_config.import_column:
            _ = df[schema.import_config.import_column].to_list()
        else:
            if schema.import_config.import_as_records:
                _ = df.to_dict(orient="records")
            else:
                _ = df.to_dict()

        # FIXME: Fix this one - data variable was removed as it's not being used
        # self.annotations[schema.import_config.annotations_key] = data
        self.save()

    # Pipeline type constants
    PIPELINE_ASSEMBLY = "assembly"
    PIPELINE_VIRIFY = "virify"
    PIPELINE_MAP = "map"

    def import_from_pipeline_results(
        self, base_path: Path, pipeline_type: str, validate_first: bool = True
    ) -> int:
        """
        Complete pipeline results import orchestrator.

        This is the central method that coordinates:
        - Pipeline schema selection
        - Structure validation (optional)
        - Annotation import
        - Download generation

        :param base_path: Path to pipeline results directory
        :param pipeline_type: Pipeline type ('assembly', 'virify', 'map')
        :param validate_first: Whether to validate structure before import
        :return: Number of downloads generated
        """
        logger.info(f"Importing {pipeline_type} results from {base_path}")

        # Get schema for validation
        schema = self.get_pipeline_schema(pipeline_type)

        # Validation (optional) - raises exception on failure
        if validate_first:
            schema.validate_results(base_path, self.assembly.first_accession)

        # Import annotations - raises exception on failure
        identifier = self.assembly.first_accession

        # Walk through schema directories and import files directly
        for dir_schema in schema.directories:
            self._import_from_directory(dir_schema, base_path / identifier, identifier)

        # Generate downloads - count successful ones
        downloads_generated = self._generate_downloads_from_schema(
            schema, base_path, identifier
        )

        self.save()

        logger.info(
            f"Successfully imported {pipeline_type} results with {downloads_generated} downloads"
        )
        return downloads_generated

    def get_pipeline_schema(self, pipeline_type: str):
        """
        Simple factory method to get appropriate pipeline schema.

        :param pipeline_type: Pipeline type string
        :return: Appropriate PipelineResultSchema instance
        :raises: ValueError for unknown pipeline types
        """
        from workflows.data_io_utils.schemas.assembly import AssemblyResultSchema
        from workflows.data_io_utils.schemas.virify import VirifyResultSchema
        from workflows.data_io_utils.schemas.map import MapResultSchema

        schemas = {
            self.PIPELINE_ASSEMBLY: AssemblyResultSchema,
            self.PIPELINE_VIRIFY: VirifyResultSchema,
            self.PIPELINE_MAP: MapResultSchema,
        }

        if pipeline_type not in schemas:
            raise ValueError(
                f"Unknown pipeline type: {pipeline_type}. Valid types: {list(schemas.keys())}"
            )

        return schemas[pipeline_type]()

    def _import_from_directory(
        self, dir_schema, base_path: Path, identifier: str  # PipelineDirectorySchema
    ):
        """
        Import annotations from a directory using its schema.

        :param dir_schema: PipelineDirectorySchema defining the directory structure
        :param base_path: Base path containing the directory
        :param identifier: Pipeline run identifier
        :raises: PipelineImportError if required files are missing
        """
        dir_path = base_path / dir_schema.folder_name

        if not dir_path.exists():
            # Check if directory is required based on validation rules
            if DirectoryExistsRule in dir_schema.validation_rules:
                # TODO: Should we define custom exception?
                raise ValueError(f"Required directory {dir_path} not found")
            return

        # Import files directly from schema
        for file_schema in dir_schema.files:
            if file_schema.import_config:  # Only import files that have import config
                file_path = dir_path / file_schema.get_filename(identifier)

                if file_path.exists():
                    self.import_from_pipeline_file_schema(file_schema, file_path)
                else:
                    # Check if file is required based on validation rules
                    if FileExistsRule in file_schema.validation_rules:
                        # TODO: Should we definea custom exception?
                        raise ValueError(f"Required file {file_path} not found")

        # Recurse into subdirectories
        for subdir_schema in dir_schema.subdirectories:
            self._import_from_directory(subdir_schema, dir_path, identifier)

    def _generate_downloads_from_schema(
        self, schema, base_path: Path, identifier: str  # PipelineResultSchema
    ) -> int:
        """
        Generate downloads from pipeline schema.

        :param schema: PipelineResultSchema defining the pipeline structure
        :param base_path: Base path containing the pipeline results
        :param identifier: Pipeline run identifier
        :return: Number of downloads generated
        """
        downloads_count = 0

        for dir_schema in schema.directories:
            downloads_count += self._generate_downloads_from_directory(
                dir_schema, base_path / identifier, identifier
            )

        return downloads_count

    def _generate_downloads_from_directory(
        self, dir_schema, base_path: Path, identifier: str  # PipelineDirectorySchema
    ) -> int:
        """
        Generate downloads from a directory schema.

        :param dir_schema: PipelineDirectorySchema defining the directory structure
        :param base_path: Base path containing the directory
        :param identifier: Pipeline run identifier
        :return: Number of downloads generated
        """
        downloads_count = 0
        dir_path = base_path / dir_schema.folder_name

        if not dir_path.exists():
            return 0

        # Generate downloads directly from schema files
        for file_schema in dir_schema.files:
            download_file = DownloadFile.from_pipeline_file_schema(
                file_schema, self, dir_path
            )
            if download_file:
                self.add_download(download_file)
                downloads_count += 1

        # Recurse into subdirectories
        for subdir_schema in dir_schema.subdirectories:
            downloads_count += self._generate_downloads_from_directory(
                subdir_schema, dir_path, identifier
            )

        return downloads_count

    class Meta:
        verbose_name_plural = "Analyses"

        indexes = [
            models.Index(
                name="idx_ready_and_not_suppressed",  # API queries might want this index for default queries
                fields=["is_ready", "is_suppressed"],
                condition=models.Q(is_ready=True, is_suppressed=False),
            )
        ]

    def __str__(self):
        return f"{self.accession} ({self.pipeline_version} {self.experiment_type})"


@receiver(post_save, sender=Analysis)
def on_analysis_saved(sender, instance: Analysis, created, **kwargs):
    """
    Whenever an Analysis is saved, determine its experiment type based on the runs/assemblies it is associated with.
    """
    if instance.experiment_type in [None, "", instance.ExperimentTypes.UNKNOWN]:
        instance.inherit_experiment_type()


@receiver(post_save, sender=Study)
def on_study_saved_update_analyses_suppression_states(
    sender, instance: Study, created, **kwargs
):
    """
    (Un)suppress the analyses associated with a Study whenever the Study is updated.
    All other models are directly related to ENA objects, so their suppression is handled directly.
    Analyses are different (no ENA accession/equivalent object) hence they follow this study-down propagation.
    This means there is no current way to suppress one analysis of a study, only entire studies.
    This is how ENA's documentation suggests suppression should work.
    """
    analyses_to_update_suppression_of = instance.analyses.exclude(
        is_suppressed=instance.is_suppressed
    )
    for analysis in analyses_to_update_suppression_of:
        logger.info(
            f"Setting is_suppressed to {instance.is_suppressed} on {analysis.accession} via {instance.accession}"
        )
        analysis.is_suppressed = instance.is_suppressed
    Analysis.objects.bulk_update(analyses_to_update_suppression_of, ["is_suppressed"])


class AnalysedContig(TimeStampedModel):
    analysis = models.ForeignKey(Analysis, on_delete=models.CASCADE)
    contig_id = models.CharField(max_length=255)
    coverage = models.FloatField()
    length = models.IntegerField()

    PFAMS = "pfams"
    KEGGS = "keggs"
    INTERPROS = "interpros"
    COGS = "cogs"
    GOS = "gos"
    ANTISMASH_GENE_CLUSTERS = "antismash_gene_clusters"

    @staticmethod
    def default_annotations():
        return {
            AnalysedContig.PFAMS: [],
            AnalysedContig.KEGGS: [],
            AnalysedContig.INTERPROS: [],
            AnalysedContig.COGS: [],
            AnalysedContig.GOS: [],
            AnalysedContig.ANTISMASH_GENE_CLUSTERS: [],
        }

    annotations = models.JSONField(default=default_annotations.__func__)


class SuperStudyImage(DbStoredFileField): ...


class SuperStudy(TimeStampedModel):
    slug = models.SlugField(unique=True, primary_key=True)
    title = models.CharField(max_length=255)
    description = models.TextField(blank=True)
    studies = models.ManyToManyField(
        Study, related_name="super_studies", through="SuperStudyStudy"
    )
    genome_catalogues = models.ManyToManyField(
        "genomes.GenomeCatalogue",
        related_name="super_studies",
        through="SuperStudyGenomeCatalogue",
    )
    # genome_catalogues #TODO
    logo = models.ImageField(
        upload_to="analyses.SuperStudyImage/bytes/filename/mimetype",
        blank=True,
        null=True,
        storage=storages["fieldfiles"],
    )

    class Meta:
        verbose_name_plural = "super studies"

    def save(self, *args, **kwargs):
        delete_file_if_needed(self, "logo")
        super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        super().delete(*args, **kwargs)
        delete_file(self, "logo")

    def __str__(self):
        return self.pk


class SuperStudyStudy(TimeStampedModel):
    study = models.ForeignKey(Study, on_delete=models.CASCADE)
    super_study = models.ForeignKey(SuperStudy, on_delete=models.CASCADE)
    is_flagship = models.BooleanField(default=True)

    class Meta:
        verbose_name = "Study in Super Study"
        verbose_name_plural = "Studies in Super Study"
        unique_together = (("study", "super_study"),)

    def __str__(self):
        return f"SuperStudyStudy {self.pk}: {self.study} in {self.super_study}"


class SuperStudyGenomeCatalogue(TimeStampedModel):
    genome_catalogue = models.ForeignKey(
        "genomes.GenomeCatalogue", on_delete=models.CASCADE
    )
    super_study = models.ForeignKey(SuperStudy, on_delete=models.CASCADE)

    class Meta:
        verbose_name = "Genome Catalogue in Super Study"
        verbose_name_plural = "Genome Catalogues in Super Study"
        unique_together = (("genome_catalogue", "super_study"),)

    def __str__(self):
        return f"SuperStudyGenomeCatalogue {self.pk}: {self.genome_catalogue} in {self.super_study}"


class StudyPublication(TimeStampedModel):
    study = models.ForeignKey(Study, on_delete=models.CASCADE)
    publication = models.ForeignKey("Publication", on_delete=models.CASCADE)


class Publication(TimeStampedModel):
    """
    Model for scientific publications related to studies.
    """

    pubmed_id = models.IntegerField(primary_key=True)
    title = models.CharField(max_length=740)
    published_year = models.SmallIntegerField(null=True, blank=True)

    class PublicationMetadata(BaseModel):
        """
        Pydantic schema for storing additional publication metadata in JSON.
        """

        authors: Optional[str] = Field(None)
        doi: Optional[str] = Field(None)
        isbn: Optional[str] = Field(None)
        iso_journal: Optional[str] = Field(None)
        pubmed_central_id: Optional[int] = Field(None)
        volume: Optional[str] = Field(None)
        pub_type: Optional[str] = Field(None)

    metadata = JSONFieldWithSchema(schema=PublicationMetadata)

    studies = models.ManyToManyField(
        Study, related_name="publications", through=StudyPublication
    )

    class Meta:
        verbose_name = "Publication"
        verbose_name_plural = "Publications"

    def __str__(self):
        return f"Publication {self.pk}: {Truncator(self.title).words(5)}"
