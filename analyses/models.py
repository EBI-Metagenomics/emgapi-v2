from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import ClassVar, Union

from django.contrib.postgres.indexes import GinIndex
from django.core.exceptions import MultipleObjectsReturned, ObjectDoesNotExist
from django.db import models
from django.db.models import JSONField, Q, Func, Value
from django.db.models.signals import post_save
from django.dispatch import receiver
from django_ltree.models import TreeModel
from pydantic import BaseModel, ConfigDict, Field

import ena.models
from analyses.base_models.base_models import (
    ENADerivedManager,
    ENADerivedModel,
    PrivacyFilterManagerMixin,
    TimeStampedModel,
    VisibilityControlledModel,
)
from analyses.base_models.mgnify_accessioned_models import MGnifyAccessionField
from analyses.base_models.with_downloads_models import WithDownloadsModel
from analyses.base_models.with_status_models import SelectByStatusManagerMixin
from analyses.base_models.with_watchers_models import WithWatchersModel
from emgapiv2.async_utils import anysync_property
from emgapiv2.enum_utils import FutureStrEnum
from emgapiv2.model_utils import JSONFieldWithSchema
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
            logger.warning(
                f"Problem getting ENA study {ena_study_accession} from ENA models DB"
            )
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


class Run(TimeStampedModel, ENADerivedModel, WithExperimentTypeModel):
    CommonMetadataKeys = ENAReadRunFields
    CommonMetadataKeys.FASTQ_FTPS = (
        "fastq_ftps"  # plural convention mismatch to ENA; TODO
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
        if ena_library_strategy.lower() == "rna-seq" and (
            ena_library_source.lower() == "metagenomic"
            or ena_library_source.lower() == "metatranscriptomic"
        ):
            self.experiment_type = Run.ExperimentTypes.METATRANSCRIPTOMIC
        elif (
            ena_library_strategy.lower() == "wgs"
            and ena_library_source.lower() == "metatranscriptomic"
        ):
            self.experiment_type = Run.ExperimentTypes.METATRANSCRIPTOMIC
        elif (
            ena_library_strategy.lower() == "wgs"
            and ena_library_source.lower() == "metagenomic"
        ):
            self.experiment_type = Run.ExperimentTypes.METAGENOMIC
        elif (
            ena_library_strategy.lower() == "amplicon"
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


class Assembly(TimeStampedModel, ENADerivedModel):
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
    is_suppressed = models.BooleanField(default=False)

    GENOME_PROPERTIES = "genome_properties"
    GO_TERMS = "go_terms"
    GO_SLIMS = "go_slims"
    INTERPRO_IDENTIFIERS = "interpro_identifiers"
    KEGG_MODULES = "kegg_modules"
    KEGG_ORTHOLOGS = "kegg_orthologs"
    ANTISMASH_GENE_CLUSTERS = "antismash_gene_clusters"
    PFAMS = "pfams"
    RHEA_REACTIONS = "rhea_reactions"

    TAXONOMIES = "taxonomies"
    CLOSED_REFERENCE = "closed_reference"
    ASV = "asv"
    FUNCTIONAL_ANNOTATION = "functional_annotation"

    class TaxonomySources(FutureStrEnum):
        SSU: str = "ssu"
        LSU: str = "lsu"
        ITS_ONE_DB: str = "its_one_db"
        UNITE: str = "unite"
        PR2: str = "pr2"
        DADA2_SILVA: str = "dada2_silva"
        DADA2_PR2: str = "dada2_pr2"
        UNIREF: str = "uniref"

    TAXONOMIES_SSU = f"{TAXONOMIES}__{TaxonomySources.SSU.value}"
    TAXONOMIES_LSU = f"{TAXONOMIES}__{TaxonomySources.LSU.value}"
    TAXONOMIES_ITS_ONE_DB = f"{TAXONOMIES}__{TaxonomySources.ITS_ONE_DB.value}"
    TAXONOMIES_UNITE = f"{TAXONOMIES}__{TaxonomySources.UNITE.value}"
    TAXONOMIES_PR2 = f"{TAXONOMIES}__{TaxonomySources.PR2.value}"
    TAXONOMIES_DADA2_SILVA = f"{TAXONOMIES}__{TaxonomySources.DADA2_SILVA.value}"
    TAXONOMIES_DADA2_PR2 = f"{TAXONOMIES}__{TaxonomySources.DADA2_PR2.value}"
    TAXONOMIES_UNIREF = f"{TAXONOMIES}__{TaxonomySources.UNIREF.value}"

    ALLOWED_DOWNLOAD_GROUP_PREFIXES = [
        "all",  # catch-all for legacy
        f"{TAXONOMIES}.closed_reference.",
        f"{TAXONOMIES}.asv.",
        "quality_control",
        "primer_identification",
        "asv",
        FUNCTIONAL_ANNOTATION,
    ]

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
