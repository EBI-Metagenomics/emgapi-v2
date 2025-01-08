from __future__ import annotations

import logging
import os
import re
from enum import Enum
from pathlib import Path
from typing import ClassVar, Union

from asgiref.sync import sync_to_async
from django.core.exceptions import MultipleObjectsReturned, ObjectDoesNotExist
from django.db import models
from django.db.models import JSONField, Q
from django.db.models.signals import post_save
from django.dispatch import receiver
from django_ltree.models import TreeModel

import ena.models
from analyses.base_models.base_models import (
    ENADerivedManager,
    ENADerivedModel,
    MGnifyAutomatedModel,
    PrivacyFilterManagerMixin,
    TimeStampedModel,
    VisibilityControlledModel,
)
from analyses.base_models.mgnify_accessioned_models import MGnifyAccessionField
from analyses.base_models.with_downloads_models import WithDownloadsModel
from emgapiv2.async_utils import anysync_property

# Some models associated with MGnify Analyses (MGYS, MGYA etc).


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


class StudyManager(models.Manager):
    async def get_or_create_for_ena_study(self, ena_study_accession):
        logging.info(f"Will get/create MGnify study for {ena_study_accession}")
        try:
            ena_study = await ena.models.Study.objects.filter(
                Q(accession=ena_study_accession)
                | Q(additional_accessions__icontains=ena_study_accession)
            ).afirst()
            logging.debug(f"Got {ena_study}")
        except (MultipleObjectsReturned, ObjectDoesNotExist) as e:
            logging.warning(
                f"Problem getting ENA study {ena_study_accession} from ENA models DB"
            )
        study, _ = await Study.objects.aget_or_create(
            ena_study=ena_study, title=ena_study.title
        )
        await sync_to_async(study.inherit_accessions_from_related_ena_object)(
            "ena_study"
        )
        return study


class PublicStudyManagerMixin(PrivacyFilterManagerMixin, StudyManager):
    """
    A custom manager that filters out private studies by default.
    """

    pass


class Study(MGnifyAutomatedModel, ENADerivedModel, TimeStampedModel):
    objects = PublicStudyManagerMixin()
    all_objects = models.Manager()

    accession = MGnifyAccessionField(
        accession_prefix="MGYS", accession_length=8, db_index=True
    )
    ena_study = models.ForeignKey(
        ena.models.Study, on_delete=models.CASCADE, null=True, blank=True
    )
    biome = models.ForeignKey(Biome, on_delete=models.CASCADE, null=True, blank=True)
    title = models.CharField(max_length=255)

    def __str__(self):
        return self.accession

    class Meta:
        verbose_name_plural = "studies"


class Sample(MGnifyAutomatedModel, ENADerivedModel, TimeStampedModel):
    ena_sample = models.ForeignKey(ena.models.Sample, on_delete=models.CASCADE)

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


class Run(
    TimeStampedModel, ENADerivedModel, MGnifyAutomatedModel, WithExperimentTypeModel
):
    class CommonMetadataKeys:
        INSTRUMENT_PLATFORM = "instrument_platform"
        INSTRUMENT_MODEL = "instrument_model"
        FASTQ_FTPS = "fastq_ftps"
        LIBRARY_STRATEGY = "library_strategy"
        LIBRARY_LAYOUT = "library_layout"
        LIBRARY_SOURCE = "library_source"
        SCIENTIFIC_NAME = "scientific_name"

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

    def set_experiment_type_by_ena_library_strategy(self, ena_library_strategy: str):
        if ena_library_strategy.lower() == "rna-seq":
            self.experiment_type = Run.ExperimentTypes.METATRANSCRIPTOMIC
        elif ena_library_strategy.lower() == "wgs":
            self.experiment_type = Run.ExperimentTypes.METAGENOMIC
        elif ena_library_strategy.lower() == "amplicon":
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

    NAME_CHOICES = [
        (METASPADES, "MetaSPAdes"),
        (MEGAHIT, "MEGAHIT"),
        (SPADES, "SPAdes"),
    ]

    assembler_default: ClassVar[str] = METASPADES

    name = models.CharField(max_length=20, null=True, blank=True, choices=NAME_CHOICES)
    version = models.CharField(max_length=20)

    def save(self, *args, **kwargs):
        self.clean()
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.name} {self.version}" if self.version is not None else self.name


class AssemblyManager(ENADerivedManager):
    def get_queryset(self):
        return super().get_queryset().select_related("run")


class Assembly(TimeStampedModel, ENADerivedModel):
    objects = AssemblyManager()

    dir = models.CharField(max_length=200, null=True, blank=True)
    run = models.ForeignKey(
        Run, on_delete=models.CASCADE, related_name="assemblies", null=True, blank=True
    )
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

    class AssemblyStates(str, Enum):
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
                check=Q(reads_study__isnull=False) | Q(assembly_study__isnull=False),
                name="at_least_one_study_present",
            )
        ]

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


class AnalysisManagerDeferringAnnotations(models.Manager):
    """
    The annotations field is a potentially large JSONB field.
    Defer it by default, since most queries don't need to transfer this large dataset.
    """

    def get_queryset(self):
        return super().get_queryset().defer("annotations")


class AnalysisManagerIncludingAnnotations(models.Manager):
    def get_queryset(self):
        return super().get_queryset()


class PublicAnalysisManagerMixin(
    PrivacyFilterManagerMixin, AnalysisManagerDeferringAnnotations
):
    """
    A custom manager that filters out private analyses by default.
    """

    pass


class PublicAnalysisManagerMixinIncludingAnnotations(
    PrivacyFilterManagerMixin, AnalysisManagerIncludingAnnotations
):
    """
    A custom manager that includes annotations but still filters out private analyses by default.
    """

    pass


class Analysis(
    MGnifyAutomatedModel,
    TimeStampedModel,
    VisibilityControlledModel,
    WithDownloadsModel,
    WithExperimentTypeModel,
):
    objects = PublicAnalysisManagerMixin()
    objects_and_annotations = PublicAnalysisManagerMixinIncludingAnnotations()

    all_objects = AnalysisManagerDeferringAnnotations()
    all_objects_and_annotations = AnalysisManagerIncludingAnnotations()

    DOWNLOAD_PARENT_IDENTIFIER_ATTR = "accession"

    accession = MGnifyAccessionField(accession_prefix="MGYA", accession_length=8)

    suppression_following_fields = ["sample"]
    study = models.ForeignKey(
        Study, on_delete=models.CASCADE, to_field="accession", related_name="analyses"
    )
    results_dir = models.CharField(max_length=100)
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

    GENOME_PROPERTIES = "genome_properties"
    GO_TERMS = "go_terms"
    GO_SLIMS = "go_slims"
    INTERPRO_IDENTIFIERS = "interpro_identifiers"
    KEGG_MODULES = "kegg_modules"
    KEGG_ORTHOLOGS = "kegg_orthologs"
    ANTISMASH_GENE_CLUSTERS = "antismash_gene_clusters"
    PFAMS = "pfams"

    TAXONOMIES = "taxonomies"

    class TaxonomySources(Enum):
        SSU: str = "ssu"
        LSU: str = "lsu"
        ITS_ONE_DB: str = "its_one_db"
        UNITE: str = "unite"
        PR2: str = "pr2"
        DADA2_SILVA: str = "dada2_silva"
        DADA2_PR2: str = "dada2_pr2"

    TAXONOMIES_SSU = f"{TAXONOMIES}__{TaxonomySources.SSU.value}"
    TAXONOMIES_LSU = f"{TAXONOMIES}__{TaxonomySources.LSU.value}"
    TAXONOMIES_ITS_ONE_DB = f"{TAXONOMIES}__{TaxonomySources.ITS_ONE_DB.value}"
    TAXONOMIES_UNITE = f"{TAXONOMIES}__{TaxonomySources.UNITE.value}"
    TAXONOMIES_PR2 = f"{TAXONOMIES}__{TaxonomySources.PR2.value}"
    TAXONOMIES_DADA2_SILVA = f"{TAXONOMIES}__{TaxonomySources.DADA2_SILVA.value}"
    TAXONOMIES_DADA2_PR2 = f"{TAXONOMIES}__{TaxonomySources.DADA2_PR2.value}"

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

    class PipelineVersions(models.TextChoices):
        v5 = "V5", "v5.0"
        v6 = "V6", "v6.0"

    pipeline_version = models.CharField(
        choices=PipelineVersions, max_length=5, default=PipelineVersions.v6
    )

    class AnalysisStates(str, Enum):
        ANALYSIS_STARTED = "analysis_started"
        ANALYSIS_COMPLETED = "analysis_completed"
        ANALYSIS_BLOCKED = "analysis_blocked"
        ANALYSIS_FAILED = "analysis_failed"
        ANALYSIS_QC_FAILED = "analysis_qc_failed"
        ANALYSIS_POST_SANITY_CHECK_FAILED = "analysis_post_sanity_check_failed"

        @classmethod
        def default_status(cls):
            return {
                cls.ANALYSIS_STARTED: False,
                cls.ANALYSIS_COMPLETED: False,
                cls.ANALYSIS_BLOCKED: False,
                cls.ANALYSIS_FAILED: False,
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
        if self.assembly:
            self.experiment_type = self.ExperimentTypes.ASSEMBLY
            # TODO: long reads and hybrids
        if self.run:
            self.experiment_type = (
                self.run.experiment_type or self.ExperimentTypes.UNKNOWN
            )
        self.save()

    class Meta:
        verbose_name_plural = "Analyses"

    def __str__(self):
        return f"{self.accession} ({self.pipeline_version} {self.experiment_type})"


@receiver(post_save, sender=Analysis)
def on_analysis_saved(sender, instance: Analysis, created, **kwargs):
    if instance.experiment_type in [None, "", instance.ExperimentTypes.UNKNOWN]:
        instance.inherit_experiment_type()


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
