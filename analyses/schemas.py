from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Union, Dict, Any, TypeVar, Generic
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

from django.conf import settings
from ninja import Field, ModelSchema, Schema
from pydantic import field_validator, BaseModel
from typing_extensions import Annotated

import analyses.models
from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadFileIndexFile,
)
from emgapiv2.api.storage import private_storage
from emgapiv2.enum_utils import FutureStrEnum
from workflows.data_io_utils.filenames import trailing_slash_ensured_dir


logger = logging.getLogger(__name__)

EMG_CONFIG = settings.EMG_CONFIG


class Biome(ModelSchema):
    biome_name: str = Field(..., examples=["Engineered", "Mammals"])
    lineage: str = Field(
        None, examples=["root:Engineered", "root:Host-associated:Mammals"]
    )

    @staticmethod
    def resolve_lineage(obj: analyses.models.Biome) -> str:
        return obj.pretty_lineage

    class Meta:
        model = analyses.models.Biome
        fields = ["biome_name"]


class MGnifyStudy(ModelSchema):
    accession: str = Field(..., examples=["MGYS00000001"])
    ena_accessions: List[str] = Field(..., examples=[["SRP135937", "PRJNA438545"]])
    title: str = Field(..., examples=["ISS Metagenomes"])
    biome: Optional[Biome]
    updated_at: datetime = Field(
        ...,
        examples=[
            datetime(
                1998, 11, 20, 9, 40, 00, tzinfo=ZoneInfo("Europe/Moscow")
            ).isoformat()
        ],
    )

    @staticmethod
    def resolve_biome_name(obj: analyses.models.Study):
        return obj.biome.biome_name if obj.biome else None

    class Meta:
        model = analyses.models.Study
        fields = ["accession", "ena_accessions", "title", "biome"]
        fields_optional = ["ena_study"]


class MGnifyStudyDetail(MGnifyStudy):
    downloads: List[MGnifyStudyDownloadFile] = Field(..., alias="downloads_as_objects")

    class Meta:
        model = analyses.models.Study
        fields = [
            "accession",
            "ena_study",
            "title",
        ]


T = TypeVar("T", bound=str)


class OrderByFilter(BaseModel, Generic[T]):
    """
    Usage:
    MyModelOrderBy = OrderByFilter[Literal["name", "-name", "created_at", "-created_at", ""]]

    @router.get("/items")
    def list_my_models(request, order: MyModelOrderBy = Query(...)):
    """

    order: Optional[T] = None

    def order_by(self, qs):
        if self.order:
            return qs.order_by(self.order)
        return qs


class MGnifySample(ModelSchema):
    accession: str = Field(
        ..., alias="first_accession", examples=["ERS000001", "SAMEA000000001"]
    )
    ena_accessions: List[str] = Field(
        ..., examples=[["ERS000001", "SAMEA000000001"], ["ERS000002"]]
    )

    class Meta:
        model = analyses.models.Sample
        fields = ["updated_at"]


class MGnifySampleDetail(MGnifySample):
    studies: List[MGnifyStudy]

    class Meta(MGnifySample.Meta): ...


class MGnifyDownloadFileIndexFile(Schema, DownloadFileIndexFile):
    path: Annotated[str, Field(exclude=True)]
    relative_url: str = Field(
        None,
        description="URL of the index file, relative to the DownloadFile it relates to.",
        examples=["annotations.tsv.gz.gzi"],
    )

    @staticmethod
    def resolve_relative_url(obj: DownloadFileIndexFile):
        # NB! This assumes that the index file is ALWAYS a sibling of the file it indexes.
        # Generally fine, but if not we would need to know about the parent DownloadFile object
        #  in this resolver, to calculate a true relative path or an absolute URL.
        return Path(obj.path).name


class MGnifyAnalysisDownloadFile(Schema, DownloadFile):
    path: Annotated[str, Field(exclude=True)]
    parent_identifier: Annotated[Union[int, str], Field(exclude=True)]
    index_file: Optional[MGnifyDownloadFileIndexFile] = None

    url: str = Field(
        None,
        examples=[
            urljoin(
                EMG_CONFIG.service_urls.transfer_services_url_root, "annotations.tsv.gz"
            )
        ],
    )

    @field_validator("index_file", mode="before")
    def coerce_index_file(cls, value):
        if isinstance(value, DownloadFileIndexFile):
            return MGnifyDownloadFileIndexFile.model_validate(value.model_dump())
        return value

    @staticmethod
    def resolve_url(obj: MGnifyAnalysisDownloadFile):
        analysis = analyses.models.Analysis.objects.get(accession=obj.parent_identifier)
        if not analysis:
            logger.warning(
                f"No parent Analysis object found with identified {obj.parent_identifier}"
            )
            return None

        if analysis.is_private:
            private_path = Path(analysis.external_results_dir) / obj.path
            return private_storage.url(private_path)

        return urljoin(
            EMG_CONFIG.service_urls.transfer_services_url_root,
            urljoin(
                trailing_slash_ensured_dir(analysis.external_results_dir), obj.path
            ),
        )


class MGnifyStudyDownloadFile(MGnifyAnalysisDownloadFile):
    path: Annotated[str, Field(exclude=True)]
    parent_identifier: Annotated[Union[int, str], Field(exclude=True)]

    url: str = None

    @staticmethod
    def resolve_url(obj: MGnifyStudyDownloadFile):
        study = analyses.models.Study.objects.get(accession=obj.parent_identifier)
        if not study:
            logger.warning(
                f"No parent Study object found with identified {obj.parent_identifier}"
            )
            return None

        if study.is_private:
            private_path = Path(study.external_results_dir) / obj.path
            return private_storage.url(private_path)

        return f"{EMG_CONFIG.service_urls.transfer_services_url_root.rstrip('/')}/{study.external_results_dir}/{obj.path}"


class AnalysedRun(ModelSchema):
    accession: str = Field(..., alias="first_accession", examples=["ERR0000001"])
    instrument_model: Optional[str] = Field(..., examples=["Illumina HiSeq 2000"])
    instrument_platform: Optional[str] = Field(..., examples=["Illumina"])

    class Meta:
        model = analyses.models.Run
        fields = ["instrument_model", "instrument_platform"]


class Assembly(ModelSchema):
    accession: str = Field(..., alias="first_accession", examples=["ERZ000001"])

    class Meta:
        model = analyses.models.Assembly
        fields = ["updated_at"]


class MGnifyAnalysis(ModelSchema):
    study_accession: str = Field(..., alias="study_id", examples=["MGYS000000001"])

    accession: str = Field(..., examples=["MGYA000000001"])
    experiment_type: str = Field(
        ...,
        examples=[
            label for _, label in analyses.models.Analysis.ExperimentTypes.choices
        ],
        description="Experiment type refers to the type of sequencing data that was analysed, e.g. amplicon reads or a metagenome assembly",
        alias="get_experiment_type_display",
    )
    run: Optional[AnalysedRun]
    sample: Optional[MGnifySample]
    assembly: Optional[Assembly]
    pipeline_version: Optional[analyses.models.Analysis.PipelineVersions]

    class Meta:
        model = analyses.models.Analysis
        fields = ["accession", "experiment_type"]


class MGnifyAnalysisDetail(MGnifyAnalysis):
    downloads: List[MGnifyAnalysisDownloadFile] = Field(
        ..., alias="downloads_as_objects"
    )
    read_run: Optional[AnalysedRun] = Field(
        ...,
        alias="raw_run",
        description="Metadata associated with the original read run this analysis is based on, whether or not those reads were assembled.",
    )
    quality_control_summary: Optional[dict] = Field(
        ...,
        alias="quality_control",
        examples=[
            {
                "before_filtering": {"total_bases": 1000000},
                "after_filtering": {"total_bases": 700000},
            }
        ],
    )

    results_dir: Optional[str] = Field(
        None,
        description="Directory path where analysis results are stored",
        examples=["http://example.org/data/analyses/MGYA00000001/results"],
    )

    @staticmethod
    def resolve_results_dir(obj: analyses.models.Analysis) -> str:
        return urljoin(
            settings.EMG_CONFIG.service_urls.transfer_services_url_root,
            obj.external_results_dir,
        )

    metadata: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional metadata associated with the analysis",
        examples=[{"marker_gene_summary": {"ssu": {"total_read_count": 11}}}],
    )

    class Meta:
        model = analyses.models.Analysis
        fields = [
            "accession",
            "sample_accession",
            "assembly_accession",
            "experiment_type",
            "pipeline_version",
        ]


class MGnifyAnalysisTypedAnnotation(Schema):
    count: Optional[int] = None  # sometimes it is just presence with no count
    description: Optional[str] = None  # for functional
    organism: Optional[str] = None  # for taxonomic


class MGnifyAnalysisWithAnnotations(MGnifyAnalysisDetail):
    annotations: dict[
        str,
        Union[
            List[MGnifyAnalysisTypedAnnotation],
            dict[str, Optional[List[MGnifyAnalysisTypedAnnotation]]],
        ],
    ] = Field(
        ...,
        examples=[
            {
                "pfams": [
                    {
                        "count": 1,
                        "description": "PFAM1",
                        "organism": None,
                    }
                ],
                "taxonomics": {
                    "lsu": [
                        {
                            "count": 1,
                            "description": None,
                            "organism": "Bacteria",
                        }
                    ]
                },
            }
        ],
    )

    class Meta:
        model = analyses.models.Analysis
        fields = ["accession", "annotations"]


class MGnifyAssemblyAnalysisRequestCreate(ModelSchema):
    class Meta:
        model = analyses.models.AssemblyAnalysisRequest
        fields = ["requestor", "request_metadata"]


class MGnifyAssemblyAnalysisRequest(ModelSchema):
    class Meta:
        model = analyses.models.AssemblyAnalysisRequest
        fields = ["requestor", "status", "study", "request_metadata", "id"]


class MGnifyFunctionalAnalysisAnnotationType(FutureStrEnum):
    genome_properties: str = analyses.models.Analysis.GENOME_PROPERTIES
    go_terms: str = analyses.models.Analysis.GO_TERMS
    go_slims: str = analyses.models.Analysis.GO_SLIMS
    interpro_identifiers: str = analyses.models.Analysis.INTERPRO_IDENTIFIERS
    kegg_modules: str = analyses.models.Analysis.KEGG_MODULES
    kegg_orthologs: str = analyses.models.Analysis.KEGG_ORTHOLOGS
    taxonomies_ssu: str = analyses.models.Analysis.TAXONOMIES_SSU
    taxonomies_lsu: str = analyses.models.Analysis.TAXONOMIES_LSU
    taxonomies_itsonedb: str = analyses.models.Analysis.TAXONOMIES_ITS_ONE_DB
    taxonomies_unite: str = analyses.models.Analysis.TAXONOMIES_UNITE
    taxonomies_pr2: str = analyses.models.Analysis.TAXONOMIES_PR2
    taxonomies_dada2_pr2: str = analyses.models.Analysis.TAXONOMIES_DADA2_PR2
    taxonomies_dada2_silva: str = analyses.models.Analysis.TAXONOMIES_DADA2_SILVA
    antismash_gene_clusters: str = analyses.models.Analysis.ANTISMASH_GENE_CLUSTERS
    pfams: str = analyses.models.Analysis.PFAMS


class StudyAnalysisIntent(Schema):
    study_accession: str


# AnalysisSchema = create_model()


# class MGnifyAnalysisWithTypedAnnotations(Schema):
#     accession: str
#     annotations_list = List[MGnifyAnalysisTypedAnnotation]
