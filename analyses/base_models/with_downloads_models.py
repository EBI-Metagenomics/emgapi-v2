from __future__ import annotations

from operator import attrgetter
from pathlib import Path
from typing import List, Optional, Union, Literal, TYPE_CHECKING

from django.db import models
from pydantic import BaseModel, field_validator, Field

from emgapiv2.enum_utils import FutureStrEnum
from workflows.data_io_utils.file_rules.common_rules import FileExistsRule

if TYPE_CHECKING:
    from workflows.data_io_utils.schemas.base import PipelineFileSchema


class DownloadType(FutureStrEnum):
    CODING_SEQUENCES = "Coding Sequences"
    SEQUENCE_DATA = "Sequence data"
    QUALITY_CONTROL = "Quality control"
    FUNCTIONAL_ANALYSIS = "Functional analysis"
    TAXONOMIC_ANALYSIS = "Taxonomic analysis"
    STATISTICS = "Statistics"
    NON_CODING_RNAS = "non-coding RNAs"
    GENOME_ANALYSIS = "Genome analysis"
    RO_CRATE = "Analysis RO Crate"
    OTHER = "Other"


class DownloadFileType(FutureStrEnum):
    FASTA = "fasta"
    TSV = "tsv"
    BIOM = "biom"
    CSV = "csv"
    JSON = "json"
    SVG = "svg"
    TREE = "tree"  # e.g. newick
    HTML = "html"
    GFF = "gff"
    OTHER = "other"


class DownloadFileIndexFileMetadata(BaseModel):
    index_type: Literal["fai", "gzi", "csi"]


class DownloadFileIndexFile(DownloadFileIndexFileMetadata):
    """
    An index file (e.g., a .fai for a FASTA file of .gzi for a bgzip file) of a DownloadFile.
    """

    path: Union[str, Path]

    @field_validator("path", mode="before")
    def coerce_path(cls, value):
        if isinstance(value, Path):
            return str(value)
        return value

    @classmethod
    def from_indexed_file_path_and_metadata(
        cls, indexed_file_path: Path, metadata: DownloadFileIndexFileMetadata
    ):
        return cls(
            path=indexed_file_path.with_suffix(
                f"{indexed_file_path.suffix}.{metadata.index_type}"
            ),
            **metadata.model_dump(),
        )


class DownloadFileMetadata(BaseModel):
    """
    Metadata configuration for generating DownloadFile objects.

    This class separates download metadata from validation logic,
    providing a clean way to configure how pipeline outputs should
    be exposed as downloadable files.
    """

    file_type: DownloadFileType = Field(
        ..., description="File type of the downloadable file, e.g. its file extension"
    )
    download_type: DownloadType = Field(..., description="Category of the download")
    download_group: Optional[str] = Field(
        None,
        description="Group identifier for the download",
        examples=["taxonomies.closed_reference.ssu"],
    )
    short_description: str = Field(
        ..., description="Brief description of the file", examples=["Tax. assignments"]
    )
    long_description: str = Field(
        ...,
        description="Detailed description of the file",
        examples=["A table of taxonomic assignments"],
    )


class DownloadFile(DownloadFileMetadata):
    """
    A download file schema for use in the `downloads` list.

    Extends DownloadFileMetadata with runtime fields like path and file size.
    """

    path: Union[
        str, Path
    ]  # relative path from results dir of the object these downloads are for (e.g. the Analysis)
    alias: str = Field(
        ..., examples=["SILVA-SSU.tsv"]
    )  # an alias for the file, unique within the downloads list
    file_size_bytes: Optional[int] = Field(None, examples=[1024])
    index_file: Optional[DownloadFileIndexFile | list[DownloadFileIndexFile]] = Field(
        None
    )

    parent_identifier: Optional[Union[str, int]] = (
        None  # e.g. the accession of an Analysis this download is for
    )

    @field_validator("path", mode="before")
    def coerce_path(cls, value):
        if isinstance(value, Path):
            return str(value)
        return value

    @classmethod
    def from_pipeline_file_schema(
        cls,
        schema: "PipelineFileSchema",
        parent_object: "WithDownloadsModel",
        directory_path: Path,
        base_path: Path,
        file_identifier_lookup_string: str = "accession",
        parent_identifier_lookup_string: str = "accession",
    ) -> Optional["DownloadFile"]:
        """
        Factory method to create a DownloadFile from a PipelineFileSchema.

        TODO: I want (mbc) to review this method paths args. I'm not convinced we need both, some cleaning upstream
        should help

        :param schema: The pipeline file schema
        :type schema: PipelineFileSchema
        :param parent_object: The pipeline output object that is the parent of the file, e.g. an Analysis or Genome
        :type parent_object: any concrete model of the abstract WithDownloadsModel
        :param directory_path: Full path to the directory containing the file
        :type directory_path: Path
        :param base_path: Base path for computing relative file paths (e.g., batch workspace or analysis.results_dir)
        :type base_path: Path
        :param file_identifier_lookup_string: The attribute name to use to get the file identifier from the parent object, defaults to "accession", but can be a relation e.g. "assembly.first_accession"
        :type file_identifier_lookup_string: str, optional
        :param parent_identifier_lookup_string: The attribute name to use to get the parent identifier from the parent object, defaults to "accession", but can be anything e.g. "id" or "other.accession"
        :type parent_identifier_lookup_string: str, optional
        :return: DownloadFile object or None if file doesn't exist and isn't required
        :rtype: Optional[DownloadFile]
        """
        file_identifier = attrgetter(file_identifier_lookup_string)(parent_object)
        file_path = directory_path / schema.get_filename(file_identifier)

        if not file_path.exists():
            # Check if a file is required based on validation rules
            if FileExistsRule in schema.validation_rules:
                raise FileNotFoundError(f"Required file {file_path} not found")
            return None

        path = file_path.relative_to(base_path)
        download_file = cls(
            path=path,
            file_type=schema.download_metadata.file_type,
            alias=file_path.name,
            download_type=schema.download_metadata.download_type,
            download_group=schema.download_metadata.download_group,
            parent_identifier=attrgetter(parent_identifier_lookup_string)(
                parent_object
            ),
            short_description=schema.download_metadata.short_description,
            long_description=schema.download_metadata.long_description,
            index_file=[
                DownloadFileIndexFile.from_indexed_file_path_and_metadata(
                    path, metadata
                )
                for metadata in schema.index_files or []
            ],
        )
        for index in download_file.index_file:
            if (
                not (index_full_path := base_path / index.path).exists()
                and FileExistsRule in schema.validation_rules
            ):
                raise FileNotFoundError(
                    f"Required index file {index_full_path} not found"
                )
        return download_file


class WithDownloadsModel(models.Model):
    """
    Abstract model providing a `downloads` field which is a list of lightly schema'd downloadable files.

     e.g.

        sequence_file = DownloadFile(
            path='sequence_data/sequences.fasta',
            alias='err1.fasta',
            download_type=DownloadType.SEQUENCE_DATA,
            file_type=DownloadFileType.FASTA
        )

        my_model.add_download(sequence_file)
    """

    DOWNLOAD_PARENT_IDENTIFIER_ATTR: str = None

    ALLOWED_DOWNLOAD_GROUP_PREFIXES: List[str] = None

    downloads = models.JSONField(blank=True, default=list)

    def add_download(self, download: DownloadFile):
        if download.alias in [dl.get("alias") for dl in self.downloads]:
            raise FileExistsError(
                f"Duplicate download alias found in {self}.downloads list - not adding {download.path}"
            )

        if (
            download.download_group
            and self.ALLOWED_DOWNLOAD_GROUP_PREFIXES
            and not any(
                download.download_group.startswith(prefix)
                for prefix in self.ALLOWED_DOWNLOAD_GROUP_PREFIXES
            )
        ):
            raise ValueError(
                f"Download group {download.download_group} is not allowed for model {self.__class__.__name__}: only prefixes {self.ALLOWED_DOWNLOAD_GROUP_PREFIXES}"
            )

        self.downloads.append(download.model_dump(exclude={"parent_identifier"}))
        self.save()

    @property
    def downloads_as_objects(self) -> List[DownloadFile]:
        return [
            DownloadFile.model_validate(
                dict(
                    **dl,
                    parent_identifier=getattr(
                        self, self.DOWNLOAD_PARENT_IDENTIFIER_ATTR
                    ),
                )
            )
            for dl in self.downloads
        ]

    class Meta:
        abstract = True
