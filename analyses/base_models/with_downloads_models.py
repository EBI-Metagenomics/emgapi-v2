from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Union, Literal

from django.db import models
from pydantic import BaseModel, field_validator, Field

from emgapiv2.enum_utils import FutureStrEnum


class DownloadType(FutureStrEnum):
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
    OTHER = "other"


class DownloadFileIndexFile(BaseModel):
    """
    An index file (e.g., a .fai for a FASTA file of .gzi for a bgzip file) of a DownloadFile.
    """

    index_type: Literal["fai", "gzi"]
    path: Union[str, Path]

    @field_validator("path", mode="before")
    def coerce_path(cls, value):
        if isinstance(value, Path):
            return str(value)
        return value


class DownloadFile(BaseModel):
    """
    A download file schema for use in the `downloads` list.
    """

    path: Union[
        str, Path
    ]  # relative path from results dir of the object these downloads are for (e.g. the Analysis)
    alias: str = Field(
        ..., examples=["SILVA-SSU.tsv"]
    )  # an alias for the file, unique within the downloads list
    download_type: DownloadType
    file_type: DownloadFileType
    long_description: str = Field(..., examples=["A table of taxonomic assignments"])
    short_description: str = Field(..., examples=["Tax. assignments"])
    download_group: Optional[str] = Field(
        None, examples=["taxonomies.closed_reference.ssu"]
    )
    file_size_bytes: Optional[int] = Field(None, examples=[1024])
    index_file: Optional[DownloadFileIndexFile] = Field(None)

    parent_identifier: Optional[Union[str, int]] = (
        None  # e.g. the accession of an Analysis this download is for
    )

    @field_validator("path", mode="before")
    def coerce_path(cls, value):
        if isinstance(value, Path):
            return str(value)
        return value


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
