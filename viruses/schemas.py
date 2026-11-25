"""Public viral catalogue response schemas."""

from datetime import datetime
from typing import Annotated
from urllib.parse import quote

from ninja import Schema
from pydantic import Field

from emgapiv2.api.downloads import MGnifyDownloadFile


class ViralDownloadFile(MGnifyDownloadFile):
    """A standard download with its catalogue URL."""

    path: Annotated[str, Field(exclude=True)]
    parent_identifier: Annotated[str | None, Field(exclude=True)] = None
    url: str


class ViralCatalogueSchema(Schema):
    """Catalogue metadata and counts for the imported release."""

    catalogue_id: str
    name: str
    version: str
    description: str
    biome: str
    ftp_url: str
    pipeline_version: str
    updated_at: datetime
    total_sequence_count: int
    fragment_count: int
    cluster_count: int
    virus_count: int
    provirus_count: int
    prophage_count: int
    plasmid_count: int | None
    other_stats: dict
    downloads: list[ViralDownloadFile]

    @staticmethod
    def resolve_plasmid_count(obj) -> int:
        return obj.plasmid_count

    @staticmethod
    def resolve_downloads(obj) -> list[dict]:
        """Resolve relative file paths against the public release directory."""
        base = obj.ftp_url.rstrip("/") + "/"
        return [
            {
                **download.model_dump(),
                "url": base + quote(str(download.path), safe="/"),
                "index_files": MGnifyDownloadFile.resolve_index_file_urls(
                    download, lambda path: base + quote(str(path), safe="/")
                ),
            }
            for download in obj.downloads_as_objects
        ]


class ViralFragmentSchema(Schema):
    """Fields needed for the representative list and its filters."""

    accession: str
    catalogue_id: str
    length: int
    cluster_size: int
    sequence_type: str
    checkv_quality: str
    completeness: float | None
    contamination: float | None
    source_types: list[str]

    @staticmethod
    def resolve_source_types(obj):
        return sorted(
            {
                "analysis" if row["Source"] == "assembly" else row["Source"]
                for row in obj.metadata_rows
            }
        )


class ViralFragmentDetail(ViralFragmentSchema):
    """Original metadata rows for a representative and its cluster members."""

    metadata_rows: list[dict[str, str]]
    metadata_url: str

    @staticmethod
    def resolve_metadata_url(obj):
        kind = "plasmids" if obj.sequence_type == "plasmid" else "viruses"
        return obj.catalogue.ftp_url.rstrip("/") + f"/{kind}/{kind}-all-metadata.tsv.gz"
