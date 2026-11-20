"""Public viral catalogue response schemas."""

from datetime import datetime
from typing import Annotated
from urllib.parse import quote

from ninja import Schema
from pydantic import Field

from emgapiv2.api.downloads import MGnifyDownloadFile
from viruses.occurrences import ViralFragmentOccurrence


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
    def resolve_plasmid_count(obj) -> int | None:
        """Read plasmid totals separately from viral fragments."""
        return obj.other_stats.get("plasmids")

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
    """Fragment quality, taxonomies and source provenance."""

    accession: str
    catalogue_id: str
    original_id: str
    cluster_representative: str
    length: int
    sequence_sha256: str
    nt_sequence_sha256: str
    cluster_size: int
    occurrence_count: int
    cluster_member_occurrence_count: int
    source_types: list[str]
    source_type: str
    source_accession: str
    source_genome_accession: str | None
    source_analysis_accession: str | None
    source_sample: str
    source_study: str
    source_lineage: str
    prediction_tool: str
    checkv_quality: str
    miuvig_quality: str
    completeness: float | None
    contamination: float | None
    is_provirus: bool
    sequence_type: str
    taxonomy: dict[str, str]

    @staticmethod
    def resolve_sequence_sha256(obj) -> str:
        """Expose the nucleotide checksum through the existing API field."""
        return obj.nt_sequence_sha256

    @staticmethod
    def resolve_cluster_representative(obj):
        return obj.accession

    @staticmethod
    def resolve_occurrence_count(obj):
        return len(obj.occurrences)

    @staticmethod
    def resolve_cluster_member_occurrence_count(obj):
        return len(obj.cluster_member_occurrences)

    @staticmethod
    def resolve_source_types(obj):
        return sorted(
            {
                ViralFragmentOccurrence.model_validate(item).source_type
                for item in [*obj.occurrences, *obj.cluster_member_occurrences]
            }
        )

    @staticmethod
    def resolve_original_id(obj):
        return primary_occurrence(obj).original_id

    @staticmethod
    def resolve_source_type(obj):
        return primary_occurrence(obj).source_type

    @staticmethod
    def resolve_source_accession(obj):
        return primary_occurrence(obj).source_accession

    @staticmethod
    def resolve_source_genome_accession(obj):
        return primary_occurrence(obj).source_genome_accession

    @staticmethod
    def resolve_source_analysis_accession(obj):
        return primary_occurrence(obj).analysis_accession

    @staticmethod
    def resolve_source_sample(obj):
        return primary_occurrence(obj).source_sample

    @staticmethod
    def resolve_source_study(obj):
        return primary_occurrence(obj).source_study

    @staticmethod
    def resolve_source_lineage(obj):
        return primary_occurrence(obj).source_lineage


class ViralFragmentDetail(ViralFragmentSchema):
    """The complete source metadata for one fragment."""

    metadata: dict
    occurrences: list[ViralFragmentOccurrence]
    cluster_member_occurrences: list[ViralFragmentOccurrence]


def primary_occurrence(obj) -> ViralFragmentOccurrence:
    """Provide the first representative detection for compatibility fields."""
    return ViralFragmentOccurrence.model_validate(obj.occurrences[0])
