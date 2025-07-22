from ninja import Schema, Field
from typing import Optional, Dict, Literal, TYPE_CHECKING
from datetime import datetime

if TYPE_CHECKING:
    from analyses.schemas import Biome

CatalogueType = Literal["prokaryotes", "eukaryotes", "viruses"]


class GenomeCatalogueBase(Schema):
    catalogue_id: str = Field(..., examples=["marine-v1-0"])
    version: str = Field(..., examples=["v1.0"])
    name: str = Field(..., examples=["Marine"])
    description: Optional[str]
    protein_catalogue_name: Optional[str] = Field(None, alias="Marine Proteins")
    protein_catalogue_description: Optional[str]
    updated_at: datetime
    result_directory: Optional[str]
    genome_count: Optional[int] = Field(None, alias="calculate_genome_count")
    unclustered_genome_count: Optional[int] = Field(
        ...,
        description="Total number of genomes in the catalogue, including non-cluster-representatives not available via this API.",
    )
    ftp_url: str
    pipeline_version_tag: str = Field(..., examples=["v2.5"])
    catalogue_biome_label: str = Field(..., examples=["Marine"])
    catalogue_type: CatalogueType
    other_stats: Optional[Dict] = Field(
        ..., examples=[{"Total proteins": "12,345,678"}]
    )
    biome: Optional["Biome"] = None


class GenomeCatalogueDetail(GenomeCatalogueBase): ...


class GenomeCatalogueList(GenomeCatalogueBase): ...
