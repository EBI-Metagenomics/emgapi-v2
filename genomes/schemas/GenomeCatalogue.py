from ninja import Schema, Field
from typing import Optional, Dict, Literal
from datetime import datetime

CatalogueType = Literal["prokaryotes", "eukaryotes", "viruses"]


class GenomeCatalogueBase(Schema):
    catalogue_id: str
    version: str
    name: str
    description: Optional[str]
    protein_catalogue_name: Optional[str]
    protein_catalogue_description: Optional[str]
    updated_at: datetime
    result_directory: Optional[str]
    genome_count: Optional[int] = Field(None, alias="calculate_genome_count")
    unclustered_genome_count: Optional[int]
    ftp_url: str
    pipeline_version_tag: str
    catalogue_biome_label: str
    catalogue_type: CatalogueType
    other_stats: Optional[Dict]


class GenomeCatalogueDetail(GenomeCatalogueBase):
    biome_id: Optional[int]


class GenomeCatalogueList(GenomeCatalogueBase):
    biome_id: Optional[int]
