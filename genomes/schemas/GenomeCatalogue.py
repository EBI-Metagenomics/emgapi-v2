from ninja import Schema
from typing import Optional, Dict
from datetime import datetime


class GenomeCatalogueBase(Schema):
    catalogue_id: str
    version: str
    name: str
    description: Optional[str]
    protein_catalogue_name: Optional[str]
    protein_catalogue_description: Optional[str]
    last_update: datetime
    result_directory: Optional[str]
    genome_count: Optional[int]
    unclustered_genome_count: Optional[int]
    ftp_url: str
    pipeline_version_tag: str
    catalogue_biome_label: str
    catalogue_type: str
    other_stats: Optional[Dict]

    class Config:
        from_attributes = True


class GenomeCatalogueDetail(GenomeCatalogueBase):
    biome_id: Optional[int]
