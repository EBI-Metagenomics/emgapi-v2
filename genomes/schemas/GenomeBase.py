from ninja import Schema
from typing import Optional

from pydantic import Field

from analyses.schemas import Biome


class GenomeBase(Schema):
    accession: str = Field(..., examples=["MGYG000000001"])
    ena_genome_accession: Optional[str]
    ena_sample_accession: Optional[str]
    ncbi_genome_accession: Optional[str]
    img_genome_accession: Optional[str]
    patric_genome_accession: Optional[str]
    biome: Biome
    length: int
    num_contigs: int
    n_50: int
    gc_content: float
    type: str
    completeness: float
    contamination: float
    catalogue_id: int

    class Config:
        from_attributes = True
