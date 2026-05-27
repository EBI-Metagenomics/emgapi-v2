from datetime import datetime
from typing import TYPE_CHECKING, Optional

from ninja import Schema
from pydantic import Field

from genomes.models import Genome

if TYPE_CHECKING:
    from analyses.schemas import Biome


class GenomeBase(Schema):
    accession: str = Field(..., examples=["MGYG000000001"])
    ena_genome_accession: Optional[str]
    ena_sample_accession: Optional[str]
    ncbi_genome_accession: Optional[str]
    img_genome_accession: Optional[str]
    patric_genome_accession: Optional[str]
    length: int
    num_contigs: int
    num_genomes_total: int = None
    n_50: int
    gc_content: float
    type: Genome.GenomeType
    completeness: float
    contamination: float
    catalogue_id: str
    taxon_lineage: str = Field(..., serialization_alias="taxon-lineage")
    updated_at: datetime
    geographic_origin: Optional[str]
    geographic_range: Optional[list[str]] = []
    biome: Optional["Biome"] = None

    class Config:
        from_attributes = True
