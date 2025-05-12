from ninja import Schema
from typing import Optional


class GenomeBase(Schema):
    accession: str
    ena_genome_accession: Optional[str]
    ena_sample_accession: Optional[str]
    ncbi_genome_accession: Optional[str]
    img_genome_accession: Optional[str]
    patric_genome_accession: Optional[str]
    biome_id: int
    length: int
    num_contigs: int
    n_50: int
    gc_content: float
    type: str
    completeness: float
    contamination: float
    catalogue_id: int