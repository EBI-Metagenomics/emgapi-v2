from typing import Optional, List
from genomes.schemas.GenomeBase import GenomeBase
from analyses.schemas import Biome
from genomes.schemas.GenomeCatalogue import GenomeCatalogueBase


class GenomeDetail(GenomeBase):
    geographic_origin: Optional[str]
    geographic_range: List[str]
    last_update: str = None
    first_created: str = None
    biome: Optional[Biome] = None
    catalogue: Optional[GenomeCatalogueBase] = None

    class Config:
        from_attributes = True
        alias_generator = lambda field_name: field_name + "_iso" if field_name in ["last_update", "first_created"] else field_name
