from typing import Optional, List
from genomes.schemas.GenomeBase import GenomeBase
from analyses.schemas import Biome, MGnifyAnalysisDownloadFile
from genomes.schemas.GenomeCatalogue import GenomeCatalogueBase
from ninja import Field


class GenomeDetail(GenomeBase):
    downloads: List[MGnifyAnalysisDownloadFile] = Field(
        ..., alias="downloads_as_objects"
    )
    geographic_origin: Optional[str]
    geographic_range: List[str]
    created_at: str = None
    updated_at: str = None
    biome: Optional[Biome] = None
    catalogue: Optional[GenomeCatalogueBase] = None
