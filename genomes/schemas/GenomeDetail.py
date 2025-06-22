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
    last_update: str = None
    first_created: str = None
    biome: Optional[Biome] = None
    catalogue: Optional[GenomeCatalogueBase] = None

    class Config:
        from_attributes = True

        @staticmethod
        def alias_generator(field_name):
            if field_name in ["last_update", "first_created"]:
                return field_name + "_iso"
            return field_name
