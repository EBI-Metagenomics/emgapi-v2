from typing import Optional
from genomes.schemas.GenomeBase import GenomeBase
from analyses.schemas import MGnifyAnalysisDownloadFile
from genomes.schemas.GenomeCatalogue import GenomeCatalogueBase
from ninja import Field


class GenomeDetail(GenomeBase):
    downloads: list[MGnifyAnalysisDownloadFile] = Field(
        ..., alias="downloads_as_objects"
    )
    catalogue: Optional[GenomeCatalogueBase] = None
