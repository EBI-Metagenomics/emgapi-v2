from typing import Optional
from genomes.schemas.GenomeBase import GenomeBase
from genomes.schemas.GenomeCatalogue import GenomeCatalogueBase
from ninja import Field
from genomes.schemas.MGnifyGenomeDownloadFile import MGnifyGenomeDownloadFile


class GenomeDetail(GenomeBase):
    downloads: list["MGnifyGenomeDownloadFile"] = Field(
        ..., alias="downloads_as_objects"
    )
    catalogue: Optional[GenomeCatalogueBase] = None
