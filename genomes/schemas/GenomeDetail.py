from typing import Optional, TYPE_CHECKING
from genomes.schemas.GenomeBase import GenomeBase
from genomes.schemas.GenomeCatalogue import GenomeCatalogueBase
from ninja import Field

if TYPE_CHECKING:
    from analyses.schemas import MGnifyAnalysisDownloadFile


class GenomeDetail(GenomeBase):
    downloads: list["MGnifyAnalysisDownloadFile"] = Field(
        ..., alias="downloads_as_objects"
    )
    catalogue: Optional[GenomeCatalogueBase] = None
