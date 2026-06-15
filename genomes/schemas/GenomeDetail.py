from typing import Optional

from ninja import Field

from genomes.schemas.GenomeBase import GenomeBase
from genomes.schemas.GenomeCatalogue import GenomeCatalogueBase
from genomes.schemas.MGnifyGenomeDownloadFile import MGnifyGenomeDownloadFile


class GenomeDetail(GenomeBase):
    # Additional fields to expose on the genome detail endpoint
    rna_5s: Optional[float] = None
    rna_5_8s: Optional[float] = None
    rna_16s: Optional[float] = None
    rna_18s: Optional[float] = None
    rna_23s: Optional[float] = None
    rna_28s: Optional[float] = None
    trnas: Optional[float] = None
    nc_rnas: Optional[float] = None
    eggnog_coverage: Optional[float] = None
    ipr_coverage: Optional[float] = None

    num_genomes_total: Optional[int] = None
    num_proteins: Optional[int] = None
    pangenome_size: Optional[int] = None
    pangenome_core_size: Optional[int] = None
    pangenome_accessory_size: Optional[int] = None

    downloads: list["MGnifyGenomeDownloadFile"] = Field(
        ..., alias="downloads_as_objects"
    )
    catalogue: Optional[GenomeCatalogueBase] = None
