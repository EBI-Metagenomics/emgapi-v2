from django.db import models
from genomes.models.BaseDownload import BaseDownload


class GenomeCatalogueDownload(BaseDownload):
    genome_catalogue = models.ForeignKey('GenomeCatalogue',
                                         db_column='GENOME_CATALOGUE_ID',
                                         on_delete=models.CASCADE)