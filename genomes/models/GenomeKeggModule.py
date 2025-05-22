from django.db import models

from genomes.models.base_model import BaseModel


class GenomeKeggModule(BaseModel):

    name = models.CharField(db_column='MODULE_NAME', max_length=10,
                            unique=True)
    description = models.CharField(db_column='DESCRIPTION', max_length=200)

    class Meta:
        db_table = 'genome_kegg_module'

    def __str__(self):
        return self.name