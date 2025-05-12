from genomes.models.BaseModel import BaseModel
from django.db import models

class GenomeCogCat(BaseModel):

    name = models.CharField(db_column='NAME', max_length=80, unique=True)
    description = models.CharField(db_column='DESCRIPTION', max_length=80)

    class Meta:
        db_table = 'genome_cog_category'
        verbose_name_plural = 'COG categories'
