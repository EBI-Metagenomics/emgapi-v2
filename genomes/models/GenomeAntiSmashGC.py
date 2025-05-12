from django.db import models

from genomes.models.BaseModel import BaseModel


class GenomeAntiSmashGC(BaseModel):
    name = models.CharField(db_column='NAME', max_length=80)
    description = models.CharField(db_column='DESCRIPTION', max_length=80)

    class Meta:
        db_table = 'genome_anitsmash_genecluster'
        verbose_name_plural = 'antiSMASH clusters'