from django.db import models

from genomes.models import Genome
from genomes.models.BaseModel import BaseModel
from genomes.models import GenomeKeggClass


class GenomeKeggClassCounts(BaseModel):

    genome = models.ForeignKey(Genome, db_column='GENOME_ID',
                               on_delete=models.CASCADE, db_index=True)
    kegg_class = models.ForeignKey(GenomeKeggClass, db_column='KEGG_ID',
                                   on_delete=models.DO_NOTHING)
    genome_count = models.IntegerField(db_column='GENOME_COUNT')

    class Meta:
        db_table = 'genome_kegg_class_counts'
        unique_together = ('genome', 'kegg_class')