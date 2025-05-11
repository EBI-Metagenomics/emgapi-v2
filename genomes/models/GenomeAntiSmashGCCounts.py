from django.db import models

from genomes.models import Genome, GenomeAntiSmashGC
from genomes.models.BaseModel import BaseModel


class GenomeAntiSmashGCCounts(BaseModel):
    genome = models.ForeignKey(Genome, db_column='GENOME_ID', on_delete=models.CASCADE, db_index=True)
    antismash_genecluster = models.ForeignKey(GenomeAntiSmashGC, db_column='ANTISMASH_GENECLUSTER',
                                              on_delete=models.DO_NOTHING)
    genome_count = models.IntegerField(db_column='GENOME_COUNT')

    class Meta:
        db_table = 'genome_anitsmash_genecluster_counts'
        unique_together = ('genome', 'antismash_genecluster')