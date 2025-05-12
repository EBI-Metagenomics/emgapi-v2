from django.db import models

from genomes.models import GenomeKeggModule
from genomes.models.BaseModel import BaseModel
from genomes.models import Genome


class GenomeKeggModuleCounts(BaseModel):

    genome = models.ForeignKey(Genome, db_column='GENOME_ID',
                               on_delete=models.CASCADE, db_index=True)
    kegg_module = models.ForeignKey(GenomeKeggModule, db_column='KEGG_MODULE',
                                    on_delete=models.DO_NOTHING)
    genome_count = models.IntegerField(db_column='GENOME_COUNT')

    class Meta:
        db_table = 'genome_kegg_module_counts'
        unique_together = ('genome', 'kegg_module')