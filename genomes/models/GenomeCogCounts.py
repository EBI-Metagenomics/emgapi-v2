from django.db import models
from genomes.models.base_model import BaseModel
from genomes.models.GenomeCogCat import GenomeCogCat
from genomes.models.genome import Genome


# DEPRECATED: This model is being replaced by the annotations field in the Genome model
class GenomeCogCounts(BaseModel):

    genome = models.ForeignKey(Genome, db_column='GENOME_ID',
                               on_delete=models.CASCADE, db_index=True)
    cog = models.ForeignKey(GenomeCogCat, db_column='COG_ID',
                            on_delete=models.DO_NOTHING)
    genome_count = models.IntegerField(db_column='GENOME_COUNT')

    class Meta:
        db_table = 'genome_cog_counts'
        unique_together = ('genome', 'cog')
