from django.db import models
from genomes.models.BaseModel import BaseModel


class GenomeSet(BaseModel):

    name = models.CharField(db_column='NAME', max_length=40, unique=True)

    class Meta:
        db_table = 'GENOME_SET'

    def __str__(self):
        return self.name