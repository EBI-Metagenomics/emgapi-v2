from django.db import models
from genomes.models.base_model import BaseModel


# DEPRECATED: This model is being replaced by the annotations field in the Genome model
class GenomeKeggClass(BaseModel):

    class_id = models.CharField(db_column='CLASS_ID', max_length=10,
                                unique=True)
    name = models.CharField(db_column='NAME', max_length=80)
    parent = models.ForeignKey('self', db_column='PARENT', null=True, on_delete=models.CASCADE)

    class Meta:
        db_table = 'genome_kegg_class'

    def __str__(self):
        return self.name
