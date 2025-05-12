from django.db import models
from genomes.models.BaseModel import BaseModel


class ChecksumAlgorithm(BaseModel):
    """Checksum for download files
    """
    name = models.CharField(
        db_column='NAME',
        max_length=255,
        unique=True)

    class Meta:
        db_table = 'checksum_algorithm'

    def __str__(self):
        return self.name
