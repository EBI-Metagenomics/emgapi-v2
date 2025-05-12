from django.db import models
from genomes.models.BaseModel import BaseModel


class FileFormat(BaseModel):
    format_id = models.AutoField(
        db_column='FORMAT_ID', primary_key=True)
    format_name = models.CharField(
        db_column='FORMAT_NAME', max_length=30)
    format_extension = models.CharField(
        db_column='FORMAT_EXTENSION', max_length=30)
    compression = models.BooleanField(
        db_column='COMPRESSION', default=True)

    class Meta:
        db_table = 'FILE_FORMAT'

    def __str__(self):
        return self.format_name