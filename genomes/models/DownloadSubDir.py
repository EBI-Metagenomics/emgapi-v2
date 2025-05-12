from django.db import models
from genomes.models.BaseModel import BaseModel

class DownloadSubdir(BaseModel):
    subdir_id = models.AutoField(
        db_column='SUBDIR_ID', primary_key=True)
    subdir = models.CharField(
        db_column='SUBDIR', max_length=100)

    class Meta:
        db_table = 'DOWNLOAD_SUBDIR'

    def __str__(self):
        return self.subdir