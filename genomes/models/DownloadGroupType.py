from django.db import models
from genomes.models.BaseModel import BaseModel


class DownloadGroupType(BaseModel):
    group_id = models.AutoField(
        db_column='GROUP_ID', primary_key=True, )
    group_type = models.CharField(
        db_column='GROUP_TYPE', max_length=30)

    class Meta:
        db_table = 'DOWNLOAD_GROUP_TYPE'

    def __str__(self):
        return self.group_type