from django.db import models

from genomes.models.BaseModel import BaseModel


class DownloadDescriptionLabel(BaseModel):
    description_id = models.AutoField(
        db_column='DESCRIPTION_ID', primary_key=True)
    description = models.CharField(
        db_column='DESCRIPTION', max_length=255)
    description_label = models.CharField(
        db_column='DESCRIPTION_LABEL', max_length=100)

    class Meta:
        db_table = 'download_description_label'

    def __str__(self):
        return self.description_label