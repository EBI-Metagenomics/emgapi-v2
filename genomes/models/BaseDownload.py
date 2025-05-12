from django.db import models

class BaseDownload(models.Model):
    parent_id = models.ForeignKey(
        'self', db_column='PARENT_DOWNLOAD_ID', related_name='parent',
        blank=True, null=True, on_delete=models.CASCADE)
    realname = models.CharField(
        db_column='REAL_NAME', max_length=255)
    alias = models.CharField(
        db_column='ALIAS', max_length=255)
    group_type = models.ForeignKey(
        'DownloadGroupType', db_column='GROUP_ID',
        on_delete=models.CASCADE, blank=True, null=True)
    subdir = models.ForeignKey(
        'DownloadSubdir', db_column='SUBDIR_ID',
        on_delete=models.CASCADE, blank=True, null=True)
    description = models.ForeignKey(
        'DownloadDescriptionLabel', db_column='DESCRIPTION_ID',
        on_delete=models.CASCADE, blank=True, null=True)
    file_format = models.ForeignKey(
        'FileFormat', db_column='FORMAT_ID',
        on_delete=models.CASCADE, blank=True, null=True)
    file_checksum = models.CharField(
        db_column='CHECKSUM', max_length=255,
        null=False, blank=True)
    checksum_algorithm = models.ForeignKey(
        'ChecksumAlgorithm', db_column='CHECKSUM_ALGORITHM',
        blank=True, null=True, on_delete=models.CASCADE
    )

    class Meta:
        abstract = True