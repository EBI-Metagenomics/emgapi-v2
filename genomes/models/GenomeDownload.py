
class GenomeDownload(BaseDownload):
    genome = models.ForeignKey(
        'Genome', db_column='GENOME_ID',
        on_delete=models.CASCADE, db_index=True)