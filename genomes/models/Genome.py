from django.db import models

from analyses.models import Biome


class Genome(models.Model):

    ISOLATE = 'isolate'
    MAG = 'mag'
    TYPE_CHOICES = (
        (MAG, 'MAG'),
        (ISOLATE, 'Isolate'),
    )

    # objects = GenomeManager()

    genome_id = models.AutoField(
        db_column='GENOME_ID', primary_key=True)
    accession = models.CharField(db_column='GENOME_ACCESSION', max_length=40, unique=True)

    ena_genome_accession = models.CharField(db_column='ENA_GENOME_ACCESSION',
                                            max_length=20, unique=True, null=True, blank=True)
    ena_sample_accession = models.CharField(db_column='ENA_SAMPLE_ACCESSION', max_length=20,
                                            null=True, blank=True)
    ena_study_accession = models.CharField(db_column='ENA_STUDY_ACCESSION', max_length=20, null=True)

    ncbi_genome_accession = models.CharField(db_column='NCBI_GENOME_ACCESSION',
                                             max_length=20, unique=True, null=True, blank=True)
    ncbi_sample_accession = models.CharField(db_column='NCBI_SAMPLE_ACCESSION', max_length=20, null=True, blank=True)
    ncbi_study_accession = models.CharField(db_column='NCBI_STUDY_ACCESSION', max_length=20, null=True, blank=True)

    img_genome_accession = models.CharField(db_column='IMG_GENOME_ACCESSION', max_length=20,
                                            unique=True, null=True, blank=True)
    patric_genome_accession = models.CharField(db_column='PATRIC_GENOME_ACCESSION', max_length=20, unique=True,
                                               blank=True, null=True)

    # genome_set = models.ForeignKey(GenomeSet,
    #                                db_column='GENOME_SET_ID',
    #                                on_delete=models.CASCADE, null=True)
    genome_set = models.JSONField(
        db_column='GENOME_SET_JSON',
        null=True,
        blank=True,
        help_text='A JSON object with keys: "id" and "name" representing the genome set.'
    )

    biome = models.ForeignKey(
        Biome, db_column='BIOME_ID',
        on_delete=models.CASCADE)

    length = models.IntegerField(db_column='LENGTH')
    num_contigs = models.IntegerField(db_column='N_CONTIGS')
    n_50 = models.IntegerField(db_column='N50')
    gc_content = models.FloatField(db_column='GC_CONTENT')
    type = models.CharField(db_column='TYPE',
                            choices=TYPE_CHOICES,
                            max_length=80)
    completeness = models.FloatField(db_column='COMPLETENESS')
    contamination = models.FloatField(db_column='CONTAMINATION')

    # EUKS:
    busco_completeness = models.FloatField(db_column='BUSCO_COMPLETENESS', null=True, blank=True)

    # EUKS + PROKS:
    rna_5s = models.FloatField(db_column='RNA_5S', null=True, blank=True)
    # PROKS:
    rna_16s = models.FloatField(db_column='RNA_16S', null=True, blank=True)
    rna_23s = models.FloatField(db_column='RNA_23S', null=True, blank=True)
    # EUKS:
    rna_5_8s = models.FloatField(db_column='RNA_5_8S', null=True, blank=True)
    rna_18s = models.FloatField(db_column='RNA_18S', null=True, blank=True)
    rna_28s = models.FloatField(db_column='RNA_28S', null=True, blank=True)

    trnas = models.FloatField(db_column='T_RNA')
    nc_rnas = models.IntegerField(db_column='NC_RNA')
    num_proteins = models.IntegerField(db_column='NUM_PROTEINS')
    eggnog_coverage = models.FloatField(db_column='EGGNOG_COVERAGE')
    ipr_coverage = models.FloatField(db_column='IPR_COVERAGE')
    taxon_lineage = models.CharField(db_column='TAXON_LINEAGE', max_length=400)

    num_genomes_total = models.IntegerField(db_column='PANGENOME_TOTAL_GENOMES', null=True, blank=True)
    pangenome_size = models.IntegerField(db_column='PANGENOME_SIZE', null=True, blank=True)
    pangenome_core_size = models.IntegerField(db_column='PANGENOME_CORE_PROP', null=True, blank=True)
    pangenome_accessory_size = models.IntegerField(db_column='PANGENOME_ACCESSORY_PROP', null=True, blank=True)

    last_update = models.DateTimeField(db_column='LAST_UPDATE', auto_now=True)
    first_created = models.DateTimeField(db_column='FIRST_CREATED', auto_now_add=True)

    geo_origin = models.ForeignKey('GeographicLocation', db_column='GEOGRAPHIC_ORIGIN', null=True, blank=True, on_delete=models.CASCADE)

    pangenome_geographic_range = models.ManyToManyField('GeographicLocation',
                                                        db_table='GENOME_PANGENOME_GEOGRAPHIC_RANGE',
                                                        related_name='geographic_range')

    cog_matches = models.ManyToManyField('GenomeCogCat',
                                         through='GenomeCogCounts')
    kegg_classes = models.ManyToManyField('GenomeKeggClass',
                                          through='GenomeKeggClassCounts')
    kegg_modules = models.ManyToManyField('GenomeKeggModule',
                                          through='GenomeKeggModuleCounts')
    antismash_geneclusters = models.ManyToManyField('GenomeAntiSmashGC', through='GenomeAntiSmashGCCounts')

    result_directory = models.CharField(
        db_column='RESULT_DIRECTORY', max_length=100, blank=True, null=True)

    catalogue = models.ForeignKey('GenomeCatalogue', db_column='GENOME_CATALOGUE', on_delete=models.CASCADE,
                                  related_name='genomes')

    @property
    def geographic_range(self):
        return [v.name for v in self.pangenome_geographic_range.all()]

    @property
    def geographic_origin(self):
        if self.geo_origin:
            name = self.geo_origin.name
        else:
            name = None
        return name

    class Meta:
        db_table = 'GENOME'

    def __str__(self):
        return self.accession