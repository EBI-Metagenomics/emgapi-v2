from django.db import models

from analyses.base_models.base_models import TimeStampedModel
from emgapiv2.enum_utils import DjangoChoicesCompatibleStrEnum

# Annotation field constants are kept in this module for backwards-compatible imports.
COG_CATEGORIES = "cog_categories"
KEGG_CLASSES = "kegg_classes"
KEGG_MODULES = "kegg_modules"
ANTISMASH_GENE_CLUSTERS = "antismash_gene_clusters"


def default_annotations():
    return {
        COG_CATEGORIES: [],
        KEGG_CLASSES: [],
        KEGG_MODULES: [],
        ANTISMASH_GENE_CLUSTERS: [],
    }


class Genome(TimeStampedModel):
    """Stable identity for an MGnify genome.

    Catalogue- and pipeline-release-specific data lives on ``CatalogueGenome``.
    This allows one MGYG accession to be staged in a new catalogue release without
    changing the version that is currently public.
    """

    class GenomeType(DjangoChoicesCompatibleStrEnum):
        MAG = "MAG"
        ISOLATE = "Isolate"

    genome_id = models.AutoField(primary_key=True)
    accession = models.CharField(max_length=40, unique=True)

    ena_genome_accession = models.CharField(
        max_length=20,
        unique=True,
        null=True,
        blank=True,
    )
    ena_sample_accession = models.CharField(max_length=20, null=True, blank=True)
    ena_study_accession = models.CharField(max_length=20, null=True)

    ncbi_genome_accession = models.CharField(
        max_length=20,
        unique=True,
        null=True,
        blank=True,
    )
    ncbi_sample_accession = models.CharField(max_length=20, null=True, blank=True)
    ncbi_study_accession = models.CharField(max_length=20, null=True, blank=True)

    img_genome_accession = models.CharField(
        max_length=20,
        unique=True,
        null=True,
        blank=True,
    )
    patric_genome_accession = models.CharField(
        max_length=20,
        unique=True,
        blank=True,
        null=True,
    )

    @property
    def published_catalogue_entry(self):
        """Return the published snapshot prefetched by assembly API queries."""
        return (
            self._published_catalogue_entries[0]
            if self._published_catalogue_entries
            else None
        )

    # Compatibility properties for schemas that expose a genome nested inside an
    # assembly link. Catalogue-specific API endpoints use CatalogueGenome directly.
    @property
    def catalogue(self):
        entry = self.published_catalogue_entry
        return entry.catalogue if entry else None

    @property
    def catalogue_id(self):
        catalogue = self.catalogue
        return catalogue.catalogue_id if catalogue else None

    @property
    def taxon_lineage(self):
        entry = self.published_catalogue_entry
        return entry.taxon_lineage if entry else None

    def __str__(self):
        return self.accession
