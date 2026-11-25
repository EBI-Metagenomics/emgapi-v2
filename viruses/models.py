"""Catalogue releases and their viral fragments."""

from django.core.exceptions import ValidationError
from django.db import models
from django.db.models import Q

from analyses.base_models.base_models import TimeStampedModel
from analyses.base_models.with_downloads_models import WithDownloadsModel


class ViralCatalogue(WithDownloadsModel, TimeStampedModel):
    """One versioned viral catalogue."""

    DOWNLOAD_PARENT_IDENTIFIER_ATTR = "catalogue_id"

    fragments = models.ManyToManyField(
        "ViralFragment",
        through="ViralCatalogueViralFragment",
        related_name="catalogues",
    )

    catalogue_id = models.CharField(max_length=40, primary_key=True)
    name = models.CharField(max_length=200)
    version = models.CharField(max_length=40)
    description = models.TextField(blank=True)
    biome = models.CharField(max_length=200)
    ftp_url = models.URLField(max_length=500)
    is_public = models.BooleanField(default=False, db_index=True)
    pipeline_version = models.CharField(max_length=40, blank=True)
    other_stats = models.JSONField(default=dict, blank=True)

    class Meta:
        ordering = ["catalogue_id"]
        constraints = [
            models.UniqueConstraint(
                fields=["is_public"],
                condition=Q(is_public=True),
                name="one_public_viral_catalogue",
            )
        ]

    def clean(self) -> None:
        """Use a release version as the catalogue identifier."""
        super().clean()
        if self.catalogue_id != f"v{self.version.removeprefix('v')}":
            raise ValidationError(
                {"catalogue_id": "Use the version ID, for example v1.0"}
            )

    def __str__(self) -> str:
        return self.name


class ViralFragment(TimeStampedModel):
    """Stable sequence identity shared by catalogue releases."""

    accession = models.CharField(max_length=40, unique=True)
    nt_sequence_sha256 = models.CharField(max_length=64, blank=True)

    class Meta:
        ordering = ["accession"]

    def __str__(self) -> str:
        return self.accession


class ViralCatalogueViralFragment(TimeStampedModel):
    """Representative summaries and original cluster metadata for one release."""

    catalogue = models.ForeignKey(
        ViralCatalogue, on_delete=models.CASCADE, related_name="fragment_entries"
    )
    fragment = models.ForeignKey(
        ViralFragment, on_delete=models.PROTECT, related_name="catalogue_entries"
    )
    length = models.PositiveIntegerField()
    cluster_size = models.PositiveIntegerField(default=1)
    prediction_tool = models.CharField(max_length=100, blank=True)
    checkv_quality = models.CharField(max_length=100, blank=True)
    miuvig_quality = models.CharField(max_length=100, blank=True)
    completeness = models.FloatField(null=True, blank=True)
    contamination = models.FloatField(null=True, blank=True)
    is_provirus = models.BooleanField(default=False)
    sequence_type = models.CharField(
        max_length=20,
        choices=[("virus", "Virus"), ("prophage", "Prophage"), ("plasmid", "Plasmid")],
        default="virus",
        db_index=True,
    )
    taxonomy = models.JSONField(default=dict, blank=True)
    metadata_rows = models.JSONField(default=list)
    source_analyses = models.ManyToManyField(
        "analyses.Analysis", blank=True, related_name="viral_cluster_entries"
    )
    source_genomes = models.ManyToManyField(
        "genomes.CatalogueGenome", blank=True, related_name="viral_cluster_entries"
    )

    class Meta:
        ordering = ["fragment__accession"]
        constraints = [
            models.UniqueConstraint(
                fields=["catalogue", "fragment"],
                name="unique_viral_fragment_per_catalogue",
            ),
            models.CheckConstraint(
                condition=Q(length__gt=0), name="viral_representative_positive_length"
            ),
            models.CheckConstraint(
                condition=Q(cluster_size__gt=0), name="viral_cluster_positive_size"
            ),
        ]

    @property
    def accession(self) -> str:
        """Expose the stable accession on a catalogue entry."""
        return self.fragment.accession

    @property
    def nt_sequence_sha256(self) -> str:
        """Expose the sequence checksum on a catalogue entry."""
        return self.fragment.nt_sequence_sha256

    def clean(self) -> None:
        """Validate the raw metadata subset against its representative."""
        super().clean()
        if not self.metadata_rows or not any(
            row.get("ID") == self.accession for row in self.metadata_rows
        ):
            raise ValidationError(
                {"metadata_rows": "The representative row is required"}
            )
        if any(row.get("Cluster_rep") != self.accession for row in self.metadata_rows):
            raise ValidationError(
                {"metadata_rows": "All rows must belong to this cluster"}
            )
        if self.cluster_size != len({row["ID"] for row in self.metadata_rows}):
            raise ValidationError(
                {"cluster_size": "Cluster size must match distinct metadata IDs"}
            )

    def __str__(self) -> str:
        return f"{self.accession} in {self.catalogue_id}"
