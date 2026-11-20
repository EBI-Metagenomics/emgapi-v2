"""Catalogue releases and their viral fragments."""

from django.core.exceptions import ValidationError
from django.db import models
from django.db.models import Q

from analyses.base_models.base_models import TimeStampedModel
from analyses.base_models.with_downloads_models import WithDownloadsModel
from emgapiv2.model_utils import JSONFieldWithSchema
from viruses.occurrences import ViralFragmentOccurrence


class ViralCatalogue(WithDownloadsModel, TimeStampedModel):
    """One versioned viral catalogue."""

    DOWNLOAD_PARENT_IDENTIFIER_ATTR = "catalogue_id"

    fragments = models.ManyToManyField(
        "ViralFragment",
        through="ViralCatalogueViralFragment",
        related_name="catalogues",
    )

    catalogue_id = models.SlugField(max_length=100, primary_key=True)
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
    """Representative annotations and source occurrences for one catalogue release."""

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
        choices=[("virus", "Virus"), ("prophage", "Prophage")],
        default="virus",
        db_index=True,
    )
    taxonomy = models.JSONField(default=dict, blank=True)
    metadata = models.JSONField(default=dict, blank=True)
    occurrences = JSONFieldWithSchema(
        schema=ViralFragmentOccurrence, is_list=True, default=list
    )
    cluster_member_occurrences = JSONFieldWithSchema(
        schema=ViralFragmentOccurrence, is_list=True, default=list, blank=True
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
        """Validate occurrence roles without imposing a checksum uniqueness rule."""
        super().clean()
        if not self.occurrences:
            raise ValidationError(
                {"occurrences": "A representative must have an occurrence"}
            )
        representatives = [
            ViralFragmentOccurrence.model_validate(item) for item in self.occurrences
        ]
        members = [
            ViralFragmentOccurrence.model_validate(item)
            for item in self.cluster_member_occurrences
        ]
        if any(item.fragment_accession != self.accession for item in representatives):
            raise ValidationError(
                {
                    "occurrences": "Representative occurrences must match the fragment accession"
                }
            )
        if any(item.fragment_accession == self.accession for item in members):
            raise ValidationError(
                {
                    "cluster_member_occurrences": "Member occurrences must exclude the representative"
                }
            )
        if self.cluster_size != 1 + len({item.fragment_accession for item in members}):
            raise ValidationError(
                {
                    "cluster_size": "Cluster size must match the distinct member accessions"
                }
            )

    def __str__(self) -> str:
        return f"{self.accession} in {self.catalogue_id}"
