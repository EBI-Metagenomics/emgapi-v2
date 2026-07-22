from django.apps import apps
from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import models, transaction
from django.db.models import Count, Q
from django.utils import timezone

from analyses.base_models.base_models import TimeStampedModel
from analyses.base_models.with_downloads_models import WithDownloadsModel
from genomes.models.genome_catalogue_series import GenomeCatalogueSeries

EMG_CONFIG = settings.EMG_CONFIG


class PublishedGenomeCatalogueManager(models.Manager):
    def get_queryset(self):
        return super().get_queryset().filter(status="published")


class GenomeCatalogue(WithDownloadsModel, TimeStampedModel):
    """One versioned release of a genome catalogue series."""

    class Status(models.TextChoices):
        DRAFT = "draft", "Draft"
        READY = "ready", "Ready to publish"
        PUBLISHED = "published", "Published"
        RETIRED = "retired", "Retired"

    PROK = GenomeCatalogueSeries.PROK
    EUKS = GenomeCatalogueSeries.EUKS
    VIRS = GenomeCatalogueSeries.VIRS
    CATALOGUE_TYPE_CHOICES = GenomeCatalogueSeries.CATALOGUE_TYPE_CHOICES

    objects = models.Manager()
    public_objects = PublishedGenomeCatalogueManager()

    updated_at = models.DateTimeField(default=timezone.now, editable=True)
    DOWNLOAD_PARENT_IDENTIFIER_ATTR = "catalogue_id"

    catalogue_id = models.SlugField(
        db_column="catalogue_id", max_length=100, primary_key=True
    )
    series = models.ForeignKey(
        GenomeCatalogueSeries,
        on_delete=models.PROTECT,
        related_name="releases",
    )
    version = models.CharField(db_column="version", max_length=20)
    name = models.CharField(db_column="name", max_length=100, unique=True)
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.DRAFT,
        db_index=True,
    )
    published_at = models.DateTimeField(null=True, blank=True)
    retired_at = models.DateTimeField(null=True, blank=True)
    description = models.TextField(
        db_column="description",
        null=True,
        blank=True,
        help_text="This is a description of the catalogue.",
    )
    protein_catalogue_name = models.CharField(
        db_column="protein_catalogue_name", max_length=100, null=True, blank=True
    )
    protein_catalogue_description = models.TextField(
        db_column="protein_catalogue_description",
        null=True,
        blank=True,
        help_text="Description of the protein catalogue, if applicable.",
    )
    result_directory = models.CharField(
        db_column="result_directory", max_length=100, null=True, blank=True
    )
    unclustered_genome_count = models.IntegerField(
        db_column="unclustered_genome_count",
        null=True,
        blank=True,
        help_text="Total number of genomes in the catalogue (including cluster reps and members)",
    )
    ftp_url = models.CharField(
        db_column="ftp_url", max_length=200, default=EMG_CONFIG.genomes.mags_ftp_site
    )
    pipeline_version_tag = models.CharField(
        db_column="pipeline_version_tag",
        max_length=20,
        default=EMG_CONFIG.genomes.latest_mags_pipeline_tag,
    )
    other_stats = models.JSONField(db_column="other_stats_json", blank=True, null=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["series", "version"],
                name="unique_genome_catalogue_series_version",
            ),
            models.UniqueConstraint(
                fields=["series"],
                condition=Q(status="published"),
                name="one_published_release_per_catalogue_series",
            ),
        ]

    @property
    def catalogue_biome_label(self):
        return self.series.catalogue_biome_label

    @property
    def catalogue_type(self):
        return self.series.catalogue_type

    @property
    def biome(self):
        return self.series.biome

    @classmethod
    def publish_ready(cls, catalogue_ids):
        """Atomically publish ready releases and retire their current versions.

        All series rows are locked because the public uniqueness rule spans catalogue
        series. Catalogue publication is rare, and serialising it avoids two admin
        actions racing with one another.
        """
        catalogue_ids = list(dict.fromkeys(str(pk) for pk in catalogue_ids))
        if not catalogue_ids:
            raise ValidationError("Select at least one catalogue release to publish.")

        from genomes.models.catalogue_genome import CatalogueGenome

        with transaction.atomic():
            # Evaluate the queryset so SELECT FOR UPDATE actually takes the locks.
            list(
                GenomeCatalogueSeries.objects.select_for_update()
                .order_by("pk")
                .values_list("pk", flat=True)
            )
            releases = list(
                cls.objects.select_for_update().filter(
                    pk__in=catalogue_ids,
                    status=cls.Status.READY,
                )
            )
            if len(releases) != len(catalogue_ids):
                raise ValidationError(
                    "Every selected catalogue release must exist and be ready."
                )

            series_ids = {release.series_id for release in releases}
            if len(series_ids) != len(releases):
                raise ValidationError(
                    "Select at most one release from each catalogue series."
                )

            unchanged_public_ids = list(
                cls.objects.filter(status=cls.Status.PUBLISHED)
                .exclude(series_id__in=series_ids)
                .values_list("pk", flat=True)
            )
            proposed_public_ids = unchanged_public_ids + catalogue_ids
            conflicts = list(
                CatalogueGenome.objects.filter(catalogue_id__in=proposed_public_ids)
                .values("genome_id", "genome__accession")
                .annotate(catalogue_count=Count("catalogue_id", distinct=True))
                .filter(catalogue_count__gt=1)
                .order_by("genome__accession")[:20]
            )
            if conflicts:
                accessions = ", ".join(
                    conflict["genome__accession"] for conflict in conflicts
                )
                raise ValidationError(
                    "Publishing this selection would place genomes in multiple public "
                    f"catalogues. Conflicting accessions include: {accessions}"
                )

            current_releases = list(
                cls.objects.select_for_update().filter(
                    series_id__in=series_ids,
                    status=cls.Status.PUBLISHED,
                )
            )

            # Keep Super Study associations following the series' public release.
            super_study_link = apps.get_model("analyses", "SuperStudyGenomeCatalogue")
            current_by_series = {
                release.series_id: release for release in current_releases
            }
            for release in releases:
                previous = current_by_series.get(release.series_id)
                if not previous:
                    continue
                super_study_ids = list(
                    super_study_link.objects.filter(
                        genome_catalogue=previous
                    ).values_list("super_study_id", flat=True)
                )
                super_study_link.objects.bulk_create(
                    [
                        super_study_link(
                            genome_catalogue=release,
                            super_study_id=super_study_id,
                        )
                        for super_study_id in super_study_ids
                    ],
                    ignore_conflicts=True,
                )
                super_study_link.objects.filter(genome_catalogue=previous).delete()

            now = timezone.now()
            retired_ids = [release.pk for release in current_releases]
            if retired_ids:
                cls.objects.filter(pk__in=retired_ids).update(
                    status=cls.Status.RETIRED,
                    retired_at=now,
                )
            cls.objects.filter(pk__in=catalogue_ids).update(
                status=cls.Status.PUBLISHED,
                published_at=now,
                retired_at=None,
            )

        return {
            "published": catalogue_ids,
            "retired": retired_ids,
        }

    def __str__(self):
        return self.name

    @property
    def calculate_genome_count(self):
        return self.genomes.count()
