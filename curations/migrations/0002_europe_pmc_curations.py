import django.core.validators
import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("curations", "0001_initial"),
        ("analyses", "0058_assembly_idx_assembly_ena_acc_gin_and_more"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.AddField(
            model_name="trapichebiomecuration",
            name="provider",
            field=models.CharField(blank=True, db_index=True, max_length=64),
        ),
        migrations.CreateModel(
            name="EuropePmcPublicationCuration",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("suggested", "Suggested"),
                            ("approved", "Approved"),
                            ("rejected", "Rejected"),
                        ],
                        db_index=True,
                        default="suggested",
                        max_length=16,
                    ),
                ),
                (
                    "provider",
                    models.CharField(blank=True, db_index=True, max_length=64),
                ),
                (
                    "confidence",
                    models.FloatField(
                        blank=True,
                        null=True,
                        validators=[
                            django.core.validators.MinValueValidator(0),
                            django.core.validators.MaxValueValidator(1),
                        ],
                    ),
                ),
                ("source_version", models.CharField(blank=True, max_length=64)),
                ("configuration", models.JSONField(blank=True, default=dict)),
                ("raw_result", models.JSONField(blank=True, default=dict)),
                ("note", models.TextField(blank=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True, db_index=True)),
                (
                    "curator",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="+",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    "publication",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="europe_pmc_curations",
                        to="analyses.publication",
                    ),
                ),
            ],
            options={
                "indexes": [
                    models.Index(
                        fields=["publication", "-updated_at"],
                        name="curations_europe_pub_idx",
                    ),
                    models.Index(
                        fields=["status", "-updated_at"],
                        name="curations_europe_status_idx",
                    ),
                ],
            },
        ),
        migrations.CreateModel(
            name="EuropePmcAnnotationGroup",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("annotation_type", models.CharField(max_length=64)),
                ("category", models.CharField(max_length=32)),
                (
                    "curation",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="groups",
                        to="curations.europepmcpublicationcuration",
                    ),
                ),
            ],
            options={
                "constraints": [
                    models.UniqueConstraint(
                        fields=("curation", "annotation_type", "category"),
                        name="unique_europe_pmc_group_per_curation",
                    )
                ],
            },
        ),
        migrations.CreateModel(
            name="EuropePmcAnnotation",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("annotation_text", models.TextField()),
                (
                    "group",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="annotations",
                        to="curations.europepmcannotationgroup",
                    ),
                ),
            ],
            options={
                "constraints": [
                    models.UniqueConstraint(
                        fields=("group", "annotation_text"),
                        name="unique_europe_pmc_annotation_per_group",
                    )
                ],
            },
        ),
        migrations.CreateModel(
            name="EuropePmcAnnotationMention",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("exact", models.TextField()),
                ("external_id", models.CharField(blank=True, max_length=255)),
                ("postfix", models.TextField(blank=True)),
                ("prefix", models.TextField(blank=True)),
                ("provider", models.CharField(blank=True, max_length=64)),
                ("annotation_type", models.CharField(max_length=64)),
                ("section", models.CharField(blank=True, max_length=255)),
                (
                    "annotation",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="mentions",
                        to="curations.europepmcannotation",
                    ),
                ),
            ],
        ),
        migrations.CreateModel(
            name="EuropePmcAnnotationTag",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("name", models.CharField(max_length=255)),
                ("uri", models.URLField(max_length=2048)),
                (
                    "mention",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="tags",
                        to="curations.europepmcannotationmention",
                    ),
                ),
            ],
            options={
                "constraints": [
                    models.UniqueConstraint(
                        fields=("mention", "name", "uri"),
                        name="unique_europe_pmc_tag_per_mention",
                    )
                ],
            },
        ),
    ]
