import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):
    """Create the data stewardship prediction models."""

    initial = True

    dependencies = [
        ("analyses", "0058_assembly_idx_assembly_ena_acc_gin_and_more"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="BiomeStudyBiomePrediction",
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
                ("raw_predicted_lineage", models.TextField(blank=True)),
                ("confidence", models.FloatField(blank=True, null=True)),
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
                    "method",
                    models.CharField(
                        choices=[
                            ("trapiche", "Trapiche"),
                            ("manual", "Manual"),
                            ("other", "Other"),
                        ],
                        default="trapiche",
                        max_length=64,
                    ),
                ),
                ("source", models.CharField(blank=True, max_length=64)),
                ("source_version", models.CharField(blank=True, max_length=64)),
                ("configuration", models.JSONField(blank=True, default=dict)),
                ("predicted_at", models.DateTimeField(auto_now=True)),
                ("note", models.TextField(blank=True)),
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
                    "evidence",
                    models.ManyToManyField(
                        blank=True, related_name="+", to="analyses.analysis"
                    ),
                ),
                (
                    "predicted_biome",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="+",
                        to="analyses.biome",
                    ),
                ),
                (
                    "study",
                    models.OneToOneField(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="biome_prediction",
                        to="analyses.study",
                    ),
                ),
            ],
        ),
        migrations.CreateModel(
            name="BiomeSampleBiomePrediction",
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
                ("raw_predicted_lineage", models.TextField(blank=True)),
                ("confidence", models.FloatField(blank=True, null=True)),
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
                    "method",
                    models.CharField(
                        choices=[
                            ("trapiche", "Trapiche"),
                            ("manual", "Manual"),
                            ("other", "Other"),
                        ],
                        default="trapiche",
                        max_length=64,
                    ),
                ),
                ("source", models.CharField(blank=True, max_length=64)),
                ("source_version", models.CharField(blank=True, max_length=64)),
                ("configuration", models.JSONField(blank=True, default=dict)),
                ("predicted_at", models.DateTimeField(auto_now=True)),
                ("note", models.TextField(blank=True)),
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
                    "evidence",
                    models.ManyToManyField(
                        blank=True, related_name="+", to="analyses.analysis"
                    ),
                ),
                (
                    "predicted_biome",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="+",
                        to="analyses.biome",
                    ),
                ),
                (
                    "sample",
                    models.OneToOneField(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="biome_prediction",
                        to="analyses.sample",
                    ),
                ),
            ],
        ),
    ]
