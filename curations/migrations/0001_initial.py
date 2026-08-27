import django.core.validators
import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = [
        ("analyses", "0058_assembly_idx_assembly_ena_acc_gin_and_more"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="TrapicheBiomeCuration",
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
                    "biome",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="+",
                        to="analyses.biome",
                    ),
                ),
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
                    "sample",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="trapiche_biome_curations",
                        to="analyses.sample",
                    ),
                ),
                (
                    "study",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="trapiche_biome_curations",
                        to="analyses.study",
                    ),
                ),
            ],
            options={
                "indexes": [
                    models.Index(
                        fields=["study", "sample", "-updated_at"],
                        name="curations_study_sample_idx",
                    ),
                    models.Index(
                        fields=["status", "-updated_at"],
                        name="curations_status_updated_idx",
                    ),
                ],
            },
        ),
    ]
