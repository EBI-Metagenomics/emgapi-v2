import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("analyses", "0060_analysis_pipeline_version_v6_2")]
    operations = [
        migrations.CreateModel(
            name="MoleculeAnnotation",
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
                ("protein_id", models.CharField(max_length=255)),
                ("rhea_id", models.CharField(max_length=32)),
                ("chebi_id", models.CharField(max_length=32)),
                ("evidence", models.CharField(blank=True, max_length=255)),
                (
                    "analysis",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="molecule_annotations",
                        to="analyses.analysis",
                    ),
                ),
            ],
            options={
                "indexes": [
                    models.Index(fields=["chebi_id", "id"], name="molecule_lookup_idx")
                ],
                "constraints": [
                    models.UniqueConstraint(
                        fields=("analysis", "protein_id", "rhea_id", "chebi_id"),
                        name="unique_protein_molecule",
                    )
                ],
            },
        )
    ]
