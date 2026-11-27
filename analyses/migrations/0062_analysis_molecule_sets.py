"""Replace the prototype protein index with per-analysis ChEBI sets."""

import django.contrib.postgres.fields
import django.contrib.postgres.indexes
import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("analyses", "0061_moleculeannotation")]

    operations = [
        migrations.CreateModel(
            name="AnalysisMoleculeIndex",
            fields=[
                (
                    "analysis",
                    models.OneToOneField(
                        on_delete=django.db.models.deletion.CASCADE,
                        primary_key=True,
                        related_name="molecule_index",
                        serialize=False,
                        to="analyses.analysis",
                    ),
                ),
                (
                    "chebi_ids",
                    django.contrib.postgres.fields.ArrayField(
                        base_field=models.PositiveIntegerField(),
                        default=list,
                        size=None,
                    ),
                ),
            ],
            options={
                "indexes": [
                    django.contrib.postgres.indexes.GinIndex(
                        fields=["chebi_ids"],
                        name="analysis_molecules_gin",
                    )
                ]
            },
        ),
        migrations.RunSQL(
            """
            INSERT INTO analyses_analysismoleculeindex (analysis_id, chebi_ids)
            SELECT analysis_id,
                   array_agg(DISTINCT split_part(chebi_id, ':', 2)::integer
                             ORDER BY split_part(chebi_id, ':', 2)::integer)
            FROM analyses_moleculeannotation
            GROUP BY analysis_id
            """,
            reverse_sql=migrations.RunSQL.noop,
        ),
        migrations.DeleteModel(name="MoleculeAnnotation"),
    ]
