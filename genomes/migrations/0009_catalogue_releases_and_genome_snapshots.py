import django.contrib.postgres.fields
import django.db.models.deletion
from django.db import migrations, models

import genomes.models.genome


class Migration(migrations.Migration):
    dependencies = [
        ("genomes", "0008_alter_genomecatalogue_updated_at"),
    ]

    operations = [
        migrations.CreateModel(
            name="GenomeCatalogueSeries",
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
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("name", models.CharField(max_length=100)),
                (
                    "catalogue_biome_label",
                    models.CharField(
                        help_text="Stable biome label shared by releases in this series.",
                        max_length=100,
                    ),
                ),
                (
                    "catalogue_type",
                    models.CharField(
                        choices=[
                            ("prokaryotes", "prokaryotes"),
                            ("eukaryotes", "eukaryotes"),
                            ("viruses", "viruses"),
                        ],
                        max_length=20,
                    ),
                ),
                (
                    "biome",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.PROTECT,
                        to="analyses.biome",
                    ),
                ),
            ],
            options={
                "verbose_name_plural": "genome catalogue series",
            },
        ),
        migrations.AddConstraint(
            model_name="genomecatalogueseries",
            constraint=models.UniqueConstraint(
                fields=("catalogue_biome_label", "catalogue_type"),
                name="unique_genome_catalogue_series",
            ),
        ),
        migrations.AddField(
            model_name="genomecatalogue",
            name="series",
            field=models.ForeignKey(
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="releases",
                to="genomes.genomecatalogueseries",
            ),
        ),
        migrations.AddField(
            model_name="genomecatalogue",
            name="status",
            field=models.CharField(
                choices=[
                    ("draft", "Draft"),
                    ("ready", "Ready to publish"),
                    ("published", "Published"),
                    ("retired", "Retired"),
                ],
                db_index=True,
                default="draft",
                max_length=20,
            ),
        ),
        migrations.AddField(
            model_name="genomecatalogue",
            name="published_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="genomecatalogue",
            name="retired_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.CreateModel(
            name="CatalogueGenome",
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
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("downloads", models.JSONField(blank=True, default=list)),
                ("length", models.IntegerField()),
                ("num_contigs", models.IntegerField()),
                ("n_50", models.IntegerField()),
                ("gc_content", models.FloatField()),
                (
                    "type",
                    models.CharField(
                        choices=[("MAG", "MAG"), ("Isolate", "ISOLATE")],
                        max_length=80,
                    ),
                ),
                ("completeness", models.FloatField()),
                ("contamination", models.FloatField()),
                ("busco_completeness", models.FloatField(blank=True, null=True)),
                ("rna_5s", models.FloatField(blank=True, null=True)),
                ("rna_16s", models.FloatField(blank=True, null=True)),
                ("rna_23s", models.FloatField(blank=True, null=True)),
                ("rna_5_8s", models.FloatField(blank=True, null=True)),
                ("rna_18s", models.FloatField(blank=True, null=True)),
                ("rna_28s", models.FloatField(blank=True, null=True)),
                ("trnas", models.FloatField()),
                ("nc_rnas", models.IntegerField()),
                ("num_proteins", models.IntegerField()),
                ("eggnog_coverage", models.FloatField()),
                ("ipr_coverage", models.FloatField()),
                ("taxon_lineage", models.CharField(max_length=400)),
                (
                    "geographic_origin",
                    models.CharField(
                        blank=True,
                        help_text="Geographic origin of this genome",
                        max_length=80,
                        null=True,
                    ),
                ),
                (
                    "annotations",
                    models.JSONField(default=genomes.models.genome.default_annotations),
                ),
                (
                    "result_directory",
                    models.CharField(blank=True, max_length=100, null=True),
                ),
                ("num_genomes_total", models.IntegerField(blank=True, null=True)),
                ("pangenome_size", models.IntegerField(blank=True, null=True)),
                (
                    "pangenome_core_size",
                    models.IntegerField(blank=True, null=True),
                ),
                (
                    "pangenome_accessory_size",
                    models.IntegerField(blank=True, null=True),
                ),
                (
                    "geographic_range",
                    django.contrib.postgres.fields.ArrayField(
                        base_field=models.CharField(max_length=80),
                        blank=True,
                        help_text="Array of geographic locations where this genome is found",
                        null=True,
                        size=None,
                    ),
                ),
                (
                    "biome",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        to="analyses.biome",
                    ),
                ),
                (
                    "catalogue",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="genomes",
                        to="genomes.genomecatalogue",
                    ),
                ),
                (
                    "genome",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="catalogue_entries",
                        to="genomes.genome",
                    ),
                ),
            ],
        ),
    ]
