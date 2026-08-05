import django.db.models.deletion
from django.db import migrations, models
from django.db.models import Q


class Migration(migrations.Migration):
    dependencies = [
        ("genomes", "0010_migrate_catalogue_releases_and_genomes"),
    ]

    operations = [
        migrations.AlterField(
            model_name="genomecatalogue",
            name="series",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.PROTECT,
                related_name="releases",
                to="genomes.genomecatalogueseries",
            ),
        ),
        migrations.AlterUniqueTogether(
            name="genomecatalogue",
            unique_together=set(),
        ),
        migrations.RemoveField(
            model_name="genomecatalogue", name="catalogue_biome_label"
        ),
        migrations.RemoveField(model_name="genomecatalogue", name="catalogue_type"),
        migrations.RemoveField(model_name="genomecatalogue", name="biome"),
        migrations.AddConstraint(
            model_name="genomecatalogue",
            constraint=models.UniqueConstraint(
                fields=("series", "version"),
                name="unique_genome_catalogue_series_version",
            ),
        ),
        migrations.AddConstraint(
            model_name="genomecatalogue",
            constraint=models.UniqueConstraint(
                condition=Q(status="published"),
                fields=("series",),
                name="one_published_release_per_catalogue_series",
            ),
        ),
        migrations.AddConstraint(
            model_name="cataloguegenome",
            constraint=models.UniqueConstraint(
                fields=("catalogue", "genome"),
                name="unique_genome_per_catalogue_release",
            ),
        ),
        migrations.RemoveField(model_name="genome", name="annotations"),
        migrations.RemoveField(model_name="genome", name="biome"),
        migrations.RemoveField(model_name="genome", name="busco_completeness"),
        migrations.RemoveField(model_name="genome", name="catalogue"),
        migrations.RemoveField(model_name="genome", name="completeness"),
        migrations.RemoveField(model_name="genome", name="contamination"),
        migrations.RemoveField(model_name="genome", name="downloads"),
        migrations.RemoveField(model_name="genome", name="eggnog_coverage"),
        migrations.RemoveField(model_name="genome", name="gc_content"),
        migrations.RemoveField(model_name="genome", name="geographic_origin"),
        migrations.RemoveField(model_name="genome", name="geographic_range"),
        migrations.RemoveField(model_name="genome", name="ipr_coverage"),
        migrations.RemoveField(model_name="genome", name="length"),
        migrations.RemoveField(model_name="genome", name="n_50"),
        migrations.RemoveField(model_name="genome", name="nc_rnas"),
        migrations.RemoveField(model_name="genome", name="num_contigs"),
        migrations.RemoveField(model_name="genome", name="num_genomes_total"),
        migrations.RemoveField(model_name="genome", name="num_proteins"),
        migrations.RemoveField(
            model_name="genome", name="pangenome_accessory_size"
        ),
        migrations.RemoveField(model_name="genome", name="pangenome_core_size"),
        migrations.RemoveField(model_name="genome", name="pangenome_size"),
        migrations.RemoveField(model_name="genome", name="result_directory"),
        migrations.RemoveField(model_name="genome", name="rna_16s"),
        migrations.RemoveField(model_name="genome", name="rna_18s"),
        migrations.RemoveField(model_name="genome", name="rna_23s"),
        migrations.RemoveField(model_name="genome", name="rna_28s"),
        migrations.RemoveField(model_name="genome", name="rna_5_8s"),
        migrations.RemoveField(model_name="genome", name="rna_5s"),
        migrations.RemoveField(model_name="genome", name="taxon_lineage"),
        migrations.RemoveField(model_name="genome", name="trnas"),
        migrations.RemoveField(model_name="genome", name="type"),
    ]
