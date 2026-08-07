from django.db import migrations
from django.utils import timezone


def migrate_catalogue_releases_and_genomes(apps, schema_editor):
    GenomeCatalogueSeries = apps.get_model("genomes", "GenomeCatalogueSeries")
    GenomeCatalogue = apps.get_model("genomes", "GenomeCatalogue")
    Genome = apps.get_model("genomes", "Genome")
    CatalogueGenome = apps.get_model("genomes", "CatalogueGenome")

    series_by_key = {}
    for catalogue in GenomeCatalogue.objects.order_by("created_at", "catalogue_id"):
        key = (catalogue.catalogue_biome_label, catalogue.catalogue_type)
        series = series_by_key.get(key)
        if series is None:
            version_suffix = f" v{catalogue.version}"
            series_name = catalogue.name
            if series_name.lower().endswith(version_suffix.lower()):
                series_name = series_name[: -len(version_suffix)]
            series = GenomeCatalogueSeries.objects.create(
                name=series_name,
                catalogue_biome_label=catalogue.catalogue_biome_label,
                catalogue_type=catalogue.catalogue_type,
                biome_id=catalogue.biome_id,
            )
            series_by_key[key] = series
        catalogue.series_id = series.pk
        catalogue.save(update_fields=["series"])

    now = timezone.now()
    for series in GenomeCatalogueSeries.objects.all():
        releases = list(
            GenomeCatalogue.objects.filter(series=series).order_by(
                "updated_at", "created_at", "catalogue_id"
            )
        )
        for release in releases[:-1]:
            release.status = "retired"
            release.retired_at = release.updated_at or now
            release.save(update_fields=["status", "retired_at"])
        if releases:
            latest = releases[-1]
            latest.status = "published"
            latest.published_at = latest.updated_at or now
            latest.save(update_fields=["status", "published_at"])

    snapshot_fields = [
        "downloads",
        "biome_id",
        "length",
        "num_contigs",
        "n_50",
        "gc_content",
        "type",
        "completeness",
        "contamination",
        "busco_completeness",
        "rna_5s",
        "rna_16s",
        "rna_23s",
        "rna_5_8s",
        "rna_18s",
        "rna_28s",
        "trnas",
        "nc_rnas",
        "num_proteins",
        "eggnog_coverage",
        "ipr_coverage",
        "taxon_lineage",
        "geographic_origin",
        "annotations",
        "result_directory",
        "num_genomes_total",
        "pangenome_size",
        "pangenome_core_size",
        "pangenome_accessory_size",
        "geographic_range",
    ]
    pending = []
    for genome in Genome.objects.all().iterator(chunk_size=1000):
        pending.append(
            CatalogueGenome(
                genome_id=genome.pk,
                catalogue_id=genome.catalogue_id,
                **{field: getattr(genome, field) for field in snapshot_fields},
            )
        )
        if len(pending) == 1000:
            CatalogueGenome.objects.bulk_create(pending, batch_size=1000)
            pending.clear()
    if pending:
        CatalogueGenome.objects.bulk_create(pending, batch_size=1000)


class Migration(migrations.Migration):
    dependencies = [
        ("genomes", "0009_catalogue_releases_and_genome_snapshots"),
    ]

    operations = [
        migrations.RunPython(
            migrate_catalogue_releases_and_genomes,
            reverse_code=migrations.RunPython.noop,
        ),
    ]
