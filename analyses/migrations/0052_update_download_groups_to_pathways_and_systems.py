from __future__ import annotations

from django.db import migrations


SUFFIXES_SHOULD_BE_PATHWAYS_AND_SYSTEMS = {
    "antismash",
    "sanntis",
    "genome_properties",
    "kegg_modules",
    "dram_distill",
}


def _rewrite_download_group(value: str, old_prefix: str, new_prefix: str) -> str:
    """
    If `value` starts with `old_prefix` and one of the SUFFIXES_SHOULD_BE_PATHWAYS_AND_SYSTEMS after the dot,
    rewrite it to start with `new_prefix` while keeping the suffix.
    Otherwise return original value.
    """
    if not value:
        return value
    # Expect patterns like "functional_annotation.<suffix>" or "pathways_and_systems.<suffix>"
    if value.startswith(old_prefix + "."):
        parts = value.split(".", 1)
        if len(parts) == 2:
            suffix = parts[1]
            if suffix in SUFFIXES_SHOULD_BE_PATHWAYS_AND_SYSTEMS:
                return f"{new_prefix}.{suffix}"
    return value


def forwards(apps, schema_editor):
    Analysis = apps.get_model("analyses", "Analysis")

    # experiment_type value for Assembly per WithExperimentTypeModel.ExperimentTypes
    ASSEMBLY = "ASSEM"

    qs = Analysis.objects.filter(experiment_type=ASSEMBLY).exclude(downloads=[])

    for analysis in qs.iterator():
        downloads = analysis.downloads or []
        changed = False
        new_downloads = []
        for dl in downloads:
            dl = dict(dl)  # ensure mutable copy
            group = dl.get("download_group")
            new_group = _rewrite_download_group(
                group,
                old_prefix="functional_annotation",
                new_prefix="pathways_and_systems",
            )
            if new_group != group:
                dl["download_group"] = new_group
                changed = True
            new_downloads.append(dl)
        if changed:
            analysis.downloads = new_downloads
            analysis.save(update_fields=["downloads"])


def backwards(apps, schema_editor):
    Analysis = apps.get_model("analyses", "Analysis")
    ASSEMBLY = "ASSEM"

    qs = Analysis.objects.filter(experiment_type=ASSEMBLY).exclude(downloads=[])

    for analysis in qs.iterator():
        downloads = analysis.downloads or []
        changed = False
        new_downloads = []
        for dl in downloads:
            dl = dict(dl)
            group = dl.get("download_group")
            new_group = _rewrite_download_group(
                group,
                old_prefix="pathways_and_systems",
                new_prefix="functional_annotation",
            )
            if new_group != group:
                dl["download_group"] = new_group
                changed = True
            new_downloads.append(dl)
        if changed:
            analysis.downloads = new_downloads
            analysis.save(update_fields=["downloads"])


class Migration(migrations.Migration):
    dependencies = [
        ("analyses", "0051_more_metadata_and_search_indexes"),
    ]

    operations = [
        migrations.RunPython(forwards, backwards),
    ]
