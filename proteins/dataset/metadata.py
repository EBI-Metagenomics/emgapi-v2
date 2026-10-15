"""The metadata snapshot: what emgapi-v2 knows of each assembly proteins record, written each day with the protein dataset."""

import logging

import pyarrow as pa

from analyses.models import Analysis, Biome
from proteins.dataset import layout

# TODO (this feature): a protein is suppressed only when every assembly that has
# it is suppressed. The metadata snapshot has is_suppressed for each assembly
# proteins record; apply the rule here or in the release.

logger = logging.getLogger(__name__)

CHUNK = 10_000


def build_snapshot(assembly_proteins: pa.Table) -> pa.Table:
    """The metadata/assembly_proteins table for every assembly proteins record, of either source, whose MGYA is in emgapi-v2.

    A record whose MGYA is not in emgapi-v2 is withheld: releases publish nothing of a record
    without metadata, and it is back in the snapshot of the day after its analysis is.

    :param assembly_proteins: The assembly_proteins rows: id, assembly_accession, source, pipeline_version, analysis_accession.
    :return: The table in the metadata/assembly_proteins schema.
    """
    records = assembly_proteins.to_pylist()
    mgyas = sorted({record["analysis_accession"] for record in records})

    known = {}
    for start in range(0, len(mgyas), CHUNK):
        rows = Analysis.objects.filter(
            accession__in=mgyas[start : start + CHUNK]
        ).values(
            "accession",
            "study__accession",
            "sample__biome_id",
            "assembly__is_private",
            "assembly__is_suppressed",
        )
        known |= {row["accession"]: row for row in rows}

    if missing := [mgya for mgya in mgyas if mgya not in known]:
        logger.warning(
            "withholding the records of %d MGYAs that are not in emgapi-v2,"
            " for example %s",
            len(missing),
            ", ".join(missing[:10]),
        )

    lineages = {
        biome.id: biome.pretty_lineage
        for biome in Biome.objects.filter(
            id__in={row["sample__biome_id"] for row in known.values()}
        )
    }
    rows = []
    for record in records:
        if (analysis := known.get(record["analysis_accession"])) is None:
            continue
        rows.append(
            {
                "assembly_proteins_id": record["id"],
                "assembly_accession": record["assembly_accession"],
                "source": record["source"],
                "pipeline_version": record["pipeline_version"],
                "analysis_accession": record["analysis_accession"],
                "study_accession": analysis["study__accession"],
                "biome_lineage": lineages.get(analysis["sample__biome_id"]),
                "is_private": analysis["assembly__is_private"],
                "is_suppressed": analysis["assembly__is_suppressed"],
            }
        )
    return pa.Table.from_pylist(rows, layout.SCHEMAS["metadata/assembly_proteins"])


# The columns of the metadata snapshot that come from emgapi-v2, one row for each ERZ and major.minor.
ASSEMBLY_ANALYSES = pa.schema(
    [
        pa.field("assembly_accession", pa.string(), False),
        pa.field("pipeline_version", pa.string(), False),
        pa.field("analysis_accession", pa.string(), False),
        pa.field("study_accession", pa.string(), False),
        pa.field("biome_lineage", pa.string(), True),
        pa.field("is_private", pa.bool_(), False),
        pa.field("is_suppressed", pa.bool_(), False),
    ]
)


def export_assembly_analyses() -> pa.Table:
    """Every assembly analysis of emgapi-v2, of any state, by (ERZ accession, major.minor), with its metadata.

    For work outside emgapi-v2 that must give an MGYA to an assembly, such as the legacy import.
    When two analyses have the same ERZ and major.minor, the one with the lowest id is taken.

    :return: A table in the ASSEMBLY_ANALYSES schema, one row for each (ERZ, major.minor).
    """
    analyses = (
        Analysis.objects.filter(assembly__isnull=False)
        .select_related("assembly", "study", "sample__biome")
        .order_by("id")
    )
    rows = {}
    for analysis in analyses.iterator(chunk_size=CHUNK):
        biome = analysis.sample.biome if analysis.sample else None
        for erz in analysis.assembly.ena_accessions:
            rows.setdefault(
                (erz, analysis.pipeline_major_minor),
                {
                    "assembly_accession": erz,
                    "pipeline_version": analysis.pipeline_major_minor,
                    "analysis_accession": analysis.accession,
                    "study_accession": analysis.study.accession,
                    "biome_lineage": biome.pretty_lineage if biome else None,
                    "is_private": analysis.assembly.is_private,
                    "is_suppressed": analysis.assembly.is_suppressed,
                },
            )
    return pa.Table.from_pylist(list(rows.values()), ASSEMBLY_ANALYSES)
