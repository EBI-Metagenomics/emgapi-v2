"""The metadata snapshot: what emgapi-v2, or MGnify v1, knows of each assembly proteins record, written each day with the protein dataset."""

import logging
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from analyses.models import Analysis, Biome, Study
from proteins.dataset import layout

logger = logging.getLogger(__name__)

CHUNK = 10_000


def find_analyses(mgyas: list[str], root: Path) -> dict[str, dict]:
    """The snapshot's metadata of each of these MGYAs, from emgapi-v2, or from MGnify v1 for those emgapi-v2 does not have.

    The protein dataset holds the migration's copy of v1's analyses until the failed imports of their
    studies into emgapi-v2 are retried, and none is left there (D26).

    :param mgyas: The MGYAs.
    :param root: The protein dataset's directory.
    :return: By MGYA, the study, its ERP, the biome lineage, the flags, and whether it is from v1.
    """
    rows = {}
    for start in range(0, len(mgyas), CHUNK):
        rows |= {
            row["accession"]: row
            for row in Analysis.objects.filter(
                accession__in=mgyas[start : start + CHUNK]
            ).values(
                "accession",
                "study__accession",
                "study__ena_accessions",
                "sample__biome_id",
                "study__biome_id",
                "is_private",
                "is_suppressed",
                "assembly__is_private",
                "assembly__is_suppressed",
                "study__is_private",
                "study__is_suppressed",
            )
        }
    biomes = {
        mgya: row["sample__biome_id"] or row["study__biome_id"]
        for mgya, row in rows.items()
    }
    lineages = {
        biome.id: biome.pretty_lineage
        for biome in Biome.objects.filter(id__in=set(biomes.values()))
    }
    known = {
        mgya: {
            "study_accession": row["study__accession"],
            "ena_study_accession": Study(
                ena_accessions=row["study__ena_accessions"]
            ).first_accession,
            "biome_lineage": lineages.get(biomes[mgya]),
            # Withheld when the analysis, its assembly or its study is.
            "is_private": row["is_private"]
            or row["assembly__is_private"]
            or row["study__is_private"],
            "is_suppressed": row["is_suppressed"]
            or row["assembly__is_suppressed"]
            or row["study__is_suppressed"],
            "from_v1": False,
        }
        for mgya, row in rows.items()
    }
    v1 = root / layout.V1_ANALYSES
    if (missing := set(mgyas) - known.keys()) and v1.exists():
        for row in pq.read_table(
            v1, filters=[("analysis_accession", "in", sorted(missing))]
        ).to_pylist():
            del row["assembly_accession"], row["pipeline_version"]
            known[row.pop("analysis_accession")] = row | {"from_v1": True}
    return known


def build_snapshot(
    assembly_proteins: pa.Table, assemblies: pa.Table, root: Path
) -> pa.Table:
    """The metadata/assembly_proteins table for every assembly proteins record, of either source, whose MGYA is in emgapi-v2 or in the copy of v1.

    A record whose MGYA is in neither is withheld: releases publish nothing of a record without
    metadata, and it is back in the snapshot of the day after its analysis is in emgapi-v2.

    :param assembly_proteins: The assembly_proteins rows: id, assembly_id, source, pipeline_version, analysis_accession.
    :param assemblies: The assembly rows: id, accession.
    :param root: The protein dataset's directory.
    :return: The table in the metadata/assembly_proteins schema.
    """
    records = assembly_proteins.to_pylist()
    mgyas = sorted({record["analysis_accession"] for record in records})
    known = find_analyses(mgyas, root)
    accessions = dict(
        zip(assemblies["id"].to_pylist(), assemblies["accession"].to_pylist())
    )

    if missing := [mgya for mgya in mgyas if mgya not in known]:
        logger.warning(
            "withholding the records of %d MGYAs that are in neither emgapi-v2 nor MGnify v1,"
            " for example %s",
            len(missing),
            ", ".join(missing[:10]),
        )
    logger.info(
        "the metadata of %d MGYAs is from MGnify v1",
        sum(analysis["from_v1"] for analysis in known.values()),
    )

    rows = [
        {
            "assembly_proteins_id": record["id"],
            "assembly_id": record["assembly_id"],
            "assembly_accession": accessions[record["assembly_id"]],
            "source": record["source"],
            "pipeline_version": record["pipeline_version"],
            "analysis_accession": record["analysis_accession"],
            **known[record["analysis_accession"]],
        }
        for record in records
        if record["analysis_accession"] in known
    ]
    return pa.Table.from_pylist(rows, layout.SCHEMAS["metadata/assembly_proteins"])
