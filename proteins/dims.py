"""The daily snapshot of the dimensions: what emgapi-v2 knows of each assembly, and the four tables built from it."""

from typing import Callable, NamedTuple

import psycopg
import pyarrow as pa

from analyses.models import Analysis, Biome
from proteins.tier2 import SCHEMAS


class Source(NamedTuple):
    """What emgapi-v2 knows about one assembly accession."""

    assembly_accession: str
    study_accession: str
    biome_lineage: str | None
    assembly_private: bool
    assembly_suppressed: bool
    study_private: bool
    study_suppressed: bool


Resolve = Callable[[list[str]], list[Source]]

CHUNK = 10_000


def resolve(accessions: list[str]) -> list[Source]:
    """Each accession's assembly in emgapi-v2, with the study and biome of its newest analysis."""
    newest = {}
    for start in range(0, len(accessions), CHUNK):
        chunk = accessions[start : start + CHUNK]
        wanted = set(chunk)
        rows = (
            Analysis.objects.filter(assembly__ena_accessions__overlap=chunk)
            .order_by("created_at", "id")
            .values_list(
                "assembly__ena_accessions",
                "assembly__is_private",
                "assembly__is_suppressed",
                "study__accession",
                "study__is_private",
                "study__is_suppressed",
                "study__biome_id",
            )
        )
        for ena_accessions, *row in rows:
            for accession in wanted.intersection(ena_accessions):
                newest[accession] = row
    lineages = {
        biome.id: biome.pretty_lineage
        for biome in Biome.objects.filter(
            id__in={biome_id for *_, biome_id in newest.values()}
        )
    }
    return [
        Source(
            accession,
            study,
            lineages.get(biome_id),
            assembly_private,
            assembly_suppressed,
            study_private,
            study_suppressed,
        )
        for accession, (
            assembly_private,
            assembly_suppressed,
            study,
            study_private,
            study_suppressed,
            biome_id,
        ) in sorted(newest.items())
    ]


def snapshot(
    conn: psycopg.Connection,
    assemblies: pa.Table,
    gene_callers: pa.Table,
    resolve: Resolve,
) -> tuple[dict[str, pa.Table], int]:
    """The four tables for every registry assembly, and how many of them emgapi-v2 does not know."""
    source = {
        row.assembly_accession: row
        for row in resolve(sorted(set(assemblies["accession"].to_pylist())))
    }
    with conn.transaction():
        studies = register(
            conn, "study", "accession", {s.study_accession for s in source.values()}
        )
        biomes = register(
            conn,
            "biome",
            "lineage",
            {s.biome_lineage for s in source.values() if s.biome_lineage},
        )

    rows = []
    for assembly in assemblies.to_pylist():
        s = source.get(assembly["accession"])
        rows.append(
            {
                **assembly,
                "study_id": s and studies[s.study_accession],
                "biome_id": s and biomes.get(s.biome_lineage),
                "private": s and s.assembly_private,
                "suppressed": s and s.assembly_suppressed,
            }
        )
    study_flags = {
        s.study_accession: (s.study_private, s.study_suppressed)
        for s in source.values()
    }
    tables = {
        "assembly": pa.Table.from_pylist(rows, SCHEMAS["dims/assembly"]),
        "study": pa.Table.from_pylist(
            [
                {
                    "id": studies[accession],
                    "accession": accession,
                    "private": private,
                    "suppressed": suppressed,
                }
                for accession, (private, suppressed) in study_flags.items()
            ],
            SCHEMAS["dims/study"],
        ),
        "biome": pa.Table.from_pylist(
            [{"id": id, "lineage": lineage} for lineage, id in biomes.items()],
            SCHEMAS["dims/biome"],
        ),
        "gene_caller": gene_callers,
    }
    unresolved = sum(1 for row in rows if row["study_id"] is None)
    return tables, unresolved


def register(
    conn: psycopg.Connection, table: str, column: str, values: set[str]
) -> dict[str, int]:
    """Each value's id in a registry, adding new values."""
    ordered = sorted(values)
    # As for gene callers: known values are filtered out first, so that they take no identity value.
    conn.execute(
        f"""
        INSERT INTO {table} ({column})
        SELECT v FROM unnest(%s::text[]) AS v
        WHERE NOT EXISTS (SELECT 1 FROM {table} t WHERE t.{column} = v)
        ORDER BY v
        ON CONFLICT DO NOTHING
        """,
        [ordered],
    )
    return dict(
        conn.execute(
            f"SELECT {column}, id FROM {table} WHERE {column} = ANY(%s)", [ordered]
        ).fetchall()
    )
