"""The daily snapshot of the dimensions: registering studies and biomes, and building the four tables."""

from typing import Callable, NamedTuple

import psycopg
import pyarrow as pa

from proteins.tier2 import SCHEMAS


class Source(NamedTuple):
    """What emgapi-v2 knows about one assembly accession."""

    assembly_accession: str
    study_accession: str
    biome_lineage: str
    assembly_private: bool
    assembly_suppressed: bool
    study_private: bool
    study_suppressed: bool


Resolve = Callable[[list[str]], list[Source]]


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
            conn, "biome", "lineage", {s.biome_lineage for s in source.values()}
        )

    rows = []
    for assembly in assemblies.to_pylist():
        s = source.get(assembly["accession"])
        rows.append(
            {
                **assembly,
                "study_id": s and studies[s.study_accession],
                "biome_id": s and biomes[s.biome_lineage],
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
