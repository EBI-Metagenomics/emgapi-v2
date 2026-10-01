"""Steps 3 to 5 of accessioning: looking up, staging, and writing the output."""

import gzip
import logging
import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Callable

import psycopg
from psycopg import sql

from .inputs import Input, Occurrence

logger = logging.getLogger(__name__)

Connect = Callable[[], psycopg.Connection]  # a new connection, in autocommit


class IncompleteRerun(Exception):
    """The analysis is already registered, but some of its proteins have no MGYP."""


def accession(
    connect: Connect,
    assembly: str,
    pipeline_version: str,
    input: Input,
    connections: int = 16,
) -> dict[bytes, int]:
    """Each protein's id, allocating and staging those that have none."""
    hashes = [hash for hash, _ in input.proteins]
    ids = lookup(connect, hashes, connections)
    with connect() as conn:
        callers = register_gene_callers(
            conn, sorted({(o.caller_name, o.caller_version) for o in input.occurrences})
        )
        staged = stage(conn, assembly, pipeline_version, input, ids, callers)
    if staged is not None:
        return ids | staged

    # A rerun: the first attempt may have committed the proteins step 3 missed since.
    ids |= lookup(connect, [hash for hash in hashes if hash not in ids], connections)
    missing = len(hashes) - len(ids)
    if missing:
        raise IncompleteRerun(
            f"{assembly} {pipeline_version} is already registered,"
            f" but {missing} of its proteins have no MGYP"
        )
    logger.warning(
        "%s %s was already accessioned; nothing was staged", assembly, pipeline_version
    )
    return ids


def lookup(connect: Connect, hashes: list[bytes], connections: int) -> dict[bytes, int]:
    """The ids of the hashes that have one, looked up partition by partition."""
    groups = defaultdict(list)
    for hash in hashes:
        groups[hash[0] >> 2].append(hash)
    shares = [
        s
        for s in (list(groups.items())[i::connections] for i in range(connections))
        if s
    ]
    if not shares:
        return {}

    def look_up_share(share):
        with connect() as conn:
            return lookup_partitions(conn, share)

    found = {}
    with ThreadPoolExecutor(len(shares)) as pool:
        for ids in pool.map(look_up_share, shares):
            found |= ids
    return found


def lookup_partitions(
    conn: psycopg.Connection, groups: list[tuple[int, list[bytes]]]
) -> dict[bytes, int]:
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS q (hash bytea)")
    found = {}
    for partition, hashes in groups:
        conn.execute("TRUNCATE q")
        with conn.cursor().copy("COPY q (hash) FROM STDIN (FORMAT binary)") as copy:
            copy.set_types(["bytea"])
            for hash in hashes:
                copy.write_row((hash,))
        conn.execute("ANALYZE q")  # autovacuum never analyses temp tables
        found |= dict(
            conn.execute(
                sql.SQL("SELECT q.hash, k.id FROM q JOIN {} k USING (hash)").format(
                    sql.Identifier("proteindb", f"protein_key_{partition:02x}")
                )
            ).fetchall()
        )
    return found


def register_gene_callers(
    conn: psycopg.Connection, callers: list[tuple[str, str]]
) -> dict[tuple[str, str], int]:
    """Each caller's id, registering new ones.

    Done before the transaction so that a new caller is committed at once, and
    other jobs meeting it do not wait for this job's transaction.
    """
    names, versions = [name for name, _ in callers], [v for _, v in callers]
    # PostgreSQL takes an identity value for every candidate row, even one ON CONFLICT discards,
    # so known callers are filtered out first or the smallint would run out.
    conn.execute(
        """
        INSERT INTO gene_caller (name, version)
        SELECT v.name, v.version FROM unnest(%s::text[], %s::text[]) AS v (name, version)
        WHERE NOT EXISTS (SELECT 1 FROM gene_caller g WHERE g.name = v.name AND g.version = v.version)
        ORDER BY v.name, v.version
        ON CONFLICT DO NOTHING
        """,
        [names, versions],
    )
    rows = conn.execute(
        """
        SELECT name, version, id FROM gene_caller
        WHERE (name, version) IN (SELECT * FROM unnest(%s::text[], %s::text[]))
        """,
        [names, versions],
    )
    return {(name, version): id for name, version, id in rows}


def stage(
    conn: psycopg.Connection,
    assembly: str,
    pipeline_version: str,
    input: Input,
    found: dict[bytes, int],
    callers: dict[tuple[str, str], int],
) -> dict[bytes, int] | None:
    """The transaction: the ids of the proteins step 3 did not find, or None if the analysis is already registered."""
    staged = None
    with conn.transaction():
        row = conn.execute(
            "INSERT INTO assembly (accession, pipeline_version) VALUES (%s, %s)"
            " ON CONFLICT DO NOTHING RETURNING id",
            [assembly, pipeline_version],
        ).fetchone()
        if row is None:
            raise psycopg.Rollback
        (assembly_id,) = row

        conn.execute(
            "CREATE TEMP TABLE new_protein (sequence text, hash bytea PRIMARY KEY) ON COMMIT DROP"
        )
        copy_rows(
            conn,
            "new_protein (hash, sequence)",
            ["bytea", "text"],
            [
                (hash, sequence)
                for hash, sequence in input.proteins
                if hash not in found
            ],
        )
        # In hash order, so that jobs with overlapping new proteins lock them in the same order.
        conn.execute(
            """
            WITH ins AS (
                INSERT INTO protein_key (hash, id)
                SELECT hash, nextval('protein_id_seq') FROM new_protein ORDER BY hash
                ON CONFLICT (hash) DO NOTHING
                RETURNING hash, id
            )
            INSERT INTO staging_protein (id, hash, sequence, assembly_id)
            SELECT ins.id, ins.hash, n.sequence, %s FROM ins JOIN new_protein n USING (hash)
            """,
            [assembly_id],
        )
        # Including the ones another job committed since step 3.
        staged = dict(
            conn.execute(
                "SELECT n.hash, k.id FROM new_protein n JOIN protein_key k USING (hash)"
            ).fetchall()
        )

        copy_rows(
            conn,
            "staging_contig (assembly_id, name, original_name, length, hash, kmer_coverage)",
            ["int4", "text", "text", "int4", "bytea", "float8"],
            [(assembly_id, *contig) for contig in input.contigs],
        )
        contig_ids = dict(
            conn.execute(
                "SELECT name, id FROM staging_contig WHERE assembly_id = %s",
                [assembly_id],
            ).fetchall()
        )
        ids = found | staged
        copy_rows(
            conn,
            "staging_occurrence (assembly_id, gene_id, protein_id, contig_id, gene_caller_id,"
            " start_position, end_position, strand)",
            ["int4", "text", "int8", "int8", "int2", "int4", "int4", "int2"],
            [
                (
                    assembly_id,
                    o.gene_id,
                    ids[o.hash],
                    contig_ids[o.contig],
                    callers[o.caller_name, o.caller_version],
                    o.start,
                    o.end,
                    o.strand,
                )
                for o in input.occurrences
            ],
        )
    return staged


def copy_rows(conn: psycopg.Connection, table: str, types: list[str], rows) -> None:
    with conn.cursor().copy(f"COPY {table} FROM STDIN (FORMAT binary)") as copy:
        copy.set_types(types)
        for row in rows:
            copy.write_row(row)


def mgyp(id: int) -> str:
    return f"MGYP{id:012d}"


def write_output(path, occurrences: list[Occurrence], ids: dict[bytes, int]) -> None:
    """The input FASTA with each gene's MGYP after its ID, written under a temporary name and renamed into place."""
    path = Path(path)
    tmp = path.with_name(f".{path.name}.tmp")
    with open(tmp, "wb") as raw:
        out = (
            gzip.GzipFile(filename="", fileobj=raw, mode="wb", compresslevel=6, mtime=0)
            if path.suffix == ".gz"
            else raw
        )
        for o in occurrences:
            header = " ".join(
                filter(None, (o.gene_id, mgyp(ids[o.hash]), o.description))
            )
            out.write(f">{header}\n{o.sequence}\n".encode())
        if out is not raw:
            out.close()
        raw.flush()
        os.fsync(raw.fileno())
    os.replace(tmp, path)
