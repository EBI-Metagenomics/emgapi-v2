"""Steps 3 and 4 of accessioning: looking up, and staging in one transaction."""

import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, ContextManager

import psycopg
from psycopg import sql

from .inputs import Input, major_minor

logger = logging.getLogger(__name__)

# Gives a connection to the MGYP database, in autocommit, for the current thread, and takes it back
# at the end of the with block. The caller decides where the connection comes from: in emgapi-v2,
# proteins.routers.mgyp_connection() gives the connection of the Django mgyp alias.
Connect = Callable[[], ContextManager[psycopg.Connection]]


class IncompleteRerun(Exception):
    """The assembly proteins record already exists, but some of its proteins have no MGYP."""


def accession(
    connect: Connect,
    assembly: str,
    source: str,
    pipeline_version: str,
    input: Input,
    analysis_accession: str,
    connections: int = 16,
) -> dict[bytes, int]:
    """Each protein's id, giving new ids to the proteins that have none and staging them.

    Safe to run again: when the assembly proteins record exists, it stages nothing and returns
    the id of every protein.

    :param connect: Gives a connection to the MGYP database, for example proteins.routers.mgyp_connection.
    :param assembly: The ENA assembly accession.
    :param source: The pipeline that predicted the genes: "assembly_analysis" or "gene_calling".
    :param pipeline_version: The source pipeline's version, reduced to major.minor.
    :param input: The proteins, contigs and occurrences, from read_input().
    :param analysis_accession: The MGYA of the assembly analysis.
    :param connections: How many connections the lookups use.
    :raises InvalidInput: When the version is not major.minor with one digit each, before any connection.
    """
    pipeline_version = major_minor(pipeline_version)
    hashes = [hash for hash, _ in input.proteins]
    ids = lookup(connect, hashes, connections)
    with connect() as conn:
        callers = register_gene_callers(
            conn, sorted({(o.caller_name, o.caller_version) for o in input.occurrences})
        )
        staged = stage(
            conn,
            assembly,
            source,
            pipeline_version,
            input,
            ids,
            callers,
            analysis_accession,
        )
    if staged is not None:
        return ids | staged

    # A rerun: the first attempt may have committed the proteins step 3 missed since.
    ids |= lookup(connect, [hash for hash in hashes if hash not in ids], connections)
    missing = len(hashes) - len(ids)
    if missing:
        raise IncompleteRerun(
            f"The assembly proteins record {assembly} {source} {pipeline_version} already exists,"
            f" but {missing} of its proteins have no MGYP"
        )
    logger.warning(
        "The assembly proteins record %s %s %s already exists; nothing was staged",
        assembly,
        source,
        pipeline_version,
    )
    return ids


def lookup(connect: Connect, hashes: list[bytes], connections: int) -> dict[bytes, int]:
    """The ids of the hashes that have one, looked up partition by partition.

    :param connect: Gives a connection to the MGYP database, in each thread of the lookups.
    :param hashes: The distinct protein hashes.
    :param connections: How many connections look up partitions at once.
    """
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
    """The ids of the hashes that have one, one partition after another, on one connection.

    :param conn: A connection to the MGYP database, in autocommit.
    :param groups: Each partition with its hashes.
    """
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
                    sql.Identifier("proteindb", f"protein_{partition:02x}")
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

    :param conn: A connection to the MGYP database, in autocommit.
    :param callers: The (name, version) of each gene caller, sorted.
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
    source: str,
    pipeline_version: str,
    input: Input,
    found: dict[bytes, int],
    callers: dict[tuple[str, str], int],
    analysis_accession: str,
) -> dict[bytes, int] | None:
    """The transaction: the ids of the proteins step 3 did not find, or None if the assembly proteins record exists.

    :param conn: A connection to the MGYP database, in autocommit.
    :param assembly: The ENA assembly accession.
    :param source: The pipeline that predicted the genes: "assembly_analysis" or "gene_calling".
    :param pipeline_version: The source pipeline's major.minor.
    :param input: The proteins, contigs and occurrences, from read_input().
    :param found: The ids the lookup found.
    :param callers: Each gene caller's id, by (name, version).
    :param analysis_accession: The MGYA of the assembly analysis.
    """
    staged = None
    with conn.transaction():
        row = conn.execute(
            "INSERT INTO assembly_proteins (assembly_accession, source, pipeline_version, analysis_accession)"
            " VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING RETURNING id",
            [assembly, source, pipeline_version, analysis_accession],
        ).fetchone()
        if row is None:
            raise psycopg.Rollback
        (assembly_proteins_id,) = row

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
                INSERT INTO protein (hash, id)
                SELECT hash, nextval('protein_id_seq') FROM new_protein ORDER BY hash
                ON CONFLICT (hash) DO NOTHING
                RETURNING hash, id
            )
            INSERT INTO staged_protein (id, hash, sequence, assembly_proteins_id)
            SELECT ins.id, ins.hash, n.sequence, %s FROM ins JOIN new_protein n USING (hash)
            """,
            [assembly_proteins_id],
        )
        # Including the ones another job committed since step 3.
        staged = dict(
            conn.execute(
                "SELECT n.hash, k.id FROM new_protein n JOIN protein k USING (hash)"
            ).fetchall()
        )

        copy_rows(
            conn,
            "staged_contig (assembly_proteins_id, name, original_name, length, hash, kmer_coverage)",
            ["int4", "text", "text", "int4", "bytea", "float8"],
            [(assembly_proteins_id, *contig) for contig in input.contigs],
        )
        contig_ids = dict(
            conn.execute(
                "SELECT name, id FROM staged_contig WHERE assembly_proteins_id = %s",
                [assembly_proteins_id],
            ).fetchall()
        )
        ids = found | staged
        copy_rows(
            conn,
            "staged_occurrence (assembly_proteins_id, gene_id, protein_id, contig_id, gene_caller_id,"
            " start_position, end_position, strand, truncation)",
            ["int4", "text", "int8", "int8", "int2", "int4", "int4", "int2", "text"],
            [
                (
                    assembly_proteins_id,
                    o.gene_id,
                    ids[o.hash],
                    contig_ids[o.contig],
                    callers[o.caller_name, o.caller_version],
                    o.start,
                    o.end,
                    o.strand,
                    o.truncation,
                )
                for o in input.occurrences
            ],
        )
    return staged


def copy_rows(conn: psycopg.Connection, table: str, types: list[str], rows) -> None:
    """Writes rows to a table with a binary COPY.

    :param conn: A connection to the MGYP database.
    :param table: The table, with its column list.
    :param types: The PostgreSQL type of each column.
    :param rows: Tuples in column order.
    """
    with conn.cursor().copy(f"COPY {table} FROM STDIN (FORMAT binary)") as copy:
        copy.set_types(types)
        for row in rows:
            copy.write_row(row)


def mgyp(id: int) -> str:
    """The MGYP accession of a protein id.

    :param id: The protein's id.
    """
    return f"MGYP{id:012d}"
