"""Accessioning: looking up the proteins, and staging them in one transaction."""

import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

from django.db import IntegrityError, connections, transaction
from psycopg.errors import UniqueViolation

from proteins.models import (
    Assembly,
    AssemblyProteins,
    GeneCaller,
    Protein,
    StagedContig,
    StagedOccurrence,
    partition,
)
from proteins.routers import MGYP

from .predicted_genes import PredictedGenes, major_minor

logger = logging.getLogger(__name__)


class IncompleteRerun(Exception):
    """The assembly proteins record already exists, but some of its proteins have no MGYP."""


class AnalysisAlreadyRecorded(Exception):
    """The MGYA already has an assembly proteins record, of another assembly, source or pipeline version."""


def accession_assembly_proteins(
    assembly: str,
    source: str,
    pipeline_version: str,
    predicted_genes: PredictedGenes,
    analysis_accession: str,
    threads: int = 16,
) -> dict[bytes, int]:
    """Each protein's id, giving new ids to the proteins that have none and staging them.

    Safe to run again: when the assembly proteins record exists, it stages nothing and returns
    the id of every protein.

    :param assembly: The ENA assembly accession.
    :param source: The pipeline that predicted the genes: "assembly_analysis" or "gene_calling".
    :param pipeline_version: The source pipeline's version, reduced to major.minor.
    :param predicted_genes: The proteins, contigs and occurrences, from read_predicted_genes().
    :param analysis_accession: The MGYA of the assembly analysis.
    :param threads: How many threads look up partitions at once, each on its own connection.
    :raises InvalidInput: When the version is not major.minor with one digit each, before any query.
    """
    pipeline_version = major_minor(pipeline_version)
    hashes = [hash for hash, _ in predicted_genes.proteins]
    ids = lookup(hashes, threads)
    assembly_id = register_assembly(assembly)
    callers = register_gene_callers(
        sorted({(o.caller_name, o.caller_version) for o in predicted_genes.occurrences})
    )
    staged = stage(
        assembly_id,
        source,
        pipeline_version,
        predicted_genes,
        ids,
        callers,
        analysis_accession,
    )
    if staged is not None:
        return ids | staged

    # A rerun: the first attempt may have committed the proteins the lookup missed since.
    ids |= lookup([hash for hash in hashes if hash not in ids], threads)
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


def lookup(hashes: list[bytes], threads: int) -> dict[bytes, int]:
    """The ids of the hashes that have one, with the partitions shared out between threads.

    Django gives each thread a connection, and never closes those of short-lived threads,
    so each thread closes its own.

    :param hashes: The distinct protein hashes.
    :param threads: How many threads look up partitions at once, each on its own connection.
    """
    by_partition = defaultdict(list)
    for hash in hashes:
        by_partition[partition(hash)].append(hash)
    groups = list(by_partition.values())
    shares = [s for s in (groups[i::threads] for i in range(threads)) if s]
    if not shares:
        return {}

    def look_up_share(share):
        try:
            return Protein.objects.ids_of_hashes(h for group in share for h in group)
        finally:
            connections[MGYP].close()

    found = {}
    with ThreadPoolExecutor(len(shares)) as pool:
        for ids in pool.map(look_up_share, shares):
            found |= ids
    return found


def register_assembly(accession: str) -> int:
    """The assembly's id, registering it if it is new.

    Done before the transaction, as register_gene_callers() is. get_or_create() looks first,
    so a registered assembly takes no identity value.

    :param accession: The ENA assembly accession.
    """
    assembly, _ = Assembly.objects.get_or_create(accession=accession)
    return assembly.id


def register_gene_callers(callers: list[tuple[str, str]]) -> dict[tuple[str, str], int]:
    """Each caller's id, registering new ones.

    Done before the transaction so that a new caller is committed at once, and
    other jobs meeting it do not wait for this job's transaction. get_or_create() looks first,
    so a known caller takes no identity value, and the smallint does not run out.

    :param callers: The (name, version) of each gene caller, sorted.
    """
    return {
        (name, version): GeneCaller.objects.get_or_create(name=name, version=version)[
            0
        ].id
        for name, version in callers
    }


def stage(
    assembly_id: int,
    source: str,
    pipeline_version: str,
    input: PredictedGenes,
    found: dict[bytes, int],
    callers: dict[tuple[str, str], int],
    analysis_accession: str,
) -> dict[bytes, int] | None:
    """The transaction: the ids of the proteins the lookup did not find, or None if the assembly proteins record exists.

    Raises AnalysisAlreadyRecorded when only the MGYA has a record.

    :param assembly_id: The assembly's id, from register_assembly().
    :param source: The pipeline that predicted the genes: "assembly_analysis" or "gene_calling".
    :param pipeline_version: The source pipeline's major.minor.
    :param input: The proteins, contigs and occurrences, from read_predicted_genes().
    :param found: The ids the lookup found.
    :param callers: Each gene caller's id, by (name, version).
    :param analysis_accession: The MGYA of the assembly analysis.
    """
    with transaction.atomic(using=MGYP):
        try:
            # Another attempt that holds the same record uncommitted makes this wait for it.
            with transaction.atomic(using=MGYP):
                record = AssemblyProteins.objects.create(
                    assembly_id=assembly_id,
                    source=source,
                    pipeline_version=pipeline_version,
                    analysis_accession=analysis_accession,
                )
        except IntegrityError as e:
            if not isinstance(e.__cause__, UniqueViolation):
                raise
            if AssemblyProteins.objects.filter(
                assembly_id=assembly_id,
                source=source,
                pipeline_version=pipeline_version,
            ).exists():
                return None
            existing = AssemblyProteins.objects.select_related("assembly").get(
                analysis_accession=analysis_accession
            )
            raise AnalysisAlreadyRecorded(
                f"{analysis_accession} already has the assembly proteins record"
                f" {existing.assembly.accession} {existing.source} {existing.pipeline_version}"
            ) from e

        # Including the ones another job committed since the lookup.
        staged = Protein.objects.allocate(
            [
                (hash, sequence)
                for hash, sequence in input.proteins
                if hash not in found
            ],
            record.id,
        )
        contig_ids = stage_contigs(record.id, input)
        stage_occurrences(record.id, input, found | staged, contig_ids, callers)
    return staged


def stage_contigs(assembly_proteins_id: int, input: PredictedGenes) -> dict[str, int]:
    """Stages the contigs that carry a gene.

    :param assembly_proteins_id: The record that introduces them.
    :param input: The proteins, contigs and occurrences, from read_predicted_genes().
    :return: Each contig's id, by name.
    """
    StagedContig.objects.copy_rows(
        [
            "assembly_proteins",
            "name",
            "original_name",
            "length",
            "hash",
            "kmer_coverage",
        ],
        ((assembly_proteins_id, *contig) for contig in input.contigs),
    )
    return dict(
        StagedContig.objects.filter(
            assembly_proteins_id=assembly_proteins_id
        ).values_list("name", "id")
    )


def stage_occurrences(
    assembly_proteins_id: int,
    input: PredictedGenes,
    ids: dict[bytes, int],
    contig_ids: dict[str, int],
    callers: dict[tuple[str, str], int],
) -> None:
    """Stages the occurrences.

    :param assembly_proteins_id: The record that introduces them.
    :param input: The proteins, contigs and occurrences, from read_predicted_genes().
    :param ids: Each protein's id.
    :param contig_ids: Each staged contig's id, by name.
    :param callers: Each gene caller's id, by (name, version).
    """
    StagedOccurrence.objects.copy_rows(
        [
            "assembly_proteins",
            "gene_id",
            "protein_id",
            "contig",
            "gene_caller",
            "start_position",
            "end_position",
            "strand",
            "truncation",
        ],
        (
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
        ),
    )
